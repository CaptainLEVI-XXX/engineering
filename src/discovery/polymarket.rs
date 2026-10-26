//! Gamma transport and paginated discovery driver.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, sleep, sleep_until};

use super::{MarketKey, Page, Registry, Snapshot, Version};

pub const GAMMA_MARKETS: &str = "https://gamma-api.polymarket.com/markets/keyset";

pub struct Gamma {
    client: Client,
    endpoint: Url,
}

impl Gamma {
    pub fn new(endpoint: &str) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(15))
                .user_agent("voluntas-discovery/0.1")
                .build()?,
            endpoint: Url::parse(endpoint)?,
        })
    }

    async fn page(
        &self,
        version: Version,
        cursor: Option<&str>,
        ids: &[String],
    ) -> Result<Vec<u8>> {
        let mut url = self.endpoint.clone();
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("version", version.as_str())
                .append_pair("limit", "100")
                .append_pair("closed", if ids.is_empty() { "false" } else { "any" });
            if let Some(cursor) = cursor {
                query.append_pair("after_cursor", cursor);
            }
            for id in ids {
                query.append_pair("id", id);
            }
        }
        for attempt in 0..3 {
            let mut retry_after = None;
            let error = match self.client.get(url.clone()).send().await {
                Ok(mut response) if response.status().is_success() => {
                    let mut body = Vec::new();
                    while let Some(chunk) = response.chunk().await? {
                        ensure!(
                            body.len() + chunk.len() <= 8 * 1024 * 1024,
                            "Gamma page exceeds 8 MiB safety budget"
                        );
                        body.extend_from_slice(&chunk);
                    }
                    return Ok(body);
                }
                Ok(response) => {
                    let status = response.status();
                    if status != StatusCode::TOO_MANY_REQUESTS && !status.is_server_error() {
                        anyhow::bail!("Gamma HTTP {status}");
                    }
                    if let Some(value) = response.headers().get("retry-after") {
                        let text = value.to_str()?;
                        let seconds = text
                            .parse::<u64>()
                            .or_else(|_| {
                                chrono::DateTime::parse_from_rfc2822(text).map(|date| {
                                    (date.timestamp() - chrono::Utc::now().timestamp()).max(0)
                                        as u64
                                })
                            })
                            .context("invalid Retry-After")?;
                        retry_after = Some(Duration::from_secs(seconds));
                    }
                    anyhow::anyhow!("Gamma HTTP {status}")
                }
                Err(error) => {
                    if !error.is_timeout() && !error.is_connect() && !error.is_request() {
                        return Err(error.into());
                    }
                    error.into()
                }
            };
            // Respect server backoff even after the last attempt, so the next
            // version/batch cannot bypass it. The driver's deadline and Stop
            // command can cancel this wait without issuing another request.
            if let Some(delay) = retry_after {
                let until = Instant::now()
                    .checked_add(delay)
                    .context("Retry-After exceeds clock range")?;
                sleep_until(until).await;
            }
            if attempt == 2 {
                return Err(error);
            }
            let jitter = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_millis() as u64
                % 100;
            if retry_after.is_none() {
                sleep(Duration::from_millis(250 * (1 << attempt) + jitter)).await;
            }
        }
        unreachable!("three attempts return success or an error")
    }
}

pub enum Command {
    Snapshot(oneshot::Sender<Arc<Snapshot>>),
    Hint(MarketKey),
    Stop,
}

#[derive(Debug, Serialize)]
pub struct ScanReport {
    pub version: Version,
    pub pages: usize,
    pub records: usize,
    pub unresolved_records: usize,
    pub pagination_complete: bool,
    pub error: Option<String>,
}

struct Scan {
    report: ScanReport,
    cursor: Option<String>,
    seen_cursors: HashSet<String>,
    missing: HashSet<MarketKey>,
    finished: bool,
}

impl Scan {
    fn new(version: Version, registry: &Registry) -> Self {
        Self {
            report: ScanReport {
                version,
                pages: 0,
                records: 0,
                unresolved_records: 0,
                pagination_complete: false,
                error: None,
            },
            cursor: None,
            seen_cursors: HashSet::new(),
            finished: false,
            missing: registry
                .markets
                .iter()
                .filter(|(k, m)| k.version == version && m.desired)
                .map(|(k, _)| k.clone())
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DiscoveryReport {
    /// Last sweep only; a completed traversal is not an atomic exchange snapshot.
    pub scans: [ScanReport; 2],
    pub elapsed_ms: u128,
    pub first_publication_ms: Option<u128>,
    pub max_page_apply_us: u128,
    pub desired_assets: usize,
    pub uncertain_markets: usize,
    pub pending_verifications: usize,
    pub failed_verification_batches: usize,
    pub stopped: bool,
}

/// One mutable owner, one request future at a time. Commands remain serviceable
/// during HTTP and retry waits. No WebSocket or market-data path runs here.
pub struct Discovery {
    gamma: Gamma,
    registry: Registry,
    commands: mpsc::Receiver<Command>,
    commands_open: bool,
    next_check: HashMap<MarketKey, Instant>,
    stopped: bool,
    failed_checks: usize,
}

impl Discovery {
    pub fn new(gamma: Gamma) -> (Self, mpsc::Sender<Command>, watch::Receiver<()>) {
        let registry = Registry::default();
        let changes = registry.subscribe();
        let (send, commands) = mpsc::channel(64);
        (
            Self {
                gamma,
                registry,
                commands,
                commands_open: true,
                next_check: HashMap::new(),
                stopped: false,
                failed_checks: 0,
            },
            send,
            changes,
        )
    }

    async fn request(
        &mut self,
        version: Version,
        cursor: Option<&str>,
        ids: &[String],
        deadline: Instant,
    ) -> Option<Result<Vec<u8>>> {
        let request = self.gamma.page(version, cursor, ids);
        tokio::pin!(request);
        loop {
            tokio::select! {
                _ = sleep_until(deadline) => { self.stopped = true; return None; }
                command = self.commands.recv(), if self.commands_open => {
                    self.commands_open = command.is_some();
                    if handle_command(&mut self.registry, command) { self.stopped = true; return None; }
                }
                response = &mut request => return Some(response),
            }
        }
    }

    async fn verify_one_batch(&mut self, deadline: Instant) -> bool {
        let now = Instant::now();
        let Some(first) = self
            .registry
            .pending
            .iter()
            .find(|k| self.next_check.get(*k).is_none_or(|t| *t <= now))
        else {
            return false;
        };
        let version = first.version;
        let keys: Vec<_> = self
            .registry
            .pending
            .iter()
            .filter(|k| k.version == version && self.next_check.get(*k).is_none_or(|t| *t <= now))
            .take(100)
            .cloned()
            .collect();
        let ids: Vec<_> = keys.iter().map(|k| k.id.clone()).collect();
        let mut records = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        let mut failure = None;
        loop {
            let Some(result) = self
                .request(version, cursor.as_deref(), &ids, deadline)
                .await
            else {
                return true;
            };
            let result =
                result.and_then(|bytes| serde_json::from_slice::<Page>(&bytes).map_err(Into::into));
            match result {
                Ok(page) => {
                    records.extend(page.markets);
                    if let Some(next) = page.next_cursor {
                        if next.is_empty() || !seen.insert(next.clone()) || records.len() > 1000 {
                            failure =
                                Some("invalid/budget-exceeding verification pagination".to_owned());
                            break;
                        }
                        cursor = Some(next);
                    } else {
                        self.registry.verify_records(version, &ids, &records);
                        break;
                    }
                }
                Err(error) => {
                    failure = Some(format!("{error:#}"));
                    break;
                }
            }
        }
        if let Some(error) = failure {
            self.failed_checks += 1;
            eprintln!("verification failed: {error}");
            for key in &keys {
                self.registry.uncertain(key, "targeted request failed");
            }
        }
        for key in keys {
            if self.registry.pending.contains(&key) {
                self.next_check
                    .insert(key, Instant::now() + Duration::from_secs(30));
            } else {
                self.next_check.remove(&key);
            }
        }
        true
    }

    async fn idle(&mut self, until: Instant, deadline: Instant) {
        tokio::select! {
            _ = sleep_until(deadline) => self.stopped = true,
            _ = sleep_until(until) => {},
            command = self.commands.recv(), if self.commands_open => {
                self.commands_open = command.is_some();
                self.stopped = handle_command(&mut self.registry, command);
            }
        }
    }

    pub async fn run_for(&mut self, duration: Duration, once: bool) -> DiscoveryReport {
        let started = Instant::now();
        let deadline = started + duration;
        let mut first_publication_ms = None;
        let mut max_page_apply_us = 0;
        let scans = loop {
            let mut scans = [
                Scan::new(Version::V1, &self.registry),
                Scan::new(Version::V2, &self.registry),
            ];
            while !self.stopped && scans.iter().any(|s| !s.finished) {
                for scan in &mut scans {
                    if self.stopped {
                        break;
                    }
                    if scan.finished {
                        continue;
                    }
                    let version = scan.report.version;
                    let Some(result) = self
                        .request(version, scan.cursor.as_deref(), &[], deadline)
                        .await
                    else {
                        break;
                    };
                    let apply_started = Instant::now();
                    match result.and_then(|bytes| self.registry.apply_listing(version, &bytes)) {
                        Ok(page) => {
                            scan.report.pages += 1;
                            scan.report.records += page.records;
                            scan.report.unresolved_records += page.unresolved;
                            for key in page.observed {
                                scan.missing.remove(&key);
                            }
                            if let Some(next) = page.next_cursor {
                                if !scan.seen_cursors.insert(next.clone()) {
                                    scan.report.error = Some("repeated cursor".into());
                                    scan.finished = true;
                                } else {
                                    scan.cursor = Some(next);
                                }
                            } else {
                                scan.report.pagination_complete = true;
                                scan.finished = true;
                                for key in scan.missing.drain() {
                                    self.registry.uncertain(&key, "absent from completed scan");
                                }
                            }
                        }
                        Err(error) => {
                            scan.report.error = Some(format!("{error:#}"));
                            scan.finished = true;
                            eprintln!("{} scan failed: {error:#}", version.as_str());
                        }
                    }
                    max_page_apply_us = max_page_apply_us.max(apply_started.elapsed().as_micros());
                    if first_publication_ms.is_none() && !self.registry.markets.is_empty() {
                        first_publication_ms = Some(started.elapsed().as_millis());
                    }
                    self.verify_one_batch(deadline).await;
                }
            }
            while !self.stopped && self.verify_one_batch(deadline).await {}
            if once || self.stopped {
                break scans;
            }
            let resume = Instant::now() + Duration::from_secs(60);
            while !self.stopped && Instant::now() < resume {
                if !self.verify_one_batch(deadline).await {
                    let wake = self
                        .next_check
                        .values()
                        .copied()
                        .min()
                        .unwrap_or(resume)
                        .min(resume);
                    self.idle(wake, deadline).await;
                }
            }
            if self.stopped {
                break scans;
            }
        };
        let snapshot = self.registry.snapshot();
        DiscoveryReport {
            scans: scans.map(|s| s.report),
            elapsed_ms: started.elapsed().as_millis(),
            first_publication_ms,
            max_page_apply_us,
            desired_assets: snapshot.desired_assets(),
            uncertain_markets: snapshot
                .markets
                .values()
                .filter(|m| m.uncertainty.is_some())
                .count(),
            pending_verifications: self.registry.pending.len(),
            failed_verification_batches: self.failed_checks,
            stopped: self.stopped,
        }
    }
}

fn handle_command(registry: &mut Registry, command: Option<Command>) -> bool {
    match command {
        Some(Command::Snapshot(reply)) => {
            let _ = reply.send(registry.snapshot());
        }
        Some(Command::Hint(key)) => registry.request_verification(key),
        Some(Command::Stop) => return true,
        None => {}
    }
    false
}
