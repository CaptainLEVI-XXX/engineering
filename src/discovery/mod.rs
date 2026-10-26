//! Single-owner desired-state registry. This module makes no subscription claims.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::sync::watch;

pub mod polymarket;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Version {
    V1,
    V2,
}

impl Version {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MarketKey {
    pub version: Version,
    pub id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Outcome {
    pub asset_id: String,
    pub label: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketEntry {
    pub outcomes: [Outcome; 2],
    pub desired: bool,
    pub uncertainty: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub markets: HashMap<MarketKey, MarketEntry>,
}

impl Snapshot {
    pub fn desired_assets(&self) -> usize {
        self.markets.values().filter(|m| m.desired).count() * 2
    }
}

#[derive(Deserialize)]
pub(crate) struct Page {
    pub markets: Vec<Box<RawValue>>,
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub struct PageReport {
    pub records: usize,
    pub unresolved: usize,
    pub observed: HashSet<MarketKey>,
    pub next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMarket {
    id: String,
    version: Option<Version>,
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
    outcomes: Option<String>,
    clob_token_ids: Option<String>,
    position_ids: Option<Vec<String>>,
}

#[derive(Debug, Eq, PartialEq)]
enum Eligibility {
    Eligible,
    Closed,
    Unavailable,
}

fn normalize(raw: &RawValue, version: Version) -> Result<(MarketKey, [Outcome; 2], Eligibility)> {
    let m: RawMarket = serde_json::from_str(raw.get()).context("malformed required field")?;
    ensure!(!m.id.is_empty(), "missing market ID");
    ensure!(
        m.version == Some(version),
        "missing or mismatched protocol version"
    );
    let ids: Vec<String> = match version {
        Version::V1 => {
            serde_json::from_str(m.clob_token_ids.as_deref().context("missing token IDs")?)?
        }
        Version::V2 => m.position_ids.context("missing position IDs")?,
    };
    let labels: Vec<String> =
        serde_json::from_str(m.outcomes.as_deref().context("missing outcomes")?)?;
    ensure!(
        ids.len() == 2 && labels.len() == 2,
        "unsupported or mismatched outcome mapping"
    );
    ensure!(
        ids[0] != ids[1]
            && ids
                .iter()
                .all(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())),
        "invalid or duplicate outcome IDs"
    );

    // active=true and closed=true is a real Gamma combination. Order entry is
    // not a prerequisite for listening to an otherwise enabled, open book.
    let eligibility = if m.closed == Some(true) || m.archived == Some(true) {
        ensure!(
            m.accepting_orders != Some(true),
            "closed/archived market accepts orders"
        );
        Eligibility::Closed
    } else {
        ensure!(
            m.closed == Some(false) && m.archived == Some(false),
            "unknown lifecycle status"
        );
        match (m.active, m.enable_order_book) {
            (Some(true), Some(true)) => Eligibility::Eligible,
            (Some(false), _) | (_, Some(false)) => Eligibility::Unavailable,
            _ => anyhow::bail!("unknown deployment/book status"),
        }
    };
    let mut pairs = ids
        .into_iter()
        .zip(labels)
        .map(|(asset_id, label)| Outcome { asset_id, label });
    let outcomes = [pairs.next().unwrap(), pairs.next().unwrap()];
    Ok((MarketKey { version, id: m.id }, outcomes, eligibility))
}

fn identity(raw: &RawValue, version: Version) -> Option<MarketKey> {
    #[derive(Deserialize)]
    struct Identity {
        id: String,
    }
    serde_json::from_str::<Identity>(raw.get())
        .ok()
        .filter(|m| !m.id.is_empty())
        .map(|m| MarketKey { version, id: m.id })
}

pub struct Registry {
    markets: HashMap<MarketKey, MarketEntry>,
    asset_owner: HashMap<(Version, String), MarketKey>,
    pending: HashSet<MarketKey>,
    cached: Option<Arc<Snapshot>>,
    changed: watch::Sender<()>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            markets: HashMap::new(),
            asset_owner: HashMap::new(),
            pending: HashSet::new(),
            cached: None,
            changed: watch::channel(()).0,
        }
    }
}

impl Registry {
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub fn snapshot(&mut self) -> Arc<Snapshot> {
        self.cached
            .get_or_insert_with(|| {
                Arc::new(Snapshot {
                    markets: self.markets.clone(),
                })
            })
            .clone()
    }

    pub fn pending_checks(&self) -> &HashSet<MarketKey> {
        &self.pending
    }

    /// Hints are requests for evidence, never desired-state mutations.
    pub fn request_verification(&mut self, key: MarketKey) {
        self.pending.insert(key);
    }

    fn publish_change(&mut self) {
        self.cached = None;
        self.changed.send_replace(());
    }

    fn uncertain(&mut self, key: &MarketKey, reason: &str) {
        self.pending.insert(key.clone());
        if let Some(entry) = self.markets.get_mut(key)
            && entry.uncertainty.as_deref() != Some(reason)
        {
            entry.uncertainty = Some(reason.to_owned());
            self.publish_change();
        }
    }

    pub fn apply_listing(&mut self, version: Version, bytes: &[u8]) -> Result<PageReport> {
        let page: Page = serde_json::from_slice(bytes).context("invalid market page")?;
        ensure!(
            page.next_cursor.as_ref().is_none_or(|c| !c.is_empty()),
            "empty cursor"
        );
        let mut report = PageReport {
            records: page.markets.len(),
            unresolved: 0,
            observed: HashSet::new(),
            next_cursor: page.next_cursor,
        };
        for raw in &page.markets {
            if let Some(key) = identity(raw, version) {
                report.observed.insert(key);
            }
            if !self.apply_record(version, raw, false) {
                report.unresolved += 1;
            }
        }
        Ok(report)
    }

    /// Only pass a fully accumulated targeted response. Missing/duplicate
    /// records do not provide evidence for retirement or reactivation.
    pub fn apply_verification(
        &mut self,
        version: Version,
        ids: &[String],
        bytes: &[u8],
    ) -> Result<()> {
        let page: Page = serde_json::from_slice(bytes).context("invalid verification page")?;
        ensure!(
            page.next_cursor.is_none(),
            "verification pagination not finished"
        );
        self.verify_records(version, ids, &page.markets);
        Ok(())
    }

    pub(crate) fn verify_records(
        &mut self,
        version: Version,
        ids: &[String],
        records: &[Box<RawValue>],
    ) {
        let mut matched: HashMap<String, Vec<&RawValue>> = HashMap::new();
        for raw in records {
            if let Some(key) = identity(raw, version) {
                matched.entry(key.id).or_default().push(raw);
            }
        }
        for id in ids {
            let key = MarketKey {
                version,
                id: id.clone(),
            };
            let Some(rows) = matched.get(id) else {
                self.uncertain(&key, "missing targeted result");
                continue;
            };
            // Compare only normalized relevant fields, not incidental metadata.
            let first = normalize(rows[0], version);
            let consistent = first.as_ref().is_ok_and(|a| {
                rows[1..]
                    .iter()
                    .all(|raw| normalize(raw, version).as_ref().is_ok_and(|b| a == b))
            });
            if !consistent {
                self.uncertain(&key, "malformed or contradictory targeted result");
                continue;
            }
            if self.apply_record(version, rows[0], true) {
                self.pending.remove(&key);
            }
        }
    }

    fn apply_record(&mut self, version: Version, raw: &RawValue, verified: bool) -> bool {
        let (key, outcomes, eligibility) = match normalize(raw, version) {
            Ok(m) => m,
            Err(error) => {
                let mut affected: HashSet<_> = identity(raw, version).into_iter().collect();
                // Error path only: a damaged market ID can still refer to known
                // assets. Flag those owners without inventing a new identity.
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(raw.get()) {
                    let ids = match version {
                        Version::V1 => value
                            .get("clobTokenIds")
                            .and_then(|v| v.as_str())
                            .and_then(|v| serde_json::from_str::<Vec<String>>(v).ok()),
                        Version::V2 => value
                            .get("positionIds")
                            .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok()),
                    };
                    for id in ids.into_iter().flatten() {
                        if let Some(owner) = self.asset_owner.get(&(version, id)) {
                            affected.insert(owner.clone());
                        }
                    }
                }
                for key in affected {
                    self.uncertain(&key, &format!("{error:#}"));
                }
                return false;
            }
        };
        let mut conflicts = HashSet::new();
        if let Some(old) = self.markets.get(&key)
            && old
                .outcomes
                .iter()
                .zip(&outcomes)
                .any(|(a, b)| a.asset_id != b.asset_id)
        {
            conflicts.insert(key.clone());
        }
        for outcome in &outcomes {
            if let Some(owner) = self.asset_owner.get(&(version, outcome.asset_id.clone()))
                && owner != &key
            {
                conflicts.insert(owner.clone());
            }
        }
        if !conflicts.is_empty() {
            conflicts.insert(key);
            for affected in conflicts {
                self.uncertain(&affected, "identity conflict");
            }
            return false;
        }
        let existing = self.markets.get(&key);
        match eligibility {
            Eligibility::Unavailable => {
                self.uncertain(&key, "market not currently active/book-enabled");
                return false;
            }
            Eligibility::Closed if existing.is_none() => return true,
            Eligibility::Closed if !verified => {
                self.uncertain(&key, "closure requires targeted verification");
                return false;
            }
            Eligibility::Eligible if !verified && existing.is_some_and(|e| !e.desired) => {
                self.uncertain(&key, "reactivation requires targeted verification");
                return false;
            }
            // A listing cannot erase an unresolved conflict/status concern.
            Eligibility::Eligible
                if !verified && existing.is_some_and(|e| e.uncertainty.is_some()) =>
            {
                return false;
            }
            _ => {}
        }
        let next = MarketEntry {
            outcomes,
            desired: eligibility == Eligibility::Eligible,
            uncertainty: None,
        };
        if existing == Some(&next) {
            return true;
        }
        // No await between checking both outcomes and committing the market.
        for outcome in &next.outcomes {
            self.asset_owner
                .insert((version, outcome.asset_id.clone()), key.clone());
        }
        self.markets.insert(key, next);
        self.publish_change();
        true
    }
}
