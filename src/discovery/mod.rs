//! Single-owner desired-state registry. This module makes no subscription claims.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::watch;
use tokio::time::Instant;

pub mod polymarket;

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum AdapterId {
    PolymarketV1,
    PolymarketV2,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MarketKey {
    pub adapter: AdapterId,
    pub id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Instrument {
    pub id: String,
    pub label: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketEntry {
    pub instruments: Vec<Instrument>,
    pub desired: bool,
    pub uncertainty: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub markets: HashMap<MarketKey, MarketEntry>,
}

impl Snapshot {
    pub fn desired_assets(&self) -> usize {
        self.markets
            .values()
            .filter(|m| m.desired)
            .map(|m| m.instruments.len())
            .sum()
    }
}

/// Adapter-normalized page. Wire parsing and eligibility policy live in adapters.
pub struct Page {
    pub markets: Vec<Result<MarketRecord, RecordError>>,
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub struct PageReport {
    pub records: usize,
    pub unresolved: usize,
    pub identity_conflicts: usize,
    pub observed: HashSet<MarketKey>,
    pub next_cursor: Option<String>,
}

/// Adapters supply nonempty, distinct native instrument IDs for eligible and
/// closed records. Unavailable records need no instruments and cannot retire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketRecord {
    pub id: String,
    pub instruments: Vec<Instrument>,
    pub eligibility: Eligibility,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordError {
    pub market_id: Option<String>,
    pub instrument_ids: Vec<String>,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Eligibility {
    Eligible,
    Closed,
    Unavailable,
}

pub struct PendingVerification {
    first_seen: Instant,
    next_attempt: Instant,
    attempts: u32,
}

const UNKNOWN_LIMIT: usize = 1024;
const UNKNOWN_LIFETIME: Duration = Duration::from_secs(300);

pub struct Registry {
    markets: HashMap<MarketKey, MarketEntry>,
    instrument_owner: HashMap<(AdapterId, String), MarketKey>,
    pending: HashMap<MarketKey, PendingVerification>,
    cached: Option<Arc<Snapshot>>,
    changed: watch::Sender<()>,
    dirty: bool,
    identity_conflicts: usize,
    expired_verifications: usize,
    verification_overflow: usize,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            markets: HashMap::new(),
            instrument_owner: HashMap::new(),
            pending: HashMap::new(),
            cached: None,
            changed: watch::channel(()).0,
            dirty: false,
            identity_conflicts: 0,
            expired_verifications: 0,
            verification_overflow: 0,
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

    pub fn pending_checks(&self) -> &HashMap<MarketKey, PendingVerification> {
        &self.pending
    }

    /// Hints are requests for evidence, never desired-state mutations.
    pub fn request_verification(&mut self, key: MarketKey) {
        if self.pending.contains_key(&key) {
            return; // Repeated observations must not reset lifetime or attempts.
        }
        if !self.markets.contains_key(&key)
            && self
                .pending
                .keys()
                .filter(|k| !self.markets.contains_key(*k))
                .count()
                >= UNKNOWN_LIMIT
        {
            self.verification_overflow += 1;
            return;
        }
        let now = Instant::now();
        self.pending.insert(
            key,
            PendingVerification {
                first_seen: now,
                next_attempt: now,
                attempts: 0,
            },
        );
    }

    /// Reserve one oldest-due, same-adapter batch. The transport bounds each
    /// round separately. Exhausted unknowns stay briefly to suppress duplicates.
    pub fn verification_batch(&mut self, now: Instant) -> Vec<MarketKey> {
        self.pending.retain(|key, job| {
            let keep = self.markets.contains_key(key) || now < job.first_seen + UNKNOWN_LIFETIME;
            if !keep {
                self.expired_verifications += 1;
            }
            keep
        });
        let mut due: Vec<_> = self
            .pending
            .iter()
            .filter(|(key, job)| {
                job.next_attempt <= now && (job.attempts < 3 || self.markets.contains_key(*key))
            })
            .map(|(key, job)| (job.next_attempt, key.clone()))
            .collect();
        due.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.adapter.cmp(&b.1.adapter))
                .then_with(|| a.1.id.cmp(&b.1.id))
        });
        let Some((_, first)) = due.first() else {
            return Vec::new();
        };
        let adapter = first.adapter;
        let keys: Vec<_> = due
            .into_iter()
            .filter(|(_, key)| key.adapter == adapter)
            .take(100)
            .map(|(_, key)| key)
            .collect();
        for key in &keys {
            let job = self.pending.get_mut(key).unwrap();
            job.attempts = job.attempts.saturating_add(1);
            job.next_attempt =
                now + Duration::from_secs((30 * (1 << (job.attempts - 1).min(4))).min(300));
        }
        keys
    }

    pub(crate) fn next_verification(&self) -> Option<Instant> {
        self.pending
            .iter()
            .map(|(key, job)| {
                if self.markets.contains_key(key) {
                    job.next_attempt
                } else if job.attempts >= 3 {
                    job.first_seen + UNKNOWN_LIFETIME
                } else {
                    job.next_attempt.min(job.first_seen + UNKNOWN_LIFETIME)
                }
            })
            .min()
    }

    fn publish_change(&mut self) {
        self.cached = None;
        self.dirty = true;
    }

    fn flush_changes(&mut self) {
        if self.dirty {
            self.dirty = false;
            self.changed.send_replace(());
        }
    }

    fn uncertain(&mut self, key: &MarketKey, reason: &str) {
        self.request_verification(key.clone());
        if let Some(entry) = self.markets.get_mut(key)
            && entry.uncertainty.as_deref() != Some(reason)
        {
            entry.uncertainty = Some(reason.to_owned());
            self.publish_change();
        }
    }

    pub fn apply_page(&mut self, adapter: AdapterId, page: Page) -> PageReport {
        let mut report = PageReport {
            records: page.markets.len(),
            unresolved: 0,
            identity_conflicts: 0,
            observed: HashSet::new(),
            next_cursor: page.next_cursor,
        };
        let conflicts_before = self.identity_conflicts;
        for record in page.markets {
            let id = match &record {
                Ok(market) => Some(&market.id),
                Err(error) => error.market_id.as_ref(),
            };
            if let Some(id) = id {
                report.observed.insert(MarketKey {
                    adapter,
                    id: id.clone(),
                });
            }
            if !self.apply_record(adapter, record, false) {
                report.unresolved += 1;
            }
        }
        report.identity_conflicts = self.identity_conflicts - conflicts_before;
        self.flush_changes();
        report
    }

    /// Only pass a fully accumulated targeted response. Missing/duplicate
    /// records do not provide evidence for retirement or reactivation.
    pub fn verify_records(
        &mut self,
        adapter: AdapterId,
        ids: &[String],
        records: Vec<Result<MarketRecord, RecordError>>,
    ) {
        let mut matched: HashMap<String, Vec<_>> = HashMap::new();
        for record in records {
            let id = match &record {
                Ok(market) => Some(&market.id),
                Err(error) => error.market_id.as_ref(),
            };
            if let Some(id) = id {
                matched.entry(id.clone()).or_default().push(record);
            }
        }
        for id in ids {
            let key = MarketKey {
                adapter,
                id: id.clone(),
            };
            let Some(rows) = matched.remove(id) else {
                self.uncertain(&key, "missing targeted result");
                continue;
            };
            // Compare only normalized relevant fields, not incidental metadata.
            let mut rows = rows.into_iter();
            let first = rows.next().unwrap();
            let consistent = first
                .as_ref()
                .is_ok_and(|a| rows.all(|record| record.as_ref().is_ok_and(|b| a == b)));
            if !consistent {
                self.uncertain(&key, "malformed or contradictory targeted result");
                continue;
            }
            if self.apply_record(adapter, first, true) {
                self.pending.remove(&key);
            }
        }
        self.flush_changes();
    }

    fn apply_record(
        &mut self,
        adapter: AdapterId,
        record: Result<MarketRecord, RecordError>,
        verified: bool,
    ) -> bool {
        let MarketRecord {
            id,
            instruments,
            eligibility,
        } = match record {
            Ok(m) => m,
            Err(error) => {
                let mut affected: HashSet<_> = error
                    .market_id
                    .map(|id| MarketKey { adapter, id })
                    .into_iter()
                    .collect();
                for id in error.instrument_ids {
                    if let Some(owner) = self.instrument_owner.get(&(adapter, id)) {
                        affected.insert(owner.clone());
                    }
                }
                for key in affected {
                    self.uncertain(&key, &error.reason);
                }
                return false;
            }
        };
        let key = MarketKey { adapter, id };
        if eligibility == Eligibility::Unavailable {
            if self.markets.contains_key(&key) {
                self.uncertain(&key, "market currently unavailable");
                return false;
            }
            self.pending.remove(&key);
            return true;
        }
        let mut conflicts = HashSet::new();
        if let Some(old) = self.markets.get(&key)
            && (old.instruments.len() != instruments.len()
                || old
                    .instruments
                    .iter()
                    .zip(&instruments)
                    .any(|(a, b)| a.id != b.id))
        {
            conflicts.insert(key.clone());
        }
        for instrument in &instruments {
            if let Some(owner) = self.instrument_owner.get(&(adapter, instrument.id.clone()))
                && owner != &key
            {
                conflicts.insert(owner.clone());
            }
        }
        if !conflicts.is_empty() {
            self.identity_conflicts += 1;
            conflicts.insert(key);
            for affected in conflicts {
                self.uncertain(&affected, "identity conflict");
            }
            return false;
        }
        let existing = self.markets.get(&key);
        match eligibility {
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
            instruments,
            desired: eligibility == Eligibility::Eligible,
            uncertainty: None,
        };
        if existing == Some(&next) {
            return true;
        }
        // No await between checking all instruments and committing the market.
        for instrument in &next.instruments {
            self.instrument_owner
                .insert((adapter, instrument.id.clone()), key.clone());
        }
        self.markets.insert(key, next);
        self.publish_change();
        true
    }
}
