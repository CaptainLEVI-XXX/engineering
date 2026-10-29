use super::*;
use crate::{
    market_data::{self, Event, Level, QuantityChange, SessionDecoder, VisitError},
    websocket::ingress::Message,
};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, BTreeSet, HashMap};

struct Instrument {
    asset: String,
    adapter: AdapterId,
    desired: bool,
    admission_revision: u64,
    unavailable_since: Option<Duration>,
    replicas: Slots,
    selected: Option<Binding>,
    snapshot_due: Option<Duration>,
    health_checked: Duration,
    next_check: Option<Duration>,
    reported: Contribution,
    reserved: Option<(Session, Duration)>,
}
struct Source {
    session: Session,
    ordinal: u64,
    active: bool,
    decoder: Option<market_data::Decoder>,
    requested: HashMap<String, Binding>,
    initialized: usize,
    recovery_requested: bool,
    repair_delivered: bool,
    repair_since: Option<Duration>,
    planned_repair: bool,
    requests_complete: bool,
    ready_reported: bool,
    renewal_due: Duration,
}
/// This instrument's last contribution to the aggregate coverage stats, so an
/// examination replaces it instead of recounting every instrument.
#[derive(Clone, Copy, Default)]
struct Contribution {
    selected_live: bool,
    coverage_overdue: bool,
    degraded_replicas: usize,
}
/// One validated change awaiting commit: target replica, new level and the
/// entry's position in the wire event (kept for output metadata).
#[derive(Clone, Copy)]
struct PreparedLevel {
    binding: Binding,
    side: Side,
    level: Level,
    entry: usize,
}
struct Rotation {
    session: Session,
    books: Vec<BookId>,
    cursor: usize,
}
pub struct Processor {
    config: Config,
    by_adapter: HashMap<AdapterId, HashMap<String, BookId>>,
    books: Vec<Instrument>,
    sources: BTreeMap<usize, Source>,
    // Active sources awaiting an accepted all-ready acknowledgment. Readiness
    // itself remains derived from Source, including recovery and request churn.
    initializing: BTreeSet<usize>,
    total_levels: usize,
    replicas: usize,
    pending: Vec<Session>,
    failed: bool,
    now: Duration,
    input: u64,
    reconciliation: Reconciliation,
    received_time: Duration,
    ingress_age: Duration,
    receive_origin: Option<std::time::Instant>,
    dirty: BTreeSet<BookId>,
    health_due: BTreeSet<(Duration, BookId)>,
    rotation: Option<Rotation>,
    rotation_cursor: usize,
    hints: std::collections::HashSet<crate::discovery::MarketKey>,
    reconcile_hint: bool,
    stats: Stats,
    snapshot_due: BTreeSet<(Duration, BookId)>,
    // Reused numeric preparation storage, each bounded by wire.entries.
    prepared: Vec<PreparedLevel>,
    quantities: HashMap<(BookId, u16, Side, Price), Quantity>,
    counts: HashMap<(BookId, u16, Side), usize>,
}
impl Processor {
    pub fn new(config: Config) -> Result<Self> {
        Self::with_clock(config, None)
    }
    /// Production deliveries and the owner clock must share this exact origin.
    /// `new` uses supplied logical process times for deterministic fixture replay.
    pub fn with_receive_clock(config: Config, origin: std::time::Instant) -> Result<Self> {
        Self::with_clock(config, Some(origin))
    }
    fn with_clock(config: Config, receive_origin: Option<std::time::Instant>) -> Result<Self> {
        ensure!(
            config.instruments > 0
                && config.instruments <= u32::MAX as usize
                && config.replicas > 0
                && config.sessions > 0,
            "invalid storage limits"
        );
        ensure!(
            config.levels_per_side > 0
                && config.levels_per_side <= u16::MAX as usize
                && config.total_levels > 0,
            "invalid level limits"
        );
        ensure!(
            config.wire.message_bytes > 0
                && config.wire.entries > 0
                && config.wire.events > 0
                && config.wire.identity_bytes > 0,
            "invalid wire limits"
        );
        ensure!(
            !config.initialization_timeout.is_zero()
                && config.recovery_timeout >= config.initialization_timeout
                && config.recovery_attempts > 0,
            "invalid recovery limits"
        );
        ensure!(
            config.snapshot_interval.is_none_or(|d| !d.is_zero()),
            "zero snapshot interval"
        );
        ensure!(
            !config.renewal_interval.is_zero()
                && std::time::Instant::now()
                    .checked_add(config.renewal_interval)
                    .is_some(),
            "invalid renewal interval"
        );
        Ok(Self {
            config,
            by_adapter: HashMap::new(),
            books: vec![],
            sources: BTreeMap::new(),
            initializing: BTreeSet::new(),
            total_levels: 0,
            replicas: 0,
            pending: vec![],
            failed: false,
            now: Duration::ZERO,
            input: 0,
            reconciliation: Reconciliation::default(),
            received_time: Duration::ZERO,
            ingress_age: Duration::ZERO,
            receive_origin,
            dirty: BTreeSet::new(),
            health_due: BTreeSet::new(),
            rotation: None,
            rotation_cursor: 0,
            hints: std::collections::HashSet::new(),
            reconcile_hint: false,
            stats: Stats::default(),
            snapshot_due: BTreeSet::new(),
            prepared: Vec::new(),
            quantities: HashMap::new(),
            counts: HashMap::new(),
        })
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    pub fn reconciliation(&self) -> Reconciliation {
        self.reconciliation
    }
    pub fn input_cutoff(&self) -> u64 {
        self.input
    }
    pub fn pending_hint(&self) -> Option<&crate::discovery::MarketKey> {
        self.hints.iter().next()
    }
    pub fn hint_delivered(&mut self, key: &crate::discovery::MarketKey) {
        self.hints.remove(key);
    }
    pub fn reconciliation_hint(&self) -> bool {
        self.reconcile_hint
    }
    pub fn reconciliation_hint_delivered(&mut self) {
        self.reconcile_hint = false;
    }
    /// Successful requests in active sessions, including superseded bindings
    /// still awaiting removal. These are observed writes, not acknowledgements.
    pub fn requested(&self) -> impl Iterator<Item = (&str, Binding)> {
        self.sources.values().filter(|s| s.active).flat_map(|s| {
            s.requested
                .iter()
                .map(|(asset, binding)| (asset.as_str(), *binding))
        })
    }
    /// Stream exact replica identities without allocating a second registry or
    /// cloning levels. One immutable borrow fixes the owner cutoff for all rows.
    pub fn coverage(&self) -> impl Iterator<Item = CoverageRecord<'_>> {
        self.books.iter().flat_map(|instrument| {
            instrument.replicas.values().map(|replica| CoverageRecord {
                asset: &instrument.asset,
                binding: replica.binding,
                state: replica.state,
                selected: instrument.selected == Some(replica.binding),
                levels: replica.book.len(),
                initialization_overdue: replica.state == Validity::AwaitingSnapshot
                    && self.now.saturating_sub(replica.requested_at)
                        >= self.config.initialization_timeout,
                unavailable_since: replica.unavailable_since,
                input_cutoff: self.input,
                owner_time: self.now,
            })
        })
    }
    pub fn replica(&self, adapter: AdapterId, asset: &str, slot: u16) -> Option<&Replica> {
        let id = self.by_adapter.get(&adapter)?.get(asset)?;
        self.books[id.0 as usize].replicas.get(&slot)
    }
    pub fn take_recovery(&mut self) -> Vec<Session> {
        let pending = std::mem::take(&mut self.pending);
        for session in &pending {
            self.recovery_delivered(*session);
        }
        pending
    }
    pub fn pending_recovery(&self) -> Option<(Session, bool)> {
        self.pending
            .first()
            .map(|s| (*s, self.sources[&s.worker].planned_repair))
    }
    pub fn recovery_delivered(&mut self, session: Session) {
        self.pending.retain(|s| *s != session);
        if let Some(source) = self
            .sources
            .get_mut(&session.worker)
            .filter(|s| s.session == session)
        {
            source.repair_delivered = true;
        }
    }
    pub fn initialized_sessions(&self) -> impl Iterator<Item = Session> + '_ {
        self.initializing
            .iter()
            .map(|worker| &self.sources[worker])
            .filter(|s| {
                s.active
                    && s.requests_complete
                    && !s.ready_reported
                    && !s.recovery_requested
                    && s.initialized == s.requested.len()
            })
            .map(|s| s.session)
    }
    pub fn initialization_delivered(&mut self, session: Session) {
        if let Some(source) = self
            .sources
            .get_mut(&session.worker)
            .filter(|s| s.session == session)
        {
            source.ready_reported = true;
            self.initializing.remove(&session.worker);
            for binding in source.requested.values() {
                let instrument = &mut self.books[binding.book.0 as usize];
                if instrument
                    .reserved
                    .is_some_and(|(s, _)| s.worker == session.worker)
                {
                    instrument.reserved = None;
                }
            }
        }
    }
    fn clock(&mut self, now: Duration) -> Result<()> {
        ensure!(now >= self.now, "nonmonotonic processor time");
        self.input = self
            .input
            .checked_add(1)
            .context("input cursor exhausted")?;
        self.now = now;
        Ok(())
    }
    pub fn process(
        &mut self,
        message: &Message,
        now: Duration,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        let result = self
            .clock(now)
            .and_then(|()| self.process_inner(message, sink));
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn active(&mut self, session: Session) -> Result<&mut Source> {
        let source = self
            .sources
            .get_mut(&session.worker)
            .context("source before Started")?;
        ensure!(
            source.active && source.session == session,
            "superseded or inactive transport session"
        );
        Ok(source)
    }
    fn process_inner(
        &mut self,
        message: &Message,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        match message {
            Message::ReconcileBegin { revision, chunks } => {
                ensure!(
                    (*revision > self.reconciliation.revision)
                        && (self.reconciliation.revision == 0 || self.reconciliation.complete),
                    "invalid reconciliation begin"
                );
                self.reconciliation = Reconciliation {
                    revision: *revision,
                    chunks: *chunks,
                    accepted_chunks: 0,
                    complete: false,
                    input_cutoff: self.input,
                };
            }
            Message::DesiredChunk {
                adapter,
                revision,
                index,
                add,
                remove,
            } => {
                ensure!(
                    !self.reconciliation.complete
                        && *revision == self.reconciliation.revision
                        && *index == self.reconciliation.accepted_chunks
                        && *index < self.reconciliation.chunks,
                    "unordered desired chunk"
                );
                self.desired_chunk(*adapter, *revision, add, remove, sink)?;
                self.reconciliation.accepted_chunks += 1;
                self.reconciliation.input_cutoff = self.input;
            }
            Message::ReconcileComplete { revision } => {
                ensure!(
                    *revision != 0
                        && *revision == self.reconciliation.revision
                        && !self.reconciliation.complete
                        && self.reconciliation.accepted_chunks == self.reconciliation.chunks,
                    "incomplete or duplicate reconciliation completion"
                );
                self.reconciliation.complete = true;
                self.reconciliation.input_cutoff = self.input;
            }
            Message::Started(session) => {
                ensure!(session.slot < 2, "unsupported replica slot");
                if let Some(old) = self.sources.get(&session.worker) {
                    ensure!(
                        !old.active
                            && session.generation > old.session.generation
                            && session.adapter == old.session.adapter
                            && session.slot == old.session.slot,
                        "invalid session start order"
                    );
                } else {
                    ensure!(
                        self.sources.len() < self.config.sessions,
                        "session budget exhausted"
                    );
                }
                self.sources.insert(
                    session.worker,
                    Source {
                        session: *session,
                        ordinal: 0,
                        active: true,
                        decoder: Some(market_data::Decoder::new(session.adapter)),
                        requested: HashMap::new(),
                        initialized: 0,
                        recovery_requested: false,
                        repair_delivered: false,
                        repair_since: None,
                        planned_repair: false,
                        requests_complete: false,
                        ready_reported: false,
                        renewal_due: self.now
                            + self.config.renewal_interval
                            + Duration::from_secs(session.worker as u64 % 301),
                    },
                );
                self.initializing.insert(session.worker);
            }
            Message::SubscriptionsRequested {
                session,
                revisions,
                add,
                remove,
            } => {
                self.active(*session)?;
                self.active(*session)?.requests_complete = false;
                self.active(*session)?.ready_reported = false;
                self.initializing.insert(session.worker);
                ensure!(
                    revisions.len() == add.len() + remove.len(),
                    "subscription revision cardinality mismatch"
                );
                for (index, asset) in add.iter().chain(remove).enumerate() {
                    self.subscription(*session, revisions[index], asset, index < add.len(), sink)?;
                }
            }
            Message::Ended { session, reason } => {
                self.active(*session)?;
                self.invalidate(*session, None, reason, InvalidationCause::SourceEnded, sink)?;
                let source = self.active(*session)?;
                source.active = false;
                source.decoder = None;
                source.requested.clear();
                source.initialized = 0;
                self.initializing.remove(&session.worker);
                self.pending.retain(|s| s != session);
            }
            Message::RequestsComplete { session } => {
                self.active(*session)?.requests_complete = true;
            }
            Message::Data {
                session,
                ordinal,
                received_at,
                received_mono,
                payload,
            } => {
                self.received_time = self.receive_origin.map_or(self.now, |origin| {
                    received_mono.saturating_duration_since(origin)
                });
                self.ingress_age = self.now.saturating_sub(self.received_time);
                self.stats.max_ingress_age = self.stats.max_ingress_age.max(self.ingress_age);
                let source = self.active(*session)?;
                ensure!(
                    Some(*ordinal) == source.ordinal.checked_add(1),
                    "unordered ingress ordinal"
                );
                source.ordinal = *ordinal;
                ensure!(
                    received_at.timestamp_micros() >= 0,
                    "receive timestamp before epoch"
                );
                self.stats.messages += 1;
                // Temporarily detach session-local parsing state, keeping the
                // processing owner exclusively mutable without a queue or lock.
                let mut decoder = self
                    .active(*session)?
                    .decoder
                    .take()
                    .context("missing session decoder")?;
                let result = self.decode_message(
                    &mut decoder,
                    *session,
                    *ordinal,
                    *received_at,
                    payload.as_str(),
                    sink,
                );
                self.active(*session)?.decoder = Some(decoder);
                result?;
            }
        }
        Ok(())
    }
    pub fn finish(&self) -> Result<()> {
        ensure!(
            !self.failed,
            "processor failed; accepted tail may be incomplete"
        );
        ensure!(
            self.reconciliation.revision == 0 || self.reconciliation.complete,
            "incomplete reconciliation; accepted desired cutoffs retained"
        );
        ensure!(
            self.sources.values().all(|s| !s.active),
            "drained prefix but session boundary missing"
        );
        Ok(())
    }
}

mod slots;
use slots::Slots;
mod apply;
mod desired;
mod invalidation;
mod schedule;
mod selection;
mod subscription;

#[cfg(test)]
mod conformance;
#[cfg(test)]
mod policy_tests;
