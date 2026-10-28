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
    replicas: BTreeMap<u16, Replica>,
    selected: Option<Binding>,
    snapshot_due: Option<Duration>,
    health_checked: Duration,
    next_check: Option<Duration>,
    reported: (bool, bool, usize),
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
    prepared: Vec<(Binding, Side, Level, usize)>,
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
        self.sources
            .values()
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
                self.invalidate(*session, None, reason, false, sink)?;
                let source = self.active(*session)?;
                source.active = false;
                source.decoder = None;
                source.requested.clear();
                source.initialized = 0;
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
    fn decode_message(
        &mut self,
        decoder: &mut impl SessionDecoder,
        session: Session,
        ordinal: u64,
        received_at: DateTime<Utc>,
        payload: &str,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let limits = self.config.wire.clone();
        let mut index = 0;
        let result = decoder.visit(payload, &limits, &mut |event| {
            self.stats.events += 1;
            match event {
                Ok(event) => self.event(session, ordinal, index, received_at, event, sink)?,
                Err(error) => {
                    if error.resource {
                        return Err(error.into());
                    }
                    self.stats.rejected += 1;
                    self.invalidate(session, error.assets.as_deref(), &error.reason, true, sink)?;
                }
            }
            index += 1;
            Ok(())
        });
        match result {
            Ok(()) => Ok(()),
            Err(VisitError::Sink(error)) => Err(error),
            Err(VisitError::Envelope(error)) => {
                if error.to_string().starts_with("resource limit:") {
                    return Err(error);
                }
                self.stats.rejected += 1;
                self.invalidate(session, None, &error.to_string(), true, sink)
            }
        }
    }
    fn instrument_id(&mut self, adapter: AdapterId, asset: &str) -> Result<BookId> {
        ensure!(
            !asset.is_empty() && asset.len() <= self.config.wire.identity_bytes,
            "invalid instrument identity"
        );
        Ok(
            match self
                .by_adapter
                .get(&adapter)
                .and_then(|m| m.get(asset))
                .copied()
            {
                Some(id) => id,
                None => {
                    ensure!(
                        self.books.len() < self.config.instruments,
                        "instrument budget exhausted"
                    );
                    let id = BookId(u32::try_from(self.books.len())?);
                    self.books.push(Instrument {
                        asset: asset.into(),
                        adapter,
                        desired: false,
                        admission_revision: 0,
                        unavailable_since: Some(self.now),
                        replicas: BTreeMap::new(),
                        selected: None,
                        snapshot_due: None,
                        health_checked: self.now,
                        next_check: Some(self.now),
                        reported: (false, false, 0),
                        reserved: None,
                    });
                    self.health_due.insert((self.now, id));
                    self.by_adapter
                        .entry(adapter)
                        .or_default()
                        .insert(asset.into(), id);
                    id
                }
            },
        )
    }
    fn desired_chunk(
        &mut self,
        adapter: AdapterId,
        revision: u64,
        add: &[String],
        remove: &[String],
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let mut unique = BTreeSet::new();
        let mut new = 0usize;
        for asset in add.iter().chain(remove) {
            ensure!(
                !asset.is_empty()
                    && asset.len() <= self.config.wire.identity_bytes
                    && unique.insert(asset),
                "invalid or duplicate desired identity"
            );
            if let Some(id) = self.by_adapter.get(&adapter).and_then(|m| m.get(asset)) {
                ensure!(
                    self.books[id.0 as usize].admission_revision < revision,
                    "duplicate desired cutoff"
                );
                ensure!(
                    !add.contains(asset) || !self.books[id.0 as usize].desired,
                    "duplicate desired admission"
                );
            } else {
                new += 1;
            }
        }
        ensure!(
            self.books
                .len()
                .checked_add(new)
                .is_some_and(|n| n <= self.config.instruments),
            "desired identity budget exhausted"
        );
        for asset in add {
            let id = self.instrument_id(adapter, asset)?;
            let instrument = &mut self.books[id.0 as usize];
            ensure!(!instrument.desired, "duplicate desired admission");
            instrument.desired = true;
            self.stats.desired_books += 1;
            if instrument.next_check.is_none() {
                self.health_due.insert((self.now, id));
                instrument.next_check = Some(self.now);
            }
            instrument.admission_revision = revision;
            instrument.unavailable_since = Some(self.now);
        }
        for asset in remove {
            let id = self.instrument_id(adapter, asset)?;
            let instrument = &mut self.books[id.0 as usize];
            self.stats.desired_books -= usize::from(instrument.desired);
            instrument.desired = false;
            instrument.admission_revision = revision;
            instrument.unavailable_since = None;
            instrument.reserved = None;
            self.dirty.remove(&id);
            let selected = instrument.selected.take();
            if let Some(due) = instrument.snapshot_due.take() {
                self.snapshot_due.remove(&(due, id));
            }
            for replica in instrument.replicas.values_mut() {
                let output_binding = selected.filter(|b| *b == replica.binding);
                if replica.state != Validity::Inactive {
                    sink(Observation {
                        asset: &instrument.asset,
                        adapter,
                        input: self.input,
                        binding: replica.binding,
                        metadata: replica.last,
                        kind: ObservationKind::Invalidated {
                            reason: "global retirement",
                            cause: InvalidationCause::Retired,
                            output_binding,
                        },
                    })?;
                }
                if let Some(source) = self.sources.get_mut(&replica.binding.session.worker)
                    && source.requested.get(asset) == Some(&replica.binding)
                {
                    source.initialized -= usize::from(replica.state == Validity::Live);
                    source.requested.remove(asset);
                    if source.requested.is_empty() {
                        source.recovery_requested = false;
                        source.repair_since = None;
                    }
                }
                replica.state = Validity::Inactive;
                replica.unavailable_since = None;
                self.total_levels -= replica.book.len();
                replica.book = Book::default();
            }
            // Superseded requests may still be queued on a former owner. They
            // remain observed writes, but cannot survive the global fence.
            for source in self
                .sources
                .values_mut()
                .filter(|s| s.session.adapter == adapter)
            {
                if source
                    .requested
                    .get(asset)
                    .is_some_and(|b| b.revision <= revision)
                {
                    source.requested.remove(asset);
                }
            }
        }
        self.pending.retain(|s| {
            self.sources
                .get(&s.worker)
                .is_some_and(|source| !source.requested.is_empty())
        });
        Ok(())
    }
    fn subscription(
        &mut self,
        session: Session,
        revision: u64,
        asset: &str,
        add: bool,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let slot = session.slot;
        let id = self.instrument_id(session.adapter, asset)?;
        // Direct fixture/manual consumers may run without a manager. Once a
        // Begin is observed, only ordered desired cutoffs can authorize admission.
        if self.reconciliation.revision == 0 && add {
            self.stats.desired_books += usize::from(!self.books[id.0 as usize].desired);
            self.books[id.0 as usize].desired = true;
        }
        let instrument = &self.books[id.0 as usize];
        ensure!(
            self.reconciliation.revision == 0 || revision <= self.reconciliation.revision,
            "subscription request ahead of desired fence"
        );
        if add
            && self.reconciliation.revision != 0
            && (!instrument.desired || revision < instrument.admission_revision)
        {
            self.stats.ignored += 1;
            return Ok(());
        }
        ensure!(
            !add || self.reconciliation.revision == 0 || revision == instrument.admission_revision,
            "subscription revision does not match admission"
        );
        if !add
            && self.reconciliation.revision != 0
            && (!instrument.desired || revision < instrument.admission_revision)
        {
            // The global fence already retired this ownership. A late physical
            // unsubscribe cannot create another replica or availability boundary.
            self.stats.ignored += 1;
            return Ok(());
        }
        // Remove obsolete source-local references even when another worker has
        // already installed a newer binding for the same logical slot.
        let was_initialized = self.sources[&session.worker]
            .requested
            .get(asset)
            .is_some_and(|b| {
                instrument
                    .replicas
                    .get(&slot)
                    .is_some_and(|r| r.binding == *b && r.state == Validity::Live)
            });
        if !add
            && let Some(source) = self.sources.get_mut(&session.worker)
            && source
                .requested
                .get(asset)
                .is_some_and(|b| b.session.slot == slot && b.revision <= revision)
        {
            source.requested.remove(asset);
            source.initialized -= usize::from(was_initialized);
        }
        let instrument = &mut self.books[id.0 as usize];
        if let Some(old) = instrument.replicas.get(&slot) {
            if revision < old.binding.revision {
                self.stats.ignored += 1;
                return Ok(());
            }
            if revision == old.binding.revision {
                ensure!(
                    old.binding.session.worker == session.worker,
                    "conflicting assignment ownership"
                );
                if !add && old.state == Validity::Inactive {
                    return Ok(());
                }
                if old.binding.session == session && add && old.state != Validity::Inactive {
                    return Ok(());
                }
                ensure!(
                    !add || old.state != Validity::Inactive,
                    "readmission requires newer assignment revision"
                );
            }
        } else {
            ensure!(
                self.replicas < self.config.replicas,
                "replica budget exhausted"
            );
            self.replicas += 1;
        }
        let binding = Binding {
            book: id,
            revision,
            session,
        };
        let output_binding = instrument.selected.filter(|b| b.session.slot == slot);
        if output_binding.is_some()
            && let Some(due) = instrument.snapshot_due.take()
        {
            self.snapshot_due.remove(&(due, id));
        }
        let old = instrument.replicas.remove(&slot);
        let initial_retry = self.now + self.config.initialization_timeout;
        let initial_cooldown = Duration::from_secs(60);
        let (since, attempts, next_retry, cooldown) = match old {
            Some(old) => {
                if old.state == Validity::Live
                    && old.binding.session.worker != session.worker
                    && let Some(source) = self.sources.get_mut(&old.binding.session.worker)
                    && source.requested.get(asset) == Some(&old.binding)
                {
                    source.initialized -= 1;
                }
                if add && old.state == Validity::Live {
                    self.stats.invalidations += 1;
                    sink(Observation {
                        asset: &instrument.asset,
                        adapter: instrument.adapter,
                        input: self.input,
                        binding: old.binding,
                        metadata: old.last,
                        kind: ObservationKind::Invalidated {
                            reason: "assignment replaced",
                            cause: InvalidationCause::AssignmentReplaced,
                            output_binding,
                        },
                    })?;
                }
                self.total_levels -= old.book.len();
                if old.binding.revision == revision {
                    (
                        old.unavailable_since,
                        old.attempts,
                        old.next_retry.max(initial_retry),
                        old.cooldown,
                    )
                } else {
                    (None, 0, initial_retry, initial_cooldown)
                }
            }
            None => (None, 0, initial_retry, initial_cooldown),
        };
        let replica = Replica {
            binding,
            state: if add {
                Validity::AwaitingSnapshot
            } else {
                Validity::Inactive
            },
            book: Book::default(),
            last: None,
            tick: None,
            applied: 0,
            observed: 0,
            last_received_at: None,
            last_valid_receive: None,
            suspect: false,
            suspicion_reported: false,
            health: health::Health::new(Duration::ZERO),
            unavailable_since: if add {
                Some(since.unwrap_or(self.now))
            } else {
                None
            },
            requested_at: self.now,
            attempts,
            next_retry,
            cooldown,
        };
        instrument.replicas.insert(slot, replica);
        for peer in instrument.replicas.values_mut() {
            peer.health.clear_confirmation();
        }
        if !add {
            if instrument.selected.is_some_and(|b| b.session.slot == slot) {
                instrument.selected = None;
            }
            sink(Observation {
                asset: &instrument.asset,
                adapter: instrument.adapter,
                input: self.input,
                binding,
                metadata: None,
                kind: ObservationKind::Invalidated {
                    reason: "subscription removed",
                    cause: InvalidationCause::Retired,
                    output_binding,
                },
            })?;
        }
        if let Some(source) = self.sources.get_mut(&session.worker) {
            if add {
                source.initialized -= usize::from(was_initialized);
                source.requested.insert(asset.into(), binding);
            } else {
                source.requested.remove(asset);
            }
        }
        Ok(())
    }
    fn resolve(&self, session: Session, asset: &str) -> Result<Option<Binding>> {
        let Some(binding) = self.sources[&session.worker].requested.get(asset).copied() else {
            // A known but unrequested identity is ignored; an unknown one still
            // invalidates this source. Keep that distinction on the miss path.
            ensure!(
                self.by_adapter
                    .get(&session.adapter)
                    .is_some_and(|m| m.contains_key(asset)),
                "unknown book asset"
            );
            return Ok(None);
        };
        let replica = &self.books[binding.book.0 as usize].replicas[&binding.session.slot];
        Ok((replica.binding == binding && replica.state != Validity::Inactive).then_some(binding))
    }
    fn invalidate(
        &mut self,
        session: Session,
        assets: Option<&[String]>,
        reason: &str,
        recover: bool,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let source = &self.sources[&session.worker];
        let unknown = assets.is_some_and(|ids| {
            ids.iter().any(|id| {
                !self
                    .by_adapter
                    .get(&session.adapter)
                    .is_some_and(|m| m.contains_key(id))
            })
        });
        let mut bindings: Vec<_> = source
            .requested
            .iter()
            .filter(|(id, _)| unknown || assets.is_none_or(|ids| ids.contains(id)))
            .map(|(_, binding)| *binding)
            .collect();
        bindings.sort_by_key(|b| (b.book.0, b.session.slot));
        for binding in bindings {
            let instrument = &mut self.books[binding.book.0 as usize];
            let replica = instrument.replicas.get_mut(&binding.session.slot).unwrap();
            if replica.binding != binding || replica.state == Validity::Inactive {
                continue;
            }
            replica.unavailable_since.get_or_insert(self.now);
            if replica.state != Validity::Stale {
                self.sources.get_mut(&session.worker).unwrap().initialized -=
                    usize::from(replica.state == Validity::Live);
                replica.state = Validity::Stale;
                let output_binding = instrument.selected.filter(|b| *b == binding);
                if output_binding.is_some() {
                    instrument.unavailable_since.get_or_insert(self.now);
                }
                self.dirty.insert(binding.book);
                if output_binding.is_some()
                    && let Some(due) = instrument.snapshot_due.take()
                {
                    self.snapshot_due.remove(&(due, binding.book));
                }
                self.stats.invalidations += 1;
                sink(Observation {
                    asset: &instrument.asset,
                    adapter: instrument.adapter,
                    input: self.input,
                    binding,
                    metadata: replica.last,
                    kind: ObservationKind::Invalidated {
                        reason,
                        output_binding,
                        cause: if recover {
                            InvalidationCause::InvalidData
                        } else {
                            InvalidationCause::SourceEnded
                        },
                    },
                })?;
            }
        }
        if recover {
            self.request_recovery(session)?;
        }
        Ok(())
    }
    fn request_recovery(&mut self, session: Session) -> Result<()> {
        let source = self
            .sources
            .get_mut(&session.worker)
            .context("unknown recovery source")?;
        if !source.active || source.session != session || source.recovery_requested {
            return Ok(());
        }
        for binding in source.requested.values() {
            let replica = self.books[binding.book.0 as usize]
                .replicas
                .get_mut(&binding.session.slot)
                .unwrap();
            if replica.binding == *binding && replica.unavailable_since.is_some() {
                replica.attempts = replica
                    .attempts
                    .checked_add(1)
                    .context("recovery attempts overflow")?;
            }
            self.dirty.insert(binding.book);
        }
        ensure!(
            self.pending.len() < self.config.sessions,
            "recovery queue budget exhausted"
        );
        source.recovery_requested = true;
        source.repair_since = Some(self.now);
        source.repair_delivered = false;
        self.pending.push(session);
        Ok(())
    }
    fn event(
        &mut self,
        session: Session,
        ordinal: u64,
        index: usize,
        received_at: DateTime<Utc>,
        event: Event<'_>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let meta = |exchange_us| Metadata {
            cursor: Cursor {
                session,
                ordinal,
                event: index,
                entry: None,
            },
            received_at,
            exchange_us,
        };
        match event {
            Event::NonBook => self.stats.ignored += 1,
            Event::Lifecycle {
                market_id, adapter, ..
            } => {
                if let Some(id) = market_id {
                    for namespace in [AdapterId::PolymarketV1, AdapterId::PolymarketV2] {
                        if adapter.is_some_and(|a| a != namespace) {
                            continue;
                        }
                        let key = crate::discovery::MarketKey {
                            adapter: namespace,
                            id: id.to_string(),
                        };
                        if self.hints.len() < 1024 || self.hints.contains(&key) {
                            self.hints.insert(key);
                        } else {
                            self.stats.lifecycle_hints_dropped += 1;
                            self.reconcile_hint = true;
                        }
                    }
                } else {
                    self.stats.lifecycle_hints_dropped += 1;
                    self.reconcile_hint = true;
                }
            }
            Event::Snapshot {
                asset,
                exchange_us,
                bids,
                asks,
            } => {
                let binding = match self.resolve(session, &asset) {
                    Ok(Some(b)) => b,
                    Ok(None) => {
                        self.stats.ignored += 1;
                        return Ok(());
                    }
                    Err(e) => {
                        self.stats.rejected += 1;
                        return self.invalidate(session, None, &e.to_string(), true, sink);
                    }
                };
                ensure!(
                    bids.len() <= self.config.levels_per_side
                        && asks.len() <= self.config.levels_per_side
                        && self
                            .total_levels
                            .checked_add(bids.len() + asks.len())
                            .is_some_and(|n| n <= self.config.total_levels),
                    "snapshot construction budget exhausted"
                );
                let book = match Book::replacement(bids, asks) {
                    Ok(book) => book,
                    Err(error) => {
                        self.stats.rejected += 1;
                        return self.invalidate(
                            session,
                            Some(&[asset.into_owned()]),
                            &error.to_string(),
                            true,
                            sink,
                        );
                    }
                };
                let instrument = &mut self.books[binding.book.0 as usize];
                let replica = instrument.replicas.get_mut(&binding.session.slot).unwrap();
                self.total_levels = self.total_levels - replica.book.len() + book.len();
                replica.book = book;
                if replica.state != Validity::Live {
                    self.sources.get_mut(&session.worker).unwrap().initialized += 1;
                }
                replica.state = Validity::Live;
                replica.last = Some(meta(exchange_us));
                if self.config.selection == SelectionPolicy::Manual
                    || (!self.sources[&session.worker].recovery_requested && !replica.suspect)
                {
                    replica.unavailable_since = None;
                    replica.attempts = 0;
                }
                replica.observed += 1;
                replica.last_received_at = Some(received_at);
                replica.last_valid_receive = Some(self.received_time);
                replica.health.observe_snapshot(self.received_time)?;
                self.stats.snapshots += 1;
                self.dirty.insert(binding.book);
                let selectable = self.config.selection == SelectionPolicy::Manual
                    || !self.sources[&session.worker].recovery_requested;
                if selectable
                    && (instrument
                        .selected
                        .is_some_and(|b| b.session.slot == binding.session.slot)
                        || (instrument.selected.is_none()
                            && (binding.session.slot == 0
                                || self.config.selection == SelectionPolicy::Automatic)))
                {
                    instrument.selected = Some(binding);
                    instrument.unavailable_since = None;
                    sink(Observation {
                        asset: &instrument.asset,
                        adapter: instrument.adapter,
                        input: self.input,
                        binding,
                        metadata: replica.last,
                        kind: ObservationKind::Snapshot {
                            book: &replica.book,
                            origin: SnapshotOrigin::Exchange,
                        },
                    })?;
                    self.schedule_snapshot(binding.book, self.now)?;
                } else {
                    sink(Observation {
                        asset: &instrument.asset,
                        adapter: instrument.adapter,
                        input: self.input,
                        binding,
                        metadata: replica.last,
                        kind: ObservationKind::StandbyBaseline,
                    })?;
                }
            }
            Event::Changes {
                exchange_us,
                changes,
            } => {
                self.prepared.clear();
                self.quantities.clear();
                self.counts.clear();
                let mut total = self.total_levels;
                for (entry, change) in changes.iter().enumerate() {
                    let binding = match self.resolve(session, &change.asset) {
                        Ok(Some(b)) => b,
                        Ok(None) => {
                            self.stats.ignored += 1;
                            continue;
                        }
                        Err(error) => {
                            self.stats.rejected += 1;
                            return self.invalidate(session, None, &error.to_string(), true, sink);
                        }
                    };
                    let replica = self.books[binding.book.0 as usize]
                        .replicas
                        .get_mut(&binding.session.slot)
                        .unwrap();
                    replica.observed += 1;
                    replica.last_received_at = Some(received_at);
                    if replica.state != Validity::Live {
                        self.stats.ignored += 1;
                        continue;
                    }
                    let key = (
                        binding.book,
                        binding.session.slot,
                        change.side,
                        change.price,
                    );
                    let previous = self
                        .quantities
                        .get(&key)
                        .copied()
                        .or_else(|| replica.book.quantity(change.side, change.price))
                        .map_or(0, Quantity::value);
                    let quantity = match change.quantity {
                        QuantityChange::Set(quantity) => Ok(quantity),
                        QuantityChange::Add(delta) => previous
                            .checked_add(delta)
                            .context("relative quantity overflow")
                            .and_then(Quantity::new),
                    };
                    let quantity = match quantity {
                        Ok(quantity) => quantity,
                        Err(error) => {
                            self.stats.rejected += 1;
                            let assets: Vec<_> =
                                changes.iter().map(|c| c.asset.to_string()).collect();
                            return self.invalidate(
                                session,
                                Some(&assets),
                                &error.to_string(),
                                true,
                                sink,
                            );
                        }
                    };
                    let occupied = previous != 0;
                    let present = quantity.value() != 0;
                    let count = self
                        .counts
                        .entry((binding.book, binding.session.slot, change.side))
                        .or_insert(replica.book.side_len(change.side));
                    if occupied != present {
                        if present {
                            *count += 1;
                            total += 1;
                        } else {
                            *count -= 1;
                            total -= 1;
                        }
                    }
                    ensure!(
                        *count <= self.config.levels_per_side && total <= self.config.total_levels,
                        "incremental level budget exhausted"
                    );
                    self.quantities.insert(key, quantity);
                    self.prepared.push((
                        binding,
                        change.side,
                        Level {
                            price: change.price,
                            quantity,
                        },
                        entry,
                    ));
                }
                // All ordinary failures are resolved before this loop.
                for (binding, side, level, entry) in &self.prepared {
                    let replica = self.books[binding.book.0 as usize]
                        .replicas
                        .get_mut(&binding.session.slot)
                        .unwrap();
                    replica.book.set(*side, *level);
                    replica.applied += 1;
                    replica.last_valid_receive = Some(self.received_time);
                    replica.health.observe_changes(self.received_time, 1)?;
                    let mut metadata = meta(exchange_us);
                    metadata.cursor.entry = Some(*entry);
                    replica.last = Some(metadata);
                }
                self.total_levels = total;
                self.stats.changes += self.prepared.len() as u64;
                for (binding, side, level, entry) in self.prepared.drain(..) {
                    let instrument = &self.books[binding.book.0 as usize];
                    if instrument.selected == Some(binding)
                        && (self.config.selection == SelectionPolicy::Manual
                            || (!self.sources[&session.worker].recovery_requested
                                && !instrument.replicas[&binding.session.slot].suspect))
                    {
                        let mut metadata = meta(exchange_us);
                        metadata.cursor.entry = Some(entry);
                        sink(Observation {
                            asset: &instrument.asset,
                            adapter: instrument.adapter,
                            input: self.input,
                            binding,
                            metadata: Some(metadata),
                            kind: ObservationKind::Level {
                                side,
                                price: level.price,
                                quantity: level.quantity,
                            },
                        })?;
                        self.stats.selected_changes += 1;
                    }
                }
            }
            Event::Tick { asset, tick, .. } => {
                if let Ok(Some(binding)) = self.resolve(session, &asset) {
                    self.books[binding.book.0 as usize]
                        .replicas
                        .get_mut(&binding.session.slot)
                        .unwrap()
                        .tick = Some(tick);
                } else {
                    self.stats.ignored += 1;
                }
            }
        }
        Ok(())
    }
    pub fn select(
        &mut self,
        adapter: AdapterId,
        asset: &str,
        slot: u16,
        now: Duration,
        at: DateTime<Utc>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        ensure!(
            at.timestamp_micros() >= 0,
            "selection timestamp before epoch"
        );
        self.clock(now)?;
        let id = *self
            .by_adapter
            .get(&adapter)
            .and_then(|m| m.get(asset))
            .context("unknown instrument")?;
        self.switch(id, slot, at, SwitchReason::Manual, sink)
    }
    fn eligible(&self, replica: &Replica) -> bool {
        replica.state == Validity::Live
            && !replica.suspect
            && self
                .sources
                .get(&replica.binding.session.worker)
                .is_some_and(|s| {
                    s.active && s.session == replica.binding.session && !s.recovery_requested
                })
    }
    fn recent(&self, replica: &Replica) -> bool {
        self.ingress_age <= Duration::from_secs(1)
            && replica
                .last_valid_receive
                .is_some_and(|t| self.now.saturating_sub(t) <= Duration::from_secs(10))
    }
    fn switch(
        &mut self,
        id: BookId,
        slot: u16,
        at: DateTime<Utc>,
        reason: SwitchReason,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let candidate = self.books[id.0 as usize]
            .replicas
            .get(&slot)
            .context("unknown replica")?;
        ensure!(
            self.books[id.0 as usize].desired && candidate.state == Validity::Live,
            "selection requires desired live replica"
        );
        if self.config.selection == SelectionPolicy::Automatic {
            ensure!(
                self.eligible(candidate),
                "selection candidate became ineligible"
            );
        }
        let freshness = if self.recent(candidate) {
            Freshness::RecentProgress
        } else {
            Freshness::Unknown
        };
        let receive_age = candidate
            .last_valid_receive
            .map(|t| self.now.saturating_sub(t));
        let instrument = &mut self.books[id.0 as usize];
        let replica = instrument.replicas.get(&slot).context("unknown replica")?;
        if instrument.selected == Some(replica.binding) {
            return Ok(());
        }
        let previous = instrument.selected;
        instrument.selected = Some(replica.binding);
        instrument.unavailable_since = None;
        let result = sink(Observation {
            asset: &instrument.asset,
            adapter: instrument.adapter,
            input: self.input,
            binding: replica.binding,
            metadata: replica.last,
            kind: ObservationKind::Snapshot {
                book: &replica.book,
                origin: SnapshotOrigin::Selection {
                    at,
                    previous,
                    reason,
                    freshness,
                    receive_age,
                },
            },
        })
        .and_then(|()| self.schedule_snapshot(id, self.now));
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    /// Policy work happens between complete inputs, never inside a delta batch.
    pub fn maintain(
        &mut self,
        now: Duration,
        at: DateTime<Utc>,
        queue_empty: bool,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        self.clock(now)?;
        if queue_empty {
            self.ingress_age = Duration::ZERO;
        }
        if self.config.selection == SelectionPolicy::Manual {
            return Ok(());
        }
        let began = std::time::Instant::now();
        let mut levels = 0;
        let result = (|| {
            for _ in 0..64 {
                if began.elapsed() >= Duration::from_millis(1) || levels >= 65_536 {
                    break;
                }
                let Some(id) = self.dirty.pop_first() else {
                    break;
                };
                let instrument = &mut self.books[id.0 as usize];
                for replica in instrument.replicas.values_mut() {
                    if replica.suspect && !replica.suspicion_reported {
                        let output_binding = instrument.selected.filter(|b| *b == replica.binding);
                        sink(Observation {
                            asset: &instrument.asset,
                            adapter: instrument.adapter,
                            input: self.input,
                            binding: replica.binding,
                            metadata: replica.last,
                            kind: ObservationKind::Invalidated {
                                reason: "L2 activity suspicion; fresh generation required",
                                cause: InvalidationCause::PolicySuspension,
                                output_binding,
                            },
                        })?;
                        replica.suspicion_reported = true;
                        if output_binding.is_some() {
                            instrument.unavailable_since.get_or_insert(now);
                            if let Some(due) = instrument.snapshot_due.take() {
                                self.snapshot_due.remove(&(due, id));
                            }
                        }
                    }
                }
                let instrument = &self.books[id.0 as usize];
                if !instrument.desired {
                    continue;
                }
                if instrument.selected.is_some_and(|b| {
                    let r = &instrument.replicas[&b.session.slot];
                    r.binding == b && self.eligible(r)
                }) {
                    continue;
                }
                let candidate = instrument
                    .replicas
                    .iter()
                    .filter(|(_, r)| self.eligible(r))
                    .min_by_key(|(slot, r)| (!self.recent(r), **slot))
                    .map(|(slot, r)| (*slot, r.book.len()));
                let reason = if instrument.replicas.values().any(|r| r.suspect) {
                    SwitchReason::HealthSuspicion
                } else {
                    SwitchReason::SourceUnavailable
                };
                if instrument.unavailable_since.is_none() {
                    if let Some(binding) = instrument.selected {
                        let replica = &instrument.replicas[&binding.session.slot];
                        sink(Observation {
                            asset: &instrument.asset,
                            adapter: instrument.adapter,
                            input: self.input,
                            binding,
                            metadata: replica.last,
                            kind: ObservationKind::Invalidated {
                                reason: "selected source suspended by recovery policy",
                                cause: InvalidationCause::PolicySuspension,
                                output_binding: Some(binding),
                            },
                        })?;
                    }
                    self.books[id.0 as usize].unavailable_since = Some(now);
                    if let Some(due) = self.books[id.0 as usize].snapshot_due.take() {
                        self.snapshot_due.remove(&(due, id));
                    }
                }
                if let Some((slot, count)) = candidate {
                    self.switch(id, slot, at, reason, sink)?;
                    levels += count;
                }
            }
            if self.config.monitoring
                && self.dirty.is_empty()
                && began.elapsed() < Duration::from_millis(1)
            {
                self.rotate(at, sink)?;
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    fn rotation_peer(&self, id: BookId, session: Session) -> Option<u16> {
        let instrument = &self.books[id.0 as usize];
        if !instrument.desired || instrument.reserved.is_some_and(|(s, _)| s != session) {
            return None;
        }
        let own = instrument.replicas.get(&session.slot)?;
        if own.binding.session != session || !self.eligible(own) {
            return None;
        }
        instrument
            .replicas
            .iter()
            .find(|(slot, peer)| {
                **slot != session.slot
                    && self.eligible(peer)
                    && (self.recent(peer)
                        || (own
                            .health
                            .aligned_window(self.now)
                            .ok()
                            .flatten()
                            .is_some_and(|w| w.changes == 0)
                            && peer
                                .health
                                .aligned_window(self.now)
                                .ok()
                                .flatten()
                                .is_some_and(|w| w.changes == 0)))
            })
            .map(|(slot, _)| *slot)
    }
    /// One bounded worker reservation at a time; captures remain sliced, while
    /// granted reservations survive until reinitialization or explicit expiry.
    fn rotate(
        &mut self,
        at: DateTime<Utc>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        if self.rotation.is_none() {
            let source = self
                .sources
                .range(self.rotation_cursor..)
                .next()
                .or_else(|| self.sources.first_key_value());
            let Some((&worker, source)) = source else {
                return Ok(());
            };
            self.rotation_cursor = worker.saturating_add(1);
            if !source.active
                || !source.ready_reported
                || source.recovery_requested
                || source.renewal_due > self.now
            {
                return Ok(());
            }
            let session = source.session;
            let mut books: Vec<_> = source.requested.values().map(|b| b.book).collect();
            books.sort_unstable();
            books.dedup();
            if books.is_empty() {
                return Ok(());
            }
            if books
                .iter()
                .any(|id| self.rotation_peer(*id, session).is_none())
            {
                self.stats.renewal_deferred += 1;
                return Ok(());
            }
            for id in &books {
                self.books[id.0 as usize].reserved =
                    Some((session, self.now + Duration::from_secs(30)));
            }
            self.rotation = Some(Rotation {
                session,
                books,
                cursor: 0,
            });
        }
        let mut job = self.rotation.take().unwrap();
        // Churn or a failed peer cancels the grant, not the already published
        // coherent snapshots. A later attempt starts from current ownership.
        let source = &self.sources[&job.session.worker];
        let valid = source.active
            && source.session == job.session
            && !source.recovery_requested
            && source.requested.len() == job.books.len()
            && job
                .books
                .iter()
                .all(|id| self.rotation_peer(*id, job.session).is_some());
        if !valid {
            for id in job.books {
                if self.books[id.0 as usize]
                    .reserved
                    .is_some_and(|(s, _)| s == job.session)
                {
                    self.books[id.0 as usize].reserved = None;
                }
            }
            self.stats.renewal_deferred += 1;
            return Ok(());
        }
        let start = std::time::Instant::now();
        let mut levels = 0;
        for _ in 0..64 {
            if job.cursor == job.books.len()
                || levels >= 65_536
                || start.elapsed() >= Duration::from_millis(1)
            {
                break;
            }
            let id = job.books[job.cursor];
            let slot = self
                .rotation_peer(id, job.session)
                .context("rotation peer lost")?;
            if self.books[id.0 as usize]
                .selected
                .is_some_and(|b| b.session == job.session)
            {
                levels += self.books[id.0 as usize].replicas[&slot].book.len();
                self.switch(id, slot, at, SwitchReason::PlannedRotation, sink)?;
            }
            job.cursor += 1;
        }
        if job.cursor == job.books.len() {
            self.sources
                .get_mut(&job.session.worker)
                .unwrap()
                .planned_repair = true;
            self.request_recovery(job.session)?;
            self.stats.renewals_granted += 1;
        } else {
            self.rotation = Some(job);
        }
        Ok(())
    }
    fn schedule_snapshot(&mut self, id: BookId, now: Duration) -> Result<()> {
        if let Some(interval) = self.config.snapshot_interval {
            let due = now
                .checked_add(interval)
                .context("snapshot deadline overflow")?;
            if let Some(old) = self.books[id.0 as usize].snapshot_due.replace(due) {
                self.snapshot_due.remove(&(old, id));
            }
            self.snapshot_due.insert((due, id));
        }
        Ok(())
    }
    pub fn next_snapshot_due(&self) -> Option<Duration> {
        self.snapshot_due.first().map(|(due, _)| *due)
    }
    /// Stop generated work at intake cutoff, without backfilling from drained state.
    pub fn stop_snapshots(&mut self, now: Duration) {
        if self.config.snapshot_interval.take().is_some() {
            self.stats.snapshots_due_at_cutoff = self
                .snapshot_due
                .iter()
                .take_while(|(due, _)| *due <= now)
                .count();
            for (_, id) in &self.snapshot_due {
                self.books[id.0 as usize].snapshot_due = None;
            }
            self.snapshot_due.clear();
        }
    }
    /// Coherent captures only between complete input messages. The clock is
    /// supplied explicitly so timer/control inputs can be replayed deterministically.
    pub fn capture_due(
        &mut self,
        clock: &mut impl FnMut() -> (Duration, DateTime<Utc>),
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        let result = (|| {
            let mut began = None;
            let mut levels = 0;
            for _ in 0..64 {
                let Some(&(due, id)) = self.snapshot_due.first() else {
                    break;
                };
                let (now, at) = clock();
                ensure!(now >= self.now, "nonmonotonic snapshot clock");
                if now < due {
                    break;
                }
                let start = *began.get_or_insert(now);
                if now - start >= Duration::from_millis(1) || levels >= 65536 {
                    break;
                }
                let late = now - due;
                ensure!(
                    late < self.config.snapshot_interval.unwrap(),
                    "whole snapshot interval missed"
                );
                ensure!(at.timestamp_micros() >= 0, "capture timestamp before epoch");
                self.clock(now)?;
                let instrument = &self.books[id.0 as usize];
                let binding = instrument.selected.context("scheduled book not selected")?;
                let replica = &instrument.replicas[&binding.session.slot];
                if self.config.selection == SelectionPolicy::Automatic && !self.eligible(replica) {
                    self.snapshot_due.remove(&(due, id));
                    self.books[id.0 as usize].snapshot_due = None;
                    continue;
                }
                ensure!(
                    replica.binding == binding && replica.state == Validity::Live,
                    "scheduled book not live"
                );
                levels += replica.book.len();
                sink(Observation {
                    asset: &instrument.asset,
                    adapter: instrument.adapter,
                    input: self.input,
                    binding,
                    metadata: replica.last,
                    kind: ObservationKind::Snapshot {
                        book: &replica.book,
                        origin: SnapshotOrigin::Periodic { at, due },
                    },
                })?;
                self.stats.periodic_snapshots += 1;
                self.stats.late_snapshots += u64::from(!late.is_zero());
                self.stats.max_snapshot_lateness = self.stats.max_snapshot_lateness.max(late);
                self.schedule_snapshot(id, now)?;
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    pub fn tick(&mut self, now: Duration) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        let result = (|| {
            self.clock(now)?;
            for session in &self.pending {
                let source = &self.sources[&session.worker];
                ensure!(
                    source
                        .repair_since
                        .is_none_or(|at| now.saturating_sub(at) < Duration::from_secs(30)),
                    "recovery delivery deadline expired"
                );
            }
            // Population-derived demand at 100ms tick cadence. The time slice
            // bounds interference, not a claim that this host meets that demand.
            let limit = self.books.len().div_ceil(100).max(64);
            let began = std::time::Instant::now();
            for _ in 0..limit {
                if began.elapsed() >= Duration::from_millis(1) {
                    break;
                }
                let Some(&(due, id)) = self.health_due.first() else {
                    break;
                };
                if due > now {
                    break;
                }
                self.stats.max_health_lateness = self.stats.max_health_lateness.max(now - due);
                self.health_due.pop_first();
                self.books[id.0 as usize].next_check = None;
                self.examine(id, now)?;
                if self.books[id.0 as usize].desired {
                    let next = now + Duration::from_secs(10);
                    self.health_due.insert((next, id));
                    self.books[id.0 as usize].next_check = Some(next);
                }
            }
            self.stats.health_pending = self.health_due.len();
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn examine(&mut self, id: BookId, now: Duration) -> Result<()> {
        let instrument = &self.books[id.0 as usize];
        let selected = instrument.desired
            && instrument.selected.is_some_and(|b| {
                instrument
                    .replicas
                    .get(&b.session.slot)
                    .is_some_and(|r| r.binding == b && self.eligible(r))
            });
        let degraded = instrument
            .replicas
            .values()
            .filter(|r| instrument.desired && !self.eligible(r))
            .count();
        // Clones are fixed-size window state, never books or financial levels.
        let peers = [0, 1].map(|slot| {
            instrument
                .replicas
                .get(&slot)
                .filter(|r| self.eligible(r))
                .map(|r| r.health.clone())
        });
        let instrument = &mut self.books[id.0 as usize];
        if instrument
            .reserved
            .is_some_and(|(_, deadline)| deadline <= now)
        {
            instrument.reserved = None;
        }
        let age = instrument
            .unavailable_since
            .map(|at| now.saturating_sub(at))
            .unwrap_or_default();
        let overdue = instrument.desired && !selected && age >= self.config.recovery_timeout;
        let (old_selected, old_overdue, old_degraded) = instrument.reported;
        self.stats.selected_live =
            self.stats.selected_live - usize::from(old_selected) + usize::from(selected);
        self.stats.coverage_overdue =
            self.stats.coverage_overdue - usize::from(old_overdue) + usize::from(overdue);
        self.stats.degraded_replicas = self.stats.degraded_replicas - old_degraded + degraded;
        instrument.reported = (selected, overdue, degraded);
        self.stats.max_unavailable_age = self.stats.max_unavailable_age.max(age);
        self.stats.max_health_revisit = self
            .stats
            .max_health_revisit
            .max(now - instrument.health_checked);
        instrument.health_checked = now;
        self.stats.health_checks = self
            .stats
            .health_checks
            .checked_add(1)
            .context("health checks overflow")?;
        let mut requests = [None; 2];
        for (slot, replica) in &mut instrument.replicas {
            if !instrument.desired || replica.state == Validity::Inactive {
                continue;
            }
            if self.config.monitoring && replica.state == Validity::Live && !replica.suspect {
                if self.ingress_age > Duration::from_secs(1) {
                    replica.health.clear_confirmation();
                } else {
                    let peer = if *slot < 2 {
                        peers[1 - *slot as usize].as_ref()
                    } else {
                        None
                    };
                    if matches!(
                        replica.health.evaluate(now, peer)?.assessment,
                        health::Assessment::Suspect(_)
                    ) {
                        self.stats.health_suspicions = self
                            .stats
                            .health_suspicions
                            .checked_add(1)
                            .context("health suspicion count overflow")?;
                        replica.suspect = true;
                        replica.unavailable_since.get_or_insert(now);
                        replica.next_retry = now;
                        self.dirty.insert(id);
                    }
                }
            }
            if let Some(since) = replica.unavailable_since
                && now >= replica.next_retry
            {
                let slow = now.saturating_sub(since) >= self.config.recovery_timeout
                    || replica.attempts >= self.config.recovery_attempts;
                let delay = if slow {
                    replica.cooldown
                } else {
                    self.config.initialization_timeout
                };
                replica.next_retry = now.checked_add(delay).context("retry deadline overflow")?;
                if slow {
                    replica.cooldown = (replica.cooldown * 2).min(Duration::from_secs(900));
                }
                if *slot < 2 {
                    requests[*slot as usize] = Some(replica.binding.session);
                }
            }
        }
        for session in requests.into_iter().flatten() {
            self.request_recovery(session)?;
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

#[cfg(test)]
#[path = "conformance.rs"]
mod conformance;
#[cfg(test)]
#[path = "policy_tests.rs"]
mod policy_tests;
