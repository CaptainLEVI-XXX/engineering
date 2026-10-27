use super::*;
use crate::{
    market_data::{self, Event, Level},
    websocket::ingress::Message,
};
use anyhow::{Context, Result, ensure};
use std::collections::{BTreeMap, HashMap};

struct Instrument {
    asset: String,
    adapter: AdapterId,
    replicas: BTreeMap<u16, Replica>,
    selected: Option<Binding>,
}
struct Source {
    session: Session,
    ordinal: u64,
    active: bool,
    requested: HashMap<String, Binding>,
    recovery_requested: bool,
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
    stats: Stats,
    // Reused numeric preparation storage, each bounded by wire.entries.
    prepared: Vec<(Binding, Side, Level, usize)>,
    occupied: HashMap<(BookId, u16, Side, Price), bool>,
    counts: HashMap<(BookId, u16, Side), usize>,
}
impl Processor {
    pub fn new(config: Config) -> Result<Self> {
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
            stats: Stats::default(),
            prepared: Vec::new(),
            occupied: HashMap::new(),
            counts: HashMap::new(),
        })
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    pub fn replica(&self, adapter: AdapterId, asset: &str, slot: u16) -> Option<&Replica> {
        let id = self.by_adapter.get(&adapter)?.get(asset)?;
        self.books[id.0 as usize].replicas.get(&slot)
    }
    pub fn take_recovery(&mut self) -> Vec<Session> {
        std::mem::take(&mut self.pending)
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
            Message::Retired {
                worker,
                adapter,
                revision,
                assets,
            } => {
                let session = self.sources.get(worker).map_or(
                    Session {
                        worker: *worker,
                        generation: 0,
                        adapter: *adapter,
                    },
                    |s| s.session,
                );
                ensure!(session.adapter == *adapter, "retirement adapter mismatch");
                for asset in assets {
                    self.subscription(session, 0, *revision, asset, false, sink)?;
                }
            }
            Message::Started(session) => {
                if let Some(old) = self.sources.get(&session.worker) {
                    ensure!(
                        !old.active
                            && session.generation > old.session.generation
                            && session.adapter == old.session.adapter,
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
                        requested: HashMap::new(),
                        recovery_requested: false,
                    },
                );
            }
            Message::SubscriptionsRequested {
                session,
                source_slot,
                revisions,
                add,
                remove,
            } => {
                self.active(*session)?;
                ensure!(
                    revisions.len() == add.len() + remove.len(),
                    "subscription revision cardinality mismatch"
                );
                for (index, asset) in add.iter().chain(remove).enumerate() {
                    self.subscription(
                        *session,
                        *source_slot,
                        revisions[index],
                        asset,
                        index < add.len(),
                        sink,
                    )?;
                }
            }
            Message::Ended { session, reason } => {
                self.active(*session)?;
                self.invalidate(*session, None, reason, false, sink)?;
                let source = self.active(*session)?;
                source.active = false;
                source.requested.clear();
                self.pending.retain(|s| s != session);
            }
            Message::Data {
                session,
                ordinal,
                received_at,
                payload,
            } => {
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
                let events = match market_data::envelope(payload.as_str(), &self.config.wire) {
                    Ok(events) => events,
                    Err(error) => {
                        if error.to_string().starts_with("resource limit:") {
                            return Err(error);
                        }
                        self.stats.rejected += 1;
                        return self.invalidate(*session, None, &error.to_string(), true, sink);
                    }
                };
                for (index, raw) in events.into_iter().enumerate() {
                    self.stats.events += 1;
                    match market_data::decode(raw, &self.config.wire) {
                        Ok(event) => {
                            self.event(*session, *ordinal, index, *received_at, event, sink)?
                        }
                        Err(error) => {
                            if error.resource {
                                return Err(error.into());
                            }
                            self.stats.rejected += 1;
                            self.invalidate(
                                *session,
                                error.assets.as_deref(),
                                &error.reason,
                                true,
                                sink,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn subscription(
        &mut self,
        session: Session,
        slot: u16,
        revision: u64,
        asset: &str,
        add: bool,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        ensure!(
            !asset.is_empty() && asset.len() <= self.config.wire.identity_bytes,
            "invalid requested identity"
        );
        let id = match self
            .by_adapter
            .get(&session.adapter)
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
                    adapter: session.adapter,
                    replicas: BTreeMap::new(),
                    selected: None,
                });
                self.by_adapter
                    .entry(session.adapter)
                    .or_default()
                    .insert(asset.into(), id);
                id
            }
        };
        // Remove obsolete source-local references even when another worker has
        // already installed a newer binding for the same logical slot.
        if !add
            && let Some(source) = self.sources.get_mut(&session.worker)
            && source
                .requested
                .get(asset)
                .is_some_and(|b| b.slot == slot && b.revision <= revision)
        {
            source.requested.remove(asset);
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
            slot,
            revision,
            session,
        };
        let old = instrument.replicas.remove(&slot);
        let (since, attempts) = match old {
            Some(old) => {
                if add && old.state == Validity::Live {
                    self.stats.invalidations += 1;
                    sink(Observation {
                        asset: &instrument.asset,
                        adapter: instrument.adapter,
                        input: self.input,
                        binding: old.binding,
                        metadata: old.last,
                        selected_at: None,
                        kind: ObservationKind::Invalidated {
                            reason: "assignment replaced",
                        },
                    })?;
                }
                self.total_levels -= old.book.len();
                if old.binding.revision == revision {
                    (old.unavailable_since, old.attempts)
                } else {
                    (None, 0)
                }
            }
            None => (None, 0),
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
            unavailable_since: if add {
                Some(since.unwrap_or(self.now))
            } else {
                None
            },
            requested_at: self.now,
            attempts,
        };
        instrument.replicas.insert(slot, replica);
        if !add {
            if instrument.selected.is_some_and(|b| b.slot == slot) {
                instrument.selected = None;
            }
            sink(Observation {
                asset: &instrument.asset,
                adapter: instrument.adapter,
                input: self.input,
                binding,
                metadata: None,
                selected_at: None,
                kind: ObservationKind::Invalidated {
                    reason: "subscription removed",
                },
            })?;
        }
        if let Some(source) = self.sources.get_mut(&session.worker) {
            if add {
                source.requested.insert(asset.into(), binding);
            } else {
                source.requested.remove(asset);
            }
        }
        Ok(())
    }
    fn resolve(&self, session: Session, asset: &str) -> Result<Option<Binding>> {
        let Some(id) = self
            .by_adapter
            .get(&session.adapter)
            .and_then(|m| m.get(asset))
        else {
            anyhow::bail!("unknown book asset");
        };
        let Some(binding) = self.sources[&session.worker].requested.get(asset).copied() else {
            return Ok(None);
        };
        let replica = &self.books[id.0 as usize].replicas[&binding.slot];
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
        bindings.sort_by_key(|b| (b.book.0, b.slot));
        for binding in bindings {
            let instrument = &mut self.books[binding.book.0 as usize];
            let replica = instrument.replicas.get_mut(&binding.slot).unwrap();
            if replica.binding != binding || replica.state == Validity::Inactive {
                continue;
            }
            replica.unavailable_since.get_or_insert(self.now);
            if replica.state != Validity::Stale {
                replica.state = Validity::Stale;
                self.stats.invalidations += 1;
                sink(Observation {
                    asset: &instrument.asset,
                    adapter: instrument.adapter,
                    input: self.input,
                    binding,
                    metadata: replica.last,
                    selected_at: None,
                    kind: ObservationKind::Invalidated { reason },
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
                .get_mut(&binding.slot)
                .unwrap();
            if replica.binding == *binding && replica.unavailable_since.is_some() {
                ensure!(
                    replica.attempts < self.config.recovery_attempts,
                    "recovery attempts exhausted"
                );
                replica.attempts += 1;
            }
        }
        ensure!(
            self.pending.len() < self.config.sessions,
            "recovery queue budget exhausted"
        );
        source.recovery_requested = true;
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
            exchange_us: Some(exchange_us),
        };
        match event {
            Event::NonBook => self.stats.ignored += 1,
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
                let replica = instrument.replicas.get_mut(&binding.slot).unwrap();
                self.total_levels = self.total_levels - replica.book.len() + book.len();
                replica.book = book;
                replica.state = Validity::Live;
                replica.last = Some(meta(exchange_us));
                replica.unavailable_since = None;
                replica.attempts = 0;
                replica.observed += 1;
                replica.last_received_at = Some(received_at);
                self.stats.snapshots += 1;
                if instrument.selected.is_some_and(|b| b.slot == binding.slot)
                    || (instrument.selected.is_none() && binding.slot == 0)
                {
                    instrument.selected = Some(binding);
                    sink(Observation {
                        asset: &instrument.asset,
                        adapter: instrument.adapter,
                        input: self.input,
                        binding,
                        metadata: replica.last,
                        selected_at: None,
                        kind: ObservationKind::Snapshot(&replica.book),
                    })?;
                }
            }
            Event::Changes {
                exchange_us,
                changes,
            } => {
                self.prepared.clear();
                self.occupied.clear();
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
                        .get_mut(&binding.slot)
                        .unwrap();
                    replica.observed += 1;
                    replica.last_received_at = Some(received_at);
                    if replica.state != Validity::Live {
                        self.stats.ignored += 1;
                        continue;
                    }
                    let key = (binding.book, binding.slot, change.side, change.level.price);
                    let occupied = self.occupied.get(&key).copied().unwrap_or_else(|| {
                        replica
                            .book
                            .quantity(change.side, change.level.price)
                            .is_some()
                    });
                    let present = change.level.quantity.value() != 0;
                    let count = self
                        .counts
                        .entry((binding.book, binding.slot, change.side))
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
                    self.occupied.insert(key, present);
                    self.prepared
                        .push((binding, change.side, change.level, entry));
                }
                // All ordinary failures are resolved before this loop.
                for (binding, side, level, entry) in &self.prepared {
                    let replica = self.books[binding.book.0 as usize]
                        .replicas
                        .get_mut(&binding.slot)
                        .unwrap();
                    replica.book.set(*side, *level);
                    replica.applied += 1;
                    let mut metadata = meta(exchange_us);
                    metadata.cursor.entry = Some(*entry);
                    replica.last = Some(metadata);
                }
                self.total_levels = total;
                self.stats.changes += self.prepared.len() as u64;
                for (binding, side, level, entry) in self.prepared.drain(..) {
                    let instrument = &self.books[binding.book.0 as usize];
                    if instrument.selected == Some(binding) {
                        let mut metadata = meta(exchange_us);
                        metadata.cursor.entry = Some(entry);
                        sink(Observation {
                            asset: &instrument.asset,
                            adapter: instrument.adapter,
                            input: self.input,
                            binding,
                            metadata: Some(metadata),
                            selected_at: None,
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
                        .get_mut(&binding.slot)
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
        at: DateTime<Utc>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        ensure!(
            at.timestamp_micros() >= 0,
            "selection timestamp before epoch"
        );
        self.clock(self.now)?;
        let id = *self
            .by_adapter
            .get(&adapter)
            .and_then(|m| m.get(asset))
            .context("unknown instrument")?;
        let instrument = &mut self.books[id.0 as usize];
        let replica = instrument.replicas.get(&slot).context("unknown replica")?;
        ensure!(
            replica.state == Validity::Live,
            "selection requires live replica"
        );
        if instrument.selected == Some(replica.binding) {
            return Ok(());
        }
        instrument.selected = Some(replica.binding);
        let result = sink(Observation {
            asset: &instrument.asset,
            adapter,
            input: self.input,
            binding: replica.binding,
            metadata: replica.last,
            selected_at: Some(at),
            kind: ObservationKind::Snapshot(&replica.book),
        });
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn tick(&mut self, now: Duration) -> Result<()> {
        ensure!(!self.failed, "processor already failed");
        let result = (|| {
            self.clock(now)?;
            let mut requests = Vec::new();
            for instrument in &self.books {
                for replica in instrument.replicas.values() {
                    if let Some(since) = replica.unavailable_since {
                        ensure!(
                            now.saturating_sub(since) < self.config.recovery_timeout,
                            "book recovery deadline exhausted"
                        );
                        if now.saturating_sub(replica.requested_at)
                            >= self.config.initialization_timeout
                        {
                            requests.push(replica.binding.session);
                        }
                    }
                }
            }
            requests.sort_by_key(|s| (s.worker, s.generation));
            requests.dedup();
            for session in requests {
                self.request_recovery(session)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn finish(&self) -> Result<()> {
        ensure!(
            !self.failed,
            "processor failed; accepted tail may be incomplete"
        );
        ensure!(
            self.sources.values().all(|s| !s.active),
            "drained prefix but session boundary missing"
        );
        Ok(())
    }
}
