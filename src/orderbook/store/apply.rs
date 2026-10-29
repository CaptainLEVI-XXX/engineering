//! Decoded exchange events applied to source replicas (C3 -> C4).
use super::*;
use crate::market_data::{Change, Price};
use std::borrow::Cow;

impl Processor {
    pub(super) fn decode_message(
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
                    // Bounded decoding rejected the event before materializing
                    // it. That is exchange-input degradation, not lost accepted
                    // input: invalidate its scope instead of the whole process.
                    let cause = if error.resource {
                        InvalidationCause::ResourceLimit
                    } else {
                        InvalidationCause::InvalidData
                    };
                    self.stats.rejected += 1;
                    self.invalidate(session, error.assets.as_deref(), &error.reason, cause, sink)?;
                }
            }
            index += 1;
            Ok(())
        });
        match result {
            Ok(()) => Ok(()),
            Err(VisitError::Sink(error)) => Err(error),
            Err(VisitError::Envelope(error)) => {
                let reason = error.to_string();
                let cause = if error.is::<market_data::ResourceLimit>() {
                    InvalidationCause::ResourceLimit
                } else {
                    InvalidationCause::InvalidData
                };
                self.stats.rejected += 1;
                self.invalidate(session, None, &reason, cause, sink)
            }
        }
    }
    pub(super) fn event(
        &mut self,
        session: Session,
        ordinal: u64,
        index: usize,
        received_at: DateTime<Utc>,
        event: Event<'_>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        // Every output carries this event's position in its source session.
        let at = |exchange_us| Metadata {
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
            Event::NonBook => {
                self.stats.ignored += 1;
                Ok(())
            }
            Event::Lifecycle {
                market_id, adapter, ..
            } => {
                self.record_lifecycle_hint(market_id, adapter);
                Ok(())
            }
            Event::Snapshot {
                asset,
                exchange_us,
                bids,
                asks,
            } => self.apply_snapshot(at(exchange_us), asset, bids, asks, sink),
            Event::Changes {
                exchange_us,
                changes,
            } => self.apply_changes(at(exchange_us), changes, sink),
            Event::Tick { asset, tick, .. } => {
                self.apply_tick(session, &asset, tick);
                Ok(())
            }
        }
    }

    /// Lifecycle hints only schedule discovery work; they never touch books.
    fn record_lifecycle_hint(
        &mut self,
        market_id: Option<Cow<'_, str>>,
        adapter: Option<AdapterId>,
    ) {
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

    /// An exchange snapshot replaces the replica's book atomically, or degrades
    /// only that replica if it is invalid or exceeds the level budget.
    fn apply_snapshot(
        &mut self,
        at: Metadata,
        asset: Cow<'_, str>,
        bids: Vec<Level>,
        asks: Vec<Level>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let (session, received_at) = (at.cursor.session, at.received_at);
        let binding = match self.resolve(session, &asset) {
            Ok(Some(b)) => b,
            Ok(None) => {
                self.stats.ignored += 1;
                return Ok(());
            }
            Err(e) => {
                self.stats.rejected += 1;
                return self.invalidate(
                    session,
                    None,
                    &e.to_string(),
                    InvalidationCause::InvalidData,
                    sink,
                );
            }
        };
        if !(bids.len() <= self.config.levels_per_side
            && asks.len() <= self.config.levels_per_side
            && self
                .total_levels
                .checked_add(bids.len() + asks.len())
                .is_some_and(|n| n <= self.config.total_levels))
        {
            // The old book is already superseded by this snapshot, so
            // it cannot stay live; the replica waits for one that fits.
            self.stats.rejected += 1;
            return self.invalidate(
                session,
                Some(&[asset.into_owned()]),
                "snapshot construction budget exhausted",
                InvalidationCause::ResourceLimit,
                sink,
            );
        }
        let book = match Book::replacement(bids, asks) {
            Ok(book) => book,
            Err(error) => {
                self.stats.rejected += 1;
                return self.invalidate(
                    session,
                    Some(&[asset.into_owned()]),
                    &error.to_string(),
                    InvalidationCause::InvalidData,
                    sink,
                );
            }
        };
        let instrument = &mut self.books[binding.book.0 as usize];
        let replica = instrument.replicas.get_mut(&binding.session.slot).unwrap();
        self.total_levels = self.total_levels - replica.book.len() + book.len();
        self.stats.peak_levels = self.stats.peak_levels.max(self.total_levels);
        replica.book = book;
        if replica.state != Validity::Live {
            self.sources.get_mut(&session.worker).unwrap().initialized += 1;
        }
        replica.state = Validity::Live;
        replica.last = Some(at);
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
        Ok(())
    }

    /// Absolute level changes: validate and budget the whole event first,
    /// then commit every entry, then emit the selected replicas' history.
    fn apply_changes(
        &mut self,
        at: Metadata,
        changes: Vec<Change<'_>>,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        let (session, received_at) = (at.cursor.session, at.received_at);
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
                    return self.invalidate(
                        session,
                        None,
                        &error.to_string(),
                        InvalidationCause::InvalidData,
                        sink,
                    );
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
                    let assets: Vec<_> = changes.iter().map(|c| c.asset.to_string()).collect();
                    return self.invalidate(
                        session,
                        Some(&assets),
                        &error.to_string(),
                        InvalidationCause::InvalidData,
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
            if *count > self.config.levels_per_side || total > self.config.total_levels {
                // Nothing in this event has been applied. Keep it
                // atomic: every book it names waits for a snapshot.
                self.stats.rejected += 1;
                let assets: Vec<_> = changes.iter().map(|c| c.asset.to_string()).collect();
                return self.invalidate(
                    session,
                    Some(&assets),
                    "incremental level budget exhausted",
                    InvalidationCause::ResourceLimit,
                    sink,
                );
            }
            self.quantities.insert(key, quantity);
            self.prepared.push(PreparedLevel {
                binding,
                side: change.side,
                level: Level {
                    price: change.price,
                    quantity,
                },
                entry,
            });
        }
        // All ordinary failures are resolved before this loop.
        for &PreparedLevel {
            binding,
            side,
            level,
            entry,
        } in &self.prepared
        {
            let replica = self.books[binding.book.0 as usize]
                .replicas
                .get_mut(&binding.session.slot)
                .unwrap();
            replica.book.set(side, level);
            replica.applied += 1;
            replica.last_valid_receive = Some(self.received_time);
            replica.health.observe_changes(self.received_time, 1)?;
            let mut metadata = at;
            metadata.cursor.entry = Some(entry);
            replica.last = Some(metadata);
        }
        self.total_levels = total;
        self.stats.peak_levels = self.stats.peak_levels.max(total);
        self.stats.changes += self.prepared.len() as u64;
        for PreparedLevel {
            binding,
            side,
            level,
            entry,
        } in self.prepared.drain(..)
        {
            let instrument = &self.books[binding.book.0 as usize];
            if instrument.selected == Some(binding)
                && (self.config.selection == SelectionPolicy::Manual
                    || (!self.sources[&session.worker].recovery_requested
                        && !instrument.replicas[&binding.session.slot].suspect))
            {
                let mut metadata = at;
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
        Ok(())
    }

    /// Tick size is replica metadata only; it does not change book levels.
    fn apply_tick(&mut self, session: Session, asset: &str, tick: Price) {
        if let Ok(Some(binding)) = self.resolve(session, asset) {
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
