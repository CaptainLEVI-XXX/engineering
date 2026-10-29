//! Reconciliation fences: admission and retirement of desired identities.
use super::*;

impl Processor {
    pub(super) fn instrument_id(&mut self, adapter: AdapterId, asset: &str) -> Result<BookId> {
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
                        replicas: Slots::default(),
                        selected: None,
                        snapshot_due: None,
                        health_checked: self.now,
                        next_check: Some(self.now),
                        reported: Contribution::default(),
                        reserved: None,
                    });
                    self.health_due.insert((self.now, id));
                    // Reserve the configured bound when an adapter first appears.
                    // On-demand growth rehashed every ~77-byte asset ID on the
                    // owner thread, and each doubling's stall doubled (53.6 ms at
                    // 458,752 IDs on an M2; 117 ms during a live ramp). Large
                    // `books` growth is a page remap, not a copy, so it is left
                    // to grow.
                    let capacity = self.config.instruments;
                    self.by_adapter
                        .entry(adapter)
                        .or_insert_with(|| HashMap::with_capacity(capacity))
                        .insert(asset.into(), id);
                    id
                }
            },
        )
    }
    pub(super) fn desired_chunk(
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
}
