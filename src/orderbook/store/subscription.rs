//! Per-session subscription bindings and requested-identity resolution.
use super::*;

impl Processor {
    pub(super) fn subscription(
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
    pub(super) fn resolve(&self, session: Session, asset: &str) -> Result<Option<Binding>> {
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
}
