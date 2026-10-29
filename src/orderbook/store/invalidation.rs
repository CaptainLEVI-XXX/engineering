//! Scoped invalidation and bounded source recovery requests.
use super::*;

impl Processor {
    pub(super) fn invalidate(
        &mut self,
        session: Session,
        assets: Option<&[String]>,
        reason: &str,
        cause: InvalidationCause,
        sink: &mut impl FnMut(Observation<'_>) -> Result<()>,
    ) -> Result<()> {
        // A scoped budget rejection would recur on a fresh session, which
        // resends the same book. Release only the named books' levels and wait
        // for an exchange snapshot that fits. Unscoped failures still recover.
        let release = matches!(cause, InvalidationCause::ResourceLimit) && assets.is_some();
        let recover = match cause {
            InvalidationCause::SourceEnded => false,
            InvalidationCause::ResourceLimit => assets.is_none(),
            _ => true,
        };
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
            .map(|(id, binding)| {
                let named = assets.is_some_and(|ids| ids.contains(id));
                (*binding, release && named)
            })
            .collect();
        bindings.sort_by_key(|(b, _)| (b.book.0, b.session.slot));
        for (binding, release) in bindings {
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
                        cause,
                    },
                })?;
            }
            if release {
                // Stale levels are never published; only a later snapshot can
                // make this replica live again, so they would merely hold budget.
                let replica = self.books[binding.book.0 as usize]
                    .replicas
                    .get_mut(&binding.session.slot)
                    .unwrap();
                self.total_levels -= replica.book.len();
                replica.book = Book::default();
                self.stats.resource_quarantines += 1;
            }
        }
        if recover {
            self.request_recovery(session)?;
        }
        Ok(())
    }
    pub(super) fn request_recovery(&mut self, session: Session) -> Result<()> {
        let source = self
            .sources
            .get(&session.worker)
            .context("unknown recovery source")?;
        if !source.active || source.session != session || source.recovery_requested {
            return Ok(());
        }
        // A missing instrument can share a socket with useful books. Do not
        // sacrifice their last eligible redundant copy while its peer repairs.
        // Already-invalid sources and single-replica operation still recover.
        if source.requested.values().any(|binding| {
            let instrument = &self.books[binding.book.0 as usize];
            instrument.desired
                && instrument.replicas.len() > 1
                && instrument
                    .replicas
                    .get(&session.slot)
                    .is_some_and(|r| r.binding == *binding && self.eligible(r))
                && !instrument
                    .replicas
                    .values()
                    .any(|r| r.binding.session != session && self.eligible(r))
        }) {
            return Ok(());
        }
        let source = self.sources.get_mut(&session.worker).unwrap();
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
}
