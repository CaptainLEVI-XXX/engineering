//! Timers: health examination, rotation, periodic snapshots and repair deadlines.
use super::*;

impl Processor {
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
    pub(super) fn rotation_peer(&self, id: BookId, session: Session) -> Option<u16> {
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
    pub(super) fn rotate(
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
    pub(super) fn schedule_snapshot(&mut self, id: BookId, now: Duration) -> Result<()> {
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
    /// Earliest health deadline, including work left by a bounded tick slice.
    pub fn next_health_due(&self) -> Option<Duration> {
        self.health_due.first().map(|(due, _)| *due)
    }
    /// Stop generated work at intake cutoff, without backfilling from drained state.
    pub fn stop_snapshots(&mut self, now: Duration) {
        if self.config.snapshot_interval.take().is_some() {
            self.stats.snapshots_due_at_cutoff = self
                .snapshot_due
                .iter()
                .take_while(|(due, _)| *due <= now)
                .count();
            // Per-book `snapshot_due` fields are left stale on purpose: every
            // reader takes the field and removes that key from this set, a
            // no-op once cleared, and with no interval nothing reschedules.
            // Resetting them touched ~510k scattered ~1.3 KB instruments on
            // the owner thread at cutoff (~240 ms with cold caches).
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
                let spent = now
                    .checked_sub(start)
                    .context("nonmonotonic snapshot clock")?;
                if spent >= Duration::from_millis(1) || levels >= 65536 {
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
            // Bound each slice; the owner must revisit unfinished due work
            // sooner than its idle cadence, without starving ingress.
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
    pub(super) fn examine(&mut self, id: BookId, now: Duration) -> Result<()> {
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
        let old = instrument.reported;
        self.stats.selected_live =
            self.stats.selected_live - usize::from(old.selected_live) + usize::from(selected);
        self.stats.coverage_overdue =
            self.stats.coverage_overdue - usize::from(old.coverage_overdue) + usize::from(overdue);
        self.stats.degraded_replicas =
            self.stats.degraded_replicas - old.degraded_replicas + degraded;
        instrument.reported = Contribution {
            selected_live: selected,
            coverage_overdue: overdue,
            degraded_replicas: degraded,
        };
        self.stats.max_unavailable_age = self.stats.max_unavailable_age.max(age);
        self.stats.max_health_revisit = self.stats.max_health_revisit.max(
            now.checked_sub(instrument.health_checked)
                .context("health examination before previous check")?,
        );
        instrument.health_checked = now;
        self.stats.health_checks = self
            .stats
            .health_checks
            .checked_add(1)
            .context("health checks overflow")?;
        let mut requests = [None; 2];
        for (slot, replica) in instrument.replicas.iter_mut() {
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
                    requests[*slot as usize] = Some((replica.attempts, replica.binding.session));
                }
            }
        }
        // When both copies remain unavailable, serialization must not always
        // retry slot zero and starve its peer. Only granted repairs count.
        requests.sort_unstable_by_key(|r| r.map(|(attempts, s)| (attempts, s.slot)));
        for (_, session) in requests.into_iter().flatten() {
            self.request_recovery(session)?;
        }
        Ok(())
    }
}
