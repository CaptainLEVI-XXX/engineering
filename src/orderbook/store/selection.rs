//! Choosing which replica feeds output, and switching with a snapshot boundary.
use super::*;

impl Processor {
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
    pub(super) fn eligible(&self, replica: &Replica) -> bool {
        replica.state == Validity::Live
            && !replica.suspect
            && self
                .sources
                .get(&replica.binding.session.worker)
                .is_some_and(|s| {
                    s.active && s.session == replica.binding.session && !s.recovery_requested
                })
    }
    pub(super) fn recent(&self, replica: &Replica) -> bool {
        self.ingress_age <= Duration::from_secs(1)
            && replica
                .last_valid_receive
                .is_some_and(|t| self.now.saturating_sub(t) <= Duration::from_secs(10))
    }
    pub(super) fn switch(
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
}
