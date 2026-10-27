//! Owner-local activity evidence, not a book-validity or source-selection owner.
//!
//! All times are `Duration`s from ONE run-local monotonic origin, including both
//! replicas. Pass application receive time to observation, and owner evaluation
//! time to evaluation. No wall clock, exchange clock, timer, queue or allocation
//! is used here. An arrival is evidence of receipt, not proof of synchronization.
//!
//! Construct a new `Health` on every binding/generation change. Observe only
//! committed valid exchange snapshots and accepted price-change entries; retain
//! attempted/observed-input counters separately in the caller. Periodic/selection
//! snapshots, PONG, trades, lifecycle messages and rejected events MUST NOT call
//! observation. The first exchange snapshot starts the valid-window warmup.
//!
//! The owner supplies only a valid, initialized, same-instrument peer. `None`
//! means single-source mode or an unavailable peer, NOT a quiet peer. Suppress
//! evaluation while ingress age exceeds 1s and call `clear_confirmation` on that
//! pause or a peer-binding change. Hard invalidity and publishing ineligibility
//! remain the owner's responsibility. A suspicion must be latched there until
//! reinitialization; these recoverable heuristics do not latch book validity.

use anyhow::{Context, Result, ensure};
use std::time::Duration;

pub const WARMUP: Duration = Duration::from_secs(60);
pub const CONFIRMATION_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Default)]
struct Bucket {
    epoch: u64,
    changes: u64,
}

/// Three prior baseline minutes, the candidate completed minute, and the
/// in-progress minute. A single high-water epoch suffices because receipts are
/// ordered; advancing clears at most five slots even across arbitrarily long gaps.
#[derive(Clone, Copy, Debug)]
struct Minutes {
    epoch: u64,
    changes: [u64; 5],
}

impl Minutes {
    fn count(&self, epoch: u64) -> u64 {
        if epoch > self.epoch || self.epoch - epoch >= 5 {
            0
        } else {
            self.changes[(epoch % 5) as usize]
        }
    }

    fn advance(&mut self, epoch: u64) {
        let elapsed = epoch - self.epoch;
        if elapsed >= 5 {
            self.changes = [0; 5];
        } else {
            for offset in 1..=elapsed {
                self.changes[((self.epoch + offset) % 5) as usize] = 0;
            }
        }
        self.epoch = epoch;
    }
}

/// Six completed 10s buckets, `[start, end)`, never the in-progress bucket.
/// This aligned minute lags evaluation time by less than 10s.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivityWindow {
    pub start: Duration,
    pub end: Duration,
    pub changes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Suspicion {
    /// Zero changes versus at least five peer changes in the aligned window.
    RelativeSilence,
    /// At most 10% of a peer with at least twenty changes.
    RelativeLowRate,
    /// A complete minute below 10% of a frozen three-minute baseline mean.
    AbsoluteLowRate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Assessment {
    /// Not initialized, not yet 60s old, or no full aligned valid window yet.
    WarmingUp,
    /// A valid peer exists but does not yet have a comparable complete window.
    PeerWarmingUp,
    /// Both comparable peers are quiet, or a sole quiet source lacks evidence.
    QuietUnknown,
    /// No rate-based suspicion; this does NOT certify health or freshness.
    NoSuspicion,
    Pending(Suspicion),
    Suspect(Suspicion),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Evaluation {
    pub assessment: Assessment,
    pub own: Option<ActivityWindow>,
    pub peer: Option<ActivityWindow>,
}

/// Fixed-size per-binding state. No identity, book, heap allocation or scheduler.
#[derive(Clone, Debug)]
pub struct Health {
    // Initially binding creation; the first snapshot replaces it with valid time.
    since: Duration,
    last_valid: Option<Duration>,
    last_evaluation: Option<Duration>,
    relative_since: Option<Duration>,
    second: Bucket,
    buckets: [Bucket; 7],
    minutes: Minutes,
    snapshots: u64,
    changes: u64,
    // Zero means unfrozen; a qualified baseline sum is always >= 60.
    baseline_sum: u64,
    first_low_minute: u64,
    last_absolute_minute: Option<u64>,
}

impl Health {
    /// `binding_at` must not be later than the first possible receive timestamp.
    /// Use the receive-side binding cutoff, not the time a queued request happens
    /// to be processed. The first snapshot starts warmup independently of this.
    pub fn new(binding_at: Duration) -> Self {
        Self {
            since: binding_at,
            last_valid: None,
            last_evaluation: None,
            relative_since: None,
            second: Bucket::default(),
            buckets: [Bucket::default(); 7],
            minutes: Minutes {
                epoch: binding_at.as_secs() / 60,
                changes: [0; 5],
            },
            snapshots: 0,
            changes: 0,
            baseline_sum: 0,
            first_low_minute: 0,
            last_absolute_minute: None,
        }
    }

    pub fn snapshots(&self) -> u64 {
        self.snapshots
    }

    pub fn accepted_changes(&self) -> u64 {
        self.changes
    }

    pub fn last_valid_receive(&self) -> Option<Duration> {
        self.last_valid
    }

    /// Exact numerator of the frozen mean (denominator three), never rounded.
    pub fn frozen_baseline_sum(&self) -> Option<u64> {
        (self.baseline_sum != 0).then_some(self.baseline_sum)
    }

    pub fn observe_snapshot(&mut self, received: Duration) -> Result<()> {
        self.observe(received, 0, true)
    }

    /// `entries` is the number of accepted entries for THIS replica, not the
    /// message's total, unique prices, changed prices, or number of JSON events.
    /// Zero quantities, repeated prices and no-op absolute sets still count.
    pub fn observe_changes(&mut self, received: Duration, entries: u64) -> Result<()> {
        ensure!(self.snapshots != 0, "health changes before initialization");
        ensure!(entries != 0, "empty health change observation");
        self.observe(received, entries, false)
    }

    fn observe(&mut self, received: Duration, entries: u64, snapshot: bool) -> Result<()> {
        ensure!(
            received >= self.since,
            "health receipt before binding/baseline"
        );
        ensure!(
            self.last_valid.is_none_or(|last| received >= last),
            "nonmonotonic health receive time"
        );
        let second = received.as_secs();
        let epoch = second / 10;
        let index = (epoch % 7) as usize;
        let minute = second / 60;

        // Preflight all counters before changing ANY state, including clocks.
        let snapshots = self
            .snapshots
            .checked_add(u64::from(snapshot))
            .context("health snapshot counter exhausted")?;
        let changes = self
            .changes
            .checked_add(entries)
            .context("health accepted-change counter exhausted")?;
        let second_changes = if self.second.epoch == second {
            self.second.changes
        } else {
            0
        }
        .checked_add(entries)
        .context("health second counter exhausted")?;
        let bucket_changes = if self.buckets[index].epoch == epoch {
            self.buckets[index].changes
        } else {
            0
        }
        .checked_add(entries)
        .context("health bucket counter exhausted")?;
        let minute_changes = self
            .minutes
            .count(minute)
            .checked_add(entries)
            .context("health minute counter exhausted")?;

        // Evaluation may run ahead of queued receipts. Keep their real receive
        // epochs, never relabel them as current activity. They can correct a
        // previously evaluated window, so discard confirmation derived from it.
        if self.last_evaluation.is_some_and(|at| received < at) {
            self.clear_confirmation();
        }
        if self.snapshots == 0 {
            self.since = received;
        }
        self.snapshots = snapshots;
        self.changes = changes;
        self.last_valid = Some(received);
        self.second = Bucket {
            epoch: second,
            changes: second_changes,
        };
        self.buckets[index] = Bucket {
            epoch,
            changes: bucket_changes,
        };
        self.minutes.advance(minute);
        self.minutes.changes[(minute % 5) as usize] = minute_changes;
        Ok(())
    }

    fn check_now(&self, now: Duration) -> Result<()> {
        ensure!(
            now >= self.since,
            "health evaluation before binding/baseline"
        );
        ensure!(
            self.last_valid.is_none_or(|received| now >= received),
            "health evaluation before last receipt"
        );
        ensure!(
            self.last_evaluation.is_none_or(|last| now >= last),
            "nonmonotonic health evaluation time"
        );
        Ok(())
    }

    /// Accepted changes in the epoch containing `now`, not an exact sliding second.
    pub fn changes_in_second(&self, now: Duration) -> Result<u64> {
        self.check_now(now)?;
        Ok(if self.second.epoch == now.as_secs() {
            self.second.changes
        } else {
            0
        })
    }

    pub fn aligned_window(&self, now: Duration) -> Result<Option<ActivityWindow>> {
        self.check_now(now)?;
        let end_epoch = now.as_secs() / 10;
        if self.snapshots == 0 || now - self.since < WARMUP || end_epoch < 6 {
            return Ok(None);
        }
        let start = Duration::from_secs((end_epoch - 6) * 10);
        if start < self.since {
            return Ok(None);
        }
        let mut changes = 0_u64;
        for epoch in end_epoch - 6..end_epoch {
            let bucket = self.buckets[(epoch % 7) as usize];
            if bucket.epoch == epoch {
                changes = changes
                    .checked_add(bucket.changes)
                    .context("health aligned-window counter exhausted")?;
            }
        }
        Ok(Some(ActivityWindow {
            start,
            end: Duration::from_secs(end_epoch * 10),
            changes,
        }))
    }

    /// Clear soft confirmation without discarding accepted activity. Use when
    /// monitoring is paused by backlog or the identity/availability of a peer
    /// changes. Reinitialization requires constructing a new `Health` instead.
    pub fn clear_confirmation(&mut self) {
        self.relative_since = None;
        self.clear_absolute();
    }

    fn clear_absolute(&mut self) {
        self.baseline_sum = 0;
        self.first_low_minute = 0;
        self.last_absolute_minute = None;
    }

    /// Evaluate both replicas at the same owner cutoff. All six completed
    /// buckets must be valid for both; a partial initial bucket can extend the
    /// 60s warmup by less than 10s. Relative confirmation uses evaluation time.
    /// Absolute confirmation uses distinct adjacent completed calendar minutes
    /// of the run-local clock, not repeated evaluations of a rolling window.
    /// Missing a whole completed-minute evaluation clears absolute confirmation.
    pub fn evaluate(&mut self, now: Duration, valid_peer: Option<&Health>) -> Result<Evaluation> {
        let own = self.aligned_window(now)?;
        let peer = valid_peer
            .map(|health| health.aligned_window(now))
            .transpose()?
            .flatten();
        let assessment = if let Some(own_window) = own {
            if valid_peer.is_some() {
                self.clear_absolute();
                if let Some(peer_window) = peer {
                    self.relative(now, own_window.changes, peer_window.changes)
                } else {
                    self.relative_since = None;
                    Assessment::PeerWarmingUp
                }
            } else {
                self.relative_since = None;
                self.absolute(now, own_window.changes)?
            }
        } else {
            self.clear_confirmation();
            Assessment::WarmingUp
        };
        self.last_evaluation = Some(now);
        Ok(Evaluation {
            assessment,
            own,
            peer,
        })
    }

    fn relative(&mut self, now: Duration, own: u64, peer: u64) -> Assessment {
        let reason = if own == 0 && peer >= 5 {
            Some(Suspicion::RelativeSilence)
        } else if peer >= 20 && u128::from(own) * 10 <= u128::from(peer) {
            Some(Suspicion::RelativeLowRate)
        } else {
            None
        };
        if let Some(reason) = reason {
            let first = *self.relative_since.get_or_insert(now);
            if now - first >= CONFIRMATION_INTERVAL {
                Assessment::Suspect(reason)
            } else {
                Assessment::Pending(reason)
            }
        } else {
            self.relative_since = None;
            if own == 0 && peer == 0 {
                Assessment::QuietUnknown
            } else {
                Assessment::NoSuspicion
            }
        }
    }

    fn absolute(&mut self, now: Duration, window_changes: u64) -> Result<Assessment> {
        let fallback = if window_changes == 0 {
            Assessment::QuietUnknown
        } else {
            Assessment::NoSuspicion
        };
        let minute = now.as_secs() / 60 - 1; // A full aligned window already exists.
        // Do not count a partial first minute, nor invent confirmations over a
        // missed maintenance pass. Repeated evaluations of one minute are safe.
        if Duration::from_secs(minute * 60) < self.since {
            self.clear_absolute();
            return Ok(fallback);
        }
        if self
            .last_absolute_minute
            .is_some_and(|last| minute > last + 1)
        {
            self.clear_absolute();
        }
        self.last_absolute_minute = Some(minute);
        let current = self.minutes.count(minute);
        if self.baseline_sum == 0 {
            if minute < 3 || Duration::from_secs((minute - 3) * 60) < self.since {
                return Ok(fallback);
            }
            let mut sum = 0_u64;
            for prior in minute - 3..minute {
                let count = self.minutes.count(prior);
                if count < 20 {
                    return Ok(fallback);
                }
                sum = sum
                    .checked_add(count)
                    .context("health baseline counter exhausted")?;
            }
            // current < 10% of (sum / 3), compared without division/rounding.
            if u128::from(current) * 30 >= u128::from(sum) {
                return Ok(fallback);
            }
            self.baseline_sum = sum;
            self.first_low_minute = minute;
        } else if u128::from(current) * 30 >= u128::from(self.baseline_sum) {
            self.clear_absolute();
            return Ok(fallback);
        }
        Ok(if minute > self.first_low_minute {
            Assessment::Suspect(Suspicion::AbsoluteLowRate)
        } else {
            Assessment::Pending(Suspicion::AbsoluteLowRate)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    fn initialized() -> Health {
        let mut health = Health::new(Duration::ZERO);
        health.observe_snapshot(Duration::ZERO).unwrap();
        health
    }

    fn assessment(health: &mut Health, seconds: u64, peer: Option<&Health>) -> Assessment {
        health.evaluate(at(seconds), peer).unwrap().assessment
    }

    #[test]
    fn snapshots_are_not_changes_and_first_valid_snapshot_starts_warmup() {
        let mut health = Health::new(Duration::ZERO);
        assert!(health.observe_changes(at(1), 1).is_err());
        health.observe_snapshot(at(30)).unwrap();
        health.observe_snapshot(at(80)).unwrap();
        assert_eq!(health.snapshots(), 2);
        assert_eq!(health.accepted_changes(), 0);
        assert_eq!(health.last_valid_receive(), Some(at(80)));
        assert_eq!(assessment(&mut health, 89, None), Assessment::WarmingUp);
        assert_eq!(assessment(&mut health, 90, None), Assessment::QuietUnknown);
    }

    #[test]
    fn epochs_exclude_current_bucket_and_expire_without_timers() {
        let mut health = initialized();
        health.observe_changes(at(1), 2).unwrap();
        health.observe_changes(at(59), 3).unwrap();
        health.observe_changes(at(60), 7).unwrap();
        let window = health.aligned_window(at(69)).unwrap().unwrap();
        assert_eq!(
            window,
            ActivityWindow {
                start: at(0),
                end: at(60),
                changes: 5
            }
        );
        assert_eq!(health.changes_in_second(at(60)).unwrap(), 7);
        assert_eq!(health.changes_in_second(at(61)).unwrap(), 0);
        assert_eq!(health.aligned_window(at(70)).unwrap().unwrap().changes, 10);
        health.observe_changes(at(140), 11).unwrap();
        assert_eq!(health.aligned_window(at(140)).unwrap().unwrap().changes, 0);
        assert_eq!(health.aligned_window(at(150)).unwrap().unwrap().changes, 11);
        assert_eq!(
            health.aligned_window(at(10_000)).unwrap().unwrap().changes,
            0
        );
    }

    #[test]
    fn relative_confirmation_requires_ten_seconds_and_resets_on_recovery() {
        let mut quiet = initialized();
        let mut peer = initialized();
        peer.observe_changes(at(20), 5).unwrap();
        assert_eq!(
            assessment(&mut quiet, 60, Some(&peer)),
            Assessment::Pending(Suspicion::RelativeSilence)
        );
        assert_eq!(
            assessment(&mut quiet, 69, Some(&peer)),
            Assessment::Pending(Suspicion::RelativeSilence)
        );
        assert_eq!(
            assessment(&mut quiet, 70, Some(&peer)),
            Assessment::Suspect(Suspicion::RelativeSilence)
        );
        quiet.observe_changes(at(70), 1).unwrap();
        assert_eq!(
            assessment(&mut quiet, 80, Some(&peer)),
            Assessment::NoSuspicion
        );
        assert!(quiet.relative_since.is_none());
    }

    #[test]
    fn exact_relative_thresholds_and_both_quiet() {
        for (own, other, expected) in [
            (0, 4, Assessment::NoSuspicion),
            (0, 5, Assessment::Pending(Suspicion::RelativeSilence)),
            (1, 19, Assessment::NoSuspicion),
            (2, 20, Assessment::Pending(Suspicion::RelativeLowRate)),
            (3, 20, Assessment::NoSuspicion),
            (0, 0, Assessment::QuietUnknown),
        ] {
            let mut health = initialized();
            let mut peer = initialized();
            if own != 0 {
                health.observe_changes(at(20), own).unwrap();
            }
            if other != 0 {
                peer.observe_changes(at(20), other).unwrap();
            }
            assert_eq!(assessment(&mut health, 60, Some(&peer)), expected);
        }
    }

    #[test]
    fn only_complete_overlapping_valid_windows_compare() {
        let mut health = initialized();
        let mut peer = Health::new(at(1));
        peer.observe_snapshot(at(1)).unwrap();
        peer.observe_changes(at(20), 50).unwrap();
        assert_eq!(
            assessment(&mut health, 61, Some(&peer)),
            Assessment::PeerWarmingUp
        );
        assert_eq!(
            assessment(&mut health, 70, Some(&peer)),
            Assessment::Pending(Suspicion::RelativeSilence)
        );
        let mut fractional = Health::new(Duration::ZERO);
        fractional
            .observe_snapshot(Duration::from_millis(1))
            .unwrap();
        assert!(fractional.aligned_window(at(60)).unwrap().is_none());
        assert!(fractional.aligned_window(at(70)).unwrap().is_some());
    }

    fn baseline() -> Health {
        let mut health = initialized();
        for second in [1, 61, 121] {
            health.observe_changes(at(second), 20).unwrap();
        }
        health
    }

    #[test]
    fn absolute_requires_two_distinct_complete_minutes_and_freezes_baseline() {
        let mut health = baseline();
        health.observe_changes(at(181), 1).unwrap();
        assert_eq!(assessment(&mut health, 239, None), Assessment::NoSuspicion);
        assert_eq!(
            assessment(&mut health, 240, None),
            Assessment::Pending(Suspicion::AbsoluteLowRate)
        );
        assert_eq!(health.frozen_baseline_sum(), Some(60));
        assert_eq!(
            assessment(&mut health, 299, None),
            Assessment::Pending(Suspicion::AbsoluteLowRate)
        );
        assert_eq!(
            assessment(&mut health, 300, None),
            Assessment::Suspect(Suspicion::AbsoluteLowRate)
        );
        assert_eq!(health.frozen_baseline_sum(), Some(60));
        health.observe_changes(at(301), 2).unwrap();
        assert_eq!(assessment(&mut health, 360, None), Assessment::NoSuspicion);
        assert_eq!(health.frozen_baseline_sum(), None);
    }

    #[test]
    fn absolute_threshold_is_strict_and_each_baseline_minute_must_qualify() {
        let mut exact = baseline();
        exact.observe_changes(at(181), 2).unwrap();
        assert_eq!(assessment(&mut exact, 240, None), Assessment::NoSuspicion);
        let mut sparse = initialized();
        sparse.observe_changes(at(1), 100).unwrap();
        sparse.observe_changes(at(121), 100).unwrap();
        assert_eq!(assessment(&mut sparse, 240, None), Assessment::QuietUnknown);
        assert_eq!(sparse.frozen_baseline_sum(), None);
    }

    #[test]
    fn minute_history_survives_wrap_and_current_minute_does_not_change_baseline() {
        let mut health = initialized();
        for second in [361, 421, 481] {
            health.observe_changes(at(second), 20).unwrap();
        }
        health.observe_changes(at(600), 100).unwrap();
        assert_eq!(
            assessment(&mut health, 600, None),
            Assessment::Pending(Suspicion::AbsoluteLowRate)
        );
        assert_eq!(health.frozen_baseline_sum(), Some(60));
        assert_eq!(assessment(&mut health, 660, None), Assessment::NoSuspicion);
        assert_eq!(health.frozen_baseline_sum(), None);
    }

    #[test]
    fn empty_observation_does_not_refresh_receive_time_or_counters() {
        let mut health = initialized();
        let before = format!("{health:?}");
        assert!(health.observe_changes(at(60), 0).is_err());
        assert_eq!(format!("{health:?}"), before);
    }

    #[test]
    fn available_quiet_peer_prevents_absolute_repair_for_market_slowdown() {
        let mut health = baseline();
        let peer = baseline();
        assert_eq!(
            assessment(&mut health, 240, Some(&peer)),
            Assessment::QuietUnknown
        );
        assert_eq!(
            assessment(&mut health, 300, Some(&peer)),
            Assessment::QuietUnknown
        );
        assert_eq!(health.frozen_baseline_sum(), None);
    }

    #[test]
    fn late_ordered_receipts_correct_counts_without_relabeling_time() {
        let mut health = initialized();
        let mut peer = initialized();
        peer.observe_changes(at(20), 20).unwrap();
        assessment(&mut health, 60, Some(&peer));
        health.observe_changes(at(50), 1).unwrap();
        assert_eq!(health.last_valid_receive(), Some(at(50)));
        assert_eq!(health.changes_in_second(at(70)).unwrap(), 0);
        assert_eq!(health.aligned_window(at(70)).unwrap().unwrap().changes, 1);
        assert_eq!(
            assessment(&mut health, 70, Some(&peer)),
            Assessment::Pending(Suspicion::RelativeLowRate)
        );
        assert_eq!(
            assessment(&mut health, 80, Some(&peer)),
            Assessment::Suspect(Suspicion::RelativeLowRate)
        );
    }

    #[test]
    fn pauses_and_missed_absolute_minutes_do_not_confirm_stale_evidence() {
        let mut health = baseline();
        assessment(&mut health, 240, None);
        assert_eq!(assessment(&mut health, 360, None), Assessment::QuietUnknown);
        assert_eq!(health.frozen_baseline_sum(), None);
        let mut quiet = initialized();
        let mut peer = initialized();
        peer.observe_changes(at(20), 5).unwrap();
        assessment(&mut quiet, 60, Some(&peer));
        quiet.clear_confirmation();
        assert_eq!(
            assessment(&mut quiet, 70, Some(&peer)),
            Assessment::Pending(Suspicion::RelativeSilence)
        );
    }

    #[test]
    fn generation_reset_discards_all_activity_and_confirmation() {
        let mut old = baseline();
        assessment(&mut old, 240, None);
        let mut fresh = Health::new(at(240));
        assert_eq!(fresh.accepted_changes(), 0);
        assert_eq!(fresh.snapshots(), 0);
        assert_eq!(fresh.last_valid_receive(), None);
        assert_eq!(fresh.frozen_baseline_sum(), None);
        assert_eq!(assessment(&mut fresh, 300, None), Assessment::WarmingUp);
    }

    #[test]
    fn overflow_and_backward_clocks_fail_without_observation_mutation() {
        let mut health = initialized();
        health.observe_changes(at(1), u64::MAX).unwrap();
        let before = format!("{health:?}");
        assert!(health.observe_changes(at(2), 1).is_err());
        assert_eq!(format!("{health:?}"), before);
        assert!(health.observe_snapshot(Duration::ZERO).is_err());
        assert_eq!(format!("{health:?}"), before);
        assert!(health.aligned_window(Duration::ZERO).is_err());
        assert_eq!(
            health.aligned_window(at(60)).unwrap().unwrap().changes,
            u64::MAX
        );
        assessment(&mut health, 60, None);
        assert!(health.evaluate(at(59), None).is_err());
        health.snapshots = u64::MAX;
        let before = format!("{health:?}");
        assert!(health.observe_snapshot(at(61)).is_err());
        assert_eq!(format!("{health:?}"), before);
    }

    #[test]
    fn long_idle_jumps_are_bounded_and_epoch_tags_do_not_alias() {
        let mut health = baseline();
        health.observe_snapshot(at(u64::MAX - 1)).unwrap();
        assert_eq!(
            assessment(&mut health, u64::MAX, None),
            Assessment::QuietUnknown
        );
        assert_eq!(health.accepted_changes(), 60);
        assert_eq!(health.minutes.changes, [0; 5]);
    }

    #[test]
    fn fixed_storage_budget() {
        // Includes all counters, windows, clocks and policy state; no heap data.
        // Not a claim about total Replica size, allocator/RSS or throughput.
        assert!(std::mem::size_of::<Health>() <= 320);
    }
}
