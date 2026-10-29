//! Opt-in stage latency diagnostics. Off by default. Each histogram belongs to
//! exactly one thread (owner or writer). Recording a sample touches only a
//! fixed array allocated before intake: no allocation, lock or atomic. Rows
//! are emitted to the integrity sidecar once per window; stdout is untouched.
use serde_json::{Value, json};
use std::time::{Duration, Instant};

/// Sub-buckets per power of two. Bucket width is at most 1/16 (6.25%) of its
/// lower bound; values below 16 are exact.
const SUB_BITS: u32 = 4;
const SUB: usize = 1 << SUB_BITS;
pub const BUCKETS: usize = (64 - SUB_BITS as usize + 1) * SUB;
pub const WINDOW: Duration = Duration::from_secs(60);
/// Messages whose S3 reaches this duration get an attribution row.
pub const OUTLIER: Duration = Duration::from_millis(5);
/// Attribution rows per run; later outliers are only counted.
pub const OUTLIER_ROWS: u64 = 1000;
pub const SCHEME: &str = "log-linear: 16 sub-buckets per power of two; quantile = inclusive upper bound of the bucket holding the nearest-rank sample, capped at the exact max";

pub fn index(value: u64) -> usize {
    if value < SUB as u64 {
        return value as usize;
    }
    let shift = 63 - value.leading_zeros() - SUB_BITS;
    ((shift as usize + 1) << SUB_BITS) + ((value >> shift) as usize & (SUB - 1))
}

/// Inclusive value range of a bucket.
pub fn bounds(index: usize) -> (u64, u64) {
    if index < SUB {
        return (index as u64, index as u64);
    }
    let shift = (index >> SUB_BITS) as u32 - 1;
    let lower = ((SUB + (index & (SUB - 1))) as u64) << shift;
    (lower, lower + ((1u64 << shift) - 1))
}

pub struct Histogram {
    counts: Box<[u64; BUCKETS]>,
    count: u64,
    sum: u128,
    min: u64,
    max: u64,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: Box::new([0; BUCKETS]),
            count: 0,
            sum: 0,
            min: u64::MAX,
            max: 0,
        }
    }
}
impl Histogram {
    #[inline]
    pub fn record(&mut self, value: u64) {
        self.counts[index(value)] += 1;
        self.count += 1;
        self.sum += u128::from(value);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
    #[inline]
    pub fn duration(&mut self, value: Duration) {
        self.record(u64::try_from(value.as_nanos()).unwrap_or(u64::MAX));
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    /// Nearest rank `ceil(count * numerator / denominator)`, reported as the
    /// upper bound of its bucket (never above the exact maximum).
    pub fn quantile(&self, numerator: u64, denominator: u64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let rank = (u128::from(self.count) * u128::from(numerator))
            .div_ceil(u128::from(denominator))
            .max(1);
        let mut seen = 0u128;
        for (i, &n) in self.counts.iter().enumerate() {
            seen += u128::from(n);
            if seen >= rank {
                return Some(bounds(i).1.min(self.max));
            }
        }
        unreachable!("rank within count")
    }
    /// Window summary plus the dense non-zero bucket span, so later analysis
    /// can merge windows exactly instead of pooling percentiles.
    pub fn report(&self) -> Value {
        let span = self
            .counts
            .iter()
            .position(|&n| n > 0)
            .map(|first| (first, self.counts.iter().rposition(|&n| n > 0).unwrap()));
        json!({
            "count": self.count,
            "sum": self.sum.to_string(),
            "min": (self.count > 0).then_some(self.min),
            "max": (self.count > 0).then_some(self.max),
            "p50": self.quantile(50, 100),
            "p90": self.quantile(90, 100),
            "p99": self.quantile(99, 100),
            "p999": self.quantile(999, 1000),
            "first_bucket": span.map(|(first, _)| first),
            "buckets": span.map_or(&[][..], |(first, last)| &self.counts[first..=last]),
        })
    }
    pub fn reset(&mut self) {
        self.counts.fill(0);
        self.count = 0;
        self.sum = 0;
        self.min = u64::MAX;
        self.max = 0;
    }
}

/// Non-data ingress message kinds, in `OPS` order.
pub const KINDS: [&str; 7] = [
    "started",
    "reconcile_begin",
    "desired_chunk",
    "reconcile_complete",
    "subscriptions_requested",
    "requests_complete",
    "ended",
];
/// Owner operations outside data-message processing. Control messages are
/// split into `Processor::process` and `record_control`, per kind.
pub const OPS: [&str; 21] = [
    "depth_sample",
    "tick",
    "maintain",
    "capture_due",
    "recovery_sends",
    "readiness_sends",
    "hint_sends",
    "process.started",
    "process.reconcile_begin",
    "process.desired_chunk",
    "process.reconcile_complete",
    "process.subscriptions_requested",
    "process.requests_complete",
    "process.ended",
    "record_control.started",
    "record_control.reconcile_begin",
    "record_control.desired_chunk",
    "record_control.reconcile_complete",
    "record_control.subscriptions_requested",
    "record_control.requests_complete",
    "record_control.ended",
];
pub const DEPTH: usize = 0;
pub const TICK: usize = 1;
pub const MAINTAIN: usize = 2;
pub const CAPTURE: usize = 3;
pub const RECOVERY: usize = 4;
pub const READINESS: usize = 5;
pub const HINTS: usize = 6;
pub fn process_op(kind: usize) -> usize {
    7 + kind
}
pub fn record_op(kind: usize) -> usize {
    7 + KINDS.len() + kind
}

/// One operation's window histogram plus where its maximum happened.
#[derive(Default)]
pub(super) struct Op {
    histogram: Histogram,
    max_ns: u64,
    max_end_us: Option<i64>,
    max_input: u64,
}

/// Owner-thread stages. Lives in the recorder because S4 is measured at
/// batch publication, which the recorder performs.
pub(super) struct OwnerStages {
    /// Receive instant of the data message being processed; attributed to the
    /// SBE records it produces.
    pub received: Option<Instant>,
    pub receive_to_enqueue: Histogram,
    pub queue_wait: Histogram,
    pub processing: Histogram,
    pub receive_to_publish: Histogram,
    pub slice_ns: Histogram,
    pub slice_levels: Histogram,
    pub slice_snapshots: Histogram,
    pub slice_open: (u64, u64),
    pub message_levels: u64,
    ops: Vec<Op>,
    pub outlier_rows: u64,
    pub outliers_dropped: u64,
    pub window: Window,
}
impl OwnerStages {
    pub fn new() -> Self {
        Self {
            received: None,
            receive_to_enqueue: Histogram::default(),
            queue_wait: Histogram::default(),
            processing: Histogram::default(),
            receive_to_publish: Histogram::default(),
            slice_ns: Histogram::default(),
            slice_levels: Histogram::default(),
            slice_snapshots: Histogram::default(),
            slice_open: (0, 0),
            message_levels: 0,
            ops: (0..OPS.len()).map(|_| Op::default()).collect(),
            outlier_rows: 0,
            outliers_dropped: 0,
            window: Window::new(),
        }
    }
    pub fn op(&mut self, op: usize, elapsed: Duration, input: u64) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let op = &mut self.ops[op];
        op.histogram.record(ns);
        if ns > op.max_ns || op.max_end_us.is_none() {
            op.max_ns = ns;
            op.max_end_us = Some(chrono::Utc::now().timestamp_micros());
            op.max_input = input;
        }
    }
    pub fn take_row(&mut self, last: bool) -> Value {
        let operations: serde_json::Map<String, Value> = OPS
            .iter()
            .zip(&self.ops)
            .map(|(name, op)| {
                let mut report = op.histogram.report();
                report["max_end_us"] = json!(op.max_end_us);
                report["max_input"] = json!(op.max_end_us.map(|_| op.max_input));
                (name.to_string(), report)
            })
            .collect();
        let row = json!({
            "kind": "latency", "thread": "owner", "unit": "ns", "scheme": SCHEME,
            "window": self.window.report(last),
            "stages": {
                "s1_receive_to_enqueue": self.receive_to_enqueue.report(),
                "s2_queue_wait": self.queue_wait.report(),
                "s3_dequeue_to_processed": self.processing.report(),
                "s4_receive_to_batch_published": self.receive_to_publish.report(),
            },
            "snapshot_slices": {
                "duration_ns": self.slice_ns.report(),
                "levels": self.slice_levels.report(),
                "snapshots": self.slice_snapshots.report(),
            },
            "operations": operations,
            "s3_outlier_rows_total": self.outlier_rows,
            "s3_outliers_dropped_total": self.outliers_dropped,
        });
        for op in &mut self.ops {
            op.histogram.reset();
            op.max_ns = 0;
            op.max_end_us = None;
            op.max_input = 0;
        }
        for h in [
            &mut self.receive_to_enqueue,
            &mut self.queue_wait,
            &mut self.processing,
            &mut self.receive_to_publish,
            &mut self.slice_ns,
            &mut self.slice_levels,
            &mut self.slice_snapshots,
        ] {
            h.reset();
        }
        row
    }
}

/// Writer-thread stages, owned by `Output::run`.
pub(super) struct WriterStages {
    pub publish_to_receive: Histogram,
    pub receive_to_written: Histogram,
    pub receive_to_flushed: Histogram,
    pub end_to_end: Histogram,
    /// Receive instants of records written but not yet flushed, and writer
    /// receipt instants of batches not yet flushed. Preallocated; overflow is
    /// counted, never grown.
    pub pending_records: Vec<Instant>,
    pub pending_batches: Vec<Instant>,
    pub unmeasured_records: u64,
    pub unmeasured_batches: u64,
    pub window: Window,
}
impl WriterStages {
    pub fn new(records: usize, batches: usize) -> Self {
        Self {
            publish_to_receive: Histogram::default(),
            receive_to_written: Histogram::default(),
            receive_to_flushed: Histogram::default(),
            end_to_end: Histogram::default(),
            pending_records: Vec::with_capacity(records),
            pending_batches: Vec::with_capacity(batches),
            unmeasured_records: 0,
            unmeasured_batches: 0,
            window: Window::new(),
        }
    }
    #[inline]
    pub fn written(&mut self, received: Instant) {
        if self.pending_records.len() < self.pending_records.capacity() {
            self.pending_records.push(received);
        } else {
            self.unmeasured_records += 1;
        }
    }
    #[inline]
    pub fn batch_written(&mut self, received: Instant) {
        self.receive_to_written.duration(received.elapsed());
        if self.pending_batches.len() < self.pending_batches.capacity() {
            self.pending_batches.push(received);
        } else {
            self.unmeasured_batches += 1;
        }
    }
    /// Call after the destination flush returned successfully.
    pub fn flushed(&mut self) {
        let now = Instant::now();
        for r in self.pending_records.drain(..) {
            self.end_to_end.duration(now.saturating_duration_since(r));
        }
        for b in self.pending_batches.drain(..) {
            self.receive_to_flushed
                .duration(now.saturating_duration_since(b));
        }
    }
    pub fn take_row(&mut self, last: bool) -> Value {
        let row = json!({
            "kind": "latency", "thread": "writer", "unit": "ns", "scheme": SCHEME,
            "window": self.window.report(last),
            "stages": {
                "s5_batch_published_to_writer": self.publish_to_receive.report(),
                "s6_writer_received_to_written": self.receive_to_written.report(),
                "s6_writer_received_to_flushed": self.receive_to_flushed.report(),
                "s7_receive_to_flushed": self.end_to_end.report(),
            },
            "unmeasured_records": self.unmeasured_records,
            "unmeasured_batches": self.unmeasured_batches,
        });
        for h in [
            &mut self.publish_to_receive,
            &mut self.receive_to_written,
            &mut self.receive_to_flushed,
            &mut self.end_to_end,
        ] {
            h.reset();
        }
        self.unmeasured_records = 0;
        self.unmeasured_batches = 0;
        row
    }
}

pub(super) struct Window {
    index: u64,
    started: Instant,
    started_us: i64,
}
impl Window {
    fn new() -> Self {
        Self {
            index: 0,
            started: Instant::now(),
            started_us: chrono::Utc::now().timestamp_micros(),
        }
    }
    pub fn due(&self, now: Instant) -> bool {
        now.duration_since(self.started) >= WINDOW
    }
    fn report(&mut self, last: bool) -> Value {
        let end_us = chrono::Utc::now().timestamp_micros();
        let row = json!({"index": self.index, "start_us": self.started_us, "end_us": end_us,
            "elapsed_ns": u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX), "last": last});
        self.index += 1;
        self.started = Instant::now();
        self.started_us = end_us;
        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_contiguous_and_contain_their_values() {
        let mut expected_lower = 0u64;
        for i in 0..BUCKETS {
            let (lower, upper) = bounds(i);
            assert_eq!(lower, expected_lower, "gap before bucket {i}");
            assert!(upper >= lower);
            assert_eq!(index(lower), i);
            assert_eq!(index(upper), i);
            // Width never exceeds 1/16 of the lower bound (exact below 16).
            assert!(lower < 16 || (upper - lower + 1) * 16 <= lower);
            if i + 1 < BUCKETS {
                expected_lower = upper + 1;
            } else {
                assert_eq!(upper, u64::MAX);
            }
        }
    }

    /// Exact sorted-sample oracle: the reported quantile is the upper bound of
    /// the bucket containing the true nearest-rank sample, capped at max.
    #[test]
    fn quantiles_match_sorted_oracle() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for case in 0..200 {
            let n = 1 + (next() % 5000) as usize;
            // Heavy-tailed mixture spanning ns..s, with deliberate duplicates.
            let mut samples: Vec<u64> = (0..n)
                .map(|_| match next() % 4 {
                    0 => next() % 20,
                    1 => 1_000 + next() % 50_000,
                    2 => 100_000 << (next() % 12),
                    _ => 104_036_000,
                })
                .collect();
            let mut h = Histogram::default();
            samples.iter().for_each(|&v| h.record(v));
            samples.sort_unstable();
            for (num, den) in [
                (1, 1000),
                (50, 100),
                (90, 100),
                (99, 100),
                (999, 1000),
                (1, 1),
            ] {
                let rank = (n as u64 * num).div_ceil(den).max(1) as usize;
                let exact = samples[rank - 1];
                let reported = h.quantile(num, den).unwrap();
                let (lower, upper) = bounds(index(exact));
                assert!(
                    reported >= exact
                        && reported >= lower
                        && reported == upper.min(*samples.last().unwrap()),
                    "case {case} q={num}/{den}: exact {exact} reported {reported}"
                );
            }
            assert_eq!(h.quantile(1, 1), Some(*samples.last().unwrap()));
            assert_eq!(h.count(), n as u64);
        }
        assert_eq!(Histogram::default().quantile(99, 100), None);
    }

    #[test]
    fn report_round_trips_bucket_span_and_resets() {
        let mut h = Histogram::default();
        for v in [3, 17, 17, 1_000_000] {
            h.record(v);
        }
        let r = h.report();
        assert_eq!(r["count"], 4);
        assert_eq!(r["max"], 1_000_000);
        assert_eq!(r["p50"], 17);
        let first = r["first_bucket"].as_u64().unwrap() as usize;
        let counts: Vec<u64> = r["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_u64().unwrap())
            .collect();
        assert_eq!(first, index(3));
        assert_eq!(first + counts.len() - 1, index(1_000_000));
        assert_eq!(counts.iter().sum::<u64>(), 4);
        assert_eq!(counts[index(17) - first], 2);
        h.reset();
        assert_eq!(h.report()["count"], 0);
        assert!(h.report()["p99"].is_null());
    }
}
