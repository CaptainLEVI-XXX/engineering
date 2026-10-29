//! Selected book recording. Book/source ownership stays in `orderbook`.
pub mod encode;
pub mod latency;
mod output;
use crate::{
    orderbook::{self, Binding, Observation, ObservationKind, SnapshotOrigin},
    util::dtos::SbeEncode,
};
use anyhow::{Context, Result, ensure};
use crossbeam::channel::{Receiver, Sender, bounded};
use output::{Batch, Entry, Kind, Outcome};
pub use output::{Output, Report};
use serde_json::{Value, json};
use std::{
    io::Write,
    time::{Duration, Instant},
};

pub const CHUNK_BYTES: usize = 64 << 20;
const INTEGRITY_BYTES: usize = 8192;
/// Reserved per owner latency row: 28 histograms of at most 976 counts of at
/// most 20 digits each (<21 KB each) plus fixed fields.
const LATENCY_ROW_BYTES: usize = 1 << 20;

#[derive(Clone, Debug, serde::Serialize)]
pub struct Config {
    pub batches: usize,
    pub batch_bytes: usize,
    pub entries: usize,
    pub publish_bytes: usize,
    pub publish_age: Duration,
    pub buffer_bytes: usize,
    pub flush_age: Duration,
    pub chunk_age: Duration,
    /// Opt-in per-stage latency rows in the integrity sidecar. Never stdout.
    pub latency_histograms: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            batches: 16,
            batch_bytes: 4 << 20,
            entries: 4096,
            publish_bytes: 256 << 10,
            // 1 ms bounds output visibility; live r2 at 5 ms spent ~95% of its
            // ~6 ms p50 end-to-end time waiting here. 1 ms is the floor the
            // millisecond config admits. Receive timestamps are unaffected.
            publish_age: Duration::from_millis(1),
            buffer_bytes: 256 << 10,
            flush_age: Duration::from_millis(1),
            chunk_age: Duration::from_secs(60),
            latency_histograms: false,
        }
    }
}

/// Allocate the entire fixed pool before intake. No allocation fallback on exhaustion.
pub fn channel(config: Config, books: &orderbook::Config) -> Result<(Recorder, Output)> {
    let maximum = books
        .levels_per_side
        .checked_mul(32)
        .and_then(|n| n.checked_add(36 + 14))
        .and_then(|n| n.checked_add(books.wire.identity_bytes))
        .context("record budget overflow")?;
    ensure!(
        maximum <= CHUNK_BYTES - 8
            && maximum
                .checked_add(INTEGRITY_BYTES)
                .is_some_and(|n| n <= config.batch_bytes),
        "admitted snapshot and provenance do not fit batch"
    );
    ensure!(
        books
            .wire
            .identity_bytes
            .checked_mul(6)
            .and_then(|n| n.checked_add(2048))
            .is_some_and(|n| n <= INTEGRITY_BYTES),
        "identity cannot fit integrity entry"
    );
    ensure!(
        config.batches > 0
            && config.entries >= 2
            && config.publish_bytes > 0
            && config.publish_bytes <= config.batch_bytes
            && config.buffer_bytes > 0
            && !config.publish_age.is_zero()
            && !config.flush_age.is_zero()
            && !config.chunk_age.is_zero(),
        "invalid recording budgets"
    );
    ensure!(
        !config.latency_histograms || LATENCY_ROW_BYTES <= config.batch_bytes,
        "latency rows do not fit batch"
    );
    let (ready_send, ready) = bounded(config.batches);
    let (recycle, free) = bounded(config.batches);
    let mut pool_bytes = 0;
    for _ in 0..config.batches {
        let batch = Batch {
            bytes: Vec::with_capacity(config.batch_bytes),
            entries: Vec::with_capacity(config.entries),
            first_record: 0,
            created_at: Instant::now(),
            publish_at: Instant::now(),
            published_at: Instant::now(),
        };
        pool_bytes +=
            batch.bytes.capacity() + batch.entries.capacity() * std::mem::size_of::<Entry>();
        recycle.send(batch)?;
    }
    let (outcome, producer) = bounded(1);
    let (alive, health) = bounded(0);
    Ok((
        Recorder {
            config: config.clone(),
            ready: ready_send,
            free,
            current: None,
            symbols: Vec::new(),
            max_symbols: books.instruments,
            ordinal: 0,
            handed: 0,
            failed: None,
            input: 0,
            outcome,
            health,
            late_snapshots: 0,
            max_snapshot_lateness_us: 0,
            snapshots_due_at_cutoff: 0,
            latency: config
                .latency_histograms
                .then(|| Box::new(latency::OwnerStages::new())),
        },
        Output {
            config,
            ready,
            recycle,
            producer,
            _alive: alive,
            pool_bytes,
        },
    ))
}

pub struct Recorder {
    config: Config,
    ready: Sender<Batch>,
    free: Receiver<Batch>,
    current: Option<Batch>,
    symbols: Vec<Option<String>>,
    max_symbols: usize,
    ordinal: u64,
    handed: u64,
    failed: Option<u64>,
    input: u64,
    outcome: Sender<Outcome>,
    health: Receiver<()>,
    late_snapshots: u64,
    max_snapshot_lateness_us: u128,
    snapshots_due_at_cutoff: usize,
    latency: Option<Box<latency::OwnerStages>>,
}
impl Recorder {
    pub fn check_output(&self) -> Result<()> {
        ensure!(
            !matches!(
                self.health.try_recv(),
                Err(crossbeam::channel::TryRecvError::Disconnected)
            ),
            "output worker ended"
        );
        ensure!(self.failed.is_none(), "recorder already failed");
        Ok(())
    }
    fn reserve(&mut self, bytes: usize, entries: usize) -> Result<()> {
        ensure!(
            bytes <= self.config.batch_bytes && entries <= self.config.entries,
            "record exceeds batch budget"
        );
        if self.current.as_ref().is_some_and(|b| {
            b.bytes.len() + bytes > self.config.batch_bytes
                || b.entries.len() + entries > self.config.entries
        }) {
            self.publish()?;
        }
        if self.current.is_none() {
            self.check_output()?;
            let mut batch = self
                .free
                .try_recv()
                .context("output pool exhausted or closed; history incomplete")?;
            batch.first_record = self.ordinal;
            batch.created_at = Instant::now();
            batch.publish_at = batch.created_at + self.config.publish_age;
            self.current = Some(batch);
        }
        Ok(())
    }
    fn publish(&mut self) -> Result<()> {
        if let Some(mut batch) = self.current.take() {
            if batch.entries.is_empty() {
                self.current = Some(batch);
                return Ok(());
            }
            if let Some(stages) = &mut self.latency {
                let now = Instant::now();
                batch.published_at = now;
                for received in batch.entries.iter().filter_map(|e| e.received) {
                    stages
                        .receive_to_publish
                        .duration(now.saturating_duration_since(received));
                }
            }
            self.ready
                .try_send(batch)
                .map_err(|_| anyhow::anyhow!("output handoff closed or full"))?;
            self.handed = self.ordinal;
        }
        Ok(())
    }
    pub fn next_publish_at(&self) -> Option<Instant> {
        self.current
            .as_ref()
            .filter(|b| !b.entries.is_empty())
            .map(|b| b.publish_at)
    }
    pub fn publish_due(&mut self, now: Instant) -> Result<()> {
        let result = self.check_output().and_then(|()| {
            if self.next_publish_at().is_some_and(|at| now >= at) {
                if self.config.batches > 1 && self.free.is_empty() {
                    // Output is behind: retain the last batch for further exact
                    // records instead of exhausting the pool with tiny aged tails.
                    // Size/descriptor limits still force publication and fail closed.
                    self.current.as_mut().unwrap().publish_at = now + self.config.publish_age;
                } else {
                    self.publish()?;
                }
            }
            Ok(())
        });
        if result.is_err() {
            self.failed.get_or_insert(self.input);
        }
        result
    }
    pub fn observe(&mut self, o: Observation<'_>) -> Result<()> {
        self.input = o.input;
        let result = self.observe_inner(o);
        if result.is_err() {
            self.failed.get_or_insert(self.input);
        }
        result
    }
    fn observe_inner(&mut self, o: Observation<'_>) -> Result<()> {
        ensure!(self.failed.is_none(), "recorder already failed");
        let id = o.binding.book.0 as usize;
        ensure!(id < self.max_symbols, "symbol cache budget exceeded");
        if id >= self.symbols.len() {
            if id >= self.symbols.capacity() {
                let capacity = (id + 1).next_power_of_two().min(self.max_symbols);
                self.symbols.reserve_exact(capacity - self.symbols.len());
            }
            self.symbols.resize_with(id + 1, || None);
        }
        let prefix = o.adapter.namespace();
        let symbol = self.symbols[id].get_or_insert_with(|| format!("{prefix}{}", o.asset));
        ensure!(
            symbol.strip_prefix(prefix) == Some(o.asset),
            "BookId identity changed"
        );
        // Prepare metadata only for snapshots/gaps; level updates allocate no JSON or symbol.
        let (size, metadata) = match &o.kind {
            ObservationKind::StandbyBaseline => (
                0,
                Some(Row::Json(json!({"kind":"standby_baseline",
                "input":o.input,"next_ordinal":self.ordinal,"symbol":symbol,"binding":binding(o.binding),
                "source_ordinal":o.metadata.map(|m| m.cursor.ordinal),"source_event":o.metadata.map(|m| m.cursor.event)}))),
            ),
            ObservationKind::Snapshot { book, origin } => {
                if let Some(stages) = &mut self.latency {
                    if let SnapshotOrigin::Periodic { .. } = origin {
                        stages.slice_open.0 += 1;
                        stages.slice_open.1 += book.len() as u64;
                    } else {
                        stages.message_levels += book.len() as u64;
                    }
                }
                let m = o.metadata.context("snapshot has no source metadata")?;
                let (name, at, due) = match origin {
                    SnapshotOrigin::Exchange => ("exchange", m.received_at, None),
                    SnapshotOrigin::Selection { at, .. } => ("selection", *at, None),
                    SnapshotOrigin::Periodic { at, due } => {
                        ("periodic", *at, Some(due.as_micros()))
                    }
                };
                let record = encode::SnapshotRecord {
                    book,
                    symbol,
                    received_at: at,
                    exchange_us: m.exchange_us.context("missing exchange time")?,
                };
                let selection = match origin {
                    SnapshotOrigin::Selection {
                        previous,
                        reason,
                        freshness,
                        receive_age,
                        ..
                    } => Some(json!({"previous":previous.map(binding),"reason":reason,
                            "freshness":freshness,"receive_age_us":receive_age.map(|age| age.as_micros())})),
                    _ => None,
                };
                (
                    record.encoded_length_with_header()?,
                    Some(Row::Snapshot(SnapshotFields {
                        binding: o.binding,
                        due_us: due,
                        exchange_us: m.exchange_us,
                        input: o.input,
                        ordinal: self.ordinal,
                        origin: name,
                        received_us: at.timestamp_micros(),
                        selection,
                        source_entry: m.cursor.entry,
                        source_event: m.cursor.event,
                        source_ordinal: m.cursor.ordinal,
                        source_received_us: m.received_at.timestamp_micros(),
                    })),
                )
            }
            ObservationKind::Level { .. } => (45 + symbol.len(), None),
            ObservationKind::Invalidated {
                reason,
                cause,
                output_binding,
            } => (
                0,
                Some(Row::Invalidation(InvalidationFields {
                    binding: output_binding.unwrap_or(o.binding),
                    cause: *cause,
                    detected_us: chrono::Utc::now().timestamp_micros(),
                    input: o.input,
                    kind: if output_binding.is_some() {
                        "unavailable"
                    } else {
                        "replica_unavailable"
                    },
                    next_ordinal: self.ordinal,
                    reason: first_chars(reason, 128),
                })),
            ),
        };
        let next = if size > 0 {
            self.ordinal
                .checked_add(1)
                .context("record ordinal exhausted")?
        } else {
            self.ordinal
        };
        self.reserve(
            size + if metadata.is_some() {
                INTEGRITY_BYTES
            } else {
                0
            },
            usize::from(size > 0) + usize::from(metadata.is_some()),
        )?;
        let symbol = self.symbols[id].as_deref().unwrap();
        let batch = self.current.as_mut().unwrap();
        let begin = batch.bytes.len();
        let result: Result<usize> = (|| {
            batch.bytes.resize(
                begin
                    + size
                    + if metadata.is_some() {
                        INTEGRITY_BYTES
                    } else {
                        0
                    },
                0,
            );
            let target = &mut batch.bytes[begin..begin + size];
            let encoded = match o.kind {
                ObservationKind::Snapshot { book, origin } => {
                    let m = o.metadata.context("snapshot metadata absent")?;
                    let at = match origin {
                        SnapshotOrigin::Exchange => m.received_at,
                        SnapshotOrigin::Selection { at, .. }
                        | SnapshotOrigin::Periodic { at, .. } => at,
                    };
                    encode::SnapshotRecord {
                        book,
                        symbol,
                        received_at: at,
                        exchange_us: m.exchange_us.context("missing exchange time")?,
                    }
                    .encode(target)?
                }
                ObservationKind::Level {
                    side,
                    price,
                    quantity,
                } => {
                    let m = o.metadata.context("level metadata absent")?;
                    encode::LevelRecord {
                        symbol,
                        received_at: m.received_at,
                        exchange_us: m.exchange_us.context("missing exchange time")?,
                        side,
                        price,
                        quantity,
                    }
                    .encode(target)?
                }
                ObservationKind::Invalidated { .. } | ObservationKind::StandbyBaseline => 0,
            };
            ensure!(encoded == size, "record length mismatch");
            let n = if let Some(row) = &metadata {
                let slot = &mut batch.bytes[begin + size..];
                match row {
                    Row::Json(value) => json_line(slot, value)?,
                    Row::Snapshot(fields) => json_line(slot, &SnapshotRow { fields, symbol })?,
                    Row::Invalidation(fields) => {
                        json_line(slot, &InvalidationRow { fields, symbol })?
                    }
                }
            } else {
                0
            };
            Ok(n)
        })();
        let n = match result {
            Ok(n) => n,
            Err(e) => {
                batch.bytes.truncate(begin);
                return Err(e);
            }
        };
        batch.bytes.truncate(begin + size + n);
        if size > 0 {
            batch.entries.push(Entry {
                end: begin + size,
                kind: Kind::Sbe,
                received: self.latency.as_ref().and_then(|l| l.received),
            });
        }
        if n > 0 {
            batch.entries.push(Entry {
                end: begin + size + n,
                kind: Kind::Integrity,
                received: None,
            });
        }
        self.ordinal = next;
        // The publish watermark is soft. Like age-based publication, retain
        // the last batch during a writer pause; reserve enforces hard limits.
        if batch.entries.len() == self.config.entries
            || (batch.bytes.len() >= self.config.publish_bytes
                && (self.config.batches == 1 || !self.free.is_empty()))
        {
            self.publish()?;
        }
        Ok(())
    }
    pub fn snapshot_stats(&mut self, stats: &orderbook::Stats) {
        self.late_snapshots = stats.late_snapshots;
        self.max_snapshot_lateness_us = stats.max_snapshot_lateness.as_micros();
        self.snapshots_due_at_cutoff = stats.snapshots_due_at_cutoff;
    }
    /// Ordered, bounded control provenance. It never writes binary stdout or
    /// advances the financial record ordinal.
    pub fn control(&mut self, input: u64, value: &Value) -> Result<()> {
        self.control_sized(input, value, INTEGRITY_BYTES)
    }
    fn control_sized(&mut self, input: u64, value: &Value, bytes: usize) -> Result<()> {
        self.input = input;
        let result = (|| {
            self.reserve(bytes, 1)?;
            let batch = self.current.as_mut().unwrap();
            let start = batch.bytes.len();
            batch.bytes.resize(start + bytes, 0);
            let encoded = json_line(
                &mut batch.bytes[start..],
                &json!({
                    "kind":"control", "input":input,"next_ordinal":self.ordinal,"event":value
                }),
            );
            match encoded {
                Ok(len) => {
                    batch.bytes.truncate(start + len);
                    batch.entries.push(Entry {
                        end: start + len,
                        kind: Kind::Integrity,
                        received: None,
                    });
                }
                Err(error) => {
                    batch.bytes.truncate(start);
                    return Err(error);
                }
            }
            if batch.entries.len() == self.config.entries
                || (batch.bytes.len() >= self.config.publish_bytes
                    && (self.config.batches == 1 || !self.free.is_empty()))
            {
                self.publish()?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed.get_or_insert(input);
        }
        result
    }
    pub fn latency_enabled(&self) -> bool {
        self.latency.is_some()
    }
    /// Attribute the SBE records produced next to this data message's receive.
    pub fn begin_message(&mut self, received: Option<Instant>) {
        if let Some(stages) = &mut self.latency {
            stages.received = received;
            stages.message_levels = 0;
        }
    }
    /// Committed levels in the snapshots observed for the current message.
    pub fn message_levels(&self) -> u64 {
        self.latency.as_ref().map_or(0, |l| l.message_levels)
    }
    /// S1 receive->enqueue, S2 enqueue->dequeue, S3 dequeue->processed.
    pub fn end_message(
        &mut self,
        received: Instant,
        queued: Instant,
        dequeued: Instant,
        processed: Instant,
    ) {
        if let Some(stages) = &mut self.latency {
            stages.received = None;
            stages
                .receive_to_enqueue
                .duration(queued.saturating_duration_since(received));
            stages
                .queue_wait
                .duration(dequeued.saturating_duration_since(queued));
            stages
                .processing
                .duration(processed.saturating_duration_since(dequeued));
        }
    }
    /// Close one `capture_due` call; records it only if it captured anything.
    pub fn end_slice(&mut self, elapsed: Duration) {
        if let Some(stages) = &mut self.latency
            && stages.slice_open.0 > 0
        {
            let (snapshots, levels) = std::mem::take(&mut stages.slice_open);
            stages.slice_ns.duration(elapsed);
            stages.slice_levels.record(levels);
            stages.slice_snapshots.record(snapshots);
        }
    }
    /// One owner operation outside data processing; no-op when disabled.
    pub fn latency_op(&mut self, op: usize, elapsed: Duration, input: u64) {
        if let Some(stages) = &mut self.latency {
            stages.op(op, elapsed, input);
        }
    }
    /// One attribution row for a message whose dequeue->processed time reached
    /// `latency::OUTLIER`, at most `latency::OUTLIER_ROWS` per run. Skipped
    /// (and counted) rather than consuming the last free output batch.
    pub fn s3_outlier(
        &mut self,
        input: u64,
        elapsed: Duration,
        row: impl FnOnce() -> Value,
    ) -> Result<()> {
        if elapsed < latency::OUTLIER {
            return Ok(());
        }
        let fits = self.latency_row_fits();
        let Some(stages) = &mut self.latency else {
            return Ok(());
        };
        if stages.outlier_rows >= latency::OUTLIER_ROWS || !fits {
            stages.outliers_dropped += 1;
            return Ok(());
        }
        stages.outlier_rows += 1;
        let mut value = row();
        value["kind"] = json!("s3_outlier");
        value["ns"] = json!(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        value["end_us"] = json!(chrono::Utc::now().timestamp_micros());
        self.control(input, &value)
    }
    /// Emit the owner window row when due. Diagnostic only.
    /// Deferred, never fatal for lack of space: a diagnostic row must not
    /// exhaust the pool that market records need.
    pub fn latency_due(&mut self, input: u64, now: Instant) -> Result<()> {
        if self.latency.as_ref().is_some_and(|l| l.window.due(now)) && self.latency_row_fits() {
            let row = self.latency.as_mut().unwrap().take_row(false);
            self.control_sized(input, &row, LATENCY_ROW_BYTES)?;
        }
        Ok(())
    }
    fn latency_row_fits(&self) -> bool {
        !self.free.is_empty()
            || self.current.as_ref().is_some_and(|b| {
                b.bytes.len() + LATENCY_ROW_BYTES <= self.config.batch_bytes
                    && b.entries.len() < self.config.entries
            })
    }
    /// Success here is handoff only; `Output::run` establishes physical completion.
    pub fn finish(mut self, input_result: Result<()>) -> Result<()> {
        // Publish first so the final batch's S4 samples enter the last row.
        let publication = self.publish().and_then(|()| {
            if self.latency.is_some() && self.failed.is_none() && self.latency_row_fits() {
                let row = self.latency.as_mut().unwrap().take_row(true);
                self.control_sized(self.input, &row, LATENCY_ROW_BYTES)?;
                self.publish()?;
            }
            Ok(())
        });
        if publication.is_err() {
            self.failed.get_or_insert(self.input);
        }
        let symbol_bytes = self.symbols.capacity() * std::mem::size_of::<Option<String>>()
            + self
                .symbols
                .iter()
                .flatten()
                .map(String::capacity)
                .sum::<usize>();
        self.outcome
            .send(Outcome {
                encoded: self.ordinal,
                handed: self.handed,
                input_complete: input_result.is_ok(),
                failure_input: self.failed,
                symbol_bytes,
                late_snapshots: self.late_snapshots,
                max_snapshot_lateness_us: self.max_snapshot_lateness_us,
                snapshots_due_at_cutoff: self.snapshots_due_at_cutoff,
            })
            .map_err(|_| anyhow::anyhow!("output outcome receiver closed"))?;
        publication?;
        input_result?;
        ensure!(
            self.failed.is_none(),
            "recording prefix incomplete at input {:?}",
            self.failed
        );
        Ok(())
    }
}
/// A sidecar row awaiting its slot. Snapshot rows are typed: they are one per
/// snapshot record (every live book, every minute), and building them as a
/// `Value` cost several times the SBE encoding itself. Invalidation rows are
/// typed too: a session end writes one per assigned book (~1,000 at once).
enum Row<'a> {
    Json(Value),
    Snapshot(SnapshotFields),
    Invalidation(InvalidationFields<'a>),
}
struct InvalidationFields<'a> {
    binding: Binding,
    cause: orderbook::InvalidationCause,
    detected_us: i64,
    input: u64,
    kind: &'static str,
    next_ordinal: u64,
    reason: &'a str,
}
/// Serializes exactly as the former `json!` invalidation row (keys in
/// serde_json's alphabetical map order).
struct InvalidationRow<'a> {
    fields: &'a InvalidationFields<'a>,
    symbol: &'a str,
}
impl serde::Serialize for InvalidationRow<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let f = self.fields;
        let mut row = serializer.serialize_struct("InvalidationRow", 8)?;
        row.serialize_field("binding", &BindingRow(f.binding))?;
        row.serialize_field("cause", &f.cause)?;
        row.serialize_field("detected_us", &f.detected_us)?;
        row.serialize_field("input", &f.input)?;
        row.serialize_field("kind", f.kind)?;
        row.serialize_field("next_ordinal", &f.next_ordinal)?;
        row.serialize_field("reason", f.reason)?;
        row.serialize_field("symbol", self.symbol)?;
        row.end()
    }
}
/// The first `n` characters, borrowed (`chars().take(n)` without the copy).
fn first_chars(text: &str, n: usize) -> &str {
    text.char_indices()
        .nth(n)
        .map_or(text, |(end, _)| &text[..end])
}
struct SnapshotFields {
    binding: Binding,
    due_us: Option<u128>,
    exchange_us: Option<u64>,
    input: u64,
    ordinal: u64,
    origin: &'static str,
    received_us: i64,
    selection: Option<Value>,
    source_entry: Option<usize>,
    source_event: usize,
    source_ordinal: u64,
    source_received_us: i64,
}
/// Serializes exactly as the former `json!` snapshot row: serde_json's map
/// orders keys alphabetically, so fields are written in that order.
struct SnapshotRow<'a> {
    fields: &'a SnapshotFields,
    symbol: &'a str,
}
impl serde::Serialize for SnapshotRow<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let f = self.fields;
        let mut row = serializer.serialize_struct("SnapshotRow", 14)?;
        row.serialize_field("binding", &BindingRow(f.binding))?;
        row.serialize_field("due_us", &f.due_us)?;
        row.serialize_field("exchange_us", &f.exchange_us)?;
        row.serialize_field("input", &f.input)?;
        row.serialize_field("kind", "snapshot")?;
        row.serialize_field("ordinal", &f.ordinal)?;
        row.serialize_field("origin", f.origin)?;
        row.serialize_field("received_us", &f.received_us)?;
        row.serialize_field("selection", &f.selection)?;
        row.serialize_field("source_entry", &f.source_entry)?;
        row.serialize_field("source_event", &f.source_event)?;
        row.serialize_field("source_ordinal", &f.source_ordinal)?;
        row.serialize_field("source_received_us", &f.source_received_us)?;
        row.serialize_field("symbol", self.symbol)?;
        row.end()
    }
}
/// The `binding()` object, in the same key order, without allocating.
struct BindingRow(Binding);
impl serde::Serialize for BindingRow {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        struct Debug(crate::discovery::AdapterId);
        impl serde::Serialize for Debug {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(&format_args!("{:?}", self.0))
            }
        }
        let b = self.0;
        let mut row = serializer.serialize_struct("Binding", 6)?;
        row.serialize_field("adapter", &Debug(b.session.adapter))?;
        row.serialize_field("book", &b.book.0)?;
        row.serialize_field("generation", &b.session.generation)?;
        row.serialize_field("revision", &b.revision)?;
        row.serialize_field("slot", &b.session.slot)?;
        row.serialize_field("worker", &b.session.worker)?;
        row.end()
    }
}
fn binding(b: Binding) -> Value {
    json!({"book":b.book.0, "slot":b.session.slot, "revision":b.revision, "worker":b.session.worker, "generation":b.session.generation, "adapter":format!("{:?}", b.session.adapter)})
}
fn json_line(buffer: &mut [u8], value: &impl serde::Serialize) -> Result<usize> {
    let mut cursor = std::io::Cursor::new(buffer);
    serde_json::to_writer(&mut cursor, value)?;
    cursor.write_all(b"\n")?;
    Ok(cursor.position() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{discovery::AdapterId, orderbook::BookId, websocket::ingress::Session};

    /// The former row construction, kept verbatim as the byte oracle.
    fn oracle(f: &SnapshotFields, symbol: &str) -> Value {
        json!({"kind":"snapshot", "ordinal":f.ordinal, "symbol":symbol, "binding":binding(f.binding), "origin":f.origin, "selection":f.selection, "input":f.input, "source_ordinal":f.source_ordinal, "source_event":f.source_event, "source_entry":f.source_entry, "received_us":f.received_us, "source_received_us":f.source_received_us, "exchange_us":f.exchange_us, "due_us":f.due_us})
    }

    #[test]
    fn typed_snapshot_row_is_byte_identical_to_the_json_row() {
        let symbols = [
            "polymarket:v1:71321045679252212594626385532706912750332728571942532289631379312455583992563",
            "polymarket:v2:0",
            "",
            "quote\" back\\slash /",
            "control \u{0} \u{1f} \n \t \r",
            "unicode é ✓ 🚀 \u{2028}",
        ];
        let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let (mut a, mut b) = (vec![0; INTEGRITY_BYTES], vec![0; INTEGRITY_BYTES]);
        for case in 0..5000 {
            let pick = |r: u64, edge: [u64; 4]| {
                if r.is_multiple_of(3) {
                    edge[(r as usize / 3) % 4]
                } else {
                    r
                }
            };
            let r = next();
            let fields = SnapshotFields {
                binding: Binding {
                    book: BookId(pick(next(), [0, 1, u32::MAX as u64, 7]) as u32),
                    revision: pick(next(), [0, 1, u64::MAX, 42]),
                    session: Session {
                        worker: pick(next(), [0, 1, usize::MAX as u64, 3]) as usize,
                        generation: pick(next(), [0, 1, u64::MAX, 9]),
                        adapter: if r & 1 == 0 { AdapterId::PolymarketV1 } else { AdapterId::PolymarketV2 },
                        slot: pick(next(), [0, 1, u16::MAX as u64, 2]) as u16,
                    },
                },
                // The former `json!` row panicked above u64::MAX (serde_json Value has
                // no u128); microseconds since start cannot get there. The oracle
                // covers the whole range it supported.
                due_us: (r & 2 != 0).then(|| u128::from(next() >> (r % 64))),
                exchange_us: (r & 4 != 0).then(|| pick(next(), [0, 1, u64::MAX, 5])),
                input: next(),
                ordinal: pick(next(), [0, 1, u64::MAX, 11]),
                origin: ["exchange", "selection", "periodic"][case % 3],
                received_us: pick(next(), [0, i64::MAX as u64, i64::MIN as u64, u64::MAX]) as i64,
                selection: (r & 8 != 0).then(|| {
                    json!({"previous":(r & 16 != 0).then(|| binding(Binding {
                        book: BookId(3), revision: 4,
                        session: Session { worker: 5, generation: 6, adapter: AdapterId::PolymarketV1, slot: 1 },
                    })), "reason":"failover \"quoted\"", "freshness":r % 7, "receive_age_us":(r & 32 != 0).then_some(u128::from(u64::MAX))})
                }),
                source_entry: (r & 64 != 0).then(|| pick(next(), [0, 1, usize::MAX as u64, 13]) as usize),
                source_event: pick(next(), [0, 1, usize::MAX as u64, 17]) as usize,
                source_ordinal: next(),
                source_received_us: next() as i64,
            };
            let symbol = symbols[case % symbols.len()];
            let n = json_line(
                &mut a,
                &SnapshotRow {
                    fields: &fields,
                    symbol,
                },
            )
            .unwrap();
            let m = json_line(&mut b, &oracle(&fields, symbol)).unwrap();
            assert_eq!(
                a[..n],
                b[..m],
                "case {case}: {}",
                String::from_utf8_lossy(&b[..m])
            );
        }
    }

    #[test]
    fn typed_invalidation_row_is_byte_identical_to_the_json_row() {
        use crate::orderbook::InvalidationCause::*;
        let long = "é✓🚀\"\\\n".repeat(40);
        let reasons = [
            "",
            "remote close",
            "quote\" back\\slash",
            long.as_str(),
            &"x".repeat(127),
            &"x".repeat(129),
        ];
        let causes = [
            SourceEnded,
            InvalidData,
            AssignmentReplaced,
            Retired,
            PolicySuspension,
            ResourceLimit,
        ];
        let (mut a, mut b) = (vec![0; INTEGRITY_BYTES], vec![0; INTEGRITY_BYTES]);
        for case in 0..600 {
            let reason = reasons[case % reasons.len()];
            let selected = case % 2 == 0;
            let fields = InvalidationFields {
                binding: Binding {
                    book: BookId(case as u32 * 7919),
                    revision: [0, 1, u64::MAX][case % 3],
                    session: Session {
                        worker: case,
                        generation: [0, 9, u64::MAX][case % 3],
                        adapter: if case % 4 < 2 {
                            AdapterId::PolymarketV1
                        } else {
                            AdapterId::PolymarketV2
                        },
                        slot: (case % 2) as u16,
                    },
                },
                cause: causes[case % causes.len()],
                detected_us: [0, -1, i64::MAX, 1_761_700_000_000_000][case % 4],
                input: case as u64 * 1_000_003,
                kind: if selected {
                    "unavailable"
                } else {
                    "replica_unavailable"
                },
                next_ordinal: [0, u64::MAX, 42][case % 3],
                reason: first_chars(reason, 128),
            };
            let symbol = ["polymarket:v1:00123", "", "control \u{1}\n"][case % 3];
            // The former construction, verbatim except for its fields' source.
            let oracle = json!({"kind":fields.kind, "next_ordinal":fields.next_ordinal, "symbol":symbol, "binding":binding(fields.binding), "input":fields.input, "cause":fields.cause, "reason":reason.chars().take(128).collect::<String>(), "detected_us":fields.detected_us});
            let n = json_line(
                &mut a,
                &InvalidationRow {
                    fields: &fields,
                    symbol,
                },
            )
            .unwrap();
            let m = json_line(&mut b, &oracle).unwrap();
            assert_eq!(
                a[..n],
                b[..m],
                "case {case}: {}",
                String::from_utf8_lossy(&b[..m])
            );
        }
        assert_eq!(first_chars("ab", 128), "ab");
        assert_eq!(first_chars("é✓🚀x", 3), "é✓🚀");
    }

    #[test]
    fn oversized_typed_row_fails_like_the_json_row() {
        let fields = SnapshotFields {
            binding: Binding {
                book: BookId(1),
                revision: 1,
                session: Session {
                    worker: 0,
                    generation: 0,
                    adapter: AdapterId::PolymarketV1,
                    slot: 0,
                },
            },
            due_us: None,
            exchange_us: Some(1),
            input: 0,
            ordinal: 0,
            origin: "periodic",
            received_us: 0,
            selection: None,
            source_entry: None,
            source_event: 0,
            source_ordinal: 0,
            source_received_us: 0,
        };
        let symbol = "x".repeat(INTEGRITY_BYTES);
        let mut slot = vec![0; INTEGRITY_BYTES];
        assert!(
            json_line(
                &mut slot,
                &SnapshotRow {
                    fields: &fields,
                    symbol: &symbol
                }
            )
            .is_err()
        );
        assert!(json_line(&mut slot, &oracle(&fields, &symbol)).is_err());
    }
}
