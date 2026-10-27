//! Selected book recording. Book/source ownership stays in `orderbook`.
pub mod encode;
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
}
impl Default for Config {
    fn default() -> Self {
        Self {
            batches: 16,
            batch_bytes: 4 << 20,
            entries: 4096,
            publish_bytes: 256 << 10,
            publish_age: Duration::from_millis(5),
            buffer_bytes: 256 << 10,
            flush_age: Duration::from_millis(5),
            chunk_age: Duration::from_secs(60),
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
        if let Some(batch) = self.current.take() {
            if batch.entries.is_empty() {
                self.current = Some(batch);
                return Ok(());
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
                Some(json!({"kind":"standby_baseline",
                "input":o.input,"next_ordinal":self.ordinal,"symbol":symbol,"binding":binding(o.binding),
                "source_ordinal":o.metadata.map(|m| m.cursor.ordinal),"source_event":o.metadata.map(|m| m.cursor.event)})),
            ),
            ObservationKind::Snapshot { book, origin } => {
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
                    Some(
                        json!({"kind":"snapshot", "ordinal":self.ordinal, "symbol":symbol, "binding":binding(o.binding), "origin":name, "selection":selection, "input":o.input, "source_ordinal":m.cursor.ordinal, "source_event":m.cursor.event, "source_entry":m.cursor.entry, "received_us":at.timestamp_micros(), "source_received_us":m.received_at.timestamp_micros(), "exchange_us":m.exchange_us, "due_us":due}),
                    ),
                )
            }
            ObservationKind::Level { .. } => (45 + symbol.len(), None),
            ObservationKind::Invalidated {
                reason,
                cause,
                output_binding,
            } => (
                0,
                Some(
                    json!({"kind":if output_binding.is_some() {"unavailable"} else {"replica_unavailable"}, "next_ordinal":self.ordinal, "symbol":symbol, "binding":binding(output_binding.unwrap_or(o.binding)), "input":o.input, "cause":cause, "reason":reason.chars().take(128).collect::<String>(), "detected_us":chrono::Utc::now().timestamp_micros()}),
                ),
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
            let n = if let Some(value) = &metadata {
                json_line(&mut batch.bytes[begin + size..], value)?
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
            });
        }
        if n > 0 {
            batch.entries.push(Entry {
                end: begin + size + n,
                kind: Kind::Integrity,
            });
        }
        self.ordinal = next;
        if batch.bytes.len() >= self.config.publish_bytes
            || batch.entries.len() == self.config.entries
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
        self.input = input;
        let result = (|| {
            self.reserve(INTEGRITY_BYTES, 1)?;
            let batch = self.current.as_mut().unwrap();
            let start = batch.bytes.len();
            batch.bytes.resize(start + INTEGRITY_BYTES, 0);
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
                    });
                }
                Err(error) => {
                    batch.bytes.truncate(start);
                    return Err(error);
                }
            }
            if batch.bytes.len() >= self.config.publish_bytes
                || batch.entries.len() == self.config.entries
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
    /// Success here is handoff only; `Output::run` establishes physical completion.
    pub fn finish(mut self, input_result: Result<()>) -> Result<()> {
        let publication = self.publish();
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
fn binding(b: Binding) -> Value {
    json!({"book":b.book.0, "slot":b.session.slot, "revision":b.revision, "worker":b.session.worker, "generation":b.session.generation, "adapter":format!("{:?}", b.session.adapter)})
}
fn json_line(buffer: &mut [u8], value: &impl serde::Serialize) -> Result<usize> {
    let mut cursor = std::io::Cursor::new(buffer);
    serde_json::to_writer(&mut cursor, value)?;
    cursor.write_all(b"\n")?;
    Ok(cursor.position() as usize)
}
