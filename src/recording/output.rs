use super::*;
use crate::util::{
    AlignedSbeWriter,
    dtos::challenge_orderbook::{SCHEMA_ID, SCHEMA_VERSION},
};
use std::io::{self, BufWriter};

pub(super) enum Kind {
    Sbe,
    Integrity,
}
pub(super) struct Entry {
    pub end: usize,
    pub kind: Kind,
}
pub(super) struct Batch {
    pub bytes: Vec<u8>,
    pub entries: Vec<Entry>,
    pub first_record: u64,
    pub created_at: Instant,
    pub publish_at: Instant,
}
#[derive(serde::Serialize)]
pub(super) struct Outcome {
    pub encoded: u64,
    pub handed: u64,
    pub input_complete: bool,
    /// Known C5 rejection position; upstream-only failures have no invented cursor.
    pub failure_input: Option<u64>,
    pub symbol_bytes: usize,
    pub late_snapshots: u64,
    pub max_snapshot_lateness_us: u128,
    pub snapshots_due_at_cutoff: usize,
}
#[derive(Debug, Default, serde::Serialize)]
pub struct Report {
    pub records_written: u64,
    pub records_confirmed: u64,
    pub raw_bytes: u64,
    pub complete_chunks: u64,
    pub reader_compatible_bytes: u64,
    pub pool_bytes: usize,
    pub batches: u64,
    pub max_batch_bytes: usize,
    /// Producer batch creation through dequeue, including publication delay.
    pub max_batch_age_us: u128,
    pub error: Option<String>,
}
pub struct Output {
    pub(super) config: Config,
    pub(super) ready: Receiver<Batch>,
    pub(super) recycle: Sender<Batch>,
    pub(super) producer: Receiver<Outcome>,
    pub(super) _alive: Sender<()>,
    pub(super) pool_bytes: usize,
}
struct Progress<W> {
    inner: W,
    bytes: u64,
}
impl<W: Write> Write for Progress<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(bytes)?;
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
/// Disarm even during unwind: BufWriter's destructor must never retry a prefix.
struct Buffered<W: Write>(Option<BufWriter<Progress<W>>>);
impl<W: Write> Buffered<W> {
    fn new(inner: W, size: usize) -> Self {
        Self(Some(BufWriter::with_capacity(
            size,
            Progress { inner, bytes: 0 },
        )))
    }
    fn bytes(&self) -> u64 {
        self.0.as_ref().unwrap().get_ref().bytes
    }
}
impl<W: Write> Write for Buffered<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.as_mut().unwrap().write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.as_mut().unwrap().flush()
    }
}
impl<W: Write> Drop for Buffered<W> {
    fn drop(&mut self) {
        let _ = self.0.take().unwrap().into_parts();
    }
}
impl Output {
    /// Call `ready` only after the startup sidecar flush. `approval` conveys the
    /// supervisor's intake/drain outcome; channel closure is never success.
    pub fn run<W: Write, S: Write>(
        self,
        stdout: W,
        sidecar: S,
        label: &str,
        ready: impl FnOnce(),
        approval: Receiver<bool>,
    ) -> Report {
        let mut writer =
            AlignedSbeWriter::new(Buffered::new(stdout, self.config.buffer_bytes), CHUNK_BYTES)
                .unwrap();
        let mut sidecar = Buffered::new(sidecar, self.config.buffer_bytes);
        let mut report = Report {
            pool_bytes: self.pool_bytes,
            ..Report::default()
        };
        let mut scratch = [0; INTEGRITY_BYTES];
        let mut sidecar_ok = true;
        let mut outcome = None;
        let result: Result<()> = (|| {
            let start = json!({"kind":"run_start", "format_version":1, "label":label, "pid":std::process::id(), "start_us":chrono::Utc::now().timestamp_micros(), "schema_id":SCHEMA_ID, "schema_version":SCHEMA_VERSION, "chunk_bytes":CHUNK_BYTES, "symbol":"polymarket:v{1|2}:<exact-id>", "config":self.config, "pool_bytes":self.pool_bytes, "timestamp_semantics":"UTC microseconds; generated snapshots use capture time; file order authoritative", "cadence":"60s after authoritative snapshot; actual lateness reported", "durability":"flush is not fsync or consumer acknowledgement"});
            let n = json_line(&mut scratch, &start)?;
            if let Err(e) = sidecar
                .write_all(&scratch[..n])
                .and_then(|()| sidecar.flush())
            {
                sidecar_ok = false;
                return Err(e.into());
            }
            ready();
            let mut last_flush = Instant::now();
            let mut chunk_started: Option<Instant> = None;
            loop {
                let now = Instant::now();
                if chunk_started.is_some_and(|t| now.duration_since(t) >= self.config.chunk_age) {
                    writer.finish_chunk()?;
                    report.records_confirmed = report.records_written;
                    chunk_started = None;
                }
                if now.duration_since(last_flush) >= self.config.flush_age {
                    writer.flush_bytes()?;
                    report.records_confirmed = report.records_written;
                    if let Err(e) = sidecar.flush() {
                        sidecar_ok = false;
                        return Err(e.into());
                    }
                    last_flush = Instant::now();
                }
                let deadline = chunk_started
                    .map(|t| t + self.config.chunk_age)
                    .unwrap_or(last_flush + self.config.flush_age)
                    .min(last_flush + self.config.flush_age);
                let mut batch = match self
                    .ready
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                {
                    Ok(batch) => batch,
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                };
                ensure!(
                    batch.first_record == report.records_written,
                    "output ordinal discontinuity"
                );
                report.batches += 1;
                report.max_batch_bytes = report.max_batch_bytes.max(batch.bytes.len());
                report.max_batch_age_us = report
                    .max_batch_age_us
                    .max(batch.created_at.elapsed().as_micros());
                let mut begin = 0;
                for entry in &batch.entries {
                    match entry.kind {
                        Kind::Sbe => {
                            let rotate = writer.offset() != 0
                                && entry.end - begin > CHUNK_BYTES - 8 - writer.offset();
                            writer.write_encoded(&batch.bytes[begin..entry.end])?;
                            report.records_written += 1;
                            if rotate || chunk_started.is_none() {
                                chunk_started = Some(Instant::now());
                            }
                        }
                        Kind::Integrity => {
                            if let Err(e) = sidecar.write_all(&batch.bytes[begin..entry.end]) {
                                sidecar_ok = false;
                                return Err(e.into());
                            }
                        }
                    }
                    begin = entry.end;
                    let now = Instant::now();
                    if chunk_started.is_some_and(|t| now.duration_since(t) >= self.config.chunk_age)
                    {
                        writer.finish_chunk()?;
                        report.records_confirmed = report.records_written;
                        chunk_started = None;
                    }
                    if now.duration_since(last_flush) >= self.config.flush_age {
                        writer.flush_bytes()?;
                        report.records_confirmed = report.records_written;
                        if let Err(e) = sidecar.flush() {
                            sidecar_ok = false;
                            return Err(e.into());
                        }
                        last_flush = Instant::now();
                    }
                }
                batch.bytes.clear();
                batch.entries.clear();
                // Producer may have completed; a disconnected recycle path is normal.
                let _ = self.recycle.try_send(batch);
            }
            outcome = Some(self.producer.recv().context("missing producer outcome")?);
            writer.finish()?;
            report.records_confirmed = report.records_written;
            ensure!(
                approval.recv().unwrap_or(false),
                "upstream intake/drain incomplete"
            );
            ensure!(
                outcome.as_ref().unwrap().input_complete
                    && outcome.as_ref().unwrap().failure_input.is_none(),
                "producer incomplete"
            );
            Ok(())
        })();
        report.raw_bytes = writer.destination().bytes();
        report.complete_chunks = report.raw_bytes / CHUNK_BYTES as u64;
        report.reader_compatible_bytes = report.complete_chunks * CHUNK_BYTES as u64;
        if let Err(error) = result {
            report.error = Some(error.to_string());
        }
        // Notify the processing owner before a best-effort failure footer can
        // itself block. It checks this channel even during idle feed periods.
        drop(self._alive);
        // A stdout error does not prevent a best-effort failure footer on a
        // healthy sidecar. A sidecar error is never retried.
        if sidecar_ok {
            let footer = json!({"kind":"run_end", "success":report.error.is_none(), "output":report, "producer":outcome});
            let result = json_line(&mut scratch, &footer).and_then(|n| {
                sidecar.write_all(&scratch[..n])?;
                sidecar.flush()?;
                Ok(())
            });
            if let Err(e) = result {
                report
                    .error
                    .get_or_insert_with(|| format!("integrity footer failed: {e}"));
            }
        }
        report
    }
}
