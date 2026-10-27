//! Opt-in discovery-only JSONL evidence. No retained observation ledger.
//!
//! The byte cap includes all rows and framing. A partial final row, missing
//! `complete` row, or unsuccessful returned status means incomplete evidence.
//! Writes are buffered (64 KiB), flushed at page/sweep boundaries and on finish.
//! Flush is output visibility, not a durability guarantee.
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::time::Instant;

#[derive(Clone, Debug, Serialize)]
pub struct EvidenceStatus {
    pub complete: bool,
    pub error: Option<String>,
    /// Bytes accepted by the bounded writer, possibly still buffered on error.
    pub bytes: u64,
    pub max_bytes: u64,
}

struct Limited<W> {
    inner: W,
    bytes: u64,
    max_bytes: u64,
}

impl<W: Write> Write for Limited<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let available = self.max_bytes.saturating_sub(self.bytes);
        if available == 0 {
            return Err(io::Error::other("discovery evidence byte cap exceeded"));
        }
        let len = bytes
            .len()
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        let written = self.inner.write(&bytes[..len])?;
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(super) struct Evidence<W = BufWriter<File>> {
    writer: Limited<W>,
    started: Instant,
    sequence: u64,
    error: Option<String>,
    complete: bool,
}

impl Evidence {
    pub(super) fn create(path: &Path, max_bytes: u64) -> io::Result<Self> {
        if max_bytes == 0 {
            return Err(io::Error::other(
                "discovery evidence byte cap must be positive",
            ));
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self::from_writer(
            BufWriter::with_capacity(64 * 1024, file),
            max_bytes,
        ))
    }
}

impl<W: Write> Evidence<W> {
    fn from_writer(writer: W, max_bytes: u64) -> Self {
        Self {
            writer: Limited {
                inner: writer,
                bytes: 0,
                max_bytes,
            },
            started: Instant::now(),
            sequence: 0,
            error: None,
            complete: false,
        }
    }

    pub(super) fn status(&self) -> EvidenceStatus {
        EvidenceStatus {
            complete: self.complete,
            error: self.error.clone(),
            bytes: self.writer.bytes,
            max_bytes: self.writer.max_bytes,
        }
    }

    pub(super) fn failed(&self) -> bool {
        self.error.is_some()
    }

    pub(super) fn emit(&mut self, event: &impl Serialize) {
        if self.error.is_some() {
            return;
        }
        if self.complete {
            self.complete = false;
            self.error = Some("discovery evidence already finalized".into());
            return;
        }
        #[derive(Serialize)]
        struct Row<'a, T> {
            sequence: u64,
            elapsed_us: u128,
            observed_unix_us: u128,
            event: &'a T,
        }
        let row = Row {
            sequence: self.sequence,
            elapsed_us: self.started.elapsed().as_micros(),
            observed_unix_us: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros(),
            event,
        };
        let result = serde_json::to_writer(&mut self.writer, &row)
            .map_err(io::Error::other)
            .and_then(|()| self.writer.write_all(b"\n"));
        match result {
            Ok(()) => self.sequence += 1,
            Err(error) => self.error = Some(error.to_string()),
        }
    }

    pub(super) fn flush(&mut self) {
        if let Err(error) = self.writer.flush() {
            self.error.get_or_insert_with(|| error.to_string());
            self.complete = false;
        }
    }

    pub(super) fn finish(&mut self) -> EvidenceStatus {
        self.flush();
        if self.error.is_none() {
            self.emit(&serde_json::json!({"kind":"complete"}));
            self.flush();
            self.complete = self.error.is_none();
        }
        self.status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Faulty {
        bytes: Vec<u8>,
        fail_write: bool,
        fail_flush: bool,
    }
    impl Write for Faulty {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail_write {
                return Err(io::Error::other("injected write failure"));
            }
            // Exercise write_all handling of short writes as well.
            let len = bytes.len().min(3);
            self.bytes.extend_from_slice(&bytes[..len]);
            Ok(len)
        }
        fn flush(&mut self) -> io::Result<()> {
            if self.fail_flush {
                Err(io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn capped_short_writes_and_failures_cannot_claim_complete() {
        for (cap, fail_write, fail_flush, complete) in [
            (4096, false, false, true),
            (20, false, false, false),
            (4096, true, false, false),
            (4096, false, true, false),
        ] {
            let mut evidence = Evidence::from_writer(
                Faulty {
                    bytes: Vec::new(),
                    fail_write,
                    fail_flush,
                },
                cap,
            );
            evidence.emit(&serde_json::json!({"kind":"test"}));
            let status = evidence.finish();
            assert_eq!(status.complete, complete);
            assert_eq!(status.error.is_none(), complete);
            assert!(evidence.writer.inner.bytes.len() as u64 <= cap);
            if complete {
                let lines = String::from_utf8(evidence.writer.inner.bytes).unwrap();
                let rows: Vec<serde_json::Value> = lines
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                assert_eq!(rows[1]["event"]["kind"], "complete");
            }
        }
    }
}
