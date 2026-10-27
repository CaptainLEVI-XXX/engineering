//! Whole SBE records in zero-padded logical chunks, independent of physical I/O buffering.
use super::dtos::SbeEncode;
use anyhow::{Result, ensure};
use std::{fs::File, io::Write, path::Path};

const HEADER_LENGTH: usize = 8;
static ZEROES: [u8; 65536] = [0; 65536];

/// Caller-owned buffering is flushed explicitly. Any error is terminal; dropping
/// this helper performs no I/O (a buffered destination must also be disarmed).
pub struct AlignedSbeWriter<W: Write = File> {
    destination: W,
    scratch: Vec<u8>,
    chunk: usize,
    offset: usize,
    failed: bool,
    finished: bool,
}
impl AlignedSbeWriter<File> {
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> Result<Self> {
        ensure!(
            chunk > HEADER_LENGTH,
            "chunk must leave room for data and header"
        );
        Self::new(File::create(path)?, chunk)
    }
}
impl<W: Write> AlignedSbeWriter<W> {
    pub fn new(destination: W, chunk: usize) -> Result<Self> {
        ensure!(
            chunk > HEADER_LENGTH,
            "chunk must leave room for data and header"
        );
        Ok(Self {
            destination,
            scratch: Vec::new(),
            chunk,
            offset: 0,
            failed: false,
            finished: false,
        })
    }
    fn writable(&self) -> Result<()> {
        ensure!(
            !self.failed && !self.finished,
            "SBE writer failed or finished"
        );
        Ok(())
    }
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> Result<()> {
        self.writable()?;
        let result = (|| {
            let len = message.encoded_length_with_header()?;
            ensure!(
                len > 0 && len <= self.chunk - HEADER_LENGTH,
                "SBE message does not fit in a chunk"
            );
            self.scratch.resize(len, 0);
            ensure!(
                message.encode(&mut self.scratch)? == len,
                "SBE encoded length mismatch"
            );
            let buffer = std::mem::take(&mut self.scratch);
            let result = self.write_encoded(&buffer);
            self.scratch = buffer;
            result
        })();
        self.failed |= result.is_err();
        result
    }
    /// Trusted complete record from the generated in-process encoder.
    pub(crate) fn write_encoded(&mut self, record: &[u8]) -> Result<()> {
        self.writable()?;
        let result = (|| {
            ensure!(
                !record.is_empty() && record.len() <= self.chunk - HEADER_LENGTH,
                "SBE message does not fit in a chunk"
            );
            if record.len() > self.chunk - HEADER_LENGTH - self.offset {
                self.pad()?;
            }
            self.destination.write_all(record)?;
            self.offset += record.len();
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    fn pad(&mut self) -> Result<()> {
        if self.offset != 0 {
            while self.offset < self.chunk {
                let n = (self.chunk - self.offset).min(ZEROES.len());
                self.destination.write_all(&ZEROES[..n])?;
                self.offset += n;
            }
            self.offset = 0;
        }
        Ok(())
    }
    /// Publish physical buffers without padding the current logical chunk.
    pub fn flush_bytes(&mut self) -> Result<()> {
        self.writable()?;
        let result = self.destination.flush().map_err(Into::into);
        self.failed |= result.is_err();
        result
    }
    pub fn finish_chunk(&mut self) -> Result<()> {
        self.writable()?;
        let result = self
            .pad()
            .and_then(|()| self.destination.flush().map_err(Into::into));
        self.failed |= result.is_err();
        result
    }
    /// Compatibility with the original helper: pad and physically flush.
    pub fn flush(&mut self) -> Result<()> {
        self.finish_chunk()
    }
    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finish_chunk()?;
        self.finished = true;
        Ok(())
    }
    pub fn into_inner_unflushed(self) -> W {
        self.destination
    }
    pub(crate) fn destination(&self) -> &W {
        &self.destination
    }
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }
}
