//! Component 3: bounded wire interpretation, without mutable book state.
mod decimal;
mod polymarket;
use crate::Side;
pub use decimal::{Price, Quantity, parse_scaled};
pub use polymarket::{decode, envelope};
use std::borrow::Cow;

/// Envelope syntax failures invalidate the source; sink failures are terminal.
/// Keeping them distinct prevents accidentally treating output failure as bad input.
#[derive(Debug)]
pub enum VisitError {
    Envelope(anyhow::Error),
    Sink(anyhow::Error),
}
pub trait SessionDecoder {
    fn visit<'a>(
        &mut self,
        text: &'a str,
        limits: &Limits,
        emit: &mut dyn FnMut(Result<Event<'a>, DecodeError>) -> anyhow::Result<()>,
    ) -> Result<(), VisitError>;
}
pub enum Decoder {
    Polymarket,
}
impl Decoder {
    pub fn new(adapter: crate::discovery::AdapterId) -> Self {
        match adapter {
            crate::discovery::AdapterId::PolymarketV1
            | crate::discovery::AdapterId::PolymarketV2 => Self::Polymarket,
        }
    }
}
impl SessionDecoder for Decoder {
    fn visit<'a>(
        &mut self,
        text: &'a str,
        limits: &Limits,
        emit: &mut dyn FnMut(Result<Event<'a>, DecodeError>) -> anyhow::Result<()>,
    ) -> Result<(), VisitError> {
        match self {
            Self::Polymarket => {
                for raw in envelope(text, limits).map_err(VisitError::Envelope)? {
                    emit(decode(raw, limits)).map_err(VisitError::Sink)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub message_bytes: usize,
    pub events: usize,
    pub entries: usize,
    pub identity_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            message_bytes: 2 << 20,
            events: 4096,
            entries: 65535,
            identity_bytes: 256,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
pub struct Level {
    pub price: Price,
    #[serde(rename = "size")]
    pub quantity: Quantity,
}

#[derive(Debug)]
pub struct Change<'a> {
    pub asset: Cow<'a, str>,
    pub side: Side,
    pub price: Price,
    pub quantity: QuantityChange,
}

#[derive(Clone, Copy, Debug)]
pub enum QuantityChange {
    Set(Quantity),
    Add(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleKind {
    NewMarket,
    Resolved,
}

#[derive(Debug)]
pub enum Event<'a> {
    Snapshot {
        asset: Cow<'a, str>,
        exchange_us: Option<u64>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Changes {
        exchange_us: Option<u64>,
        changes: Vec<Change<'a>>,
    },
    Tick {
        asset: Cow<'a, str>,
        exchange_us: Option<u64>,
        tick: Price,
    },
    Lifecycle {
        kind: LifecycleKind,
        market_id: Option<Cow<'a, str>>,
        adapter: Option<crate::discovery::AdapterId>,
    },
    NonBook,
}

#[derive(Debug)]
pub struct DecodeError {
    /// None means the affected source scope cannot be safely narrowed.
    pub assets: Option<Vec<String>>,
    pub reason: String,
    pub resource: bool,
}
impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}
impl std::error::Error for DecodeError {}
