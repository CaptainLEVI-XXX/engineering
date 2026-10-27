//! Component 3: bounded wire interpretation, without mutable book state.
mod decimal;
mod polymarket;
use crate::Side;
pub use decimal::{Price, Quantity, parse_scaled};
pub use polymarket::{decode, envelope};
use std::borrow::Cow;

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
    pub level: Level,
}

#[derive(Debug)]
pub enum Event<'a> {
    Snapshot {
        asset: Cow<'a, str>,
        exchange_us: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Changes {
        exchange_us: u64,
        changes: Vec<Change<'a>>,
    },
    Tick {
        asset: Cow<'a, str>,
        exchange_us: u64,
        tick: Price,
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
