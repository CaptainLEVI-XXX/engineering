//! Single-owner source replicas. No network I/O or SBE encoding in mutation.
mod book;
mod store;
use crate::{
    Side,
    discovery::AdapterId,
    market_data::{Limits, Price, Quantity},
    websocket::ingress::Session,
};
pub use book::Book;
use chrono::{DateTime, Utc};
use std::time::Duration;
pub use store::Processor;

#[derive(Clone, Debug)]
pub struct Config {
    pub wire: Limits,
    pub instruments: usize,
    pub replicas: usize,
    pub sessions: usize,
    pub levels_per_side: usize,
    /// Includes replacement construction peak, not just committed levels.
    pub total_levels: usize,
    pub initialization_timeout: Duration,
    pub recovery_timeout: Duration,
    pub recovery_attempts: u32,
    /// Enabled only by a recording consumer; scheduling stays with the book owner.
    pub snapshot_interval: Option<Duration>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            wire: Limits::default(),
            instruments: 1_000_000,
            replicas: 2_000_000,
            sessions: 2048,
            levels_per_side: 65535,
            total_levels: 10_000_000,
            initialization_timeout: Duration::from_secs(30),
            recovery_timeout: Duration::from_secs(120),
            recovery_attempts: 3,
            snapshot_interval: None,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct BookId(pub u32);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Binding {
    pub book: BookId,
    pub slot: u16,
    pub revision: u64,
    pub session: Session,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Validity {
    AwaitingSnapshot,
    Live,
    Stale,
    Inactive,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cursor {
    pub session: Session,
    pub ordinal: u64,
    pub event: usize,
    pub entry: Option<usize>,
}
#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    pub cursor: Cursor,
    pub received_at: DateTime<Utc>,
    /// Polymarket requires this; a future adapter must not invent a missing time.
    pub exchange_us: Option<u64>,
}
#[derive(Debug)]
pub struct Replica {
    pub binding: Binding,
    pub state: Validity,
    pub book: Book,
    pub last: Option<Metadata>,
    pub tick: Option<Price>,
    pub applied: u64,
    pub observed: u64,
    pub last_received_at: Option<DateTime<Utc>>,
    pub(crate) unavailable_since: Option<Duration>,
    pub(crate) requested_at: Duration,
    pub(crate) attempts: u32,
}
pub struct Observation<'a> {
    pub asset: &'a str,
    pub adapter: AdapterId,
    /// Position in the processor's data/control/timer/selection input trace.
    pub input: u64,
    pub binding: Binding,
    pub metadata: Option<Metadata>,
    pub kind: ObservationKind<'a>,
}
#[derive(Clone, Copy, Debug)]
pub enum SnapshotOrigin {
    Exchange,
    Selection { at: DateTime<Utc> },
    Periodic { at: DateTime<Utc>, due: Duration },
}
#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationCause {
    SourceEnded,
    InvalidData,
    AssignmentReplaced,
    Retired,
}
pub enum ObservationKind<'a> {
    Snapshot {
        book: &'a Book,
        origin: SnapshotOrigin,
    },
    Level {
        side: Side,
        price: Price,
        quantity: Quantity,
    },
    Invalidated {
        reason: &'a str,
        cause: InvalidationCause,
        /// Selected binding before this transition; None for standby-only loss.
        output_binding: Option<Binding>,
    },
}
#[derive(Debug, Default)]
pub struct Stats {
    pub messages: u64,
    pub events: u64,
    pub snapshots: u64,
    pub changes: u64,
    pub selected_changes: u64,
    pub rejected: u64,
    pub ignored: u64,
    pub invalidations: u64,
    pub periodic_snapshots: u64,
    pub late_snapshots: u64,
    pub max_snapshot_lateness: Duration,
    pub snapshots_due_at_cutoff: usize,
}
