//! Bounded, ordered raw-message handoff. No financial decoding or persistence.
use crate::discovery::AdapterId;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, Sender as QueueSender, bounded};
use std::{mem::size_of, sync::Arc, time::Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::Utf8Bytes;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Session {
    pub worker: usize,
    pub generation: u64,
    pub adapter: AdapterId,
    pub slot: u16,
}

#[derive(Debug)]
pub enum Message {
    Started(Session),
    ReconcileBegin {
        revision: u64,
        chunks: u32,
    },
    /// Global ownership fence, not a successfully sent subscription request.
    DesiredChunk {
        adapter: AdapterId,
        revision: u64,
        index: u32,
        add: Vec<String>,
        remove: Vec<String>,
    },
    ReconcileComplete {
        revision: u64,
    },
    /// Successfully sent requests, NOT exchange acknowledgements. Initial and
    /// incremental requests precede subsequent data in this session's stream.
    SubscriptionsRequested {
        session: Session,
        /// One assignment revision per ID, in add-then-remove order.
        revisions: Vec<u64>,
        add: Vec<String>,
        remove: Vec<String>,
    },
    /// All currently desired requests and their controls have been written.
    /// This is not book initialization; only the book owner can confirm that.
    RequestsComplete {
        session: Session,
    },
    Data {
        session: Session,
        ordinal: u64,
        received_at: DateTime<Utc>,
        received_mono: Instant,
        payload: Utf8Bytes,
    },
    Ended {
        session: Session,
        reason: String,
    },
}

pub struct Envelope {
    pub message: Message,
    pub queued_at: Instant,
    _budget: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct Sender {
    queue: QueueSender<Envelope>,
    budget: Arc<Semaphore>,
    bytes: usize,
}

/// Bounds outstanding envelopes and accounted bytes, not total RSS. Text uses
/// logical length: Utf8Bytes does not expose its backing allocation's capacity.
/// Keep the envelope until processing finishes to retain its budget permit.
pub fn channel(items: usize, bytes: usize) -> Result<(Sender, Receiver<Envelope>)> {
    ensure!(
        items > 0 && bytes > 0 && bytes <= u32::MAX as usize,
        "invalid ingress budget"
    );
    let (queue, receiver) = bounded(items);
    Ok((
        Sender {
            queue,
            budget: Arc::new(Semaphore::new(bytes)),
            bytes,
        },
        receiver,
    ))
}

impl Message {
    pub(crate) fn accounted_bytes(&self) -> Result<usize> {
        fn strings(values: &Vec<String>) -> Result<usize> {
            let initial = values
                .capacity()
                .checked_mul(size_of::<String>())
                .context("ingress vector accounting overflow")?;
            values.iter().try_fold(initial, |bytes, value| {
                bytes
                    .checked_add(value.capacity())
                    .context("ingress string accounting overflow")
            })
        }
        let dynamic = match self {
            Message::Data { payload, .. } => payload.len(),
            Message::DesiredChunk { add, remove, .. } => strings(add)?
                .checked_add(strings(remove)?)
                .context("ingress desired accounting overflow")?,
            Message::SubscriptionsRequested {
                add,
                remove,
                revisions,
                ..
            } => {
                let ids = strings(add)?
                    .checked_add(strings(remove)?)
                    .context("ingress request ID accounting overflow")?;
                revisions
                    .capacity()
                    .checked_mul(size_of::<u64>())
                    .and_then(|bytes| bytes.checked_add(ids))
                    .context("ingress request accounting overflow")?
            }
            Message::Ended { reason, .. } => reason.capacity(),
            Message::Started(_)
            | Message::RequestsComplete { .. }
            | Message::ReconcileBegin { .. }
            | Message::ReconcileComplete { .. } => 0,
        };
        size_of::<Envelope>()
            .checked_add(dynamic)
            .context("ingress envelope accounting overflow")
    }
}

impl Sender {
    /// Total configured budget, not currently available credit. Used only for
    /// control-plane preflight; runtime exhaustion remains terminal.
    pub(crate) fn byte_limit(&self) -> usize {
        self.bytes
    }

    /// Never blocks the socket owner. Failure is fatal to the transport run;
    /// workers signal shutdown independently of this potentially full queue.
    pub fn send(&self, message: Message) -> Result<()> {
        let bytes = message.accounted_bytes()?;
        let permit = self
            .budget
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes)?)
            .context("ingress byte budget exhausted")?;
        self.queue
            .try_send(Envelope {
                message,
                queued_at: Instant::now(),
                _budget: permit,
            })
            .map_err(|e| {
                anyhow::anyhow!(
                    "ingress {}",
                    if e.is_full() {
                        "queue full"
                    } else {
                        "consumer closed"
                    }
                )
            })
    }
}
