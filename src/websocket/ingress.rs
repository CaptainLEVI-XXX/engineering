//! Bounded, ordered raw-message handoff. No financial decoding or persistence.
use crate::discovery::AdapterId;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, Sender as QueueSender, bounded};
use std::{mem::size_of, sync::Arc, time::Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::Utf8Bytes;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Session {
    pub worker: usize,
    pub generation: u64,
    pub adapter: AdapterId,
}

#[derive(Debug)]
pub enum Message {
    Started(Session),
    /// Authoritative assignment cutoff, even if the worker is disconnected.
    /// This is NOT a successfully sent unsubscribe request.
    Retired {
        worker: usize,
        adapter: AdapterId,
        revision: u64,
        assets: Vec<String>,
    },
    /// Successfully sent requests, NOT exchange acknowledgements. Initial and
    /// incremental requests precede subsequent data in this session's stream.
    SubscriptionsRequested {
        session: Session,
        source_slot: u16,
        /// One assignment revision per ID, in add-then-remove order.
        revisions: Vec<u64>,
        add: Vec<String>,
        remove: Vec<String>,
    },
    Data {
        session: Session,
        ordinal: u64,
        received_at: DateTime<Utc>,
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
        },
        receiver,
    ))
}

impl Sender {
    /// Never blocks the socket owner. Failure is fatal to the transport run;
    /// workers signal shutdown independently of this potentially full queue.
    pub fn send(&self, message: Message) -> Result<()> {
        let bytes = size_of::<Envelope>()
            + match &message {
                Message::Data { payload, .. } => payload.len(),
                Message::Retired { assets, .. } => {
                    assets.capacity() * size_of::<String>()
                        + assets.iter().map(String::capacity).sum::<usize>()
                }
                Message::SubscriptionsRequested {
                    add,
                    remove,
                    revisions,
                    ..
                } => {
                    revisions.capacity() * size_of::<u64>()
                        + (add.capacity() + remove.capacity()) * size_of::<String>()
                        + add
                            .iter()
                            .chain(remove)
                            .map(String::capacity)
                            .sum::<usize>()
                }
                Message::Ended { reason, .. } => reason.capacity(),
                Message::Started(_) => 0,
            };
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
