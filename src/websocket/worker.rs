use super::{
    Config,
    ingress::{self, Session},
    stopping,
};
use crate::discovery::AdapterId;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    collections::HashSet,
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::{Instant, interval_at, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{self, Message, Utf8Bytes, protocol::WebSocketConfig},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Default)]
pub struct WorkerCommand {
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

#[derive(Debug, Default)]
pub struct WorkerReport {
    pub sessions: u64,
    pub messages: u64,
    pub requested_at_cutoff: usize,
}

pub async fn run_worker(
    id: usize,
    adapter: AdapterId,
    initial: Vec<String>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
) -> Result<WorkerReport> {
    let mut worker = Worker {
        session: Session {
            worker: id,
            generation: 0,
            adapter,
        },
        desired: initial.into_iter().collect(),
        commands,
        output,
        config,
        stop: stop.subscribe(),
        report: WorkerReport::default(),
        ordinal: 0,
    };
    let result = worker.run(&stop).await;
    if let Err(ref error) = result {
        stop.send_replace(true);
        eprintln!(
            "integrity_failure worker={id} generation={} last_accepted_message={} received_messages={} error={error:#}",
            worker.session.generation, worker.ordinal, worker.report.messages
        );
    }
    result.map(|()| worker.report)
}

struct Worker {
    session: Session,
    desired: HashSet<String>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Receiver<bool>,
    report: WorkerReport,
    ordinal: u64,
}

impl Worker {
    fn update(&mut self, command: &WorkerCommand) -> Result<()> {
        for id in &command.remove {
            self.desired.remove(id);
        }
        self.desired.extend(command.add.iter().cloned());
        ensure!(
            self.desired.len() <= self.config.assets_per_socket,
            "worker assignment budget exceeded"
        );
        Ok(())
    }

    fn emit(&mut self, payload: Utf8Bytes, received_at: DateTime<Utc>) -> Result<()> {
        let ordinal = self
            .ordinal
            .checked_add(1)
            .context("receive ordinal exhausted")?;
        self.output.send(ingress::Message::Data {
            session: self.session,
            ordinal,
            received_at,
            payload,
        })?;
        self.ordinal = ordinal;
        Ok(())
    }

    async fn run(&mut self, failure: &watch::Sender<bool>) -> Result<()> {
        self.config.validate()?;
        ensure!(
            self.desired.len() <= self.config.assets_per_socket,
            "initial assignment exceeds budget"
        );
        let mut delay = self.config.reconnect_min;
        loop {
            if *self.stop.borrow() {
                return Ok(());
            }
            while let Ok(command) = self.commands.try_recv() {
                self.update(&command)?;
            }
            if self.desired.is_empty() {
                tokio::select! {
                    biased;
                    _ = stopping(&mut self.stop) => return Ok(()),
                    command = self.commands.recv() => self.update(&command.context("assignment owner closed")?)?,
                }
                continue;
            }
            let config = self.config.clone();
            let ws_config = WebSocketConfig::default()
                .max_message_size(Some(config.max_message_bytes))
                .max_frame_size(Some(config.max_message_bytes))
                .max_write_buffer_size(256 << 10);
            let connect = timeout(
                config.io_timeout,
                connect_async_with_config(&config.endpoint, Some(ws_config), true),
            );
            tokio::pin!(connect);
            let connected = loop {
                tokio::select! {
                    biased;
                    _ = stopping(&mut self.stop) => return Ok(()),
                    command = self.commands.recv() => {
                        self.update(&command.context("assignment owner closed")?)?;
                        if self.desired.is_empty() { break None; }
                    },
                    result = &mut connect => break Some(result),
                }
            };
            let Some(connected) = connected else { continue };
            match connected {
                Ok(Ok((mut socket, _))) => {
                    if *self.stop.borrow() {
                        return Ok(());
                    }
                    self.session.generation = self
                        .session
                        .generation
                        .checked_add(1)
                        .context("session generation exhausted")?;
                    self.ordinal = 0;
                    self.output.send(ingress::Message::Started(self.session))?;
                    self.report.sessions += 1;
                    let started = Instant::now();
                    let result = self.stream(&mut socket).await;
                    if result.is_err() {
                        failure.send_replace(true);
                    }
                    // Close attempts are bounded even when the peer has vanished.
                    let _ = timeout(config.io_timeout.min(Duration::from_secs(1)), async {
                        socket.close(None).await?;
                        while let Some(message) = socket.next().await {
                            if matches!(message?, Message::Close(_)) {
                                break;
                            }
                        }
                        socket.flush().await
                    })
                    .await;
                    let reason = match &result {
                        Ok(reason) => reason.clone(),
                        Err(error) => format!("integrity failure: {error:#}"),
                    };
                    let boundary = self.output.send(ingress::Message::Ended {
                        session: self.session,
                        reason,
                    });
                    result?;
                    boundary?;
                    if *self.stop.borrow() {
                        return Ok(());
                    }
                    if started.elapsed() >= Duration::from_secs(30) {
                        delay = config.reconnect_min;
                    }
                }
                error => eprintln!(
                    "connect_failed worker={} adapter={:?}: {error:?}",
                    self.session.worker, self.session.adapter
                ),
            }
            let jitter = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_millis() as u64
                % 100;
            let until = Instant::now() + delay + Duration::from_millis(jitter);
            loop {
                tokio::select! {
                    biased;
                    _ = stopping(&mut self.stop) => return Ok(()),
                    command = self.commands.recv() => self.update(&command.context("assignment owner closed")?)?,
                    _ = sleep_until(until) => break,
                }
            }
            delay = delay.saturating_mul(2).min(config.reconnect_max);
        }
    }

    async fn stream(&mut self, socket: &mut Socket) -> Result<String> {
        let config = self.config.clone();
        let mut requested = HashSet::new();
        let mut removed = HashSet::new();
        let mut pending_subscriptions: Vec<_> = self.desired.iter().cloned().collect();
        let mut pending_unsubscriptions = Vec::new();
        pending_subscriptions.sort();
        let mut initial = true;
        let mut heartbeat = interval_at(Instant::now() + config.heartbeat, config.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pong = Instant::now();
        self.report.requested_at_cutoff = 0;
        let result = 'session: loop {
            if *self.stop.borrow() {
                break Ok("shutdown".into());
            }
            if self.desired.is_empty() && pending_unsubscriptions.is_empty() {
                break Ok("no assignments".into());
            }
            tokio::select! {
                _ = stopping(&mut self.stop) => continue,
                incoming = socket.next() => {
                    let received_at = Utc::now(); // Complete-message delivery, before parse.
                    let message = match incoming {
                        Some(Ok(message)) => message,
                        Some(Err(tungstenite::Error::Capacity(error))) => return Err(error.into()),
                        other => break 'session Ok(format!("connection interrupted: {other:?}")),
                    };
                    match message {
                        Message::Text(text) if text == "PONG" => last_pong = Instant::now(),
                        Message::Text(text) => {
                            self.report.messages += 1;
                            self.emit(text, received_at)?;
                        }
                        Message::Ping(_) => {
                            if let Err(error) = io(socket.flush(), config.io_timeout, &mut self.stop).await {
                                break 'session Ok(format!("control pong failed: {error}"));
                            }
                        }
                        Message::Pong(_) => {},
                        Message::Close(_) => {
                            let _ = io(socket.flush(), config.io_timeout, &mut self.stop).await;
                            break 'session Ok("remote close".into());
                        }
                        _ => anyhow::bail!("unexpected data-bearing WebSocket message"),
                    }
                }
                command = self.commands.recv() => {
                    let command = command.context("assignment owner closed")?;
                    // Persist intent BEFORE any fallible send.
                    pending_subscriptions.extend(command.add.iter().filter(|id| !self.desired.contains(*id)).cloned());
                    self.update(&command)?;
                    pending_subscriptions.retain(|id| self.desired.contains(id));
                    for id in &command.remove {
                        if removed.insert(id.clone()) && requested.contains(id) {
                            pending_unsubscriptions.push(id.clone());
                        }
                    }
                    if command.add.iter().any(|id| removed.contains(id)) || removed.len() >= config.assets_per_socket {
                        break 'session Ok("assignment reactivation requires fresh connection".into());
                    }
                }
                // One batch per iteration gives reception, heartbeat and shutdown
                // another polling opportunity. A single write can still stall reads
                // until io_timeout; uncertain writes always end this session.
                _ = tokio::task::yield_now(), if !pending_unsubscriptions.is_empty() => {
                    let count = config.subscription_batch.min(pending_unsubscriptions.len());
                    let removals: Vec<_> = pending_unsubscriptions.drain(..count).collect();
                    let payload = json!({"operation":"unsubscribe", "assets_ids":removals});
                    if let Err(error) = io(socket.send(Message::Text(payload.to_string().into())), config.io_timeout, &mut self.stop).await {
                        break 'session Ok(format!("unsubscribe uncertain: {error}"));
                    }
                    for id in &removals { requested.remove(id); }
                    self.output.send(ingress::Message::SubscriptionsRequested {
                        session: self.session, add: vec![], remove: removals,
                    })?;
                }
                _ = tokio::task::yield_now(), if !pending_subscriptions.is_empty() && pending_unsubscriptions.is_empty() => {
                    let count = config.subscription_batch.min(pending_subscriptions.len());
                    let additions: Vec<_> = pending_subscriptions.drain(..count).collect();
                    let payload = if initial {
                        json!({"type":"market", "assets_ids":additions, "level":2, "initial_dump":true})
                    } else {
                        json!({"operation":"subscribe", "assets_ids":additions, "level":2})
                    };
                    if let Err(error) = io(socket.send(Message::Text(payload.to_string().into())), config.io_timeout, &mut self.stop).await {
                        break 'session Ok(format!("subscribe uncertain: {error}"));
                    }
                    initial = false;
                    requested.extend(additions.iter().cloned());
                    self.output.send(ingress::Message::SubscriptionsRequested {
                        session: self.session, add: additions, remove: vec![],
                    })?;
                }
                _ = heartbeat.tick() => {
                    if last_pong.elapsed() > config.pong_timeout { break 'session Ok("application heartbeat timeout".into()); }
                    if let Err(error) = io(socket.send(Message::Text("PING".into())), config.io_timeout, &mut self.stop).await {
                        break 'session Ok(format!("heartbeat write failed: {error}"));
                    }
                }
            }
        };
        // Include shutdown that interrupted a write, not only the receive loop.
        if *self.stop.borrow() {
            self.report.requested_at_cutoff = requested.len();
        }
        result
    }
}

async fn io<T>(
    future: impl Future<Output = std::result::Result<T, tungstenite::Error>>,
    limit: Duration,
    stop: &mut watch::Receiver<bool>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = stopping(stop) => anyhow::bail!("shutdown"),
        result = timeout(limit, future) => Ok(result.context("socket write timeout; delivery uncertain")??),
    }
}
