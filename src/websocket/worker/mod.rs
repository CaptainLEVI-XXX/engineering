use super::{
    Config,
    ingress::{self, Session},
    protocol::{ControlFrame, SubscriptionOp, WireProtocol, protocol_with_lifecycle},
    stopping,
};
use crate::discovery::AdapterId;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch},
    time::{Instant, interval_at, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{self, Message, Utf8Bytes, protocol::WebSocketConfig},
};

mod admission;
mod session;
#[cfg(test)]
mod tests;
pub(super) use admission::ConnectionBudget;
use admission::{ColdRecovery, ConnectionLease};
use session::SessionEnd;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Default)]
pub struct WorkerCommand {
    pub add: Vec<String>,
    pub remove: Vec<String>,
    pub revision: u64,
    pub restart: Option<Session>,
    pub planned_restart: bool,
    pub initialized: Option<Session>,
}

#[derive(Debug, Default)]
pub struct WorkerReport {
    pub sessions: u64,
    pub messages: u64,
    pub requested_at_cutoff: usize,
    pub connection_attempts: u64,
    pub connection_wait_intervals: u64,
    pub initialization_expiries: u64,
    pub initialized_sessions: u64,
    pub connection_wait: Duration,
    pub full_ready_latency: Duration,
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
    run_worker_session(
        Session {
            worker: id,
            generation: 0,
            adapter,
            slot: 0,
        },
        initial,
        commands,
        output,
        config,
        stop,
    )
    .await
}

/// Slot and adapter are fixed for this worker's entire lifetime. Only generation
/// advances, after a connection succeeds and before its Started boundary.
pub async fn run_worker_session(
    session: Session,
    initial: Vec<String>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
) -> Result<WorkerReport> {
    let budget = match ConnectionBudget::new(&config) {
        Ok(budget) => budget,
        Err(error) => {
            stop.send_replace(true);
            return Err(error);
        }
    };
    run_worker_shared(session, initial, commands, output, config, stop, budget).await
}

pub(super) async fn run_worker_shared(
    session: Session,
    initial: Vec<String>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
    budget: Arc<ConnectionBudget>,
) -> Result<WorkerReport> {
    let protocol = protocol_with_lifecycle(session.adapter, config.lifecycle_hints);
    run_worker_protocol(
        session,
        initial,
        commands,
        output,
        config,
        stop,
        (budget, protocol),
    )
    .await
}

/// Test-only protocol injection; the same worker and connection budget run below.
/// Production callers retain enum/static dispatch through run_worker_shared.
#[cfg(test)]
pub(crate) async fn run_worker_with_protocol<P: WireProtocol + Clone>(
    session: Session,
    initial: Vec<String>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
    protocol: P,
) -> Result<WorkerReport> {
    let budget = match ConnectionBudget::new(&config) {
        Ok(budget) => budget,
        Err(error) => {
            stop.send_replace(true);
            return Err(error);
        }
    };
    run_worker_protocol(
        session,
        initial,
        commands,
        output,
        config,
        stop,
        (budget, protocol),
    )
    .await
}

async fn run_worker_protocol<P: WireProtocol + Clone>(
    session: Session,
    initial: Vec<String>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
    (budget, protocol): (Arc<ConnectionBudget>, P),
) -> Result<WorkerReport> {
    let id = session.worker;
    let mut worker = Worker {
        session,
        desired: initial.into_iter().map(|id| (id, 0)).collect(),
        commands,
        output,
        config,
        protocol,
        stop: stop.subscribe(),
        report: WorkerReport::default(),
        ordinal: 0,
        budget,
        lease: None,
        ready_started: None,
        next_planned: false,
        cold_recovery: ColdRecovery::default(),
    };
    let result = worker.run(&stop).await;
    worker.release_lease("worker stopped");
    if let Err(ref error) = result {
        stop.send_replace(true);
        eprintln!(
            "integrity_failure worker={id} generation={} last_accepted_message={} received_messages={} error={error:#}",
            worker.session.generation, worker.ordinal, worker.report.messages
        );
    }
    result.map(|()| worker.report)
}

struct Worker<P: WireProtocol + Clone> {
    session: Session,
    desired: HashMap<String, u64>,
    commands: mpsc::Receiver<WorkerCommand>,
    output: ingress::Sender,
    config: Arc<Config>,
    protocol: P,
    stop: watch::Receiver<bool>,
    report: WorkerReport,
    ordinal: u64,
    budget: Arc<ConnectionBudget>,
    lease: Option<ConnectionLease>,
    ready_started: Option<Instant>,
    next_planned: bool,
    cold_recovery: ColdRecovery,
}

impl<P: WireProtocol + Clone> Worker<P> {
    fn release_lease(&mut self, reason: &str) {
        if let Some(lease) = self.lease.take() {
            eprintln!(
                "connection_lease_released session={:?} reason={reason} elapsed_ms={} full_ready=false",
                self.session,
                lease.started.elapsed().as_millis()
            );
        }
    }

    fn expire_lease(&mut self) -> Result<()> {
        if self.lease.is_some() {
            self.report.initialization_expiries = self
                .report
                .initialization_expiries
                .checked_add(1)
                .context("initialization expiry counter exhausted")?;
            self.release_lease("initialization timeout; degraded attempt");
        }
        Ok(())
    }

    fn initialized(&mut self) -> Result<()> {
        if let Some(started) = self.ready_started.take() {
            let expired = self.lease.take().is_none();
            let elapsed = started.elapsed();
            self.report.initialized_sessions = self
                .report
                .initialized_sessions
                .checked_add(1)
                .context("initialized session counter exhausted")?;
            self.report.full_ready_latency = self
                .report
                .full_ready_latency
                .checked_add(elapsed)
                .context("full ready duration overflow")?;
            // Called only for this generation's accepted all-ready signal.
            // ready_started is consumed once: duplicate acknowledgments cannot
            // clear a subsequent recovery episode.
            self.cold_recovery.ready();
            eprintln!(
                "connection_full_ready session={:?} latency_ms={} lease_already_expired={expired}",
                self.session,
                elapsed.as_millis()
            );
        }
        Ok(())
    }

    fn write_timeout(&self) -> Duration {
        self.lease.as_ref().map_or(self.config.io_timeout, |lease| {
            self.config
                .io_timeout
                .min(lease.deadline.saturating_duration_since(Instant::now()))
        })
    }

    fn offline_update(&mut self, command: &WorkerCommand) -> Result<()> {
        // A mandatory request can supersede a planned attempt still waiting for
        // admission. Old readiness signals never release a new attempt's lease.
        if command.restart == Some(self.session) && !command.planned_restart {
            self.next_planned = false;
        }
        self.update(command)
    }

    async fn wait_retry(&mut self, normal: Duration) -> Result<()> {
        let now = Instant::now();
        let delay = self.cold_recovery.retry_delay(now, normal);
        if delay.is_zero() {
            return Ok(());
        }
        // Keep configured reconnect jitter before escalation. The cold policy
        // has an exact 900-second ceiling, not 900 seconds plus jitter.
        let jitter = if self.cold_recovery.cooldown.is_zero() {
            Duration::from_millis(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_millis() as u64
                    % 100,
            )
        } else {
            eprintln!(
                "connection_cold_cooldown worker={} slot={} delay_s={} full_ready=false",
                self.session.worker,
                self.session.slot,
                delay.as_secs()
            );
            Duration::ZERO
        };
        let until = now
            .checked_add(delay)
            .and_then(|until| until.checked_add(jitter))
            .context("reconnect deadline overflow")?;
        loop {
            tokio::select! {
                biased;
                _ = stopping(&mut self.stop) => return Ok(()),
                command = self.commands.recv() => self.offline_update(&command.context("assignment owner closed")?)?,
                _ = sleep_until(until) => return Ok(()),
            }
        }
    }

    async fn acquire_lease(&mut self) -> Result<bool> {
        let budget = self.budget.clone();
        let planned = self.next_planned;
        let waiting = Instant::now();
        // One cancellable waiter per bounded worker. Cancelling and rejoining
        // the fair semaphore queue on a timer can starve older assignments.
        // Waiting for local capacity is not a failed network attempt.
        let acquire = budget.acquire(planned);
        tokio::pin!(acquire);
        let mut report_at = waiting + Duration::from_secs(30);
        let acquired = loop {
            tokio::select! {
                biased;
                _ = stopping(&mut self.stop) => return Ok(false),
                command = self.commands.recv() => {
                    self.offline_update(&command.context("assignment owner closed")?)?;
                    if self.desired.is_empty() || (planned && !self.next_planned) { break None; }
                },
                result = &mut acquire => break Some(result),
                _ = sleep_until(report_at) => {
                    self.report.connection_wait_intervals = self.report.connection_wait_intervals
                        .checked_add(1).context("connection wait interval counter exhausted")?;
                    eprintln!("connection_wait_pending worker={} slot={} planned={planned} wait_ms={} retaining_position=true", self.session.worker, self.session.slot, waiting.elapsed().as_millis());
                    report_at = Instant::now() + Duration::from_secs(30);
                },
            }
        };
        let waited = waiting.elapsed();
        self.report.connection_wait = self
            .report
            .connection_wait
            .checked_add(waited)
            .context("connection wait duration overflow")?;
        match acquired {
            Some(result) => {
                let lease = result?;
                self.cold_recovery.begin(lease.started);
                self.lease = Some(lease);
                self.next_planned = false; // Only this next attempt has planned status.
                self.report.connection_attempts = self
                    .report
                    .connection_attempts
                    .checked_add(1)
                    .context("connection attempt counter exhausted")?;
                eprintln!(
                    "connection_start worker={} slot={} planned={planned} wait_ms={}",
                    self.session.worker,
                    self.session.slot,
                    waited.as_millis()
                );
                Ok(true)
            }
            None => {
                eprintln!(
                    "connection_wait_cancelled worker={} slot={} wait_ms={}",
                    self.session.worker,
                    self.session.slot,
                    waited.as_millis()
                );
                Ok(false)
            }
        }
    }

    fn update(&mut self, command: &WorkerCommand) -> Result<()> {
        for id in &command.remove {
            self.desired.remove(id);
        }
        self.desired
            .extend(command.add.iter().map(|id| (id.clone(), command.revision)));
        ensure!(
            self.desired.len() <= self.config.assets_per_socket,
            "worker assignment budget exceeded"
        );
        Ok(())
    }

    fn emit(
        &mut self,
        payload: Utf8Bytes,
        received_at: DateTime<Utc>,
        received_mono: std::time::Instant,
    ) -> Result<()> {
        let ordinal = self
            .ordinal
            .checked_add(1)
            .context("receive ordinal exhausted")?;
        self.output.send(ingress::Message::Data {
            session: self.session,
            ordinal,
            received_at,
            received_mono,
            payload,
        })?;
        self.ordinal = ordinal;
        Ok(())
    }

    async fn run(&mut self, failure: &watch::Sender<bool>) -> Result<()> {
        self.config.validate()?;
        ensure!(
            self.session.generation == 0 && self.session.slot < self.config.replicas_per_market,
            "invalid initial worker session"
        );
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
                self.offline_update(&command)?;
            }
            if self.desired.is_empty() {
                self.next_planned = false;
                tokio::select! {
                    biased;
                    _ = stopping(&mut self.stop) => return Ok(()),
                    command = self.commands.recv() => self.offline_update(&command.context("assignment owner closed")?)?,
                }
                continue;
            }
            if !self.acquire_lease().await? {
                continue;
            }
            let config = self.config.clone();
            let ws_config = WebSocketConfig::default()
                .max_message_size(Some(config.max_message_bytes))
                .max_frame_size(Some(config.max_message_bytes))
                .max_write_buffer_size(256 << 10);
            let connect = timeout(
                self.write_timeout(),
                connect_async_with_config(
                    self.protocol.handshake_request(&config.endpoint)?,
                    Some(ws_config),
                    true,
                ),
            );
            tokio::pin!(connect);
            let connected = loop {
                tokio::select! {
                    biased;
                    _ = stopping(&mut self.stop) => return Ok(()),
                    result = &mut connect => break Some(result),
                    command = self.commands.recv() => {
                        self.offline_update(&command.context("assignment owner closed")?)?;
                        if self.desired.is_empty() { break None; }
                    },
                }
            };
            let Some(connected) = connected else {
                self.release_lease("assignment removed during handshake");
                continue;
            };
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
                    self.report.sessions = self.session.generation;
                    self.ready_started = self.lease.as_ref().map(|lease| lease.started);
                    eprintln!(
                        "connection_handshake session={:?} elapsed_ms={} full_ready=false",
                        self.session,
                        self.lease
                            .as_ref()
                            .expect("connecting lease")
                            .started
                            .elapsed()
                            .as_millis()
                    );
                    let started = Instant::now();
                    let result = self.stream(&mut socket).await;
                    // A ready session ending starts a NEW episode; an unready
                    // one ending must retain its original start and cooldown.
                    self.cold_recovery.begin(Instant::now());
                    if result.is_err() {
                        failure.send_replace(true);
                    }
                    // Intake has stopped. Publish the boundary before any close
                    // I/O so a healthy peer can take over without handshake delay.
                    let reason = match &result {
                        Ok(end) => end.reason().to_owned(),
                        Err(error) => format!("integrity failure: {error:#}"),
                    };
                    let boundary = self.output.send(ingress::Message::Ended {
                        session: self.session,
                        reason,
                    });
                    if boundary.is_err() {
                        failure.send_replace(true);
                    }
                    self.ready_started = None;
                    if self
                        .lease
                        .as_ref()
                        .is_some_and(|lease| Instant::now() >= lease.deadline)
                    {
                        self.expire_lease()?;
                    } else {
                        self.release_lease("session ended");
                    }
                    if boundary.is_ok()
                        && !*self.stop.borrow()
                        && result
                            .as_ref()
                            .is_ok_and(|end| matches!(end, SessionEnd::OwnerRestart(_)))
                    {
                        // Intake is already fenced. Best-effort unsubscribe is
                        // bounded across ALL batches; neither success nor failure
                        // permits reuse of this generation or restores history.
                        let assets: Vec<_> = self.desired.keys().cloned().collect();
                        let protocol = self.protocol.clone();
                        let unsubscribe = async {
                            for batch in assets.chunks(config.subscription_batch) {
                                let payload =
                                    protocol.subscription(SubscriptionOp::Remove, batch)?;
                                io(
                                    socket.send(Message::Text(payload.into())),
                                    config.io_timeout,
                                    &mut self.stop,
                                )
                                .await?;
                            }
                            Ok::<(), anyhow::Error>(())
                        };
                        let result = timeout(config.io_timeout, unsubscribe).await;
                        eprintln!(
                            "renewal_unsubscribe session={:?} requested_assets={} result={result:?}; delivery is not acknowledged",
                            self.session,
                            assets.len()
                        );
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
                    result?;
                    boundary?;
                    if *self.stop.borrow() {
                        return Ok(());
                    }
                    // Preserve the normal reconnect policy. This reset does NOT
                    // reset the independent unready episode or its escalation.
                    if started.elapsed() >= Duration::from_secs(30) {
                        delay = config.reconnect_min;
                    }
                }
                error => {
                    eprintln!(
                        "connect_failed worker={} adapter={:?}: {error:?}",
                        self.session.worker, self.session.adapter
                    );
                    if self
                        .lease
                        .as_ref()
                        .is_some_and(|lease| Instant::now() >= lease.deadline)
                    {
                        self.expire_lease()?;
                    } else {
                        self.release_lease("handshake failed");
                    }
                }
            }
            self.wait_retry(delay).await?;
            delay = delay.saturating_mul(2).min(config.reconnect_max);
        }
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
