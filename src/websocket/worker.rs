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

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One shared administrative limiter per manager. No market-data path takes
/// this mutex. Planned work acquires the smaller pool first, leaving at least
/// one global lease available to mandatory connection attempts.
pub(super) struct ConnectionBudget {
    all: Arc<Semaphore>,
    planned: Arc<Semaphore>,
    next_start: Mutex<Instant>,
    interval: Duration,
    initialization_timeout: Duration,
}

impl ConnectionBudget {
    pub(super) fn new(config: &Config) -> Result<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            all: Arc::new(Semaphore::new(config.connection_concurrency)),
            planned: Arc::new(Semaphore::new(config.connection_concurrency - 1)),
            next_start: Mutex::new(Instant::now()),
            interval: config.connection_interval,
            initialization_timeout: config.initialization_timeout,
        }))
    }

    async fn acquire(&self, planned: bool) -> Result<ConnectionLease> {
        let planned_permit = if planned {
            Some(
                self.planned
                    .clone()
                    .acquire_owned()
                    .await
                    .context("planned connection limiter closed")?,
            )
        } else {
            None
        };
        let permit = self
            .all
            .clone()
            .acquire_owned()
            .await
            .context("connection limiter closed")?;
        let mut next = self.next_start.lock().await;
        sleep_until(*next).await;
        let started = Instant::now();
        *next = started
            .checked_add(self.interval)
            .context("connection pacing deadline overflow")?;
        let deadline = started
            .checked_add(self.initialization_timeout)
            .context("initialization deadline overflow")?;
        Ok(ConnectionLease {
            _permit: permit,
            _planned_permit: planned_permit,
            started,
            deadline,
        })
    }
}

struct ConnectionLease {
    _permit: OwnedSemaphorePermit,
    _planned_permit: Option<OwnedSemaphorePermit>,
    started: Instant,
    deadline: Instant,
}

/// Transport recovery also covers attempts that never publish Started, which
/// the processor cannot cool down. Handshakes do not establish full readiness.
#[derive(Default)]
struct ColdRecovery {
    since: Option<Instant>,
    cooldown: Duration,
}

impl ColdRecovery {
    fn begin(&mut self, now: Instant) {
        self.since.get_or_insert(now);
    }

    fn ready(&mut self) {
        *self = Self::default();
    }

    fn retry_delay(&mut self, now: Instant, normal: Duration) -> Duration {
        if self
            .since
            .is_some_and(|since| now.saturating_duration_since(since) >= Duration::from_secs(120))
        {
            self.cooldown = if self.cooldown.is_zero() {
                Duration::from_secs(60)
            } else {
                self.cooldown
                    .saturating_mul(2)
                    .min(Duration::from_secs(900))
            };
            self.cooldown
        } else {
            normal
        }
    }
}

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
                        Ok(reason) => reason.clone(),
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
                            .is_ok_and(|reason| reason.starts_with("processor "))
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

    async fn stream(&mut self, socket: &mut Socket) -> Result<String> {
        let config = self.config.clone();
        let protocol = self.protocol.clone();
        let mut requested = HashSet::new();
        let mut removed = HashSet::new();
        let mut pending_subscriptions: Vec<_> = self.desired.keys().cloned().collect();
        let mut pending_unsubscriptions = Vec::new();
        pending_subscriptions.sort();
        let mut initial = true;
        let mut requests_complete = false;
        let mut readiness_ambiguous = false;
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
            if self
                .lease
                .as_ref()
                .is_some_and(|lease| Instant::now() >= lease.deadline)
            {
                self.expire_lease()?;
            }
            if !requests_complete
                && pending_subscriptions.is_empty()
                && pending_unsubscriptions.is_empty()
                && self.commands.is_empty()
            {
                ensure!(
                    requested.len() == self.desired.len()
                        && self.desired.keys().all(|id| requested.contains(id)),
                    "request-complete marker without complete desired request set"
                );
                self.output.send(ingress::Message::RequestsComplete {
                    session: self.session,
                })?;
                requests_complete = true;
            }
            let lease_deadline = self
                .lease
                .as_ref()
                .map_or(last_pong, |lease| lease.deadline);
            tokio::select! {
                _ = stopping(&mut self.stop) => continue,
                _ = async { sleep_until(lease_deadline).await }, if self.lease.is_some() => self.expire_lease()?,
                incoming = socket.next() => {
                    let received_at = Utc::now(); // Complete-message delivery, before parse.
                    let received_mono = std::time::Instant::now();
                    let message = match incoming {
                        Some(Ok(message)) => message,
                        Some(Err(tungstenite::Error::Capacity(error))) => return Err(error.into()),
                        other => break 'session Ok(format!("connection interrupted: {other:?}")),
                    };
                    match message {
                        Message::Text(text) if protocol.classify(&text) == ControlFrame::ApplicationPong => last_pong = Instant::now(),
                        Message::Text(text) => {
                            self.report.messages = self.report.messages.checked_add(1)
                                .context("received message counter exhausted")?;
                            self.emit(text, received_at, received_mono)?;
                        }
                        Message::Ping(_) => {
                            if let Err(error) = io(socket.flush(), self.write_timeout(), &mut self.stop).await {
                                break 'session Ok(format!("control pong failed: {error}"));
                            }
                        }
                        Message::Pong(_) => {},
                        Message::Close(_) => {
                            break 'session Ok("remote close".into());
                        }
                        _ => anyhow::bail!("unexpected data-bearing WebSocket message"),
                    }
                }
                command = self.commands.recv() => {
                    let command = command.context("assignment owner closed")?;
                    if !command.add.is_empty() || !command.remove.is_empty() {
                        // Initialized carries only a generation, not a marker
                        // revision. After a published marker is invalidated, an
                        // old in-flight acknowledgement is indistinguishable
                        // from a fresh one: retain the lease until timeout/end.
                        if requests_complete && self.lease.is_some() && !readiness_ambiguous {
                            readiness_ambiguous = true;
                            eprintln!("connection_readiness_ambiguous session={:?} reason=assignment_changed_after_marker lease_release=timeout_or_end", self.session);
                        }
                        requests_complete = false;
                    }
                    // Persist intent BEFORE any fallible send.
                    pending_subscriptions.extend(command.add.iter().filter(|id| !self.desired.contains_key(*id)).cloned());
                    self.update(&command)?;
                    pending_subscriptions.retain(|id| self.desired.contains_key(id));
                    for id in &command.remove {
                        if removed.insert(id.clone()) && requested.contains(id) {
                            pending_unsubscriptions.push((id.clone(), command.revision));
                        }
                    }
                    if command.initialized == Some(self.session) && command.restart != Some(self.session)
                        && requests_complete && !readiness_ambiguous {
                        if self.lease.as_ref().is_some_and(|lease| Instant::now() >= lease.deadline) {
                            self.expire_lease()?;
                        }
                        self.initialized()?;
                    }
                    if command.restart == Some(self.session) {
                        self.next_planned = command.planned_restart;
                        break 'session Ok(if command.planned_restart {
                            "processor granted planned renewal".into()
                        } else { "processor requested fresh baseline".into() });
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
                    let (removals, revisions): (Vec<_>, Vec<_>) = pending_unsubscriptions.drain(..count).unzip();
                    let payload = protocol.subscription(SubscriptionOp::Remove, &removals)?;
                    if let Err(error) = io(socket.send(Message::Text(payload.into())), self.write_timeout(), &mut self.stop).await {
                        break 'session Ok(format!("unsubscribe uncertain: {error}"));
                    }
                    for id in &removals { requested.remove(id); }
                    self.output.send(ingress::Message::SubscriptionsRequested {
                        session: self.session, revisions, add: vec![], remove: removals,
                    })?;
                }
                _ = tokio::task::yield_now(), if !pending_subscriptions.is_empty() && pending_unsubscriptions.is_empty() => {
                    let count = config.subscription_batch.min(pending_subscriptions.len());
                    let additions: Vec<_> = pending_subscriptions.drain(..count).collect();
                    let payload = protocol.subscription(if initial { SubscriptionOp::Initial } else { SubscriptionOp::Add }, &additions)?;
                    if let Err(error) = io(socket.send(Message::Text(payload.into())), self.write_timeout(), &mut self.stop).await {
                        break 'session Ok(format!("subscribe uncertain: {error}"));
                    }
                    initial = false;
                    requested.extend(additions.iter().cloned());
                    let revisions = additions.iter().map(|id| self.desired[id]).collect();
                    self.output.send(ingress::Message::SubscriptionsRequested {
                        session: self.session, revisions, add: additions, remove: vec![],
                    })?;
                }
                _ = heartbeat.tick() => {
                    let Some(heartbeat) = protocol.heartbeat() else { continue };
                    if last_pong.elapsed() > config.pong_timeout { break 'session Ok("application heartbeat timeout".into()); }
                    if let Err(error) = io(socket.send(Message::Text(heartbeat.into())), self.write_timeout(), &mut self.stop).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};
    use tungstenite::protocol::Role;

    const UNSUBSCRIBE: &str = r#"{"operation":"unsubscribe","assets_ids":["A"]}"#;

    #[test]
    fn cold_recovery_escalates_without_real_time_and_only_readiness_resets_it() {
        let origin = Instant::now();
        let normal = Duration::from_millis(250);
        let mut recovery = ColdRecovery::default();
        recovery.begin(origin);
        assert_eq!(recovery.retry_delay(origin, normal), normal);
        let almost = origin + Duration::from_secs(120) - Duration::from_nanos(1);
        assert_eq!(recovery.retry_delay(almost, normal), normal);
        // Further attempts and handshakes retain the first episode start.
        recovery.begin(origin + Duration::from_secs(119));
        assert_eq!(recovery.since, Some(origin));
        let mut now = origin + Duration::from_secs(120);
        for seconds in [60, 120, 240, 480, 900, 900, 900] {
            // Even if a long connected-but-unready session reset the ordinary
            // reconnect delay, the cold delay wins and remains bounded.
            let delay = recovery.retry_delay(now, normal);
            assert_eq!(delay, Duration::from_secs(seconds));
            now += delay;
            recovery.begin(now);
        }
        recovery.ready();
        assert!(recovery.since.is_none());
        assert_eq!(recovery.cooldown, Duration::ZERO);
        recovery.begin(now);
        assert_eq!(recovery.retry_delay(now, normal), normal);
        assert_eq!(
            recovery.retry_delay(now + Duration::from_secs(120), normal),
            Duration::from_secs(60)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn queued_worker_keeps_admission_priority_and_cancels_on_retirement() {
        let config = Arc::new(Config {
            connection_interval: Duration::from_millis(1),
            ..Config::default()
        });
        let budget = ConnectionBudget::new(&config).unwrap();
        let first = budget.acquire(false).await.unwrap();
        let second = budget.acquire(false).await.unwrap();
        let (commands, receiver) = mpsc::channel(2);
        let (output, _ingress) = ingress::channel(4, 4096).unwrap();
        let (stop, _) = watch::channel(false);
        let worker = run_worker_protocol(
            Session {
                worker: 0,
                generation: 0,
                adapter: AdapterId::PolymarketV1,
                slot: 0,
            },
            vec!["A".into()],
            receiver,
            output,
            config,
            stop.clone(),
            (
                budget.clone(),
                protocol_with_lifecycle(AdapterId::PolymarketV1, false),
            ),
        );
        tokio::pin!(worker);
        assert!(futures_util::poll!(&mut worker).is_pending());
        let later = budget.acquire(false);
        tokio::pin!(later);
        assert!(futures_util::poll!(&mut later).is_pending());
        for _ in 0..5 {
            tokio::time::advance(Duration::from_secs(31)).await;
            assert!(futures_util::poll!(&mut worker).is_pending());
        }
        drop(first);
        // The first worker must still own the first queue position, even after
        // long local contention; no network failure has occurred.
        assert!(futures_util::poll!(&mut later).is_pending());
        // Retiring its entire assignment must cancel admission without dialing.
        commands
            .try_send(WorkerCommand {
                remove: vec!["A".into()],
                ..WorkerCommand::default()
            })
            .unwrap();
        assert!(futures_util::poll!(&mut worker).is_pending());
        let _later = later.await.unwrap();
        stop.send_replace(true);
        let report = worker.await.unwrap();
        assert_eq!(report.connection_attempts, 0);
        assert_eq!(report.sessions, 0);
        drop(second);
    }

    #[tokio::test(start_paused = true)]
    async fn local_admission_wait_does_not_delay_first_network_retry() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Arc::new(Config {
            endpoint: format!("ws://{}", listener.local_addr().unwrap()),
            connection_interval: Duration::from_millis(1),
            reconnect_min: Duration::from_millis(1),
            ..Config::default()
        });
        let budget = ConnectionBudget::new(&config).unwrap();
        let first = budget.acquire(false).await.unwrap();
        let second = budget.acquire(false).await.unwrap();
        let (_commands, receiver) = mpsc::channel(2);
        let (output, _ingress) = ingress::channel(4, 4096).unwrap();
        let (stop, _) = watch::channel(false);
        let worker = run_worker_protocol(
            Session {
                worker: 0,
                generation: 0,
                adapter: AdapterId::PolymarketV1,
                slot: 0,
            },
            vec!["A".into()],
            receiver,
            output,
            config,
            stop.clone(),
            (
                budget,
                protocol_with_lifecycle(AdapterId::PolymarketV1, false),
            ),
        );
        tokio::pin!(worker);
        assert!(futures_util::poll!(&mut worker).is_pending());
        for _ in 0..5 {
            tokio::time::advance(Duration::from_secs(31)).await;
            assert!(futures_util::poll!(&mut worker).is_pending());
        }
        // Use real time for socket I/O; only local admission was simulated.
        tokio::time::resume();
        drop(first);
        drop(second);
        let peer = async {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket); // Reject the first handshake, without publishing data.
            let (_retry, _) = listener.accept().await.unwrap();
            stop.send_replace(true);
        };
        let (report, ()) = timeout(Duration::from_secs(2), async { tokio::join!(worker, peer) })
            .await
            .expect("local waiting incorrectly charged as a cold network failure");
        let report = report.unwrap();
        assert_eq!(report.connection_attempts, 2);
        assert_eq!(report.sessions, 0);
    }

    #[tokio::test]
    async fn cold_cooldown_ignores_duplicate_readiness_and_is_shutdown_cancellable() {
        let config = Arc::new(Config::default());
        let budget = ConnectionBudget::new(&config).unwrap();
        let (commands, command_receiver) = mpsc::channel(2);
        let (output, _ingress) = ingress::channel(2, 1024).unwrap();
        let (stop, stop_receiver) = watch::channel(false);
        let now = Instant::now();
        let mut worker = Worker {
            session: Session {
                adapter: AdapterId::PolymarketV1,
                worker: 0,
                slot: 0,
                generation: 1,
            },
            desired: HashMap::from([("A".into(), 0)]),
            commands: command_receiver,
            output,
            protocol: protocol_with_lifecycle(AdapterId::PolymarketV1, false),
            config,
            stop: stop_receiver,
            report: WorkerReport::default(),
            ordinal: 0,
            budget,
            lease: None,
            ready_started: Some(now),
            next_planned: false,
            cold_recovery: ColdRecovery::default(),
        };
        worker.cold_recovery.begin(now - Duration::from_secs(120));
        worker.initialized().unwrap();
        assert!(worker.cold_recovery.since.is_none());
        assert_eq!(worker.report.initialized_sessions, 1);

        // The consumed acknowledgment cannot terminate another cold episode.
        let since = now - Duration::from_secs(120);
        worker.cold_recovery.begin(since);
        worker.initialized().unwrap();
        assert_eq!(worker.cold_recovery.since, Some(since));
        assert_eq!(worker.report.initialized_sessions, 1);
        let session = worker.session;
        {
            let waiting = worker.wait_retry(Duration::from_millis(1));
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            // Assignment churn and a late offline acknowledgment are serviced,
            // but neither releases or restarts the pending cooldown.
            commands
                .try_send(WorkerCommand {
                    add: vec!["B".into()],
                    initialized: Some(session),
                    ..WorkerCommand::default()
                })
                .unwrap();
            assert!(futures_util::poll!(&mut waiting).is_pending());
            stop.send_replace(true);
            timeout(Duration::from_millis(250), &mut waiting)
                .await
                .expect("shutdown failed to cancel cold cooldown")
                .unwrap();
        }
        assert!(worker.desired.contains_key("B"));
        assert_eq!(worker.cold_recovery.since, Some(since));
        assert_eq!(worker.cold_recovery.cooldown, Duration::from_secs(60));
        assert_eq!(worker.report.initialized_sessions, 1);
    }

    #[tokio::test]
    async fn shared_budget_paces_starts_and_reserves_mandatory_capacity() {
        timeout(Duration::from_secs(2), async {
            let config = Config {
                connection_concurrency: 2,
                connection_interval: Duration::from_millis(10),
                ..Config::default()
            };
            let budget = ConnectionBudget::new(&config).unwrap();
            // Hold the pacing lock to observe a genuinely pending admission,
            // rather than inferring blocked work from an arbitrary quiet period.
            let gate = budget.next_start.lock().await;
            let first = budget.acquire(true);
            tokio::pin!(first);
            assert!(futures_util::poll!(&mut first).is_pending());
            drop(gate);
            let first = first.await.unwrap();
            assert_eq!(budget.all.available_permits(), 1);
            assert_eq!(budget.planned.available_permits(), 0);

            let voluntary = budget.acquire(true);
            tokio::pin!(voluntary);
            assert!(futures_util::poll!(&mut voluntary).is_pending());
            // The pending voluntary attempt must not consume the reserved
            // global lease or block a mandatory acquisition on the pacing lock.
            let mandatory = budget.acquire(false).await.unwrap();
            assert!(mandatory.started.duration_since(first.started) >= config.connection_interval);
            assert!(futures_util::poll!(&mut voluntary).is_pending());
            assert_eq!(budget.all.available_permits(), 0);

            let previous_start = mandatory.started;
            drop(first);
            let voluntary = voluntary.await.unwrap();
            assert!(voluntary.started.duration_since(previous_start) >= config.connection_interval);
            let next_mandatory = budget.acquire(false);
            tokio::pin!(next_mandatory);
            assert!(futures_util::poll!(&mut next_mandatory).is_pending());
            let previous_start = voluntary.started;
            drop(voluntary);
            let next_mandatory = next_mandatory.await.unwrap();
            assert!(
                next_mandatory.started.duration_since(previous_start) >= config.connection_interval
            );
            drop((mandatory, next_mandatory));
            assert_eq!(budget.all.available_permits(), 2);
            assert_eq!(budget.planned.available_permits(), 1);
        })
        .await
        .expect("shared connection budget fixture exceeded its deadline");
    }

    async fn socket_pair() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
        // Smaller than the unsubscribe frame: send cannot flush until the peer reads.
        let (client, peer) = duplex(8);
        (
            WebSocketStream::from_raw_socket(client, Role::Client, None).await,
            WebSocketStream::from_raw_socket(peer, Role::Server, None).await,
        )
    }

    #[tokio::test]
    async fn pending_websocket_send_is_cancelled_by_shutdown() {
        let (mut client, _peer) = socket_pair().await;
        let (stop, mut receiver) = watch::channel(false);
        let send = io(
            client.send(Message::Text(UNSUBSCRIBE.into())),
            Duration::from_secs(5),
            &mut receiver,
        );
        tokio::pin!(send);
        assert!(futures_util::poll!(&mut send).is_pending());

        // The peer remains alive and unread; only shutdown can release this send
        // before its write timeout. Silence on a real TCP socket is not this proof.
        stop.send_replace(true);
        let error = timeout(Duration::from_millis(250), &mut send)
            .await
            .expect("shutdown did not cancel the pending send")
            .expect_err("a blocked send cannot succeed without the peer reading");
        assert_eq!(error.to_string(), "shutdown");
    }

    #[tokio::test]
    async fn pending_websocket_send_succeeds_when_peer_reads() {
        let (mut client, mut peer) = socket_pair().await;
        let (stop, mut receiver) = watch::channel(false);
        let send = io(
            client.send(Message::Text(UNSUBSCRIBE.into())),
            Duration::from_secs(5),
            &mut receiver,
        );
        tokio::pin!(send);
        assert!(futures_util::poll!(&mut send).is_pending());

        let (sent, received) = timeout(Duration::from_secs(1), async {
            tokio::join!(&mut send, peer.next())
        })
        .await
        .expect("reading the peer did not release the pending send");
        sent.unwrap();
        assert_eq!(
            received.unwrap().unwrap(),
            Message::Text(UNSUBSCRIBE.into())
        );
        assert!(!*stop.borrow());
    }

    #[tokio::test]
    async fn pending_websocket_send_times_out_without_shutdown() {
        let (mut client, _peer) = socket_pair().await;
        let (stop, mut receiver) = watch::channel(false);
        let send = io(
            client.send(Message::Text(UNSUBSCRIBE.into())),
            Duration::from_millis(100),
            &mut receiver,
        );
        tokio::pin!(send);
        assert!(futures_util::poll!(&mut send).is_pending());

        let error = timeout(Duration::from_secs(1), &mut send)
            .await
            .expect("pending send exceeded its write timeout")
            .expect_err("a blocked send cannot succeed without the peer reading");
        assert_eq!(
            error.to_string(),
            "socket write timeout; delivery uncertain"
        );
        assert!(!*stop.borrow());
    }
}
