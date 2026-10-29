use super::{
    Config, RecoveryRequest, WorkerCommand, WorkerReport, ingress, stopping,
    worker::{ConnectionBudget, run_worker_shared},
};
use crate::discovery::{AdapterId, MarketKey, Snapshot, polymarket::Command};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::{Instant, interval, timeout},
};

pub struct AssignmentChange {
    pub worker: usize,
    pub adapter: AdapterId,
    pub slot: u16,
    pub revision: u64,
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

/// Borrowed assignment sample; admission does not imply a successful wire write.
pub struct Assignment<'a> {
    pub market: &'a MarketKey,
    pub worker: usize,
    pub slot: u16,
    pub revision: u64,
    pub assets: &'a [String],
}

struct MarketAssignment {
    workers: Vec<usize>,
    revision: u64,
    assets: Vec<String>,
}

#[derive(Clone)]
struct WorkerLoad {
    adapter: AdapterId,
    slot: u16,
    assets: usize,
}

/// Ephemeral control-plane simulation. It owns only changed markets, not a
/// second persistent eligibility registry. No effects escape failed preflight.
struct Plan {
    revision: u64,
    workers: Vec<WorkerLoad>,
    removed: Vec<MarketKey>,
    added: Vec<(MarketKey, MarketAssignment)>,
    changes: Vec<AssignmentChange>,
}

pub struct Assignments {
    markets: HashMap<MarketKey, MarketAssignment>,
    workers: Vec<WorkerLoad>,
    capacity: usize,
    maximum: usize,
    replicas: u16,
    revision: u64,
}

impl Assignments {
    pub fn new(capacity: usize, maximum: usize) -> Self {
        Self::with_replicas(capacity, maximum, 1)
    }
    /// Configuration is validated before any reconciliation effects.
    pub fn with_replicas(capacity: usize, maximum: usize, replicas: u16) -> Self {
        Self {
            markets: HashMap::new(),
            workers: vec![],
            capacity,
            maximum,
            replicas,
            revision: 0,
        }
    }
    pub fn assigned_assets(&self) -> usize {
        // prepare checks capacity * maximum before changing any loads.
        self.workers.iter().map(|load| load.assets).sum()
    }
    pub fn current(&self) -> impl Iterator<Item = Assignment<'_>> {
        self.markets.iter().flat_map(move |(market, assignment)| {
            assignment.workers.iter().map(move |worker| Assignment {
                market,
                worker: *worker,
                slot: self.workers[*worker].slot,
                revision: assignment.revision,
                assets: &assignment.assets,
            })
        })
    }
    pub fn reconcile(&mut self, snapshot: &Snapshot) -> Result<Vec<AssignmentChange>> {
        let mut plan = self.prepare(snapshot)?;
        let changes = std::mem::take(&mut plan.changes);
        self.commit(plan);
        Ok(changes)
    }

    fn prepare(&self, snapshot: &Snapshot) -> Result<Plan> {
        ensure!(
            self.capacity > 0 && self.maximum > 0 && matches!(self.replicas, 1 | 2),
            "invalid assignment capacity/replicas"
        );
        self.capacity
            .checked_mul(self.maximum)
            .context("assignment capacity overflow")?;
        let revision = self
            .revision
            .checked_add(1)
            .context("assignment revision exhausted")?;
        let mut workers = self.workers.clone();
        let mut changes: BTreeMap<usize, AssignmentChange> = BTreeMap::new();
        let mut removed: Vec<_> = self
            .markets
            .keys()
            .filter(|key| !snapshot.markets.get(*key).is_some_and(|m| m.desired))
            .cloned()
            .collect();
        removed.sort_by(|a, b| a.adapter.cmp(&b.adapter).then(a.id.cmp(&b.id)));
        for key in &removed {
            let assignment = &self.markets[key];
            for &worker in &assignment.workers {
                workers[worker].assets = workers[worker]
                    .assets
                    .checked_sub(assignment.assets.len())
                    .context("assignment load underflow")?;
                changes
                    .entry(worker)
                    .or_insert_with(|| AssignmentChange {
                        worker,
                        adapter: key.adapter,
                        slot: workers[worker].slot,
                        revision,
                        add: vec![],
                        remove: vec![],
                    })
                    .remove
                    .extend(assignment.assets.iter().cloned());
            }
        }
        // C1 must retire an old identity before reassigning it. Silently keeping
        // obsolete IDs under an unchanged market key would falsely certify coverage.
        for (key, assignment) in &self.markets {
            if let Some(market) = snapshot.markets.get(key).filter(|market| market.desired) {
                ensure!(
                    market
                        .instruments
                        .iter()
                        .map(|i| &i.id)
                        .eq(assignment.assets.iter()),
                    "desired identity changed without retirement: {key:?}; assigned={:?}; desired={:?}",
                    assignment.assets,
                    market.instruments
                );
            }
        }
        let mut additions: Vec<_> = snapshot
            .markets
            .iter()
            .filter(|(key, m)| m.desired && !self.markets.contains_key(*key))
            .collect();
        additions.sort_by(|(a, _), (b, _)| a.adapter.cmp(&b.adapter).then(a.id.cmp(&b.id)));
        let mut added = Vec::new();
        let mut unassigned = Vec::new();
        for (key, market) in additions {
            let count = market.instruments.len();
            let assets: Vec<_> = market.instruments.iter().map(|i| i.id.clone()).collect();
            let mut selected = Vec::with_capacity(usize::from(self.replicas));
            let old_len = workers.len();
            if count > 0 && count <= self.capacity {
                for slot in 0..self.replicas {
                    let worker = workers
                        .iter()
                        .enumerate()
                        .filter(|(_, load)| {
                            load.adapter == key.adapter
                                && load.slot == slot
                                && load.assets <= self.capacity - count
                        })
                        .min_by_key(|(id, load)| (load.assets, *id))
                        .map(|(id, _)| id);
                    let worker = match worker {
                        Some(worker) => worker,
                        None if workers.len() < self.maximum => {
                            workers.push(WorkerLoad {
                                adapter: key.adapter,
                                slot,
                                assets: 0,
                            });
                            workers.len() - 1
                        }
                        None => break,
                    };
                    workers[worker].assets = workers[worker]
                        .assets
                        .checked_add(count)
                        .context("assignment load overflow")?;
                    selected.push(worker);
                }
            }
            if selected.len() != usize::from(self.replicas) {
                for worker in selected {
                    workers[worker].assets -= count;
                }
                workers.truncate(old_len);
                unassigned.push((key.clone(), assets));
                continue;
            }
            for &worker in &selected {
                changes
                    .entry(worker)
                    .or_insert_with(|| AssignmentChange {
                        worker,
                        adapter: key.adapter,
                        slot: workers[worker].slot,
                        revision,
                        add: vec![],
                        remove: vec![],
                    })
                    .add
                    .extend(assets.iter().cloned());
            }
            added.push((
                key.clone(),
                MarketAssignment {
                    workers: selected,
                    revision,
                    assets,
                },
            ));
        }
        if !unassigned.is_empty() {
            let blocked: Vec<_> = unassigned.iter().map(|(key, _)| key.clone()).collect();
            unassigned.extend(
                added
                    .into_iter()
                    .map(|(key, assignment)| (key, assignment.assets)),
            );
            unassigned.sort_by(|(a, _), (b, _)| a.adapter.cmp(&b.adapter).then(a.id.cmp(&b.id)));
            anyhow::bail!(
                "socket capacity preflight failed; replicas={}; physical_limit={}; packing_blocked={blocked:?}; exact unassigned markets/assets={unassigned:?}; no reconciliation committed",
                self.replicas,
                self.maximum
            );
        }
        Ok(Plan {
            revision,
            workers,
            removed,
            added,
            changes: changes.into_values().collect(),
        })
    }

    fn commit(&mut self, plan: Plan) {
        for key in plan.removed {
            self.markets.remove(&key);
        }
        self.markets.extend(plan.added);
        self.workers = plan.workers;
        self.revision = plan.revision;
    }
}

struct DesiredDelivery {
    fence: ingress::Message,
    commands: BTreeMap<usize, WorkerCommand>,
}

impl Plan {
    /// Deduplicate the global fence across replicas, retaining exact per-worker
    /// routing. Removal chunks precede additions within each adapter so a reused
    /// worker never transiently exceeds its assignment capacity.
    fn deliveries(&self, count_limit: usize, byte_limit: usize) -> Result<Vec<DesiredDelivery>> {
        ensure!(
            count_limit > 0,
            "desired chunk count limit must be positive"
        );
        let mut routes: BTreeMap<(AdapterId, bool, String), Vec<usize>> = BTreeMap::new();
        for change in &self.changes {
            for (adding, ids) in [(false, &change.remove), (true, &change.add)] {
                for id in ids {
                    routes
                        .entry((change.adapter, adding, id.clone()))
                        .or_default()
                        .push(change.worker);
                }
            }
        }
        let mut routes = routes.into_iter().peekable();
        let mut deliveries = Vec::new();
        while let Some(((adapter, adding, _), _)) = routes.peek() {
            let (adapter, adding) = (*adapter, *adding);
            let mut ids = Vec::new();
            let mut commands: BTreeMap<usize, WorkerCommand> = BTreeMap::new();
            let mut bytes = std::mem::size_of::<ingress::Envelope>();
            while let Some(((next_adapter, next_adding, id), _)) = routes.peek() {
                if *next_adapter != adapter || *next_adding != adding || ids.len() == count_limit {
                    break;
                }
                let next_bytes = bytes
                    .checked_add(std::mem::size_of::<String>())
                    .and_then(|bytes| bytes.checked_add(id.capacity()))
                    .context("desired chunk byte arithmetic overflow")?;
                if next_bytes > byte_limit {
                    ensure!(
                        !ids.is_empty(),
                        "desired identity cannot fit ingress budget: adapter={adapter:?} id={id:?}"
                    );
                    break;
                }
                bytes = next_bytes;
                let ((_, _, id), workers) = routes.next().expect("peeked route");
                for worker in workers {
                    let command = commands.entry(worker).or_insert_with(|| WorkerCommand {
                        revision: self.revision,
                        ..WorkerCommand::default()
                    });
                    if adding {
                        command.add.push(id.clone());
                    } else {
                        command.remove.push(id.clone());
                    }
                }
                ids.push(id);
            }
            // Tight vector capacity makes the preflight match actual accounting,
            // rather than budgeting a Vec's geometric allocation slack.
            let ids = ids.into_boxed_slice().into_vec();
            let (add, remove) = if adding { (ids, vec![]) } else { (vec![], ids) };
            let fence = ingress::Message::DesiredChunk {
                adapter,
                revision: self.revision,
                index: u32::try_from(deliveries.len())?,
                add,
                remove,
            };
            ensure!(
                fence.accounted_bytes()? <= byte_limit,
                "desired chunk exceeds ingress budget"
            );
            deliveries.push(DesiredDelivery { fence, commands });
        }
        // Include Begin/Complete in preflight, even for a zero-chunk revision.
        ensure!(
            ingress::Message::ReconcileBegin {
                revision: self.revision,
                chunks: u32::try_from(deliveries.len())?,
            }
            .accounted_bytes()?
                <= byte_limit,
            "reconciliation boundary exceeds ingress budget"
        );
        Ok(deliveries)
    }
}

#[derive(Clone, Copy)]
struct PendingControl {
    session: ingress::Session,
    restart: Option<bool>,
    initialized: bool,
    queued: Instant,
    deadline: Instant,
    deferred: bool,
}

/// One bounded combined recovery/readiness intent per physical worker. Keep
/// delivered flags too: duplicate observations must not refill a worker channel
/// or extend the delivery deadline. A mandatory request supersedes planned work.
#[derive(Default)]
struct ControlDelivery {
    session: Option<ingress::Session>,
    delivered_restart: Option<bool>,
    delivered_initialized: bool,
    pending: Option<PendingControl>,
}

impl ControlDelivery {
    fn offer(&mut self, request: RecoveryRequest) -> Result<()> {
        let session = match request {
            RecoveryRequest::Restart { session, .. } | RecoveryRequest::Initialized(session) => {
                session
            }
        };
        if let Some(current) = self.session
            && session.generation < current.generation
        {
            return Ok(());
        }
        if self.session != Some(session) {
            if let Some(pending) = self.pending {
                eprintln!(
                    "recovery_control_superseded old={:?} new={session:?}",
                    pending.session
                );
            }
            *self = Self {
                session: Some(session),
                ..Self::default()
            };
        }
        let (restart, initialized) = match request {
            RecoveryRequest::Restart { planned, .. } => {
                if self.delivered_restart == Some(false)
                    || (planned && self.delivered_restart.is_some())
                {
                    return Ok(());
                }
                (Some(planned), false)
            }
            RecoveryRequest::Initialized(_) => {
                if self.delivered_initialized {
                    return Ok(());
                }
                (None, true)
            }
        };
        if self.pending.is_none() {
            let queued = Instant::now();
            self.pending = Some(PendingControl {
                session,
                restart: None,
                initialized: false,
                queued,
                deadline: queued
                    .checked_add(Duration::from_secs(30))
                    .context("control deadline overflow")?,
                deferred: false,
            });
        }
        let pending = self.pending.as_mut().expect("pending control");
        pending.initialized |= initialized;
        if let Some(planned) = restart {
            pending.restart = Some(pending.restart.unwrap_or(true) && planned);
        }
        Ok(())
    }

    fn flush(&mut self, sender: &mpsc::Sender<WorkerCommand>) -> Result<()> {
        let Some(pending) = self.pending else {
            return Ok(());
        };
        ensure!(
            Instant::now() < pending.deadline,
            "recovery/control delivery expired after 30s: session={:?} restart={:?} initialized={}",
            pending.session,
            pending.restart,
            pending.initialized
        );
        let command = WorkerCommand {
            restart: pending.restart.map(|_| pending.session),
            planned_restart: pending.restart.unwrap_or(false),
            initialized: pending.initialized.then_some(pending.session),
            ..WorkerCommand::default()
        };
        match sender.try_send(command) {
            Ok(()) => {
                self.delivered_initialized |= pending.initialized;
                if pending.restart.is_some() {
                    self.delivered_restart = pending.restart;
                }
                self.pending = None;
                eprintln!(
                    "recovery_control_delivered session={:?} restart={:?} initialized={} wait_ms={}",
                    pending.session,
                    pending.restart,
                    pending.initialized,
                    pending.queued.elapsed().as_millis()
                );
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !pending.deferred {
                    eprintln!(
                        "recovery_control_deferred session={:?} deadline_ms=30000",
                        pending.session
                    );
                    self.pending.as_mut().expect("pending control").deferred = true;
                }
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => anyhow::bail!(
                "recovery/control worker channel closed: session={:?}",
                pending.session
            ),
        }
    }
}

fn flush_controls(
    controls: &mut [ControlDelivery],
    commands: &[mpsc::Sender<WorkerCommand>],
) -> Result<()> {
    for (control, command) in controls.iter_mut().zip(commands) {
        control.flush(command)?;
    }
    Ok(())
}

fn accept_control(
    request: RecoveryRequest,
    loads: &[WorkerLoad],
    controls: &mut [ControlDelivery],
    commands: &[mpsc::Sender<WorkerCommand>],
) -> Result<()> {
    let session = match request {
        RecoveryRequest::Restart { session, .. } | RecoveryRequest::Initialized(session) => session,
    };
    ensure!(
        session.generation > 0
            && loads
                .get(session.worker)
                .is_some_and(|load| load.adapter == session.adapter && load.slot == session.slot),
        "unknown recovery assignment/session: {session:?}"
    );
    controls[session.worker].offer(request)?;
    controls[session.worker].flush(&commands[session.worker])
}

/// Notifications are wakeups, not a change log. Polling repairs a missed final
/// wakeup and bounds reconciliation to once per second during large scans.
pub async fn run_manager(
    discovery: mpsc::Sender<Command>,
    mut changes: watch::Receiver<()>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
    mut recovery: mpsc::Receiver<RecoveryRequest>,
) -> Result<Vec<WorkerReport>> {
    let budget = match ConnectionBudget::new(&config) {
        Ok(budget) => budget,
        Err(error) => {
            stop.send_replace(true);
            return Err(error);
        }
    };
    let mut shutdown = stop.subscribe();
    let mut assignments = Assignments::with_replicas(
        config.assets_per_socket,
        config.max_sockets,
        config.replicas_per_market,
    );
    let mut commands: Vec<mpsc::Sender<WorkerCommand>> = vec![];
    let mut controls: Vec<ControlDelivery> = vec![];
    let mut workers: JoinSet<Result<WorkerReport>> = JoinSet::new();
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut control_tick = interval(Duration::from_millis(100));
    control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut repair = 5;
    let mut recovery_open = true;
    let mut stopped_first = vec![];
    let result: Result<()> = async {
        loop {
            tokio::select! {
                biased;
                _ = stopping(&mut shutdown) => return Ok(()),
                finished = workers.join_next(), if !workers.is_empty() => {
                    let report = finished.context("worker set ended")???;
                    // A worker exits cleanly only after observing stop, yet one
                    // was seen joined before the stop branch above fired. Read the
                    // flag itself: a clean exit after stop is shutdown, not a
                    // failure, and its report is kept with the others.
                    if *shutdown.borrow() {
                        stopped_first.push(report);
                        return Ok(());
                    }
                    anyhow::bail!("worker exited before shutdown: {report:?}");
                },
                _ = control_tick.tick() => flush_controls(&mut controls, &commands)?,
                _ = tick.tick() => {
                    repair += 1;
                    if repair < 5 && !changes.has_changed().context("discovery notifications closed")? {
                        continue;
                    }
                    repair = 0;
                    changes.borrow_and_update(); // Consume BEFORE requesting the authoritative state.
                    let request = async {
                        let (reply, receive) = oneshot::channel();
                        discovery.send(Command::Snapshot(reply)).await.context("discovery stopped")?;
                        receive.await.context("discovery snapshot lost")
                    };
                    let request = timeout(config.io_timeout, request);
                    tokio::pin!(request);
                    let snapshot = loop {
                        tokio::select! {
                            biased;
                            _ = stopping(&mut shutdown) => return Ok(()),
                            _ = control_tick.tick() => flush_controls(&mut controls, &commands)?,
                            result = &mut request => break result.context("discovery snapshot timeout")??,
                            control = recovery.recv(), if recovery_open => {
                                if let Some(control) = control {
                                    accept_control(control, &assignments.workers, &mut controls, &commands)?;
                                } else { recovery_open = false; }
                            },
                        }
                    };
                    let plan = assignments.prepare(&snapshot)?;
                    let deliveries = plan.deliveries(config.subscription_batch, output.byte_limit())?;
                    let revision = plan.revision;
                    let chunks = u32::try_from(deliveries.len())?;
                    // Preflight can outlast the intake cutoff on a large universe.
                    // Do not publish a new reconciliation after shutdown.
                    if *shutdown.borrow() { return Ok(()); }
                    output.send(ingress::Message::ReconcileBegin { revision, chunks })
                        .with_context(|| format!("reconciliation {revision} Begin rejected"))?;
                    // Workers start empty; no subscription can precede its fence.
                    for worker in commands.len()..plan.workers.len() {
                        let load = &plan.workers[worker];
                        let (send, receive) = mpsc::channel(32);
                        commands.push(send);
                        controls.push(ControlDelivery::default());
                        workers.spawn(run_worker_shared(
                            ingress::Session { worker, adapter: load.adapter, slot: load.slot, generation: 0 },
                            vec![], receive, output.clone(), config.clone(), stop.clone(), budget.clone(),
                        ));
                    }
                    let mut control_poll = Instant::now();
                    for (index, delivery) in deliveries.into_iter().enumerate() {
                        ensure!(!*shutdown.borrow(), "incomplete reconciliation {revision}: shutdown before chunk {index}");
                        output.send(delivery.fence)
                            .with_context(|| format!("incomplete reconciliation {revision}: chunk {index} rejected"))?;
                        for (worker, command) in delivery.commands {
                            ensure!(!*shutdown.borrow(), "incomplete reconciliation {revision}: shutdown before chunk {index} worker {worker}");
                            commands[worker].try_send(command)
                                .with_context(|| format!("incomplete reconciliation {revision}: chunk {index} accepted, worker {worker} delivery failed"))?;
                        }
                        // This is scheduling fairness, not queue retry or a rollback.
                        tokio::task::yield_now().await;
                        if control_poll.elapsed() >= Duration::from_millis(100) {
                            flush_controls(&mut controls, &commands)
                                .with_context(|| format!("incomplete reconciliation {revision} after chunk {index}: control delivery failed"))?;
                            control_poll = Instant::now();
                        }
                    }
                    // All fences and commands have been accepted. Shutdown does
                    // not undo delivery, including an empty reconciliation.
                    // Partial delivery still fails at the checks inside the loop.
                    output.send(ingress::Message::ReconcileComplete { revision })
                        .with_context(|| format!("incomplete reconciliation {revision}: all commands delivered, Complete rejected"))?;
                    assignments.commit(plan);
                },
                request = recovery.recv(), if recovery_open => {
                    if let Some(request) = request {
                        accept_control(request, &assignments.workers, &mut controls, &commands)?;
                    } else { recovery_open = false; }
                },
            }
        }
    }.await;
    stop.send_replace(true);
    for control in &controls {
        if let Some(pending) = control.pending {
            eprintln!(
                "recovery_control_cancelled session={:?} reason=manager_stopping delivered=false wait_ms={}",
                pending.session,
                pending.queued.elapsed().as_millis()
            );
        }
    }
    let joined = timeout(config.shutdown_timeout, async {
        let mut reports = stopped_first;
        let mut error = None;
        while let Some(result) = workers.join_next().await {
            match result {
                Ok(Ok(report)) => reports.push(report),
                Ok(Err(e)) => {
                    error.get_or_insert(e);
                }
                Err(e) => {
                    error.get_or_insert(e.into());
                }
            }
        }
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(reports)
        }
    })
    .await;
    if joined.is_err() {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    result?;
    joined.context("workers failed to stop before deadline; incomplete drain")?
}
