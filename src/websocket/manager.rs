use super::{Config, WorkerCommand, WorkerReport, ingress, run_worker, stopping};
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
    time::{interval, timeout},
};

pub struct AssignmentChange {
    pub worker: usize,
    pub adapter: AdapterId,
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

pub struct Assignments {
    markets: HashMap<MarketKey, (usize, Vec<String>)>,
    workers: Vec<(AdapterId, usize)>,
    capacity: usize,
    maximum: usize,
}

impl Assignments {
    pub fn new(capacity: usize, maximum: usize) -> Self {
        Self {
            markets: HashMap::new(),
            workers: vec![],
            capacity,
            maximum,
        }
    }
    pub fn assigned_assets(&self) -> usize {
        self.workers.iter().map(|(_, load)| load).sum()
    }
    pub fn reconcile(&mut self, snapshot: &Snapshot) -> Result<Vec<AssignmentChange>> {
        let mut changes: BTreeMap<usize, AssignmentChange> = BTreeMap::new();
        let removed: Vec<_> = self
            .markets
            .keys()
            .filter(|key| !snapshot.markets.get(*key).is_some_and(|m| m.desired))
            .cloned()
            .collect();
        for key in removed {
            let (worker, assets) = self.markets.remove(&key).expect("existing assignment");
            self.workers[worker].1 -= assets.len();
            changes
                .entry(worker)
                .or_insert_with(|| AssignmentChange {
                    worker,
                    adapter: key.adapter,
                    add: vec![],
                    remove: vec![],
                })
                .remove
                .extend(assets);
        }
        let mut additions: Vec<_> = snapshot
            .markets
            .iter()
            .filter(|(key, m)| m.desired && !self.markets.contains_key(*key))
            .collect();
        additions.sort_by(|(a, _), (b, _)| a.adapter.cmp(&b.adapter).then(a.id.cmp(&b.id)));
        for (key, market) in additions {
            let count = market.instruments.len();
            ensure!(
                count > 0 && count <= self.capacity,
                "market does not fit a socket budget"
            );
            let worker = self
                .workers
                .iter()
                .enumerate()
                .filter(|(_, (adapter, load))| {
                    *adapter == key.adapter && *load + count <= self.capacity
                })
                .min_by_key(|(i, (_, load))| (*load, *i))
                .map(|(i, _)| i);
            let worker = match worker {
                Some(worker) => worker,
                None => {
                    ensure!(
                        self.workers.len() < self.maximum,
                        "socket capacity exhausted; unassigned desired instruments"
                    );
                    self.workers.push((key.adapter, 0));
                    self.workers.len() - 1
                }
            };
            let assets: Vec<_> = market.instruments.iter().map(|i| i.id.clone()).collect();
            self.workers[worker].1 += count;
            changes
                .entry(worker)
                .or_insert_with(|| AssignmentChange {
                    worker,
                    adapter: key.adapter,
                    add: vec![],
                    remove: vec![],
                })
                .add
                .extend(assets.iter().cloned());
            self.markets.insert(key.clone(), (worker, assets));
        }
        Ok(changes.into_values().collect())
    }
}

/// Notifications are wakeups, not a change log. Polling repairs a missed final
/// wakeup and bounds reconciliation to once per second during large scans.
pub async fn run_manager(
    discovery: mpsc::Sender<Command>,
    mut changes: watch::Receiver<()>,
    output: ingress::Sender,
    config: Arc<Config>,
    stop: watch::Sender<bool>,
) -> Result<Vec<WorkerReport>> {
    config.validate()?;
    let mut shutdown = stop.subscribe();
    let mut assignments = Assignments::new(config.assets_per_socket, config.max_sockets);
    let mut commands: Vec<mpsc::Sender<WorkerCommand>> = vec![];
    let mut workers: JoinSet<Result<WorkerReport>> = JoinSet::new();
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut repair = 5;
    let result: Result<()> = async {
        loop {
            tokio::select! {
                biased;
                _ = stopping(&mut shutdown) => return Ok(()),
                finished = workers.join_next(), if !workers.is_empty() => {
                    finished.context("worker set ended")???;
                    anyhow::bail!("worker exited before shutdown");
                },
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
                    let snapshot = tokio::select! {
                        biased;
                        _ = stopping(&mut shutdown) => return Ok(()),
                        result = timeout(config.io_timeout, request) => result.context("discovery snapshot timeout")??,
                    };
                    for change in assignments.reconcile(&snapshot)? {
                        if change.worker == commands.len() {
                            let (send, receive) = mpsc::channel(32);
                            commands.push(send);
                            workers.spawn(run_worker(
                                change.worker, change.adapter, change.add, receive,
                                output.clone(), config.clone(), stop.clone(),
                            ));
                        } else {
                            commands[change.worker].try_send(WorkerCommand {
                                add: change.add,
                                remove: change.remove,
                            }).context("assignment delivery failed")?;
                        }
                    }
                }
            }
        }
    }.await;
    stop.send_replace(true);
    let joined = timeout(config.shutdown_timeout, async {
        let mut reports = vec![];
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
