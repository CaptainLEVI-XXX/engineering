//! The market-data owner thread: one synchronous consumer of accepted ingress
//! that decodes (C3), maintains books (C4) and records output (C5) in order.
use crate::{
    discovery::polymarket::Command,
    orderbook::{self, ObservationKind, Processor},
    recording,
    websocket::{
        RecoveryRequest,
        ingress::{self, Message, Session},
    },
};
use anyhow::{Context, Result, ensure};
use std::{collections::HashMap, time::Duration};
use tokio::sync::{mpsc, watch};

/// Diagnostic consumer only. Drain the accepted prefix after stop; never stop
/// reading just because intake stopped. Financial validation belongs downstream.
pub(super) fn validate_transport(
    receiver: crossbeam::channel::Receiver<ingress::Envelope>,
) -> Result<u64> {
    let mut sessions: HashMap<usize, (Session, u64, bool)> = HashMap::new();
    let mut messages = 0;
    for envelope in receiver {
        match &envelope.message {
            Message::ReconcileBegin { .. }
            | Message::DesiredChunk { .. }
            | Message::ReconcileComplete { .. } => {}
            Message::Started(session) => {
                if let Some((previous, _, active)) = sessions.get(&session.worker) {
                    ensure!(
                        !active
                            && session.generation > previous.generation
                            && session.adapter == previous.adapter,
                        "invalid session start order"
                    );
                }
                sessions.insert(session.worker, (*session, 0, true));
            }
            Message::Data {
                session, ordinal, ..
            } => {
                let (current, sequence, active) = sessions
                    .get_mut(&session.worker)
                    .context("data before session start")?;
                ensure!(
                    *active && *current == *session && *ordinal == *sequence + 1,
                    "superseded or unordered ingress"
                );
                *sequence = *ordinal;
                messages += 1;
            }
            Message::SubscriptionsRequested { session, .. }
            | Message::RequestsComplete { session } => {
                ensure!(
                    sessions
                        .get(&session.worker)
                        .is_some_and(|(current, _, active)| *active && *current == *session),
                    "request outside active session"
                );
            }
            Message::Ended { session, reason } => {
                let (current, _, active) = sessions
                    .get_mut(&session.worker)
                    .context("end before session start")?;
                ensure!(*active && *current == *session, "unordered session end");
                *active = false;
                eprintln!(
                    "session_end worker={} generation={} adapter={:?} reason={reason}; no replay guarantee",
                    session.worker, session.generation, session.adapter
                );
            }
        }
    }
    ensure!(
        sessions.values().all(|(_, _, active)| !active),
        "drained prefix but session boundary missing; history incomplete"
    );
    Ok(messages)
}

#[derive(Debug)]
pub(super) struct ConsumerReport {
    pub(super) messages: u64,
    pub(super) diagnostics: String,
}

/// Committed depth across live replicas, sampled during the run (not at
/// shutdown, when every session has ended and no replica is live). Sizes
/// `books.total_levels` from an observed universe instead of a guess.
#[derive(Debug, Default, PartialEq, serde::Serialize)]
pub(super) struct DepthSummary {
    /// Owner time of the reported sample: the deepest one taken.
    pub(super) sampled_at_ms: u64,
    pub(super) live_replicas: usize,
    pub(super) levels: usize,
    pub(super) p50: usize,
    pub(super) p95: usize,
    pub(super) p99: usize,
    pub(super) max: usize,
}
impl DepthSummary {
    /// Nearest-rank percentiles over levels per live replica.
    pub(super) fn from_levels(mut levels: Vec<usize>) -> Self {
        levels.sort_unstable();
        // Nearest rank: the smallest value with at least p% of samples <= it.
        let rank = |p: usize| match levels.len() {
            0 => 0,
            n => levels[(n * p).div_ceil(100).max(1) - 1],
        };
        Self {
            sampled_at_ms: 0,
            live_replicas: levels.len(),
            levels: levels.iter().sum(),
            p50: rank(50),
            p95: rank(95),
            p99: rank(99),
            max: levels.last().copied().unwrap_or(0),
        }
    }
    fn of(processor: &Processor, at: Duration) -> Self {
        let mut sample = Self::from_levels(
            processor
                .coverage()
                .filter(|r| r.state == orderbook::Validity::Live)
                .map(|r| r.levels)
                .collect(),
        );
        sample.sampled_at_ms = u64::try_from(at.as_millis()).unwrap_or(u64::MAX);
        sample
    }
    /// Keep whichever sample held more committed levels; on a tie the newer
    /// one, so an idle run still reports its live replica count.
    fn deeper(self, newer: Self) -> Self {
        if newer.levels >= self.levels {
            newer
        } else {
            self
        }
    }
}

/// One owner-thread scan of every replica per interval: the same cadence and
/// order of cost as the minute snapshots, absorbed by the bounded ingress.
const DEPTH_SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

pub(super) fn process_books(
    receiver: crossbeam::channel::Receiver<ingress::Envelope>,
    config: orderbook::Config,
    recovery: mpsc::Sender<RecoveryRequest>,
    stop: watch::Receiver<bool>,
    mut recorder: Option<recording::Recorder>,
    discovery: Option<mpsc::Sender<Command>>,
    start: std::time::Instant,
) -> Result<ConsumerReport> {
    if let Some(recorder) = &mut recorder {
        recorder.control(
            0,
            &serde_json::json!({"kind":"policy","selection":format!("{:?}",config.selection),
            "monitoring":config.monitoring,"renewal_seconds":config.renewal_interval.as_secs(),
            "health_revisit_seconds":10,"ingress_lag_budget_seconds":1,
            "relative_zero_peer_min":5,"relative_rate_peer_min":20,"relative_percent":10,
            "confirmation_seconds":10,"absolute_baseline_minutes":3,"absolute_low_minutes":2,
            "initialization_seconds":config.initialization_timeout.as_secs(),
            "coverage_escalation_seconds":config.recovery_timeout.as_secs()}),
        )?;
    }
    let mut processor = Processor::with_receive_clock(config, start)?;
    let mut next_tick = Duration::ZERO;
    let mut depth = DepthSummary::default();
    let mut next_depth = DEPTH_SAMPLE_INTERVAL;
    let mut sampled_at_stop = false;
    let mut invalidations = 0u64;
    let mut samples = Vec::new();
    let result: Result<()> = (|| {
        loop {
            if *stop.borrow() {
                processor.stop_snapshots(start.elapsed());
            }
            let mut wait = Duration::from_millis(100);
            if let Some(recorder) = &recorder {
                recorder.check_output()?;
                if let Some(at) = recorder.next_publish_at() {
                    wait = wait.min(at.saturating_duration_since(std::time::Instant::now()));
                }
            }
            if !*stop.borrow() {
                wait = wait.min(next_tick.saturating_sub(start.elapsed()));
                if let Some(due) = processor.next_snapshot_due() {
                    wait = wait.min(due.saturating_sub(start.elapsed()));
                }
            }
            let mut sink = |observation: orderbook::Observation<'_>,
                            recorder: &mut Option<recording::Recorder>| {
                if let ObservationKind::Invalidated { reason, .. } = observation.kind {
                    invalidations += 1;
                    if samples.len() < 4 {
                        samples.push(format!(
                            "input={} binding={:?} asset={:.128} reason={:.128}",
                            observation.input, observation.binding, observation.asset, reason
                        ));
                    }
                }
                if let Some(recorder) = recorder {
                    recorder.observe(observation)?;
                }
                Ok(())
            };
            let envelope = match receiver.recv_timeout(wait) {
                Ok(envelope) => {
                    // Workers send shutdown boundaries only after observing
                    // stop, so checking here, before processing, samples the
                    // books while every session is still live.
                    if !sampled_at_stop && *stop.borrow() {
                        sampled_at_stop = true;
                        depth = std::mem::take(&mut depth)
                            .deeper(DepthSummary::of(&processor, start.elapsed()));
                    }
                    processor
                        .process(&envelope.message, start.elapsed(), &mut |o| {
                            sink(o, &mut recorder)
                        })
                        .context("C3/C4 processing failed; accepted tail incomplete")?;
                    Some(envelope)
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => None,
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
            };
            if let Some(envelope) = envelope
                && let Some(recorder) = &mut recorder
            {
                record_control(recorder, processor.input_cutoff(), &envelope.message)?;
            }
            if !*stop.borrow() {
                let now = start.elapsed();
                if now >= next_depth {
                    depth = std::mem::take(&mut depth).deeper(DepthSummary::of(&processor, now));
                    next_depth = now + DEPTH_SAMPLE_INTERVAL;
                }
                if now >= next_tick {
                    processor.tick(now)?;
                    // A tick is a bounded slice, not necessarily all due work.
                    // Leave ingress a turn before another slice; idle books do
                    // not require polling at this catch-up cadence.
                    let after = start.elapsed();
                    let delay_ms = if processor.next_health_due().is_some_and(|due| due <= after) {
                        1
                    } else {
                        100
                    };
                    next_tick = after + Duration::from_millis(delay_ms);
                }
                processor.maintain(
                    start.elapsed(),
                    chrono::Utc::now(),
                    receiver.is_empty(),
                    &mut |o| sink(o, &mut recorder),
                )?;
                processor.capture_due(&mut || (start.elapsed(), chrono::Utc::now()), &mut |o| {
                    sink(o, &mut recorder)
                })?;
                while let Some((session, planned)) = processor.pending_recovery() {
                    match recovery.try_send(RecoveryRequest::Restart { session, planned }) {
                        Ok(()) => processor.recovery_delivered(session),
                        Err(mpsc::error::TrySendError::Full(_)) => break,
                        Err(error) => return Err(error).context("book recovery channel closed"),
                    }
                }
                let ready: Vec<_> = processor.initialized_sessions().collect();
                for session in ready {
                    match recovery.try_send(RecoveryRequest::Initialized(session)) {
                        Ok(()) => processor.initialization_delivered(session),
                        Err(mpsc::error::TrySendError::Full(_)) => break,
                        Err(error) => return Err(error).context("book readiness channel closed"),
                    }
                }
                if let Some(commands) = &discovery {
                    for _ in 0..64 {
                        let Some(key) = processor.pending_hint().cloned() else {
                            break;
                        };
                        match commands.try_send(Command::Hint(key.clone())) {
                            Ok(()) => processor.hint_delivered(&key),
                            Err(mpsc::error::TrySendError::Full(_)) => break,
                            Err(error) => {
                                return Err(error).context("discovery hint channel closed");
                            }
                        }
                    }
                    if processor.reconciliation_hint() {
                        match commands.try_send(Command::Reconcile) {
                            Ok(()) => processor.reconciliation_hint_delivered(),
                            Err(mpsc::error::TrySendError::Full(_)) => {}
                            Err(error) => {
                                return Err(error)
                                    .context("discovery reconciliation channel closed");
                            }
                        }
                    }
                }
            }
            if let Some(recorder) = &mut recorder {
                recorder.publish_due(std::time::Instant::now())?;
            }
        }
        processor.finish()
    })();
    let result = if let Some(mut recorder) = recorder {
        recorder.snapshot_stats(processor.stats());
        let evidence = recorder.control(
            processor.input_cutoff(),
            &serde_json::json!({"kind":"owner_summary",
            "stats":processor.stats(),"reconciliation":processor.reconciliation(),"depth":depth,
            "coverage_counts":"rolling owner samples, not full-universe acceptance"}),
        );
        recorder.finish(result.and(evidence))
    } else {
        result
    };
    let diagnostics = format!(
        "books {:?}; depth {depth:?}; invalidation_observations={invalidations} omitted_invalidation_details={} first_samples={samples:?}; asset/reason previews capped at 128 characters; initialization counts are observations, not full-universe coverage; replacement cannot recover missed history",
        processor.stats(),
        invalidations - samples.len() as u64
    );
    if let Err(error) = result {
        return Err(error).context(diagnostics);
    }
    Ok(ConsumerReport {
        messages: processor.stats().messages,
        diagnostics,
    })
}

fn record_control(recorder: &mut recording::Recorder, input: u64, message: &Message) -> Result<()> {
    use serde_json::json;
    let session = |s: &Session| json!({"worker":s.worker,"slot":s.slot,"generation":s.generation,"adapter":s.adapter});
    match message {
        Message::Data {..} => {},
        Message::DesiredChunk {adapter,revision,index,add,remove} => {
            recorder.control(input,&json!({"kind":"desired_chunk","adapter":adapter,"revision":revision,
                "chunk":index,"add_count":add.len(),"remove_count":remove.len()}))?;
            for (action,ids) in [("add",add),("remove",remove)] {
                for asset in ids {
                    recorder.control(input,&json!({"kind":"desired","adapter":adapter,"revision":revision,
                        "chunk":index,"action":action,"asset":asset}))?;
                }
            }
        },
        Message::SubscriptionsRequested {session:s,revisions,add,remove} => {
            for (i,asset) in add.iter().chain(remove).enumerate() {
                recorder.control(input,&json!({"kind":"requested","session":session(s),"revision":revisions[i],
                    "action":if i<add.len() {"add"} else {"remove"},"asset":asset}))?;
            }
        },
        Message::ReconcileBegin {revision,chunks} => recorder.control(input,&json!({"kind":"reconcile_begin","revision":revision,"chunks":chunks}))?,
        Message::ReconcileComplete {revision} => recorder.control(input,&json!({"kind":"reconcile_complete","revision":revision}))?,
        Message::Started(s) => recorder.control(input,&json!({"kind":"started","session":session(s)}))?,
        Message::RequestsComplete {session:s} => recorder.control(input,&json!({"kind":"requests_complete","session":session(s)}))?,
        Message::Ended {session:s,reason} => recorder.control(input,&json!({"kind":"ended","session":session(s),"reason":reason.chars().take(256).collect::<String>()}))?,
    }
    Ok(())
}

#[cfg(test)]
mod load_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_summary_uses_nearest_rank_over_live_replicas() {
        assert_eq!(DepthSummary::from_levels(vec![]), DepthSummary::default());
        let one = DepthSummary::from_levels(vec![7]);
        assert_eq!(
            (one.live_replicas, one.levels, one.p50, one.p99, one.max),
            (1, 7, 7, 7, 7)
        );
        // Small samples: p95 of [1, 2, 3] is the largest value, p50 the middle.
        let few = DepthSummary::from_levels(vec![3, 1, 2]);
        assert_eq!((few.p50, few.p95, few.max), (2, 3, 3));
        // Deepest sample wins; ties (including empty books) take the newer.
        let mut empty_live = DepthSummary::from_levels(vec![0, 0]);
        empty_live.sampled_at_ms = 9;
        let kept = DepthSummary::default().deeper(empty_live);
        assert_eq!((kept.live_replicas, kept.sampled_at_ms), (2, 9));
        let deep = DepthSummary::from_levels(vec![5]);
        assert_eq!(deep.deeper(DepthSummary::from_levels(vec![1, 1])).levels, 5);
        // 1..=100 in reverse: ranks are exact, order of input is irrelevant.
        let d = DepthSummary::from_levels((1..=100).rev().collect());
        assert_eq!(
            (d.live_replicas, d.levels, d.p50, d.p95, d.p99, d.max),
            (100, 5050, 50, 95, 99, 100)
        );
    }

    #[test]
    fn invalidation_burst_drains_without_stderr_progress() {
        const CHILD: &str = "COLLECTOR_TEST_UNREAD_STDERR";
        if std::env::var_os(CHILD).is_some() {
            let (send, receive) = ingress::channel(16, 64 << 20).unwrap();
            let sessions = [0, 1].map(|worker| Session {
                worker,
                generation: 1,
                adapter: crate::discovery::AdapterId::PolymarketV1,
                slot: 0,
            });
            for (session, assets) in [
                (sessions[0], (0..50_000).map(|i| format!("A{i}")).collect()),
                (sessions[1], vec!["healthy".into()]),
            ] {
                send.send(Message::Started(session)).unwrap();
                send.send(Message::SubscriptionsRequested {
                    session,
                    revisions: vec![1; assets.len()],
                    add: assets,
                    remove: vec![],
                })
                .unwrap();
            }
            send.send(Message::Data {
                session: sessions[1], ordinal: 1, received_at: chrono::Utc::now(),
                received_mono: std::time::Instant::now(),
                payload: r#"{"event_type":"book","asset_id":"healthy","timestamp":"1","bids":[],"asks":[]}"#.into(),
            }).unwrap();
            send.send(Message::Ended {
                session: sessions[0],
                reason: "large source disconnect λ".repeat(1_000),
            })
            .unwrap();
            // Already accepted work for an independent source must survive the burst.
            send.send(Message::Data {
                session: sessions[1], ordinal: 2, received_at: chrono::Utc::now(),
                received_mono: std::time::Instant::now(),
                payload: r#"{"event_type":"price_change","timestamp":"2","price_changes":[{"asset_id":"healthy","side":"BUY","price":"0.4","size":"7"}]}"#.into(),
            }).unwrap();
            send.send(Message::Ended {
                session: sessions[1],
                reason: "shutdown".into(),
            })
            .unwrap();
            drop(send);
            let (_stop, stopped) = watch::channel(true);
            let (recovery, _requests) = mpsc::channel(1);
            let report = process_books(
                receive,
                orderbook::Config::default(),
                recovery,
                stopped,
                None,
                None,
                std::time::Instant::now(),
            )
            .unwrap();
            assert_eq!(report.messages, 2);
            assert!(
                report
                    .diagnostics
                    .contains("changes: 1, selected_changes: 1")
            );
            assert!(
                report
                    .diagnostics
                    .contains("invalidation_observations=50001")
            );
            assert!(
                report
                    .diagnostics
                    .contains("omitted_invalidation_details=49997")
            );
            assert!(report.diagnostics.len() < 8192);
            return;
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "collector::owner::tests::invalidation_burst_drains_without_stderr_progress",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let started = std::time::Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if started.elapsed() > Duration::from_secs(10) {
                child.kill().unwrap();
                break None;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let output = child.wait_with_output().unwrap();
        assert!(
            status.is_some_and(|s| s.success()),
            "invalidation burst failed to drain without stderr progress: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "book consumer performed diagnostic I/O"
        );
    }

    #[test]
    fn failed_recovery_delivery_is_explicit_but_shutdown_drains_without_scheduling() {
        for mode in ["full", "closed", "shutdown"] {
            let session = Session {
                worker: 0,
                generation: 1,
                adapter: crate::discovery::AdapterId::PolymarketV1,
                slot: 0,
            };
            let (send, receive) = ingress::channel(8, 1 << 20).unwrap();
            send.send(Message::Started(session)).unwrap();
            send.send(Message::SubscriptionsRequested {
                session,
                revisions: vec![1],
                add: vec!["A".into()],
                remove: vec![],
            })
            .unwrap();
            send.send(Message::Data {session,ordinal:1,received_at:chrono::Utc::now(),received_mono:std::time::Instant::now(),payload:r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"0.3","size":"bad"}],"asks":[]}"#.into()}).unwrap();
            send.send(Message::Ended {
                session,
                reason: "shutdown".into(),
            })
            .unwrap();
            drop(send);
            let (recovery, mut requests) = mpsc::channel(1);
            if mode == "full" {
                recovery
                    .try_send(RecoveryRequest::Restart {
                        session,
                        planned: false,
                    })
                    .unwrap();
            }
            if mode == "closed" {
                requests.close();
            }
            let (_stop, stopped) = watch::channel(mode == "shutdown");
            let result = process_books(
                receive,
                orderbook::Config::default(),
                recovery,
                stopped,
                None,
                None,
                std::time::Instant::now(),
            );
            if mode == "shutdown" || mode == "full" {
                assert_eq!(result.unwrap().messages, 1);
                if mode == "shutdown" {
                    assert!(requests.try_recv().is_err());
                }
            } else {
                let error = match result {
                    Err(error) => error,
                    Ok(_) => panic!("expected recovery failure"),
                };
                let diagnostic = format!("{error:#}");
                assert!(diagnostic.contains("recovery channel"));
                assert!(diagnostic.contains("invalidation_observations=1"));
            }
        }
    }
}
