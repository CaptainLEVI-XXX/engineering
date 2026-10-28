use anyhow::{Context, Result, ensure};
use engineering_challenge::{
    discovery::polymarket::{Command, Discovery, GAMMA_MARKETS, Gamma},
    orderbook::{self, ObservationKind, Processor},
    recording,
    websocket::{
        Config, RecoveryRequest,
        ingress::{self, Message, Session},
        run_manager,
    },
};
use std::{
    collections::HashMap, io::IsTerminal, os::fd::AsFd, process::ExitCode, sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

#[cfg(test)]
mod owner_load_tests;

// Live full-universe bursts exceeded 23k envelopes and drained promptly.
// Keep independent byte accounting at 64 MiB; this is burst headroom, not speed.
const DEFAULT_QUEUE_ITEMS: usize = 65_536;

fn main() -> ExitCode {
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .map_err(anyhow::Error::from)
        .and_then(|runtime| {
            let result = runtime.block_on(run());
            runtime.shutdown_timeout(Duration::from_secs(1));
            result
        });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("collector_failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let mut config = Config::default();
    let mut gamma = GAMMA_MARKETS.to_owned();
    let mut seconds = None;
    let mut queue_items = DEFAULT_QUEUE_ITEMS;
    let mut queue_bytes = 64 << 20;
    let mut transport_only = false;
    let mut monitoring = false;
    let mut integrity_path = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value=args.next().context("options require a value: --seconds N --gamma-url URL --ws-url URL --assets-per-socket N --max-sockets N --queue-items N --queue-bytes N")?;
        match arg.as_str() {
            "--seconds" => seconds = Some(value.parse::<u64>()?),
            "--gamma-url" => gamma = value,
            "--ws-url" => config.endpoint = value,
            "--assets-per-socket" => config.assets_per_socket = value.parse()?,
            "--max-sockets" => config.max_sockets = value.parse()?,
            "--replicas-per-market" => config.replicas_per_market = value.parse()?,
            "--lifecycle-hints" => config.lifecycle_hints = value.parse()?,
            "--monitoring" => monitoring = value.parse()?,
            "--queue-items" => queue_items = value.parse()?,
            "--queue-bytes" => queue_bytes = value.parse()?,
            "--transport-only" => transport_only = value.parse()?,
            "--integrity-path" => integrity_path = Some(value),
            _ => anyhow::bail!("unknown option {arg}"),
        }
    }
    config.validate()?;
    ensure!(seconds != Some(0), "duration must be positive");
    if transport_only {
        eprintln!(
            "transport validation only: raw messages are counted and discarded; stdout remains empty"
        );
    } else {
        ensure!(
            integrity_path.is_some(),
            "recording requires --integrity-path PATH (new file)"
        );
        ensure!(
            !std::io::stdout().is_terminal(),
            "binary stdout must be redirected"
        );
    }
    let config = Arc::new(config);
    let (mut discovery, commands, changes) = Discovery::new(Gamma::new(&gamma)?);
    let (output, receiver) = ingress::channel(queue_items, queue_bytes)?;
    let (stop, _) = watch::channel(false);
    let (recovery_send, recovery_receive) = mpsc::channel(config.max_sockets);
    let consumer_stop = stop.subscribe();
    let book_config = orderbook::Config {
        sessions: config.max_sockets,
        selection: if config.replicas_per_market == 2 || monitoring {
            orderbook::SelectionPolicy::Automatic
        } else {
            orderbook::SelectionPolicy::Manual
        },
        monitoring,
        snapshot_interval: (!transport_only).then_some(Duration::from_secs(60)),
        wire: engineering_challenge::market_data::Limits {
            message_bytes: config.max_message_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    let (approval_send, approval) = crossbeam::channel::bounded(1);
    let (writer_send, mut writer_receive) = oneshot::channel();
    let mut writer_thread = None;
    let recorder = if transport_only {
        drop(writer_send);
        None
    } else {
        let label = integrity_path.unwrap();
        let sidecar = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&label)
            .context("create integrity sidecar")?;
        let stdout = std::fs::File::from(std::io::stdout().as_fd().try_clone_to_owned()?);
        let (recorder, output) = recording::channel(recording::Config::default(), &book_config)?;
        let (ready_send, ready) = oneshot::channel();
        writer_thread = Some(
            std::thread::Builder::new()
                .name("sbe-output".into())
                .spawn(move || {
                    let report = output.run(
                        stdout,
                        sidecar,
                        &label,
                        || {
                            let _ = ready_send.send(());
                        },
                        approval,
                    );
                    let _ = writer_send.send(report);
                })?,
        );
        match tokio::time::timeout(config.shutdown_timeout, ready).await {
            Ok(result) => result.context("output startup failed")?,
            Err(_) => std::process::exit(1), // blocked output: no locks or thread joins
        }
        Some(recorder)
    };
    let (consumer_send, mut consumer_receive) = oneshot::channel();
    let hint_commands = config.lifecycle_hints.then(|| commands.clone());
    let receive_origin = std::time::Instant::now();
    let consumer = std::thread::Builder::new()
        .name("market-data".into())
        .spawn(move || {
            let result = if transport_only {
                validate_transport(receiver).map(|messages| ConsumerReport {
                    messages,
                    diagnostics: String::new(),
                })
            } else {
                process_books(
                    receiver,
                    book_config,
                    recovery_send,
                    consumer_stop,
                    recorder,
                    hint_commands,
                    receive_origin,
                )
            };
            let _ = consumer_send.send(result);
        })?;
    let mut discovery_task = tokio::spawn(async move { discovery.run_until_stopped().await });
    let mut manager = tokio::spawn(run_manager(
        commands.clone(),
        changes,
        output,
        config.clone(),
        stop.clone(),
        recovery_receive,
    ));
    let mut manager_result = None;
    let mut discovery_result = None;
    let mut consumer_result = None;
    let mut writer_result = None;
    let mut errors = vec![];
    let mut summaries = vec![];
    let mut failure = stop.subscribe();
    tokio::select! {
        _ = failure.wait_for(|stopped| *stopped) => {},
        result = &mut manager => manager_result = Some(result),
        result = &mut discovery_task => {
            discovery_result = Some(result);
            errors.push("discovery exited unexpectedly".to_owned());
        },
        result = &mut consumer_receive => consumer_result = Some(result),
        result = &mut writer_receive, if !transport_only => {
            writer_result = Some(result);
            errors.push("output ended before intake shutdown".into());
        },
        signal = async {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => signal.context("SIGINT handler failed"),
                _ = async {
                    match seconds {
                        Some(n) => tokio::time::sleep(Duration::from_secs(n)).await,
                        None => std::future::pending().await,
                    }
                } => Ok(()),
            }
        } => {
            if let Err(error) = signal { errors.push(error.to_string()); }
        },
    }
    stop.send_replace(true);
    let deadline = Instant::now() + config.shutdown_timeout;
    if timeout_at(deadline, commands.send(Command::Stop))
        .await
        .is_err()
    {
        std::process::exit(1);
    }
    if manager_result.is_none() {
        match timeout_at(deadline, &mut manager).await {
            Ok(result) => manager_result = Some(result),
            Err(_) => {
                manager.abort();
                std::process::exit(1);
            }
        }
    }
    if let Some(result) = manager_result {
        match result {
            Ok(Ok(reports)) => summaries.push(format!(
                "ingestion workers={} sessions={} received_messages={} requested_at_cutoff={}; requested is not confirmed or book-initialized",
                reports.len(),
                reports.iter().map(|r| r.sessions).sum::<u64>(),
                reports.iter().map(|r| r.messages).sum::<u64>(),
                reports.iter().map(|r| r.requested_at_cutoff).sum::<usize>(),
            )),
            result => errors.push(format!("ingestion: {result:?}")),
        }
    }
    if discovery_result.is_none() {
        match timeout_at(deadline, &mut discovery_task).await {
            Ok(result) => discovery_result = Some(result),
            Err(_) => {
                discovery_task.abort();
                std::process::exit(1);
            }
        }
    }
    if let Some(result) = discovery_result {
        match result {
            Ok(report) => summaries.push(format!("discovery {}", serde_json::to_string(&report)?)),
            Err(error) => errors.push(format!("discovery: {error}")),
        }
    }
    if consumer_result.is_none() {
        match timeout_at(deadline, &mut consumer_receive).await {
            Ok(result) => consumer_result = Some(result),
            Err(_) => std::process::exit(1),
        }
    }
    if let Some(result) = consumer_result {
        if consumer.join().is_err() {
            errors.push("transport consumer panicked".into());
        }
        match result {
            Ok(Ok(report)) => {
                // Book diagnostics are bounded and reported only after the owner exits.
                // Stderr is not part of accepted-message processing or its drain.
                if !report.diagnostics.is_empty() {
                    summaries.push(report.diagnostics);
                }
                summaries.push(format!("drained_messages={}", report.messages));
                if report.messages == 0 {
                    errors.push("no raw messages received".into());
                }
            }
            result => errors.push(format!("transport validation: {result:?}")),
        }
    }
    if !transport_only {
        let _ = approval_send.send(errors.is_empty());
        if writer_result.is_none() {
            writer_result = Some(match timeout_at(deadline, &mut writer_receive).await {
                Ok(result) => result,
                Err(_) => std::process::exit(1),
            });
        }
        if writer_thread.unwrap().join().is_err() {
            errors.push("output worker panicked".into());
        }
        match writer_result.unwrap() {
            Ok(report) => {
                if let Some(error) = &report.error {
                    errors.push(format!("output: {error}"));
                }
                summaries.push(format!(
                    "recording {report:?}; flush does not imply durability"
                ));
            }
            Err(error) => errors.push(format!("output worker lost: {error}")),
        }
    }
    for summary in summaries {
        eprintln!("{summary}");
    }
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}

/// Diagnostic consumer only. Drain the accepted prefix after stop; never stop
/// reading just because intake stopped. Financial validation belongs downstream.
fn validate_transport(receiver: crossbeam::channel::Receiver<ingress::Envelope>) -> Result<u64> {
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
struct ConsumerReport {
    messages: u64,
    diagnostics: String,
}

fn process_books(
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
            "stats":processor.stats(),"reconciliation":processor.reconciliation(),
            "coverage_counts":"rolling owner samples, not full-universe acceptance"}),
        );
        recorder.finish(result.and(evidence))
    } else {
        result
    };
    let diagnostics = format!(
        "books {:?}; invalidation_observations={invalidations} omitted_invalidation_details={} first_samples={samples:?}; asset/reason previews capped at 128 characters; initialization counts are observations, not full-universe coverage; replacement cannot recover missed history",
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
mod tests {
    use super::*;

    #[test]
    fn invalidation_burst_drains_without_stderr_progress() {
        const CHILD: &str = "COLLECTOR_TEST_UNREAD_STDERR";
        if std::env::var_os(CHILD).is_some() {
            let (send, receive) = ingress::channel(16, 64 << 20).unwrap();
            let sessions = [0, 1].map(|worker| Session {
                worker,
                generation: 1,
                adapter: engineering_challenge::discovery::AdapterId::PolymarketV1,
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
                "tests::invalidation_burst_drains_without_stderr_progress",
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
                adapter: engineering_challenge::discovery::AdapterId::PolymarketV1,
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
