use anyhow::{Context, Result, ensure};
use engineering_challenge::{
    discovery::polymarket::{Command, Discovery, GAMMA_MARKETS, Gamma},
    orderbook::{self, ObservationKind, Processor},
    websocket::{
        Config,
        ingress::{self, Message, Session},
        run_manager,
    },
};
use std::{collections::HashMap, process::ExitCode, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

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
            eprintln!("transport_validation_failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let mut config = Config::default();
    let mut gamma = GAMMA_MARKETS.to_owned();
    let mut seconds = None;
    let mut queue_items = 8192;
    let mut queue_bytes = 64 << 20;
    let mut transport_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value=args.next().context("options require a value: --seconds N --gamma-url URL --ws-url URL --assets-per-socket N --max-sockets N --queue-items N --queue-bytes N")?;
        match arg.as_str() {
            "--seconds" => seconds = Some(value.parse::<u64>()?),
            "--gamma-url" => gamma = value,
            "--ws-url" => config.endpoint = value,
            "--assets-per-socket" => config.assets_per_socket = value.parse()?,
            "--max-sockets" => config.max_sockets = value.parse()?,
            "--queue-items" => queue_items = value.parse()?,
            "--queue-bytes" => queue_bytes = value.parse()?,
            "--transport-only" => transport_only = value.parse()?,
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
        eprintln!(
            "C3/C4 validation: exact decoding and source-specific books; observations are not persisted; stdout remains empty until C5"
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
        wire: engineering_challenge::market_data::Limits {
            message_bytes: config.max_message_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    let (consumer_send, mut consumer_receive) = oneshot::channel();
    let consumer = std::thread::Builder::new()
        .name("market-data".into())
        .spawn(move || {
            let result = if transport_only {
                validate_transport(receiver)
            } else {
                process_books(receiver, book_config, recovery_send, consumer_stop)
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
    let mut errors = vec![];
    let mut failure = stop.subscribe();
    tokio::select! {
        _ = failure.wait_for(|stopped| *stopped) => {},
        result = &mut manager => manager_result = Some(result),
        result = &mut discovery_task => {
            discovery_result = Some(result);
            errors.push("discovery exited unexpectedly".to_owned());
        },
        result = &mut consumer_receive => consumer_result = Some(result),
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
        errors.push("discovery Stop timed out".into());
    }
    if manager_result.is_none() {
        match timeout_at(deadline, &mut manager).await {
            Ok(result) => manager_result = Some(result),
            Err(_) => {
                manager.abort();
                errors.push("worker shutdown timed out; drain incomplete".into());
            }
        }
    }
    if let Some(result) = manager_result {
        match result {
            Ok(Ok(reports)) => eprintln!(
                "ingestion workers={} sessions={} received_messages={} requested_at_cutoff={}; requested is not confirmed or book-initialized",
                reports.len(),
                reports.iter().map(|r| r.sessions).sum::<u64>(),
                reports.iter().map(|r| r.messages).sum::<u64>(),
                reports.iter().map(|r| r.requested_at_cutoff).sum::<usize>(),
            ),
            result => errors.push(format!("ingestion: {result:?}")),
        }
    }
    if discovery_result.is_none() {
        match timeout_at(deadline, &mut discovery_task).await {
            Ok(result) => discovery_result = Some(result),
            Err(_) => {
                discovery_task.abort();
                errors.push("discovery shutdown timed out".into());
            }
        }
    }
    if let Some(result) = discovery_result {
        match result {
            Ok(report) => eprintln!("discovery {}", serde_json::to_string(&report)?),
            Err(error) => errors.push(format!("discovery: {error}")),
        }
    }
    if consumer_result.is_none() {
        match timeout_at(deadline, &mut consumer_receive).await {
            Ok(result) => consumer_result = Some(result),
            Err(_) => errors.push("ingress drain timed out; accepted tail not validated".into()),
        }
    }
    if let Some(result) = consumer_result {
        match result {
            Ok(Ok(messages)) => {
                eprintln!("drained_messages={messages}; observations not persisted");
                if messages == 0 {
                    errors.push("no raw messages received".into());
                }
            }
            result => errors.push(format!("transport validation: {result:?}")),
        }
        if consumer.join().is_err() {
            errors.push("transport consumer panicked".into());
        }
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
            Message::Retired { .. } => {}
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
            Message::SubscriptionsRequested { session, .. } => {
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

fn process_books(
    receiver: crossbeam::channel::Receiver<ingress::Envelope>,
    config: orderbook::Config,
    recovery: mpsc::Sender<Session>,
    stop: watch::Receiver<bool>,
) -> Result<u64> {
    let mut processor = Processor::new(config)?;
    let start = std::time::Instant::now();
    let mut next_tick = Duration::ZERO;
    let mut sink = |observation: orderbook::Observation<'_>| {
        if let ObservationKind::Invalidated { reason } = observation.kind {
            eprintln!(
                "book_unavailable input={} binding={:?} asset={} reason={reason}; replacement cannot recover missed history",
                observation.input, observation.binding, observation.asset
            );
        }
        Ok(())
    };
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(envelope) => processor
                .process(&envelope.message, start.elapsed(), &mut sink)
                .context("C3/C4 processing failed; accepted tail incomplete")?,
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
        }
        if !*stop.borrow() {
            let now = start.elapsed();
            if now >= next_tick {
                processor.tick(now)?;
                next_tick = now + Duration::from_millis(100);
            }
            for session in processor.take_recovery() {
                if let Err(error) = recovery.try_send(session)
                    && !*stop.borrow()
                {
                    return Err(error).context("book recovery channel full or closed; coverage and accepted tail incomplete");
                }
            }
        }
    }
    processor.finish()?;
    eprintln!(
        "books {:?}; initialization counts are observations, not full-universe coverage",
        processor.stats()
    );
    Ok(processor.stats().messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_recovery_delivery_is_explicit_but_shutdown_drains_without_scheduling() {
        for mode in ["full", "closed", "shutdown"] {
            let session = Session {
                worker: 0,
                generation: 1,
                adapter: engineering_challenge::discovery::AdapterId::PolymarketV1,
            };
            let (send, receive) = ingress::channel(8, 1 << 20).unwrap();
            send.send(Message::Started(session)).unwrap();
            send.send(Message::SubscriptionsRequested {
                session,
                source_slot: 0,
                revisions: vec![1],
                add: vec!["A".into()],
                remove: vec![],
            })
            .unwrap();
            send.send(Message::Data {session,ordinal:1,received_at:chrono::Utc::now(),payload:r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"0.3","size":"bad"}],"asks":[]}"#.into()}).unwrap();
            send.send(Message::Ended {
                session,
                reason: "shutdown".into(),
            })
            .unwrap();
            drop(send);
            let (recovery, mut requests) = mpsc::channel(1);
            if mode == "full" {
                recovery.try_send(session).unwrap();
            }
            if mode == "closed" {
                requests.close();
            }
            let (_stop, stopped) = watch::channel(mode == "shutdown");
            let result = process_books(receive, orderbook::Config::default(), recovery, stopped);
            if mode == "shutdown" {
                assert_eq!(result.unwrap(), 1);
                assert!(requests.try_recv().is_err());
            } else {
                assert!(result.unwrap_err().to_string().contains("recovery channel"));
            }
        }
    }
}
