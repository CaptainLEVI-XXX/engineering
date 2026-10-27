use anyhow::{Context, Result, ensure};
use engineering_challenge::{
    discovery::polymarket::{Command, Discovery, GAMMA_MARKETS, Gamma},
    websocket::{
        Config,
        ingress::{self, Message, Session},
        run_manager,
    },
};
use std::{collections::HashMap, process::ExitCode, sync::Arc, time::Duration};
use tokio::{
    sync::{oneshot, watch},
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
            _ => anyhow::bail!("unknown option {arg}"),
        }
    }
    config.validate()?;
    ensure!(seconds != Some(0), "duration must be positive");
    eprintln!(
        "transport validation only: raw messages are counted and discarded; no book reconstruction or persistence; stdout remains empty"
    );
    let config = Arc::new(config);
    let (mut discovery, commands, changes) = Discovery::new(Gamma::new(&gamma)?);
    let (output, receiver) = ingress::channel(queue_items, queue_bytes)?;
    let (stop, _) = watch::channel(false);
    let (consumer_send, mut consumer_receive) = oneshot::channel();
    let consumer = std::thread::Builder::new()
        .name("transport-validation".into())
        .spawn(move || {
            let _ = consumer_send.send(validate_transport(receiver));
        })?;
    let mut discovery_task = tokio::spawn(async move { discovery.run_until_stopped().await });
    let mut manager = tokio::spawn(run_manager(
        commands.clone(),
        changes,
        output,
        config.clone(),
        stop.clone(),
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
                eprintln!(
                    "transport drained_messages={messages}; payloads discarded, not persisted"
                );
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
