//! Process wiring: discovery (C1) -> ingestion (C2) -> market-data owner
//! thread (C3/C4) -> output thread (C5), then ordered shutdown and drain.
use crate::{
    discovery::polymarket::{Command, Discovery, Gamma},
    orderbook, recording,
    websocket::{ingress, run_manager},
};
use anyhow::{Context, Result, ensure};
use std::{io::IsTerminal, os::fd::AsFd, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

mod options;
mod owner;
pub use options::{DEFAULT_QUEUE_ITEMS, Options};
use owner::{ConsumerReport, process_books, validate_transport};

pub async fn run(options: Options) -> Result<()> {
    let Options {
        transport: config,
        gamma,
        seconds,
        queue_items,
        queue_bytes,
        transport_only,
        monitoring,
        integrity_path,
    } = options;
    if transport_only {
        eprintln!(
            "transport validation only: raw messages are counted and discarded; stdout remains empty"
        );
    } else {
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
        wire: crate::market_data::Limits {
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
