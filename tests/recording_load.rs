//! Opt-in synthetic full-recording replay. The oracle uses linear vectors and
//! independently specified integer actions, never the production book state.
use chrono::{DateTime, Utc};
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    orderbook::{self, ObservationKind, Processor, SnapshotOrigin},
    recording,
    util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema, dtos::challenge_orderbook::*},
    websocket::{
        self,
        ingress::{self, Message, Session},
        run_worker,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    io::BufWriter,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};
use tokio_tungstenite::{accept_async, tungstenite::Message as Ws};

const BOOKS: usize = 100;
enum Action {
    Snapshot(usize),
    Level(usize, i64, i64),
}
fn actions(n: usize) -> Vec<Action> {
    if n < BOOKS {
        return vec![Action::Snapshot(n)];
    }
    let n = n - BOOKS;
    if n.is_multiple_of(4000) {
        return (0..8).map(Action::Snapshot).collect();
    }
    (0..[1, 4, 16][n % 3])
        .map(|i| {
            Action::Level(
                n % BOOKS,
                i + 1,
                if (n + i as usize).is_multiple_of(13) {
                    0
                } else {
                    (n / 3 % 7 + 1) as i64 * 1_000_000
                },
            )
        })
        .collect()
}
fn initial(asset: usize) -> Vec<(i64, i64)> {
    (1..=asset % 32 + 1)
        .map(|p| (p as i64, (p * 1_000_000) as i64))
        .collect()
}
fn decimal(v: i64) -> String {
    format!("{}.{:06}", v / 1_000_000, v % 1_000_000)
}
fn payload(n: usize) -> String {
    let timestamp = (50_000 + n % 97).to_string();
    let actions = actions(n);
    if matches!(actions[0], Action::Snapshot(_)) {
        serde_json::to_string(&actions.iter().map(|a| {
            let Action::Snapshot(asset) = a else { unreachable!() };
            json!({"event_type":"book", "asset_id":format!("A{asset}"), "timestamp":timestamp, "bids":initial(*asset).iter().map(|(p,q)| json!({"price":decimal(*p),"size":decimal(*q)})).collect::<Vec<_>>(), "asks":[{"price":"0.9","size":"11"}]})
        }).collect::<Vec<_>>()).unwrap()
    } else {
        json!({"event_type":"price_change", "timestamp":timestamp, "price_changes":actions.iter().map(|a| {
            let Action::Level(asset,p,q) = a else { unreachable!() };
            json!({"asset_id":format!("A{asset}"),"side":"BUY","price":decimal(*p),"size":decimal(*q)})
        }).collect::<Vec<_>>()}).to_string()
    }
}
fn snapshot(asset: usize, bids: &[(i64, i64)], at: DateTime<Utc>, exchange: u64) -> SnapshotDomain {
    let mut bids = bids.to_vec();
    bids.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(*p));
    SnapshotDomain {
        symbol: format!("polymarket:v1:A{asset}"),
        received_at: at,
        exchange_timestamp: DateTime::from_timestamp_micros(exchange as i64).unwrap(),
        bids: bids
            .iter()
            .map(|(p, q)| SnapshotBidsEntryDomain {
                price_level: *p,
                quantity: *q,
            })
            .collect(),
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 900_000,
            quantity: 11_000_000,
        }],
    }
}
fn distribution(mut samples: Vec<u128>) -> serde_json::Value {
    samples.sort_unstable();
    let at = |p: usize| samples[(samples.len() - 1) * p / 1000];
    json!({"samples":samples.len(), "p50_ns":at(500), "p95_ns":at(950), "p99_ns":at(990), "p999_ns":at(999), "max_ns":samples.last()})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 120-second full-recording benchmark; C5_SECONDS/C5_RATE/C5_MODE override"]
async fn mixed_recording_matches_every_oracle_record() {
    let seconds: u64 = std::env::var("C5_SECONDS")
        .unwrap_or("120".into())
        .parse()
        .unwrap();
    let rate: usize = std::env::var("C5_RATE")
        .unwrap_or("1000".into())
        .parse()
        .unwrap();
    let mode = std::env::var("C5_MODE").unwrap_or("ingress".into());
    let destination = std::env::var("C5_OUTPUT").unwrap_or("file".into());
    assert!(seconds > 0 && rate > 0 && ["ingress", "socket"].contains(&mode.as_str()));
    assert!(["file", "pipe", "throttled"].contains(&destination.as_str()));
    let count = (seconds as usize + 2) * rate + BOOKS; // two-second warmup
    let archive = tempfile::NamedTempFile::new().unwrap();
    let expected = tempfile::NamedTempFile::new().unwrap();
    let sidecar = tempfile::NamedTempFile::new().unwrap();
    let (send, receive) = ingress::channel(8192, 64 << 20).unwrap();
    let config = orderbook::Config {
        snapshot_interval: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    let (mut recorder, output) = recording::channel(recording::Config::default(), &config).unwrap();
    let (approval, approved) = crossbeam::channel::bounded(1);
    let mut pipe_consumer = None;
    let file: Box<dyn std::io::Write + Send> = if destination == "file" {
        Box::new(archive.reopen().unwrap())
    } else {
        use std::process::{Command, Stdio};
        let mut command = if destination == "pipe" {
            Command::new("cat")
        } else {
            let mut command = Command::new("python3");
            command.args(["-c", "import os,sys,time\nwhile True:\n b=os.read(0,65536)\n if not b: break\n sys.stdout.buffer.write(b);sys.stdout.buffer.flush();time.sleep(.001)"]);
            command
        };
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(archive.reopen().unwrap())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        pipe_consumer = Some(child);
        Box::new(input)
    };
    let metadata = sidecar.reopen().unwrap();
    let writer =
        std::thread::spawn(move || output.run(file, metadata, "mixed benchmark", || {}, approved));
    let reference = expected.reopen().unwrap();
    let processor = std::thread::spawn(move || {
        let mut p = Processor::new(config).unwrap();
        let mut expected =
            AlignedSbeWriter::new(BufWriter::new(reference), recording::CHUNK_BYTES).unwrap();
        let mut books = vec![Vec::<(i64, i64)>::new(); BOOKS];
        let mut last_exchange = vec![0; BOOKS];
        let start = Instant::now();
        let mut ages = vec![];
        let mut processing = vec![];
        let mut encoding = vec![];
        let mut periodic = 0;
        let mut max_capture_ns = 0;
        let mut records = 0u64;
        loop {
            let wait = recorder
                .next_publish_at()
                .map(|at| at.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::from_millis(5))
                .min(Duration::from_millis(5));
            match receive.recv_timeout(wait) {
                Ok(envelope) => {
                    let warm = start.elapsed() >= Duration::from_secs(2);
                    if let Message::Data {
                        ordinal,
                        received_at,
                        ..
                    } = &envelope.message
                    {
                        if warm {
                            ages.push(envelope.queued_at.elapsed().as_nanos());
                        }
                        let n = *ordinal as usize - 1;
                        let exchange = ((50_000 + n % 97) * 1000) as u64;
                        for action in actions(n) {
                            match action {
                                Action::Snapshot(asset) => {
                                    books[asset] = initial(asset);
                                    last_exchange[asset] = exchange;
                                    expected
                                        .write_msg(&snapshot(
                                            asset,
                                            &books[asset],
                                            *received_at,
                                            exchange,
                                        ))
                                        .unwrap();
                                }
                                Action::Level(asset, price, quantity) => {
                                    let levels = &mut books[asset];
                                    if let Some(i) = levels.iter().position(|(p, _)| *p == price) {
                                        levels.remove(i);
                                    }
                                    if quantity != 0 {
                                        levels.push((price, quantity));
                                    }
                                    last_exchange[asset] = exchange;
                                    expected
                                        .write_msg(&PriceLevelUpdateDomain {
                                            symbol: format!("polymarket:v1:A{asset}"),
                                            backtest_timestamp: *received_at,
                                            exchange_timestamp: DateTime::from_timestamp_micros(
                                                exchange as i64,
                                            )
                                            .unwrap(),
                                            side: Side::Buy,
                                            price_level: price,
                                            quantity,
                                        })
                                        .unwrap();
                                }
                            }
                            records += 1;
                        }
                    }
                    let began = Instant::now();
                    p.process(&envelope.message, start.elapsed(), &mut |o| {
                        let began = Instant::now();
                        recorder.observe(o)?;
                        if warm {
                            encoding.push(began.elapsed().as_nanos());
                        }
                        Ok(())
                    })
                    .unwrap();
                    if warm {
                        processing.push(began.elapsed().as_nanos());
                    }
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
            }
            p.capture_due(&mut || (start.elapsed(), Utc::now()), &mut |o| {
                let began = Instant::now();
                let ObservationKind::Snapshot {
                    origin: SnapshotOrigin::Periodic { at, .. },
                    ..
                } = o.kind
                else {
                    panic!("unexpected capture")
                };
                let asset = o.asset[1..].parse::<usize>().unwrap();
                expected
                    .write_msg(&snapshot(asset, &books[asset], at, last_exchange[asset]))
                    .unwrap();
                recorder.observe(o)?;
                max_capture_ns = max_capture_ns.max(began.elapsed().as_nanos());
                periodic += 1;
                records += 1;
                Ok(())
            })
            .unwrap();
            recorder.publish_due(Instant::now()).unwrap();
        }
        recorder.snapshot_stats(p.stats());
        recorder.finish(p.finish()).unwrap();
        expected.finish().unwrap();
        (
            records,
            periodic,
            max_capture_ns,
            distribution(ages),
            distribution(processing),
            distribution(encoding),
            p.stats().max_snapshot_lateness.as_micros(),
        )
    });
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
    };
    let assets: Vec<_> = (0..BOOKS).map(|i| format!("A{i}")).collect();
    let began = Instant::now();
    let mut max_offer_lag = Duration::ZERO;
    if mode == "ingress" {
        send.send(Message::Started(session)).unwrap();
        send.send(Message::SubscriptionsRequested {
            session,
            source_slot: 0,
            revisions: vec![1; BOOKS],
            add: assets,
            remove: vec![],
        })
        .unwrap();
        for n in 0..count {
            let due = began + Duration::from_secs_f64(n.saturating_sub(BOOKS) as f64 / rate as f64);
            tokio::time::sleep_until(due.into()).await;
            max_offer_lag = max_offer_lag.max(due.elapsed());
            send.send(Message::Data {
                session,
                ordinal: n as u64 + 1,
                received_at: Utc::now(),
                payload: payload(n).into(),
            })
            .unwrap();
        }
        send.send(Message::Ended {
            session,
            reason: "benchmark complete".into(),
        })
        .unwrap();
        drop(send);
    } else {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Arc::new(websocket::Config {
            endpoint: format!("ws://{}", listener.local_addr().unwrap()),
            ..Default::default()
        });
        let (_commands, commands) = mpsc::channel(8);
        let (stop, _) = watch::channel(false);
        let worker = tokio::spawn(run_worker(
            0,
            session.adapter,
            assets,
            commands,
            send,
            config,
            stop.clone(),
        ));
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        let mut n = 0;
        while n < count {
            let due = began + Duration::from_secs_f64(n.saturating_sub(BOOKS) as f64 / rate as f64);
            tokio::select! {
                _ = tokio::time::sleep_until(due.into()) => {
                    max_offer_lag = max_offer_lag.max(due.elapsed());
                    ws.send(Ws::Text(payload(n).into())).await.unwrap(); n += 1;
                }
                message = ws.next() => {
                    if matches!(message.unwrap().unwrap(), Ws::Text(t) if t == "PING") { ws.send(Ws::Text("PONG".into())).await.unwrap(); }
                }
            }
        }
        ws.send(Ws::Ping(vec![42].into())).await.unwrap();
        loop {
            match ws.next().await.unwrap().unwrap() {
                Ws::Pong(p) if p.as_ref() == [42] => break,
                Ws::Text(t) if t == "PING" => ws.send(Ws::Text("PONG".into())).await.unwrap(),
                _ => {}
            }
        }
        stop.send_replace(true);
        while let Some(Ok(m)) = ws.next().await {
            if matches!(m, Ws::Close(_)) {
                let _ = ws.flush().await;
                break;
            }
        }
        worker.await.unwrap().unwrap();
    }
    let offered_seconds = began.elapsed().as_secs_f64();
    let (records, periodic, max_capture_ns, ages, processing, encoding, late_us) =
        processor.join().unwrap();
    approval.send(true).unwrap();
    let report = writer.join().unwrap();
    if let Some(mut child) = pipe_consumer {
        assert!(child.wait().unwrap().success());
    }
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.records_confirmed, records);
    if seconds >= 120 {
        assert!(periodic >= 184, "minute captures absent: {periodic}");
    }
    let mut actual = AlignedSbeReader::<OrderbookSchema>::open(archive.path()).unwrap();
    let mut want = AlignedSbeReader::<OrderbookSchema>::open(expected.path()).unwrap();
    let mut compared = 0;
    while let Some(a) = actual.read_msg().unwrap() {
        let b = want.read_msg().unwrap().expect("extra output record");
        match (a, b) {
            (AnyMessage::Snapshot(a), AnyMessage::Snapshot(b)) => assert_eq!(
                SnapshotDomain::try_from_decoder(a).unwrap(),
                SnapshotDomain::try_from_decoder(b).unwrap()
            ),
            (AnyMessage::PriceLevelUpdate(a), AnyMessage::PriceLevelUpdate(b)) => assert_eq!(
                PriceLevelUpdateDomain::try_from_decoder(a).unwrap(),
                PriceLevelUpdateDomain::try_from_decoder(b).unwrap()
            ),
            _ => panic!("record order mismatch"),
        }
        compared += 1;
    }
    assert!(want.read_msg().unwrap().is_none(), "missing output record");
    assert_eq!(compared, records);
    println!(
        "{}",
        json!({"mode":mode,"destination":destination,"books":BOOKS,"warmup_seconds":2,"offered_rate":rate,"messages":count,"achieved_rate":count as f64/offered_seconds,"offered_seconds":offered_seconds,"max_offer_lag_us":max_offer_lag.as_micros(),"records_compared":compared,"periodic_snapshots":periodic,"max_capture_including_oracle_ns":max_capture_ns,"max_snapshot_lateness_us":late_us,"ingress_age":ages,"processing_including_encoding":processing,"encoding":encoding,"output":report})
    );
}
