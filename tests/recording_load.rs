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
    if samples.is_empty() {
        return json!({"samples":0,"p50_ns":null,"p95_ns":null,"p99_ns":null,"p999_ns":null,"max_ns":null});
    }
    samples.sort_unstable();
    let at = |p: usize| samples[(samples.len() - 1) * p / 1000];
    json!({"samples":samples.len(), "p50_ns":at(500), "p95_ns":at(950), "p99_ns":at(990), "p999_ns":at(999), "max_ns":samples.last()})
}

// Wrap only the binary destination, below C5's buffering/framing. These are
// individual Write/flush call durations, not per-record visibility, fsync,
// consumer acknowledgement, or output-queue residence time. No production hook
// currently exposes a batch dequeue timestamp or a record-to-write association.
struct TimedDestination<W> {
    inner: W,
    writes: Vec<u128>,
    flushes: Vec<u128>,
    write_calls: u64,
    flush_calls: u64,
    bytes: u64,
    errors: u64,
}
impl<W> TimedDestination<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            writes: vec![],
            flushes: vec![],
            write_calls: 0,
            flush_calls: 0,
            bytes: 0,
            errors: 0,
        }
    }
    fn report(self) -> serde_json::Value {
        json!({
            "scope":"whole run including initialization, warmup, measured traffic, padding and final flush",
            "boundary":"binary destination Write/flush entry to return; not durable or consumer-visible",
            "sampling":"first 65536 calls of each kind; counts include later calls; truncated distributions are not full-run tails",
            "write_calls":self.write_calls,"flush_calls":self.flush_calls,"bytes_accepted":self.bytes,"errors":self.errors,
            "write_samples_omitted":self.write_calls - self.writes.len() as u64,
            "flush_samples_omitted":self.flush_calls - self.flushes.len() as u64,
            "write_call":distribution(self.writes),"flush_call":distribution(self.flushes),
            "output_queue_residence_ns":null,"record_to_consumer_visibility_ns":null
        })
    }
}
impl<W: std::io::Write> std::io::Write for TimedDestination<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let start = Instant::now();
        let result = self.inner.write(bytes);
        let elapsed = start.elapsed().as_nanos();
        self.write_calls += 1;
        if self.writes.len() < 65_536 {
            self.writes.push(elapsed);
        }
        match &result {
            Ok(n) => self.bytes += *n as u64,
            Err(_) => self.errors += 1,
        }
        result
    }
    fn flush(&mut self) -> std::io::Result<()> {
        let start = Instant::now();
        let result = self.inner.flush();
        let elapsed = start.elapsed().as_nanos();
        self.flush_calls += 1;
        if self.flushes.len() < 65_536 {
            self.flushes.push(elapsed);
        }
        if result.is_err() {
            self.errors += 1;
        }
        result
    }
}

#[test]
fn output_stage_wrapper_preserves_short_writes_and_failure() {
    use std::io::{self, Write};
    struct ShortWriter;
    impl Write for ShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            Ok(bytes.len().min(2))
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed"))
        }
    }
    let mut writer = TimedDestination::new(ShortWriter);
    writer.write_all(b"abcde").unwrap();
    assert_eq!(
        writer.flush().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    let report = writer.report();
    assert_eq!(report["bytes_accepted"], 5);
    assert_eq!(report["write_calls"], 3);
    assert_eq!(report["write_call"]["samples"], 3);
    assert_eq!(report["flush_calls"], 1);
    assert_eq!(report["errors"], 1);
    assert!(distribution(vec![])["p99_ns"].is_null());
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
    let warmup_messages = 2 * rate;
    let measured_messages = seconds as usize * rate;
    let measured_start = BOOKS + warmup_messages;
    let count = measured_start + measured_messages;
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
    let writer = std::thread::spawn(move || {
        let mut destination = TimedDestination::new(file);
        let report = output.run(
            &mut destination,
            metadata,
            "mixed benchmark",
            || {},
            approved,
        );
        (report, destination.report())
    });
    let reference = expected.reopen().unwrap();
    let processor = std::thread::spawn(move || {
        let start = Instant::now();
        let mut p = Processor::with_receive_clock(config, start).unwrap();
        let mut expected =
            AlignedSbeWriter::new(BufWriter::new(reference), recording::CHUNK_BYTES).unwrap();
        let mut books = vec![Vec::<(i64, i64)>::new(); BOOKS];
        let mut last_exchange = vec![0; BOOKS];
        let mut ages = vec![];
        let mut receive_to_enqueue = vec![];
        let mut oracle = vec![];
        let mut owner_excluding_observer = vec![];
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
                    let dequeued = Instant::now();
                    // Phase belongs to the offered trace, not consumer wall time:
                    // backlog must not turn warmup messages into measured samples.
                    let measured = matches!(&envelope.message, Message::Data { ordinal, .. }
                        if *ordinal as usize > measured_start);
                    if let Message::Data {
                        ordinal,
                        received_at,
                        received_mono,
                        ..
                    } = &envelope.message
                    {
                        if measured {
                            ages.push(dequeued.duration_since(envelope.queued_at).as_nanos());
                            receive_to_enqueue.push(
                                envelope
                                    .queued_at
                                    .checked_duration_since(*received_mono)
                                    .expect("receive stamp must precede ingress enqueue")
                                    .as_nanos(),
                            );
                        }
                        let oracle_start = Instant::now();
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
                        if measured {
                            oracle.push(oracle_start.elapsed().as_nanos());
                        }
                    }
                    let began = Instant::now();
                    let mut owner_resumed = began;
                    let mut owner_ns = 0;
                    p.process(&envelope.message, start.elapsed(), &mut |o| {
                        let observer_start = Instant::now();
                        owner_ns += observer_start.duration_since(owner_resumed).as_nanos();
                        recorder.observe(o)?;
                        if measured {
                            encoding.push(observer_start.elapsed().as_nanos());
                        }
                        owner_resumed = Instant::now();
                        Ok(())
                    })
                    .unwrap();
                    let finished = Instant::now();
                    owner_ns += finished.duration_since(owner_resumed).as_nanos();
                    if measured {
                        processing.push(finished.duration_since(began).as_nanos());
                        owner_excluding_observer.push(owner_ns);
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
            json!({
                "receive_to_ingress_enqueue":distribution(receive_to_enqueue),
                "oracle_before_owner":distribution(oracle),
                "owner_excluding_observer":distribution(owner_excluding_observer),
                "owner_boundary":"sum of direct monotonic spans inside Processor::process outside observation callbacks; includes decode, preparation, mutation and clock/instrumentation overhead",
                "encode_boundary":"one Recorder::observe callback including symbol mapping, SBE encoding, integrity metadata, batching and possible handoff; not pure encoder CPU time",
                "queue_boundary":"ingress enqueue to owner dequeue, before oracle work",
                "receive_boundary":"worker complete-message receive to ingress enqueue in socket mode; synthetic stamp to enqueue in ingress mode",
                "sampling":"every measured data message for queue/owner/oracle; every observation from those messages for encode; percentiles are independently measured and must not be subtracted",
                "observer_instrumentation":"timers and sample vectors perturb timing; oracle work is outside owner spans but delays subsequent dequeue"
            }),
        )
    });
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let assets: Vec<_> = (0..BOOKS).map(|i| format!("A{i}")).collect();
    let began = Instant::now();
    let mut max_offer_lag = Duration::ZERO;
    let mut initialization_at = began;
    let mut traffic_at = began;
    let mut measured_at = began;
    let offered_until;
    if mode == "ingress" {
        send.send(Message::Started(session)).unwrap();
        send.send(Message::SubscriptionsRequested {
            session,
            revisions: vec![1; BOOKS],
            add: assets,
            remove: vec![],
        })
        .unwrap();
        for n in 0..count {
            if n == 0 {
                initialization_at = Instant::now();
            }
            if n == BOOKS {
                traffic_at = Instant::now();
            }
            let due =
                traffic_at + Duration::from_secs_f64(n.saturating_sub(BOOKS) as f64 / rate as f64);
            tokio::time::sleep_until(due.into()).await;
            if n == measured_start {
                measured_at = Instant::now();
            }
            if n >= measured_start {
                max_offer_lag = max_offer_lag.max(due.elapsed());
            }
            send.send(Message::Data {
                session,
                ordinal: n as u64 + 1,
                received_at: Utc::now(),
                received_mono: Instant::now(),
                payload: payload(n).into(),
            })
            .unwrap();
        }
        tokio::time::sleep_until((traffic_at + Duration::from_secs(seconds + 2)).into()).await;
        offered_until = Instant::now();
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
        initialization_at = Instant::now();
        while n < count {
            // Only reset once: incoming PING traffic must not slide the schedule.
            if n == BOOKS && traffic_at == began {
                traffic_at = Instant::now();
            }
            let due =
                traffic_at + Duration::from_secs_f64(n.saturating_sub(BOOKS) as f64 / rate as f64);
            tokio::select! {
                _ = tokio::time::sleep_until(due.into()) => {
                    if n == measured_start { measured_at = Instant::now(); }
                    if n >= measured_start { max_offer_lag = max_offer_lag.max(due.elapsed()); }
                    ws.send(Ws::Text(payload(n).into())).await.unwrap(); n += 1;
                }
                message = ws.next() => {
                    if matches!(message.unwrap().unwrap(), Ws::Text(t) if t == "PING") { ws.send(Ws::Text("PONG".into())).await.unwrap(); }
                }
            }
        }
        tokio::time::sleep_until((traffic_at + Duration::from_secs(seconds + 2)).into()).await;
        offered_until = Instant::now();
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
    let measured_offer_seconds = offered_until.duration_since(measured_at).as_secs_f64();
    let (records, periodic, max_capture_ns, ages, processing, encoding, late_us, stages) =
        processor.join().unwrap();
    approval.send(true).unwrap();
    let (report, output_stages) = writer.join().unwrap();
    if let Some(mut child) = pipe_consumer {
        assert!(child.wait().unwrap().success());
    }
    let drain_seconds = offered_until.elapsed().as_secs_f64();
    assert_eq!(ages["samples"], measured_messages);
    assert_eq!(processing["samples"], measured_messages);
    assert_eq!(
        stages["owner_excluding_observer"]["samples"],
        measured_messages
    );
    assert_eq!(
        stages["receive_to_ingress_enqueue"]["samples"],
        measured_messages
    );
    assert_eq!(stages["oracle_before_owner"]["samples"], measured_messages);
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
        json!({"mode":mode,"destination":destination,"books":BOOKS,"offered_rate":rate,"messages":count,"initialization_messages":BOOKS,"initialization_seconds":traffic_at.duration_since(initialization_at).as_secs_f64(),"warmup_messages":warmup_messages,"warmup_seconds":measured_at.duration_since(traffic_at).as_secs_f64(),"measured_messages":measured_messages,"measured_offer_seconds":measured_offer_seconds,"measured_offered_rate":measured_messages as f64/measured_offer_seconds,"drain_seconds":drain_seconds,"max_measured_offer_lag_us":max_offer_lag.as_micros(),"records_compared":compared,"periodic_snapshots":periodic,"max_capture_including_oracle_ns":max_capture_ns,"max_snapshot_lateness_us":late_us,"ingress_age":ages,"processing_including_encoding":processing,"encoding":encoding,"local_stages":stages,"binary_destination_calls":output_stages,"output":report})
    );
}

#[test]
#[ignore = "separate pipe consumer visibility measurement; 1 second and one 64MiB chunk"]
fn pipe_consumer_distinguishes_record_bytes_from_decodable_chunk() {
    use engineering_challenge::util::dtos::SbeEncode;
    use std::{
        io::{Read, Write},
        process::{Command, Stdio},
    };
    const CHILD: &str = "R0_VISIBILITY_ARCHIVE";
    if let Some(path) = std::env::var_os(CHILD) {
        let mut file = std::fs::File::create(path).unwrap();
        let record_end: usize = std::env::var("R0_RECORD_END").unwrap().parse().unwrap();
        let began = Instant::now();
        let mut offset = 0;
        let mut physical = None;
        let mut complete = None;
        let mut input = std::io::stdin().lock();
        let mut buffer = [0; 65536];
        loop {
            let n = input.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            offset += n;
            let at = began.elapsed().as_nanos(); // consumer-local monotonic clock
            if offset >= record_end && physical.is_none() {
                physical = Some((offset, at));
            }
            if offset >= recording::CHUNK_BYTES && complete.is_none() {
                complete = Some((offset, at));
            }
            file.write_all(&buffer[..n]).unwrap();
        }
        file.flush().unwrap();
        println!(
            "VISIBILITY:{}",
            json!({"sample_ordinal":0,"record_end_offset":record_end,
            "physical":physical.unwrap(),"reader_compatible":complete.unwrap(),"total_bytes":offset,
            "clock":"nanoseconds since consumer start; not producer-to-consumer latency"})
        );
        return;
    }
    let archive = tempfile::NamedTempFile::new().unwrap();
    let sidecar = tempfile::NamedTempFile::new().unwrap();
    let received = DateTime::from_timestamp(100, 0).unwrap();
    let expected = snapshot(0, &initial(0), received, 50_000_000);
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "pipe_consumer_distinguishes_record_bytes_from_decodable_chunk",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD, archive.path())
        .env(
            "R0_RECORD_END",
            expected.to_bytes().unwrap().len().to_string(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = child;
    let pipe = child.stdin.take().unwrap();
    let (mut recorder, output) = recording::channel(
        recording::Config {
            chunk_age: Duration::from_millis(200),
            ..Default::default()
        },
        &orderbook::Config::default(),
    )
    .unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    let metadata = sidecar.reopen().unwrap();
    let writer =
        std::thread::spawn(move || output.run(pipe, metadata, "visibility", || {}, approval));
    let mut p = Processor::new(orderbook::Config::default()).unwrap();
    let session = Session {
        adapter: AdapterId::PolymarketV1,
        worker: 0,
        generation: 1,
        slot: 0,
    };
    for message in [
        Message::Started(session),
        Message::SubscriptionsRequested {
            session,
            revisions: vec![1],
            add: vec!["A0".into()],
            remove: vec![],
        },
        Message::Data {
            session,
            ordinal: 1,
            received_at: received,
            received_mono: Instant::now(),
            payload: payload(0).into(),
        },
    ] {
        p.process(&message, Duration::ZERO, &mut |o| recorder.observe(o))
            .unwrap();
    }
    recorder
        .publish_due(recorder.next_publish_at().unwrap())
        .unwrap();
    std::thread::sleep(Duration::from_secs(1));
    p.process(
        &Message::Ended {
            session,
            reason: "probe end".into(),
        },
        Duration::from_secs(1),
        &mut |o| recorder.observe(o),
    )
    .unwrap();
    recorder.finish(p.finish()).unwrap();
    approve.send(true).unwrap();
    assert!(writer.join().unwrap().error.is_none());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    let row: serde_json::Value = serde_json::from_str(
        text.lines()
            .find_map(|l| l.strip_prefix("VISIBILITY:"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(row["total_bytes"], recording::CHUNK_BYTES);
    assert!(row["physical"][0].as_u64().unwrap() < row["reader_compatible"][0].as_u64().unwrap());
    assert!(row["physical"][1].as_u64().unwrap() <= row["reader_compatible"][1].as_u64().unwrap());
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive.path()).unwrap();
    let AnyMessage::Snapshot(message) = reader.read_msg().unwrap().unwrap() else {
        panic!("wrong record")
    };
    assert_eq!(SnapshotDomain::try_from_decoder(message).unwrap(), expected);
    assert!(reader.read_msg().unwrap().is_none());
    println!("{row}");
}
