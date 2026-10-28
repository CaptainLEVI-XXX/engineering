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
fn actions(n: usize, snapshot_burst: bool) -> Vec<Action> {
    if n < BOOKS {
        return vec![Action::Snapshot(n)];
    }
    let n = n - BOOKS;
    if n.is_multiple_of(if snapshot_burst { 20 } else { 4000 }) {
        return (0..8)
            .map(|i| {
                Action::Snapshot(if snapshot_burst {
                    (n / 20 + i) % BOOKS
                } else {
                    i
                })
            })
            .collect();
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
fn initial(asset: usize, snapshot_burst: bool) -> Vec<(i64, i64)> {
    let depth = if snapshot_burst {
        128 + (asset * 73) % 256
    } else {
        asset % 32 + 1
    };
    (1..=depth)
        .map(|p| (p as i64, (p * 1_000_000) as i64))
        .collect()
}
fn asset_id(asset: usize, native_ids: bool) -> String {
    // Synthetic opaque strings with the native token-ID width, not live IDs.
    if native_ids {
        format!("{asset:078}")
    } else {
        format!("A{asset}")
    }
}
fn payload(n: usize, snapshot_burst: bool, native_ids: bool) -> String {
    use std::fmt::Write;
    // Only generated ASCII identities and nonnegative integer values occur here.
    // Preserve the original JSON bytes without allocating a temporary Value tree.
    let timestamp = 50_000 + n % 97;
    let actions = actions(n, snapshot_burst);
    let mut text = String::new();
    if matches!(actions[0], Action::Snapshot(_)) {
        text.push('[');
        for (index, action) in actions.iter().enumerate() {
            let Action::Snapshot(asset) = action else {
                unreachable!()
            };
            if index > 0 {
                text.push(',');
            }
            write!(
                text,
                r#"{{"asks":[{{"price":"0.9","size":"11"}}],"asset_id":"{}","bids":["#,
                asset_id(*asset, native_ids)
            )
            .unwrap();
            for (index, (p, q)) in initial(*asset, snapshot_burst).into_iter().enumerate() {
                if index > 0 {
                    text.push(',');
                }
                write!(
                    text,
                    r#"{{"price":"{}.{:06}","size":"{}.{:06}"}}"#,
                    p / 1_000_000,
                    p % 1_000_000,
                    q / 1_000_000,
                    q % 1_000_000
                )
                .unwrap();
            }
            write!(text, r#"],"event_type":"book","timestamp":"{timestamp}"}}"#).unwrap();
        }
        text.push(']');
    } else {
        text.push_str(r#"{"event_type":"price_change","price_changes":["#);
        for (index, action) in actions.iter().enumerate() {
            let Action::Level(asset, p, q) = action else {
                unreachable!()
            };
            if index > 0 {
                text.push(',');
            }
            write!(
                text,
                r#"{{"asset_id":"{}","price":"{}.{:06}","side":"BUY","size":"{}.{:06}"}}"#,
                asset_id(*asset, native_ids),
                p / 1_000_000,
                p % 1_000_000,
                q / 1_000_000,
                q % 1_000_000
            )
            .unwrap();
        }
        write!(text, r#"],"timestamp":"{timestamp}"}}"#).unwrap();
    }
    text
}
fn snapshot(
    asset: usize,
    bids: &[(i64, i64)],
    at: DateTime<Utc>,
    exchange: u64,
    native_ids: bool,
) -> SnapshotDomain {
    let mut bids = bids.to_vec();
    bids.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(*p));
    SnapshotDomain {
        symbol: format!("polymarket:v1:{}", asset_id(asset, native_ids)),
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

// Offered-trace classification, not an inference from measured duration. Track
// only the first three updates after each eight-book replacement array, and
// distinguish books that the array did not replace (head-of-line controls).
fn snapshot_neighborhood(n: usize, snapshot_burst: bool) -> usize {
    if n < BOOKS {
        return 0;
    }
    let offset = n - BOOKS;
    let period = if snapshot_burst { 20 } else { 4000 };
    let distance = offset % period;
    if distance == 0 {
        return 0;
    }
    if distance > 3 {
        return 3;
    }
    let first = if snapshot_burst { offset / period } else { 0 };
    if (0..8).any(|i| (first + i) % BOOKS == offset % BOOKS) {
        1
    } else {
        2
    }
}

#[test]
fn snapshot_neighborhood_follows_offered_assets_including_wraparound() {
    assert_eq!(snapshot_neighborhood(BOOKS + 20, true), 0);
    assert_eq!(snapshot_neighborhood(BOOKS + 21, true), 2);
    assert_eq!(snapshot_neighborhood(BOOKS + 1, true), 1);
    assert_eq!(snapshot_neighborhood(BOOKS + 4, true), 3);
    assert_eq!(snapshot_neighborhood(BOOKS + 99 * 20 + 1, true), 2);
    assert_eq!(snapshot_neighborhood(BOOKS + 100 * 20 + 1, true), 1);
    assert_eq!(snapshot_neighborhood(BOOKS + 4001, false), 1);
}

// Capture only input identities/times and periodic capture cutoffs. Reconstruct
// the independent Vec books and expected bytes AFTER timed intake/output drain.
// Never copy financial values from Processor observations into the oracle.
enum ReferenceEvent {
    Data { n: usize, at: DateTime<Utc> },
    Periodic { asset: usize, at: DateTime<Utc> },
}

fn write_reference(
    events: &[ReferenceEvent],
    destination: impl std::io::Write,
    snapshot_burst: bool,
    native_ids: bool,
) -> u64 {
    let mut expected = AlignedSbeWriter::new(destination, recording::CHUNK_BYTES).unwrap();
    let mut books = vec![Vec::<(i64, i64)>::new(); BOOKS];
    let mut last_exchange = vec![0; BOOKS];
    let mut records = 0;
    for event in events {
        match *event {
            ReferenceEvent::Data { n, at } => {
                let exchange = ((50_000 + n % 97) * 1000) as u64;
                for action in actions(n, snapshot_burst) {
                    match action {
                        Action::Snapshot(asset) => {
                            books[asset] = initial(asset, snapshot_burst);
                            last_exchange[asset] = exchange;
                            expected
                                .write_msg(&snapshot(
                                    asset,
                                    &books[asset],
                                    at,
                                    exchange,
                                    native_ids,
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
                                    symbol: format!(
                                        "polymarket:v1:{}",
                                        asset_id(asset, native_ids)
                                    ),
                                    backtest_timestamp: at,
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
            ReferenceEvent::Periodic { asset, at } => {
                expected
                    .write_msg(&snapshot(
                        asset,
                        &books[asset],
                        at,
                        last_exchange[asset],
                        native_ids,
                    ))
                    .unwrap();
                records += 1;
            }
        }
    }
    expected.finish().unwrap();
    records
}

#[test]
fn deferred_reference_preserves_capture_cutoffs_and_replacements() {
    let at = |seconds| DateTime::from_timestamp(seconds, 0).unwrap();
    let mut events = vec![];
    // Asset zero: initial -> four updates -> sixteen (including a delete) ->
    // replacement. Captures must see each intermediate state, not the final one.
    for (index, n) in [0, 200, 300, 4100].into_iter().enumerate() {
        events.push(ReferenceEvent::Data { n, at: at(0) });
        events.push(ReferenceEvent::Periodic {
            asset: 0,
            at: at(index as i64 + 1),
        });
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    assert_eq!(
        write_reference(&events, file.reopen().unwrap(), false, false),
        33
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let mut captures = vec![];
    let mut records = 0;
    while let Some(record) = reader.read_msg().unwrap() {
        records += 1;
        if let AnyMessage::Snapshot(record) = record {
            let record = SnapshotDomain::try_from_decoder(record).unwrap();
            if record.received_at != at(0) {
                captures.push(record);
            }
        }
    }
    assert_eq!(records, 33);
    assert_eq!(captures.len(), 4);
    for (index, (depth, quantity, exchange)) in [
        (1, 1_000_000, 50_000_000),
        (4, 6_000_000, 50_006_000),
        (15, 4_000_000, 50_009_000),
        (1, 1_000_000, 50_026_000),
    ]
    .into_iter()
    .enumerate()
    {
        let capture = &captures[index];
        assert_eq!(capture.symbol, "polymarket:v1:A0");
        assert_eq!(capture.received_at, at(index as i64 + 1));
        assert_eq!(capture.exchange_timestamp.timestamp_micros(), exchange);
        assert_eq!(capture.bids.len(), depth);
        assert_eq!(capture.bids.last().unwrap().price_level, 1);
        assert_eq!(capture.bids.last().unwrap().quantity, quantity);
        assert!(
            capture
                .bids
                .windows(2)
                .all(|pair| pair[0].price_level > pair[1].price_level)
        );
    }
    assert!(!captures[2].bids.iter().any(|level| level.price_level == 9));
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
    let workload = std::env::var("C5_WORKLOAD").unwrap_or("mixed".into());
    assert!(["mixed", "snapshot-burst", "native-ids"].contains(&workload.as_str()));
    let snapshot_burst = workload == "snapshot-burst";
    let native_ids = workload == "native-ids";
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
    let processor = std::thread::spawn(move || {
        let start = Instant::now();
        let mut p = Processor::with_receive_clock(config, start).unwrap();
        // Fixed metadata budget: all offered inputs plus periodic captures and
        // two intervals of scheduling slack. Exceeding it fails, never truncates.
        let trace_limit = count + BOOKS * (seconds as usize / 60 + 2);
        let mut trace = Vec::with_capacity(trace_limit);
        let mut inputs = 0;
        let mut ages = vec![];
        let mut age_windows: [Vec<u128>; 5] = std::array::from_fn(|_| Vec::new());
        let mut receive_to_enqueue = vec![];
        let mut owner_excluding_observer = vec![];
        let mut processing = vec![];
        let mut by_shape: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
        let mut owner_by_shape: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
        let mut recorder_by_shape: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
        let mut queue_by_neighborhood: [Vec<u128>; 4] = std::array::from_fn(|_| Vec::new());
        // This fixture emits at most 16 observations per measured message.
        // Reserve AND touch sample pages before intake: growing this vector or
        // faulting its new pages inside the callback contaminates processing time.
        let mut encoding = vec![1_u128; measured_messages.checked_mul(16).unwrap()];
        std::hint::black_box(&encoding);
        encoding.clear();
        let mut periodic = 0;
        let mut max_capture_ns = 0;
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
                    let mut shape = 0;
                    if let Message::Data {
                        ordinal,
                        received_at,
                        received_mono,
                        ..
                    } = &envelope.message
                    {
                        if measured {
                            let age = dequeued.duration_since(envelope.queued_at).as_nanos();
                            ages.push(age);
                            queue_by_neighborhood
                                [snapshot_neighborhood(*ordinal as usize - 1, snapshot_burst)]
                            .push(age);
                            // Offered ordinal, not consumer time: a backlog cannot
                            // move a delayed input into a later measurement window.
                            let index = *ordinal as usize - 1 - measured_start;
                            age_windows[index * 5 / measured_messages].push(age);
                            receive_to_enqueue.push(
                                envelope
                                    .queued_at
                                    .checked_duration_since(*received_mono)
                                    .expect("receive stamp must precede ingress enqueue")
                                    .as_nanos(),
                            );
                        }
                        let n = *ordinal as usize - 1;
                        assert_eq!(n, inputs, "missing, duplicate or reordered input");
                        inputs += 1;
                        assert!(
                            trace.len() < trace_limit,
                            "reference trace budget exhausted"
                        );
                        trace.push(ReferenceEvent::Data {
                            n,
                            at: *received_at,
                        });
                        // Classify from the independent offered trace, outside
                        // owner timing, without constructing oracle actions.
                        shape = if n < BOOKS
                            || (n - BOOKS).is_multiple_of(if snapshot_burst { 20 } else { 4000 })
                        {
                            0
                        } else {
                            1 + (n - BOOKS) % 3
                        };
                    }
                    let began = Instant::now();
                    let mut owner_resumed = began;
                    let mut owner_ns = 0;
                    let mut recorder_ns = 0;
                    p.process(&envelope.message, start.elapsed(), &mut |o| {
                        let observer_start = Instant::now();
                        owner_ns += observer_start.duration_since(owner_resumed).as_nanos();
                        recorder.observe(o)?;
                        let elapsed = observer_start.elapsed().as_nanos();
                        recorder_ns += elapsed;
                        if measured {
                            assert!(
                                encoding.len() < encoding.capacity(),
                                "encoding sample budget exhausted"
                            );
                            encoding.push(elapsed);
                        }
                        owner_resumed = Instant::now();
                        Ok(())
                    })
                    .unwrap();
                    let finished = Instant::now();
                    owner_ns += finished.duration_since(owner_resumed).as_nanos();
                    if measured {
                        let elapsed = finished.duration_since(began).as_nanos();
                        processing.push(elapsed);
                        by_shape[shape].push(elapsed);
                        owner_excluding_observer.push(owner_ns);
                        owner_by_shape[shape].push(owner_ns);
                        recorder_by_shape[shape].push(recorder_ns);
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
                let asset = if native_ids { o.asset } else { &o.asset[1..] }
                    .parse::<usize>()
                    .unwrap();
                assert!(
                    trace.len() < trace_limit,
                    "reference trace budget exhausted"
                );
                trace.push(ReferenceEvent::Periodic { asset, at });
                recorder.observe(o)?;
                max_capture_ns = max_capture_ns.max(began.elapsed().as_nanos());
                periodic += 1;
                Ok(())
            })
            .unwrap();
            recorder.publish_due(Instant::now()).unwrap();
        }
        recorder.snapshot_stats(p.stats());
        recorder.finish(p.finish()).unwrap();
        assert_eq!(inputs, count, "incomplete reference input trace");
        assert_eq!(
            by_shape.iter().map(Vec::len).sum::<usize>(),
            processing.len()
        );
        let [replacement, delta_one, delta_four, delta_sixteen] = by_shape;
        (
            trace,
            periodic,
            max_capture_ns,
            distribution(ages),
            distribution(processing),
            distribution(encoding),
            p.stats().max_snapshot_lateness.as_micros(),
            json!({
                "receive_to_ingress_enqueue":distribution(receive_to_enqueue),
                "ingress_age_by_window":age_windows.map(distribution),
                "window_boundary":"five equal offered-message-count windows in chronological order; queue-age trend, not queue occupancy or a sustainable-capacity proof",
                "oracle_placement":"after_output_drain",
                "owner_excluding_observer":distribution(owner_excluding_observer),
                "processing_by_shape":{
                    "replacement_array":distribution(replacement),
                    "delta_1":distribution(delta_one),
                    "delta_4":distribution(delta_four),
                    "delta_16":distribution(delta_sixteen)
                },
                "owner_by_shape":owner_by_shape.map(distribution),
                "recorder_by_shape":recorder_by_shape.map(distribution),
                "shape_order":["replacement_array","delta_1","delta_4","delta_16"],
                "ingress_age_by_snapshot_neighborhood":queue_by_neighborhood.map(distribution),
                "snapshot_neighborhood_order":["replacement_array","next_three_affected","next_three_unrelated","later_updates"],
                "neighborhood_boundary":"offered ordinals and independent fixture asset identities; next three updates after an array, partitioned by whether that array replaced their book; association is not proof of causation",
                "recorder_shape_boundary":"sum of measured Recorder::observe spans per message, not subtraction of independently ranked quantiles; excludes sample append overhead",
                "shape_boundary":"same processing-including-encoding spans partitioned by offered message shape; replacement arrays contain eight snapshots; sparse class tails are exploratory, not reliable extreme quantiles",
                "policy_boundary":"health tick and automatic maintenance are excluded; separate lifecycle diagnostic required",
                "owner_boundary":"sum of direct monotonic spans inside Processor::process outside observation callbacks; includes decode, preparation, mutation and clock/instrumentation overhead",
                "encode_boundary":"one Recorder::observe callback including symbol mapping, SBE encoding, integrity metadata, batching and possible handoff; not pure encoder CPU time",
                "queue_boundary":"ingress enqueue to owner dequeue; no reference reconstruction on the owner",
                "receive_boundary":"worker complete-message receive to ingress enqueue in socket mode; synthetic stamp to enqueue in ingress mode",
                "sampling":"every measured data message for queue/owner; every observation from those messages for encode; percentiles are independently measured and must not be subtracted",
                "observer_instrumentation":"timers, sample vectors and bounded reference metadata capture perturb timing; encoding samples are preallocated and page-touched before intake (at most 16 observations/message); reference reconstruction and comparison happen after drain"
            }),
        )
    });
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let assets: Vec<_> = (0..BOOKS).map(|i| asset_id(i, native_ids)).collect();
    let began = Instant::now();
    let mut max_offer_lag = Duration::ZERO;
    let mut initialization_at = began;
    let mut traffic_at = began;
    let mut measured_at = began;
    let offered_until;
    let mut offered_lags = Vec::with_capacity(measured_messages);
    let mut offered_gaps = Vec::with_capacity(measured_messages.saturating_sub(1));
    let mut last_offered = None;
    if mode == "ingress" {
        // Tokio timers have millisecond granularity. Keep the independently
        // paced synchronous ingress producer off the async timer wheel; retain
        // absolute deadlines and expose catch-up bursts rather than dropping work.
        (
            initialization_at,
            traffic_at,
            measured_at,
            offered_until,
            max_offer_lag,
            offered_lags,
            offered_gaps,
        ) = tokio::task::spawn_blocking(move || {
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
                let due = traffic_at
                    + Duration::from_secs_f64(n.saturating_sub(BOOKS) as f64 / rate as f64);
                std::thread::sleep(due.saturating_duration_since(Instant::now()));
                // Synthetic receipt begins when the complete payload exists, like
                // socket receipt. Do not charge JSON generation to ingress handoff.
                let payload = payload(n, snapshot_burst, native_ids).into();
                let received_at = Utc::now();
                let received_mono = Instant::now();
                if n == measured_start {
                    measured_at = received_mono;
                }
                if n >= measured_start {
                    max_offer_lag = max_offer_lag.max(received_mono.duration_since(due));
                    offered_lags.push(received_mono.duration_since(due).as_nanos());
                    if let Some(last) = last_offered {
                        offered_gaps.push(received_mono.duration_since(last).as_nanos());
                    }
                    last_offered = Some(received_mono);
                }
                send.send(Message::Data {
                    session,
                    ordinal: n as u64 + 1,
                    received_at,
                    received_mono,
                    payload,
                })
                .unwrap();
            }
            std::thread::sleep(
                (traffic_at + Duration::from_secs(seconds + 2))
                    .saturating_duration_since(Instant::now()),
            );
            let offered_until = Instant::now();
            send.send(Message::Ended {
                session,
                reason: "benchmark complete".into(),
            })
            .unwrap();
            drop(send);
            (
                initialization_at,
                traffic_at,
                measured_at,
                offered_until,
                max_offer_lag,
                offered_lags,
                offered_gaps,
            )
        })
        .await
        .unwrap();
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
                    let payload = payload(n, snapshot_burst, native_ids).into();
                    let offered = Instant::now();
                    if n == measured_start { measured_at = offered; }
                    if n >= measured_start {
                        max_offer_lag = max_offer_lag.max(offered.duration_since(due));
                        offered_lags.push(offered.duration_since(due).as_nanos());
                        if let Some(last) = last_offered {
                            offered_gaps.push(offered.duration_since(last).as_nanos());
                        }
                        last_offered = Some(offered);
                    }
                    ws.send(Ws::Text(payload)).await.unwrap(); n += 1;
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
    let (trace, periodic, max_capture_ns, ages, processing, encoding, late_us, stages) =
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
    assert!(report.error.is_none(), "{report:?}");
    let oracle_start = Instant::now();
    let records = write_reference(
        &trace,
        BufWriter::new(expected.reopen().unwrap()),
        snapshot_burst,
        native_ids,
    );
    let oracle_seconds = oracle_start.elapsed().as_secs_f64();
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
    assert_eq!(offered_lags.len(), measured_messages);
    assert_eq!(offered_gaps.len(), measured_messages.saturating_sub(1));
    let short_gaps = offered_gaps
        .iter()
        .filter(|gap| **gap < 1_000_000_000 / (2 * rate as u128))
        .count();
    let arrivals = json!({
        "pacing": if mode == "ingress" {"thread_sleep_absolute"} else {"tokio_sleep_absolute"},
        "boundary":"payload-ready synthetic receipt in ingress; before WebSocket send in socket mode; not NIC arrival",
        "lag":distribution(offered_lags), "interarrival":distribution(offered_gaps),
        "short_gaps":short_gaps, "short_gap_threshold_ns":1_000_000_000 / (2 * rate as u128),
        "policy":"absolute deadlines; late messages catch up without skipping; no spin waiting"
    });
    println!(
        "{}",
        json!({"mode":mode,"destination":destination,"workload":workload,"books":BOOKS,"offered_rate":rate,"initialization_messages":BOOKS,"messages":count,"initialization_seconds":traffic_at.duration_since(initialization_at).as_secs_f64(),"warmup_messages":warmup_messages,"warmup_seconds":measured_at.duration_since(traffic_at).as_secs_f64(),"measured_messages":measured_messages,"measured_offer_seconds":measured_offer_seconds,"measured_offered_rate":measured_messages as f64/measured_offer_seconds,"drain_seconds":drain_seconds,"max_measured_offer_lag_us":max_offer_lag.as_micros(),"offered_arrivals":arrivals,"records_compared":compared,"periodic_snapshots":periodic,"max_capture_callback_ns":max_capture_ns,"max_snapshot_lateness_us":late_us,"ingress_age":ages,"processing_including_encoding":processing,"encoding":encoding,"local_stages":stages,"binary_destination_calls":output_stages,"reference_generation_seconds":oracle_seconds,"reference_trace_bytes":trace.capacity()*std::mem::size_of::<ReferenceEvent>(),"output":report})
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
    let expected = snapshot(0, &initial(0, false), received, 50_000_000, false);
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
            payload: payload(0, false, false).into(),
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
