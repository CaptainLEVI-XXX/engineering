use chrono::{TimeZone, Utc};
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    market_data::{Price, Quantity},
    orderbook::{Config, ObservationKind, Processor},
    recording::encode::{LevelRecord, SnapshotRecord},
    util::{
        AlignedSbeReader, AlignedSbeWriter, OrderbookSchema,
        dtos::{SbeEncode, challenge_orderbook::*},
    },
    websocket::ingress::{Message, Session},
};
use std::{
    io::{self, Write},
    time::{Duration, Instant},
};

#[test]
fn retained_public_deltas_round_trip_against_independent_decimal_expectations() {
    use engineering_challenge::recording;
    use serde_json::{Value, json};
    // Raw contiguous message prefixes, with expectations calculated separately
    // using Python Decimal, not the production decoder/book/encoder.
    let adapter = AdapterId::PolymarketV1;
    let fixture = include_str!("fixtures/polymarket-v1-trace.json");
    let fixture: Value = serde_json::from_str(fixture).unwrap();
    let session = Session {
        adapter,
        worker: 0,
        generation: 1,
        slot: 0,
    };
    let config = Config::default();
    let (mut recorder, output) = recording::channel(recording::Config::default(), &config).unwrap();
    let mut processor = Processor::new(config).unwrap();
    let assets: Vec<String> = serde_json::from_value(fixture["assets"].clone()).unwrap();
    for message in [
        Message::Started(session),
        Message::SubscriptionsRequested {
            session,
            revisions: vec![1; assets.len()],
            add: assets,
            remove: vec![],
        },
    ] {
        processor
            .process(&message, Duration::ZERO, &mut |o| recorder.observe(o))
            .unwrap();
    }
    for row in fixture["messages"].as_array().unwrap() {
        processor
            .process(
                &Message::Data {
                    session,
                    ordinal: row["ordinal"].as_u64().unwrap(),
                    received_at: row["received_at"].as_str().unwrap().parse().unwrap(),
                    received_mono: Instant::now(),
                    payload: row["text"].as_str().unwrap().to_owned().into(),
                },
                Duration::from_micros(row["capture_elapsed_us"].as_u64().unwrap()),
                &mut |o| recorder.observe(o),
            )
            .unwrap();
    }
    assert_eq!(processor.stats().rejected, 0);
    for (asset, expected) in fixture["final_books"].as_object().unwrap() {
        let replica = processor.replica(adapter, asset, 0).unwrap();
        assert_eq!(
            replica.state,
            engineering_challenge::orderbook::Validity::Live
        );
        let levels = |side| {
            replica
                .book
                .levels(side)
                .map(|(p, q)| [p.value(), q.value()])
                .collect::<Vec<_>>()
        };
        assert_eq!(
            json!({"bids":levels(Side::Buy),"asks":levels(Side::Sell)}),
            *expected
        );
    }
    let cutoff = fixture["messages"].as_array().unwrap().last().unwrap()["capture_elapsed_us"]
        .as_u64()
        .unwrap();
    processor
        .process(
            &Message::Ended {
                session,
                reason: "fixture prefix cutoff".into(),
            },
            Duration::from_micros(cutoff),
            &mut |o| recorder.observe(o),
        )
        .unwrap();
    recorder.finish(processor.finish()).unwrap();
    let archive = tempfile::NamedTempFile::new().unwrap();
    let sidecar = tempfile::NamedTempFile::new().unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    approve.send(true).unwrap();
    let report = output.run(
        archive.reopen().unwrap(),
        sidecar.reopen().unwrap(),
        "public fixture",
        || {},
        approval,
    );
    assert!(report.error.is_none(), "{report:?}");
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive.path()).unwrap();
    let mut actual = vec![];
    while let Some(message) = reader.read_msg().unwrap() {
        actual.push(match message {
                AnyMessage::Snapshot(s) => {
                    let s = SnapshotDomain::try_from_decoder(s).unwrap();
                    json!({"kind":"snapshot","symbol":s.symbol,"received_us":s.received_at.timestamp_micros(),
                        "exchange_us":s.exchange_timestamp.timestamp_micros(),
                        "bids":s.bids.iter().map(|l| [l.price_level,l.quantity]).collect::<Vec<_>>(),
                        "asks":s.asks.iter().map(|l| [l.price_level,l.quantity]).collect::<Vec<_>>()})
                }
                AnyMessage::PriceLevelUpdate(l) => {
                    let l = PriceLevelUpdateDomain::try_from_decoder(l).unwrap();
                    json!({"kind":"level","symbol":l.symbol,"received_us":l.backtest_timestamp.timestamp_micros(),
                        "exchange_us":l.exchange_timestamp.timestamp_micros(),"side":if l.side==Side::Buy {"BUY"} else {"SELL"},
                        "price":l.price_level,"quantity":l.quantity})
                }
                _ => panic!("unexpected record"),
            });
    }
    assert_eq!(json!(actual), fixture["expected"]);
    assert_eq!(report.records_confirmed, actual.len() as u64);
}

#[test]
fn borrowed_records_preserve_exact_fields_and_generated_wire_layout() {
    let received = Utc.timestamp_opt(101, 123_456_000).unwrap();
    let exchange = Utc.timestamp_opt(42, 0).unwrap();
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let mut p = Processor::new(Config::default()).unwrap();
    for message in [
        Message::Started(session),
        Message::SubscriptionsRequested {
            session,
            revisions: vec![1],
            add: vec!["00123".into()],
            remove: vec![],
        },
    ] {
        p.process(&message, Duration::ZERO, &mut |_| Ok(()))
            .unwrap();
    }
    let expected = SnapshotDomain {
        received_at: received,
        exchange_timestamp: exchange,
        symbol: "polymarket:v1:00123".into(),
        bids: vec![
            SnapshotBidsEntryDomain {
                price_level: 200000,
                quantity: 3000000,
            },
            SnapshotBidsEntryDomain {
                price_level: 100000,
                quantity: 7000000,
            },
        ],
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 800000,
            quantity: 11000000,
        }],
    };
    let mut bytes = vec![];
    p.process(&Message::Data { session, ordinal: 1, received_at: received, received_mono: Instant::now(),
        payload: r#"{"event_type":"book","asset_id":"00123","timestamp":"42000","bids":[{"price":"0.1","size":"7"},{"price":"0.2","size":"3"}],"asks":[{"price":"0.8","size":"11"}]}"#.into(),
    }, Duration::ZERO, &mut |o| {
        if let ObservationKind::Snapshot { book, .. } = o.kind {
            bytes = SnapshotRecord { book, symbol: "polymarket:v1:00123", received_at: received, exchange_us: 42000000 }.to_bytes()?;
        }
        Ok(())
    }).unwrap();
    assert_eq!(bytes, expected.to_bytes().unwrap());
    assert_eq!(
        SnapshotDomain::try_from_slice_with_header(&bytes, 0).unwrap(),
        expected
    );
    assert_eq!(bytes.len(), 36 + 3 * 16 + 19);

    for quantity in [9, 9, 0] {
        let record = LevelRecord {
            symbol: "polymarket:v2:00123",
            received_at: received,
            exchange_us: 42000000,
            side: Side::Sell,
            price: Price::new(800000).unwrap(),
            quantity: Quantity::new(quantity).unwrap(),
        };
        let want = PriceLevelUpdateDomain {
            symbol: "polymarket:v2:00123".into(),
            backtest_timestamp: received,
            exchange_timestamp: exchange,
            side: Side::Sell,
            price_level: 800000,
            quantity,
        };
        assert_eq!(record.to_bytes().unwrap(), want.to_bytes().unwrap());
    }
}

struct Fragmented {
    bytes: Vec<u8>,
    fail_at: usize,
    interrupted: bool,
}
impl Write for Fragmented {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.bytes.len() == self.fail_at {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let n = bytes.len().min(7).min(self.fail_at - self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn update() -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        symbol: "A".into(),
        backtest_timestamp: Utc.timestamp_opt(7, 0).unwrap(),
        exchange_timestamp: Utc.timestamp_opt(3, 0).unwrap(),
        side: Side::Buy,
        price_level: 200000,
        quantity: 0,
    }
}

#[test]
fn framing_separates_physical_flush_and_padding_and_never_retries_a_failed_prefix() {
    let record = update();
    assert_eq!(record.to_bytes().unwrap().len(), 46);
    let mut writer = AlignedSbeWriter::new(Vec::new(), 100).unwrap();
    writer.write_msg(&record).unwrap();
    writer.flush_bytes().unwrap();
    writer.write_msg(&record).unwrap(); // exact payload fit: 92 + 8 sentinel
    writer.write_msg(&record).unwrap(); // next chunk
    writer.finish().unwrap();
    writer.finish().unwrap();
    let bytes = writer.into_inner_unflushed();
    let mut expected = record.to_bytes().unwrap().repeat(2);
    expected.resize(100, 0);
    expected.extend(record.to_bytes().unwrap());
    expected.resize(200, 0);
    assert_eq!(bytes, expected);
    let mut one_short = AlignedSbeWriter::new(Vec::new(), 99).unwrap();
    one_short.write_msg(&record).unwrap();
    one_short.write_msg(&record).unwrap(); // would exceed payload space by one byte
    one_short.finish().unwrap();
    let mut chunk = record.to_bytes().unwrap();
    chunk.resize(99, 0);
    assert_eq!(one_short.into_inner_unflushed(), chunk.repeat(2));
    let mut empty = AlignedSbeWriter::new(Vec::new(), 100).unwrap();
    empty.finish().unwrap();
    empty.finish().unwrap();
    assert!(empty.into_inner_unflushed().is_empty());
    for fail_at in [0, 1, 45, 46, 99, 100, 117] {
        let destination = Fragmented {
            bytes: vec![],
            fail_at,
            interrupted: false,
        };
        let mut writer = AlignedSbeWriter::new(destination, 100).unwrap();
        let result = (|| -> anyhow::Result<()> {
            for _ in 0..3 {
                writer.write_msg(&record)?;
            }
            writer.finish()
        })();
        assert!(result.is_err());
        assert!(writer.write_msg(&record).is_err());
        assert!(writer.finish().is_err());
        assert_eq!(writer.into_inner_unflushed().bytes, expected[..fail_at]);
    }
}

#[test]
fn default_chunks_round_trip_with_and_without_prefetch_and_ignore_torn_tail() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(file.path(), 64 << 20).unwrap();
    writer.write_msg(&update()).unwrap();
    writer.flush_bytes().unwrap();
    assert!(
        AlignedSbeReader::<OrderbookSchema>::open(file.path())
            .unwrap()
            .read_msg()
            .unwrap()
            .is_none()
    );
    writer.finish().unwrap();
    for prefetch in [false, true] {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        let thread = prefetch.then(|| std::thread::spawn(reader.create_prefetch_task().unwrap()));
        let AnyMessage::PriceLevelUpdate(value) = reader.read_msg().unwrap().unwrap() else {
            panic!("wrong template")
        };
        assert_eq!(
            PriceLevelUpdateDomain::try_from_decoder(value).unwrap(),
            update()
        );
        assert!(reader.read_msg().unwrap().is_none());
        drop(reader);
        if let Some(thread) = thread {
            thread.join().unwrap();
        }
    }
}

#[test]
fn recorder_preserves_record_order_and_sidecar_provenance() {
    use engineering_challenge::recording;
    let book_config = Config::default();
    let (mut recorder, output) =
        recording::channel(recording::Config::default(), &book_config).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let sidecar = tempfile::NamedTempFile::new().unwrap();
    let binary = file.reopen().unwrap();
    let metadata = sidecar.reopen().unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    approve.send(true).unwrap();
    let writer = std::thread::spawn(move || output.run(binary, metadata, "test", || {}, approval));
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let mut processor = Processor::new(book_config).unwrap();
    for message in [
        Message::Started(session),
        Message::SubscriptionsRequested { session, revisions: vec![1], add: vec!["00123".into()], remove: vec![] },
        Message::Data { session, ordinal: 1, received_at: Utc.timestamp_opt(100, 0).unwrap(), received_mono: Instant::now(), payload: r#"{"event_type":"book","asset_id":"00123","timestamp":"42000","bids":[{"price":"0.2","size":"3"}],"asks":[]}"#.into() },
        Message::Data { session, ordinal: 2, received_at: Utc.timestamp_opt(101, 0).unwrap(), received_mono: Instant::now(), payload: r#"{"event_type":"price_change","timestamp":"41000","price_changes":[{"asset_id":"00123","side":"BUY","price":"0.2","size":"3"},{"asset_id":"00123","side":"BUY","price":"0.2","size":"0"}]}"#.into() },
        Message::Ended { session, reason: "shutdown".into() },
    ] {
        processor.process(&message, Duration::ZERO, &mut |o| recorder.observe(o)).unwrap();
    }
    recorder.finish(processor.finish()).unwrap();
    let report = writer.join().unwrap();
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.records_confirmed, 3);
    assert_eq!(report.raw_bytes, 64 << 20);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    assert!(matches!(
        reader.read_msg().unwrap(),
        Some(AnyMessage::Snapshot(_))
    ));
    for quantity in [3_000_000, 0] {
        let Some(AnyMessage::PriceLevelUpdate(update)) = reader.read_msg().unwrap() else {
            panic!("missing update")
        };
        let value = PriceLevelUpdateDomain::try_from_decoder(update).unwrap();
        assert_eq!(value.quantity, quantity);
        assert_eq!(value.symbol, "polymarket:v1:00123");
        assert_eq!(value.exchange_timestamp, Utc.timestamp_opt(41, 0).unwrap());
    }
    assert!(reader.read_msg().unwrap().is_none());
    let lines: Vec<serde_json::Value> = std::fs::read_to_string(sidecar.path())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        lines
            .iter()
            .map(|l| l["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["run_start", "snapshot", "unavailable", "run_end"]
    );
    assert_eq!(lines[1]["ordinal"], 0);
    assert_eq!(lines[2]["next_ordinal"], 3);
    assert_eq!(lines[3]["success"], true);
}

fn observed_level(quantity: i64) -> engineering_challenge::orderbook::Observation<'static> {
    use engineering_challenge::orderbook::{Binding, BookId, Cursor, Metadata, Observation};
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    Observation {
        asset: "00123",
        adapter: session.adapter,
        input: quantity as u64,
        binding: Binding {
            book: BookId(0),
            revision: 1,
            session,
        },
        metadata: Some(Metadata {
            cursor: Cursor {
                session,
                ordinal: 1,
                event: 0,
                entry: None,
            },
            received_at: Utc.timestamp_opt(100, 0).unwrap(),
            exchange_us: Some(42_000_000),
        }),
        kind: ObservationKind::Level {
            side: Side::Buy,
            price: Price::new(200_000).unwrap(),
            quantity: Quantity::new(quantity).unwrap(),
        },
    }
}

#[test]
fn exhausted_pool_preserves_prefix_and_encoding_failure_never_publishes_partial_record() {
    use engineering_challenge::recording;
    for exhausted in [false, true] {
        let config = recording::Config {
            batches: 2,
            publish_bytes: if exhausted { 1 } else { 262144 },
            ..Default::default()
        };
        let (mut recorder, output) = recording::channel(config, &Config::default()).unwrap();
        recorder.observe(observed_level(7)).unwrap();
        recorder.observe(observed_level(9)).unwrap();
        let mut invalid = observed_level(11);
        if !exhausted {
            invalid.metadata.as_mut().unwrap().exchange_us = None;
        }
        assert!(recorder.observe(invalid).is_err());
        assert!(recorder.observe(observed_level(13)).is_err());
        assert!(recorder.finish(Ok(())).is_err());
        let file = tempfile::NamedTempFile::new().unwrap();
        let sidecar = tempfile::NamedTempFile::new().unwrap();
        let (send, approval) = crossbeam::channel::bounded(1);
        send.send(true).unwrap();
        let report = output.run(
            file.reopen().unwrap(),
            sidecar.reopen().unwrap(),
            "prefix",
            || {},
            approval,
        );
        assert!(report.error.is_some());
        assert_eq!(report.records_confirmed, 2);
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
        for q in [7, 9] {
            let Some(AnyMessage::PriceLevelUpdate(v)) = reader.read_msg().unwrap() else {
                panic!("missing prefix")
            };
            assert_eq!(
                PriceLevelUpdateDomain::try_from_decoder(v)
                    .unwrap()
                    .quantity,
                q
            );
        }
        assert!(reader.read_msg().unwrap().is_none());
        let text = std::fs::read_to_string(sidecar.path()).unwrap();
        let footer: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
        assert_eq!(footer["success"], false);
        assert_eq!(footer["producer"]["encoded"], 2);
        assert_eq!(footer["producer"]["failure_input"], 11);
    }
}

#[derive(Clone)]
struct FailingSink {
    state: std::sync::Arc<std::sync::Mutex<(Vec<u8>, bool, usize)>>,
    limit: usize,
    zero: bool,
}
impl Write for FailingSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap();
        if state.1 {
            state.2 += 1;
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if state.0.len() == self.limit {
            state.1 = true;
            return if self.zero {
                Ok(0)
            } else {
                Err(io::ErrorKind::BrokenPipe.into())
            };
        }
        let n = bytes.len().min(self.limit - state.0.len());
        state.0.extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn output_failure_counts_exact_prefix_and_never_retries_on_drop() {
    use engineering_challenge::recording;
    for (sidecar_fails, zero, limit) in [
        (false, false, 17),
        (false, true, 70),
        (true, false, 0),
        (true, false, 1000),
    ] {
        let (mut recorder, output) =
            recording::channel(recording::Config::default(), &Config::default()).unwrap();
        recorder.observe(observed_level(7)).unwrap();
        recorder.finish(Ok(())).unwrap();
        let sink = FailingSink {
            state: Default::default(),
            limit,
            zero,
        };
        let state = sink.state.clone();
        let (send, approval) = crossbeam::channel::bounded(1);
        send.send(true).unwrap();
        let report = if sidecar_fails {
            output.run(io::sink(), sink, "failure", || {}, approval)
        } else {
            output.run(sink, io::sink(), "failure", || {}, approval)
        };
        assert!(report.error.is_some(), "{report:?}");
        let state = state.lock().unwrap();
        assert_eq!(state.0.len(), limit);
        assert_eq!(state.2, 0, "buffer retried after terminal failure");
        if !sidecar_fails {
            assert_eq!(report.raw_bytes, limit as u64);
            assert_eq!(report.records_confirmed, 0);
        }
        if sidecar_fails && limit == 0 {
            assert_eq!(report.raw_bytes, 0);
        }
    }
}

#[test]
fn idle_publication_and_sparse_chunk_completion_work_without_new_messages() {
    use engineering_challenge::recording;
    let (mut recorder, output) = recording::channel(
        recording::Config {
            chunk_age: Duration::from_millis(20),
            ..Default::default()
        },
        &Config::default(),
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let destination = file.reopen().unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    let worker =
        std::thread::spawn(move || output.run(destination, io::sink(), "idle", || {}, approval));
    recorder.observe(observed_level(9)).unwrap();
    let due = recorder.next_publish_at().unwrap();
    recorder.publish_due(due).unwrap();
    assert!(recorder.next_publish_at().is_none());
    let began = std::time::Instant::now();
    while file.as_file().metadata().unwrap().len() < (64 << 20) {
        assert!(began.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(5));
    }
    recorder.finish(Ok(())).unwrap();
    approve.send(true).unwrap();
    let report = worker.join().unwrap();
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.raw_bytes, 64 << 20);
}

#[test]
fn selected_replicas_and_generated_snapshots_preserve_provenance_and_file_order() {
    use engineering_challenge::recording;
    let config = Config {
        snapshot_interval: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    let (mut recorder, output) = recording::channel(recording::Config::default(), &config).unwrap();
    let mut processor = Processor::new(config).unwrap();
    let a = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let b = Session {
        worker: 1,
        slot: 1,
        ..a
    };
    let received = Utc.timestamp_opt(100, 0).unwrap();
    for (session, q) in [(a, 3), (b, 8)] {
        for m in [Message::Started(session), Message::SubscriptionsRequested {session, revisions:vec![1], add:vec!["A".into()],remove:vec![]}, Message::Data {session,ordinal:1,received_at:received,received_mono:Instant::now(),payload:format!(r#"{{"event_type":"book","asset_id":"A","timestamp":"42000","bids":[{{"price":"0.2","size":"{q}"}}],"asks":[]}}"#).into()}] {
            processor.process(&m,Duration::ZERO,&mut |o| recorder.observe(o)).unwrap();
        }
    }
    processor
        .select(
            a.adapter,
            "A",
            1,
            Duration::from_secs(1),
            Utc.timestamp_opt(200, 0).unwrap(),
            &mut |o| recorder.observe(o),
        )
        .unwrap();
    processor
        .process(
            &Message::Ended {
                session: a,
                reason: "standby lost".into(),
            },
            Duration::from_secs(2),
            &mut |o| recorder.observe(o),
        )
        .unwrap();
    processor.process(&Message::Data { session:b,ordinal:2,received_at:Utc.timestamp_opt(150,0).unwrap(),received_mono:Instant::now(),payload:r#"{"event_type":"price_change","timestamp":"41000","price_changes":[{"asset_id":"A","side":"BUY","price":"0.2","size":"9"}]}"#.into() }, Duration::from_secs(3), &mut |o| recorder.observe(o)).unwrap();
    processor
        .capture_due(
            &mut || (Duration::from_secs(61), Utc.timestamp_opt(300, 0).unwrap()),
            &mut |o| recorder.observe(o),
        )
        .unwrap();
    processor
        .process(
            &Message::Ended {
                session: b,
                reason: "selected lost".into(),
            },
            Duration::from_secs(62),
            &mut |o| recorder.observe(o),
        )
        .unwrap();
    // Recovery restores state, not the missing interval. A pre-baseline delta
    // and a post-retirement delta must produce no record or minute snapshot.
    let recovered = Session { generation: 2, ..a };
    for message in [
        Message::Started(recovered),
        Message::SubscriptionsRequested { session: recovered,
            revisions: vec![1], add: vec!["A".into()], remove: vec![] },
        Message::Data { session: recovered, ordinal: 1, received_at: received, received_mono: Instant::now(),
            payload: r#"{"event_type":"price_change","timestamp":"40000","price_changes":[{"asset_id":"A","side":"BUY","price":"0.2","size":"99"}]}"#.into() },
        Message::Data { session: recovered, ordinal: 2, received_at: received, received_mono: Instant::now(),
            payload: r#"{"event_type":"book","asset_id":"A","timestamp":"40000","bids":[{"price":"0.2","size":"2"}],"asks":[]}"#.into() },
        Message::Data { session: recovered, ordinal: 3, received_at: received, received_mono: Instant::now(),
            payload: r#"{"event_type":"price_change","timestamp":"40000","price_changes":[{"asset_id":"A","side":"BUY","price":"0.2","size":"0"}]}"#.into() },
        Message::ReconcileBegin { revision: 2, chunks: 1 },
        Message::DesiredChunk { adapter: a.adapter, revision: 2, index: 0, add: vec![], remove: vec!["A".into()] },
        Message::ReconcileComplete { revision: 2 },
        Message::Data { session: recovered, ordinal: 4, received_at: received, received_mono: Instant::now(),
            payload: r#"{"event_type":"price_change","timestamp":"40000","price_changes":[{"asset_id":"A","side":"BUY","price":"0.2","size":"333"}]}"#.into() },
        Message::Ended { session: recovered, reason: "retired".into() },
    ] {
        processor.process(&message, Duration::from_secs(63), &mut |o| recorder.observe(o)).unwrap();
        if matches!(message, Message::Data { ordinal: 2, .. }) {
            // Existing policy is manual after switching slots; automatic
            // candidate choice belongs to R3, not this baseline replay.
            processor.select(a.adapter, "A", 0, Duration::from_secs(63), received, &mut |o| recorder.observe(o)).unwrap();
        }
    }
    processor
        .capture_due(&mut || (Duration::from_secs(124), received), &mut |o| {
            recorder.observe(o)
        })
        .unwrap();
    recorder.finish(processor.finish()).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let sidecar = tempfile::NamedTempFile::new().unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    approve.send(true).unwrap();
    let report = output.run(
        file.reopen().unwrap(),
        sidecar.reopen().unwrap(),
        "replicas",
        || {},
        approval,
    );
    assert!(report.error.is_none(), "{report:?}");
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    for (kind, time, exchange, q) in [
        ("snapshot", 100, 42, 3_000_000),
        ("snapshot", 200, 42, 8_000_000),
        ("level", 150, 41, 9_000_000),
        ("snapshot", 300, 41, 9_000_000),
        ("snapshot", 100, 40, 2_000_000),
        ("level", 100, 40, 0),
    ] {
        match reader.read_msg().unwrap().unwrap() {
            AnyMessage::Snapshot(s) => {
                assert_eq!(kind, "snapshot");
                let s = SnapshotDomain::try_from_decoder(s).unwrap();
                assert_eq!(
                    s,
                    SnapshotDomain {
                        symbol: "polymarket:v1:A".into(),
                        received_at: Utc.timestamp_opt(time, 0).unwrap(),
                        exchange_timestamp: Utc.timestamp_opt(exchange, 0).unwrap(),
                        bids: vec![SnapshotBidsEntryDomain {
                            price_level: 200_000,
                            quantity: q
                        }],
                        asks: vec![],
                    }
                );
            }
            AnyMessage::PriceLevelUpdate(l) => {
                assert_eq!(kind, "level");
                let l = PriceLevelUpdateDomain::try_from_decoder(l).unwrap();
                assert_eq!(
                    l,
                    PriceLevelUpdateDomain {
                        symbol: "polymarket:v1:A".into(),
                        backtest_timestamp: Utc.timestamp_opt(time, 0).unwrap(),
                        exchange_timestamp: Utc.timestamp_opt(exchange, 0).unwrap(),
                        side: Side::Buy,
                        price_level: 200_000,
                        quantity: q,
                    }
                );
            }
            _ => panic!("wrong template"),
        }
    }
    assert!(reader.read_msg().unwrap().is_none());
    let text = std::fs::read_to_string(sidecar.path()).unwrap();
    let entries: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let gaps: Vec<_> = entries
        .iter()
        .filter(|e| e["kind"] == "unavailable")
        .collect();
    assert_eq!(gaps.len(), 2);
    assert_eq!(gaps[0]["binding"]["worker"], 1);
    assert_eq!(gaps[0]["next_ordinal"], 4);
    assert_eq!(gaps[1]["binding"]["worker"], 0);
    assert_eq!(gaps[1]["binding"]["generation"], 2);
    assert_eq!(gaps[1]["next_ordinal"], 6);
    assert_eq!(gaps[1]["cause"], "retired");
    let periodic: Vec<_> = entries
        .iter()
        .filter(|e| e["origin"] == "periodic")
        .collect();
    assert_eq!(periodic.len(), 1);
    assert_eq!(periodic[0]["due_us"], 61_000_000);
    let standby: Vec<_> = entries
        .iter()
        .filter(|e| e["kind"] == "standby_baseline")
        .collect();
    assert_eq!(standby.len(), 2);
    assert_eq!(standby[0]["binding"]["worker"], 1);
    assert_eq!(standby[1]["binding"]["generation"], 2);
    assert!(entries.iter().any(|e| e["kind"] == "replica_unavailable"
        && e["binding"]["worker"] == 0
        && e["binding"]["generation"] == 1
        && e["cause"] == "source_ended"));
}

#[test]
fn a_paused_writer_exhausts_the_fixed_pool_without_losing_the_earlier_prefix() {
    use engineering_challenge::recording;
    struct Gate {
        file: std::fs::File,
        entered: Option<crossbeam::channel::Sender<()>>,
        release: crossbeam::channel::Receiver<()>,
    }
    impl Write for Gate {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.file.write(b)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.file.flush()
        }
    }
    let (mut recorder, output) = recording::channel(
        recording::Config {
            batches: 1,
            publish_bytes: 1,
            buffer_bytes: 1, // Block while the writer still owns the sole batch.
            ..Default::default()
        },
        &Config::default(),
    )
    .unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let (entered, entry) = crossbeam::channel::bounded(1);
    let (release, gate) = crossbeam::channel::bounded(1);
    let (approve, approval) = crossbeam::channel::bounded(1);
    let destination = Gate {
        file: file.reopen().unwrap(),
        entered: Some(entered),
        release: gate,
    };
    let thread =
        std::thread::spawn(move || output.run(destination, io::sink(), "pause", || {}, approval));
    recorder.observe(observed_level(7)).unwrap();
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(recorder.observe(observed_level(9)).is_err());
    assert!(recorder.finish(Ok(())).is_err());
    release.send(()).unwrap();
    approve.send(true).unwrap();
    let report = thread.join().unwrap();
    assert!(report.error.is_some());
    assert_eq!(report.records_confirmed, 1);
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let Some(AnyMessage::PriceLevelUpdate(v)) = reader.read_msg().unwrap() else {
        panic!("lost prefix")
    };
    assert_eq!(
        PriceLevelUpdateDomain::try_from_decoder(v)
            .unwrap()
            .quantity,
        7
    );
    assert!(reader.read_msg().unwrap().is_none());
}

#[test]
fn a_writer_pause_uses_the_remaining_batch_instead_of_publishing_tiny_tails() {
    use engineering_challenge::recording;
    let (mut recorder, output) = recording::channel(
        recording::Config {
            batches: 2,
            ..Default::default()
        },
        &Config::default(),
    )
    .unwrap();
    // Writer is deliberately not scheduled yet. All records still fit comfortably
    // in the bounded pool; age alone must not waste its last reusable batch.
    for q in 1..=100 {
        recorder.observe(observed_level(q)).unwrap();
        recorder
            .publish_due(recorder.next_publish_at().unwrap())
            .unwrap();
    }
    recorder.finish(Ok(())).unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    approve.send(true).unwrap();
    let report = output.run(
        file.reopen().unwrap(),
        io::sink(),
        "temporary pause",
        || {},
        approval,
    );
    assert!(report.error.is_none(), "{report:?}");
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    for q in 1..=100 {
        let Some(AnyMessage::PriceLevelUpdate(value)) = reader.read_msg().unwrap() else {
            panic!("missing update")
        };
        assert_eq!(
            PriceLevelUpdateDomain::try_from_decoder(value)
                .unwrap()
                .quantity,
            q
        );
    }
    assert!(reader.read_msg().unwrap().is_none());
}

#[test]
fn output_panic_is_detectable_without_another_market_message() {
    use engineering_challenge::recording;
    struct Panics;
    impl Write for Panics {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            panic!("injected output panic")
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (mut recorder, output) = recording::channel(
        recording::Config {
            publish_bytes: 1,
            buffer_bytes: 1,
            ..Default::default()
        },
        &Config::default(),
    )
    .unwrap();
    let (_approve, approval) = crossbeam::channel::bounded(1);
    recorder.observe(observed_level(7)).unwrap();
    let worker =
        std::thread::spawn(move || output.run(Panics, io::sink(), "panic", || {}, approval));
    assert!(worker.join().is_err());
    assert!(recorder.check_output().is_err());
    assert!(recorder.finish(Ok(())).is_err());
}

#[test]
fn upstream_failure_is_incomplete_without_inventing_a_recorder_failure_position() {
    use engineering_challenge::recording;
    let (mut recorder, output) =
        recording::channel(recording::Config::default(), &Config::default()).unwrap();
    recorder.observe(observed_level(7)).unwrap();
    assert!(
        recorder
            .finish(Err(anyhow::anyhow!("upstream failed after its next input")))
            .is_err()
    );
    let sidecar = tempfile::NamedTempFile::new().unwrap();
    let (approve, approval) = crossbeam::channel::bounded(1);
    approve.send(true).unwrap();
    let report = output.run(
        io::sink(),
        sidecar.reopen().unwrap(),
        "upstream failure",
        || {},
        approval,
    );
    assert!(report.error.is_some());
    let text = std::fs::read_to_string(sidecar.path()).unwrap();
    let footer: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(footer["success"], false);
    assert_eq!(footer["producer"]["input_complete"], false);
    assert_eq!(footer["producer"]["failure_input"], serde_json::Value::Null);
}
