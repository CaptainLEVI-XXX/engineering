//! Opt-in capacity diagnostic through the actual production owner loop and C5.
//! Synthetic independent arrivals; no REST, socket parsing or exchange coverage claim.
use super::*;
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::*},
};
use std::{fmt::Write, time::Instant as Clock};

fn number(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |s| s.parse().unwrap())
}

#[test]
fn default_ingress_retains_a_live_sized_burst_in_order() {
    // The live diagnostic observed 23,404 queued messages that promptly drained.
    // Hold the consumer unscheduled to make a nearby 25k burst deterministic.
    let (send, receive) = ingress::channel(DEFAULT_QUEUE_ITEMS, 64 << 20).unwrap();
    let session = Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let received_at = chrono::Utc::now();
    for ordinal in 1..=25_000 {
        send.send(Message::Data {
            session,
            ordinal,
            received_at,
            received_mono: Clock::now(),
            payload: format!("observed-{ordinal}").into(),
        })
        .unwrap();
    }
    drop(send);
    for ordinal in 1..=25_000 {
        let envelope = receive.recv().unwrap();
        let Message::Data {
            ordinal: actual,
            received_at: actual_time,
            payload,
            ..
        } = &envelope.message
        else {
            panic!("unexpected control")
        };
        assert_eq!(*actual, ordinal);
        assert_eq!(*actual_time, received_at);
        assert_eq!(payload.as_str(), format!("observed-{ordinal}"));
    }
    assert!(receive.recv().is_err());
}

#[test]
#[ignore = "bounded full-owner capacity test: OWNER_BOOKS/OWNER_RATE/OWNER_SECONDS; local only"]
fn production_owner_capacity_preserves_every_record() {
    let books = number("OWNER_BOOKS", 550_000);
    let rate = number("OWNER_RATE", 10_000);
    let seconds = number("OWNER_SECONDS", 70);
    let replicas = number("OWNER_REPLICAS", 1);
    assert!((1..=2).contains(&replicas));
    assert!((1..=600_000).contains(&books));
    assert!((1..=100_000).contains(&rate) && (1..=120).contains(&seconds));
    assert!((rate * seconds).is_multiple_of(replicas));
    let groups = books.div_ceil(1000);
    let workers = groups * replicas;
    let directory = tempfile::tempdir().unwrap();
    let archive = directory.path().join("archive.sbe");
    let sidecar = directory.path().join("integrity.jsonl");
    let config = orderbook::Config {
        sessions: workers,
        selection: if replicas == 2 {
            orderbook::SelectionPolicy::Automatic
        } else {
            orderbook::SelectionPolicy::Manual
        },
        monitoring: replicas == 2,
        snapshot_interval: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    let (recorder, output) = recording::channel(recording::Config::default(), &config).unwrap();
    let (approval, approved) = crossbeam::channel::bounded(1);
    let file = std::fs::File::create(&archive).unwrap();
    let metadata = std::fs::File::create(&sidecar).unwrap();
    let (ready, started) = crossbeam::channel::bounded(1);
    let writer = std::thread::spawn(move || {
        output.run(
            file,
            metadata,
            "owner capacity",
            || {
                ready.send(()).unwrap();
            },
            approved,
        )
    });
    // Match the executable: no intake before the writer's startup flush.
    started.recv_timeout(Duration::from_secs(15)).unwrap();
    let (send, receive) = ingress::channel(DEFAULT_QUEUE_ITEMS, 64 << 20).unwrap();
    let (recovery, mut requests) = mpsc::channel(workers);
    let (stop, stopped) = watch::channel(false);
    let start = Clock::now();
    let owner = std::thread::spawn(move || {
        process_books(
            receive,
            config,
            recovery,
            stopped,
            Some(recorder),
            None,
            start,
        )
    });
    let sessions: Vec<_> = (0..workers)
        .map(|worker| Session {
            worker,
            generation: 1,
            adapter: AdapterId::PolymarketV1,
            slot: (worker % replicas) as u16,
        })
        .collect();
    let mut ordinals = vec![0u64; workers];
    send.send(Message::ReconcileBegin {
        revision: 1,
        chunks: groups as u32,
    })
    .unwrap();
    for session in &sessions {
        let group = session.worker / replicas;
        let first = group * 1000;
        let end = (first + 1000).min(books);
        let assets: Vec<_> = (first..end).map(|id| format!("{id:078}")).collect();
        if session.slot == 0 {
            send.send(Message::DesiredChunk {
                adapter: session.adapter,
                revision: 1,
                index: group as u32,
                add: assets.clone(),
                remove: vec![],
            })
            .unwrap();
        }
        send.send(Message::Started(*session)).unwrap();
        send.send(Message::SubscriptionsRequested {
            session: *session,
            revisions: vec![1; assets.len()],
            add: assets,
            remove: vec![],
        })
        .unwrap();
        send.send(Message::RequestsComplete { session: *session })
            .unwrap();
        // Initialization is deliberately paced by full readiness, outside offered-load timing.
        let mut payload = String::from("[");
        for id in first..end {
            if id != first {
                payload.push(',');
            }
            write!(
                payload,
                r#"{{"event_type":"book","asset_id":"{id:078}","timestamp":"1","bids":["#
            )
            .unwrap();
            for p in 1..=id % 9 {
                if p > 1 {
                    payload.push(',');
                }
                write!(payload, r#"{{"price":"0.{p:02}","size":"3"}}"#).unwrap();
            }
            payload.push_str("],\"asks\":[");
            for p in 1..=id % 9 {
                if p > 1 {
                    payload.push(',');
                }
                write!(payload, r#"{{"price":"0.{:02}","size":"7"}}"#, 100 - p).unwrap();
            }
            payload.push_str("]}");
        }
        payload.push(']');
        ordinals[session.worker] += 1;
        send.send(Message::Data {
            session: *session,
            ordinal: ordinals[session.worker],
            received_at: chrono::Utc::now(),
            received_mono: Clock::now(),
            payload: payload.into(),
        })
        .unwrap();
        let deadline = Clock::now() + Duration::from_secs(30);
        loop {
            match requests.try_recv() {
                Ok(RecoveryRequest::Initialized(s)) => {
                    assert_eq!(s, *session);
                    break;
                }
                Ok(other) => panic!("unexpected initialization recovery: {other:?}"),
                Err(mpsc::error::TryRecvError::Empty) => {
                    assert!(Clock::now() < deadline, "initialization timeout");
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => {
                    let result = owner.join().unwrap();
                    let _ = approval.send(false);
                    let output = writer.join().unwrap();
                    panic!("owner failed during initialization: {error}; {result:?}; {output:?}");
                }
            }
        }
    }
    send.send(Message::ReconcileComplete { revision: 1 })
        .unwrap();
    let initialized_seconds = start.elapsed().as_secs_f64();
    eprintln!(
        "owner_load_ready pid={} books={books} workers={workers} initialization_s={initialized_seconds}",
        std::process::id()
    );
    let offered = Clock::now();
    let mut accepted = 0;
    let mut failure = None;
    let mut max_offer_lag = Duration::ZERO;
    for n in 0..rate * seconds {
        let due = offered + Duration::from_secs_f64(n as f64 / rate as f64);
        if let Some(wait) = due.checked_duration_since(Clock::now()) {
            std::thread::sleep(wait);
        }
        max_offer_lag = max_offer_lag.max(Clock::now().saturating_duration_since(due));
        // Physical deliveries alternate replicas; both receive the same logical
        // history. Only slot zero may publish, never a duplicate from standby.
        let event = n / replicas;
        let id = event.wrapping_mul(104729) % books;
        let worker = id / 1000 * replicas + n % replicas;
        let payload = if event.is_multiple_of(5) {
            format!(r#"{{"event_type":"last_trade_price","asset_id":"{id:078}"}}"#)
        } else {
            format!(
                r#"{{"event_type":"price_change","timestamp":"{}","price_changes":[{{"asset_id":"{id:078}","side":"BUY","price":"0.01","size":"{}"}}]}}"#,
                event % 7 + 1,
                event % 11
            )
        };
        ordinals[worker] += 1;
        if let Err(error) = send.send(Message::Data {
            session: sessions[worker],
            ordinal: ordinals[worker],
            received_at: chrono::Utc::now(),
            received_mono: Clock::now(),
            payload: payload.into(),
        }) {
            failure = Some(error.to_string());
            break;
        }
        accepted += 1;
    }
    let offered_seconds = offered.elapsed().as_secs_f64();
    stop.send_replace(true);
    for session in sessions {
        if let Err(error) = send.send(Message::Ended {
            session,
            reason: "shutdown".into(),
        }) {
            failure.get_or_insert(error.to_string());
            break;
        }
    }
    drop(send);
    let result = owner.join().unwrap();
    let _ = approval.send(result.is_ok() && failure.is_none());
    let output = writer.join().unwrap();
    eprintln!(
        "owner_load_result {}",
        serde_json::json!({"books":books,"workers":workers,"replicas":replicas,"rate":rate,
        "queue_items":DEFAULT_QUEUE_ITEMS,"queue_bytes":64 << 20,
        "seconds":seconds,"initialized_seconds":initialized_seconds,"accepted":accepted,
        "offered_seconds":offered_seconds,"max_offer_lag_us":max_offer_lag.as_micros(),
        "failure":failure,"owner":format!("{result:?}"),"output":output})
    );

    // Independent arithmetic oracle: verify every delta in producer order and
    // every full snapshot, including timed captures, after the writer drains.
    let mut quantities: Vec<_> = (0..books)
        .map(|id| if id % 9 == 0 { 0 } else { 3_000_000 })
        .collect();
    let mut exchange = vec![1000i64; books];
    let mut initialized = vec![false; books];
    let mut captured = vec![false; books];
    let mut next = 0usize;
    let mut records = 0u64;
    let mut periodic = 0usize;
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive).unwrap();
    while let Some(record) = reader.read_msg().unwrap() {
        match record {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s).unwrap();
                let id: usize = s
                    .symbol
                    .strip_prefix("polymarket:v1:")
                    .unwrap()
                    .parse()
                    .unwrap();
                assert_eq!(s.symbol, format!("polymarket:v1:{id:078}"));
                assert_eq!(s.exchange_timestamp.timestamp_micros(), exchange[id]);
                let bids: Vec<_> = (1..=(id % 9).max(1))
                    .rev()
                    .filter_map(|p| {
                        let q = if p == 1 { quantities[id] } else { 3_000_000 };
                        (q != 0).then_some([p as i64 * 10_000, q])
                    })
                    .collect();
                let asks: Vec<_> = (1..=id % 9)
                    .rev()
                    .map(|p| [(100 - p) as i64 * 10_000, 7_000_000])
                    .collect();
                assert_eq!(
                    s.bids
                        .iter()
                        .map(|l| [l.price_level, l.quantity])
                        .collect::<Vec<_>>(),
                    bids
                );
                assert_eq!(
                    s.asks
                        .iter()
                        .map(|l| [l.price_level, l.quantity])
                        .collect::<Vec<_>>(),
                    asks
                );
                periodic += usize::from(initialized[id]);
                captured[id] |= initialized[id];
                initialized[id] = true;
            }
            AnyMessage::PriceLevelUpdate(d) => {
                while next.is_multiple_of(5) {
                    next += 1;
                }
                assert!(
                    next < accepted / replicas,
                    "unexpected delta after accepted prefix"
                );
                let id = next.wrapping_mul(104729) % books;
                let d = PriceLevelUpdateDomain::try_from_decoder(d).unwrap();
                assert!(initialized[id]);
                assert_eq!(d.symbol, format!("polymarket:v1:{id:078}"));
                assert_eq!(d.side, Side::Buy);
                assert_eq!(d.price_level, 10_000);
                assert_eq!(d.quantity, (next % 11) as i64 * 1_000_000);
                assert_eq!(
                    d.exchange_timestamp.timestamp_micros(),
                    (next % 7 + 1) as i64 * 1000
                );
                quantities[id] = d.quantity;
                exchange[id] = d.exchange_timestamp.timestamp_micros();
                next += 1;
            }
            _ => panic!("unknown record"),
        }
        records += 1;
    }
    while next < accepted / replicas && next.is_multiple_of(5) {
        next += 1;
    }
    assert_eq!(
        next,
        accepted / replicas,
        "missing accepted financial update"
    );
    assert!(initialized.into_iter().all(|ready| ready));
    assert_eq!(records, output.records_confirmed);
    eprintln!(
        "owner_load_oracle records={records} periodic={periodic} accepted={accepted} matched=true"
    );
    assert!(
        failure.is_none() && result.is_ok() && output.error.is_none(),
        "capacity gate failed"
    );
    assert_eq!(accepted, rate * seconds);
    if seconds >= 65 {
        assert!(
            captured.into_iter().all(|seen| seen),
            "missing periodic capture for an initialized live book"
        );
    }
    assert!(
        max_offer_lag < Duration::from_secs(1),
        "producer could not sustain offered workload"
    );
    if number("OWNER_CHECK_HEALTH", 0) != 0 {
        let summary = std::fs::read_to_string(&sidecar)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|row| row["event"]["kind"] == "owner_summary")
            .expect("missing owner summary");
        let stats = &summary["event"]["stats"];
        let revisit: Duration =
            serde_json::from_value(stats["max_health_revisit"].clone()).unwrap();
        let late: Duration = serde_json::from_value(stats["max_health_lateness"].clone()).unwrap();
        assert!(
            revisit <= Duration::from_secs(11),
            "health revisit: {revisit:?}"
        );
        assert!(late <= Duration::from_secs(1), "health lateness: {late:?}");
    }
}
