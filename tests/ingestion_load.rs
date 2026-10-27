//! Opt-in raw transport load validation, not an exchange-capacity benchmark.
use engineering_challenge::{
    discovery::AdapterId,
    websocket::{Config, ingress, run_worker},
};
use futures_util::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit release-mode local raw-message load run"]
async fn offered_bursts_preserve_every_raw_message_in_order() {
    const COUNT: usize = 100_000;
    const BURST: usize = 40;
    let (send, receive) = ingress::channel(8192, 64 << 20).unwrap();
    let consumer = std::thread::spawn(move || {
        let mut count = 0;
        let mut queue_ages = Vec::with_capacity(COUNT);
        let mut started = false;
        let mut ended = false;
        for envelope in receive {
            match &envelope.message {
                ingress::Message::Retired { .. } => panic!("unexpected retirement"),
                ingress::Message::Started(session) => {
                    assert!(!started);
                    assert_eq!(session.generation, 1);
                    assert_eq!(session.adapter, AdapterId::PolymarketV2);
                    started = true;
                }
                ingress::Message::SubscriptionsRequested { add, remove, .. } => {
                    assert!(started && !ended);
                    assert_eq!(add, &["00123"]);
                    assert!(remove.is_empty());
                }
                ingress::Message::Data {
                    session,
                    ordinal,
                    payload,
                    ..
                } => {
                    assert!(started && !ended);
                    assert_eq!(session.generation, 1);
                    assert_eq!(*ordinal, count as u64 + 1);
                    assert_eq!(payload.as_str(), format!("{count}: raw message λ"));
                    queue_ages.push(envelope.queued_at.elapsed());
                    count += 1;
                }
                ingress::Message::Ended { .. } => {
                    assert!(started && !ended);
                    ended = true;
                }
            }
        }
        assert!(ended);
        assert_eq!(count, COUNT);
        queue_ages.sort_unstable();
        queue_ages
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Arc::new(Config {
        endpoint: format!("ws://{}", listener.local_addr().unwrap()),
        ..Config::default()
    });
    let (_commands, receive) = mpsc::channel(8);
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV2,
        vec!["00123".into()],
        receive,
        send,
        config,
        stop.clone(),
    ));
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        let started = Instant::now();
        let mut max_offer_lag = Duration::ZERO;
        for i in 0..COUNT {
            // Fixed offered schedule, never waiting for a consumer acknowledgement.
            let scheduled = started + Duration::from_millis((i / BURST) as u64);
            if i % BURST == 0 {
                sleep_until(scheduled).await;
            }
            max_offer_lag = max_offer_lag.max(Instant::now().saturating_duration_since(scheduled));
            ws.send(Message::Text(format!("{i}: raw message λ").into()))
                .await
                .unwrap();
        }
        // Pong establishes that the worker read the preceding complete messages.
        ws.send(Message::Ping(vec![42].into())).await.unwrap();
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Pong(p) if p.as_ref() == [42] => break,
                Message::Text(t) if t == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap();
                }
                other => panic!("unexpected response {other:?}"),
            }
        }
        stop.send_replace(true);
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                let _ = ws.flush().await;
                break;
            }
        }
        (started, max_offer_lag)
    });
    let (started, max_offer_lag) = timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap();
    let report = timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.messages, COUNT as u64);
    let ages = consumer.join().unwrap();
    let drained_in = started.elapsed();
    eprintln!(
        "RAW_LOAD accepted={COUNT} offered_per_second=40000 drain_seconds={:.6} drained_messages_per_second={:.0} max_offer_lag_us={} queue_age_us_p50={} p95={} p99={} p999={} max={}",
        drained_in.as_secs_f64(),
        COUNT as f64 / drained_in.as_secs_f64(),
        max_offer_lag.as_micros(),
        ages[COUNT / 2].as_micros(),
        ages[COUNT * 95 / 100].as_micros(),
        ages[COUNT * 99 / 100].as_micros(),
        ages[COUNT * 999 / 1000].as_micros(),
        ages.last().unwrap().as_micros()
    );
}

#[test]
#[ignore = "explicit release-mode paced C3/C4 replica replay; no network or SBE"]
fn paced_replica_replay_preserves_selected_history() {
    use chrono::Utc;
    use engineering_challenge::{
        Side,
        orderbook::{self, ObservationKind, Processor},
    };
    use serde_json::json;
    const COUNT: usize = 100_000;
    const ASSETS: usize = 32;
    let (send, receive) = ingress::channel(8192, 64 << 20).unwrap();
    let consumer = std::thread::spawn(move || {
        let mut p = Processor::new(orderbook::Config::default()).unwrap();
        let start = std::time::Instant::now();
        let mut queue = Vec::with_capacity(COUNT);
        let mut completion = Vec::with_capacity(COUNT);
        let mut processing = Vec::with_capacity(COUNT);
        let mut levels = 0;
        let mut snapshots = 0;
        let mut sink = |o: orderbook::Observation<'_>| {
            match o.kind {
                ObservationKind::Level {
                    price,
                    quantity,
                    side,
                } => {
                    let m = o.metadata.unwrap();
                    let i = (m.cursor.ordinal as usize - 1) * 2 + m.cursor.session.worker;
                    let entry = m.cursor.entry.unwrap();
                    assert_eq!(o.binding.slot, 0);
                    assert_eq!(o.asset, format!("A{}", (i / 2) % ASSETS));
                    assert_eq!(price.value(), 500000);
                    assert_eq!(quantity.value(), ((i + entry) % 13) as i64 * 1_000_000);
                    assert_eq!(
                        side,
                        if (i / 64).is_multiple_of(2) {
                            Side::Buy
                        } else {
                            Side::Sell
                        }
                    );
                    levels += 1;
                }
                ObservationKind::Snapshot(book) => {
                    let asset = o.asset[1..].parse::<usize>().unwrap();
                    assert_eq!(book.len(), asset % 16 + 1);
                    assert_eq!(
                        book.best(Side::Buy).unwrap().0.value(),
                        (asset % 16 + 1) as i64 * 10000
                    );
                    snapshots += 1;
                }
                ObservationKind::Invalidated { .. } => {}
            }
            Ok(())
        };
        for envelope in receive {
            let before = std::time::Instant::now();
            let age = envelope.queued_at.elapsed();
            p.process(&envelope.message, start.elapsed(), &mut sink)
                .unwrap();
            if let ingress::Message::Data { received_at, .. } = &envelope.message {
                processing.push(before.elapsed());
                queue.push(age);
                // Same-process wall-clock difference; not NIC/network latency.
                completion.push((Utc::now() - *received_at).num_microseconds().unwrap());
            }
        }
        p.finish().unwrap();
        assert_eq!(p.stats().messages, COUNT as u64);
        assert_eq!(p.stats().changes, (COUNT - 100) as u64 * 4);
        assert_eq!(p.stats().rejected, 0);
        assert_eq!(levels, (COUNT / 2 - 50) * 4);
        assert_eq!(snapshots, 50 * ASSETS);
        // The last update for each replica/asset must survive independently.
        for slot in 0..2 {
            for asset in 0..ASSETS {
                let r = p
                    .replica(AdapterId::PolymarketV2, &format!("A{asset}"), slot)
                    .unwrap();
                let i = (0..COUNT)
                    .rev()
                    .find(|i| i % 2 == slot as usize && (i / 2) % ASSETS == asset)
                    .unwrap();
                let side = if (i / 64) % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let quantity = r
                    .book
                    .quantity(
                        side,
                        engineering_challenge::market_data::Price::new(500000).unwrap(),
                    )
                    .map_or(0, |q| q.value());
                assert_eq!(quantity, ((i + 3) % 13) as i64 * 1_000_000);
            }
        }
        queue.sort_unstable();
        completion.sort_unstable();
        processing.sort_unstable();
        (queue, completion, processing)
    });
    let sessions = [0, 1].map(|worker| ingress::Session {
        worker,
        generation: 1,
        adapter: AdapterId::PolymarketV2,
    });
    for s in sessions {
        send.send(ingress::Message::Started(s)).unwrap();
        send.send(ingress::Message::SubscriptionsRequested {
            session: s,
            source_slot: s.worker as u16,
            revisions: vec![1; ASSETS],
            add: (0..ASSETS).map(|i| format!("A{i}")).collect(),
            remove: vec![],
        })
        .unwrap();
    }
    let snapshot=json!((0..ASSETS).map(|i|json!({"event_type":"book","asset_id":format!("A{i}"),"timestamp":"42","bids":(1..=i%16+1).map(|n|json!({"price":format!("0.{:02}",n),"size":n.to_string()})).collect::<Vec<_>>(),"asks":[]})).collect::<Vec<_>>()).to_string();
    // Prebuild a finite workload so JSON construction does not pace the producer.
    let changes:Vec<_>=(0..(ASSETS*2*13*2)).map(|i|json!({"event_type":"price_change","timestamp":"42","price_changes":(0..4).map(|j|json!({"asset_id":format!("A{}",(i/2)%ASSETS),"side":if (i/64)%2==0 {"BUY"}else{"SELL"},"price":"0.5","size":((i+j)%13).to_string()})).collect::<Vec<_>>()} ).to_string()).collect();
    let start = std::time::Instant::now();
    let mut lag = Duration::ZERO;
    let mut bytes = 0;
    for i in 0..COUNT {
        let scheduled = start + Duration::from_millis((i / 40) as u64);
        if i % 40 == 0 {
            std::thread::sleep(scheduled.saturating_duration_since(std::time::Instant::now()));
        }
        lag = lag.max(std::time::Instant::now().saturating_duration_since(scheduled));
        let payload = if i % 2000 < 2 {
            &snapshot
        } else {
            &changes[i % changes.len()]
        };
        bytes += payload.len();
        send.send(ingress::Message::Data {
            session: sessions[i % 2],
            ordinal: (i / 2 + 1) as u64,
            received_at: Utc::now(),
            payload: payload.clone().into(),
        })
        .unwrap();
    }
    for session in sessions {
        send.send(ingress::Message::Ended {
            session,
            reason: "replay end".into(),
        })
        .unwrap();
    }
    drop(send);
    let (queue, completion, processing) = consumer.join().unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    let indices = [
        COUNT / 2,
        COUNT * 95 / 100,
        COUNT * 99 / 100,
        COUNT * 999 / 1000,
        COUNT - 1,
    ];
    eprintln!(
        "BOOK_LOAD messages={COUNT} instruments={ASSETS} replicas=64 bytes={bytes} target_rate=40000 achieved_rate={:.0} seconds={elapsed:.6} max_offer_lag_us={} queue_us={:?} completion_us={:?} processing_us={:?}",
        COUNT as f64 / elapsed,
        lag.as_micros(),
        indices.map(|i| queue[i].as_micros()),
        indices.map(|i| completion[i]),
        indices.map(|i| processing[i].as_micros())
    );
}
