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
