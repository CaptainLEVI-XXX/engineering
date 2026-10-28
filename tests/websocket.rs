use engineering_challenge::{
    discovery::{AdapterId, Instrument, MarketEntry, MarketKey, Snapshot},
    websocket::{
        Assignments, Config, RecoveryRequest, WorkerCommand,
        ingress::{self as output, Message as OutputMessage},
        run_worker,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    time::timeout,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn snapshot(id: &str) -> String {
    json!({"event_type":"book","asset_id":id,"timestamp":"1793090300123","bids":[{"price":"0.2","size":"7"}],"asks":[]}).to_string()
}
fn market(state: &mut Snapshot, id: &str, adapter: AdapterId, assets: &[&str]) {
    state.markets.insert(
        MarketKey {
            adapter,
            id: id.into(),
        },
        MarketEntry {
            desired: true,
            uncertainty: None,
            instruments: assets
                .iter()
                .map(|a| Instrument {
                    id: (*a).into(),
                    label: (*a).into(),
                })
                .collect(),
        },
    );
}

#[test]
fn assignments_are_stable_atomic_version_scoped_and_capacity_checked() {
    let mut state = Snapshot::default();
    market(&mut state, "M", AdapterId::PolymarketV1, &["A", "B"]);
    market(&mut state, "M", AdapterId::PolymarketV2, &["A", "B"]);
    let mut a = Assignments::new(2, 3);
    let first = a.reconcile(&state).unwrap();
    assert_eq!(first.len(), 2);
    assert_ne!(first[0].worker, first[1].worker);
    assert!(
        first
            .iter()
            .all(|c| c.add.len() == 2 && c.remove.is_empty())
    );
    assert!(a.reconcile(&state).unwrap().is_empty());
    market(&mut state, "N", AdapterId::PolymarketV1, &["C", "D"]);
    let next = a.reconcile(&state).unwrap();
    assert_eq!(next.len(), 1);
    assert!(!first.iter().any(|c| c.worker == next[0].worker));
    assert_eq!(a.assigned_assets(), 6);
    state.markets.remove(&MarketKey {
        adapter: AdapterId::PolymarketV1,
        id: "M".into(),
    });
    let removed = a.reconcile(&state).unwrap();
    assert_eq!(removed[0].remove, vec!["A", "B"]);
    market(
        &mut state,
        "large",
        AdapterId::PolymarketV1,
        &["E", "F", "G"],
    );
    assert!(a.reconcile(&state).is_err());
}

#[test]
fn replicated_assignments_are_version_scoped_stable_and_physically_bounded() {
    use std::collections::BTreeMap;
    let mut state = Snapshot::default();
    for adapter in [AdapterId::PolymarketV1, AdapterId::PolymarketV2] {
        market(&mut state, "M", adapter, &["A", "B"]);
        market(&mut state, "N", adapter, &["C"]);
    }
    let mut assignments = Assignments::with_replicas(3, 5, 2);
    assignments.reconcile(&state).unwrap();
    let rows = |a: &Assignments| -> BTreeMap<_, _> {
        a.current()
            .map(|row| {
                (
                    (row.market.adapter, row.market.id.clone(), row.slot),
                    (row.worker, row.revision, row.assets.to_vec()),
                )
            })
            .collect()
    };
    let before = rows(&assignments);
    assert_eq!(before.len(), 8);
    assert_eq!(assignments.assigned_assets(), 12);
    for adapter in [AdapterId::PolymarketV1, AdapterId::PolymarketV2] {
        assert_ne!(
            before[&(adapter, "M".into(), 0)].0,
            before[&(adapter, "M".into(), 1)].0
        );
        for slot in [0, 1] {
            assert_eq!(
                before[&(adapter, "M".into(), slot)].0,
                before[&(adapter, "N".into(), slot)].0
            );
            assert_ne!(
                before[&(AdapterId::PolymarketV1, "M".into(), slot)].0,
                before[&(AdapterId::PolymarketV2, "M".into(), slot)].0
            );
        }
    }
    assert!(assignments.reconcile(&state).unwrap().is_empty());
    assert_eq!(rows(&assignments), before);
    // Four workers are full. One physical slot remains, but the new market
    // needs two: failure must not admit its first replica or disturb old ones.
    market(&mut state, "blocked", AdapterId::PolymarketV1, &["D"]);
    let error = assignments
        .reconcile(&state)
        .err()
        .expect("last slot must not allow a one-replica fallback")
        .to_string();
    assert!(error.contains("blocked") && error.contains("D"));
    assert_eq!(rows(&assignments), before);
    assert_eq!(assignments.assigned_assets(), 12);
}

fn config(endpoint: String) -> Arc<Config> {
    Arc::new(Config {
        endpoint,
        heartbeat: Duration::from_millis(30),
        pong_timeout: Duration::from_millis(300),
        reconnect_min: Duration::from_millis(10),
        reconnect_max: Duration::from_millis(30),
        io_timeout: Duration::from_millis(300),
        ..Config::default()
    })
}

#[tokio::test]
async fn worker_preserves_history_across_dynamic_add_and_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (tx, commands) = mpsc::channel(8);
    let (output, received) = output::channel(64, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV1,
        vec!["A".into()],
        commands,
        output,
        config,
        stop.clone(),
    ));
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let initial = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&initial).unwrap()["assets_ids"],
            json!(["A"])
        );
        ws.send(Message::Text(snapshot("A").into())).await.unwrap();
        ws.send(Message::Text(json!({"event_type":"price_change","timestamp":"1793090300122","price_changes":[{"asset_id":"A","side":"BUY","price":"0.2","size":"3"}]}).to_string().into())).await.unwrap();
        tx.send(WorkerCommand {
            add: vec!["B".into()],
            remove: vec![],
            ..WorkerCommand::default()
        })
        .await
        .unwrap();
        loop {
            let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
            if text == "PING" {
                ws.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            assert_eq!(
                serde_json::from_str::<Value>(&text).unwrap()["assets_ids"],
                json!(["B"])
            );
            break;
        }
        ws.send(Message::Text(snapshot("B").into())).await.unwrap();
        ws.close(None).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap()["assets_ids"],
            json!(["A", "B"])
        );
        ws.send(Message::Text(
            format!("[{},{}]", snapshot("A"), snapshot("B")).into(),
        ))
        .await
        .unwrap();
        while let Some(message) = ws.next().await {
            match message.unwrap() {
                Message::Text(t) if t == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap();
                    stop.send_replace(true);
                }
                Message::Close(_) => {
                    let _ = ws.flush().await;
                    break;
                }
                _ => {}
            }
        }
        // Keep the command owner alive through shutdown.
        drop(tx);
    });
    timeout(Duration::from_secs(4), server)
        .await
        .unwrap()
        .unwrap();
    let report = timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.sessions, 2);
    assert_eq!(report.messages, 4);
    let messages: Vec<_> = received.try_iter().map(|e| e.message).collect();
    assert!(matches!(messages.first(), Some(OutputMessage::Started(_))));
    assert!(matches!(messages.last(), Some(OutputMessage::Ended { .. })));
    let mut generation = 0;
    let mut sequence = 0;
    let mut ended = true;
    let mut requests = vec![];
    for message in messages {
        match message {
            OutputMessage::ReconcileBegin { .. }
            | OutputMessage::DesiredChunk { .. }
            | OutputMessage::ReconcileComplete { .. } => {
                panic!("worker cannot publish global desired fences")
            }
            OutputMessage::Started(s) => {
                assert!(ended);
                assert_eq!(s.generation, generation + 1);
                generation = s.generation;
                sequence = 0;
                ended = false;
            }
            OutputMessage::Data {
                session,
                ordinal,
                payload,
                ..
            } => {
                assert!(!ended);
                assert_eq!(session.generation, generation);
                assert_eq!(ordinal, sequence + 1);
                sequence = ordinal;
                assert!(!payload.is_empty());
            }
            OutputMessage::SubscriptionsRequested {
                session,
                add,
                remove,
                ..
            } => {
                assert!(!ended);
                assert_eq!(session.generation, generation);
                assert!(remove.is_empty());
                requests.push((generation, add));
            }
            OutputMessage::RequestsComplete { session } => {
                assert!(!ended);
                assert_eq!(session.generation, generation);
                assert_eq!(session.slot, 0);
            }
            OutputMessage::Ended { session, .. } => {
                assert_eq!(session.generation, generation);
                ended = true;
            }
        }
    }
    assert_eq!(
        requests,
        vec![
            (1, vec!["A".to_owned()]),
            (1, vec!["B".to_owned()]),
            (2, vec!["A".to_owned(), "B".to_owned()])
        ]
    );
}

#[tokio::test]
async fn full_output_fails_the_run_and_preserves_the_accepted_prefix() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (_tx, commands) = mpsc::channel(8);
    let (output, received) = output::channel(4, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV1,
        vec!["A".into()],
        commands,
        output,
        config,
        stop.clone(),
    ));
    let (socket, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(socket).await.unwrap();
    ws.next().await.unwrap().unwrap();
    ws.send(Message::Text(snapshot("A").into())).await.unwrap();
    ws.send(Message::Text("second message".into()))
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(*stop.borrow());
    let prefix: Vec<_> = received.try_iter().collect();
    assert_eq!(prefix.len(), 4);
    assert!(matches!(prefix[0].message, OutputMessage::Started(_)));
    assert!(matches!(
        prefix[1].message,
        OutputMessage::SubscriptionsRequested { .. }
    ));
    assert!(matches!(
        prefix[2].message,
        OutputMessage::RequestsComplete { .. }
    ));
    assert!(
        matches!(&prefix[3].message, OutputMessage::Data { ordinal: 1, payload, .. } if payload == &snapshot("A"))
    );
}

#[tokio::test]
async fn binary_and_oversized_data_fail_closed() {
    for invalid in [
        Message::Binary(vec![1].into()),
        Message::Text("x".repeat(513).into()),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(format!("ws://{}", listener.local_addr().unwrap()));
        Arc::make_mut(&mut config).max_message_bytes = 512;
        let (_tx, commands) = mpsc::channel(8);
        let (output, received) = output::channel(16, 1 << 20).unwrap();
        let (stop, _) = watch::channel(false);
        let worker = tokio::spawn(run_worker(
            0,
            AdapterId::PolymarketV1,
            vec!["A".into()],
            commands,
            output,
            config,
            stop.clone(),
        ));
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(invalid).await.unwrap();
        let peer = tokio::spawn(async move {
            while let Some(Ok(message)) = ws.next().await {
                match message {
                    Message::Text(t) if t == "PING" => {
                        let _ = ws.send(Message::Text("PONG".into())).await;
                    }
                    Message::Close(_) => {
                        let _ = ws.flush().await;
                        break;
                    }
                    _ => {}
                }
            }
        });
        assert!(
            timeout(Duration::from_secs(2), worker)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(*stop.borrow());
        assert!(
            !received
                .try_iter()
                .any(|e| matches!(e.message, OutputMessage::Data { .. }))
        );
        peer.abort();
        let _ = peer.await;
    }
}

#[tokio::test]
async fn shutdown_cancels_a_stalled_handshake_without_starting_a_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(format!("ws://{}", listener.local_addr().unwrap()));
    Arc::make_mut(&mut config).io_timeout = Duration::from_secs(30);
    let (_tx, commands) = mpsc::channel(8);
    let (output, received) = output::channel(16, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV1,
        vec!["A".into()],
        commands,
        output,
        config,
        stop.clone(),
    ));
    let (_socket, _) = listener.accept().await.unwrap();
    stop.send_replace(true);
    let report = timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.sessions, 0);
    assert!(received.try_iter().next().is_none());
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn transport_forwards_text_without_financial_decoding() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (_tx, commands) = mpsc::channel(8);
    let (output, received) = output::channel(32, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV1,
        vec!["A".into()],
        commands,
        output,
        config,
        stop.clone(),
    ));
    let (socket, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(socket).await.unwrap();
    ws.next().await.unwrap().unwrap();
    // A transport must not reject or rewrite downstream financial input.
    let payloads = [
        "not JSON: λ\n",
        r#"{"event_type":"price_change","price_changes":[{"price":"invalid"}]}"#,
    ];
    let before = chrono::Utc::now();
    let before_mono = std::time::Instant::now();
    for payload in payloads {
        ws.send(Message::Text(payload.into())).await.unwrap();
    }
    ws.send(Message::Ping(vec![42].into())).await.unwrap();
    let peer = tokio::spawn(async move {
        while let Some(Ok(message)) = ws.next().await {
            match message {
                Message::Text(t) if t == "PING" => {
                    let _ = ws.send(Message::Text("PONG".into())).await;
                }
                Message::Pong(bytes) if bytes.as_ref() == [42] => {
                    stop.send_replace(true);
                }
                Message::Close(_) => {
                    let _ = ws.flush().await;
                    break;
                }
                _ => {}
            }
        }
    });
    timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    peer.await.unwrap();
    let after = chrono::Utc::now();
    let after_mono = std::time::Instant::now();
    let data: Vec<_> = received
        .try_iter()
        .filter_map(|e| match e.message {
            OutputMessage::Data {
                payload,
                ordinal,
                received_at,
                received_mono,
                ..
            } => Some((payload, ordinal, received_at, received_mono)),
            _ => None,
        })
        .collect();
    assert_eq!(data.len(), 2);
    for (i, (payload, ordinal, received_at, received_mono)) in data.iter().enumerate() {
        assert_eq!(payload.as_bytes(), payloads[i].as_bytes());
        assert_eq!(*ordinal, i as u64 + 1);
        assert!(*received_at >= before && *received_at <= after);
        assert!(*received_mono >= before_mono && *received_mono <= after_mono);
    }
    assert!(data.windows(2).all(|pair| pair[0].3 <= pair[1].3));
}

#[test]
fn byte_budget_and_closed_consumer_are_distinct_failures() {
    let session = output::Session {
        worker: 0,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    };
    let (send, _receive) = output::channel(8, 1).unwrap();
    assert!(
        send.send(OutputMessage::Started(session))
            .unwrap_err()
            .to_string()
            .contains("byte budget")
    );
    let (send, receive) = output::channel(8, 4096).unwrap();
    drop(receive);
    assert!(
        send.send(OutputMessage::Started(session))
            .unwrap_err()
            .to_string()
            .contains("consumer closed")
    );
    // Raw payload bytes count even after dequeue, until processing releases the envelope.
    let (send, receive) = output::channel(8, 2048).unwrap();
    let frame = || OutputMessage::Data {
        session,
        ordinal: 1,
        received_at: chrono::Utc::now(),
        received_mono: std::time::Instant::now(),
        payload: "x".repeat(1500).into(),
    };
    send.send(frame()).unwrap();
    let processing = receive.try_recv().unwrap();
    assert!(
        send.send(frame())
            .unwrap_err()
            .to_string()
            .contains("byte budget")
    );
    drop(processing);
    send.send(frame()).unwrap();
    assert!(
        matches!(receive.try_recv().unwrap().message, OutputMessage::Data { payload, .. } if payload.len() == 1500)
    );
}

#[tokio::test]
async fn readmission_rotates_connection_and_forwards_raw_data_in_the_new_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (commands, receive) = mpsc::channel(8);
    let (output, received) = output::channel(32, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV1,
        vec!["A".into(), "B".into()],
        receive,
        output,
        config,
        stop.clone(),
    ));
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            format!("[{},{}]", snapshot("A"), snapshot("B")).into(),
        ))
        .await
        .unwrap();
        // Wait for the accepted message, not merely the server's send.
        timeout(Duration::from_secs(1), async {
            loop {
                if received
                    .try_iter()
                    .any(|e| matches!(e.message, OutputMessage::Data { .. }))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        commands
            .send(WorkerCommand {
                add: vec![],
                remove: vec!["A".into()],
                ..WorkerCommand::default()
            })
            .await
            .unwrap();
        loop {
            let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
            if text == "PING" {
                ws.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            assert_eq!(
                serde_json::from_str::<Value>(&text).unwrap()["operation"],
                "unsubscribe"
            );
            break;
        }
        commands
            .send(WorkerCommand {
                add: vec!["A".into()],
                remove: vec![],
                ..WorkerCommand::default()
            })
            .await
            .unwrap();
        while let Some(Ok(message)) = ws.next().await {
            match message {
                Message::Close(_) => {
                    let _ = ws.flush().await;
                    break;
                }
                Message::Text(t) if t == "PING" => {
                    let _ = ws.send(Message::Text("PONG".into())).await;
                }
                _ => panic!("reactivation reused old connection"),
            }
        }
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&request).unwrap()["assets_ids"],
            json!(["A", "B"])
        );
        ws.send(Message::Text(
            json!({"event_type":"price_change", "timestamp":"1793090300124", "price_changes":[
                {"asset_id":"A","side":"BUY","price":"0.2","size":"99"}
            ]})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Ping(vec![42].into())).await.unwrap();
        while let Some(Ok(message)) = ws.next().await {
            match message {
                Message::Pong(bytes) if bytes.as_ref() == [42] => {
                    server_stop.send_replace(true);
                }
                Message::Text(t) if t == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap();
                }
                Message::Close(_) => {
                    let _ = ws.flush().await;
                    break;
                }
                _ => {}
            }
        }
        drop(commands);
        received
    });
    let received = timeout(Duration::from_secs(4), server)
        .await
        .unwrap()
        .unwrap();
    let report = timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.sessions, 2);
    assert!(*stop.borrow());
    let tail: Vec<_> = received.try_iter().map(|e| e.message).collect();
    assert!(
        tail.iter()
            .any(|m| matches!(m, OutputMessage::Started(s) if s.generation == 2))
    );
    assert!(tail.iter().any(
        |m| matches!(m, OutputMessage::Data { session, ordinal: 1, .. } if session.generation == 2)
    ));
    assert!(
        matches!(tail.last(), Some(OutputMessage::Ended { session, .. }) if session.generation == 2)
    );
}

#[tokio::test]
async fn shutdown_completes_delivered_reconciliation_but_not_partial_delivery() {
    use engineering_challenge::{discovery::polymarket::Command, websocket::run_manager};
    for batch in [1, 2] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = Arc::new(Config {
            endpoint: format!("ws://{}", listener.local_addr().unwrap()),
            subscription_batch: batch,
            ..Config::default()
        });
        let (discovery, mut requests) = mpsc::channel(2);
        let (_notify, changes) = watch::channel(());
        let (output, received) = output::channel(16, 65536).unwrap();
        let (stop, _) = watch::channel(false);
        let manager = run_manager(
            discovery,
            changes,
            output,
            config,
            stop.clone(),
            mpsc::channel(1).1,
        );
        tokio::pin!(manager);
        let request = tokio::select! {
            result = &mut manager => panic!("manager exited before discovery: {result:?}"),
            request = requests.recv() => request.unwrap(),
        };
        let Command::Snapshot(reply) = request else {
            panic!("expected discovery snapshot request");
        };
        let mut state = Snapshot::default();
        market(&mut state, "M", AdapterId::PolymarketV1, &["A", "B"]);
        reply.send(Arc::new(state)).unwrap();
        // Stop at the existing fairness yield after the first delivered chunk.
        // With batch=2 this is all work; with batch=1 another chunk is owed.
        assert!(futures_util::poll!(&mut manager).is_pending());
        let before: Vec<_> = received.try_iter().map(|e| e.message).collect();
        assert!(
            matches!(before.as_slice(), [OutputMessage::ReconcileBegin { chunks, .. }, OutputMessage::DesiredChunk { index: 0, .. }] if *chunks == (2 / batch) as u32)
        );
        stop.send_replace(true);
        let result = timeout(Duration::from_secs(1), manager).await.unwrap();
        let tail: Vec<_> = received.try_iter().map(|e| e.message).collect();
        if batch == 2 {
            assert!(result.unwrap().iter().all(|r| r.connection_attempts == 0));
            assert!(matches!(
                tail.as_slice(),
                [OutputMessage::ReconcileComplete { .. }]
            ));
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("shutdown before chunk 1")
            );
            assert!(tail.is_empty(), "must not certify an undelivered chunk");
        }
    }
}

#[tokio::test]
async fn manager_repairs_a_missed_notification_without_migrating_existing_assets() {
    use engineering_challenge::{discovery::polymarket::Command, websocket::run_manager};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (discovery, mut requests) = mpsc::channel(8);
    let (_notify, changes) = watch::channel(());
    let (output, received) = output::channel(64, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let owner = tokio::spawn(async move {
        let mut state = Snapshot::default();
        market(&mut state, "M", AdapterId::PolymarketV1, &["A", "B"]);
        let mut first = true;
        while let Some(Command::Snapshot(reply)) = requests.recv().await {
            if !first {
                market(&mut state, "N", AdapterId::PolymarketV1, &["C", "D"]);
            }
            first = false;
            let _ = reply.send(Arc::new(state.clone()));
        }
    });
    let manager = tokio::spawn(run_manager(
        discovery,
        changes,
        output,
        config,
        stop.clone(),
        mpsc::channel(1).1,
    ));
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let initial = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&initial).unwrap()["assets_ids"],
            json!(["A", "B"])
        );
        ws.send(Message::Text(
            format!("[{},{}]", snapshot("A"), snapshot("B")).into(),
        ))
        .await
        .unwrap();
        loop {
            let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
            if text == "PING" {
                ws.send(Message::Text("PONG".into())).await.unwrap();
                continue;
            }
            let request: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(request["operation"], "subscribe");
            assert_eq!(request["assets_ids"], json!(["C", "D"]));
            break;
        }
        stop.send_replace(true);
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                let _ = ws.flush().await;
                break;
            }
        }
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    });
    timeout(Duration::from_secs(8), server)
        .await
        .unwrap()
        .unwrap();
    let reports = timeout(Duration::from_secs(2), manager)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].sessions, 1);
    assert!(
        received
            .try_iter()
            .any(|e| matches!(e.message, OutputMessage::Data { .. }))
    );
    owner.await.unwrap();
}

#[tokio::test]
async fn manager_retirement_precedes_unsubscribe_and_excludes_late_book_updates() {
    use engineering_challenge::{
        discovery::polymarket::Command,
        orderbook::{self, ObservationKind, Processor, Validity},
        websocket::run_manager,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (discovery, mut requests) = mpsc::channel(8);
    let (notify, changes) = watch::channel(());
    let (retire, retiring) = watch::channel(false);
    let (output, received) = output::channel(64, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let owner = tokio::spawn(async move {
        while let Some(Command::Snapshot(reply)) = requests.recv().await {
            let mut state = Snapshot::default();
            market(&mut state, "N", AdapterId::PolymarketV1, &["B"]);
            if !*retiring.borrow() {
                market(&mut state, "M", AdapterId::PolymarketV1, &["A"]);
            }
            let _ = reply.send(Arc::new(state));
        }
    });
    let manager = tokio::spawn(run_manager(
        discovery,
        changes,
        output,
        config,
        stop.clone(),
        mpsc::channel(1).1,
    ));
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let initial = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&initial).unwrap()["assets_ids"],
            json!(["A", "B"])
        );
        ws.send(Message::Text(
            format!("[{},{}]", snapshot("A"), snapshot("B")).into(),
        ))
        .await
        .unwrap();
        ws.send(Message::Text(
            json!({"event_type":"price_change","timestamp":"42","price_changes":[
                {"asset_id":"A","side":"BUY","price":"0.2","size":"21"}
            ]})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        while let Some(Ok(message)) = ws.next().await {
            match message {
                Message::Text(t) if t == "PING" => {
                    ws.send(Message::Text("PONG".into())).await.unwrap()
                }
                Message::Text(t) => {
                    let request: Value = serde_json::from_str(&t).unwrap();
                    assert_eq!(request["operation"], "unsubscribe");
                    assert_eq!(request["assets_ids"], json!(["A"]));
                    // Valid delayed data is still possible after a request send;
                    // receipt of unsubscribe is not an exchange acknowledgement.
                    ws.send(Message::Text(
                        json!({"event_type":"price_change","timestamp":"41","price_changes":[
                            {"asset_id":"A","side":"BUY","price":"0.2","size":"99"},
                            {"asset_id":"B","side":"BUY","price":"0.2","size":"23"}
                        ]})
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
                }
                Message::Close(_) => {
                    let _ = ws.flush().await;
                    break;
                }
                _ => {}
            }
        }
    });
    let mut p = Processor::new(orderbook::Config::default()).unwrap();
    let mut levels = vec![];
    let mut retired = false;
    let mut unsubscribed = false;
    timeout(Duration::from_secs(4), async {
        loop {
            while let Ok(envelope) = received.try_recv() {
                match &envelope.message {
                    OutputMessage::DesiredChunk { remove, .. } if !remove.is_empty() => {
                        assert_eq!(remove, &["A"]);
                        retired = true;
                    }
                    OutputMessage::SubscriptionsRequested { remove, .. } if !remove.is_empty() => {
                        assert!(
                            retired,
                            "worker unsubscribe notification overtook manager retirement"
                        );
                        assert_eq!(remove, &["A"]);
                        unsubscribed = true;
                    }
                    _ => {}
                }
                p.process(&envelope.message, Duration::ZERO, &mut |o| {
                    if let ObservationKind::Level { quantity, .. } = o.kind {
                        levels.push((o.asset.to_owned(), quantity.value()));
                    }
                    Ok(())
                })
                .unwrap();
                if matches!(envelope.message, OutputMessage::Data { ordinal: 2, .. }) {
                    assert_eq!(levels, vec![("A".to_owned(), 21_000_000)]);
                    retire.send_replace(true);
                    notify.send_replace(());
                }
                if matches!(envelope.message, OutputMessage::Data { ordinal: 3, .. }) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(retired && unsubscribed);
    assert_eq!(
        levels,
        vec![("A".to_owned(), 21_000_000), ("B".to_owned(), 23_000_000)]
    );
    assert_eq!(
        p.replica(AdapterId::PolymarketV1, "A", 0).unwrap().state,
        Validity::Inactive
    );
    assert_eq!(
        p.replica(AdapterId::PolymarketV1, "B", 0).unwrap().state,
        Validity::Live
    );
    assert_eq!(p.stats().rejected, 0);
    assert!(p.take_recovery().is_empty());
    stop.send_replace(true);
    let reports = timeout(Duration::from_secs(2), manager)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reports[0].sessions, 1);
    for envelope in received.try_iter() {
        p.process(&envelope.message, Duration::ZERO, &mut |_| Ok(()))
            .unwrap();
    }
    p.finish().unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    owner.await.unwrap();
}

#[tokio::test]
async fn processor_recovery_rotates_only_the_target_generation() {
    use engineering_challenge::{
        discovery::polymarket::Command,
        orderbook::{self, Processor},
        websocket::run_manager,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("ws://{}", listener.local_addr().unwrap()));
    let (discovery, mut requests) = mpsc::channel(8);
    let (_notify, changes) = watch::channel(());
    let (output, received) = output::channel(64, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let (recovery, recoveries) = mpsc::channel(8);
    let owner = tokio::spawn(async move {
        let mut state = Snapshot::default();
        market(&mut state, "M", AdapterId::PolymarketV1, &["A", "B"]);
        while let Some(Command::Snapshot(reply)) = requests.recv().await {
            let _ = reply.send(Arc::new(state.clone()));
        }
    });
    let manager = tokio::spawn(run_manager(
        discovery,
        changes,
        output,
        config,
        stop.clone(),
        recoveries,
    ));
    let server = tokio::spawn(async move {
        for generation in 1..=2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&request).unwrap()["assets_ids"],
                json!(["A", "B"])
            );
            let text = if generation == 1 {
                r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"0.3","size":"bad"}],"asks":[]}"#.to_owned()
            } else {
                format!("[{},{}]", snapshot("A"), snapshot("B"))
            };
            ws.send(Message::Text(text.into())).await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                match message {
                    Message::Text(t) if t == "PING" => {
                        ws.send(Message::Text("PONG".into())).await.unwrap()
                    }
                    Message::Close(_) => {
                        let _ = ws.flush().await;
                        break;
                    }
                    _ => {}
                }
            }
        }
    });
    let start = std::time::Instant::now();
    let mut processor = Processor::new(orderbook::Config::default()).unwrap();
    let mut obsolete_sent = false;
    let mut recovered_at = None;
    timeout(Duration::from_secs(5), async {
        loop {
            while let Ok(envelope) = received.try_recv() {
                processor
                    .process(&envelope.message, start.elapsed(), &mut |_| Ok(()))
                    .unwrap();
                for session in processor.take_recovery() {
                    recovery
                        .try_send(RecoveryRequest::Restart {
                            session,
                            planned: false,
                        })
                        .unwrap();
                }
                if processor.stats().snapshots == 2 && !obsolete_sent {
                    recovery
                        .try_send(RecoveryRequest::Restart {
                            session: output::Session {
                                worker: 0,
                                generation: 1,
                                adapter: AdapterId::PolymarketV1,
                                slot: 0,
                            },
                            planned: false,
                        })
                        .unwrap();
                    obsolete_sent = true;
                    recovered_at = Some(std::time::Instant::now());
                }
            }
            if recovered_at.is_some_and(|at| at.elapsed() > Duration::from_millis(100)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    stop.send_replace(true);
    let reports = timeout(Duration::from_secs(2), manager)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reports[0].sessions, 2);
    server.await.unwrap();
    for envelope in received {
        processor
            .process(&envelope.message, start.elapsed(), &mut |_| Ok(()))
            .unwrap();
    }
    processor.finish().unwrap();
    assert_eq!(processor.stats().rejected, 1);
    assert_eq!(processor.stats().snapshots, 2);
    assert_eq!(
        processor
            .replica(AdapterId::PolymarketV1, "A", 0)
            .unwrap()
            .binding
            .revision,
        1
    );
    owner.abort();
}

#[tokio::test]
async fn heartbeat_failure_ends_the_session_and_shutdown_interrupts_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(format!("ws://{}", listener.local_addr().unwrap()));
    Arc::make_mut(&mut config).pong_timeout = Duration::from_millis(90);
    Arc::make_mut(&mut config).reconnect_min = Duration::from_secs(30);
    Arc::make_mut(&mut config).reconnect_max = Duration::from_secs(30);
    let (_commands, receive) = mpsc::channel(8);
    let (output, received) = output::channel(32, 1 << 20).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = tokio::spawn(run_worker(
        0,
        AdapterId::PolymarketV1,
        vec!["A".into()],
        receive,
        output,
        config,
        stop.clone(),
    ));
    let (socket, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(socket).await.unwrap();
    ws.next().await.unwrap().unwrap();
    ws.send(Message::Text(snapshot("A").into())).await.unwrap();
    timeout(Duration::from_secs(2), async {
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) { ws.flush().await.unwrap(); break; }
            // Deliberately withhold the application PONG.
        }
        loop {
            if received.try_iter().any(|e| matches!(e.message, OutputMessage::Ended { reason, .. } if reason == "application heartbeat timeout")) { break; }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }).await.unwrap();
    stop.send_replace(true);
    let report = timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.sessions, 1);
    assert!(
        timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unsubscribe_batches_allow_incoming_messages_to_progress() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(format!("ws://{}", listener.local_addr().unwrap()));
        let settings = Arc::make_mut(&mut config);
        settings.subscription_batch = 1;
        settings.heartbeat = Duration::from_secs(30);
        settings.pong_timeout = Duration::from_secs(60);
        let assets: Vec<_> = (0..512).map(|i| format!("{i:077}")).collect();
        let (tx, commands) = mpsc::channel(8);
        let (output, received) = output::channel(2048, 2 << 20).unwrap();
        let (stop, _) = watch::channel(false);
        let worker = tokio::spawn(run_worker(
            0, AdapterId::PolymarketV1, assets.clone(), commands,
            output, config, stop.clone(),
        ));
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        for _ in &assets {
            ws.next().await.unwrap().unwrap();
        }
        tx.send(WorkerCommand { add: vec![], remove: assets[..511].to_vec(), ..WorkerCommand::default() }).await.unwrap();
        let mut wire_removals = vec![];
        let mut sent_at = None;
        for i in 0..511 {
            let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
            let request: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(request["operation"], "unsubscribe");
            wire_removals.push(request["assets_ids"][0].as_str().unwrap().to_owned());
            if i == 0 {
                sent_at = Some(std::time::Instant::now());
                for text in ["during-unsubscribe-1", "during-unsubscribe-2", "during-unsubscribe-3"] {
                    ws.send(Message::Text(text.into())).await.unwrap();
                }
                // Repeated intent while removals are pending must not duplicate
                // requests or consume the pending-work budget twice.
                tx.send(WorkerCommand { add: vec![], remove: assets[..511].to_vec(), ..WorkerCommand::default() }).await.unwrap();
            }
        }
        assert_eq!(wire_removals, assets[..511]);
        // Wait for the complete accepted prefix before establishing the cutoff.
        let mut removals = 0;
        let mut data = 0;
        while removals < 511 || data < 3 {
            while let Ok(envelope) = received.try_recv() {
                match envelope.message {
                    OutputMessage::SubscriptionsRequested { remove, .. } => removals += remove.len(),
                    OutputMessage::Data { session, ordinal, payload, .. } => {
                        data += 1;
                        assert_eq!(session.generation, 1);
                        assert_eq!(ordinal, data);
                        assert_eq!(payload.as_str(), format!("during-unsubscribe-{data}"));
                        // A whole command must not monopolize reception. Allow ample
                        // scheduling slack; this is not a hard real-time guarantee.
                        assert!(removals < 64, "received only after {removals} unsubscribe batches");
                        eprintln!("unsubscribe receive progress: batches={removals} peer-send-to-enqueue={:?}", envelope.queued_at.duration_since(sent_at.unwrap()));
                    }
                    _ => {}
                }
            }
            tokio::task::yield_now().await;
        }
        tx.send(WorkerCommand { add: vec![], remove: vec![assets[511].clone()], ..WorkerCommand::default() }).await.unwrap();
        let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let request: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(request["operation"], "unsubscribe");
        assert_eq!(request["assets_ids"], json!([assets[511]]));
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                let _ = ws.flush().await;
                break;
            }
        }
        stop.send_replace(true);
        let report = worker.await.unwrap().unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.messages, 3);
        let tail: Vec<_> = received.try_iter().filter_map(|e| match e.message {
            OutputMessage::RequestsComplete { session } => {
                assert_eq!(session.generation, 1);
                None
            }
            message => Some(message),
        }).collect();
        assert!(matches!(&tail[..], [OutputMessage::SubscriptionsRequested { remove, .. }, OutputMessage::Ended { reason, .. }]
            if remove == &vec![assets[511].clone()] && reason == "no assignments"));
    }).await.unwrap();
}

#[tokio::test]
async fn stalled_unsubscribe_writes_end_on_timeout_or_shutdown() {
    for shutdown in [false, true] {
        timeout(Duration::from_secs(15), async {
            // Bound the peer's TCP receive window, then stop reading requests.
            // The unusually large synthetic assignment fills the OS send buffers;
            // it is fault injection, not a proposed production socket budget.
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(16 << 10).unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = socket.listen(1).unwrap();
            let mut config = config(format!("ws://{}", listener.local_addr().unwrap()));
            let settings = Arc::make_mut(&mut config);
            settings.assets_per_socket = 32768;
            settings.subscription_batch = 512;
            settings.io_timeout = Duration::from_millis(if shutdown { 5000 } else { 600 });
            settings.heartbeat = Duration::from_secs(30);
            settings.pong_timeout = Duration::from_secs(60);
            settings.reconnect_min = Duration::from_secs(30);
            settings.reconnect_max = Duration::from_secs(30);
            let assets: Vec<_> = (0..32768).map(|i| format!("{i:077}")).collect();
            let (tx, commands) = mpsc::channel(8);
            let (output, received) = output::channel(1024, 16 << 20).unwrap();
            let (stop, _) = watch::channel(false);
            let worker = tokio::spawn(run_worker(
                0, AdapterId::PolymarketV1, assets.clone(), commands,
                output, config, stop.clone(),
            ));
            let (socket, _) = listener.accept().await.unwrap();
            socket.set_nodelay(true).unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            let mut subscribed = 0;
            while subscribed < assets.len() {
                let text = ws.next().await.unwrap().unwrap().into_text().unwrap();
                let request: Value = serde_json::from_str(&text).unwrap();
                subscribed += request["assets_ids"].as_array().unwrap().len();
            }
            received.try_iter().for_each(drop);
            tx.send(WorkerCommand { add: vec![], remove: assets[..32767].to_vec(), ..WorkerCommand::default() }).await.unwrap();
            let started = std::time::Instant::now();
            let mut last_progress = started;
            let mut cutoff = None;
            let mut sent = 0;
            let mut accepted = 0;
            let mut removed = 0;
            let mut peer_writable = true;
            let reason = 'peer: loop {
                // No ws.next(): client writes must eventually block while this
                // direction stays writable and produces ordered numbered messages.
                if cutoff.is_none() && peer_writable {
                    // The worker may close just before we observe its Ended event.
                    if ws.send(Message::Text(format!("peer-{sent}").into())).await.is_ok() {
                        sent += 1;
                    } else {
                        peer_writable = false;
                    }
                }
                while let Ok(envelope) = received.try_recv() {
                    match envelope.message {
                        OutputMessage::Data { session, ordinal, payload, .. } => {
                            assert_eq!(session.generation, 1);
                            assert_eq!(ordinal, accepted + 1);
                            assert_eq!(payload.as_str(), format!("peer-{accepted}"));
                            accepted += 1;
                            last_progress = std::time::Instant::now();
                        }
                        OutputMessage::SubscriptionsRequested { remove, .. } => {
                            removed += remove.len();
                            last_progress = std::time::Instant::now();
                        }
                        OutputMessage::RequestsComplete { session } => {
                            assert_eq!(session.generation, 1);
                        }
                        OutputMessage::Ended { session, reason } => {
                            assert_eq!(session.generation, 1);
                            break 'peer reason;
                        }
                        _ => panic!("unexpected session restart"),
                    }
                }
                if shutdown && cutoff.is_none() && last_progress.elapsed() > Duration::from_millis(100) {
                    cutoff = Some(std::time::Instant::now());
                    stop.send_replace(true);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert!(removed < 32767, "test failed to create backpressure");
            assert!(sent > accepted, "test failed to expose unread incoming traffic");
            if shutdown {
                // Quiet TCP progress does not prove a send is pending: shutdown
                // may land between batches. The io unit tests force a pending send.
                assert!(matches!(reason.as_str(), "shutdown" | "unsubscribe uncertain: shutdown"), "{reason}");
                assert!(cutoff.unwrap().elapsed() < Duration::from_secs(2));
            } else {
                assert!(reason.contains("unsubscribe uncertain"), "{reason}");
                assert!(reason.contains("socket write timeout"), "{reason}");
                assert!(started.elapsed() < Duration::from_secs(3));
                stop.send_replace(true);
            }
            let report = worker.await.unwrap().unwrap();
            assert_eq!(report.sessions, 1);
            assert_eq!(report.messages, accepted);
            assert!(received.is_empty());
            eprintln!("stalled unsubscribe: shutdown={shutdown} removed={removed} sent={sent} accepted={accepted} elapsed={:?} reason={reason}", started.elapsed());
        }).await.unwrap();
    }
}
