//! Exercise discovery, transport and optional books through local sockets and shutdown.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{Instant, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[test]
fn coverage_compares_exact_identities_at_owner_cutoffs_not_equal_totals() {
    use engineering_challenge::{
        discovery::{AdapterId, Instrument, MarketEntry, MarketKey, Snapshot},
        orderbook::{self, Processor, Validity},
        websocket::{
            Assignments,
            ingress::{Message as Input, Session},
        },
    };
    use std::collections::BTreeSet;
    let adapter = AdapterId::PolymarketV1;
    let mut desired = Snapshot::default();
    for id in ["A", "B", "C"] {
        desired.markets.insert(
            MarketKey {
                adapter,
                id: id.into(),
            },
            MarketEntry {
                instruments: vec![Instrument {
                    id: id.into(),
                    label: id.into(),
                }],
                desired: true,
                uncertainty: None,
            },
        );
    }
    let mut assignments = Assignments::new(2, 2);
    assignments.reconcile(&desired).unwrap();
    let session = Session {
        adapter,
        worker: 0,
        generation: 1,
        slot: 0,
    };
    let mut p = Processor::new(orderbook::Config::default()).unwrap();
    for message in [
        Input::Started(session),
        Input::SubscriptionsRequested { session, revisions: vec![1; 3],
            add: vec!["A".into(), "B".into(), "X".into()], remove: vec![] },
        Input::Data { session, ordinal: 1, received_at: chrono::Utc::now(), received_mono: std::time::Instant::now(),
            payload: r#"[{"event_type":"book","asset_id":"A","timestamp":"1","bids":[],"asks":[]},{"event_type":"book","asset_id":"X","timestamp":"1","bids":[],"asks":[]}]"#.into() },
    ] {
        p.process(&message, Duration::ZERO, &mut |_| Ok(())).unwrap();
    }
    let identities = |s: &Snapshot| -> BTreeSet<_> {
        s.markets
            .iter()
            .filter(|(_, m)| m.desired)
            .flat_map(|(k, m)| m.instruments.iter().map(|i| (k.adapter, i.id.clone())))
            .collect()
    };
    let before = identities(&desired);
    let requested: BTreeSet<_> = p
        .requested()
        .map(|(asset, binding)| (binding.session.adapter, asset.to_owned()))
        .collect();
    assert_eq!(before.len(), requested.len()); // A counts-only check would pass.
    assert_eq!(
        before.difference(&requested).cloned().collect::<Vec<_>>(),
        vec![(adapter, "C".into())]
    );
    assert_eq!(
        requested.difference(&before).cloned().collect::<Vec<_>>(),
        vec![(adapter, "X".into())]
    );
    let rows: Vec<_> = p.coverage().collect();
    assert!(
        rows.iter()
            .all(|r| r.input_cutoff == 3 && r.owner_time == Duration::ZERO)
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.selected && r.state == Validity::Live)
            .map(|r| r.asset)
            .collect::<Vec<_>>(),
        vec!["A", "X"]
    );
    assert!(
        !rows
            .iter()
            .find(|r| r.asset == "B")
            .unwrap()
            .initialization_overdue
    );
    for (ordinal, second, overdue) in [(2, 29, false), (3, 30, true)] {
        p.process(
            &Input::Data {
                session,
                ordinal,
                received_at: chrono::Utc::now(),
                received_mono: std::time::Instant::now(),
                payload: "[]".into(),
            },
            Duration::from_secs(second),
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(
            p.coverage()
                .find(|r| r.asset == "B")
                .unwrap()
                .initialization_overdue,
            overdue
        );
    }

    desired
        .markets
        .get_mut(&MarketKey {
            adapter,
            id: "B".into(),
        })
        .unwrap()
        .desired = false;
    let after = identities(&desired);
    // Bracket changes are unresolved sampling intervals, not atomic cross-owner state.
    assert_eq!(
        before
            .symmetric_difference(&after)
            .cloned()
            .collect::<Vec<_>>(),
        vec![(adapter, "B".into())]
    );
    let changes = assignments.reconcile(&desired).unwrap();
    let removal = changes.iter().find(|c| c.remove == ["B"]).unwrap();
    for message in [
        Input::ReconcileBegin {
            revision: removal.revision,
            chunks: 1,
        },
        Input::DesiredChunk {
            adapter,
            revision: removal.revision,
            index: 0,
            add: vec![],
            remove: removal.remove.clone(),
        },
        Input::ReconcileComplete {
            revision: removal.revision,
        },
    ] {
        p.process(&message, Duration::from_secs(31), &mut |_| Ok(()))
            .unwrap();
    }
    let assigned: BTreeSet<_> = assignments
        .current()
        .flat_map(|a| {
            a.assets
                .iter()
                .map(move |id| (a.market.adapter, id.clone()))
        })
        .collect();
    assert_eq!(assigned, after);
    assert!(assignments.current().all(|a| a.revision == 1)); // Unchanged assignments keep their admission revision.
    let retired = p.coverage().find(|r| r.asset == "B").unwrap();
    assert_eq!(retired.state, Validity::Inactive);
    assert!(!retired.selected && !retired.initialization_overdue);
    assert_eq!(retired.input_cutoff, 8);
    assert_eq!(retired.owner_time, Duration::from_secs(31));
    assert!(!p.requested().any(|(id, _)| id == "B"));
    assert!(!after.contains(&(adapter, "B".into())));
    let live: BTreeSet<_> = p
        .coverage()
        .filter(|r| r.state == Validity::Live)
        .map(|r| (r.binding.session.adapter, r.asset.to_owned()))
        .collect();
    assert_eq!(
        after.difference(&live).cloned().collect::<Vec<_>>(),
        vec![(adapter, "C".into())]
    );
    assert_eq!(
        live.difference(&after).cloned().collect::<Vec<_>>(),
        vec![(adapter, "X".into())]
    );
    p.process(
        &Input::Ended {
            session,
            reason: "cutoff".into(),
        },
        Duration::from_secs(32),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(p.requested().count(), 0);
    assert!(!p.coverage().any(|r| r.state == Validity::Live));
}

#[test]
fn recording_rejects_missing_existing_and_uncreatable_integrity_paths() {
    let directory = tempfile::tempdir().unwrap();
    let existing = directory.path().join("existing.jsonl");
    std::fs::write(&existing, b"preserve existing history\n").unwrap();
    for path in [
        None,
        Some(existing.clone()),
        Some(directory.path().join("absent/sidecar.jsonl")),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"));
        command.args([
            "--seconds",
            "1",
            "--gamma-url",
            "http://127.0.0.1:9/markets",
            "--ws-url",
            "ws://127.0.0.1:9",
        ]);
        if let Some(path) = path {
            command.arg("--integrity-path").arg(path);
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("integrity")
        );
    }
    assert_eq!(
        std::fs::read(&existing).unwrap(),
        b"preserve existing history\n"
    );
}

#[tokio::test]
async fn collector_records_or_validates_and_bounds_stalled_output_shutdown() {
    for mode in [
        "deadline",
        "sigint",
        "books",
        "books-sigint",
        "books-dual",
        "books-dual-sigint",
        "books-blocked",
        "books-broken",
    ] {
        let books = mode.starts_with("books");
        let dual = mode.contains("dual");
        let sockets = if dual { 4 } else { 2 };
        let interrupt = mode.ends_with("sigint");
        let blocked = mode.ends_with("blocked");
        let broken = mode.ends_with("broken");
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gamma_url = format!("http://{}/markets/keyset", http.local_addr().unwrap());
        let ws_url = format!("ws://{}", ws.local_addr().unwrap());
        let http_task = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = http.accept().await.unwrap();
                let mut request = vec![];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut buf = [0; 2048];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert_ne!(n, 0);
                    request.extend_from_slice(&buf[..n]);
                }
                let request = String::from_utf8(request).unwrap();
                let version = if request.contains("version=v1") {
                    "v1"
                } else {
                    assert!(request.contains("version=v2"));
                    "v2"
                };
                // Overlapping exact IDs across adapters still require two sockets.
                let body = json!({"markets":[{
                    "id":"123", "version":version, "active":true, "closed":false,
                    "archived":false, "enableOrderBook":true, "acceptingOrders":true,
                    "outcomes":"[\"Yes\",\"No\"]", "clobTokenIds":"[\"00123\",\"00456\"]",
                    "positionIds":["00123","00456"]
                }]})
                .to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            }
        });
        let received_prefixes = Arc::new(AtomicUsize::new(0));
        let server_received = received_prefixes.clone();
        let ws_task = tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            for _ in 0..sockets {
                let (socket, _) = ws.accept().await.unwrap();
                let received = server_received.clone();
                peers.spawn(async move {
                    let mut socket = accept_async(socket).await.unwrap();
                    let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
                    let request: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(request["assets_ids"], json!(["00123", "00456"]));
                    if books {
                        socket.send(Message::Text(json!([
                            {"event_type":"book","asset_id":"00123","timestamp":"42","bids":[{"price":"0.2","size":"3"}],"asks":[]},
                            {"event_type":"book","asset_id":"00456","timestamp":"41","bids":[],"asks":[]}
                        ]).to_string().into())).await.unwrap();
                        socket.send(Message::Text(json!({"event_type":"price_change","timestamp":"42","price_changes":[
                            {"asset_id":"00123","side":"BUY","price":"0.2","size":"5"},
                            {"asset_id":"00123","side":"BUY","price":"0.2","size":"0"}
                        ]}).to_string().into())).await.unwrap();
                    } else {
                        // Transport-only coverage must continue to accept opaque text.
                        socket.send(Message::Text("opaque raw message".into())).await.unwrap();
                        socket.send(Message::Text("another message λ".into())).await.unwrap();
                    }
                    socket.send(Message::Ping(vec![42].into())).await.unwrap();
                    while let Some(Ok(message)) = socket.next().await {
                        match message {
                            Message::Text(text) if text == "PING" => {
                                socket.send(Message::Text("PONG".into())).await.unwrap();
                            }
                            Message::Pong(bytes) if bytes.as_ref() == [42] => {
                                received.fetch_add(1, Ordering::SeqCst);
                            }
                            Message::Close(_) => {
                                let _ = socket.flush().await;
                                break;
                            }
                            _ => {}
                        }
                    }
                });
            }
            while let Some(result) = peers.join_next().await {
                result.unwrap();
            }
        });
        let directory = tempfile::tempdir().unwrap();
        let archive = directory.path().join("archive.sbe");
        let integrity = directory.path().join("integrity.jsonl");
        let mut command = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"));
        command
            .args([
                "--gamma-url",
                &gamma_url,
                "--ws-url",
                &ws_url,
                "--assets-per-socket",
                "2",
                "--transport-only",
                if books { "false" } else { "true" },
                "--replicas-per-market",
                if dual { "2" } else { "1" },
                "--monitoring",
                if dual { "true" } else { "false" },
                "--seconds",
                if interrupt {
                    "30"
                } else if dual {
                    "5"
                } else {
                    "3"
                },
            ])
            .stdout(if books && !blocked && !broken {
                Stdio::from(std::fs::File::create(&archive).unwrap())
            } else {
                Stdio::piped()
            })
            .stderr(Stdio::piped());
        if books {
            command.arg("--integrity-path").arg(&integrity);
        }
        let mut child = command.spawn().unwrap();
        if broken {
            drop(child.stdout.take());
        }
        let started = Instant::now();
        let mut interrupted = false;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if interrupt && !interrupted && received_prefixes.load(Ordering::SeqCst) == sockets {
                assert!(
                    Command::new("kill")
                        .args(["-INT", &child.id().to_string()])
                        .status()
                        .unwrap()
                        .success()
                );
                interrupted = true;
            }
            if started.elapsed() > Duration::from_secs(23) {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "transport shutdown timed out: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let output = child.wait_with_output().unwrap();
        let diagnostics = String::from_utf8(output.stderr).unwrap();
        if blocked || broken {
            assert!(!status.success(), "{mode}: stalled/broken output succeeded");
            assert!(started.elapsed() < Duration::from_secs(23));
            let lines = std::fs::read_to_string(&integrity).unwrap();
            assert!(!lines.lines().any(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                v["kind"] == "run_end" && v["success"] == true
            }));
            http_task.abort();
            ws_task.abort();
            continue;
        }
        timeout(Duration::from_secs(2), http_task)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), ws_task)
            .await
            .unwrap()
            .unwrap();
        assert!(status.success(), "{mode}: {diagnostics}");
        assert!(!interrupt || interrupted);
        assert!(output.stdout.is_empty());
        if books {
            use engineering_challenge::util::{
                AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::*,
            };
            let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&archive).unwrap();
            let mut traces = std::collections::BTreeMap::<String, Vec<i64>>::new();
            while let Some(record) = reader.read_msg().unwrap() {
                match record {
                    AnyMessage::Snapshot(s) => {
                        let s = SnapshotDomain::try_from_decoder(s).unwrap();
                        traces
                            .entry(s.symbol)
                            .or_default()
                            .push(s.bids.first().map_or(-1, |l| l.quantity));
                    }
                    AnyMessage::PriceLevelUpdate(l) => {
                        let l = PriceLevelUpdateDomain::try_from_decoder(l).unwrap();
                        assert_eq!(l.price_level, 200_000);
                        traces.entry(l.symbol).or_default().push(l.quantity);
                    }
                    _ => panic!("unexpected SBE template"),
                }
            }
            assert_eq!(traces.len(), 4);
            for version in ["v1", "v2"] {
                assert_eq!(
                    traces[&format!("polymarket:{version}:00123")],
                    [3_000_000, 5_000_000, 0]
                );
                assert_eq!(traces[&format!("polymarket:{version}:00456")], [-1]);
            }
            let text = std::fs::read_to_string(&integrity).unwrap();
            let last: Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
            assert_eq!(last["kind"], "run_end");
            assert_eq!(last["success"], true);
            assert_eq!(last["output"]["records_confirmed"], 8);
            assert!(
                diagnostics.contains(if dual {
                    "snapshots: 8, changes: 8, selected_changes: 4, rejected: 0"
                } else {
                    "snapshots: 4, changes: 4, selected_changes: 4, rejected: 0"
                }),
                "{diagnostics}"
            );
            if dual {
                assert_eq!(
                    text.lines()
                        .map(|line| serde_json::from_str::<Value>(line).unwrap())
                        .filter(|row| row["kind"] == "standby_baseline")
                        .count(),
                    4
                );
            }
        } else {
            assert!(
                diagnostics.contains("transport validation only"),
                "{diagnostics}"
            );
        }
        assert!(
            diagnostics.contains(&format!("requested_at_cutoff={}", sockets * 2)),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(&format!("drained_messages={}", sockets * 2)),
            "{diagnostics}"
        );
    }
}
