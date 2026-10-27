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
        "books-blocked",
        "books-broken",
    ] {
        let books = mode.starts_with("books");
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
            for _ in 0..2 {
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
                "--seconds",
                if interrupt { "30" } else { "3" },
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
            if interrupt && !interrupted && received_prefixes.load(Ordering::SeqCst) == 2 {
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
                diagnostics.contains("snapshots: 4, changes: 4, selected_changes: 4, rejected: 0"),
                "{diagnostics}"
            );
        } else {
            assert!(
                diagnostics.contains("transport validation only"),
                "{diagnostics}"
            );
        }
        assert!(
            diagnostics.contains("requested_at_cutoff=4"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("drained_messages=4"), "{diagnostics}");
    }
}
