//! Exercise Components 1/2 through real HTTP/WS sockets and process shutdown.
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

#[tokio::test]
async fn transport_runner_drains_on_deadline_and_sigint_without_writing_stdout() {
    for mode in ["deadline", "sigint"] {
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
                    // No book initialization required: these must be handed off as-is.
                    socket
                        .send(Message::Text("opaque raw message".into()))
                        .await
                        .unwrap();
                    socket
                        .send(Message::Text("another message λ".into()))
                        .await
                        .unwrap();
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
        let mut child = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
            .args([
                "--gamma-url",
                &gamma_url,
                "--ws-url",
                &ws_url,
                "--assets-per-socket",
                "2",
                "--seconds",
                if mode == "sigint" { "30" } else { "3" },
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let started = Instant::now();
        let mut interrupted = false;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if mode == "sigint" && !interrupted && received_prefixes.load(Ordering::SeqCst) == 2 {
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
        timeout(Duration::from_secs(2), http_task)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), ws_task)
            .await
            .unwrap()
            .unwrap();
        assert!(status.success(), "{mode}: {diagnostics}");
        assert!(mode != "sigint" || interrupted);
        assert!(output.stdout.is_empty());
        assert!(
            diagnostics.contains("transport validation only"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("requested_at_cutoff=4"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("drained_messages=4"), "{diagnostics}");
    }
}
