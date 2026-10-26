use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use engineering_challenge::discovery::polymarket::{Command, Discovery, Gamma};
use serde_json::{Value, json};
use tokio::sync::oneshot;

fn market(index: usize) -> Value {
    let records: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/gamma-markets.json")).unwrap();
    records[index].clone()
}

// Real HTTP boundary, no mocks of registry or reconciliation behavior.
fn server(replies: Vec<(Vec<&'static str>, u16, Value)>) -> (String, thread::JoinHandle<()>) {
    gated_server(replies, None)
}

fn gated_server(
    replies: Vec<(Vec<&'static str>, u16, Value)>,
    gate: Option<(usize, std::sync::mpsc::Receiver<()>)>,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/markets/keyset", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        for (index, (query_parts, status, body)) in replies.into_iter().enumerate() {
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "expected request never arrived"
                        );
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(e) => panic!("{e}"),
                }
            };
            // macOS can inherit O_NONBLOCK from the listening socket.
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 2048];
            while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let request = String::from_utf8(bytes).unwrap();
            for part in query_parts {
                assert!(request.contains(part), "missing {part}: {request}");
            }
            if let Some((pause_at, receiver)) = &gate
                && index == *pause_at
            {
                receiver.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            let body = body.to_string();
            write!(socket, "HTTP/1.1 {status} response\r\nContent-Length: {}\r\nContent-Type: application/json\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
    });
    (url, handle)
}

#[tokio::test]
async fn pagination_is_incremental_and_a_failed_version_does_not_erase_the_other() {
    let (release, gate) = std::sync::mpsc::channel();
    let (url, server) = gated_server(
        vec![
            (
                vec!["version=v1", "closed=false", "limit=100"],
                200,
                json!({"markets":[market(0)],"next_cursor":"next"}),
            ),
            (
                vec!["version=v2", "closed=false"],
                200,
                json!({"markets":[market(1)]}),
            ),
            (
                vec!["version=v1", "after_cursor=next"],
                422,
                json!({"error":"invalid cursor"}),
            ),
        ],
        Some((1, gate)),
    );
    let (mut discovery, commands, mut changes) = Discovery::new(Gamma::new(&url).unwrap());
    let consumer = async {
        changes.changed().await.unwrap();
        let (send, receive) = oneshot::channel();
        commands.send(Command::Snapshot(send)).await.unwrap();
        let snapshot = receive.await.unwrap();
        assert_eq!(snapshot.desired_assets(), 2);
        // The server cannot complete the sweep until the consumer sees page 1.
        release.send(()).unwrap();
    };
    let (report, ()) = tokio::time::timeout(Duration::from_secs(12), async {
        tokio::join!(discovery.run_for(Duration::from_secs(10), true), consumer)
    })
    .await
    .expect("consumer did not receive the published snapshot");
    assert_eq!(report.desired_assets, 4);
    assert!(!report.scans[0].pagination_complete);
    assert!(report.scans[0].error.is_some());
    assert!(report.scans[1].pagination_complete);
    server.join().unwrap();
}

#[tokio::test]
async fn uncertain_record_triggers_closed_any_lookup_and_does_not_block_other_markets() {
    let mut uncertain = market(0);
    uncertain["active"] = Value::Null;
    let (url, server) = server(vec![
        (
            vec!["version=v1", "closed=false"],
            200,
            json!({"markets":[uncertain]}),
        ),
        (
            vec!["version=v1", "closed=any", "id=559651"],
            200,
            json!({"markets":[market(0)]}),
        ),
        (
            vec!["version=v2", "closed=false"],
            200,
            json!({"markets":[market(1)]}),
        ),
    ]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    let report = discovery.run_for(Duration::from_secs(10), true).await;
    assert_eq!(report.desired_assets, 4);
    assert_eq!(report.pending_verifications, 0);
    assert_eq!(report.scans[0].unresolved_records, 1); // Historical scan accounting is not rewritten.
    server.join().unwrap();
}

#[tokio::test]
async fn retryable_error_is_bounded_and_repeated_cursor_is_not_success() {
    let (url, server) = server(vec![
        (vec!["version=v1"], 429, json!({})),
        (
            vec!["version=v1"],
            200,
            json!({"markets":[market(0)],"next_cursor":"loop"}),
        ),
        (vec!["version=v2"], 200, json!({"markets":[market(1)]})),
        (
            vec!["version=v1", "after_cursor=loop"],
            200,
            json!({"markets":[],"next_cursor":"loop"}),
        ),
    ]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    let report = discovery.run_for(Duration::from_secs(10), true).await;
    assert_eq!(report.desired_assets, 4);
    assert!(!report.scans[0].pagination_complete);
    server.join().unwrap();
}

#[tokio::test]
async fn exhausted_retries_do_not_stop_the_other_version() {
    let (url, server) = server(vec![
        (vec!["version=v1"], 503, json!({})),
        (vec!["version=v1"], 503, json!({})),
        (vec!["version=v1"], 503, json!({})),
        (vec!["version=v2"], 200, json!({"markets":[market(1)]})),
    ]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    let report = discovery.run_for(Duration::from_secs(10), true).await;
    assert_eq!(report.desired_assets, 2);
    assert_eq!(report.scans[0].pages, 0);
    assert!(report.scans[0].error.is_some());
    assert!(report.scans[1].pagination_complete);
    server.join().unwrap();
}

#[tokio::test]
async fn stop_and_snapshot_requests_work_during_retry_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/markets/keyset", listener.local_addr().unwrap());
    let (sent, received) = oneshot::channel();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut request = Vec::new();
        let mut buf = [0; 2048];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = socket.read(&mut buf).unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buf[..n]);
        }
        write!(socket, "HTTP/1.1 429 Limited\r\nContent-Length: 0\r\nRetry-After: 120\r\nConnection: close\r\n\r\n").unwrap();
        sent.send(()).unwrap();
    });
    let (mut discovery, commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    let control = async {
        received.await.unwrap();
        let (send, receive) = oneshot::channel();
        commands.send(Command::Snapshot(send)).await.unwrap();
        assert_eq!(receive.await.unwrap().desired_assets(), 0);
        commands.send(Command::Stop).await.unwrap();
    };
    let (report, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(discovery.run_for(Duration::from_secs(20), true), control)
    })
    .await
    .expect("control requests were blocked by retry sleep");
    assert!(report.stopped);
    assert!(!report.scans[0].pagination_complete);
    server.join().unwrap();
}

#[tokio::test]
#[ignore = "public Gamma read-only coverage probe; bounded to 120 seconds"]
async fn live_discovery_coverage() {
    use engineering_challenge::discovery::polymarket::GAMMA_MARKETS;
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(GAMMA_MARKETS).unwrap());
    let report = discovery.run_for(Duration::from_secs(120), true).await;
    eprintln!(
        "LIVE_DISCOVERY {}",
        serde_json::to_string_pretty(&report).unwrap()
    );
    assert!(
        report.scans.iter().all(|s| s.pages > 0),
        "both protocol versions must be reached"
    );
    assert!(report.desired_assets > 0);
    // Traversal completion and unresolved coverage are reported, not assumed.
}
