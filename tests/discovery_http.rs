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
    encoded_server(
        replies
            .into_iter()
            .map(|(query, status, body)| (query, status, body.to_string().into_bytes(), ""))
            .collect(),
        gate,
    )
}

type EncodedReply = (Vec<&'static str>, u16, Vec<u8>, &'static str);

fn encoded_server(
    replies: Vec<EncodedReply>,
    gate: Option<(usize, std::sync::mpsc::Receiver<()>)>,
) -> (String, thread::JoinHandle<()>) {
    encoded_server_with_budget(replies, gate, Duration::from_secs(15))
}

fn encoded_server_with_budget(
    replies: Vec<EncodedReply>,
    gate: Option<(usize, std::sync::mpsc::Receiver<()>)>,
    budget: Duration,
) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/markets/keyset", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let deadline = std::time::Instant::now() + budget;
        for (index, (query_parts, status, body, headers)) in replies.into_iter().enumerate() {
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
            write!(socket, "HTTP/1.1 {status} response\r\nContent-Length: {}\r\nContent-Type: application/json\r\nRetry-After: 0\r\n{headers}Connection: close\r\n\r\n", body.len()).unwrap();
            socket.write_all(&body).unwrap();
        }
    });
    (url, handle)
}

#[tokio::test]
async fn compressed_pages_respect_the_decompressed_size_budget() {
    // Both contain market(0); the second adds padding to 8 MiB + 1 byte.
    // gzip fixtures were generated with mtime=0.
    for (body, desired, accepted) in [
        (
            include_bytes!("fixtures/gamma-market-page.json.gz").as_slice(),
            4,
            true,
        ),
        (
            include_bytes!("fixtures/gamma-oversized-page.json.gz").as_slice(),
            2,
            false,
        ),
    ] {
        let (url, server) = encoded_server(
            vec![
                (
                    vec!["version=v1", "accept-encoding: gzip"],
                    200,
                    body.to_vec(),
                    "Content-Encoding: gzip\r\n",
                ),
                (
                    vec!["version=v2"],
                    200,
                    serde_json::to_vec(&json!({"markets":[market(1)]})).unwrap(),
                    "",
                ),
            ],
            None,
        );
        let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
        let report = discovery.run_for(Duration::from_secs(10), true).await;
        assert_eq!(report.desired_assets, desired);
        assert_eq!(report.coverage_accepted(), accepted);
        assert_eq!(report.scans[0].error.is_none(), accepted);
        server.join().unwrap();
    }
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
    assert!(!report.coverage_accepted());
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
    assert!(!report.coverage_accepted());
    let counters = serde_json::to_value(&report).unwrap();
    assert_eq!(counters["listing_requests"], 2);
    assert_eq!(counters["verification_requests"], 1);
    server.join().unwrap();
}

#[tokio::test]
async fn evidence_preserves_scan_errors_and_verified_recovery_with_exact_final_identities() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("discovery.jsonl");
    let mut v1 = market(0);
    v1["id"] = json!("same-native-id");
    v1["clobTokenIds"] = json!("[\"00123\",\"456\"]");
    let mut malformed = v1.clone();
    malformed["active"] = Value::Null;
    let mut v2 = market(1);
    v2["id"] = json!("same-native-id");
    v2["positionIds"] = json!(["00123", "789"]);
    let (url, server) = server(vec![
        (
            vec!["version=v1", "closed=false"],
            200,
            json!({"markets":[malformed]}),
        ),
        (
            vec!["version=v1", "closed=any", "id=same-native-id"],
            200,
            json!({"markets":[v1]}),
        ),
        (
            vec!["version=v2", "closed=false"],
            200,
            json!({"markets":[v2]}),
        ),
    ]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    discovery.enable_evidence(&path, 1024 * 1024).unwrap();
    let report = discovery.run_for(Duration::from_secs(10), true).await;
    server.join().unwrap();
    assert!(!report.coverage_accepted());
    assert!(report.evidence.as_ref().unwrap().complete);
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let events: Vec<_> = rows.iter().map(|row| &row["event"]).collect();
    assert!(events.iter().any(|e| e["kind"] == "record"
        && e["resolved"] == false
        && e["adapter"] == "polymarket_v1"
        && e["record"]["Err"]["market_id"] == "same-native-id"));
    assert!(events.iter().any(|e| e["kind"] == "verification_result"
        && e["resolved"] == true
        && e["key"] == json!({"adapter":"polymarket_v1", "id":"same-native-id"})));
    let scans: Vec<_> = events.iter().filter(|e| e["kind"] == "scan").collect();
    assert_eq!(scans.len(), 2);
    assert_eq!(scans[0]["report"]["unresolved_records"], 1);
    let mut desired: Vec<_> = events
        .iter()
        .filter(|e| e["kind"] == "desired")
        .map(|e| {
            (
                e["key"].clone(),
                e["entry"]["instruments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| i["id"].clone())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    desired.sort_by_key(|(key, _)| key["adapter"].as_str().unwrap().to_owned());
    assert_eq!(
        desired,
        vec![
            (
                json!({"adapter":"polymarket_v1", "id":"same-native-id"}),
                vec![json!("00123"), json!("456")]
            ),
            (
                json!({"adapter":"polymarket_v2", "id":"same-native-id"}),
                vec![json!("00123"), json!("789")]
            ),
        ]
    );
    assert!(!events.iter().any(|e| e["kind"] == "pending"));
    assert_eq!(events.last().unwrap()["kind"], "complete");
    let before = std::fs::read(&path).unwrap();
    assert!(discovery.enable_evidence(&path, 1024 * 1024).is_err());
    assert_eq!(std::fs::read(path).unwrap(), before);
}

#[tokio::test]
async fn evidence_cap_stops_intake_and_does_not_claim_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capped.jsonl");
    let (url, server) = server(vec![(
        vec!["version=v1"],
        200,
        json!({"markets":[market(0)]}),
    )]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    discovery.enable_evidence(&path, 512).unwrap();
    let report = discovery.run_for(Duration::from_secs(10), true).await;
    server.join().unwrap();
    let status = report.evidence.unwrap();
    assert!(!status.complete);
    assert!(status.error.unwrap().contains("byte cap"));
    assert!(report.stopped);
    assert_eq!(report.listing_requests, 1);
    assert_eq!(report.desired_assets, 2);
    assert!(std::fs::metadata(&path).unwrap().len() <= 512);
    let prefix = std::fs::read_to_string(&path).unwrap();
    assert!(
        !prefix
            .lines()
            .filter_map(|s| serde_json::from_str::<Value>(s).ok())
            .any(|row| row["event"]["kind"] == "complete")
    );
    let (mut other, _commands, _changes) =
        Discovery::new(Gamma::new("http://127.0.0.1:1").unwrap());
    assert!(other.enable_evidence(&path, 4096).is_err());
    assert_eq!(std::fs::read_to_string(path).unwrap(), prefix);
}

#[test]
fn discovery_cli_reports_evidence_creation_failure_without_stdout_or_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.jsonl");
    std::fs::write(&path, b"independent existing evidence\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_discovery"))
        .args(["--seconds", "1", "--once", "--strict", "--evidence"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("cannot enable discovery evidence")
    );
    assert_eq!(
        std::fs::read(path).unwrap(),
        b"independent existing evidence\n"
    );
    // Failure happens before the run loop can issue any public HTTP request.
}

#[tokio::test]
async fn evidence_keeps_each_sweep_not_only_the_last_report() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sweeps.jsonl");
    let (url, server) = encoded_server_with_budget(
        vec![
            (vec!["version=v1"], 422, b"{}".to_vec(), ""),
            (
                vec!["version=v2"],
                200,
                json!({"markets":[market(1)]}).to_string().into_bytes(),
                "",
            ),
            (
                vec!["version=v1"],
                200,
                json!({"markets":[market(0)]}).to_string().into_bytes(),
                "",
            ),
            (
                vec!["version=v2"],
                200,
                json!({"markets":[market(1)]}).to_string().into_bytes(),
                "",
            ),
        ],
        None,
        Duration::from_secs(70),
    );
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    discovery.enable_evidence(&path, 1024 * 1024).unwrap();
    // Exercise the real production reconciliation interval rather than changing
    // scheduling or adding test-clock dependencies for evidence reporting.
    let report = discovery.run_for(Duration::from_secs(62), false).await;
    server.join().unwrap();
    assert!(report.stopped);
    assert!(
        report
            .scans
            .iter()
            .all(|s| s.pagination_complete && s.error.is_none())
    );
    assert!(report.evidence.unwrap().complete);
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let scans: Vec<_> = rows
        .iter()
        .map(|r| &r["event"])
        .filter(|e| e["kind"] == "scan")
        .collect();
    assert_eq!(scans.len(), 4);
    assert_eq!(scans[0]["sweep"], 1);
    assert!(
        scans[0]["report"]["error"]
            .as_str()
            .unwrap()
            .contains("422")
    );
    assert_eq!(scans[2]["sweep"], 2);
    assert_eq!(scans[2]["report"]["pagination_complete"], true);
    assert!(scans[2]["report"]["error"].is_null());
}

#[tokio::test]
async fn v2_verification_uses_its_adapter_even_when_native_ids_overlap_v1() {
    let v1 = market(0);
    let mut v2 = market(1);
    v2["id"] = v1["id"].clone();
    v2["positionIds"] = serde_json::from_str(v1["clobTokenIds"].as_str().unwrap()).unwrap();
    let mut uncertain = v2.clone();
    uncertain["active"] = Value::Null;
    let (url, server) = server(vec![
        (
            vec!["version=v1", "closed=false"],
            200,
            json!({"markets":[v1]}),
        ),
        (
            vec!["version=v2", "closed=false"],
            200,
            json!({"markets":[uncertain]}),
        ),
        (
            vec!["version=v2", "closed=any", "id=559651"],
            200,
            json!({"markets":[v2]}),
        ),
    ]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    let report = discovery.run_for(Duration::from_secs(10), true).await;
    assert_eq!(report.desired_assets, 4);
    assert_eq!(report.identity_conflicts, 0);
    assert_eq!(report.uncertain_markets, 0);
    assert_eq!(report.pending_verifications, 0);
    assert_eq!(report.scans[0].unresolved_records, 0);
    assert_eq!(report.scans[1].unresolved_records, 1);
    server.join().unwrap();
}

#[tokio::test]
async fn strict_acceptance_requires_clean_complete_traversals_not_just_connectivity() {
    let mut disabled = market(0);
    disabled["id"] = json!("disabled");
    disabled["enableOrderBook"] = json!(false);
    disabled["clobTokenIds"] = Value::Null;
    let (url, server) = server(vec![
        (
            vec!["version=v1", "closed=false"],
            200,
            json!({"markets":[market(0),disabled],"next_cursor":"tail"}),
        ),
        (
            vec!["version=v2", "closed=false"],
            200,
            json!({"markets":[market(1)]}),
        ),
        (
            vec!["version=v1", "after_cursor=tail"],
            200,
            json!({"markets":[]}),
        ),
    ]);
    let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
    let mut report = discovery.run_for(Duration::from_secs(10), true).await;
    assert_eq!(report.desired_assets, 4);
    assert!(report.coverage_accepted());
    server.join().unwrap();
    // Counterexamples: discarded unknown work and conflicts cannot pass by
    // merely emptying pending work. These counters outlive individual sweeps.
    report.expired_verifications = 1;
    assert!(!report.coverage_accepted());
    report.expired_verifications = 0;
    report.verification_overflow = 1;
    assert!(!report.coverage_accepted());
    report.verification_overflow = 0;
    report.identity_conflicts = 1;
    assert!(!report.coverage_accepted());
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
    let counters = serde_json::to_value(&report).unwrap();
    assert_eq!(counters["listing_requests"], 4); // Includes the HTTP retry, not just successful pages.
    assert_eq!(counters["verification_requests"], 0);
    server.join().unwrap();
}

#[tokio::test]
async fn oversized_final_verification_page_cannot_retire_known_market() {
    for (count, desired, failures) in [(1000, 2, 0), (1001, 4, 1)] {
        let mut closed = market(0);
        closed["closed"] = json!(true);
        closed["acceptingOrders"] = json!(false);
        let (url, server) = server(vec![
            (
                vec!["version=v1", "closed=false"],
                200,
                json!({"markets":[market(0),closed.clone()]}),
            ),
            (
                vec!["version=v1", "closed=any", "id=559651"],
                200,
                json!({"markets":vec![closed; count]}),
            ),
            (
                vec!["version=v2", "closed=false"],
                200,
                json!({"markets":[market(1)]}),
            ),
        ]);
        let (mut discovery, _commands, _changes) = Discovery::new(Gamma::new(&url).unwrap());
        let report = discovery.run_for(Duration::from_secs(10), true).await;
        assert_eq!(report.desired_assets, desired);
        assert_eq!(report.failed_verification_batches, failures);
        assert_eq!(report.uncertain_markets, failures);
        server.join().unwrap();
    }
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
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stopped.jsonl");
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
    discovery.enable_evidence(&path, 1024 * 1024).unwrap();
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
    assert!(report.evidence.unwrap().complete);
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let scans: Vec<_> = rows
        .iter()
        .map(|row| &row["event"])
        .filter(|event| event["kind"] == "scan")
        .collect();
    assert_eq!(scans.len(), 2);
    assert!(
        scans
            .iter()
            .all(|scan| scan["report"]["pagination_complete"] == false)
    );
    assert!(rows.iter().any(|row| row["event"]["kind"] == "snapshot_end"
        && row["event"]["desired_assets"] == 0
        && row["event"]["pending_verifications"] == 0));
    assert_eq!(rows.last().unwrap()["event"]["kind"], "complete");
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
        report.coverage_accepted(),
        "strict observed-discovery acceptance failed; see report (not proof of exchange-wide coverage)"
    );
}

#[tokio::test]
async fn continuous_discovery_serves_snapshots_until_explicit_stop() {
    let (url, server) = server(vec![
        (vec!["version=v1"], 200, json!({"markets":[market(0)]})),
        (vec!["version=v2"], 200, json!({"markets":[market(1)]})),
    ]);
    let (mut owner, commands, mut changed) = Discovery::new(Gamma::new(&url).unwrap());
    let task = tokio::spawn(async move { owner.run_until_stopped().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            changed.changed().await.unwrap();
            let (send, receive) = oneshot::channel();
            commands.send(Command::Snapshot(send)).await.unwrap();
            if receive.await.unwrap().desired_assets() == 4 {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    commands.send(Command::Stop).await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(report.stopped);
    assert_eq!(report.desired_assets, 4);
    assert!(report.scans.iter().all(|s| s.pagination_complete));
    server.join().unwrap();
}
