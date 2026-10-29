use super::*;
use tokio::io::{DuplexStream, duplex};
use tungstenite::protocol::Role;

const UNSUBSCRIBE: &str = r#"{"operation":"unsubscribe","assets_ids":["A"]}"#;

#[test]
fn cold_recovery_escalates_without_real_time_and_only_readiness_resets_it() {
    let origin = Instant::now();
    let normal = Duration::from_millis(250);
    let mut recovery = ColdRecovery::default();
    recovery.begin(origin);
    assert_eq!(recovery.retry_delay(origin, normal), normal);
    let almost = origin + Duration::from_secs(120) - Duration::from_nanos(1);
    assert_eq!(recovery.retry_delay(almost, normal), normal);
    // Further attempts and handshakes retain the first episode start.
    recovery.begin(origin + Duration::from_secs(119));
    assert_eq!(recovery.since, Some(origin));
    let mut now = origin + Duration::from_secs(120);
    for seconds in [60, 120, 240, 480, 900, 900, 900] {
        // Even if a long connected-but-unready session reset the ordinary
        // reconnect delay, the cold delay wins and remains bounded.
        let delay = recovery.retry_delay(now, normal);
        assert_eq!(delay, Duration::from_secs(seconds));
        now += delay;
        recovery.begin(now);
    }
    recovery.ready();
    assert!(recovery.since.is_none());
    assert_eq!(recovery.cooldown, Duration::ZERO);
    recovery.begin(now);
    assert_eq!(recovery.retry_delay(now, normal), normal);
    assert_eq!(
        recovery.retry_delay(now + Duration::from_secs(120), normal),
        Duration::from_secs(60)
    );
}

#[tokio::test(start_paused = true)]
async fn queued_worker_keeps_admission_priority_and_cancels_on_retirement() {
    let config = Arc::new(Config {
        connection_interval: Duration::from_millis(1),
        ..Config::default()
    });
    let budget = ConnectionBudget::new(&config).unwrap();
    let first = budget.acquire(false).await.unwrap();
    let second = budget.acquire(false).await.unwrap();
    let (commands, receiver) = mpsc::channel(2);
    let (output, _ingress) = ingress::channel(4, 4096).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = run_worker_protocol(
        Session {
            worker: 0,
            generation: 0,
            adapter: AdapterId::PolymarketV1,
            slot: 0,
        },
        vec!["A".into()],
        receiver,
        output,
        config,
        stop.clone(),
        (
            budget.clone(),
            protocol_with_lifecycle(AdapterId::PolymarketV1, false),
        ),
    );
    tokio::pin!(worker);
    assert!(futures_util::poll!(&mut worker).is_pending());
    let later = budget.acquire(false);
    tokio::pin!(later);
    assert!(futures_util::poll!(&mut later).is_pending());
    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(futures_util::poll!(&mut worker).is_pending());
    }
    drop(first);
    // The first worker must still own the first queue position, even after
    // long local contention; no network failure has occurred.
    assert!(futures_util::poll!(&mut later).is_pending());
    // Retiring its entire assignment must cancel admission without dialing.
    commands
        .try_send(WorkerCommand {
            remove: vec!["A".into()],
            ..WorkerCommand::default()
        })
        .unwrap();
    assert!(futures_util::poll!(&mut worker).is_pending());
    let _later = later.await.unwrap();
    stop.send_replace(true);
    let report = worker.await.unwrap();
    assert_eq!(report.connection_attempts, 0);
    assert_eq!(report.sessions, 0);
    drop(second);
}

#[tokio::test(start_paused = true)]
async fn local_admission_wait_does_not_delay_first_network_retry() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Arc::new(Config {
        endpoint: format!("ws://{}", listener.local_addr().unwrap()),
        connection_interval: Duration::from_millis(1),
        reconnect_min: Duration::from_millis(1),
        ..Config::default()
    });
    let budget = ConnectionBudget::new(&config).unwrap();
    let first = budget.acquire(false).await.unwrap();
    let second = budget.acquire(false).await.unwrap();
    let (_commands, receiver) = mpsc::channel(2);
    let (output, _ingress) = ingress::channel(4, 4096).unwrap();
    let (stop, _) = watch::channel(false);
    let worker = run_worker_protocol(
        Session {
            worker: 0,
            generation: 0,
            adapter: AdapterId::PolymarketV1,
            slot: 0,
        },
        vec!["A".into()],
        receiver,
        output,
        config,
        stop.clone(),
        (
            budget,
            protocol_with_lifecycle(AdapterId::PolymarketV1, false),
        ),
    );
    tokio::pin!(worker);
    assert!(futures_util::poll!(&mut worker).is_pending());
    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(futures_util::poll!(&mut worker).is_pending());
    }
    // Use real time for socket I/O; only local admission was simulated.
    tokio::time::resume();
    drop(first);
    drop(second);
    let peer = async {
        let (socket, _) = listener.accept().await.unwrap();
        drop(socket); // Reject the first handshake, without publishing data.
        let (_retry, _) = listener.accept().await.unwrap();
        stop.send_replace(true);
    };
    let (report, ()) = timeout(Duration::from_secs(2), async { tokio::join!(worker, peer) })
        .await
        .expect("local waiting incorrectly charged as a cold network failure");
    let report = report.unwrap();
    assert_eq!(report.connection_attempts, 2);
    assert_eq!(report.sessions, 0);
}

#[tokio::test]
async fn cold_cooldown_ignores_duplicate_readiness_and_is_shutdown_cancellable() {
    let config = Arc::new(Config::default());
    let budget = ConnectionBudget::new(&config).unwrap();
    let (commands, command_receiver) = mpsc::channel(2);
    let (output, _ingress) = ingress::channel(2, 1024).unwrap();
    let (stop, stop_receiver) = watch::channel(false);
    let now = Instant::now();
    let mut worker = Worker {
        session: Session {
            adapter: AdapterId::PolymarketV1,
            worker: 0,
            slot: 0,
            generation: 1,
        },
        desired: HashMap::from([("A".into(), 0)]),
        commands: command_receiver,
        output,
        protocol: protocol_with_lifecycle(AdapterId::PolymarketV1, false),
        config,
        stop: stop_receiver,
        report: WorkerReport::default(),
        ordinal: 0,
        budget,
        lease: None,
        ready_started: Some(now),
        next_planned: false,
        cold_recovery: ColdRecovery::default(),
    };
    worker.cold_recovery.begin(now - Duration::from_secs(120));
    worker.initialized().unwrap();
    assert!(worker.cold_recovery.since.is_none());
    assert_eq!(worker.report.initialized_sessions, 1);

    // The consumed acknowledgment cannot terminate another cold episode.
    let since = now - Duration::from_secs(120);
    worker.cold_recovery.begin(since);
    worker.initialized().unwrap();
    assert_eq!(worker.cold_recovery.since, Some(since));
    assert_eq!(worker.report.initialized_sessions, 1);
    let session = worker.session;
    {
        let waiting = worker.wait_retry(Duration::from_millis(1));
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        // Assignment churn and a late offline acknowledgment are serviced,
        // but neither releases or restarts the pending cooldown.
        commands
            .try_send(WorkerCommand {
                add: vec!["B".into()],
                initialized: Some(session),
                ..WorkerCommand::default()
            })
            .unwrap();
        assert!(futures_util::poll!(&mut waiting).is_pending());
        stop.send_replace(true);
        timeout(Duration::from_millis(250), &mut waiting)
            .await
            .expect("shutdown failed to cancel cold cooldown")
            .unwrap();
    }
    assert!(worker.desired.contains_key("B"));
    assert_eq!(worker.cold_recovery.since, Some(since));
    assert_eq!(worker.cold_recovery.cooldown, Duration::from_secs(60));
    assert_eq!(worker.report.initialized_sessions, 1);
}

#[tokio::test]
async fn shared_budget_paces_starts_and_reserves_mandatory_capacity() {
    timeout(Duration::from_secs(2), async {
        let config = Config {
            connection_concurrency: 2,
            connection_interval: Duration::from_millis(10),
            ..Config::default()
        };
        let budget = ConnectionBudget::new(&config).unwrap();
        // Hold the pacing lock to observe a genuinely pending admission,
        // rather than inferring blocked work from an arbitrary quiet period.
        let gate = budget.next_start.lock().await;
        let first = budget.acquire(true);
        tokio::pin!(first);
        assert!(futures_util::poll!(&mut first).is_pending());
        drop(gate);
        let first = first.await.unwrap();
        assert_eq!(budget.all.available_permits(), 1);
        assert_eq!(budget.planned.available_permits(), 0);

        let voluntary = budget.acquire(true);
        tokio::pin!(voluntary);
        assert!(futures_util::poll!(&mut voluntary).is_pending());
        // The pending voluntary attempt must not consume the reserved
        // global lease or block a mandatory acquisition on the pacing lock.
        let mandatory = budget.acquire(false).await.unwrap();
        assert!(mandatory.started.duration_since(first.started) >= config.connection_interval);
        assert!(futures_util::poll!(&mut voluntary).is_pending());
        assert_eq!(budget.all.available_permits(), 0);

        let previous_start = mandatory.started;
        drop(first);
        let voluntary = voluntary.await.unwrap();
        assert!(voluntary.started.duration_since(previous_start) >= config.connection_interval);
        let next_mandatory = budget.acquire(false);
        tokio::pin!(next_mandatory);
        assert!(futures_util::poll!(&mut next_mandatory).is_pending());
        let previous_start = voluntary.started;
        drop(voluntary);
        let next_mandatory = next_mandatory.await.unwrap();
        assert!(
            next_mandatory.started.duration_since(previous_start) >= config.connection_interval
        );
        drop((mandatory, next_mandatory));
        assert_eq!(budget.all.available_permits(), 2);
        assert_eq!(budget.planned.available_permits(), 1);
    })
    .await
    .expect("shared connection budget fixture exceeded its deadline");
}

async fn socket_pair() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
    // Smaller than the unsubscribe frame: send cannot flush until the peer reads.
    let (client, peer) = duplex(8);
    (
        WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        WebSocketStream::from_raw_socket(peer, Role::Server, None).await,
    )
}

#[tokio::test]
async fn pending_websocket_send_is_cancelled_by_shutdown() {
    let (mut client, _peer) = socket_pair().await;
    let (stop, mut receiver) = watch::channel(false);
    let send = io(
        client.send(Message::Text(UNSUBSCRIBE.into())),
        Duration::from_secs(5),
        &mut receiver,
    );
    tokio::pin!(send);
    assert!(futures_util::poll!(&mut send).is_pending());

    // The peer remains alive and unread; only shutdown can release this send
    // before its write timeout. Silence on a real TCP socket is not this proof.
    stop.send_replace(true);
    let error = timeout(Duration::from_millis(250), &mut send)
        .await
        .expect("shutdown did not cancel the pending send")
        .expect_err("a blocked send cannot succeed without the peer reading");
    assert_eq!(error.to_string(), "shutdown");
}

#[tokio::test]
async fn pending_websocket_send_succeeds_when_peer_reads() {
    let (mut client, mut peer) = socket_pair().await;
    let (stop, mut receiver) = watch::channel(false);
    let send = io(
        client.send(Message::Text(UNSUBSCRIBE.into())),
        Duration::from_secs(5),
        &mut receiver,
    );
    tokio::pin!(send);
    assert!(futures_util::poll!(&mut send).is_pending());

    let (sent, received) = timeout(Duration::from_secs(1), async {
        tokio::join!(&mut send, peer.next())
    })
    .await
    .expect("reading the peer did not release the pending send");
    sent.unwrap();
    assert_eq!(
        received.unwrap().unwrap(),
        Message::Text(UNSUBSCRIBE.into())
    );
    assert!(!*stop.borrow());
}

#[tokio::test]
async fn pending_websocket_send_times_out_without_shutdown() {
    let (mut client, _peer) = socket_pair().await;
    let (stop, mut receiver) = watch::channel(false);
    let send = io(
        client.send(Message::Text(UNSUBSCRIBE.into())),
        Duration::from_millis(100),
        &mut receiver,
    );
    tokio::pin!(send);
    assert!(futures_util::poll!(&mut send).is_pending());

    let error = timeout(Duration::from_secs(1), &mut send)
        .await
        .expect("pending send exceeded its write timeout")
        .expect_err("a blocked send cannot succeed without the peer reading");
    assert_eq!(
        error.to_string(),
        "socket write timeout; delivery uncertain"
    );
    assert!(!*stop.borrow());
}
