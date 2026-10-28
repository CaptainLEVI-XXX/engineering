//! Local synthetic peers, real C2 workers/ingress and C4/C5/reader boundaries.
//! Only scheduling is injected: Processor::new uses explicit logical process
//! time for health windows. Socket receive timestamps and session boundaries are
//! never rewritten. These accelerated thresholds are NOT latency measurements.
//! Standalone workers bypass discovery/manager routing and their shared dial
//! budget; feedback below forwards the production session-checked commands.
use chrono::Utc;
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    orderbook::{self, Processor, SelectionPolicy, Validity},
    recording::{self, Recorder},
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
    websocket::{
        self, WorkerCommand, WorkerReport,
        ingress::{self, Envelope, Message, Session},
        run_worker_session,
    },
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, sync::watch, task::JoinHandle, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message as Wire};

const WAIT: Duration = Duration::from_secs(5);
const ADAPTER: AdapterId = AdapterId::PolymarketV1;

fn session(worker: usize, generation: u64) -> Session {
    Session {
        worker,
        generation,
        adapter: ADAPTER,
        slot: worker as u16,
    }
}

fn book(asset: &str, quantity: u32) -> String {
    json!({"event_type":"book", "asset_id":asset, "timestamp":"1000",
        "bids":[{"price":"0.3", "size":quantity.to_string()}], "asks":[]})
    .to_string()
}

fn changes(asset: &str, quantities: &[u32], exchange_ms: u32) -> String {
    let entries: Vec<_> = quantities
        .iter()
        .map(|q| {
            json!({"asset_id":asset,
        "side":"BUY", "price":"0.3", "size":q.to_string()})
        })
        .collect();
    json!({"event_type":"price_change", "timestamp":exchange_ms.to_string(),
        "price_changes":entries})
    .to_string()
}

enum PeerCommand {
    Send(String),
    // Send a nonfinancial marker immediately AFTER a real PING/PONG exchange.
    // Consuming it in ingress proves the worker read past that PONG.
    AfterPong(String),
    Disconnect,
}

async fn peer(
    listener: TcpListener,
    mut commands: mpsc::Receiver<PeerCommand>,
    assets: Vec<String>,
) {
    loop {
        let socket = tokio::select! {
            accepted = listener.accept() => accepted.unwrap().0,
            command = commands.recv() => {
                assert!(command.is_none(), "command before connection");
                return;
            }
        };
        let mut ws = accept_async(socket).await.unwrap();
        let request = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let request: Value = serde_json::from_str(&request).unwrap();
        let mut requested: Vec<String> =
            serde_json::from_value(request["assets_ids"].clone()).unwrap();
        requested.sort();
        assert_eq!(requested, assets);
        let mut after_pong = None;
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(PeerCommand::Send(text)) => ws.send(Wire::Text(text.into())).await.unwrap(),
                    Some(PeerCommand::AfterPong(text)) => {
                        assert!(after_pong.replace(text).is_none());
                    }
                    Some(PeerCommand::Disconnect) => break, // Abrupt TCP loss, not an injected boundary.
                    None => return,
                },
                message = ws.next() => match message {
                    Some(Ok(Wire::Text(text))) if text == "PING" => {
                        ws.send(Wire::Text("PONG".into())).await.unwrap();
                        if let Some(marker) = after_pong.take() {
                            ws.send(Wire::Text(marker.into())).await.unwrap();
                        }
                    }
                    Some(Ok(Wire::Close(_))) => {
                        let _ = ws.flush().await;
                        break;
                    }
                    Some(Ok(_)) => {} // Includes best-effort unsubscribe during renewal.
                    Some(Err(_)) | None => break,
                }
            }
        }
    }
}

struct Harness {
    processor: Processor,
    recorder: Recorder,
    output: recording::Output,
    ingress: crossbeam::channel::Receiver<Envelope>,
    commands: Vec<mpsc::Sender<WorkerCommand>>,
    peers: Vec<mpsc::Sender<PeerCommand>>,
    workers: Vec<JoinHandle<anyhow::Result<WorkerReport>>>,
    servers: Vec<JoinHandle<()>>,
    stop: watch::Sender<bool>,
    complete: [u64; 2],
    active: [Option<Session>; 2],
    ordinals: HashMap<Session, u64>,
    ended: Vec<Session>,
    now: u64,
    started_at: chrono::DateTime<Utc>,
    started_mono: std::time::Instant,
    // Receipt evidence comes from raw ingress, never from reconstructed books.
    receipts: HashMap<(String, i64, i64), i64>,
}

impl Harness {
    async fn new(assets: &[&str], renewal_seconds: u64) -> Self {
        let config = orderbook::Config {
            selection: SelectionPolicy::Automatic,
            monitoring: true,
            renewal_interval: Duration::from_secs(renewal_seconds),
            instruments: 8,
            replicas: 16,
            sessions: 2,
            levels_per_side: 8,
            total_levels: 64,
            snapshot_interval: None,
            ..Default::default()
        };
        let (recorder, output) = recording::channel(
            recording::Config {
                batches: 4,
                batch_bytes: 128 << 10,
                publish_bytes: 128 << 10,
                entries: 512,
                ..Default::default()
            },
            &config,
        )
        .unwrap();
        let (sender, ingress) = ingress::channel(64, 1 << 20).unwrap();
        let (stop, _) = watch::channel(false);
        let mut h = Self {
            processor: Processor::new(config).unwrap(),
            recorder,
            output,
            ingress,
            commands: vec![],
            peers: vec![],
            workers: vec![],
            servers: vec![],
            stop,
            complete: [0; 2],
            active: [None; 2],
            ordinals: HashMap::new(),
            ended: vec![],
            now: 0,
            started_at: Utc::now(),
            started_mono: std::time::Instant::now(),
            receipts: HashMap::new(),
        };
        for worker in 0..2 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = websocket::Config {
                endpoint: format!("ws://{}", listener.local_addr().unwrap()),
                replicas_per_market: 2,
                heartbeat: Duration::from_millis(20),
                pong_timeout: Duration::from_secs(2),
                io_timeout: Duration::from_secs(1),
                reconnect_min: Duration::from_millis(5),
                reconnect_max: Duration::from_millis(5),
                connection_interval: Duration::from_millis(1),
                // Missing books are evaluated by logical C4 time, not a wall-clock dial expiry.
                initialization_timeout: Duration::from_secs(60),
                ..Default::default()
            };
            let mut assets: Vec<String> = assets.iter().map(|s| (*s).into()).collect();
            assets.sort();
            let (peer_send, peer_recv) = mpsc::channel(8);
            h.servers
                .push(tokio::spawn(peer(listener, peer_recv, assets.clone())));
            h.peers.push(peer_send);
            let (commands, receive) = mpsc::channel(8);
            h.commands.push(commands);
            h.workers.push(tokio::spawn(run_worker_session(
                session(worker, 0),
                assets,
                receive,
                sender.clone(),
                Arc::new(config),
                h.stop.clone(),
            )));
            h.wait_complete(worker, 1).await;
        }
        h
    }

    fn process(&mut self, message: &Message) {
        match message {
            Message::Started(s) => {
                assert!(self.active[s.worker].replace(*s).is_none());
                if s.generation > 1 {
                    assert!(self.ended.contains(&session(s.worker, s.generation - 1)));
                }
                assert!(self.ordinals.insert(*s, 0).is_none());
            }
            Message::RequestsComplete { session: s } => self.complete[s.worker] = s.generation,
            Message::Ended { session: s, .. } => {
                assert_eq!(self.active[s.worker].take(), Some(*s));
                self.ended.push(*s);
            }
            Message::Data {
                session: s,
                ordinal,
                received_at,
                received_mono,
                payload,
            } => {
                assert_eq!(self.active[s.worker], Some(*s));
                let previous = self.ordinals.get_mut(s).unwrap();
                assert_eq!(*ordinal, *previous + 1);
                *previous = *ordinal;
                assert!(*received_at >= self.started_at && *received_at <= Utc::now());
                assert!(
                    *received_mono >= self.started_mono
                        && *received_mono <= std::time::Instant::now()
                );
                let wire: Value = serde_json::from_str(payload).unwrap();
                if let Some(entries) = wire["price_changes"].as_array() {
                    let exchange: i64 = wire["timestamp"].as_str().unwrap().parse().unwrap();
                    for entry in entries {
                        let quantity: i64 = entry["size"].as_str().unwrap().parse().unwrap();
                        self.receipts.insert(
                            (
                                entry["asset_id"].as_str().unwrap().into(),
                                quantity * 1_000_000,
                                exchange * 1000,
                            ),
                            received_at.timestamp_micros(),
                        );
                    }
                }
            }
            _ => {}
        }
        self.processor
            .process(message, Duration::from_secs(self.now), &mut |o| {
                self.recorder.observe(o)
            })
            .unwrap();
    }

    async fn ready_feedback(&mut self) {
        let ready: Vec<_> = self.processor.initialized_sessions().collect();
        for s in ready {
            self.commands[s.worker]
                .send(WorkerCommand {
                    initialized: Some(s),
                    ..Default::default()
                })
                .await
                .unwrap();
            self.processor.initialization_delivered(s);
        }
    }

    async fn until(&mut self, matches: impl Fn(&Message) -> bool) {
        timeout(WAIT, async {
            loop {
                if let Ok(envelope) = self.ingress.try_recv() {
                    self.process(&envelope.message);
                    self.ready_feedback().await;
                    if matches(&envelope.message) {
                        break;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        })
        .await
        .expect("phase did not reach its consumed-ingress boundary");
    }

    async fn wait_complete(&mut self, worker: usize, generation: u64) {
        if self.complete[worker] < generation {
            self.until(|m| {
                matches!(m, Message::RequestsComplete { session: s }
                if *s == session(worker, generation))
            })
            .await;
        }
    }

    async fn send(&mut self, worker: usize, payload: String) {
        self.peers[worker]
            .send(PeerCommand::Send(payload.clone()))
            .await
            .unwrap();
        self.until(|m| {
            matches!(m, Message::Data { session: s, payload: text, .. }
            if s.worker == worker && text.as_str() == payload)
        })
        .await;
    }

    async fn pong(&mut self, worker: usize) {
        let payload =
            json!({"event_type":"last_trade_price", "barrier":format!("{worker}-{}", self.now)})
                .to_string();
        self.peers[worker]
            .send(PeerCommand::AfterPong(payload.clone()))
            .await
            .unwrap();
        self.until(|m| {
            matches!(m, Message::Data { session: s, payload: text, .. }
            if s.worker == worker && text.as_str() == payload)
        })
        .await;
    }

    fn maintain(&mut self) {
        assert!(
            self.ingress.is_empty(),
            "drain actual deliveries before policy phases"
        );
        for _ in 0..32 {
            self.processor
                .maintain(Duration::from_secs(self.now), Utc::now(), true, &mut |o| {
                    self.recorder.observe(o)
                })
                .unwrap();
        }
    }

    fn tick(&mut self, now: u64) {
        assert!(now >= self.now);
        self.now = now;
        for _ in 0..32 {
            self.processor.tick(Duration::from_secs(now)).unwrap();
        }
        self.maintain();
    }

    async fn forward_recovery(&mut self) -> Vec<(Session, bool)> {
        let mut sent = vec![];
        while let Some((s, planned)) = self.processor.pending_recovery() {
            self.commands[s.worker]
                .send(WorkerCommand {
                    restart: Some(s),
                    planned_restart: planned,
                    ..Default::default()
                })
                .await
                .unwrap();
            self.processor.recovery_delivered(s);
            sent.push((s, planned));
        }
        sent
    }

    fn selected(&self, asset: &str) -> Option<Session> {
        self.processor
            .coverage()
            .find(|r| r.asset == asset && r.selected && r.state == Validity::Live)
            .map(|r| r.binding.session)
    }

    async fn pair(renewal: u64) -> Self {
        let mut h = Self::new(&["A"], renewal).await;
        h.send(0, book("A", 3)).await;
        h.send(1, book("A", 7)).await;
        h.maintain();
        assert_eq!(h.selected("A"), Some(session(0, 1)));
        h
    }

    async fn finish(mut self) -> Vec<Value> {
        self.stop.send_replace(true);
        for worker in self.workers.drain(..) {
            timeout(WAIT, worker).await.unwrap().unwrap().unwrap();
        }
        while let Ok(envelope) = self.ingress.try_recv() {
            self.process(&envelope.message);
        }
        assert_eq!(self.active, [None; 2]);
        assert_eq!(self.processor.stats().rejected, 0);
        drop(self.peers);
        for server in self.servers {
            timeout(WAIT, server).await.unwrap().unwrap();
        }
        self.recorder.finish(self.processor.finish()).unwrap();
        let archive = tempfile::NamedTempFile::new().unwrap();
        let sidecar = tempfile::NamedTempFile::new().unwrap();
        let (approve, approval) = crossbeam::channel::bounded(1);
        approve.send(true).unwrap();
        // The small bounded fixture fits the pool; output is deliberately drained
        // at the end, as in recording.rs. This is not an output-throughput test.
        let report = self.output.run(
            archive.reopen().unwrap(),
            sidecar.reopen().unwrap(),
            "localhost dual-feed fixture",
            || {},
            approval,
        );
        assert!(report.error.is_none(), "{report:?}");
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive.path()).unwrap();
        let mut rows = vec![];
        while let Some(message) = reader.read_msg().unwrap() {
            match message {
                AnyMessage::Snapshot(s) => {
                    let s = SnapshotDomain::try_from_decoder(s).unwrap();
                    assert!(s.asks.is_empty());
                    assert_eq!(s.bids.len(), 1);
                    assert_eq!(s.bids[0].price_level, 300_000);
                    assert!(s.received_at >= self.started_at && s.received_at <= Utc::now());
                    rows.push(json!([
                        "snapshot",
                        s.symbol,
                        s.bids[0].quantity,
                        s.exchange_timestamp.timestamp_micros()
                    ]));
                }
                AnyMessage::PriceLevelUpdate(l) => {
                    let l = PriceLevelUpdateDomain::try_from_decoder(l).unwrap();
                    assert_eq!(l.side, Side::Buy);
                    assert_eq!(l.price_level, 300_000);
                    let asset = l.symbol.strip_prefix("polymarket:v1:").unwrap();
                    let exchange = l.exchange_timestamp.timestamp_micros();
                    assert_eq!(
                        l.backtest_timestamp.timestamp_micros(),
                        self.receipts[&(asset.into(), l.quantity, exchange)]
                    );
                    rows.push(json!(["level", l.symbol, l.quantity, exchange]));
                }
                _ => panic!("unexpected SBE message"),
            }
        }
        assert_eq!(report.records_confirmed, rows.len() as u64);
        rows
    }
}

fn snapshot(asset: &str, quantity: i64, exchange_us: i64) -> Value {
    json!([
        "snapshot",
        format!("polymarket:v1:{asset}"),
        quantity,
        exchange_us
    ])
}
fn level(asset: &str, quantity: i64, exchange_us: i64) -> Value {
    json!([
        "level",
        format!("polymarket:v1:{asset}"),
        quantity,
        exchange_us
    ])
}

#[tokio::test]
async fn selected_disconnect_restarts_fresh_and_snapshots_before_standby_deltas() {
    let mut h = Harness::pair(10_000).await;
    h.send(1, changes("A", &[8], 1001)).await;
    h.peers[0].send(PeerCommand::Disconnect).await.unwrap();
    h.until(|m| matches!(m, Message::Ended { session: s, .. } if *s == session(0, 1)))
        .await;
    h.maintain();
    assert_eq!(h.selected("A"), Some(session(1, 1)));
    h.send(1, changes("A", &[9], 999)).await;
    h.wait_complete(0, 2).await;
    h.send(0, book("A", 30)).await;
    h.send(0, changes("A", &[31], 1002)).await; // Recovered standby cannot leak.
    assert_eq!(h.selected("A"), Some(session(1, 1)));
    assert_eq!(
        h.finish().await,
        vec![
            snapshot("A", 3_000_000, 1_000_000),
            snapshot("A", 8_000_000, 1_001_000),
            level("A", 9_000_000, 999_000)
        ]
    );
}

#[tokio::test]
async fn per_instrument_silence_despite_pong_repairs_only_after_confirmation() {
    let mut h = Harness::pair(10_000).await;
    h.now = 50;
    h.send(1, changes("A", &[7, 8, 9, 10, 11], 1001)).await;
    h.pong(0).await;
    h.tick(60);
    assert_eq!(h.processor.pending_recovery(), None);
    h.tick(69);
    assert_eq!(h.processor.pending_recovery(), None);
    h.pong(0).await;
    h.tick(70);
    assert_eq!(h.selected("A"), Some(session(1, 1)));
    assert_eq!(h.forward_recovery().await, vec![(session(0, 1), false)]);
    h.send(1, changes("A", &[12], 998)).await;
    h.wait_complete(0, 2).await;
    h.send(0, book("A", 30)).await;
    assert!(!h.processor.replica(ADAPTER, "A", 0).unwrap().suspect);
    assert_eq!(
        h.processor
            .replica(ADAPTER, "A", 0)
            .unwrap()
            .health
            .accepted_changes(),
        0
    );
    assert_eq!(
        h.finish().await,
        vec![
            snapshot("A", 3_000_000, 1_000_000),
            snapshot("A", 11_000_000, 1_001_000),
            level("A", 12_000_000, 998_000)
        ]
    );
}

#[tokio::test]
async fn both_quiet_with_pongs_remains_unknown_not_a_repair() {
    let mut h = Harness::pair(10_000).await;
    for now in [60, 70, 120, 180, 300] {
        h.now = now;
        h.pong(0).await;
        h.pong(1).await;
        h.tick(now);
        assert_eq!(h.processor.pending_recovery(), None);
        assert_eq!(h.selected("A"), Some(session(0, 1)));
    }
    assert_eq!(h.finish().await, vec![snapshot("A", 3_000_000, 1_000_000)]);
}

#[tokio::test]
async fn delayed_equal_rate_feed_is_not_detectable_and_retains_wire_order() {
    let mut h = Harness::pair(10_000).await;
    h.now = 40;
    h.send(1, changes("A", &[71, 72, 73, 74, 75, 76, 77], 2000))
        .await;
    // Synthetic exchange delay: selected delivery is withheld until a later
    // logical phase. No network-latency claim and no fabricated receive clock.
    h.now = 50;
    h.send(0, changes("A", &[4, 5, 6, 7, 8], 1000)).await;
    h.send(0, changes("A", &[9, 10], 999)).await;
    h.tick(60);
    h.tick(70);
    assert_eq!(
        h.processor.pending_recovery(),
        None,
        "rate evidence cannot diagnose constant delay or equal-rate wrong state"
    );
    let mut expected = vec![snapshot("A", 3_000_000, 1_000_000)];
    expected.extend((4..=8).map(|q| level("A", q * 1_000_000, 1_000_000)));
    expected.extend([
        level("A", 9_000_000, 999_000),
        level("A", 10_000_000, 999_000),
    ]);
    assert_eq!(h.finish().await, expected);
}

#[tokio::test]
async fn unavailable_on_both_replicas_must_not_repair_both_initialized_neighbors() {
    let mut h = Harness::new(&["A", "B", "X"], 10_000).await;
    for (worker, a, b) in [(0, 3, 4), (1, 7, 8)] {
        h.send(worker, book("A", a)).await;
        h.send(worker, book("B", b)).await;
        h.pong(worker).await;
    }
    assert_eq!(h.selected("X"), None);
    assert!(h.processor.initialized_sessions().next().is_none());
    h.tick(30); // X never supplied a baseline on either otherwise useful socket.
    let repairs = h.forward_recovery().await;
    for (s, _) in &repairs {
        h.wait_complete(s.worker, s.generation + 1).await;
    }
    // Do not initialize the restarted worker. The original peer must continue
    // recording neighbors while X is still absent and replacement is cold.
    h.send(1, changes("A", &[9], 1001)).await;
    h.send(1, changes("B", &[10], 1002)).await;
    h.tick(40);
    let overlapping = h.processor.pending_recovery();
    h.send(1, changes("A", &[11], 1003)).await;
    // Restored neighboring baselines permit the deferred peer to try next.
    // X remains missing on BOTH: slot zero must not monopolize recovery.
    h.send(0, book("A", 30)).await;
    h.send(0, book("B", 40)).await;
    h.tick(60);
    assert_eq!(h.selected("X"), None);
    let resumed = h.forward_recovery().await;
    for (s, _) in &resumed {
        h.wait_complete(s.worker, s.generation + 1).await;
    }
    h.send(0, changes("A", &[31], 1004)).await;
    let rows = h.finish().await;
    // Keep this regression strict: two source-wide repairs remove the ONLY
    // usable neighboring replica after the first repair has already fenced one.
    assert_eq!(
        repairs,
        vec![(session(0, 1), false)],
        "missing X must serialize repairs; it must not sacrifice both A/B replicas"
    );
    assert_eq!(
        overlapping, None,
        "cold replacement must not authorize peer repair"
    );
    assert_eq!(resumed, vec![(session(1, 1), false)]);
    assert_eq!(
        rows,
        vec![
            snapshot("A", 3_000_000, 1_000_000),
            snapshot("B", 4_000_000, 1_000_000),
            snapshot("A", 7_000_000, 1_000_000),
            snapshot("B", 8_000_000, 1_000_000),
            level("A", 9_000_000, 1_001_000),
            level("B", 10_000_000, 1_002_000),
            level("A", 11_000_000, 1_003_000),
            snapshot("A", 30_000_000, 1_000_000),
            snapshot("B", 40_000_000, 1_000_000),
            level("A", 31_000_000, 1_004_000)
        ]
    );
}

#[tokio::test]
async fn scheduled_renewals_are_staggered_until_fresh_peer_initialization() {
    let mut h = Harness::pair(5).await;
    h.now = 5;
    h.maintain();
    assert_eq!(h.selected("A"), Some(session(1, 1)));
    assert_eq!(h.forward_recovery().await, vec![(session(0, 1), true)]);
    h.wait_complete(0, 2).await;
    h.now = 6; // Worker 1's deterministic stagger is now due, peer still cold.
    h.maintain();
    assert_eq!(h.processor.pending_recovery(), None);
    h.send(1, changes("A", &[9], 1001)).await;
    h.send(0, book("A", 30)).await;
    h.maintain();
    assert_eq!(h.selected("A"), Some(session(0, 2)));
    assert_eq!(h.forward_recovery().await, vec![(session(1, 1), true)]);
    h.send(0, changes("A", &[31], 999)).await;
    h.wait_complete(1, 2).await;
    h.send(1, book("A", 70)).await;
    assert_eq!(h.processor.stats().renewals_granted, 2);
    assert_eq!(
        h.finish().await,
        vec![
            snapshot("A", 3_000_000, 1_000_000),
            snapshot("A", 7_000_000, 1_000_000),
            level("A", 9_000_000, 1_001_000),
            snapshot("A", 30_000_000, 1_000_000),
            level("A", 31_000_000, 999_000)
        ]
    );
}
