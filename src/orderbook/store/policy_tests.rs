//! Small policy-integration fixtures. The parent includes this as a test-only
//! child of `store`; no sockets, sleeps, wall-clock jumps, or production writes.
use super::*;
use chrono::TimeZone;
use serde_json::json;
use std::time::Instant;

const ADAPTER: AdapterId = AdapterId::PolymarketV1;

fn seconds(value: u64) -> Duration {
    Duration::from_secs(value)
}

fn session(worker: usize, slot: u16) -> Session {
    Session {
        worker,
        generation: 1,
        adapter: ADAPTER,
        slot,
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Row {
    asset: String,
    session: Session,
    kind: &'static str,
    quantity: Option<i64>,
}

fn record(rows: &mut Vec<Row>, observation: Observation<'_>) -> Result<()> {
    let (kind, quantity) = match observation.kind {
        ObservationKind::StandbyBaseline => return Ok(()),
        ObservationKind::Snapshot { book, origin } => {
            let kind = match origin {
                SnapshotOrigin::Exchange => "exchange",
                SnapshotOrigin::Periodic { .. } => "periodic",
                SnapshotOrigin::Selection {
                    reason: SwitchReason::HealthSuspicion,
                    ..
                } => "health-selection",
                SnapshotOrigin::Selection {
                    reason: SwitchReason::PlannedRotation,
                    ..
                } => "planned-selection",
                SnapshotOrigin::Selection { .. } => "selection",
            };
            (
                kind,
                book.best(Side::Buy).map(|(_, quantity)| quantity.value()),
            )
        }
        ObservationKind::Level { quantity, .. } => ("level", Some(quantity.value())),
        ObservationKind::Invalidated { output_binding, .. } => (
            if output_binding.is_some() {
                "unavailable"
            } else {
                "standby-invalidated"
            },
            None,
        ),
    };
    rows.push(Row {
        asset: observation.asset.to_owned(),
        session: observation.binding.session,
        kind,
        quantity,
    });
    Ok(())
}

fn snapshot(asset: &str, quantity: &str) -> String {
    json!({"event_type":"book", "asset_id":asset, "timestamp":"1000",
        "bids":[{"price":"0.3", "size":quantity}], "asks":[]})
    .to_string()
}

fn changes(asset: &str, quantity: &str, count: usize) -> String {
    let entries: Vec<_> = (0..count)
        .map(|_| json!({"asset_id":asset, "side":"BUY", "price":"0.3", "size":quantity}))
        .collect();
    json!({"event_type":"price_change", "timestamp":"1000", "price_changes":entries}).to_string()
}

struct Fixture {
    processor: Processor,
    origin: Instant,
    ordinals: BTreeMap<usize, u64>,
    rows: Vec<Row>,
}

impl Fixture {
    fn new(renewal_seconds: u64) -> Self {
        let origin = Instant::now();
        let config = Config {
            selection: SelectionPolicy::Automatic,
            monitoring: true,
            renewal_interval: seconds(renewal_seconds),
            instruments: 8,
            replicas: 16,
            sessions: 8,
            levels_per_side: 8,
            total_levels: 64,
            snapshot_interval: None,
            ..Config::default()
        };
        Self {
            processor: Processor::with_receive_clock(config, origin).unwrap(),
            origin,
            ordinals: BTreeMap::new(),
            rows: Vec::new(),
        }
    }

    fn process(&mut self, message: Message, now: u64) {
        let rows = &mut self.rows;
        self.processor
            .process(&message, seconds(now), &mut |observation| {
                record(rows, observation)
            })
            .unwrap();
    }

    fn reconcile(&mut self, revision: u64, add: &[&str], remove: &[&str], now: u64) {
        self.process(
            Message::ReconcileBegin {
                revision,
                chunks: 1,
            },
            now,
        );
        self.process(
            Message::DesiredChunk {
                adapter: ADAPTER,
                revision,
                index: 0,
                add: add.iter().map(|asset| (*asset).to_owned()).collect(),
                remove: remove.iter().map(|asset| (*asset).to_owned()).collect(),
            },
            now,
        );
        self.process(Message::ReconcileComplete { revision }, now);
    }

    fn data_at(&mut self, source: Session, payload: String, now: u64, received: u64) {
        let ordinal = self
            .ordinals
            .get_mut(&source.worker)
            .expect("started source");
        *ordinal += 1;
        let message = Message::Data {
            session: source,
            ordinal: *ordinal,
            received_at: Utc.timestamp_opt(100 + received as i64, 0).unwrap(),
            received_mono: self.origin + seconds(received),
            payload: payload.into(),
        };
        self.process(message, now);
    }

    fn data(&mut self, source: Session, payload: String, now: u64) {
        self.data_at(source, payload, now, now);
    }

    fn start(&mut self, source: Session, assets: &[&str], initialized: bool) {
        self.ordinals.insert(source.worker, 0);
        self.process(Message::Started(source), 0);
        self.process(
            Message::SubscriptionsRequested {
                session: source,
                revisions: vec![1; assets.len()],
                add: assets.iter().map(|asset| (*asset).to_owned()).collect(),
                remove: vec![],
            },
            0,
        );
        if initialized {
            for asset in assets {
                self.data(
                    source,
                    snapshot(asset, if source.slot == 0 { "3" } else { "7" }),
                    0,
                );
            }
        }
        self.process(Message::RequestsComplete { session: source }, 0);
        if initialized {
            self.ready(source);
        } else {
            assert!(
                !self
                    .processor
                    .initialized_sessions()
                    .any(|ready| ready == source)
            );
        }
    }

    fn ready(&mut self, source: Session) {
        assert!(
            self.processor
                .initialized_sessions()
                .any(|ready| ready == source)
        );
        self.processor.initialization_delivered(source);
        assert!(
            !self
                .processor
                .initialized_sessions()
                .any(|ready| ready == source)
        );
    }

    fn pair(renewal_seconds: u64) -> Self {
        let mut fixture = Self::new(renewal_seconds);
        fixture.reconcile(1, &["A"], &[], 0);
        fixture.start(session(0, 0), &["A"], true);
        fixture.start(session(1, 1), &["A"], true);
        fixture
    }

    fn tick(&mut self, now: u64) {
        // Production maintenance is sliced by elapsed execution time. Repeated
        // calls at ONE logical cutoff drain these <=2-book fixtures without
        // advancing confirmation windows or depending on one slice's duration.
        for _ in 0..32 {
            self.processor.tick(seconds(now)).unwrap();
        }
    }

    fn maintain(&mut self, now: u64, queue_empty: bool) {
        for _ in 0..32 {
            let rows = &mut self.rows;
            self.processor
                .maintain(
                    seconds(now),
                    Utc.timestamp_opt(100 + now as i64, 0).unwrap(),
                    queue_empty,
                    &mut |observation| record(rows, observation),
                )
                .unwrap();
        }
    }

    fn selected(&self, asset: &str) -> Option<Session> {
        self.processor
            .coverage()
            .find(|row| row.asset == asset && row.selected && row.state == Validity::Live)
            .map(|row| row.binding.session)
    }

    fn replica(&self, asset: &str, slot: u16) -> &Replica {
        self.processor.replica(ADAPTER, asset, slot).unwrap()
    }
}

#[test]
fn silent_selected_source_needs_two_observations_then_switches_before_peer_delta() {
    let mut fixture = Fixture::pair(10_000);
    let quiet = session(0, 0);
    let active = session(1, 1);
    // Identical absolute sets are accepted observations, not deduplicated trades.
    fixture.data(active, changes("A", "7", 5), 50);
    fixture.tick(60);
    assert!(!fixture.replica("A", 0).suspect);
    assert_eq!(fixture.processor.pending_recovery(), None);
    fixture.tick(69);
    assert_eq!(fixture.processor.pending_recovery(), None);
    fixture.tick(70);
    assert!(fixture.replica("A", 0).suspect);
    assert!(!fixture.replica("A", 1).suspect);
    assert_eq!(fixture.processor.pending_recovery(), Some((quiet, false)));

    let cutoff = fixture.rows.len();
    fixture.maintain(70, true);
    assert_eq!(fixture.selected("A"), Some(active));
    fixture.data(active, changes("A", "9", 1), 70);
    let financial: Vec<_> = fixture.rows[cutoff..]
        .iter()
        .filter(|row| row.quantity.is_some())
        .map(|row| (row.kind, row.session, row.quantity))
        .collect();
    assert_eq!(
        financial,
        vec![
            ("health-selection", active, Some(7_000_000)),
            ("level", active, Some(9_000_000)),
        ]
    );
    fixture.tick(80);
    assert_eq!(fixture.processor.take_recovery(), vec![quiet]);
    assert!(fixture.processor.take_recovery().is_empty());
}

#[test]
fn both_quiet_initialized_peers_do_not_trigger_health_repairs() {
    let mut fixture = Fixture::pair(10_000);
    for now in [60, 70, 120, 180, 300] {
        fixture.tick(now);
        fixture.maintain(now, true);
        assert_eq!(fixture.processor.pending_recovery(), None);
        assert_eq!(fixture.selected("A"), Some(session(0, 0)));
        for slot in [0, 1] {
            assert!(!fixture.replica("A", slot).suspect);
            assert_eq!(fixture.replica("A", slot).health.accepted_changes(), 0);
        }
    }
    assert_eq!(fixture.processor.stats().renewals_granted, 0);
}

#[test]
fn queued_receive_age_suppresses_and_resets_soft_confirmation() {
    let mut fixture = Fixture::pair(10_000);
    let active = session(1, 1);
    fixture.data(active, changes("A", "7", 5), 50);
    fixture.tick(60); // First low-rate observation.
    fixture.data_at(
        active,
        json!({"event_type":"last_trade_price"}).to_string(),
        70,
        50,
    );
    fixture.tick(70); // Twenty seconds behind input receipt: do not confirm.
    fixture.maintain(70, false);
    assert_eq!(fixture.processor.pending_recovery(), None);
    assert!(!fixture.replica("A", 0).suspect);
    assert_eq!(fixture.replica("A", 1).health.accepted_changes(), 5);
    assert_eq!(
        fixture.replica("A", 1).last_valid_receive,
        Some(seconds(50))
    );

    fixture.maintain(70, true); // Explicit queue-drained evidence resumes policy.
    fixture.tick(80);
    assert_eq!(
        fixture.processor.pending_recovery(),
        None,
        "old confirmation must have been cleared"
    );
    fixture.tick(90);
    assert_eq!(
        fixture.processor.pending_recovery(),
        Some((session(0, 0), false))
    );
}

#[test]
fn backlog_does_not_suppress_hard_session_end_or_peer_selection() {
    let mut fixture = Fixture::pair(10_000);
    let ended = session(0, 0);
    let peer = session(1, 1);
    fixture.data_at(
        peer,
        json!({"event_type":"last_trade_price"}).to_string(),
        70,
        0,
    );
    fixture.process(
        Message::Ended {
            session: ended,
            reason: "fixture disconnect".into(),
        },
        70,
    );
    fixture.maintain(70, false);
    assert_eq!(fixture.replica("A", 0).state, Validity::Stale);
    assert_eq!(fixture.selected("A"), Some(peer));
    assert!(
        fixture
            .rows
            .iter()
            .any(|row| row.kind == "selection" && row.session == peer)
    );
}

#[test]
fn rejected_batch_and_stale_updates_do_not_refresh_accepted_activity() {
    let mut fixture = Fixture::pair(10_000);
    let standby = session(1, 1);
    fixture.data(standby, changes("A", "7", 2), 10);
    fixture.data(standby, snapshot("A", "7"), 20);
    assert_eq!(fixture.replica("A", 1).observed, 4); // Two snapshots, two entries.
    assert_eq!(fixture.replica("A", 1).applied, 2);
    assert_eq!(fixture.replica("A", 1).health.snapshots(), 2);
    assert_eq!(fixture.replica("A", 1).health.accepted_changes(), 2);
    fixture.data(
        standby,
        json!({"event_type":"price_change", "timestamp":"1000", "price_changes":[
            {"asset_id":"A", "side":"BUY", "price":"0.3", "size":"9"},
            {"asset_id":"A", "side":"BUY", "price":"0.3", "size":"-1"}
        ]})
        .to_string(),
        30,
    );
    assert_eq!(fixture.processor.stats().rejected, 1);
    assert_eq!(fixture.replica("A", 1).state, Validity::Stale);
    assert_eq!(
        fixture
            .replica("A", 1)
            .book
            .best(Side::Buy)
            .unwrap()
            .1
            .value(),
        7_000_000
    );
    assert_eq!(
        fixture.replica("A", 1).health.last_valid_receive(),
        Some(seconds(20))
    );
    assert_eq!(fixture.replica("A", 1).health.accepted_changes(), 2);

    fixture.data(standby, changes("A", "11", 1), 40);
    assert_eq!(fixture.replica("A", 1).observed, 5);
    assert_eq!(fixture.replica("A", 1).applied, 2);
    assert_eq!(fixture.replica("A", 1).health.accepted_changes(), 2);
    assert_eq!(
        fixture.replica("A", 1).last_valid_receive,
        Some(seconds(20))
    );
    assert_eq!(fixture.selected("A"), Some(session(0, 0)));
}

#[test]
fn prebaseline_updates_are_observed_but_not_counted_as_valid_progress() {
    let mut fixture = Fixture::new(10_000);
    let source = session(0, 0);
    fixture.reconcile(1, &["A"], &[], 0);
    fixture.start(source, &["A"], false);
    fixture.data(source, changes("A", "3", 1), 1);
    assert_eq!(fixture.replica("A", 0).observed, 1);
    assert_eq!(fixture.replica("A", 0).applied, 0);
    assert_eq!(fixture.replica("A", 0).health.accepted_changes(), 0);
    assert_eq!(fixture.replica("A", 0).health.last_valid_receive(), None);
    fixture.data(source, snapshot("A", "3"), 2);
    assert_eq!(fixture.replica("A", 0).observed, 2);
    assert_eq!(fixture.replica("A", 0).health.snapshots(), 1);
    assert_eq!(
        fixture.replica("A", 0).health.last_valid_receive(),
        Some(seconds(2))
    );
}

#[test]
fn overdue_standby_does_not_fail_or_suspend_healthy_selected_coverage() {
    let mut fixture = Fixture::new(10_000);
    let selected = session(0, 0);
    let standby = session(1, 1);
    fixture.reconcile(1, &["A"], &[], 0);
    fixture.start(selected, &["A"], true);
    fixture.start(standby, &["A"], false);
    fixture.tick(30);
    assert_eq!(fixture.processor.pending_recovery(), Some((standby, false)));
    fixture.processor.recovery_delivered(standby);
    for now in [60, 120, 180, 300] {
        fixture.tick(now);
        fixture.maintain(now, true);
        assert_eq!(fixture.selected("A"), Some(selected));
        assert_eq!(fixture.processor.stats().selected_live, 1);
        assert_eq!(fixture.processor.stats().coverage_overdue, 0);
        assert_eq!(fixture.processor.stats().degraded_replicas, 1);
    }
    fixture.data(selected, changes("A", "9", 1), 301);
    let last = fixture.rows.last().unwrap();
    assert_eq!(
        (last.kind, last.session, last.quantity),
        ("level", selected, Some(9_000_000))
    );
}

#[test]
fn asymmetric_rotation_requires_every_peer_and_blocks_overlapping_renewals() {
    let mut fixture = Fixture::new(5);
    let primary = session(0, 0); // Owns A and B; its peers live on different workers.
    let peer_a = session(1, 1);
    let peer_b = session(2, 1);
    fixture.reconcile(1, &["A", "B"], &[], 0);
    fixture.start(primary, &["A", "B"], true);
    fixture.start(peer_a, &["A"], true);
    fixture.start(peer_b, &["B"], false);
    fixture.maintain(5, true); // Only worker 0 is due: other workers have deterministic jitter.
    assert_eq!(fixture.processor.pending_recovery(), None);
    assert_eq!(fixture.processor.stats().renewals_granted, 0);
    assert!(fixture.processor.stats().renewal_deferred > 0);
    assert_eq!(fixture.selected("A"), Some(primary));
    assert_eq!(fixture.selected("B"), Some(primary));
    assert!(
        !fixture
            .rows
            .iter()
            .any(|row| row.kind == "planned-selection")
    );

    fixture.data(peer_b, snapshot("B", "7"), 5);
    fixture.ready(peer_b);
    fixture.maintain(5, true);
    assert_eq!(fixture.processor.pending_recovery(), Some((primary, true)));
    assert_eq!(fixture.selected("A"), Some(peer_a));
    assert_eq!(fixture.selected("B"), Some(peer_b));
    let switched: Vec<_> = fixture
        .rows
        .iter()
        .filter(|row| row.kind == "planned-selection")
        .map(|row| (row.asset.as_str(), row.session))
        .collect();
    assert_eq!(switched, vec![("A", peer_a), ("B", peer_b)]);

    fixture.maintain(10, true); // Both overlapping peer workers are now due too.
    assert_eq!(fixture.processor.stats().renewals_granted, 1);
    assert_eq!(fixture.processor.take_recovery(), vec![primary]);
    fixture.maintain(10, true); // Delivery is not reinitialization/reservation release.
    assert_eq!(fixture.processor.pending_recovery(), None);
    assert_eq!(fixture.processor.stats().renewals_granted, 1);
}

#[test]
fn sole_source_renewal_is_deferred_without_creating_an_outage() {
    let mut fixture = Fixture::new(5);
    let source = session(0, 0);
    fixture.reconcile(1, &["A"], &[], 0);
    fixture.start(source, &["A"], true);
    for now in [5, 10, 15] {
        fixture.maintain(now, true);
        assert_eq!(fixture.processor.pending_recovery(), None);
        assert_eq!(fixture.selected("A"), Some(source));
    }
    assert!(fixture.processor.stats().renewal_deferred > 0);
    assert_eq!(fixture.processor.stats().renewals_granted, 0);
    assert!(!fixture.rows.iter().any(|row| row.kind == "unavailable"));
}

#[test]
fn retirement_cancels_pending_planned_and_health_repairs() {
    for planned in [false, true] {
        let mut fixture = Fixture::pair(if planned { 5 } else { 10_000 });
        let cutoff = if planned {
            fixture.maintain(5, true);
            6
        } else {
            fixture.data(session(1, 1), changes("A", "7", 5), 50);
            fixture.tick(60);
            fixture.tick(70);
            71
        };
        assert_eq!(
            fixture.processor.pending_recovery(),
            Some((session(0, 0), planned))
        );
        fixture.reconcile(2, &[], &["A"], cutoff);
        assert_eq!(fixture.processor.pending_recovery(), None);
        assert_eq!(fixture.processor.requested().count(), 0);
        assert_eq!(fixture.selected("A"), None);
        for slot in [0, 1] {
            assert_eq!(fixture.replica("A", slot).state, Validity::Inactive);
            assert!(fixture.replica("A", slot).book.is_empty());
        }
        let rows_at_retirement = fixture.rows.len();
        fixture.tick(200); // Beyond both the delivery and reservation deadlines.
        fixture.maintain(200, true);
        assert_eq!(fixture.processor.pending_recovery(), None);
        assert_eq!(fixture.rows.len(), rows_at_retirement);
    }
}

#[test]
fn request_complete_marker_does_not_replace_missing_baselines() {
    let mut fixture = Fixture::new(10_000);
    let source = session(0, 0);
    fixture.reconcile(1, &["A", "B"], &[], 0);
    fixture.start(source, &["A", "B"], false); // Marker exists, neither baseline does.
    fixture.data(source, snapshot("A", "3"), 1);
    assert!(fixture.processor.initialized_sessions().next().is_none());
    fixture.data(source, snapshot("B", "3"), 2);
    fixture.ready(source);
}

#[test]
fn valid_baselines_without_request_complete_do_not_release_initialization() {
    let mut fixture = Fixture::new(10_000);
    let source = session(0, 0);
    fixture.reconcile(1, &["A", "B"], &[], 0);
    fixture.ordinals.insert(source.worker, 0);
    fixture.process(Message::Started(source), 0);
    fixture.process(
        Message::SubscriptionsRequested {
            session: source,
            revisions: vec![1, 1],
            add: vec!["A".into(), "B".into()],
            remove: vec![],
        },
        0,
    );
    fixture.data(source, snapshot("A", "3"), 1);
    fixture.data(source, snapshot("B", "3"), 2);
    assert!(fixture.processor.initialized_sessions().next().is_none());
    fixture.process(Message::RequestsComplete { session: source }, 2);
    fixture.ready(source);
}

#[test]
fn lifecycle_hints_are_bounded_namespaced_and_never_change_book_desire() {
    let mut fixture = Fixture::pair(10_000);
    let hint = r#"{"event_type":"new_market","id":"0007","market":"wrong-condition"}"#;
    fixture.data(session(0, 0), hint.into(), 1);
    fixture.data(session(1, 1), hint.into(), 1);
    let mut hints = Vec::new();
    while let Some(key) = fixture.processor.pending_hint().cloned() {
        hints.push(key.clone());
        fixture.processor.hint_delivered(&key);
    }
    hints.sort();
    assert_eq!(
        hints,
        [AdapterId::PolymarketV1, AdapterId::PolymarketV2].map(|adapter| {
            crate::discovery::MarketKey {
                adapter,
                id: "0007".into(),
            }
        })
    );
    for id in 0..600 {
        fixture.data(
            session(0, 0),
            json!({"event_type":"market_resolved","id":id.to_string()}).to_string(),
            2,
        );
    }
    let mut retained = 0;
    while let Some(key) = fixture.processor.pending_hint().cloned() {
        retained += 1;
        fixture.processor.hint_delivered(&key);
    }
    assert_eq!(retained, 1024);
    assert!(fixture.processor.reconciliation_hint());
    assert_eq!(fixture.processor.stats().lifecycle_hints_dropped, 176);
    assert_eq!(fixture.processor.stats().desired_books, 1);
    assert_eq!(fixture.selected("A"), Some(session(0, 0)));
    assert_eq!(fixture.replica("A", 0).health.accepted_changes(), 0);
    fixture.processor.reconciliation_hint_delivered();
    assert!(!fixture.processor.reconciliation_hint());
}

#[test]
fn hundred_churn_cycles_retain_identity_but_release_books_and_repair_work() {
    let mut processor = Processor::new(Config {
        selection: SelectionPolicy::Automatic,
        instruments: 100,
        replicas: 200,
        sessions: 2,
        total_levels: 4,
        ..Config::default()
    })
    .unwrap();
    let mut snapshots = 0;
    let mut sink = |o: Observation<'_>| {
        if matches!(o.kind, ObservationKind::Snapshot { .. }) {
            snapshots += 1;
        }
        Ok(())
    };
    for cycle in 0..100u64 {
        let now = seconds(cycle);
        let asset = format!("opaque-{cycle:03}");
        for (revision, add, remove) in [
            (cycle * 2 + 1, vec![asset.clone()], vec![]),
            (cycle * 2 + 2, vec![], vec![asset.clone()]),
        ] {
            processor
                .process(
                    &Message::ReconcileBegin {
                        revision,
                        chunks: 1,
                    },
                    now,
                    &mut sink,
                )
                .unwrap();
            processor
                .process(
                    &Message::DesiredChunk {
                        adapter: ADAPTER,
                        revision,
                        index: 0,
                        add,
                        remove,
                    },
                    now,
                    &mut sink,
                )
                .unwrap();
            processor
                .process(&Message::ReconcileComplete { revision }, now, &mut sink)
                .unwrap();
            if revision % 2 == 1 {
                for slot in 0..2 {
                    let source = Session {
                        worker: slot as usize,
                        slot,
                        generation: cycle + 1,
                        adapter: ADAPTER,
                    };
                    processor
                        .process(&Message::Started(source), now, &mut sink)
                        .unwrap();
                    processor
                        .process(
                            &Message::SubscriptionsRequested {
                                session: source,
                                revisions: vec![revision],
                                add: vec![asset.clone()],
                                remove: vec![],
                            },
                            now,
                            &mut sink,
                        )
                        .unwrap();
                    processor
                        .process(
                            &Message::Data {
                                session: source,
                                ordinal: 1,
                                received_at: Utc.timestamp_opt(100, 0).unwrap(),
                                received_mono: Instant::now(),
                                payload: snapshot(&asset, if slot == 0 { "3" } else { "7" }).into(),
                            },
                            now,
                            &mut sink,
                        )
                        .unwrap();
                }
            }
        }
        for slot in 0..2 {
            processor
                .process(
                    &Message::Ended {
                        session: Session {
                            worker: slot as usize,
                            slot,
                            generation: cycle + 1,
                            adapter: ADAPTER,
                        },
                        reason: "retired".into(),
                    },
                    now,
                    &mut sink,
                )
                .unwrap();
        }
        processor.tick(now).unwrap();
        assert_eq!(processor.books.len(), cycle as usize + 1);
        assert_eq!(processor.replicas, (cycle as usize + 1) * 2);
        assert_eq!(processor.total_levels, 0);
        assert_eq!(processor.stats().desired_books, 0);
        assert_eq!(processor.requested().count(), 0);
        assert!(processor.pending_recovery().is_none());
    }
    processor.finish().unwrap();
    assert_eq!(snapshots, 100);
}

#[test]
fn identity_map_is_reserved_once_up_to_the_bound() {
    let instruments = 5000;
    let mut processor = Processor::new(Config {
        instruments,
        ..Default::default()
    })
    .unwrap();
    processor.instrument_id(ADAPTER, "00000").unwrap();
    let reserved = processor.by_adapter[&ADAPTER].capacity();
    assert!(reserved >= instruments);
    for i in 1..instruments {
        processor
            .instrument_id(ADAPTER, &format!("{i:05}"))
            .unwrap();
    }
    let map = &processor.by_adapter[&ADAPTER];
    // No resize (and so no rehash of every identity) happened on the way.
    assert_eq!(map.capacity(), reserved);
    // Exact identities, leading zeros preserved, and the bound still enforced.
    assert_eq!(map["00042"], BookId(42));
    assert!(processor.instrument_id(ADAPTER, "extra").is_err());
}
