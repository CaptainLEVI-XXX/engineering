use chrono::{TimeZone, Utc};
use engineering_challenge::{
    discovery::AdapterId,
    orderbook::{Config, ObservationKind, Processor, SelectionPolicy, Validity},
    websocket::ingress::{Message, Session},
};
use std::time::{Duration, Instant};

fn session(worker: usize, generation: u64) -> Session {
    Session {
        worker,
        generation,
        adapter: AdapterId::PolymarketV1,
        slot: 0,
    }
}
fn request(session: Session, slot: u16, revision: u64, add: bool) -> Message {
    assert_eq!(session.slot, slot);
    Message::SubscriptionsRequested {
        session,
        revisions: vec![revision],
        add: if add { vec!["A".into()] } else { vec![] },
        remove: if add { vec![] } else { vec!["A".into()] },
    }
}
fn data(session: Session, ordinal: u64, snapshot: bool, quantity: &str) -> Message {
    let payload = if snapshot {
        format!(
            r#"{{"event_type":"book","asset_id":"A","timestamp":"10","bids":[{{"price":"0.3","size":"{quantity}"}}],"asks":[]}}"#
        )
    } else {
        format!(
            r#"{{"event_type":"price_change","timestamp":"9","price_changes":[{{"asset_id":"A","side":"BUY","price":"0.3","size":"{quantity}"}}]}}"#
        )
    };
    Message::Data {
        session,
        ordinal,
        received_at: Utc.timestamp_opt(100, 0).unwrap(),
        received_mono: Instant::now(),
        payload: payload.into(),
    }
}
fn apply(p: &mut Processor, message: Message, seconds: u64) {
    p.process(&message, Duration::from_secs(seconds), &mut |_| Ok(()))
        .unwrap();
}

fn reconciliation(
    adapter: AdapterId,
    revision: u64,
    add: &[&str],
    remove: &[&str],
) -> [Message; 3] {
    [
        Message::ReconcileBegin {
            revision,
            chunks: 1,
        },
        Message::DesiredChunk {
            adapter,
            revision,
            index: 0,
            add: add.iter().map(|s| (*s).to_owned()).collect(),
            remove: remove.iter().map(|s| (*s).to_owned()).collect(),
        },
        Message::ReconcileComplete { revision },
    ]
}

#[test]
fn readiness_survives_backpressure_churn_and_ignores_old_generation_acknowledgments() {
    let mut p = Processor::new(Config::default()).unwrap();
    let a = session(0, 1);
    apply(&mut p, Message::Started(a), 0);
    apply(&mut p, request(a, 0, 1, true), 0);
    apply(&mut p, Message::RequestsComplete { session: a }, 0);
    assert_eq!(p.initialized_sessions().count(), 0);
    apply(&mut p, data(a, 1, true, "3"), 0);
    for _ in 0..2 {
        // An unaccepted control must remain available on the next attempt.
        assert_eq!(p.initialized_sessions().collect::<Vec<_>>(), [a]);
    }
    p.initialization_delivered(a);
    assert_eq!(p.initialized_sessions().count(), 0);

    // Even an already initialized subscription needs a new complete marker.
    apply(&mut p, request(a, 0, 1, true), 1);
    assert_eq!(p.initialized_sessions().count(), 0);
    apply(&mut p, Message::RequestsComplete { session: a }, 1);
    assert_eq!(p.initialized_sessions().collect::<Vec<_>>(), [a]);
    apply(
        &mut p,
        Message::Ended {
            session: a,
            reason: "disconnect".into(),
        },
        1,
    );
    assert_eq!(p.initialized_sessions().count(), 0);
    let b = session(0, 2);
    apply(&mut p, Message::Started(b), 2);
    apply(&mut p, request(b, 0, 1, true), 2);
    apply(&mut p, Message::RequestsComplete { session: b }, 2);
    apply(&mut p, data(b, 1, true, "7"), 2);
    p.initialization_delivered(a);
    assert_eq!(p.initialized_sessions().collect::<Vec<_>>(), [b]);
    // A failed event before control delivery retracts readiness immediately.
    apply(&mut p, data(b, 2, false, "bad"), 3);
    assert_eq!(p.initialized_sessions().count(), 0);
}

#[test]
fn cross_worker_revisions_reject_late_removal_data_and_end() {
    let mut p = Processor::new(Config::default()).unwrap();
    let a = session(0, 9);
    let b = session(1, 1);
    for s in [a, b] {
        apply(&mut p, Message::Started(s), 0);
    }
    apply(&mut p, request(a, 0, 1, true), 0);
    apply(&mut p, data(a, 1, true, "10"), 0);
    apply(&mut p, request(b, 0, 3, true), 1);
    apply(&mut p, data(b, 1, true, "17"), 1);
    apply(&mut p, request(a, 0, 2, false), 2);
    apply(&mut p, data(a, 2, false, "99"), 2);
    apply(
        &mut p,
        Message::Ended {
            session: a,
            reason: "old source".into(),
        },
        2,
    );
    let r = p.replica(a.adapter, "A", 0).unwrap();
    assert_eq!(r.binding.session, b);
    assert_eq!(r.state, Validity::Live);
    assert_eq!(
        r.book
            .best(engineering_challenge::Side::Buy)
            .unwrap()
            .1
            .value(),
        17_000_000
    );
    assert!(p.finish().is_err()); // B has no end boundary yet.
    apply(
        &mut p,
        Message::Ended {
            session: b,
            reason: "shutdown".into(),
        },
        2,
    );
    p.finish().unwrap();
}

#[test]
fn retirement_cutoff_preserves_exact_history_across_all_valid_interleavings() {
    // Three producers: old worker, new worker, manager. Preserve each producer's
    // order. Retirement must precede dispatch of unsubscribe and readmission,
    // but neither worker's completion is ordered against the other worker.
    fn schedules(positions: [usize; 3], prefix: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if positions == [5, 3, 1] {
            out.push(prefix.clone());
            return;
        }
        for actor in 0..3 {
            if positions[actor] == [5, 3, 1][actor]
                || (actor == 0 && positions[0] == 2 && positions[2] == 0)
                || (actor == 1 && positions[2] == 0)
            {
                continue;
            }
            let mut next = positions;
            next[actor] += 1;
            prefix.push(actor);
            schedules(next, prefix, out);
            prefix.pop();
        }
    }
    let mut cases = vec![];
    schedules([0; 3], &mut vec![], &mut cases);
    assert_eq!(cases.len(), 111);
    for schedule in cases {
        let a = session(0, 9);
        let b = session(1, 1);
        let (send, receive) =
            engineering_challenge::websocket::ingress::channel(32, 1 << 20).unwrap();
        let producers = [send.clone(), send.clone(), send.clone()];
        for message in [
            Message::Started(a),
            Message::Started(b),
            request(a, 0, 1, true),
            data(a, 1, true, "10"),
        ] {
            send.send(message).unwrap();
        }
        let mut old = vec![
            data(a, 2, false, "21"),
            data(a, 3, false, "22"),
            request(a, 0, 2, false),
            data(a, 4, false, "99"),
            Message::Ended {
                session: a,
                reason: "old source".into(),
            },
        ]
        .into_iter();
        let mut new = vec![
            request(b, 0, 3, true),
            data(b, 1, true, "17"),
            data(b, 2, false, "19"),
        ]
        .into_iter();
        let cutoff = schedule.iter().position(|actor| *actor == 2).unwrap();
        let mut expected = vec![("snapshot", a, 10_000_000)];
        // Literal prices/quantities and producer prefixes, not the book engine,
        // define which old updates are eligible before the collector cutoff.
        for quantity in [21_000_000, 22_000_000].into_iter().take(cutoff) {
            expected.push(("level", a, quantity));
        }
        expected.extend([
            ("invalid", a, 0),
            ("snapshot", b, 17_000_000),
            ("level", b, 19_000_000),
        ]);
        for actor in &schedule {
            if *actor == 2 {
                // Ordered global retirement precedes the new admission fence;
                // both complete before either worker's next scheduled operation.
                for message in reconciliation(a.adapter, 2, &[], &["A"])
                    .into_iter()
                    .chain(reconciliation(a.adapter, 3, &["A"], &[]))
                {
                    producers[2].send(message).unwrap();
                }
                continue;
            }
            let message = match actor {
                0 => old.next().unwrap(),
                1 => new.next().unwrap(),
                _ => unreachable!(),
            };
            producers[*actor].send(message).unwrap();
        }
        drop(producers);
        let mut p = Processor::new(Config::default()).unwrap();
        let mut observed = vec![];
        for envelope in receive.try_iter() {
            p.process(&envelope.message, Duration::ZERO, &mut |o| {
                assert_eq!(o.asset, "A");
                assert_eq!(o.adapter, a.adapter);
                assert_eq!(o.binding.session.slot, 0);
                let value = match o.kind {
                    ObservationKind::Snapshot { book, .. } => (
                        "snapshot",
                        book.best(engineering_challenge::Side::Buy)
                            .unwrap()
                            .1
                            .value(),
                    ),
                    ObservationKind::Level {
                        side,
                        price,
                        quantity,
                    } => {
                        assert_eq!(side, engineering_challenge::Side::Buy);
                        assert_eq!(price.value(), 300_000);
                        ("level", quantity.value())
                    }
                    ObservationKind::Invalidated { .. } => ("invalid", 0),
                    ObservationKind::StandbyBaseline => return Ok(()),
                };
                observed.push((value.0, o.binding.session, value.1));
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(observed, expected, "schedule={schedule:?}");
        let replica = p.replica(a.adapter, "A", 0).unwrap();
        assert_eq!(replica.state, Validity::Live);
        assert_eq!(replica.binding.session, b);
        assert_eq!(replica.binding.revision, 3);
        assert_eq!(
            replica
                .book
                .best(engineering_challenge::Side::Buy)
                .unwrap()
                .1
                .value(),
            19_000_000
        );
        assert!(p.take_recovery().is_empty());
        apply(
            &mut p,
            Message::Ended {
                session: b,
                reason: "shutdown".into(),
            },
            1,
        );
        p.finish().unwrap();
    }
}

#[test]
fn initialization_retries_and_deadlines_survive_reconnect_and_duplicate_requests() {
    let config = Config {
        initialization_timeout: Duration::from_secs(10),
        recovery_timeout: Duration::from_secs(120),
        recovery_attempts: 2,
        ..Config::default()
    };
    let mut p = Processor::new(config.clone()).unwrap();
    let a = session(0, 1);
    apply(&mut p, Message::Started(a), 0);
    apply(&mut p, request(a, 0, 1, true), 0);
    apply(&mut p, data(a, 1, false, "8"), 1);
    assert!(p.replica(a.adapter, "A", 0).unwrap().book.is_empty());
    let mut current = a;
    let mut repairs = vec![];
    for second in 1..=360 {
        if second % 3 == 0 {
            apply(&mut p, request(current, 0, 1, true), second);
        }
        p.tick(Duration::from_secs(second)).unwrap();
        let pending = p.take_recovery();
        assert!(pending.len() <= 1, "one outstanding repair per session");
        if !pending.is_empty() {
            assert_eq!(pending, vec![current]);
            repairs.push(second);
            apply(
                &mut p,
                Message::Ended {
                    session: current,
                    reason: "repair".into(),
                },
                second,
            );
            current.generation += 1;
            apply(&mut p, Message::Started(current), second);
            apply(&mut p, request(current, 0, 1, true), second);
        }
        if second >= 130 {
            assert_eq!(p.stats().coverage_overdue, 1);
            assert_eq!(p.stats().degraded_replicas, 1);
        }
    }
    // Allow the bounded policy sweep's ten-second scheduling granularity,
    // without treating the 120-second coverage escalation as process failure.
    assert!(
        (3..=5).contains(&repairs.len()),
        "unexpected retry schedule: {repairs:?}"
    );
    for pair in repairs[2..].windows(2) {
        assert!(
            pair[1] - pair[0] >= 60,
            "duplicate requests reset cooldown: {repairs:?}"
        );
    }
    if repairs.len() >= 5 {
        assert!(repairs[4] - repairs[3] >= 120);
    }
    assert!(p.stats().max_unavailable_age >= Duration::from_secs(350));

    let mut p = Processor::new(Config {
        initialization_timeout: Duration::from_secs(2),
        recovery_timeout: Duration::from_secs(9),
        ..config
    })
    .unwrap();
    apply(&mut p, Message::Started(a), 0);
    apply(&mut p, request(a, 0, 1, true), 0);
    apply(&mut p, data(a, 1, false, "bad"), 8);
    p.tick(Duration::from_secs(9)).unwrap();
    assert_eq!(p.stats().coverage_overdue, 1);
    assert_eq!(p.stats().degraded_replicas, 1);
    assert_eq!(p.stats().max_unavailable_age, Duration::from_secs(9));
    assert_ne!(p.replica(a.adapter, "A", 0).unwrap().state, Validity::Live);
}

#[test]
fn selection_requires_live_baseline_and_equal_books_still_create_boundary() {
    let mut p = Processor::new(Config::default()).unwrap();
    let at = Utc.timestamp_opt(101, 0).unwrap();
    let mut snapshots = 0;
    let mut sink = |o: engineering_challenge::orderbook::Observation<'_>| {
        if let ObservationKind::Snapshot { origin, .. } = o.kind {
            let metadata = o.metadata.unwrap();
            assert_eq!(metadata.exchange_us, Some(10000));
            assert_eq!(metadata.received_at, Utc.timestamp_opt(100, 0).unwrap());
            if let engineering_challenge::orderbook::SnapshotOrigin::Selection {
                at: selected_at,
                ..
            } = origin
            {
                assert_eq!(selected_at, at);
                assert_eq!(o.input, 8);
            }
            snapshots += 1;
        }
        Ok(())
    };
    let a = session(0, 1);
    let b = Session {
        slot: 1,
        ..session(1, 1)
    };
    for (slot, s) in [(0, a), (1, b)] {
        p.process(&Message::Started(s), Duration::ZERO, &mut sink)
            .unwrap();
        p.process(&request(s, slot, 1, true), Duration::ZERO, &mut sink)
            .unwrap();
    }
    assert!(
        p.select(
            AdapterId::PolymarketV1,
            "A",
            1,
            Duration::ZERO,
            at,
            &mut sink
        )
        .is_err()
    );
    for s in [a, b] {
        p.process(&data(s, 1, true, "17"), Duration::ZERO, &mut sink)
            .unwrap();
    }
    p.select(
        AdapterId::PolymarketV1,
        "A",
        1,
        Duration::ZERO,
        at,
        &mut sink,
    )
    .unwrap();
    p.select(
        AdapterId::PolymarketV1,
        "A",
        1,
        Duration::ZERO,
        at,
        &mut sink,
    )
    .unwrap();
    assert_eq!(snapshots, 2);
    apply(&mut p, data(session(0, 1), 2, false, "bad"), 1);
    assert!(
        p.select(
            AdapterId::PolymarketV1,
            "A",
            0,
            Duration::from_secs(1),
            at,
            &mut |_| Ok(())
        )
        .is_err()
    );
    let mut quantities = vec![];
    p.process(&data(b, 2, false, "19"), Duration::from_secs(1), &mut |o| {
        if let ObservationKind::Level { quantity, .. } = o.kind {
            quantities.push(quantity.value());
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(quantities, vec![19000000]);
}

#[test]
fn retirement_while_disconnected_cancels_coverage_recovery() {
    let mut p = Processor::new(Config::default()).unwrap();
    let s = session(0, 1);
    apply(&mut p, Message::Started(s), 0);
    apply(&mut p, request(s, 0, 1, true), 0);
    apply(
        &mut p,
        Message::Ended {
            session: s,
            reason: "network".into(),
        },
        1,
    );
    for message in reconciliation(s.adapter, 2, &[], &["A"]) {
        apply(&mut p, message, 2);
    }
    p.tick(Duration::from_secs(200)).unwrap();
    assert_eq!(
        p.replica(s.adapter, "A", 0).unwrap().state,
        Validity::Inactive
    );
    assert!(p.take_recovery().is_empty());
}

#[test]
fn periodic_capture_uses_selected_live_state_and_actual_deadlines() {
    use engineering_challenge::orderbook::SnapshotOrigin;
    let mut p = Processor::new(Config {
        snapshot_interval: Some(Duration::from_secs(60)),
        ..Config::default()
    })
    .unwrap();
    let a = session(0, 1);
    let b = Session {
        slot: 1,
        ..session(1, 1)
    };
    for (s, slot) in [(a, 0), (b, 1)] {
        apply(&mut p, Message::Started(s), 0);
        apply(&mut p, request(s, slot, 1, true), 0);
        apply(
            &mut p,
            data(s, 1, true, if slot == 0 { "3" } else { "8" }),
            0,
        );
    }
    let mut captures = vec![];
    for millis in [59999, 60000, 119999, 120010] {
        let now = Duration::from_millis(millis);
        let wall = Utc.timestamp_opt(1000, 0).unwrap();
        p.capture_due(&mut || (now, wall), &mut |o| {
            let ObservationKind::Snapshot {
                book,
                origin: SnapshotOrigin::Periodic { at, due },
            } = o.kind
            else {
                panic!("periodic origin missing")
            };
            assert_eq!(at, wall);
            captures.push((
                due,
                book.best(engineering_challenge::Side::Buy)
                    .unwrap()
                    .1
                    .value(),
            ));
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(
        captures,
        [
            (Duration::from_secs(60), 3000000),
            (Duration::from_secs(120), 3000000)
        ]
    );
    assert_eq!(p.next_snapshot_due(), Some(Duration::from_millis(180010)));
    let mut output_losses = vec![];
    for s in [b, a] {
        p.process(
            &Message::Ended {
                session: s,
                reason: "lost".into(),
            },
            Duration::from_secs(121),
            &mut |o| {
                if let ObservationKind::Invalidated { output_binding, .. } = o.kind {
                    output_losses.push(output_binding.map(|b| b.session));
                }
                Ok(())
            },
        )
        .unwrap();
    }
    assert_eq!(output_losses, [None, Some(a)]);
    assert_eq!(p.next_snapshot_due(), None);
}

#[test]
fn retirement_fences_late_slots_and_requires_new_baselines_after_readmission() {
    for disconnect_first in [false, true] {
        let mut p = Processor::new(Config {
            selection: SelectionPolicy::Automatic,
            snapshot_interval: Some(Duration::from_secs(60)),
            ..Config::default()
        })
        .unwrap();
        let a = session(0, 1);
        let b = Session {
            slot: 1,
            ..session(1, 1)
        };
        for message in reconciliation(a.adapter, 1, &["A"], &[]) {
            apply(&mut p, message, 0);
        }
        for s in [a, b] {
            apply(&mut p, Message::Started(s), 0);
        }
        apply(&mut p, request(a, 0, 1, true), 0);
        apply(&mut p, data(a, 1, true, "3"), 0);
        if disconnect_first {
            apply(&mut p, request(b, 1, 1, true), 0);
            apply(&mut p, data(b, 1, true, "7"), 0);
            for s in [a, b] {
                apply(
                    &mut p,
                    Message::Ended {
                        session: s,
                        reason: "disconnected".into(),
                    },
                    0,
                );
            }
        }
        for message in reconciliation(a.adapter, 2, &[], &["A"]) {
            apply(&mut p, message, 0);
        }
        let fresh = if disconnect_first {
            let fresh = Session { generation: 2, ..b };
            apply(&mut p, Message::Started(fresh), 0);
            fresh
        } else {
            b
        };
        apply(&mut p, request(fresh, 1, 1, true), 0);
        assert!(
            p.replica(a.adapter, "A", 1)
                .is_none_or(|r| r.state == Validity::Inactive)
        );
        assert_eq!(
            p.replica(a.adapter, "A", 0).unwrap().state,
            Validity::Inactive
        );
        assert_eq!(p.next_snapshot_due(), None);
        assert!(p.take_recovery().is_empty());
        for message in reconciliation(a.adapter, 3, &["A"], &[]) {
            apply(&mut p, message, 0);
        }
        // An old request remains inadmissible even after desired state returns.
        apply(&mut p, request(fresh, 1, 1, true), 0);
        assert!(
            p.replica(a.adapter, "A", 1)
                .is_none_or(|r| r.state != Validity::Live)
        );
        apply(&mut p, request(fresh, 1, 3, true), 0);
        apply(&mut p, data(fresh, 1, false, "99"), 0);
        assert_eq!(
            p.replica(a.adapter, "A", 1).unwrap().state,
            Validity::AwaitingSnapshot
        );
        assert!(p.replica(a.adapter, "A", 1).unwrap().book.is_empty());
        apply(&mut p, data(fresh, 2, true, "11"), 0);
        let replica = p.replica(a.adapter, "A", 1).unwrap();
        assert_eq!(replica.binding.revision, 3);
        assert_eq!(replica.state, Validity::Live);
        assert_eq!(
            replica
                .book
                .best(engineering_challenge::Side::Buy)
                .unwrap()
                .1
                .value(),
            11_000_000
        );
    }
}

#[test]
fn reconciliation_rejects_invalid_boundaries_and_retains_accepted_cutoffs() {
    let adapter = AdapterId::PolymarketV1;
    let chunk = || Message::DesiredChunk {
        adapter,
        revision: 1,
        index: 0,
        add: vec!["A".into()],
        remove: vec![],
    };
    let mut empty = Processor::new(Config::default()).unwrap();
    apply(
        &mut empty,
        Message::ReconcileBegin {
            revision: 1,
            chunks: 0,
        },
        0,
    );
    apply(&mut empty, Message::ReconcileComplete { revision: 1 }, 0);
    assert!(empty.reconciliation().complete);
    empty.finish().unwrap();
    for invalid in [
        chunk(),
        Message::DesiredChunk {
            adapter,
            revision: 1,
            index: 2,
            add: vec![],
            remove: vec![],
        },
        Message::ReconcileComplete { revision: 1 },
        Message::ReconcileComplete { revision: 2 },
        Message::ReconcileBegin {
            revision: 2,
            chunks: 0,
        },
    ] {
        let mut p = Processor::new(Config::default()).unwrap();
        apply(
            &mut p,
            Message::ReconcileBegin {
                revision: 1,
                chunks: 2,
            },
            0,
        );
        apply(&mut p, chunk(), 0);
        assert_eq!(p.reconciliation().accepted_chunks, 1);
        assert!(!p.reconciliation().complete);
        assert!(
            p.finish().is_err(),
            "missing final boundary must remain incomplete"
        );
        assert!(
            p.process(&invalid, Duration::ZERO, &mut |_| Ok(()))
                .is_err()
        );
        assert_eq!(p.reconciliation().accepted_chunks, 1);
        assert!(!p.reconciliation().complete);
    }
}

#[test]
fn automatic_failover_publishes_latest_snapshot_before_any_standby_delta() {
    for defer in [false, true] {
        let mut p = Processor::new(Config {
            selection: SelectionPolicy::Automatic,
            ..Config::default()
        })
        .unwrap();
        let a = session(0, 1);
        let b = Session {
            slot: 1,
            ..session(1, 1)
        };
        let mut trace = vec![];
        let mut record = |o: engineering_challenge::orderbook::Observation<'_>| {
            match o.kind {
                ObservationKind::Snapshot { book, .. } => trace.push(format!(
                    "snapshot:{}:{}",
                    o.binding.session.slot,
                    book.best(engineering_challenge::Side::Buy)
                        .unwrap()
                        .1
                        .value()
                )),
                ObservationKind::Level { quantity, .. } => trace.push(format!(
                    "delta:{}:{}",
                    o.binding.session.slot,
                    quantity.value()
                )),
                ObservationKind::Invalidated {
                    output_binding: Some(_),
                    ..
                } => trace.push(format!("unavailable:{}", o.binding.session.slot)),
                ObservationKind::Invalidated {
                    output_binding: None,
                    ..
                } => {}
                ObservationKind::StandbyBaseline => {}
            }
            Ok(())
        };
        for message in [
            Message::Started(a),
            request(a, 0, 1, true),
            Message::Started(b),
            request(b, 1, 1, true),
            data(a, 1, true, "3"),
            data(b, 1, true, "7"),
            Message::Ended {
                session: a,
                reason: "lost selected source".into(),
            },
        ] {
            p.process(&message, Duration::ZERO, &mut record).unwrap();
        }
        if defer {
            p.process(&data(b, 2, false, "9"), Duration::ZERO, &mut record)
                .unwrap();
        }
        p.maintain(
            Duration::ZERO,
            Utc.timestamp_opt(101, 0).unwrap(),
            true,
            &mut record,
        )
        .unwrap();
        if !defer {
            p.process(&data(b, 2, false, "9"), Duration::ZERO, &mut record)
                .unwrap();
        }
        // Recovered slot zero must not steal selection back from the live peer.
        let recovered = Session { generation: 2, ..a };
        for message in [
            Message::Started(recovered),
            request(recovered, 0, 1, true),
            data(recovered, 1, true, "20"),
            data(recovered, 2, false, "21"),
        ] {
            p.process(&message, Duration::ZERO, &mut record).unwrap();
        }
        p.maintain(
            Duration::ZERO,
            Utc.timestamp_opt(102, 0).unwrap(),
            true,
            &mut record,
        )
        .unwrap();
        p.process(&data(b, 3, false, "5"), Duration::ZERO, &mut record)
            .unwrap();
        let mut expected = vec!["snapshot:0:3000000", "unavailable:0"];
        if defer {
            expected.push("snapshot:1:9000000");
        } else {
            expected.extend(["snapshot:1:7000000", "delta:1:9000000"]);
        }
        expected.push("delta:1:5000000");
        assert_eq!(trace, expected);
    }
}

#[test]
fn automatic_startup_from_slot_one_preserves_snapshot_then_delta_in_one_frame() {
    let mut p = Processor::new(Config {
        selection: SelectionPolicy::Automatic,
        ..Config::default()
    })
    .unwrap();
    let s = Session {
        slot: 1,
        ..session(1, 1)
    };
    apply(&mut p, Message::Started(s), 0);
    apply(&mut p, request(s, 1, 1, true), 0);
    let Message::Data {
        payload: baseline, ..
    } = data(s, 1, true, "7")
    else {
        unreachable!()
    };
    let Message::Data { payload: delta, .. } = data(s, 1, false, "5") else {
        unreachable!()
    };
    let mut frame = data(s, 1, true, "7");
    let Message::Data { payload, .. } = &mut frame else {
        unreachable!()
    };
    *payload = format!("[{baseline},{delta}]").into();
    let mut trace = vec![];
    p.process(&frame, Duration::ZERO, &mut |o| {
        assert_eq!(o.binding.session.slot, 1);
        match o.kind {
            ObservationKind::Snapshot { book, .. } => trace.push((
                "snapshot",
                book.best(engineering_challenge::Side::Buy)
                    .unwrap()
                    .1
                    .value(),
            )),
            ObservationKind::Level { quantity, .. } => trace.push(("delta", quantity.value())),
            ObservationKind::Invalidated { .. } => panic!("valid startup frame invalidated"),
            ObservationKind::StandbyBaseline => panic!("first valid snapshot must select slot one"),
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(trace, vec![("snapshot", 7_000_000), ("delta", 5_000_000)]);
}

#[test]
fn failed_automatic_switch_sink_is_terminal() {
    let mut p = Processor::new(Config {
        selection: SelectionPolicy::Automatic,
        ..Config::default()
    })
    .unwrap();
    let a = session(0, 1);
    let b = Session {
        slot: 1,
        ..session(1, 1)
    };
    for message in [
        Message::Started(a),
        request(a, 0, 1, true),
        Message::Started(b),
        request(b, 1, 1, true),
        data(a, 1, true, "3"),
        data(b, 1, true, "7"),
        Message::Ended {
            session: a,
            reason: "lost".into(),
        },
    ] {
        apply(&mut p, message, 0);
    }
    let mut attempted = 0;
    assert!(
        p.maintain(Duration::ZERO, Utc::now(), true, &mut |o| {
            assert!(matches!(o.kind, ObservationKind::Snapshot { .. }));
            assert_eq!(o.binding.session, b);
            attempted += 1;
            anyhow::bail!("synthetic output failure")
        })
        .is_err()
    );
    assert_eq!(attempted, 1);
    assert!(
        p.process(&data(b, 2, false, "9"), Duration::ZERO, &mut |_| panic!(
            "delta published after failed switch"
        ))
        .is_err()
    );
}

#[test]
fn quiet_selected_book_continues_when_standby_is_overdue() {
    let mut p = Processor::new(Config {
        selection: SelectionPolicy::Automatic,
        ..Config::default()
    })
    .unwrap();
    let a = session(0, 1);
    let b = Session {
        slot: 1,
        ..session(1, 1)
    };
    for message in [
        Message::Started(a),
        request(a, 0, 1, true),
        Message::Started(b),
        request(b, 1, 1, true),
        data(a, 1, true, "7"),
    ] {
        apply(&mut p, message, 0);
    }
    for second in 1..=130 {
        p.tick(Duration::from_secs(second)).unwrap();
        assert!(p.take_recovery().iter().all(|s| *s == b));
    }
    assert_eq!(p.stats().coverage_overdue, 0);
    assert_eq!(p.stats().selected_live, 1);
    assert_eq!(p.stats().degraded_replicas, 1);
    let mut quantities = vec![];
    p.process(
        &data(a, 2, false, "2"),
        Duration::from_secs(131),
        &mut |o| {
            if let ObservationKind::Level { quantity, .. } = o.kind {
                quantities.push(quantity.value());
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(quantities, vec![2_000_000]);
}

#[test]
fn unknown_freshness_fallback_is_explicit_but_invalid_or_ended_backups_are_ineligible() {
    use engineering_challenge::orderbook::{Freshness, SnapshotOrigin, SwitchReason};
    for backup in ["quiet", "invalid", "ended"] {
        let mut p = Processor::new(Config {
            selection: SelectionPolicy::Automatic,
            ..Config::default()
        })
        .unwrap();
        let a = session(0, 1);
        let b = Session {
            slot: 1,
            ..session(1, 1)
        };
        for message in [
            Message::Started(a),
            request(a, 0, 1, true),
            Message::Started(b),
            request(b, 1, 1, true),
            data(a, 1, true, "3"),
            data(b, 1, true, "7"),
        ] {
            apply(&mut p, message, 0);
        }
        if backup == "invalid" {
            apply(&mut p, data(b, 2, false, "bad"), 0);
        }
        if backup == "ended" {
            apply(
                &mut p,
                Message::Ended {
                    session: b,
                    reason: "standby ended".into(),
                },
                0,
            );
        }
        apply(
            &mut p,
            Message::Ended {
                session: a,
                reason: "selected ended".into(),
            },
            0,
        );
        let mut snapshots = 0;
        p.maintain(
            Duration::from_secs(20),
            Utc.timestamp_opt(120, 0).unwrap(),
            true,
            &mut |o| {
                let ObservationKind::Snapshot {
                    origin:
                        SnapshotOrigin::Selection {
                            previous,
                            reason,
                            freshness,
                            receive_age,
                            ..
                        },
                    ..
                } = o.kind
                else {
                    panic!("unexpected policy observation")
                };
                assert_eq!(backup, "quiet");
                assert_eq!(o.binding.session, b);
                assert_eq!(previous.unwrap().session, a);
                assert!(matches!(reason, SwitchReason::SourceUnavailable));
                assert!(matches!(freshness, Freshness::Unknown));
                assert_eq!(receive_age, Some(Duration::from_secs(20)));
                snapshots += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(snapshots, usize::from(backup == "quiet"));
    }
}

#[test]
fn deferred_recovery_is_deduplicated_expires_and_is_cancelled_by_retirement() {
    for retire in [false, true] {
        let mut p = Processor::new(Config::default()).unwrap();
        let s = session(0, 1);
        for message in [
            Message::Started(s),
            request(s, 0, 1, true),
            data(s, 1, false, "bad"),
        ] {
            apply(&mut p, message, 0);
        }
        for second in [1, 10, 20, 29] {
            apply(&mut p, request(s, 0, 1, true), second);
            p.tick(Duration::from_secs(second)).unwrap();
            assert_eq!(p.pending_recovery(), Some((s, false)));
        }
        if retire {
            for message in reconciliation(s.adapter, 2, &[], &["A"]) {
                apply(&mut p, message, 29);
            }
            p.tick(Duration::from_secs(60)).unwrap();
            assert!(p.pending_recovery().is_none());
        } else {
            let error = p.tick(Duration::from_secs(30)).unwrap_err();
            assert!(error.to_string().contains("recovery delivery deadline"));
        }
    }
}

#[test]
fn periodic_sink_failure_and_missed_interval_are_terminal() {
    for sink_failure in [false, true] {
        let mut p = Processor::new(Config {
            snapshot_interval: Some(Duration::from_secs(60)),
            ..Config::default()
        })
        .unwrap();
        let s = session(0, 1);
        apply(&mut p, Message::Started(s), 0);
        apply(&mut p, request(s, 0, 1, true), 0);
        apply(&mut p, data(s, 1, true, "0"), 0);
        let now = Duration::from_secs(if sink_failure { 60 } else { 120 });
        let result = p.capture_due(&mut || (now, Utc::now()), &mut |_| {
            anyhow::bail!("output failed")
        });
        assert!(result.is_err());
        assert!(
            p.process(&data(s, 2, false, "9"), now, &mut |_| Ok(()))
                .is_err()
        );
    }
}

#[test]
fn periodic_sweeps_are_fair_and_replacement_and_retirement_cancel_old_deadlines() {
    let mut p = Processor::new(Config {
        snapshot_interval: Some(Duration::from_secs(60)),
        ..Default::default()
    })
    .unwrap();
    let s = session(0, 1);
    let assets: Vec<_> = (0..130).map(|i| format!("A{i}")).collect();
    for m in [
        Message::Started(s),
        Message::SubscriptionsRequested {
            session: s,
            revisions: vec![1; 130],
            add: assets.clone(),
            remove: vec![],
        },
    ] {
        p.process(&m, Duration::ZERO, &mut |_| Ok(())).unwrap();
    }
    let frame=serde_json::to_string(&assets.iter().map(|a| serde_json::json!({"event_type":"book","asset_id":a,"timestamp":"42","bids":[],"asks":[]})).collect::<Vec<_>>()).unwrap();
    p.process(
        &Message::Data {
            session: s,
            ordinal: 1,
            received_at: Utc::now(),
            received_mono: Instant::now(),
            payload: frame.into(),
        },
        Duration::ZERO,
        &mut |_| Ok(()),
    )
    .unwrap();
    // A replacement resets only A0's timer; retirement cancels A1's timer.
    p.process(
        &Message::Data {
            session: s,
            ordinal: 2,
            received_at: Utc::now(),
            received_mono: Instant::now(),
            payload:
                r#"{"event_type":"book","asset_id":"A0","timestamp":"40","bids":[],"asks":[]}"#
                    .into(),
        },
        Duration::from_secs(59),
        &mut |_| Ok(()),
    )
    .unwrap();
    for message in reconciliation(s.adapter, 2, &[], &["A1"]) {
        p.process(&message, Duration::from_secs(59), &mut |_| Ok(()))
            .unwrap();
    }
    let mut captures = vec![];
    for want in [64, 128] {
        p.capture_due(&mut || (Duration::from_secs(60), Utc::now()), &mut |o| {
            captures.push(o.asset.to_owned());
            Ok(())
        })
        .unwrap();
        assert_eq!(captures.len(), want);
    }
    assert!(!captures.contains(&"A0".into()));
    assert!(!captures.contains(&"A1".into()));
    assert_eq!(p.next_snapshot_due(), Some(Duration::from_secs(119)));
    p.capture_due(&mut || (Duration::from_secs(119), Utc::now()), &mut |o| {
        assert_eq!(o.asset, "A0");
        assert_eq!(o.metadata.unwrap().exchange_us, Some(40000));
        Ok(())
    })
    .unwrap();
    assert_eq!(p.next_snapshot_due(), Some(Duration::from_secs(120)));
    p.stop_snapshots(Duration::from_secs(120));
    assert_eq!(p.stats().snapshots_due_at_cutoff, 128);
    p.capture_due(&mut || (Duration::from_secs(120), Utc::now()), &mut |_| {
        panic!("shutdown backfilled an unfinished snapshot sweep")
    })
    .unwrap();
    assert_eq!(p.next_snapshot_due(), None);
}
