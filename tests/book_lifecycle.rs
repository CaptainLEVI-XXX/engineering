use chrono::{TimeZone, Utc};
use engineering_challenge::{
    discovery::AdapterId,
    orderbook::{Config, ObservationKind, Processor, Validity},
    websocket::ingress::{Message, Session},
};
use std::time::Duration;

fn session(worker: usize, generation: u64) -> Session {
    Session {
        worker,
        generation,
        adapter: AdapterId::PolymarketV1,
    }
}
fn request(session: Session, slot: u16, revision: u64, add: bool) -> Message {
    Message::SubscriptionsRequested {
        session,
        source_slot: slot,
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
        payload: payload.into(),
    }
}
fn apply(p: &mut Processor, message: Message, seconds: u64) {
    p.process(&message, Duration::from_secs(seconds), &mut |_| Ok(()))
        .unwrap();
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
            let message = match actor {
                0 => old.next().unwrap(),
                1 => new.next().unwrap(),
                _ => Message::Retired {
                    worker: a.worker,
                    adapter: a.adapter,
                    revision: 2,
                    assets: vec!["A".into()],
                },
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
                assert_eq!(o.binding.slot, 0);
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
        initialization_timeout: Duration::from_secs(2),
        recovery_timeout: Duration::from_secs(9),
        recovery_attempts: 2,
        ..Config::default()
    };
    let mut p = Processor::new(config.clone()).unwrap();
    let a = session(0, 1);
    apply(&mut p, Message::Started(a), 0);
    apply(&mut p, request(a, 0, 1, true), 0);
    apply(&mut p, data(a, 1, false, "8"), 1);
    assert!(p.replica(a.adapter, "A", 0).unwrap().book.is_empty());
    p.tick(Duration::from_secs(2)).unwrap();
    assert_eq!(p.take_recovery(), vec![a]);
    apply(&mut p, request(a, 0, 1, true), 3);
    p.tick(Duration::from_secs(3)).unwrap();
    assert!(p.take_recovery().is_empty());
    apply(
        &mut p,
        Message::Ended {
            session: a,
            reason: "restart".into(),
        },
        3,
    );
    let b = session(0, 2);
    apply(&mut p, Message::Started(b), 4);
    apply(&mut p, request(b, 0, 1, true), 4);
    p.tick(Duration::from_secs(6)).unwrap();
    assert_eq!(p.take_recovery(), vec![b]);
    apply(
        &mut p,
        Message::Ended {
            session: b,
            reason: "restart".into(),
        },
        6,
    );
    let c = session(0, 3);
    apply(&mut p, Message::Started(c), 6);
    apply(&mut p, request(c, 0, 1, true), 6);
    assert!(
        p.tick(Duration::from_secs(8))
            .unwrap_err()
            .to_string()
            .contains("attempts")
    );

    let mut p = Processor::new(config).unwrap();
    apply(&mut p, Message::Started(a), 0);
    apply(&mut p, request(a, 0, 1, true), 0);
    apply(&mut p, data(a, 1, false, "bad"), 8);
    assert!(
        p.tick(Duration::from_secs(9))
            .unwrap_err()
            .to_string()
            .contains("deadline")
    );
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
            if let engineering_challenge::orderbook::SnapshotOrigin::Selection { at: selected_at } =
                origin
            {
                assert_eq!(selected_at, at);
                assert_eq!(o.input, 8);
            }
            snapshots += 1;
        }
        Ok(())
    };
    for (slot, s) in [(0, session(0, 1)), (1, session(1, 1))] {
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
    for s in [session(0, 1), session(1, 1)] {
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
    p.process(
        &data(session(1, 1), 2, false, "19"),
        Duration::from_secs(1),
        &mut |o| {
            if let ObservationKind::Level { quantity, .. } = o.kind {
                quantities.push(quantity.value());
            }
            Ok(())
        },
    )
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
    apply(
        &mut p,
        Message::Retired {
            worker: 0,
            adapter: s.adapter,
            revision: 2,
            assets: vec!["A".into()],
        },
        2,
    );
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
    let b = session(1, 1);
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
            source_slot: 0,
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
            payload:
                r#"{"event_type":"book","asset_id":"A0","timestamp":"40","bids":[],"asks":[]}"#
                    .into(),
        },
        Duration::from_secs(59),
        &mut |_| Ok(()),
    )
    .unwrap();
    p.process(
        &Message::Retired {
            worker: 0,
            adapter: s.adapter,
            revision: 2,
            assets: vec!["A1".into()],
        },
        Duration::from_secs(59),
        &mut |_| Ok(()),
    )
    .unwrap();
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
