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
        if matches!(o.kind, ObservationKind::Snapshot(_)) {
            let metadata = o.metadata.unwrap();
            assert_eq!(metadata.exchange_us, Some(10000));
            assert_eq!(metadata.received_at, Utc.timestamp_opt(100, 0).unwrap());
            if o.selected_at.is_some() {
                assert_eq!(o.selected_at, Some(at));
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
        p.select(AdapterId::PolymarketV1, "A", 1, at, &mut sink)
            .is_err()
    );
    for s in [session(0, 1), session(1, 1)] {
        p.process(&data(s, 1, true, "17"), Duration::ZERO, &mut sink)
            .unwrap();
    }
    p.select(AdapterId::PolymarketV1, "A", 1, at, &mut sink)
        .unwrap();
    p.select(AdapterId::PolymarketV1, "A", 1, at, &mut sink)
        .unwrap();
    assert_eq!(snapshots, 2);
    apply(&mut p, data(session(0, 1), 2, false, "bad"), 1);
    assert!(
        p.select(AdapterId::PolymarketV1, "A", 0, at, &mut |_| Ok(()))
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
