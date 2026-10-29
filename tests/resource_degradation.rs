//! Exchange input that exceeds a configured budget degrades only its scope.
//! The process, its other books and the output stream continue.
use chrono::{TimeZone, Utc};
use engineering_challenge::{
    discovery::AdapterId,
    market_data::Limits,
    orderbook::{Config, ObservationKind, Processor, Validity},
    websocket::ingress::{Message, Session},
};
use serde_json::json;
use std::time::{Duration, Instant};

const S: Session = Session {
    worker: 0,
    generation: 1,
    adapter: AdapterId::PolymarketV1,
    slot: 0,
};

fn data(n: u64, text: serde_json::Value) -> Message {
    Message::Data {
        session: S,
        ordinal: n,
        received_at: Utc.timestamp_opt(100, 0).unwrap(),
        received_mono: Instant::now(),
        payload: text.to_string().into(),
    }
}
fn book(id: &str, levels: usize) -> serde_json::Value {
    let bids: Vec<_> = (1..=levels)
        .map(|i| json!({"price": format!("0.{i:03}"), "size": "1"}))
        .collect();
    json!({"event_type":"book","asset_id":id,"timestamp":"42","bids":bids,"asks":[]})
}
fn change(id: &str, price: &str) -> serde_json::Value {
    json!({"asset_id":id,"side":"BUY","price":price,"size":"2"})
}
/// Returns "kind:asset[:cause]" for every observation, in emission order.
fn run(p: &mut Processor, m: Message) -> Vec<String> {
    let mut trace = vec![];
    p.process(&m, Duration::ZERO, &mut |o| {
        trace.push(match o.kind {
            ObservationKind::Snapshot { .. } => format!("snapshot:{}", o.asset),
            ObservationKind::Level { .. } => format!("level:{}", o.asset),
            ObservationKind::Invalidated { cause, .. } => format!("invalid:{}:{cause:?}", o.asset),
            ObservationKind::StandbyBaseline => format!("standby:{}", o.asset),
        });
        Ok(())
    })
    .expect("budget rejection must not fail the processor");
    trace
}
fn start(config: Config) -> Processor {
    let mut p = Processor::new(config).unwrap();
    run(&mut p, Message::Started(S));
    run(
        &mut p,
        Message::SubscriptionsRequested {
            session: S,
            revisions: vec![1, 1],
            add: vec!["A".into(), "B".into()],
            remove: vec![],
        },
    );
    p
}

#[test]
fn deep_snapshot_over_aggregate_budget_quarantines_only_that_book() {
    let mut p = start(Config {
        total_levels: 10,
        ..Config::default()
    });
    assert_eq!(run(&mut p, data(1, book("A", 3))), ["snapshot:A"]);
    // A legitimate exchange book deeper than the remaining budget.
    assert_eq!(
        run(&mut p, data(2, book("B", 9))),
        ["invalid:B:ResourceLimit"]
    );
    assert_eq!(p.replica(S.adapter, "B", 0).unwrap().state, Validity::Stale);
    assert!(p.take_recovery().is_empty(), "a resent book would not fit");
    // The healthy neighbor keeps recording.
    let next =
        json!({"event_type":"price_change","timestamp":"43","price_changes":[change("A","0.001")]});
    assert_eq!(run(&mut p, data(3, next)), ["level:A"]);
    // B's deltas are not published against a book it never accepted.
    let stale =
        json!({"event_type":"price_change","timestamp":"44","price_changes":[change("B","0.001")]});
    assert!(run(&mut p, data(4, stale)).is_empty());
    // A snapshot that fits makes B live again with a visible boundary.
    assert_eq!(run(&mut p, data(5, book("B", 7))), ["snapshot:B"]);
    assert_eq!(p.replica(S.adapter, "B", 0).unwrap().state, Validity::Live);
    assert_eq!(p.stats().resource_quarantines, 1);
}

#[test]
fn quarantine_releases_levels_held_by_the_superseded_book() {
    let mut p = start(Config {
        total_levels: 8,
        ..Config::default()
    });
    run(&mut p, data(1, book("A", 4)));
    run(&mut p, data(2, book("B", 3)));
    // Replacement peak 3 + 7 + 4 exceeds 8: B is rejected and its 3 levels freed.
    assert_eq!(
        run(&mut p, data(3, book("B", 7))),
        ["invalid:B:ResourceLimit"]
    );
    assert!(p.replica(S.adapter, "B", 0).unwrap().book.is_empty());
    // 4 + 4 now fits only because the stale levels were released.
    assert_eq!(run(&mut p, data(4, book("B", 4))), ["snapshot:B"]);
}

#[test]
fn decoder_entry_limit_on_a_book_quarantines_that_asset() {
    let mut p = start(Config {
        wire: Limits {
            entries: 4,
            ..Limits::default()
        },
        ..Config::default()
    });
    run(&mut p, data(1, book("A", 2)));
    assert_eq!(
        run(&mut p, data(2, book("B", 5))),
        ["invalid:B:ResourceLimit"]
    );
    assert!(p.take_recovery().is_empty());
    assert_eq!(p.replica(S.adapter, "A", 0).unwrap().state, Validity::Live);
}

#[test]
fn unscoped_decoder_limit_invalidates_the_source_and_requests_recovery() {
    let mut p = start(Config {
        wire: Limits {
            entries: 1,
            ..Limits::default()
        },
        ..Config::default()
    });
    run(&mut p, data(1, book("A", 1)));
    run(&mut p, data(2, book("B", 1)));
    // Too many entries to even identify the affected assets.
    let wide = json!({"event_type":"price_change","timestamp":"43",
        "price_changes":[change("A","0.001"), change("B","0.001")]});
    assert_eq!(
        run(&mut p, data(3, wide)),
        ["invalid:A:ResourceLimit", "invalid:B:ResourceLimit"]
    );
    assert_eq!(p.take_recovery(), [S]);
}
