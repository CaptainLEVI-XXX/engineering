use chrono::{TimeZone, Utc};
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    orderbook::{Config, Observation, ObservationKind, Processor, Validity},
    websocket::ingress::{Message, Session},
};
use serde_json::json;
use std::time::Duration;

fn session(worker: usize) -> Session {
    Session {
        worker,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
    }
}
fn sink(observation: Observation<'_>, trace: &mut Vec<String>) -> anyhow::Result<()> {
    match observation.kind {
        ObservationKind::Snapshot(book) => trace.push(format!(
            "snapshot:{}:{:?}",
            observation.asset,
            book.levels(Side::Buy)
                .map(|(p, q)| (p.value(), q.value()))
                .collect::<Vec<_>>()
        )),
        ObservationKind::Level {
            side,
            price,
            quantity,
        } => trace.push(format!(
            "level:{}:{side}:{}:{}",
            observation.asset,
            price.value(),
            quantity.value()
        )),
        ObservationKind::Invalidated { .. } => trace.push(format!("invalid:{}", observation.asset)),
    }
    Ok(())
}
fn send(p: &mut Processor, m: Message, trace: &mut Vec<String>) {
    p.process(&m, Duration::ZERO, &mut |o| sink(o, trace))
        .unwrap();
}
fn subscribe(p: &mut Processor, s: Session, slot: u16, revision: u64, trace: &mut Vec<String>) {
    send(p, Message::Started(s), trace);
    send(
        p,
        Message::SubscriptionsRequested {
            session: s,
            source_slot: slot,
            revisions: vec![revision, revision],
            add: vec!["A".into(), "B".into()],
            remove: vec![],
        },
        trace,
    );
}
fn data(s: Session, n: u64, text: serde_json::Value) -> Message {
    Message::Data {
        session: s,
        ordinal: n,
        received_at: Utc.timestamp_opt(100, 0).unwrap(),
        payload: text.to_string().into(),
    }
}
fn book(id: &str, levels: serde_json::Value) -> serde_json::Value {
    json!({"event_type":"book","asset_id":id,"timestamp":"42","bids":levels,"asks":[]})
}
fn change(id: &str, price: &str, quantity: &str) -> serde_json::Value {
    json!({"asset_id":id,"side":"BUY","price":price,"size":quantity})
}

#[test]
fn replicas_replace_atomically_and_preserve_selected_intermediate_history() {
    let mut p = Processor::new(Config::default()).unwrap();
    let mut trace = vec![];
    let a = session(0);
    let b = session(1);
    subscribe(&mut p, a, 0, 1, &mut trace);
    subscribe(&mut p, b, 1, 1, &mut trace);
    send(
        &mut p,
        data(
            a,
            1,
            book(
                "A",
                json!([{"price":"0.4","size":"10"},{"price":"0.3","size":"7"}]),
            ),
        ),
        &mut trace,
    );
    send(
        &mut p,
        data(b, 1, book("A", json!([{"price":"0.4","size":"17"}]))),
        &mut trace,
    );
    trace.clear();
    send(
        &mut p,
        data(
            a,
            2,
            json!({"event_type":"price_change","timestamp":"41","price_changes":[change("A","0.4","20"),change("A","0.4","30"),change("A","0.4","30")]}),
        ),
        &mut trace,
    );
    assert_eq!(
        trace,
        vec![
            "level:A:Buy:400000:20000000",
            "level:A:Buy:400000:30000000",
            "level:A:Buy:400000:30000000"
        ]
    );
    p.select(
        AdapterId::PolymarketV1,
        "A",
        1,
        Utc.timestamp_opt(101, 0).unwrap(),
        &mut |o| sink(o, &mut trace),
    )
    .unwrap();
    assert_eq!(trace.last().unwrap(), "snapshot:A:[(400000, 17000000)]");
    send(
        &mut p,
        data(a, 3, book("A", json!([{"price":"0.4","size":"5"}]))),
        &mut trace,
    );
    let r = p.replica(AdapterId::PolymarketV1, "A", 0).unwrap();
    assert_eq!(r.book.levels(Side::Buy).count(), 1);
    assert_eq!(r.state, Validity::Live);
    assert_eq!(trace.last().unwrap(), "snapshot:A:[(400000, 17000000)]");
}

#[test]
fn malformed_event_invalidates_its_source_but_independent_event_can_commit() {
    let mut p = Processor::new(Config::default()).unwrap();
    let mut trace = vec![];
    let a = session(0);
    let b = session(1);
    subscribe(&mut p, a, 0, 1, &mut trace);
    subscribe(&mut p, b, 1, 1, &mut trace);
    for s in [a, b] {
        send(
            &mut p,
            data(
                s,
                1,
                json!([
                    book("A", json!([{"price":"0.4","size":"10"}])),
                    book("B", json!([]))
                ]),
            ),
            &mut trace,
        );
    }
    trace.clear();
    send(
        &mut p,
        data(
            a,
            2,
            json!([{"event_type":"price_change","timestamp":"43","price_changes":[change("A","0.4","20"),change("B","0.2","bad")]},book("B",json!([{"price":"0.2","size":"9"}]))]),
        ),
        &mut trace,
    );
    let r = p.replica(AdapterId::PolymarketV1, "A", 0).unwrap();
    assert_eq!(r.state, Validity::Stale);
    assert_eq!(r.book.levels(Side::Buy).next().unwrap().1.value(), 10000000);
    assert_eq!(
        p.replica(AdapterId::PolymarketV1, "A", 1).unwrap().state,
        Validity::Live
    );
    assert_eq!(
        p.replica(AdapterId::PolymarketV1, "B", 0).unwrap().state,
        Validity::Live
    );
    assert!(!trace.iter().any(|s| s.starts_with("level:")));
    assert_eq!(p.take_recovery(), vec![a]);
}

#[test]
fn budget_preflight_rejects_intermediate_peak_and_sink_failure_is_terminal() {
    let mut p = Processor::new(Config {
        levels_per_side: 1,
        ..Config::default()
    })
    .unwrap();
    let mut trace = vec![];
    let s = session(0);
    subscribe(&mut p, s, 0, 1, &mut trace);
    send(
        &mut p,
        data(s, 1, book("A", json!([{"price":"0.4","size":"10"}]))),
        &mut trace,
    );
    let result=p.process(&data(s,2,json!({"event_type":"price_change","timestamp":"44","price_changes":[change("A","0.3","2"),change("A","0.4","0")]})),Duration::ZERO,&mut |o|sink(o,&mut trace));
    assert!(result.is_err());
    assert_eq!(
        p.replica(AdapterId::PolymarketV1, "A", 0)
            .unwrap()
            .book
            .levels(Side::Buy)
            .next()
            .unwrap()
            .0
            .value(),
        400000
    );
    let mut p = Processor::new(Config::default()).unwrap();
    subscribe(&mut p, s, 0, 1, &mut trace);
    assert!(
        p.process(
            &data(s, 1, book("A", json!([]))),
            Duration::ZERO,
            &mut |_| anyhow::bail!("sink failed")
        )
        .is_err()
    );
    assert!(
        p.process(
            &data(s, 2, book("A", json!([]))),
            Duration::ZERO,
            &mut |_| Ok(())
        )
        .is_err()
    );
}

#[test]
fn generated_traces_match_independent_linear_oracle_after_every_event() {
    // Fixed seeds make the generated valid workload reproducible. The oracle
    // uses integer operations and linear collections, not production decoding.
    for seed in [7u64, 91, 1023] {
        let mut p = Processor::new(Config::default()).unwrap();
        let mut trace = vec![];
        let s = session(0);
        subscribe(&mut p, s, 0, 1, &mut trace);
        let mut model: [Vec<(Side, i64, i64)>; 2] = Default::default();
        let mut rng = seed;
        let mut next = || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng >> 32
        };
        for ordinal in 1..=1000 {
            let mut expected = vec![];
            let event = if ordinal == 1 || ordinal % 37 == 0 {
                let mut snapshots = vec![];
                for (i, id) in ["A", "B"].into_iter().enumerate() {
                    model[i].clear();
                    let q = (next() % 17 + 1) as i64;
                    model[i].push((Side::Buy, 300000, q * 1_000_000));
                    snapshots.push(book(id, json!([{"price":"0.3","size":q.to_string()}])));
                    expected.push(format!("snapshot:{id}:[(300000, {})]", q * 1_000_000));
                }
                json!(snapshots)
            } else {
                let mut changes = vec![];
                for _ in 0..7 {
                    let i = (next() % 2) as usize;
                    let id = ["A", "B"][i];
                    let side = if next() % 2 == 0 {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let price = (next() % 23 + 1) as i64 * 10000;
                    let quantity = (next() % 13) as i64 * 1_000_000;
                    model[i].retain(|(s, p, _)| (*s, *p) != (side, price));
                    if quantity != 0 {
                        model[i].push((side, price, quantity));
                    }
                    changes.push(json!({"asset_id":id,"side":if side == Side::Buy {"BUY"} else {"SELL"},"price":format!("0.{price:06}"),"size":(quantity/1_000_000).to_string()}));
                    expected.push(format!("level:{id}:{side}:{price}:{quantity}"));
                }
                json!({"event_type":"price_change","timestamp":"39","price_changes":changes})
            };
            trace.clear();
            send(&mut p, data(s, ordinal, event), &mut trace);
            assert_eq!(trace, expected, "seed={seed} ordinal={ordinal}");
            for (i, id) in ["A", "B"].into_iter().enumerate() {
                for side in [Side::Buy, Side::Sell] {
                    let mut expected: Vec<_> = model[i]
                        .iter()
                        .filter(|(s, _, _)| *s == side)
                        .map(|(_, p, q)| (*p, *q))
                        .collect();
                    expected.sort_unstable();
                    if side == Side::Buy {
                        expected.reverse();
                    }
                    let actual: Vec<_> = p
                        .replica(s.adapter, id, 0)
                        .unwrap()
                        .book
                        .levels(side)
                        .map(|(p, q)| (p.value(), q.value()))
                        .collect();
                    assert_eq!(
                        actual, expected,
                        "seed={seed} ordinal={ordinal} asset={id} side={side}"
                    );
                }
            }
        }
    }
}

#[test]
fn invalid_snapshot_and_inactive_peer_have_different_failure_scopes() {
    let mut p = Processor::new(Config::default()).unwrap();
    let mut trace = vec![];
    let s = session(0);
    subscribe(&mut p, s, 0, 1, &mut trace);
    send(
        &mut p,
        data(s, 1, book("A", json!([{"price":"0.4","size":"10"}]))),
        &mut trace,
    );
    send(
        &mut p,
        Message::SubscriptionsRequested {
            session: s,
            source_slot: 0,
            revisions: vec![2],
            add: vec![],
            remove: vec!["B".into()],
        },
        &mut trace,
    );
    trace.clear();
    send(
        &mut p,
        data(
            s,
            2,
            json!({"event_type":"price_change","timestamp":"40","price_changes":[change("A","0.4","11"),change("B","0.4","7")]}),
        ),
        &mut trace,
    );
    assert_eq!(trace, vec!["level:A:Buy:400000:11000000"]);
    send(
        &mut p,
        data(
            s,
            3,
            book(
                "A",
                json!([{"price":"0.4","size":"1"},{"price":"0.40","size":"2"}]),
            ),
        ),
        &mut trace,
    );
    let r = p.replica(s.adapter, "A", 0).unwrap();
    assert_eq!(r.state, Validity::Stale);
    assert_eq!(r.book.best(Side::Buy).unwrap().1.value(), 11000000);
    send(&mut p, data(s, 4, book("A", json!([]))), &mut trace);
    assert!(p.replica(s.adapter, "A", 0).unwrap().book.is_empty());
    assert_eq!(p.replica(s.adapter, "A", 0).unwrap().state, Validity::Live);
}

#[test]
fn replacement_budget_includes_old_and_new_books() {
    for limit in [2, 3, 4] {
        let mut p = Processor::new(Config {
            total_levels: limit,
            ..Config::default()
        })
        .unwrap();
        let s = session(0);
        let mut trace = vec![];
        subscribe(&mut p, s, 0, 1, &mut trace);
        send(
            &mut p,
            data(s, 1, book("A", json!([{"price":"0.4","size":"10"}]))),
            &mut trace,
        );
        let replacement = data(
            s,
            2,
            book(
                "A",
                json!([{"price":"0.2","size":"3"},{"price":"0.3","size":"7"}]),
            ),
        );
        let result = p.process(&replacement, Duration::ZERO, &mut |o| sink(o, &mut trace));
        let levels: Vec<_> = p
            .replica(s.adapter, "A", 0)
            .unwrap()
            .book
            .levels(Side::Buy)
            .map(|(p, q)| (p.value(), q.value()))
            .collect();
        if limit == 2 {
            assert!(result.is_err());
            assert_eq!(levels, vec![(400000, 10000000)]);
        } else {
            result.unwrap();
            assert_eq!(levels, vec![(300000, 7000000), (200000, 3000000)]);
        }
    }
}
