use chrono::{TimeZone, Utc};
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    orderbook::{Config, Observation, ObservationKind, Processor, Validity},
    websocket::ingress::{Message, Session},
};
use serde_json::json;
use std::time::{Duration, Instant};

fn session(worker: usize) -> Session {
    Session {
        worker,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
        slot: u16::try_from(worker).unwrap(),
    }
}
fn sink(observation: Observation<'_>, trace: &mut Vec<String>) -> anyhow::Result<()> {
    match observation.kind {
        ObservationKind::Snapshot { book, .. } => trace.push(format!(
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
        ObservationKind::StandbyBaseline => {}
    }
    Ok(())
}
fn send(p: &mut Processor, m: Message, trace: &mut Vec<String>) {
    p.process(&m, Duration::ZERO, &mut |o| sink(o, trace))
        .unwrap();
}
fn subscribe(p: &mut Processor, s: Session, slot: u16, revision: u64, trace: &mut Vec<String>) {
    assert_eq!(s.slot, slot);
    send(p, Message::Started(s), trace);
    send(
        p,
        Message::SubscriptionsRequested {
            session: s,
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
        received_mono: Instant::now(),
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
        Duration::ZERO,
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
fn budget_preflight_quarantines_intermediate_peak_and_sink_failure_is_terminal() {
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
    send(
        &mut p,
        data(s, 2, book("B", json!([{"price":"0.2","size":"5"}]))),
        &mut trace,
    );
    trace.clear();
    // The final state fits, but the intermediate peak does not: nothing from
    // the event is applied or emitted, and only A degrades.
    let result=p.process(&data(s,3,json!({"event_type":"price_change","timestamp":"44","price_changes":[change("A","0.3","2"),change("A","0.4","0")]})),Duration::ZERO,&mut |o|sink(o,&mut trace));
    result.unwrap();
    assert_eq!(trace, ["invalid:A"]);
    let a = p.replica(AdapterId::PolymarketV1, "A", 0).unwrap();
    assert_eq!(a.state, Validity::Stale);
    assert!(a.book.is_empty(), "quarantined levels release budget");
    assert_eq!(p.stats().resource_quarantines, 1);
    assert!(
        p.take_recovery().is_empty(),
        "a resend would not fit either"
    );
    // The neighbor keeps recording, and A recovers on a snapshot that fits.
    trace.clear();
    send(
        &mut p,
        data(
            s,
            4,
            json!({"event_type":"price_change","timestamp":"45","price_changes":[change("B","0.2","6")]}),
        ),
        &mut trace,
    );
    send(
        &mut p,
        data(s, 5, book("A", json!([{"price":"0.3","size":"2"}]))),
        &mut trace,
    );
    assert_eq!(
        trace,
        [
            "level:B:Buy:200000:6000000",
            "snapshot:A:[(300000, 2000000)]"
        ]
    );
    assert_eq!(
        p.replica(AdapterId::PolymarketV1, "A", 0).unwrap().state,
        Validity::Live
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

    // Zero-sized entries still participate in duplicate validation, on either
    // side and even when the equal prices are separated in the wire order.
    let mut ordinal = 5;
    for side in ["bids", "asks"] {
        for sizes in [["0", "7"], ["7", "0"], ["0", "0"], ["3", "7"]] {
            send(
                &mut p,
                data(s, ordinal, book("A", json!([{"price":"0.4","size":"11"}]))),
                &mut trace,
            );
            ordinal += 1;
            trace.clear();
            let mut invalid = book("A", json!([]));
            invalid[side] = json!([
                {"price":"0.4","size":sizes[0]},
                {"price":"0.2","size":"8"},
                {"price":"0.40","size":sizes[1]}
            ]);
            send(&mut p, data(s, ordinal, invalid), &mut trace);
            ordinal += 1;
            let replica = p.replica(s.adapter, "A", 0).unwrap();
            assert_eq!(replica.state, Validity::Stale);
            assert_eq!(replica.book.best(Side::Buy).unwrap().1.value(), 11000000);
            assert!(replica.book.best(Side::Sell).is_none());
            assert_eq!(trace, ["invalid:A"], "no partial snapshot may escape");
        }
    }
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
            result.unwrap();
            assert_eq!(trace.last().unwrap(), "invalid:A");
            assert_eq!(p.replica(s.adapter, "A", 0).unwrap().state, Validity::Stale);
            assert!(
                levels.is_empty(),
                "superseded book is released, never kept live"
            );
        } else {
            result.unwrap();
            assert_eq!(levels, vec![(300000, 7000000), (200000, 3000000)]);
        }
    }
}

#[test]
fn occupancy_limits_match_ordered_array_oracle_for_repeated_multi_asset_changes() {
    // Include positive replacement, absent-zero, remove/reinsert, both sides,
    // and competing assets. Exhaust all length-four combinations. The oracle
    // recounts a tiny array after EVERY entry, independently of C4 scratch maps.
    let operations = [
        (0, 0, 0, 0),
        (0, 0, 0, 2),
        (0, 0, 1, 3),
        (1, 1, 0, 0),
        (1, 1, 0, 4),
        (1, 0, 0, 5),
        (0, 1, 1, 0),
    ];
    for (side_limit, total_limit) in [(1, 2), (2, 2), (2, 3), (2, 4)] {
        for case in 0..7_usize.pow(4) {
            let mut p = Processor::new(Config {
                levels_per_side: side_limit,
                total_levels: total_limit,
                ..Config::default()
            })
            .unwrap();
            let s = session(0);
            let mut trace = vec![];
            subscribe(&mut p, s, 0, 1, &mut trace);
            send(
                &mut p,
                data(s, 1, book("A", json!([{"price":"0.1","size":"7"}]))),
                &mut trace,
            );
            let mut b = book("B", json!([]));
            b["asks"] = json!([{"price":"0.1","size":"11"}]);
            send(&mut p, data(s, 2, b), &mut trace);
            trace.clear();
            let initial = [[[7, 0], [0, 0]], [[0, 0], [11, 0]]];
            let mut expected = initial;
            let mut accepted = true;
            let mut inputs = Vec::new();
            let mut outputs = Vec::new();
            let mut code = case;
            for _ in 0..4 {
                let (asset, side, price, quantity) = operations[code % 7];
                code /= 7;
                let name = ["A", "B"][asset];
                inputs.push(json!({"asset_id":name,"side":(["BUY","SELL"][side]),
                    "price":(["0.1","0.2"][price]),"size":quantity.to_string()}));
                expected[asset][side][price] = quantity;
                accepted &= expected.iter().all(|book| {
                    book.iter()
                        .all(|levels| levels.iter().filter(|q| **q != 0).count() <= side_limit)
                });
                accepted &= expected
                    .iter()
                    .flatten()
                    .flatten()
                    .filter(|q| **q != 0)
                    .count()
                    <= total_limit;
                outputs.push(format!(
                    "level:{name}:{}:{}:{}",
                    [Side::Buy, Side::Sell][side],
                    (price + 1) * 100_000,
                    quantity * 1_000_000
                ));
            }
            let result = p.process(
                &data(
                    s,
                    3,
                    json!({"event_type":"price_change",
                "timestamp":"44","price_changes":inputs}),
                ),
                Duration::ZERO,
                &mut |o| sink(o, &mut trace),
            );
            result
                .unwrap_or_else(|e| panic!("case {case}, limits {side_limit}/{total_limit}: {e}"));
            if accepted {
                assert_eq!(
                    trace, outputs,
                    "every no-op and intermediate value must be emitted"
                );
            } else {
                // No partial output: every book the event names is quarantined
                // and empty; any book it does not name is untouched.
                expected = initial;
                let mut invalid = Vec::new();
                for (asset, name) in ["A", "B"].iter().enumerate() {
                    if inputs.iter().any(|i| i["asset_id"] == *name) {
                        expected[asset] = [[0, 0], [0, 0]];
                        invalid.push(format!("invalid:{name}"));
                    }
                }
                assert_eq!(
                    trace, invalid,
                    "case {case}, limits {side_limit}/{total_limit}"
                );
            }
            for (asset, name) in ["A", "B"].iter().enumerate() {
                let replica = p.replica(s.adapter, name, 0).unwrap();
                for (side, direction) in [Side::Buy, Side::Sell].iter().enumerate() {
                    let mut actual: Vec<_> = replica
                        .book
                        .levels(*direction)
                        .map(|(price, quantity)| (price.value(), quantity.value()))
                        .collect();
                    actual.sort_unstable();
                    let levels: Vec<_> = expected[asset][side]
                        .iter()
                        .enumerate()
                        .filter(|(_, q)| **q != 0)
                        .map(|(price, quantity)| {
                            (((price + 1) * 100_000) as i64, quantity * 1_000_000)
                        })
                        .collect();
                    assert_eq!(actual, levels, "case {case}, {name}/{direction}");
                }
            }
        }
    }
}

#[test]
fn polymarket_normalization_remains_absolute_and_probability_scoped() {
    use engineering_challenge::market_data::{self, Event, Limits, Price, QuantityChange};
    let limits = Limits::default();
    let raw = serde_json::value::RawValue::from_string(
        json!({
            "event_type":"price_change","timestamp":"42",
            "price_changes":[change("A","0.4","5"),change("A","0.4","8")]
        })
        .to_string(),
    )
    .unwrap();
    let Event::Changes {
        exchange_us,
        changes,
    } = market_data::decode(&raw, &limits).unwrap()
    else {
        panic!("expected normalized changes")
    };
    assert_eq!(exchange_us, Some(42_000));
    let absolute: Vec<_> = changes
        .iter()
        .map(|change| match change.quantity {
            QuantityChange::Set(quantity) => quantity.value(),
            QuantityChange::Add(_) => panic!("Polymarket absolute size became relative"),
        })
        .collect();
    assert_eq!(absolute, vec![5_000_000, 8_000_000]);
    assert_eq!(Price::new(2_000_000).unwrap().value(), 2_000_000);
    assert!(Price::new(-1).is_err());
    for invalid in [
        book("A", json!([{"price":"1.000001","size":"7"}])),
        json!({"event_type":"price_change","timestamp":"42","price_changes":[change("A","1.000001","7")]}),
    ] {
        let raw = serde_json::value::RawValue::from_string(invalid.to_string()).unwrap();
        assert!(market_data::decode(&raw, &limits).is_err());
    }
}

#[test]
fn signed_polymarket_size_rejects_the_whole_event_without_partial_mutation() {
    for signed in ["-2", "+3"] {
        let mut p = Processor::new(Config::default()).unwrap();
        let s = session(0);
        let mut trace = vec![];
        subscribe(&mut p, s, 0, 1, &mut trace);
        send(
            &mut p,
            data(s, 1, book("A", json!([{"price":"0.4","size":"7"}]))),
            &mut trace,
        );
        trace.clear();
        send(
            &mut p,
            data(
                s,
                2,
                json!({
                    "event_type":"price_change","timestamp":"42",
                    "price_changes":[change("A","0.4","5"),change("A","0.4",signed),change("A","0.4","8")]
                }),
            ),
            &mut trace,
        );
        assert_eq!(trace, vec!["invalid:A"]);
        let replica = p.replica(s.adapter, "A", 0).unwrap();
        assert_eq!(replica.state, Validity::Stale);
        assert_eq!(replica.book.best(Side::Buy).unwrap().1.value(), 7_000_000);
        assert_eq!(p.stats().rejected, 1);
    }
}

#[test]
fn requested_identity_lookup_preserves_namespace_and_unknown_scope() {
    let mut p = Processor::new(Config::default()).unwrap();
    let v1 = session(0);
    let v2 = Session {
        adapter: AdapterId::PolymarketV2,
        slot: 0,
        ..session(1)
    };
    let mut trace = vec![];
    for (s, assets, quantities) in [
        (v1, ["0007", "7"], [11, 13]),
        (v2, ["0007", "only-v2"], [17, 19]),
    ] {
        send(&mut p, Message::Started(s), &mut trace);
        send(
            &mut p,
            Message::SubscriptionsRequested {
                session: s,
                revisions: vec![1, 1],
                add: assets.map(str::to_owned).to_vec(),
                remove: vec![],
            },
            &mut trace,
        );
        let snapshots: Vec<_> = assets
            .into_iter()
            .zip(quantities)
            .map(|(asset, quantity)| {
                book(asset, json!([{"price":"0.4","size":quantity.to_string()}]))
            })
            .collect();
        send(&mut p, data(s, 1, json!(snapshots)), &mut trace);
    }
    assert_eq!(
        p.replica(v1.adapter, "0007", 0)
            .unwrap()
            .book
            .best(Side::Buy)
            .unwrap()
            .1
            .value(),
        11_000_000
    );
    assert_eq!(
        p.replica(v2.adapter, "0007", 0)
            .unwrap()
            .book
            .best(Side::Buy)
            .unwrap()
            .1
            .value(),
        17_000_000
    );
    send(
        &mut p,
        Message::SubscriptionsRequested {
            session: v1,
            revisions: vec![2],
            add: vec![],
            remove: vec!["0007".into()],
        },
        &mut trace,
    );
    trace.clear();
    send(
        &mut p,
        data(
            v1,
            2,
            json!({"event_type":"price_change","timestamp":"41",
        "price_changes":[change("0007","0.4","99")]}),
        ),
        &mut trace,
    );
    assert!(trace.is_empty(), "known unrequested input is ignored");
    assert_eq!(p.replica(v1.adapter, "7", 0).unwrap().state, Validity::Live);
    send(
        &mut p,
        data(
            v1,
            3,
            json!({"event_type":"price_change","timestamp":"40",
        "price_changes":[change("only-v2","0.4","101")]}),
        ),
        &mut trace,
    );
    assert_eq!(trace, ["invalid:7"], "unknown in V1 invalidates V1, not V2");
    let remaining = p.replica(v1.adapter, "7", 0).unwrap();
    assert_eq!(remaining.state, Validity::Stale);
    assert_eq!(
        remaining.book.best(Side::Buy).unwrap().1.value(),
        13_000_000
    );
    for (asset, quantity) in [("0007", 17_000_000), ("only-v2", 19_000_000)] {
        let replica = p.replica(v2.adapter, asset, 0).unwrap();
        assert_eq!(replica.state, Validity::Live);
        assert_eq!(replica.book.best(Side::Buy).unwrap().1.value(), quantity);
    }
    assert_eq!(p.take_recovery(), [v1]);
}
