use engineering_challenge::market_data::{Event, Limits, decode, envelope, parse_scaled};

#[test]
fn optional_wire_text_preserves_escaped_values_and_missing_field_scope() {
    use engineering_challenge::market_data::{Decoder, SessionDecoder};
    let limits = Limits::default();
    for (asset, timestamp) in [(r#""001""#, r#""123""#), (r#""00\u0031""#, r#""12\u0033""#)] {
        let text = format!(
            r#"{{"event_type":"book","asset_id":{asset},"timestamp":{timestamp},"bids":[],"asks":[]}}"#
        );
        for message in [text.clone(), format!("[{text}]")] {
            let mut events = Vec::new();
            Decoder::Polymarket
                .visit(&message, &limits, &mut |event| {
                    events.push(event.unwrap());
                    Ok(())
                })
                .unwrap();
            assert_eq!(events.len(), 1);
            let Event::Snapshot {
                asset, exchange_us, ..
            } = &events[0]
            else {
                panic!("snapshot expected");
            };
            assert_eq!(asset, "001");
            assert_eq!(*exchange_us, Some(123_000));
        }
    }
    for fields in ["", r#", "asset_id":null,"timestamp":null"#] {
        let text = format!(r#"{{"event_type":"last_trade_price"{fields}}}"#);
        assert!(matches!(
            decode(envelope(&text, &limits).unwrap()[0], &limits).unwrap(),
            Event::NonBook
        ));
    }
    for (fields, expected_assets) in [
        (r#""timestamp":"123""#, None),
        (r#""asset_id":null,"timestamp":"123""#, None),
        (r#""asset_id":"001""#, Some(vec!["001".to_owned()])),
        (
            r#""asset_id":"001","timestamp":null"#,
            Some(vec!["001".to_owned()]),
        ),
        (r#""asset_id":"001","timestamp":123"#, None),
    ] {
        let text = format!(r#"{{"event_type":"book",{fields},"bids":[],"asks":[]}}"#);
        for message in [text.clone(), format!("[{text}]")] {
            let mut emitted = 0;
            Decoder::Polymarket
                .visit(&message, &limits, &mut |event| {
                    let error = event.unwrap_err();
                    assert_eq!(error.assets, expected_assets);
                    assert!(!error.resource);
                    emitted += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(emitted, 1);
        }
    }
}

#[test]
fn decimals_are_exact_at_precision_and_overflow_boundaries() {
    for (text, want) in [
        ("0.65", 650000),
        ("100.5", 100500000),
        ("0.000001", 1),
        ("1.0000000", 1000000),
        ("9223372036854.775807", i64::MAX),
    ] {
        assert_eq!(parse_scaled(text).unwrap(), want, "{text}");
    }
    for text in [
        "0.0000001",
        "9223372036854.775808",
        "-1",
        "+1",
        "1e2",
        "",
        " 1",
        "1.",
        ".1",
    ] {
        assert!(parse_scaled(text).is_err(), "{text}");
    }
}

#[test]
fn envelope_validates_the_tail_before_exposing_events() {
    use engineering_challenge::market_data::{Decoder, SessionDecoder, VisitError};
    let limits = Limits::default();
    assert!(envelope("[{\"event_type\":\"book\"},", &limits).is_err());
    assert_eq!(envelope("[]", &limits).unwrap().len(), 0);
    assert!(
        envelope(
            "[{},{}]",
            &Limits {
                events: 1,
                ..limits
            }
        )
        .is_err()
    );
    let valid = r#"{"event_type":"book","asset_id":"001","timestamp":"42","bids":[],"asks":[]}"#;
    for invalid in [
        format!("{valid} trailing"),
        format!("[{valid},"),
        format!("{valid}{valid}"),
        valid.replace("\"asks\":[]", "\"asks\":[],\"extra\":[1,]"),
        // An early typed-field error must not hide malformed later syntax.
        valid
            .replace("\"001\"", "null")
            .replace("\"asks\":[]", "\"asks\":["),
    ] {
        let mut emitted = 0;
        let result = Decoder::Polymarket.visit(&invalid, &limits, &mut |_| {
            emitted += 1;
            Ok(())
        });
        assert!(matches!(result, Err(VisitError::Envelope(_))), "{invalid}");
        assert_eq!(emitted, 0);
    }
    let mut emitted = 0;
    let result = Decoder::Polymarket.visit(
        valid,
        &Limits {
            events: 0,
            ..limits.clone()
        },
        &mut |_| {
            emitted += 1;
            Ok(())
        },
    );
    assert!(matches!(result, Err(VisitError::Envelope(_))));
    assert_eq!(emitted, 0);
    let semantic_error = valid.replace("\"bids\":[]", r#""bids":[{"price":"0.4","size":"bad"}]"#);
    Decoder::Polymarket
        .visit(&semantic_error, &limits, &mut |event| {
            let error = event.unwrap_err();
            assert_eq!(error.assets, Some(vec!["001".to_owned()]));
            assert!(!error.resource);
            emitted += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(emitted, 1);
    let result = Decoder::Polymarket.visit(valid, &limits, &mut |event| {
        assert!(matches!(event?, Event::Snapshot { .. }));
        emitted += 1;
        anyhow::bail!("closed sink")
    });
    assert!(matches!(result, Err(VisitError::Sink(_))));
    assert_eq!(emitted, 2, "sink failure must not retry delivery");
}

#[test]
fn decoding_preserves_exact_values_and_isolates_bad_event_scope() {
    let limits = Limits::default();
    let text = r#"[{"event_type":"book","asset_id":"001","timestamp":"1001","bids":[{"price":"\u0030.4","size":"3.000001"}],"asks":[]},{"event_type":"price_change","timestamp":"1000","price_changes":[{"asset_id":"001","side":"BUY","price":"0.4","size":"7"},{"asset_id":"002","side":"SELL","price":"0.6","size":"bad"}]}]"#;
    let events = envelope(text, &limits).unwrap();
    match decode(events[0], &limits).unwrap() {
        Event::Snapshot {
            asset,
            exchange_us,
            bids,
            asks,
        } => {
            assert_eq!(asset, "001");
            assert_eq!(exchange_us, Some(1001000));
            assert_eq!(
                (bids[0].price.value(), bids[0].quantity.value()),
                (400000, 3000001)
            );
            assert!(asks.is_empty());
        }
        _ => panic!("snapshot expected"),
    }
    let error = decode(events[1], &limits).unwrap_err();
    assert_eq!(error.assets.unwrap(), vec!["001", "002"]);
    let duplicate = r#"{"event_type":"book","asset_id":"001","asset_id":"002","timestamp":"1","bids":[],"asks":[]}"#;
    assert!(
        decode(envelope(duplicate, &limits).unwrap()[0], &limits)
            .unwrap_err()
            .assets
            .is_none()
    );

    use engineering_challenge::market_data::{Decoder, SessionDecoder};
    // Dispatch must not depend on event_type being first. A late validation
    // error must retain the identity without cloning it on the successful path.
    let reordered = r#"{"asks":[],"asset_id":"\u0030\u00301","bids":[{"price":"0.3","size":"7"}],"timestamp":"42","event_type":"book"}"#;
    Decoder::Polymarket
        .visit(reordered, &limits, &mut |event| {
            let Event::Snapshot {
                asset,
                exchange_us,
                bids,
                asks,
            } = event.unwrap()
            else {
                panic!("snapshot expected");
            };
            assert_eq!(asset, "001");
            assert_eq!(exchange_us, Some(42_000));
            assert_eq!(
                (bids[0].price.value(), bids[0].quantity.value()),
                (300_000, 7_000_000)
            );
            assert!(asks.is_empty());
            Ok(())
        })
        .unwrap();
    for (text, assets) in [
        (reordered.replace("\"42\"", "\"bad\""), Some(vec!["001".to_owned()])),
        (r#"{"asset_id":"001","new_tick_size":"0","event_type":"tick_size_change"}"#.to_owned(), Some(vec!["001".to_owned()])),
        (r#"{"price_changes":[{"asset_id":"A","side":"BUY","price":"0.2","size":"bad"},{"asset_id":"B","side":"SELL","price":"0.8","size":"3"}],"event_type":"price_change","timestamp":"42"}"#.to_owned(), Some(vec!["A".to_owned(), "B".to_owned()])),
        (duplicate.to_owned(), None),
        (r#"{"asset_id":7,"event_type":"book"}"#.to_owned(), None),
    ] {
        for message in [text.clone(), format!(" \n{text}\t "), format!("[{text}]")] {
            let mut emitted = 0;
            Decoder::Polymarket.visit(&message, &limits, &mut |event| {
                let error = event.unwrap_err();
                assert_eq!(error.assets, assets, "{message}");
                assert!(!error.resource);
                emitted += 1;
                Ok(())
            }).unwrap();
            assert_eq!(emitted, 1);
        }
    }
}

#[test]
fn limits_and_timestamp_overflow_fail_without_silent_truncation() {
    let limits = Limits {
        entries: 1,
        ..Limits::default()
    };
    let text = r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"0.1","size":"1"},{"price":"0.2","size":"2"}],"asks":[]}"#;
    assert!(decode(envelope(text, &limits).unwrap()[0], &limits).is_err());
    let text = r#"{"event_type":"book","asset_id":"A","timestamp":"18446744073709552","bids":[],"asks":[]}"#;
    assert!(decode(envelope(text, &limits).unwrap()[0], &limits).is_err());
    // Fits u64 microseconds, but the supplied Chrono-backed reader cannot decode it.
    let text = r#"{"event_type":"book","asset_id":"A","timestamp":"9223372036854775","bids":[],"asks":[]}"#;
    assert!(decode(envelope(text, &limits).unwrap()[0], &limits).is_err());
}

#[test]
fn retained_live_snapshots_match_independent_fixture_expectations() {
    let limits = Limits::default();
    let v1 = envelope(include_str!("fixtures/polymarket-v1-book.json"), &limits).unwrap();
    assert_eq!(v1.len(), 2);
    for (index, raw) in v1.into_iter().enumerate() {
        let Event::Snapshot {
            bids,
            asks,
            exchange_us,
            ..
        } = decode(raw, &limits).unwrap()
        else {
            panic!("snapshot")
        };
        assert_eq!(exchange_us, Some(1_793_090_283_325_000));
        assert_eq!(
            (bids.len(), asks.len()),
            if index == 0 { (0, 72) } else { (72, 0) }
        );
        // Independently summed using Python Decimal from the raw fixture.
        assert_eq!(
            bids.iter()
                .chain(&asks)
                .map(|l| l.quantity.value())
                .sum::<i64>(),
            6_877_897_800_000
        );
    }
    let v2 = envelope(include_str!("fixtures/polymarket-v2-book.json"), &limits).unwrap();
    assert_eq!(v2.len(), 4);
    for raw in v2 {
        let Event::Snapshot { bids, asks, .. } = decode(raw, &limits).unwrap() else {
            panic!("snapshot")
        };
        assert!(bids.is_empty() && asks.is_empty());
    }
}

#[test]
fn generated_decimal_boundaries_and_unknown_semantics_are_not_silently_accepted() {
    for whole in [0i64, 1, 73, 9223372036853] {
        for fractional in [0, 1, 29, 999999] {
            let expected = whole * 1_000_000 + fractional;
            let value = format!("{whole}.{fractional:06}");
            assert_eq!(parse_scaled(&value).unwrap(), expected);
            assert_eq!(parse_scaled(&format!("{value}000")).unwrap(), expected);
            assert!(parse_scaled(&format!("{value}001")).is_err());
        }
    }
    let limits = Limits::default();
    for kind in ["last_trade_price", "best_bid_ask"] {
        let text = format!(r#"{{"event_type":"{kind}"}}"#);
        assert!(matches!(
            decode(envelope(&text, &limits).unwrap()[0], &limits).unwrap(),
            Event::NonBook
        ));
    }
    for kind in ["new_market", "market_resolved"] {
        let text =
            format!(r#"{{"event_type":"{kind}","id":"0007","market":"condition-not-gamma"}}"#);
        assert!(
            matches!(decode(envelope(&text,&limits).unwrap()[0],&limits).unwrap(),
            Event::Lifecycle { market_id:Some(id), adapter:None, .. } if id == "0007")
        );
    }
    for text in [
        r#"{"event_type":"unknown"}"#,
        r#"{"event_type":"price_change","timestamp":"1","price_changes":[{"asset_id":"A","asset_id":"B","price":"0.3","size":"1","side":"BUY"}]}"#,
    ] {
        assert!(
            decode(envelope(text, &limits).unwrap()[0], &limits)
                .unwrap_err()
                .assets
                .is_none()
        );
    }
}

/// Diagnostic only: no network, books, output queue, or latency acceptance gate.
#[test]
#[ignore = "bounded decoder cost attribution; run explicitly in release mode"]
fn profile_decoder_stages_against_retained_oracle() {
    use engineering_challenge::{
        Side,
        market_data::{Decoder, QuantityChange, SessionDecoder},
    };
    use serde_json::{Value, json};
    use std::{hint::black_box, time::Instant};

    let limits = Limits::default();
    let trace: Value =
        serde_json::from_str(include_str!("fixtures/polymarket-v1-trace.json")).unwrap();
    let messages: Vec<_> = trace["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["text"].as_str().unwrap().to_owned())
        .collect();
    let mut actual = Vec::new();
    for text in &messages {
        for raw in envelope(text, &limits).unwrap() {
            match decode(raw, &limits).unwrap() {
                Event::Snapshot {
                    asset,
                    exchange_us,
                    bids,
                    asks,
                } => {
                    let pairs = |levels: Vec<engineering_challenge::market_data::Level>, buy| {
                        let mut pairs: Vec<_> = levels
                            .into_iter()
                            .map(|l| [l.price.value(), l.quantity.value()])
                            .collect();
                        pairs.sort_unstable();
                        if buy {
                            pairs.reverse();
                        }
                        pairs
                    };
                    actual.push(json!({"kind":"snapshot", "symbol":format!("polymarket:v1:{asset}"),
                        "exchange_us":exchange_us, "bids":pairs(bids, true), "asks":pairs(asks, false)}));
                }
                Event::Changes {
                    exchange_us,
                    changes,
                } => {
                    for change in changes {
                        let QuantityChange::Set(quantity) = change.quantity else {
                            panic!("Polymarket must normalize absolute quantities")
                        };
                        actual.push(json!({"kind":"level", "symbol":format!("polymarket:v1:{}",change.asset),
                            "exchange_us":exchange_us, "side":match change.side { Side::Buy => "BUY", Side::Sell => "SELL" },
                            "price":change.price.value(), "quantity":quantity.value()}));
                    }
                }
                other => panic!("unexpected trace event: {other:?}"),
            }
        }
    }
    let mut expected = trace["expected"].as_array().unwrap().clone();
    for record in &mut expected {
        record.as_object_mut().unwrap().remove("received_us");
    }
    assert_eq!(actual, expected, "independent retained decimal oracle");
    assert_eq!(actual.len(), 66);

    fn measure(case: &str, stage: &str, mut work: impl FnMut(usize)) {
        const N: usize = 10_000;
        for i in 0..1_000 {
            work(i);
        }
        let mut samples = Vec::with_capacity(N);
        let start = Instant::now();
        for i in 0..N {
            let before = Instant::now();
            work(i);
            samples.push(before.elapsed().as_nanos() as u64);
        }
        let total = start.elapsed().as_nanos();
        samples.sort_unstable();
        eprintln!(
            "{}",
            json!({"case":case,"stage":stage,"samples":N,
            "p50_ns":samples[N/2],"p99_ns":samples[N*99/100],
            "total_ns":total,"note":"isolated hot-cache diagnostic, not pipeline latency"})
        );
    }
    measure("control", "clock", |i| {
        black_box(i);
    });
    let mut cases = vec![
        ("retained_snapshots".to_owned(), vec![messages[0].clone()]),
        ("retained_deltas".to_owned(), messages[1..].to_vec()),
    ];
    for width in [1, 4, 16, 64] {
        let rows: Vec<_> = (0..width)
            .map(|i| {
                json!({"asset_id":format!("{:078}",i%2),
                "side":if i%2==0 {"BUY"} else {"SELL"},
                "price":format!("0.{:06}",i+1),"size":format!("{}.000003",i+2)})
            })
            .collect();
        let text = json!({"event_type":"price_change","timestamp":"1234","price_changes":rows})
            .to_string();
        Decoder::Polymarket
            .visit(&text, &limits, &mut |event| {
                let Event::Changes {
                    exchange_us,
                    changes,
                } = event.unwrap()
                else {
                    panic!("changes expected");
                };
                assert_eq!(exchange_us, Some(1_234_000));
                assert_eq!(changes.len(), width);
                for (i, change) in changes.iter().enumerate() {
                    assert_eq!(change.asset, format!("{:078}", i % 2));
                    assert_eq!(change.side, if i % 2 == 0 { Side::Buy } else { Side::Sell });
                    assert_eq!(change.price.value(), i as i64 + 1);
                    let QuantityChange::Set(q) = change.quantity else {
                        panic!("absolute size expected");
                    };
                    assert_eq!(q.value(), (i as i64 + 2) * 1_000_000 + 3);
                }
                Ok(())
            })
            .unwrap();
        cases.push((format!("synthetic_changes_{width}"), vec![text]));
    }
    for depth in [8, 256, 2048] {
        let levels: Vec<_> = (1..=depth)
            .map(|i| json!({"price":format!("0.{i:06}"),"size":format!("{i}.000003")}))
            .collect();
        let text = json!({"event_type":"book","asset_id":"0007","timestamp":"1234",
            "bids":levels,"asks":[{"price":"0.900001","size":"2.000007"}]})
        .to_string();
        let Event::Snapshot { bids, asks, .. } =
            decode(envelope(&text, &limits).unwrap()[0], &limits).unwrap()
        else {
            panic!("synthetic snapshot")
        };
        assert_eq!(bids.len(), depth);
        for (i, level) in bids.iter().enumerate() {
            assert_eq!(level.price.value(), (i + 1) as i64);
            assert_eq!(level.quantity.value(), (i + 1) as i64 * 1_000_000 + 3);
        }
        assert_eq!(
            (asks[0].price.value(), asks[0].quantity.value()),
            (900001, 2000007)
        );
        cases.push((format!("synthetic_depth_{depth}"), vec![text]));
    }
    for (name, texts) in cases {
        let raws: Vec<_> = texts
            .iter()
            .map(|text| envelope(text, &limits).unwrap())
            .collect();
        measure(&name, "envelope", |i| {
            drop(black_box(
                envelope(black_box(&texts[i % texts.len()]), &limits).unwrap(),
            ));
        });
        measure(&name, "decode_prevalidated", |i| {
            for raw in &raws[i % raws.len()] {
                drop(black_box(decode(black_box(raw), &limits).unwrap()));
            }
        });
        measure(&name, "envelope_and_decode", |i| {
            for raw in envelope(black_box(&texts[i % texts.len()]), &limits).unwrap() {
                drop(black_box(decode(black_box(raw), &limits).unwrap()));
            }
        });
        measure(&name, "session_decoder", |i| {
            Decoder::Polymarket
                .visit(black_box(&texts[i % texts.len()]), &limits, &mut |event| {
                    drop(black_box(event.unwrap()));
                    Ok(())
                })
                .unwrap();
        });
    }
}

#[test]
fn budget_rejections_are_typed_and_keep_their_positioned_reason() {
    use engineering_challenge::market_data::{Decoder, ResourceLimit, SessionDecoder, VisitError};
    let limits = Limits {
        entries: 1,
        events: 1,
        message_bytes: 512,
        ..Limits::default()
    };
    // Entry budget on a book: a scoped, typed resource rejection.
    let text = r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"0.1","size":"1"},{"price":"0.2","size":"2"}],"asks":[]}"#;
    let error = decode(envelope(text, &limits).unwrap()[0], &limits).unwrap_err();
    assert!(error.resource);
    assert_eq!(error.assets.as_deref(), Some(&["A".to_string()][..]));
    assert!(
        error
            .reason
            .starts_with("resource limit: array entries at line 1"),
        "{}",
        error.reason
    );
    // Same shape but invalid grammar: not a budget rejection.
    let text = r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"x","size":"1"}],"asks":[]}"#;
    assert!(
        !decode(envelope(text, &limits).unwrap()[0], &limits)
            .unwrap_err()
            .resource
    );
    // Envelope budgets (events, bytes) are typed on the envelope error.
    for text in [
        "[{}, {}]".to_string(),
        format!(r#"{{"event_type":"{}"}}"#, "x".repeat(600)),
    ] {
        let result = Decoder::Polymarket.visit(&text, &limits, &mut |_| Ok(()));
        let Err(VisitError::Envelope(error)) = result else {
            panic!("expected envelope rejection for {text:.40}");
        };
        assert!(error.is::<ResourceLimit>(), "{error}");
    }
    // A malformed envelope is not mistaken for a budget rejection.
    let Err(VisitError::Envelope(error)) =
        Decoder::Polymarket.visit("[{", &limits, &mut |_| Ok(()))
    else {
        panic!("expected envelope rejection");
    };
    assert!(!error.is::<ResourceLimit>());
}
