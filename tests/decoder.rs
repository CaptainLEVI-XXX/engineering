use engineering_challenge::market_data::{Event, Limits, decode, envelope, parse_scaled};

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
            assert_eq!(exchange_us, 1001000);
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
        assert_eq!(exchange_us, 1_793_090_283_325_000);
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
    for kind in [
        "last_trade_price",
        "best_bid_ask",
        "new_market",
        "market_resolved",
    ] {
        let text = format!(r#"{{"event_type":"{kind}"}}"#);
        assert!(matches!(
            decode(envelope(&text, &limits).unwrap()[0], &limits).unwrap(),
            Event::NonBook
        ));
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
