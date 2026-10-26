use engineering_challenge::discovery::{MarketKey, Registry, Version};
use serde_json::{Value, json};

fn fixtures() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/gamma-markets.json")).unwrap()
}

fn page(records: Vec<Value>) -> Vec<u8> {
    serde_json::to_vec(&json!({"markets": records})).unwrap()
}

fn key(version: Version, id: &str) -> MarketKey {
    MarketKey {
        version,
        id: id.to_owned(),
    }
}

#[test]
fn real_records_select_versioned_ids_and_do_not_admit_closed_markets() {
    let records = fixtures();
    let mut registry = Registry::default();
    registry
        .apply_listing(
            Version::V1,
            &page(vec![records[0].clone(), records[2].clone()]),
        )
        .unwrap();
    registry
        .apply_listing(
            Version::V2,
            &page(vec![records[1].clone(), records[3].clone()]),
        )
        .unwrap();
    let snapshot = registry.snapshot();
    assert_eq!(snapshot.desired_assets(), 4);
    let v1 = &snapshot.markets[&key(Version::V1, "559651")];
    assert_eq!(
        v1.outcomes[0].asset_id,
        "32338220190071351435772801779725302244575775216413325951443816017994629993401"
    );
    assert_eq!(
        snapshot.markets[&key(Version::V2, "5931604")].outcomes[1].label,
        "Korfez Basket"
    );
    // Synthetic disagreement proves V2 never falls back to the CTF field.
    let mut v2 = records[1].clone();
    v2["clobTokenIds"] = json!("[\"901\",\"902\"]");
    let mut registry = Registry::default();
    registry
        .apply_listing(Version::V2, &page(vec![v2]))
        .unwrap();
    assert_eq!(
        registry.snapshot().markets[&key(Version::V2, "5931604")].outcomes[1].asset_id,
        "832164947364691551324361485165400028834171767032415940519474181712990175233"
    );
}

#[test]
fn conflict_changes_cached_snapshot_and_notifies_without_partial_market_addition() {
    let mut registry = Registry::default();
    let mut notifications = registry.subscribe();
    let original = fixtures()[0].clone();
    registry
        .apply_listing(Version::V1, &page(vec![original.clone()]))
        .unwrap();
    notifications.borrow_and_update();
    let before = registry.snapshot();

    let mut conflicting = original.clone();
    conflicting["id"] = json!("another-market");
    // Shares only one asset: blindly inserting the other would leave half a market.
    let ids: Vec<String> =
        serde_json::from_str(original["clobTokenIds"].as_str().unwrap()).unwrap();
    conflicting["clobTokenIds"] = json!(serde_json::to_string(&[ids[0].as_str(), "999"]).unwrap());
    registry
        .apply_listing(Version::V1, &page(vec![conflicting]))
        .unwrap();

    assert!(notifications.has_changed().unwrap());
    let after = registry.snapshot();
    assert!(
        before.markets[&key(Version::V1, "559651")]
            .uncertainty
            .is_none()
    );
    assert!(
        after.markets[&key(Version::V1, "559651")]
            .uncertainty
            .is_some()
    );
    assert_eq!(after.desired_assets(), 2);
    assert!(
        !after
            .markets
            .contains_key(&key(Version::V1, "another-market"))
    );
    assert!(
        registry
            .pending_checks()
            .contains(&key(Version::V1, "559651"))
    );
    assert!(
        registry
            .pending_checks()
            .contains(&key(Version::V1, "another-market"))
    );

    // A listing cannot clear a conflict; a subsequent matching targeted read can.
    notifications.borrow_and_update();
    registry
        .apply_listing(Version::V1, &page(vec![original.clone()]))
        .unwrap();
    assert!(!notifications.has_changed().unwrap());
    registry
        .apply_verification(Version::V1, &["559651".to_owned()], &page(vec![original]))
        .unwrap();
    assert!(notifications.has_changed().unwrap());
    assert!(
        registry.snapshot().markets[&key(Version::V1, "559651")]
            .uncertainty
            .is_none()
    );
}

#[test]
fn retired_market_requires_targeted_matching_open_evidence_to_reactivate() {
    let mut registry = Registry::default();
    let open = fixtures()[1].clone();
    let id = key(Version::V2, "5931604");
    registry
        .apply_listing(Version::V2, &page(vec![open.clone()]))
        .unwrap();
    let mut closed = open.clone();
    closed["closed"] = json!(true);
    closed["acceptingOrders"] = json!(false);
    registry
        .apply_listing(Version::V2, &page(vec![closed.clone()]))
        .unwrap();
    assert!(registry.snapshot().markets[&id].desired);
    registry
        .apply_verification(
            Version::V2,
            std::slice::from_ref(&id.id),
            &page(vec![closed]),
        )
        .unwrap();
    assert!(!registry.snapshot().markets[&id].desired);

    let mut notifications = registry.subscribe();
    registry
        .apply_listing(Version::V2, &page(vec![open.clone()]))
        .unwrap();
    assert!(!registry.snapshot().markets[&id].desired);
    assert!(registry.pending_checks().contains(&id));
    let mut wrong = open.clone();
    wrong["positionIds"] = json!(["111", "222"]);
    registry
        .apply_verification(
            Version::V2,
            std::slice::from_ref(&id.id),
            &page(vec![wrong]),
        )
        .unwrap();
    assert!(!registry.snapshot().markets[&id].desired);
    notifications.borrow_and_update();
    registry
        .apply_verification(Version::V2, std::slice::from_ref(&id.id), &page(vec![open]))
        .unwrap();
    assert!(notifications.has_changed().unwrap());
    assert!(registry.snapshot().markets[&id].desired);
    assert!(registry.snapshot().markets[&id].uncertainty.is_none());
}

#[test]
fn uncertain_existing_records_keep_desire_schedule_checks_and_publish_uncertainty() {
    let open = fixtures()[0].clone();
    for (field, bad_value) in [
        ("active", Value::Null),
        ("archived", Value::Null),
        ("closed", json!("false")),
        ("closed", json!(true)), // Contradicts the fixture's acceptingOrders=true.
        ("clobTokenIds", json!("[broken")),
        ("enableOrderBook", json!(false)),
        ("version", Value::Null),
    ] {
        let mut registry = Registry::default();
        registry
            .apply_listing(Version::V1, &page(vec![open.clone()]))
            .unwrap();
        let before = registry.snapshot();
        let notifications = registry.subscribe();
        let mut uncertain = open.clone();
        uncertain[field] = bad_value;
        registry
            .apply_listing(Version::V1, &page(vec![uncertain]))
            .unwrap();
        let id = key(Version::V1, "559651");
        let after = registry.snapshot();
        assert!(after.markets[&id].desired, "{field}");
        assert!(after.markets[&id].uncertainty.is_some(), "{field}");
        assert!(before.markets[&id].uncertainty.is_none());
        assert!(notifications.has_changed().unwrap());
        assert!(registry.pending_checks().contains(&id));
        registry
            .apply_listing(Version::V2, &page(vec![fixtures()[1].clone()]))
            .unwrap();
        assert_eq!(registry.snapshot().desired_assets(), 4);
    }
}

#[test]
fn order_entry_pause_is_not_retirement_and_missing_lookup_is_not_evidence() {
    let mut open = fixtures()[0].clone();
    open["acceptingOrders"] = json!(false);
    let mut registry = Registry::default();
    registry
        .apply_listing(Version::V1, &page(vec![open]))
        .unwrap();
    let id = key(Version::V1, "559651");
    assert_eq!(registry.snapshot().desired_assets(), 2);
    registry
        .apply_verification(Version::V1, std::slice::from_ref(&id.id), &page(vec![]))
        .unwrap();
    assert!(registry.snapshot().markets[&id].desired);
    assert!(registry.snapshot().markets[&id].uncertainty.is_some());
}

#[test]
fn missing_market_id_can_still_flag_known_outcome_identities() {
    let mut registry = Registry::default();
    let mut record = fixtures()[0].clone();
    registry
        .apply_listing(Version::V1, &page(vec![record.clone()]))
        .unwrap();
    let before = registry.snapshot();
    let notifications = registry.subscribe();
    record.as_object_mut().unwrap().remove("id");
    registry
        .apply_listing(Version::V1, &page(vec![record]))
        .unwrap();
    let id = key(Version::V1, "559651");
    assert!(registry.pending_checks().contains(&id));
    assert!(registry.snapshot().markets[&id].uncertainty.is_some());
    assert!(before.markets[&id].uncertainty.is_none());
    assert!(notifications.has_changed().unwrap());
    assert_eq!(registry.snapshot().desired_assets(), 2);
}

#[test]
fn malformed_envelope_does_not_erase_already_published_additions() {
    let mut registry = Registry::default();
    registry
        .apply_listing(Version::V1, &page(vec![fixtures()[0].clone()]))
        .unwrap();
    assert!(
        registry
            .apply_listing(Version::V1, br#"{"next_cursor":"abc"}"#)
            .is_err()
    );
    assert_eq!(registry.snapshot().desired_assets(), 2);
    // Contradictory duplicate records in a targeted response must not retire.
    let open = fixtures()[0].clone();
    let mut closed = open.clone();
    closed["closed"] = json!(true);
    closed["acceptingOrders"] = json!(false);
    registry
        .apply_verification(Version::V1, &["559651".into()], &page(vec![closed, open]))
        .unwrap();
    assert!(registry.snapshot().markets[&key(Version::V1, "559651")].desired);
}
