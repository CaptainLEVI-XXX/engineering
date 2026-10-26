use engineering_challenge::discovery::polymarket::{
    GammaAdapter, PolymarketV1, PolymarketV2, Version,
};
use engineering_challenge::discovery::{MarketKey, PageReport, Registry};
use serde_json::{Value, json};

fn adapter(version: Version) -> &'static dyn GammaAdapter {
    match version {
        Version::V1 => &PolymarketV1,
        Version::V2 => &PolymarketV2,
    }
}

// Preserve the fixture scenarios while exercising the new adapter → registry
// boundary. The registry itself no longer accepts Gamma bytes or wire versions.
trait FixtureInput {
    fn apply_listing(&mut self, version: Version, bytes: &[u8]) -> anyhow::Result<PageReport>;
    fn apply_verification(
        &mut self,
        version: Version,
        ids: &[String],
        bytes: &[u8],
    ) -> anyhow::Result<()>;
}

impl FixtureInput for Registry {
    fn apply_listing(&mut self, version: Version, bytes: &[u8]) -> anyhow::Result<PageReport> {
        let adapter = adapter(version);
        Ok(self.apply_page(adapter.id(), adapter.decode_page(bytes)?))
    }
    fn apply_verification(
        &mut self,
        version: Version,
        ids: &[String],
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        let adapter = adapter(version);
        let page = adapter.decode_page(bytes)?;
        anyhow::ensure!(
            page.next_cursor.is_none(),
            "verification pagination not finished"
        );
        self.verify_records(adapter.id(), ids, page.markets);
        Ok(())
    }
}

fn fixtures() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/gamma-markets.json")).unwrap()
}

fn page(records: Vec<Value>) -> Vec<u8> {
    serde_json::to_vec(&json!({"markets": records})).unwrap()
}

fn key(version: Version, id: &str) -> MarketKey {
    MarketKey {
        adapter: adapter(version).id(),
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
        v1.instruments[0].id,
        "32338220190071351435772801779725302244575775216413325951443816017994629993401"
    );
    assert_eq!(
        snapshot.markets[&key(Version::V2, "5931604")].instruments[1].label,
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
        registry.snapshot().markets[&key(Version::V2, "5931604")].instruments[1].id,
        "832164947364691551324361485165400028834171767032415940519474181712990175233"
    );
    // Both fields may exist, but the wrong protocol response is never admitted.
    for (version, row) in [
        (Version::V1, records[1].clone()),
        (Version::V2, records[0].clone()),
    ] {
        let mut registry = Registry::default();
        assert_eq!(
            registry
                .apply_listing(version, &page(vec![row]))
                .unwrap()
                .unresolved,
            1
        );
        assert_eq!(registry.snapshot().desired_assets(), 0);
    }
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
            .contains_key(&key(Version::V1, "559651"))
    );
    assert!(
        registry
            .pending_checks()
            .contains_key(&key(Version::V1, "another-market"))
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
    assert!(registry.pending_checks().contains_key(&id));
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
        assert!(registry.pending_checks().contains_key(&id));
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
    assert!(registry.pending_checks().contains_key(&id));
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

#[test]
fn never_admitted_disabled_markets_do_not_request_verification() {
    let mut disabled = fixtures()[0].clone();
    disabled["enableOrderBook"] = json!(false);
    disabled["clobTokenIds"] = Value::Null;
    let mut registry = Registry::default();
    for _ in 0..3 {
        let report = registry
            .apply_listing(
                Version::V1,
                &page(vec![fixtures()[4].clone(), disabled.clone()]),
            )
            .unwrap();
        assert_eq!(report.unresolved, 0);
        assert!(registry.pending_checks().is_empty());
        assert_eq!(registry.snapshot().desired_assets(), 0);
    }
    // A later eligible listing still admits the market without a tombstone.
    registry
        .apply_listing(Version::V1, &page(vec![fixtures()[0].clone()]))
        .unwrap();
    assert_eq!(registry.snapshot().desired_assets(), 2);
    registry
        .apply_listing(Version::V1, &page(vec![disabled]))
        .unwrap();
    assert_eq!(registry.snapshot().desired_assets(), 2);
    assert_eq!(registry.pending_checks().len(), 1);
}

#[test]
fn unknown_verification_has_backoff_attempt_and_age_limits_without_resetting_on_hints() {
    use std::time::Duration;
    use tokio::time::Instant;
    let mut registry = Registry::default();
    let unknown = key(Version::V1, "2290078");
    let report = registry
        .apply_listing(Version::V1, &page(vec![fixtures()[5].clone()]))
        .unwrap();
    assert_eq!(report.unresolved, 1);
    assert_eq!(registry.snapshot().desired_assets(), 0);
    let start = Instant::now();
    for seconds in [0, 30, 90] {
        registry.request_verification(unknown.clone());
        assert_eq!(
            registry.verification_batch(start + Duration::from_secs(seconds)),
            vec![unknown.clone()]
        );
        assert!(
            registry
                .verification_batch(start + Duration::from_secs(seconds + 29))
                .is_empty()
        );
    }
    registry.request_verification(unknown.clone());
    assert!(
        registry
            .verification_batch(start + Duration::from_secs(210))
            .is_empty()
    );
    assert!(registry.pending_checks().contains_key(&unknown));
    assert!(
        registry
            .verification_batch(start + Duration::from_secs(301))
            .is_empty()
    );
    assert!(registry.pending_checks().is_empty());

    // Expiry also bounds work that never got its first attempt.
    registry.request_verification(unknown);
    assert!(
        registry
            .verification_batch(Instant::now() + Duration::from_secs(301))
            .is_empty()
    );
    assert!(registry.pending_checks().is_empty());
}

#[test]
fn unknown_capacity_does_not_block_known_verification_or_expire_desire() {
    use std::time::Duration;
    use tokio::time::Instant;
    let mut registry = Registry::default();
    for i in 0..1100 {
        registry.request_verification(key(Version::V2, &format!("unknown-{i}")));
    }
    assert_eq!(registry.pending_checks().len(), 1024);
    let known = key(Version::V1, "559651");
    registry
        .apply_listing(Version::V1, &page(vec![fixtures()[0].clone()]))
        .unwrap();
    registry
        .apply_verification(Version::V1, std::slice::from_ref(&known.id), &page(vec![]))
        .unwrap();
    assert_eq!(registry.pending_checks().len(), 1025);
    let start = Instant::now() + Duration::from_secs(301);
    for seconds in [0, 30, 90, 210, 450, 750] {
        assert_eq!(
            registry.verification_batch(start + Duration::from_secs(seconds)),
            vec![known.clone()]
        );
        assert_eq!(registry.snapshot().desired_assets(), 2);
        assert!(registry.snapshot().markets[&known].uncertainty.is_some());
    }
    assert_eq!(registry.pending_checks().len(), 1);
}

#[test]
fn final_page_notification_and_snapshot_recover_all_atomic_additions() {
    let mut registry = Registry::default();
    let mut changes = registry.subscribe();
    let before = registry.snapshot();
    let first = fixtures()[0].clone();
    let mut second = first.clone();
    second["id"] = json!("second");
    second["clobTokenIds"] = json!("[\"777\",\"888\"]");
    let bytes = page(vec![first, second]);
    registry.apply_listing(Version::V1, &bytes).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    assert_eq!(before.desired_assets(), 0);
    assert_eq!(registry.snapshot().desired_assets(), 4);
    registry.apply_listing(Version::V1, &bytes).unwrap();
    assert!(!changes.has_changed().unwrap());
    // A restarted/late consumer needs no replay of notifications.
    assert_eq!(registry.snapshot().desired_assets(), 4);
}

#[test]
fn normalized_registry_counts_instruments_and_isolates_adapter_namespaces() {
    use engineering_challenge::discovery::{
        AdapterId, Eligibility, Instrument, MarketRecord, Page,
    };
    let mut registry = Registry::default();
    let record = MarketRecord {
        id: "same-market".into(),
        instruments: vec![Instrument {
            id: "same-instrument".into(),
            label: "native label".into(),
        }],
        eligibility: Eligibility::Eligible,
    };
    for adapter in [AdapterId::PolymarketV1, AdapterId::PolymarketV2] {
        registry.apply_page(
            adapter,
            Page {
                markets: vec![Ok(record.clone())],
                next_cursor: None,
            },
        );
    }
    assert_eq!(registry.snapshot().markets.len(), 2);
    assert_eq!(registry.snapshot().desired_assets(), 2); // One per market, not two.
    let original = registry.snapshot();
    let mut changed = record;
    changed.instruments.push(Instrument {
        id: "extra".into(),
        label: "extra".into(),
    });
    let report = registry.apply_page(
        AdapterId::PolymarketV1,
        Page {
            markets: vec![Ok(changed)],
            next_cursor: None,
        },
    );
    assert_eq!(report.identity_conflicts, 1); // A matching prefix is not an unchanged identity.
    let after = registry.snapshot();
    assert_eq!(after.desired_assets(), 2);
    let v1 = MarketKey {
        adapter: AdapterId::PolymarketV1,
        id: "same-market".into(),
    };
    let v2 = MarketKey {
        adapter: AdapterId::PolymarketV2,
        id: "same-market".into(),
    };
    assert!(after.markets[&v1].uncertainty.is_some());
    assert!(after.markets[&v2].uncertainty.is_none());
    assert!(original.markets[&v1].uncertainty.is_none());
}

#[test]
#[ignore = "release-mode control-plane measurement, not a latency acceptance test"]
fn large_universe_measurement() {
    use std::{hint::black_box, time::Instant};
    let template = fixtures()[0].clone();
    let pages: Vec<_> = (0..500)
        .map(|p| {
            page(
                (0..100)
                    .map(|i| {
                        let n = p * 100 + i;
                        let mut market = template.clone();
                        market["id"] = json!(format!("market-{n}"));
                        market["clobTokenIds"] = json!(
                            serde_json::to_string(&[
                                format!("{:077}", n * 2 + 1),
                                format!("{:077}", n * 2 + 2)
                            ])
                            .unwrap()
                        );
                        market
                    })
                    .collect(),
            )
        })
        .collect();
    let mut ingest = Vec::new();
    let mut cold = Vec::new();
    let mut cached = Vec::new();
    for _ in 0..7 {
        let mut registry = Registry::default();
        let start = Instant::now();
        for p in &pages {
            registry.apply_listing(Version::V1, p).unwrap();
        }
        ingest.push(start.elapsed().as_micros());
        let start = Instant::now();
        let snapshot = registry.snapshot();
        cold.push(start.elapsed().as_micros());
        assert_eq!(snapshot.desired_assets(), 100_000);
        let start = Instant::now();
        for _ in 0..10_000 {
            black_box(registry.snapshot());
        }
        cached.push(start.elapsed().as_nanos() / 10_000);
    }
    ingest.sort_unstable();
    cold.sort_unstable();
    cached.sort_unstable();
    eprintln!(
        "50k markets / 100k assets; 7 samples; median ingest_us={} cold_snapshot_us={} cached_snapshot_ns={}",
        ingest[3], cold[3], cached[3]
    );
}
