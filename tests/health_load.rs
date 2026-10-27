//! Explicit small-first owner/health measurement, not a socket or SBE benchmark.
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    orderbook::{Config, ObservationKind, Processor, SelectionPolicy},
    websocket::ingress::{Message, Session},
};
use std::time::{Duration, Instant};

#[test]
#[ignore = "explicit release owner/health measurement; HEALTH_ASSETS defaults to 100"]
fn flowing_replicas_keep_health_revisits_bounded_and_selected_history_exact() {
    let assets: usize = std::env::var("HEALTH_ASSETS")
        .unwrap_or_else(|_| "100".into())
        .parse()
        .unwrap();
    assert!((1..=600_000).contains(&assets));
    let mut p = Processor::new(Config {
        selection: SelectionPolicy::Automatic,
        monitoring: true,
        instruments: assets,
        replicas: assets * 2,
        sessions: 2,
        total_levels: assets * 3,
        ..Config::default()
    })
    .unwrap();
    let sessions = [0, 1].map(|slot| Session {
        worker: slot as usize,
        slot,
        generation: 1,
        adapter: AdapterId::PolymarketV1,
    });
    let ids: Vec<_> = (0..assets).map(|id| format!("A{id}")).collect();
    let mut selected = 0usize;
    let mut snapshots = 0usize;
    let mut sink = |o: engineering_challenge::orderbook::Observation<'_>| {
        match o.kind {
            ObservationKind::Level { quantity, .. } => {
                let id: usize = o.asset[1..].parse().unwrap();
                // Distinct expected values for every instrument and source.
                assert_eq!(quantity.value(), (id as i64 + 3) * 1_000_000);
                assert_eq!(o.binding.session.slot, 0);
                selected += 1;
            }
            ObservationKind::Snapshot { .. } => snapshots += 1,
            ObservationKind::StandbyBaseline => {}
            ObservationKind::Invalidated { .. } => panic!("healthy workload invalidated"),
        }
        Ok(())
    };
    let mut ordinals = [0u64; 2];
    for s in sessions {
        p.process(&Message::Started(s), Duration::ZERO, &mut sink)
            .unwrap();
        p.process(
            &Message::SubscriptionsRequested {
                session: s,
                revisions: vec![1; assets],
                add: ids.clone(),
                remove: vec![],
            },
            Duration::ZERO,
            &mut sink,
        )
        .unwrap();
        for id in &ids {
            ordinals[s.slot as usize] += 1;
            p.process(&Message::Data {session:s,ordinal:ordinals[s.slot as usize],received_at:chrono::Utc::now(),received_mono:Instant::now(),
                payload:format!(r#"{{"event_type":"book","asset_id":"{id}","timestamp":"1","bids":[],"asks":[]}}"#).into()},Duration::ZERO,&mut sink).unwrap();
        }
    }
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut message_count = 0usize;
    // Service startup deadlines at the same cadence as later deadlines. Leaving
    // the owner unticked until second one measures a harness-imposed stall.
    for tenth in 0..10 {
        p.tick(Duration::from_millis(tenth * 100)).unwrap();
    }
    // Accelerated logical minutes; measured durations remain real CPU/elapsed
    // time. This does NOT claim a real-time offered rate or socket latency.
    for second in 1..=80u64 {
        let now = Duration::from_secs(second);
        for (id, asset) in ids.iter().enumerate() {
            for s in sessions {
                ordinals[s.slot as usize] += 1;
                let quantity = id as u64 + 3 + u64::from(s.slot) * 11;
                let message = Message::Data {session:s,ordinal:ordinals[s.slot as usize],received_at:chrono::Utc::now(),received_mono:Instant::now(),
                    payload:format!(r#"{{"event_type":"price_change","timestamp":"1","price_changes":[{{"asset_id":"{asset}","side":"BUY","price":"0.4","size":"{quantity}"}}]}}"#).into()};
                let before = Instant::now();
                p.process(&message, now, &mut sink).unwrap();
                if message_count.is_multiple_of(100) {
                    samples.push(before.elapsed().as_nanos());
                }
                message_count += 1;
            }
        }
        // Service the declared population demand at 100ms logical cadence.
        // Sustained debt remains visible in max revisit/lateness diagnostics.
        for tenth in 0..10 {
            p.tick(now + Duration::from_millis(tenth * 100)).unwrap();
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(p.stats().health_suspicions, 0);
    assert_eq!(p.stats().changes, assets as u64 * 160);
    assert_eq!(p.stats().selected_changes, assets as u64 * 80);
    assert_eq!(selected, assets * 80);
    assert_eq!(snapshots, assets);
    for (id, asset) in ids.iter().enumerate() {
        for slot in 0..2 {
            let replica = p.replica(AdapterId::PolymarketV1, asset, slot).unwrap();
            assert_eq!(
                replica.book.best(Side::Buy).unwrap().1.value(),
                (id as i64 + 3 + i64::from(slot) * 11) * 1_000_000
            );
            assert_eq!(replica.health.accepted_changes(), 80);
        }
    }
    samples.sort_unstable();
    let percentile = |n: usize| samples[(samples.len() - 1) * n / 1000];
    eprintln!(
        "{}",
        serde_json::json!({"assets":assets,"replicas":assets*2,"messages":message_count,
        "elapsed_seconds":elapsed.as_secs_f64(),"sample_count":samples.len(),
        "owner_ns":{"p50":percentile(500),"p95":percentile(950),"p99":percentile(990),"p999_exploratory":percentile(999)},
        "health_bytes":std::mem::size_of::<engineering_challenge::orderbook::health::Health>(),"stats":p.stats(),
        "scope":"accelerated-clock owner/health only; no queue, network, SBE, constant-ingress or full-universe qualification"})
    );
    assert!(
        p.stats().max_health_revisit <= Duration::from_secs(11),
        "health schedule exceeded candidate envelope"
    );
    assert!(
        p.stats().max_health_lateness <= Duration::from_secs(1),
        "health scheduling lateness exceeded candidate envelope"
    );
}
