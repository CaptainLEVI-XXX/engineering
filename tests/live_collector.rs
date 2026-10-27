//! Explicit small public fixture capture, not full-universe or latency acceptance.
use engineering_challenge::{
    discovery::{
        AdapterId, Eligibility, Page, Registry,
        polymarket::{GAMMA_MARKETS, adapter},
    },
    orderbook::{self, Config as BookConfig, Processor, Validity},
    websocket::{
        self,
        ingress::{self, Message},
        run_worker,
    },
};
use serde_json::json;
use std::{
    fs::OpenOptions,
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

fn create(path: impl AsRef<Path>) -> std::fs::File {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap()
}

async fn fetch_page(
    client: &reqwest::Client,
    id: AdapterId,
    query: &[(&str, &str)],
    path: &Path,
) -> anyhow::Result<Page> {
    let mut response = client
        .get(GAMMA_MARKETS)
        .query(query)
        .send()
        .await?
        .error_for_status()?;
    let mut bytes = vec![];
    while let Some(chunk) = response.chunk().await? {
        anyhow::ensure!(
            bytes.len() + chunk.len() <= 8 << 20,
            "Gamma page budget exceeded"
        );
        bytes.extend_from_slice(&chunk);
    }
    create(path).write_all(&bytes)?;
    adapter(id).decode_page(&bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "public sample only: <=4 assets/version, one socket, 90s/version, <=32MiB raw; R0_FIXTURE_DIR must be new"]
async fn bounded_public_fixture_probe() {
    let path = std::env::var("R0_FIXTURE_DIR").expect("explicit new fixture destination required");
    let path = Path::new(&path);
    std::fs::create_dir(path).unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let mut results = vec![];
    for (id, version) in [
        (AdapterId::PolymarketV1, "v1"),
        (AdapterId::PolymarketV2, "v2"),
    ] {
        let discovery_at = chrono::Utc::now();
        let page = match fetch_page(
            &client,
            id,
            &[("version", version), ("limit", "100"), ("closed", "false")],
            &path.join(format!("{version}-gamma.json")),
        )
        .await
        {
            Ok(page) => page,
            Err(error) => {
                results.push(json!({"version":version,"error":error.to_string()}));
                continue;
            }
        };
        let markets: Vec<_> = page
            .markets
            .into_iter()
            .filter_map(Result::ok)
            .filter(|m| m.eligibility == Eligibility::Eligible)
            .take(2)
            .collect();
        let assets: Vec<_> = markets
            .iter()
            .flat_map(|m| m.instruments.iter().map(|i| i.id.clone()))
            .collect();
        if assets.is_empty() {
            results.push(
                json!({"version":version,"error":"no validated eligible sample in first page"}),
            );
            continue;
        }
        assert!(assets.len() <= 4);
        let mut registry = Registry::default();
        registry.apply_page(
            id,
            Page {
                markets: markets.iter().cloned().map(Ok).collect(),
                next_cursor: None,
            },
        );
        let desired_before = registry.snapshot();
        let mut capture = create(path.join(format!("{version}-raw.jsonl")));
        let config = Arc::new(websocket::Config {
            reconnect_min: Duration::from_secs(300),
            reconnect_max: Duration::from_secs(300),
            ..Default::default()
        });
        writeln!(capture, "{}", json!({"kind":"sample","version":version,"discovery_at":discovery_at.to_rfc3339(),
            "markets":markets.iter().map(|m| &m.id).collect::<Vec<_>>(),"assets":assets,
            "endpoint":config.endpoint,"seconds":90,"scope":"first-page diagnostic sample; not universe coverage"})).unwrap();
        let (send, receive) = ingress::channel(1024, 8 << 20).unwrap();
        let (_commands, commands) = mpsc::channel(1);
        let (stop, _) = watch::channel(false);
        let owner_stop = stop.clone();
        let sample_assets = assets.clone();
        let owner = std::thread::spawn(move || {
            let started = Instant::now();
            let mut processor =
                Processor::with_receive_clock(BookConfig::default(), started).unwrap();
            let mut raw_bytes = 0usize;
            let mut messages = 0usize;
            let mut failure = None;
            let mut before_end = vec![];
            while let Ok(envelope) = receive.recv() {
                if let Message::Data {
                    session,
                    ordinal,
                    received_at,
                    received_mono,
                    payload,
                } = &envelope.message
                {
                    messages += 1;
                    let row = json!({"kind":"data","worker":session.worker,"generation":session.generation,"slot":session.slot,
                        "ordinal":ordinal,"received_at":received_at.to_rfc3339(),"receive_elapsed_us":received_mono.checked_duration_since(started).map(|d| d.as_micros()),
                        "capture_elapsed_us":started.elapsed().as_micros(),"text":payload.as_str()}).to_string();
                    if raw_bytes + row.len() + 1 > 32 << 20 || messages > 20_000 {
                        failure.get_or_insert("raw capture budget exceeded".to_string());
                        owner_stop.send_replace(true);
                    } else if failure.is_none() {
                        writeln!(capture, "{row}").unwrap();
                        raw_bytes += row.len() + 1;
                    }
                } else {
                    let row = match &envelope.message {
                        Message::Started(s) => {
                            json!({"kind":"started","worker":s.worker,"generation":s.generation})
                        }
                        Message::SubscriptionsRequested {
                            session: s,
                            revisions,
                            add,
                            remove,
                        } => {
                            json!({"kind":"requested","worker":s.worker,"generation":s.generation,"slot":s.slot,"revisions":revisions,"add":add,"remove":remove})
                        }
                        Message::RequestsComplete { session: s } => {
                            json!({"kind":"requests_complete","worker":s.worker,"generation":s.generation,"slot":s.slot})
                        }
                        Message::Ended { session: s, reason } => {
                            json!({"kind":"ended","worker":s.worker,"generation":s.generation,"reason":reason.chars().take(128).collect::<String>()})
                        }
                        _ => panic!("unexpected fixture control"),
                    };
                    writeln!(capture, "{row}").unwrap();
                }
                if matches!(envelope.message, Message::Ended { .. }) {
                    before_end = processor
                        .coverage()
                        .map(|r| {
                            json!({"asset":r.asset,
                        "generation":r.binding.session.generation,"revision":r.binding.revision,
                        "live":r.state==Validity::Live,"selected":r.selected,"levels":r.levels,
                        "input_cutoff":r.input_cutoff,"sampled_at":chrono::Utc::now().to_rfc3339()})
                        })
                        .collect();
                }
                if failure.is_none()
                    && let Err(error) =
                        processor.process(&envelope.message, started.elapsed(), &mut |_| Ok(()))
                {
                    failure = Some(error.to_string());
                    owner_stop.send_replace(true);
                }
            }
            if let Err(error) = processor.finish() {
                failure.get_or_insert(error.to_string());
            }
            capture.flush().unwrap();
            let missing: Vec<_> = sample_assets
                .iter()
                .filter(|id| {
                    !before_end
                        .iter()
                        .any(|r| r["asset"] == **id && r["live"] == true)
                })
                .collect();
            json!({"version":version,"scope":"sample only; no SBE archive or latency measurement",
                "messages":messages,"raw_bytes":raw_bytes,"snapshots":processor.stats().snapshots,
                "changes":processor.stats().changes,"rejected":processor.stats().rejected,
                "missing_initialized":missing,"before_end":before_end,"error":failure})
        });
        let mut worker = tokio::spawn(run_worker(
            0,
            id,
            assets,
            commands,
            send,
            config,
            stop.clone(),
        ));
        let result = tokio::select! {
            result = &mut worker => result,
            _ = tokio::time::sleep(Duration::from_secs(90)) => {
                stop.send_replace(true);
                tokio::time::timeout(Duration::from_secs(15), &mut worker).await.expect("probe shutdown deadline")
            }
        };
        let mut report = owner.join().unwrap();
        if let Err(error) = result.unwrap() {
            report["error"] = json!(error.to_string());
        }
        let after_started_at = chrono::Utc::now();
        let ids: Vec<_> = markets.iter().map(|m| m.id.clone()).collect();
        let mut query = vec![("version", version), ("limit", "100"), ("closed", "any")];
        query.extend(ids.iter().map(|id| ("id", id.as_str())));
        match fetch_page(
            &client,
            id,
            &query,
            &path.join(format!("{version}-gamma-after.json")),
        )
        .await
        {
            Ok(page) if page.next_cursor.is_none() => {
                registry.verify_records(id, &ids, page.markets)
            }
            Ok(_) => report["bracket_error"] = json!("targeted sample response was incomplete"),
            Err(error) => report["bracket_error"] = json!(error.to_string()),
        }
        let desired_after = registry.snapshot();
        let rows = |s: &engineering_challenge::discovery::Snapshot| {
            s.markets
                .iter()
                .map(|(key, entry)| json!({"key":key,"entry":entry}))
                .collect::<Vec<_>>()
        };
        report["desired_before"] = json!(rows(&desired_before));
        report["desired_after"] = json!(rows(&desired_after));
        report["bracket_times"] = json!({"before_request":discovery_at.to_rfc3339(),
            "after_request":after_started_at.to_rfc3339(),"after_sample":chrono::Utc::now().to_rfc3339()});
        report["sampling_uncertain_markets"] = json!(
            ids.iter()
                .filter(|native| {
                    let key = engineering_challenge::discovery::MarketKey {
                        adapter: id,
                        id: (*native).clone(),
                    };
                    desired_before.markets.get(&key) != desired_after.markets.get(&key)
                        || report["bracket_error"].is_string()
                })
                .collect::<Vec<_>>()
        );
        results.push(report);
    }
    let mut report = create(path.join("report.json"));
    serde_json::to_writer_pretty(&mut report, &results).unwrap();
    report.flush().unwrap();
    println!("{}", serde_json::to_string(&results).unwrap());
    assert!(
        results.iter().all(|r| r["error"].is_null()
            && r["bracket_error"].is_null()
            && r["sampling_uncertain_markets"]
                .as_array()
                .is_some_and(Vec::is_empty)
            && r["rejected"] == 0
            && r["changes"].as_u64().is_some_and(|n| n > 0)
            && r["missing_initialized"]
                .as_array()
                .is_some_and(Vec::is_empty)),
        "sample live delta acceptance failed; preserve report and raw evidence"
    );
}

#[test]
#[ignore = "offline capacity worksheet from a completed R0_DISCOVERY_EVIDENCE file; no sockets"]
fn project_current_universe_capacity() {
    use engineering_challenge::{
        discovery::{Instrument, MarketEntry, MarketKey, Snapshot},
        websocket::Assignments,
    };
    use std::{
        collections::BTreeMap,
        io::{BufRead, BufReader},
    };
    let path =
        std::env::var("R0_DISCOVERY_EVIDENCE").expect("completed discovery evidence required");
    let mut snapshot = Snapshot::default();
    let mut complete = false;
    let mut accepted = false;
    let mut scans = vec![];
    for line in BufReader::new(std::fs::File::open(path).unwrap()).lines() {
        let row: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let event = &row["event"];
        complete = event["kind"] == "complete";
        match event["kind"].as_str().unwrap() {
            "report" => accepted = event["coverage_accepted"].as_bool().unwrap(),
            "scan" => scans.push(event["report"].clone()),
            "desired" => {
                let adapter = match event["key"]["adapter"].as_str().unwrap() {
                    "polymarket_v1" => AdapterId::PolymarketV1,
                    "polymarket_v2" => AdapterId::PolymarketV2,
                    other => panic!("unknown adapter {other}"),
                };
                let instruments = event["entry"]["instruments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|i| Instrument {
                        id: i["id"].as_str().unwrap().into(),
                        label: i["label"].as_str().unwrap().into(),
                    })
                    .collect();
                assert!(
                    snapshot
                        .markets
                        .insert(
                            MarketKey {
                                adapter,
                                id: event["key"]["id"].as_str().unwrap().into()
                            },
                            MarketEntry {
                                instruments,
                                desired: true,
                                uncertainty: event["entry"]["uncertainty"]
                                    .as_str()
                                    .map(str::to_owned),
                            }
                        )
                        .is_none(),
                    "duplicate final desired identity"
                );
            }
            _ => {}
        }
    }
    assert!(
        complete && snapshot.desired_assets() > 0,
        "incomplete or empty evidence"
    );
    let config = websocket::Config::default();
    let mut assignments = Assignments::new(config.assets_per_socket, config.max_sockets);
    assignments.reconcile(&snapshot).unwrap();
    let mut workers = BTreeMap::new();
    for assignment in assignments.current() {
        let load = workers
            .entry((assignment.market.adapter, assignment.worker))
            .or_insert(0usize);
        *load += assignment.assets.len();
    }
    assert!(workers.values().all(|n| *n <= config.assets_per_socket));
    assert_eq!(workers.values().sum::<usize>(), snapshot.desired_assets());
    let assets = snapshot.desired_assets();
    let replicas = assets * 2;
    let mut slots = vec![];
    for slot in [0, 1] {
        for adapter in [AdapterId::PolymarketV1, AdapterId::PolymarketV2] {
            let loads: Vec<_> = workers
                .iter()
                .filter(|((a, _), _)| *a == adapter)
                .map(|(_, n)| *n)
                .collect();
            slots.push(json!({"slot":slot,"adapter":adapter,"packed_workers":loads.len(),"assets":loads.iter().sum::<usize>(),"minimum_load":loads.iter().min(),"maximum_load":loads.iter().max()}));
        }
    }
    println!(
        "{}",
        json!({"kind":"capacity_projection","discovery_coverage_accepted":accepted,"scans":scans,
        "desired_assets":assets,"slots":slots,"two_copy_replicas":replicas,"two_copy_sockets":workers.len()*2,
        "configured_socket_cap":config.max_sockets,"fits_socket_cap":workers.len()*2<=config.max_sockets,
        "configured_total_levels_including_replacement":BookConfig::default().total_levels,
        "levels_per_replica_before_replacement":BookConfig::default().total_levels as f64/replicas as f64,
        "replica_struct_bytes_excluding_heap":replicas*std::mem::size_of::<orderbook::Replica>(),
        "max_reassembled_payload_bytes_per_socket":config.max_message_bytes,
        "max_write_buffer_bytes_per_socket":256<<10,"ingress_byte_budget":64<<20,"output_pool_bytes":68157440,
        "qualification":"offline initial packing only; no initialized replicas, depth distribution, RSS or service-rate guarantee"})
    );
}
