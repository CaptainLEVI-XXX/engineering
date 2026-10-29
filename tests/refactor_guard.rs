//! Refactor characterization guard, not a correctness oracle: expected
//! financial values come from the independent-oracle tests. This drives seeded
//! workloads through the public C4 API and renders every observation plus final
//! replica/stats state. A behavior-preserving refactor reproduces the trace:
//!   REFACTOR_TRACE=before.txt cargo test --release --test refactor_guard  # old tree
//!   REFACTOR_GOLDEN=before.txt cargo test --release --test refactor_guard # new tree
use chrono::{DateTime, TimeZone, Utc};
use engineering_challenge::{
    Side,
    discovery::AdapterId,
    orderbook::{Config, Observation, ObservationKind, Processor, SelectionPolicy},
    websocket::ingress::{Message, Session},
};
use serde_json::json;
use std::{
    fmt::Write as _,
    time::{Duration, Instant},
};

const ADAPTER: AdapterId = AdapterId::PolymarketV1;
const ASSETS: [&str; 6] = ["A0", "A1", "B0", "B1", "C0", "C1"];

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn render(out: &mut String, o: &Observation<'_>) {
    let _ = write!(
        out,
        "obs input={} asset={} binding={:?} meta={:?} ",
        o.input, o.asset, o.binding, o.metadata
    );
    let _ = match &o.kind {
        ObservationKind::StandbyBaseline => writeln!(out, "standby"),
        ObservationKind::Snapshot { book, origin } => {
            let bids: Vec<_> = book
                .levels(Side::Buy)
                .map(|(p, q)| (p.value(), q.value()))
                .collect();
            let asks: Vec<_> = book
                .levels(Side::Sell)
                .map(|(p, q)| (p.value(), q.value()))
                .collect();
            writeln!(
                out,
                "snapshot origin={origin:?} bids={bids:?} asks={asks:?}"
            )
        }
        ObservationKind::Level {
            side,
            price,
            quantity,
        } => {
            writeln!(out, "level {side:?} {} {}", price.value(), quantity.value())
        }
        ObservationKind::Invalidated {
            reason,
            cause,
            output_binding,
        } => {
            writeln!(
                out,
                "invalidated cause={cause:?} output={output_binding:?} reason={reason}"
            )
        }
    };
}

struct Run {
    p: Processor,
    origin: Instant,
    out: String,
    ordinals: [u64; 4],
    generations: [u64; 4],
    live: [bool; 4],
    now_ms: u64,
}

impl Run {
    fn at(&self) -> DateTime<Utc> {
        Utc.timestamp_opt(1_000 + (self.now_ms / 1000) as i64, 0)
            .unwrap()
    }
    fn now(&self) -> Duration {
        Duration::from_millis(self.now_ms)
    }
    fn session(&self, worker: usize) -> Session {
        Session {
            worker,
            generation: self.generations[worker],
            adapter: ADAPTER,
            slot: (worker % 2) as u16,
        }
    }
    /// Returns false once the processor has failed; the run then stops.
    fn process(&mut self, m: Message) -> bool {
        let now = self.now();
        let out = &mut self.out;
        match self.p.process(&m, now, &mut |o| {
            render(out, &o);
            Ok(())
        }) {
            Ok(()) => true,
            Err(e) => {
                let _ = writeln!(self.out, "process error: {e:#}");
                false
            }
        }
    }
    fn data(&mut self, worker: usize, payload: serde_json::Value) -> bool {
        self.ordinals[worker] += 1;
        let m = Message::Data {
            session: self.session(worker),
            ordinal: self.ordinals[worker],
            received_at: self.at(),
            received_mono: self.origin + self.now(),
            payload: payload.to_string().into(),
        };
        self.process(m)
    }
    fn start(&mut self, worker: usize, assets: &[&str]) -> bool {
        self.generations[worker] += 1;
        self.ordinals[worker] = 0;
        self.live[worker] = true;
        let s = self.session(worker);
        self.process(Message::Started(s))
            && self.process(Message::SubscriptionsRequested {
                session: s,
                revisions: vec![1; assets.len()],
                add: assets.iter().map(|a| (*a).into()).collect(),
                remove: vec![],
            })
            && self.process(Message::RequestsComplete { session: s })
    }
}

fn price(rng: &mut Rng) -> String {
    format!("0.{:02}", 1 + rng.below(9) * 10)
}
fn size(rng: &mut Rng) -> String {
    match rng.below(5) {
        0 => "0".into(),
        1 => format!("{}.5", rng.below(9)),
        _ => format!("{}", 1 + rng.below(50)),
    }
}

fn workload(seed: u64) -> String {
    let origin = Instant::now();
    let config = Config {
        selection: if seed.is_multiple_of(2) {
            SelectionPolicy::Automatic
        } else {
            SelectionPolicy::Manual
        },
        monitoring: true,
        renewal_interval: Duration::from_secs(900),
        levels_per_side: 5,
        total_levels: 40,
        snapshot_interval: Some(Duration::from_secs(60)),
        ..Config::default()
    };
    let mut run = Run {
        p: Processor::with_receive_clock(config, origin).unwrap(),
        origin,
        out: String::new(),
        ordinals: [0; 4],
        generations: [0; 4],
        live: [false; 4],
        now_ms: 0,
    };
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut revision = 1;
    let ok = run.process(Message::ReconcileBegin {
        revision,
        chunks: 1,
    }) && run.process(Message::DesiredChunk {
        adapter: ADAPTER,
        revision,
        index: 0,
        add: ASSETS.iter().map(|a| (*a).into()).collect(),
        remove: vec![],
    }) && run.process(Message::ReconcileComplete { revision });
    assert!(ok, "initial reconciliation\n{}", run.out);
    // Workers 0/1 cover A,B on slots 0/1; workers 2/3 cover C on slots 0/1.
    let scope =
        |w: usize| -> &'static [&'static str] { if w < 2 { &ASSETS[..4] } else { &ASSETS[4..] } };
    for w in 0..4 {
        assert!(run.start(w, scope(w)), "start\n{}", run.out);
    }
    'steps: for step in 0..1500 {
        run.now_ms += 50 + rng.below(4000);
        let w = rng.below(4) as usize;
        let assets = scope(w);
        let action = rng.below(100);
        let _ = writeln!(
            run.out,
            "-- step {step} t={} w={w} action={action}",
            run.now_ms
        );
        let alive = match action {
            _ if !run.live[w] => run.start(w, assets),
            0..=24 => {
                let asset = assets[rng.below(assets.len() as u64) as usize];
                let deep = rng.below(8) == 0;
                let depth = rng.below(if deep { 9 } else { 4 });
                let mut seen = std::collections::BTreeSet::new();
                let mut bids = vec![];
                for _ in 0..depth {
                    let p = price(&mut rng);
                    if seen.insert(p.clone()) {
                        bids.push(json!({"price": p, "size": size(&mut rng)}));
                    }
                }
                let asks = vec![json!({"price":"0.95","size": size(&mut rng)})];
                run.data(
                    w,
                    json!({"event_type":"book","asset_id":asset,
                    "timestamp": (1_700_000_000_000u64 + run.now_ms).to_string(),
                    "bids":bids,"asks":asks}),
                )
            }
            25..=69 => {
                let n = 1 + rng.below(4);
                let entries: Vec<_> = (0..n)
                    .map(|_| {
                        let asset = match rng.below(40) {
                            0 => "ZZ-unknown",
                            1 => scope(3 - w)[0], // known, unrequested here
                            _ => assets[rng.below(assets.len() as u64) as usize],
                        };
                        let p = if rng.below(60) == 0 {
                            "x.1".into()
                        } else {
                            price(&mut rng)
                        };
                        json!({"asset_id":asset,"side":if rng.below(2)==0 {"BUY"} else {"SELL"},
                            "price":p,"size":size(&mut rng)})
                    })
                    .collect();
                run.data(
                    w,
                    json!({"event_type":"price_change",
                    "timestamp": (1_700_000_000_000u64 + run.now_ms).to_string(),
                    "price_changes":entries}),
                )
            }
            70..=72 => run.data(
                w,
                json!({"event_type":"last_trade_price","asset_id":assets[0]}),
            ),
            73..=75 => {
                run.live[w] = false;
                let s = run.session(w);
                run.process(Message::Ended {
                    session: s,
                    reason: "guard disconnect".into(),
                })
            }
            76..=79 => {
                let asset = ASSETS[rng.below(ASSETS.len() as u64) as usize];
                let slot = rng.below(2) as u16;
                let (now, at) = (run.now(), run.at());
                let out = &mut run.out;
                if let Err(e) = run.p.select(ADAPTER, asset, slot, now, at, &mut |o| {
                    render(out, &o);
                    Ok(())
                }) {
                    let _ = writeln!(run.out, "select error: {e:#}");
                }
                true
            }
            80..=81 => {
                // Retire then readmit one asset through ordered reconciliation.
                let asset = ASSETS[rng.below(ASSETS.len() as u64) as usize].to_string();
                let mut ok = true;
                for add in [false, true] {
                    revision += 1;
                    ok =
                        ok && run.process(Message::ReconcileBegin {
                            revision,
                            chunks: 1,
                        }) && run.process(Message::DesiredChunk {
                            adapter: ADAPTER,
                            revision,
                            index: 0,
                            add: if add { vec![asset.clone()] } else { vec![] },
                            remove: if add { vec![] } else { vec![asset.clone()] },
                        }) && run.process(Message::ReconcileComplete { revision });
                }
                ok
            }
            _ => {
                let (now, at) = (run.now(), run.at());
                let out = &mut run.out;
                let maintained = run.p.maintain(now, at, true, &mut |o| {
                    render(out, &o);
                    Ok(())
                });
                let ticked = maintained.and_then(|()| run.p.tick(now));
                let captured = ticked.and_then(|()| {
                    run.p.capture_due(&mut || (now, at), &mut |o| {
                        render(out, &o);
                        Ok(())
                    })
                });
                if let Err(e) = captured {
                    let _ = writeln!(run.out, "timer error: {e:#}");
                    break 'steps;
                }
                let recovery = run.p.take_recovery();
                let _ = writeln!(run.out, "recovery {recovery:?}");
                for s in recovery {
                    run.live[s.worker] = false;
                    run.process(Message::Ended {
                        session: s,
                        reason: "guard recovery".into(),
                    });
                }
                true
            }
        };
        if !alive {
            break;
        }
        // Deliver owner requests promptly, as the collector loop does.
        while let Some((s, planned)) = run.p.pending_recovery() {
            let _ = writeln!(run.out, "recovery {s:?} planned={planned}");
            run.p.recovery_delivered(s);
            run.live[s.worker] = false;
            if !run.process(Message::Ended {
                session: s,
                reason: "guard recovery".into(),
            }) {
                break 'steps;
            }
        }
        let ready: Vec<_> = run.p.initialized_sessions().collect();
        for s in ready {
            let _ = writeln!(run.out, "initialized {s:?}");
            run.p.initialization_delivered(s);
        }
    }
    for asset in ASSETS {
        for slot in 0..2 {
            let _ = writeln!(
                run.out,
                "final {asset}/{slot}: {:?}",
                run.p.replica(ADAPTER, asset, slot)
            );
        }
    }
    let _ = writeln!(run.out, "stats {:?}", run.p.stats());
    run.out
}

#[test]
fn seeded_workloads_reproduce_the_characterized_trace() {
    let mut all = String::new();
    for seed in 1..=24 {
        let _ = writeln!(all, "==== seed {seed}");
        all.push_str(&workload(seed));
    }
    // Guard the guard: the workload must reach the paths a refactor moves.
    for needle in [
        "snapshot origin=Exchange",
        "snapshot origin=Periodic",
        "snapshot origin=Selection",
        "level Buy",
        "cause=ResourceLimit",
        "cause=InvalidData",
        "cause=SourceEnded",
        "cause=Retired",
        "standby",
    ] {
        assert!(all.contains(needle), "workload never reached {needle}");
    }
    if let Ok(path) = std::env::var("REFACTOR_TRACE") {
        std::fs::write(path, &all).unwrap();
    }
    if let Ok(path) = std::env::var("REFACTOR_GOLDEN") {
        let golden = std::fs::read_to_string(&path).unwrap();
        if let Some((line, (want, got))) = golden
            .lines()
            .zip(all.lines())
            .enumerate()
            .find(|(_, (want, got))| want != got)
        {
            panic!(
                "trace diverges at line {}:\n want {want}\n  got {got}",
                line + 1
            );
        }
        assert_eq!(
            golden.lines().count(),
            all.lines().count(),
            "trace length differs"
        );
    }
}
