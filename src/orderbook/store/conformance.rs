//! Synthetic R6 conformance, compiled only as a child of `orderbook::store`.
//!
//! This is deliberately NOT Kalshi and makes no exchange-compatibility claim.
//! ALT/1 uses line envelopes, signed decimal changes, and one sequence per
//! subscribed instrument per session. A sequence violation poisons only that
//! subscription until a new session decoder is constructed. A snapshot cannot
//! silently bypass a sequence gap on the old session.
//!
//! Tests enter the production decode/error/mutation path through decode_message
//! and send observations through Recorder, Output and the supplied SBE reader.
//! The loopback fixture also runs the actual worker with a statically dispatched
//! alternate protocol, real bounded ingress and fresh session-local decoders.
//! Production still chooses its protocol/decoder by AdapterId; PolymarketV1 here
//! is only the existing identity namespace needed by the generic owner/recorder.

use super::Processor;
use crate::{
    Side,
    discovery::AdapterId,
    market_data::{
        self, Change, DecodeError, Event, Level, Limits, Price, Quantity, QuantityChange,
        SessionDecoder, VisitError,
    },
    orderbook::{Config, Observation, ObservationKind, SelectionPolicy, Validity},
    recording::{self, Output, Recorder},
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
    websocket::{
        ingress::{Message, Session},
        protocol::{ControlFrame, SubscriptionOp, WireProtocol},
    },
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::{borrow::Cow, collections::HashMap, time::Duration};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::Request};

#[derive(Clone)]
struct AlternateProtocol;

impl WireProtocol for AlternateProtocol {
    fn handshake_request(&self, endpoint: &str) -> Result<Request<()>> {
        let mut request = endpoint.into_client_request()?;
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "alt.l2.v1".parse()?);
        Ok(request)
    }

    fn subscription(&self, operation: SubscriptionOp, assets: &[String]) -> Result<String> {
        let verb = match operation {
            SubscriptionOp::Initial => "OPEN",
            SubscriptionOp::Add => "WATCH",
            SubscriptionOp::Remove => "UNWATCH",
        };
        Ok(format!("{verb} {}", serde_json::to_string(assets)?))
    }

    fn heartbeat(&self) -> Option<&'static str> {
        Some("ALIVE?")
    }

    fn classify(&self, text: &str) -> ControlFrame {
        if text == "ALIVE!" {
            ControlFrame::ApplicationPong
        } else {
            ControlFrame::Data
        }
    }
}

#[derive(Clone, Copy)]
enum Sequence {
    AwaitingBaseline,
    Live(u64),
    Broken,
}

struct AlternateDecoder {
    subscriptions: HashMap<String, Sequence>,
}

impl AlternateDecoder {
    fn new(assets: &[&str]) -> Self {
        Self {
            subscriptions: assets
                .iter()
                .map(|id| ((*id).to_owned(), Sequence::AwaitingBaseline))
                .collect(),
        }
    }

    /// BASE|instrument|sequence|exchange_us|B,price,quantity;S,price,quantity
    /// MOVE|instrument|sequence|exchange_us|B,price,signed_delta;...
    /// Every entry in one MOVE becomes part of ONE atomic normalized event.
    /// An empty exchange_us field means explicitly unknown, never receive time.
    fn line<'a>(&mut self, line: &'a str, limits: &Limits) -> Result<Event<'a>, DecodeError> {
        let fields: Vec<_> = line.split('|').collect();
        let asset = fields.get(1).copied().filter(|id| {
            !id.is_empty()
                && id.len() <= limits.identity_bytes
                && self.subscriptions.contains_key(*id)
        });
        let decoded = (|| -> Result<(Event<'a>, u64)> {
            ensure!(fields.len() == 5, "invalid ALT field count");
            let asset = asset.context("unknown or invalid ALT subscription")?;
            let baseline = match fields[0] {
                "BASE" => true,
                "MOVE" => false,
                _ => anyhow::bail!("unknown ALT operation"),
            };
            let sequence = unsigned_integer(fields[2])?;
            match self.subscriptions[asset] {
                Sequence::AwaitingBaseline => ensure!(baseline, "ALT change before baseline"),
                Sequence::Live(last) => ensure!(
                    last.checked_add(1) == Some(sequence),
                    "ALT sequence gap/replay"
                ),
                Sequence::Broken => {
                    anyhow::bail!("ALT subscription requires fresh session baseline")
                }
            }
            let exchange_us = if fields[3].is_empty() {
                None
            } else {
                let micros = unsigned_integer(fields[3])?;
                ensure!(
                    DateTime::<Utc>::from_timestamp_micros(i64::try_from(micros)?).is_some(),
                    "ALT exchange time outside recording range"
                );
                Some(micros)
            };
            let entries = if fields[4].is_empty() {
                0
            } else {
                fields[4].split(';').count()
            };
            ensure!(entries <= limits.entries, "resource limit: ALT entries");
            ensure!(baseline || entries > 0, "empty ALT change event");
            let mut bids = Vec::new();
            let mut asks = Vec::new();
            let mut changes = Vec::new();
            for entry in fields[4].split(';').take(entries) {
                let fields: Vec<_> = entry.split(',').collect();
                ensure!(fields.len() == 3, "invalid ALT level");
                let side = match fields[0] {
                    "B" => Side::Buy,
                    "S" => Side::Sell,
                    _ => anyhow::bail!("invalid ALT side"),
                };
                // Unlike Polymarket, ALT prices may exceed one. No float path.
                let price = Price::new(market_data::parse_scaled(fields[1])?)?;
                if baseline {
                    let level = Level {
                        price,
                        quantity: Quantity::new(market_data::parse_scaled(fields[2])?)?,
                    };
                    match side {
                        Side::Buy => bids.push(level),
                        Side::Sell => asks.push(level),
                    }
                } else {
                    changes.push(Change {
                        asset: Cow::Borrowed(asset),
                        side,
                        price,
                        quantity: QuantityChange::Add(signed_scaled(fields[2])?),
                    });
                }
            }
            Ok((
                if baseline {
                    Event::Snapshot {
                        asset: Cow::Borrowed(asset),
                        exchange_us,
                        bids,
                        asks,
                    }
                } else {
                    Event::Changes {
                        exchange_us,
                        changes,
                    }
                },
                sequence,
            ))
        })();
        match decoded {
            Ok((event, sequence)) => {
                *self.subscriptions.get_mut(asset.unwrap()).unwrap() = Sequence::Live(sequence);
                Ok(event)
            }
            Err(error) => {
                if let Some(asset) = asset {
                    *self.subscriptions.get_mut(asset).unwrap() = Sequence::Broken;
                }
                let reason = error.to_string();
                Err(DecodeError {
                    assets: asset.map(|asset| vec![asset.to_owned()]),
                    resource: reason.starts_with("resource limit:"),
                    reason,
                })
            }
        }
    }
}

impl SessionDecoder for AlternateDecoder {
    fn visit<'a>(
        &mut self,
        text: &'a str,
        limits: &Limits,
        emit: &mut dyn FnMut(Result<Event<'a>, DecodeError>) -> Result<()>,
    ) -> Result<(), VisitError> {
        let lines = (|| -> Result<&str> {
            ensure!(
                text.len() <= limits.message_bytes,
                "resource limit: ALT message bytes"
            );
            let body = text
                .strip_prefix("ALT/1\n")
                .context("invalid ALT envelope")?;
            ensure!(!body.is_empty(), "empty ALT envelope");
            ensure!(
                body.lines().count() <= limits.events,
                "resource limit: ALT events"
            );
            Ok(body)
        })()
        .map_err(VisitError::Envelope)?;
        for line in lines.lines() {
            emit(self.line(line, limits)).map_err(VisitError::Sink)?;
        }
        Ok(())
    }
}

fn unsigned_integer(text: &str) -> Result<u64> {
    ensure!(
        !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()),
        "invalid ALT unsigned integer"
    );
    Ok(text.parse()?)
}

/// Signed fixed-point fixture grammar: optional '-', digits, optional fractional
/// digits; no '+', exponent, whitespace or rounding. Includes the full i64 range.
fn signed_scaled(text: &str) -> Result<i64> {
    let (negative, magnitude) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (whole, fraction) = magnitude
        .split_once('.')
        .map_or((magnitude, None), |(a, b)| (a, Some(b)));
    let mut value = unsigned_integer(whole)?
        .checked_mul(1_000_000)
        .context("ALT decimal overflow")?;
    if let Some(fraction) = fraction {
        ensure!(
            !fraction.is_empty() && fraction.bytes().all(|b| b.is_ascii_digit()),
            "invalid ALT fraction"
        );
        let mut scale = 100_000;
        for digit in fraction.bytes() {
            let digit = u64::from(digit - b'0');
            if scale == 0 {
                ensure!(digit == 0, "ALT unrepresentable precision");
            } else {
                value = value
                    .checked_add(digit * scale)
                    .context("ALT decimal overflow")?;
                scale /= 10;
            }
        }
    }
    let signed = if negative {
        -i128::from(value)
    } else {
        i128::from(value)
    };
    Ok(i64::try_from(signed)?)
}

fn session(worker: usize, slot: u16, generation: u64) -> Session {
    Session {
        adapter: AdapterId::PolymarketV1,
        worker,
        slot,
        generation,
    }
}

fn capture(
    trace: &mut Vec<Value>,
    recorder: &mut Recorder,
    observation: Observation<'_>,
) -> Result<()> {
    let kind = match &observation.kind {
        ObservationKind::StandbyBaseline => json!({"kind":"standby_baseline"}),
        ObservationKind::Snapshot { book, .. } => json!({
            "kind":"snapshot",
            "bids":book.levels(Side::Buy).map(|(p,q)| [p.value(),q.value()]).collect::<Vec<_>>(),
            "asks":book.levels(Side::Sell).map(|(p,q)| [p.value(),q.value()]).collect::<Vec<_>>(),
        }),
        ObservationKind::Level {
            side,
            price,
            quantity,
        } => json!({
            "kind":"level", "side":if *side == Side::Buy {"B"} else {"S"},
            "price":price.value(), "quantity":quantity.value(),
        }),
        ObservationKind::Invalidated {
            reason,
            cause,
            output_binding,
        } => json!({
            "kind":"unavailable", "reason":reason, "cause":cause, "selected":output_binding.is_some(),
        }),
    };
    trace.push(json!({
        "asset":observation.asset, "session":{
            "worker":observation.binding.session.worker,
            "slot":observation.binding.session.slot,
            "generation":observation.binding.session.generation,
        },
        "exchange_us":observation.metadata.and_then(|m| m.exchange_us),
        "entry":observation.metadata.and_then(|m| m.cursor.entry), "data":kind,
    }));
    recorder.observe(observation)
}

struct Harness {
    processor: Processor,
    recorder: Recorder,
    output: Output,
    trace: Vec<Value>,
    tick: u64,
    ordinals: HashMap<Session, u64>,
}

impl Harness {
    fn new() -> Self {
        let config = Config {
            selection: SelectionPolicy::Manual,
            monitoring: false,
            instruments: 8,
            replicas: 16,
            sessions: 4,
            levels_per_side: 8,
            total_levels: 128,
            ..Config::default()
        };
        // A finite fixture fits this pool before Output runs, so no thread,
        // unbounded producer queue or wall-clock-dependent flush is needed.
        let recording = recording::Config {
            batches: 2,
            batch_bytes: 64 << 10,
            publish_bytes: 64 << 10,
            entries: 256,
            ..recording::Config::default()
        };
        let (recorder, output) = recording::channel(recording, &config).unwrap();
        Self {
            processor: Processor::new(config).unwrap(),
            recorder,
            output,
            trace: Vec::new(),
            tick: 0,
            ordinals: HashMap::new(),
        }
    }

    fn control(&mut self, message: Message) {
        self.tick += 1;
        self.processor
            .process(
                &message,
                Duration::from_secs(self.tick),
                &mut |observation| capture(&mut self.trace, &mut self.recorder, observation),
            )
            .unwrap();
    }

    fn start(&mut self, session: Session, assets: &[&str]) {
        self.control(Message::Started(session));
        self.control(Message::SubscriptionsRequested {
            session,
            revisions: vec![1; assets.len()],
            add: assets.iter().map(|id| (*id).to_owned()).collect(),
            remove: vec![],
        });
        self.ordinals.insert(session, 0);
    }

    fn end(&mut self, session: Session) {
        self.control(Message::Ended {
            session,
            reason: "ALT fixture cutoff".into(),
        });
        self.ordinals.remove(&session);
    }

    fn feed(
        &mut self,
        decoder: &mut impl SessionDecoder,
        session: Session,
        body: &str,
    ) -> Result<()> {
        self.tick += 1;
        let ordinal = self
            .ordinals
            .get_mut(&session)
            .context("fixture session not started")?;
        *ordinal += 1;
        let now = Duration::from_secs(self.tick);
        self.processor.clock(now)?;
        self.processor.received_time = now;
        let received_at =
            DateTime::from_timestamp_micros(1_000_000_000 + self.tick as i64 * 1000).unwrap();
        self.processor.decode_message(
            decoder,
            session,
            *ordinal,
            received_at,
            body,
            &mut |observation| capture(&mut self.trace, &mut self.recorder, observation),
        )
    }

    fn quantity(&self, session: Session, asset: &str, side: Side, price: i64) -> Option<i64> {
        self.processor
            .replica(session.adapter, asset, session.slot)
            .unwrap()
            .book
            .quantity(side, Price::new(price).unwrap())
            .map(Quantity::value)
    }

    fn finish(mut self) -> (Vec<Value>, Vec<Value>) {
        let mut sessions: Vec<_> = self.ordinals.keys().copied().collect();
        sessions.sort_by_key(|s| (s.worker, s.generation));
        for session in sessions {
            self.end(session);
        }
        self.recorder.finish(self.processor.finish()).unwrap();
        let archive = tempfile::NamedTempFile::new().unwrap();
        let sidecar = tempfile::NamedTempFile::new().unwrap();
        let (approve, approval) = crossbeam::channel::bounded(1);
        approve.send(true).unwrap();
        let report = self.output.run(
            archive.reopen().unwrap(),
            sidecar.reopen().unwrap(),
            "synthetic ALT conformance (not a live exchange)",
            || {},
            approval,
        );
        assert!(report.error.is_none(), "{report:?}");
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(archive.path()).unwrap();
        let mut records = Vec::new();
        while let Some(message) = reader.read_msg().unwrap() {
            records.push(match message {
                AnyMessage::Snapshot(snapshot) => {
                    let snapshot = SnapshotDomain::try_from_decoder(snapshot).unwrap();
                    json!({"kind":"snapshot", "symbol":snapshot.symbol,
                        "received_us":snapshot.received_at.timestamp_micros(),
                        "exchange_us":snapshot.exchange_timestamp.timestamp_micros(),
                        "bids":snapshot.bids.iter().map(|l| [l.price_level,l.quantity]).collect::<Vec<_>>(),
                        "asks":snapshot.asks.iter().map(|l| [l.price_level,l.quantity]).collect::<Vec<_>>()})
                }
                AnyMessage::PriceLevelUpdate(level) => {
                    let level = PriceLevelUpdateDomain::try_from_decoder(level).unwrap();
                    json!({"kind":"level", "symbol":level.symbol,
                        "received_us":level.backtest_timestamp.timestamp_micros(),
                        "exchange_us":level.exchange_timestamp.timestamp_micros(),
                        "side":if level.side == Side::Buy {"B"} else {"S"},
                        "price":level.price_level, "quantity":level.quantity})
                }
                _ => panic!("unexpected ALT archive record"),
            });
        }
        assert_eq!(report.records_confirmed, records.len() as u64);
        let integrity = std::fs::read_to_string(sidecar.path())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        (records, integrity)
    }
}

#[test]
fn alternate_wire_commands_and_heartbeat_are_not_polymarket() {
    let protocol = AlternateProtocol;
    // Request construction only: .invalid is never connected or resolved.
    let request = protocol
        .handshake_request("wss://alt.invalid/feed")
        .unwrap();
    assert_eq!(request.headers()["sec-websocket-protocol"], "alt.l2.v1");
    let assets = vec!["A".into(), "opaque\"B".into()];
    for (op, verb) in [
        (SubscriptionOp::Initial, "OPEN"),
        (SubscriptionOp::Add, "WATCH"),
        (SubscriptionOp::Remove, "UNWATCH"),
    ] {
        let wire = protocol.subscription(op, &assets).unwrap();
        let (actual_verb, body) = wire.split_once(' ').unwrap();
        assert_eq!(actual_verb, verb);
        assert_eq!(serde_json::from_str::<Vec<String>>(body).unwrap(), assets);
    }
    assert_eq!(protocol.heartbeat(), Some("ALIVE?"));
    assert!(matches!(
        protocol.classify("ALIVE!"),
        ControlFrame::ApplicationPong
    ));
    assert!(matches!(protocol.classify("PONG"), ControlFrame::Data));
    assert!(matches!(protocol.classify("ALIVE! "), ControlFrame::Data));
}

#[test]
fn relative_entries_publish_intermediate_absolute_values_through_sbe() {
    let mut harness = Harness::new();
    let session = session(0, 0, 1);
    harness.start(session, &["A"]);
    let mut decoder = AlternateDecoder::new(&["A"]);
    harness
        .feed(&mut decoder, session, "ALT/1\nBASE|A|1|42000000|B,12.5,7")
        .unwrap();
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nMOVE|A|2|42000000|B,12.5,-2;B,12.5,3",
        )
        .unwrap();
    assert_eq!(
        harness.quantity(session, "A", Side::Buy, 12_500_000),
        Some(8_000_000)
    );
    assert_eq!(harness.trace[1]["entry"], 0);
    assert_eq!(harness.trace[2]["entry"], 1);
    let (records, integrity) = harness.finish();
    assert_eq!(
        records,
        vec![
            json!({"kind":"snapshot","symbol":"polymarket:v1:A","received_us":1000003000_i64,
            "exchange_us":42000000,"bids":[[12500000,7000000]],"asks":[]}),
            json!({"kind":"level","symbol":"polymarket:v1:A","received_us":1000004000_i64,
            "exchange_us":42000000,"side":"B","price":12500000,"quantity":5000000}),
            json!({"kind":"level","symbol":"polymarket:v1:A","received_us":1000004000_i64,
            "exchange_us":42000000,"side":"B","price":12500000,"quantity":8000000}),
        ]
    );
    assert_eq!(
        integrity
            .iter()
            .filter(|row| row["kind"] == "snapshot")
            .count(),
        1
    );
    assert!(!integrity.iter().any(|row| row["cause"] == "invalid_data"));
}

#[test]
fn intermediate_negative_rejects_entire_event_even_when_net_change_is_zero() {
    let mut harness = Harness::new();
    let session = session(0, 0, 1);
    harness.start(session, &["A"]);
    let mut decoder = AlternateDecoder::new(&["A"]);
    harness
        .feed(&mut decoder, session, "ALT/1\nBASE|A|1|42000000|B,12.5,7")
        .unwrap();
    let received = harness
        .processor
        .replica(session.adapter, "A", 0)
        .unwrap()
        .last_valid_receive;
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nMOVE|A|2|42000000|B,12.5,-8;B,12.5,8",
        )
        .unwrap();
    let replica = harness.processor.replica(session.adapter, "A", 0).unwrap();
    assert_eq!(replica.state, Validity::Stale);
    assert_eq!(replica.applied, 0);
    assert_eq!(replica.health.accepted_changes(), 0);
    assert_eq!(replica.last_valid_receive, received);
    assert_eq!(
        harness.quantity(session, "A", Side::Buy, 12_500_000),
        Some(7_000_000)
    );
    assert_eq!(harness.trace.len(), 2);
    assert_eq!(harness.trace[1]["data"]["kind"], "unavailable");
    assert_eq!(harness.processor.take_recovery(), vec![session]);
}

#[test]
fn overflow_rejects_all_prior_entries_without_partial_book_or_output_changes() {
    let mut harness = Harness::new();
    let session = session(0, 0, 1);
    harness.start(session, &["A"]);
    let mut decoder = AlternateDecoder::new(&["A"]);
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nBASE|A|1|42000000|B,12.5,7;S,13,9223372036854.775807",
        )
        .unwrap();
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nMOVE|A|2|42000000|B,12.5,1;S,13,0.000001",
        )
        .unwrap();
    assert_eq!(
        harness.quantity(session, "A", Side::Buy, 12_500_000),
        Some(7_000_000)
    );
    assert_eq!(
        harness.quantity(session, "A", Side::Sell, 13_000_000),
        Some(i64::MAX)
    );
    assert_eq!(
        harness
            .processor
            .replica(session.adapter, "A", 0)
            .unwrap()
            .applied,
        0
    );
    assert_eq!(harness.trace.len(), 2);
    assert_eq!(harness.trace[1]["data"]["cause"], "invalid_data");
}

#[test]
fn normalized_multi_asset_event_is_atomic_across_replicas() {
    let mut harness = Harness::new();
    let session = session(0, 0, 1);
    harness.start(session, &["A", "B"]);
    let mut decoder = AlternateDecoder::new(&["A", "B"]);
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nBASE|A|1|42000000|B,12.5,7\nBASE|B|1|42000000|B,12.5,7",
        )
        .unwrap();
    // ALT has subscription-local single-asset messages. Exercise the broader
    // normalized contract directly rather than inventing cross-subscription
    // sequence semantics in the fixture protocol.
    harness.tick += 1;
    let now = Duration::from_secs(harness.tick);
    harness.processor.clock(now).unwrap();
    harness.processor.received_time = now;
    let changes = [("A", 1_000_000), ("B", -8_000_000), ("B", 8_000_000)]
        .into_iter()
        .map(|(asset, delta)| Change {
            asset: Cow::Borrowed(asset),
            side: Side::Buy,
            price: Price::new(12_500_000).unwrap(),
            quantity: QuantityChange::Add(delta),
        })
        .collect();
    harness
        .processor
        .event(
            session,
            2,
            0,
            DateTime::from_timestamp_micros(1_000_004_000).unwrap(),
            Event::Changes {
                exchange_us: Some(42_000_000),
                changes,
            },
            &mut |observation| capture(&mut harness.trace, &mut harness.recorder, observation),
        )
        .unwrap();
    for asset in ["A", "B"] {
        assert_eq!(
            harness.quantity(session, asset, Side::Buy, 12_500_000),
            Some(7_000_000)
        );
        let replica = harness
            .processor
            .replica(session.adapter, asset, 0)
            .unwrap();
        assert_eq!(replica.state, Validity::Stale);
        assert_eq!(replica.applied, 0);
        assert_eq!(replica.health.accepted_changes(), 0);
    }
    assert_eq!(harness.trace.len(), 4);
    assert!(
        harness.trace[2..]
            .iter()
            .all(|row| row["data"]["kind"] == "unavailable")
    );
}

#[test]
fn zero_deletion_absent_zero_and_both_sides_preserve_entry_order() {
    let mut harness = Harness::new();
    let session = session(0, 0, 1);
    harness.start(session, &["A"]);
    let mut decoder = AlternateDecoder::new(&["A"]);
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nBASE|A|1|42000000|B,12.5,7;S,13,2",
        )
        .unwrap();
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nMOVE|A|2|42000000|B,12.5,-7;B,12.5,0;S,13,-2;S,13,1",
        )
        .unwrap();
    assert_eq!(harness.quantity(session, "A", Side::Buy, 12_500_000), None);
    assert_eq!(
        harness.quantity(session, "A", Side::Sell, 13_000_000),
        Some(1_000_000)
    );
    let emitted: Vec<_> = harness.trace[1..]
        .iter()
        .map(|row| row["data"]["quantity"].clone())
        .collect();
    assert_eq!(emitted, vec![json!(0), json!(0), json!(0), json!(1000000)]);
}

#[test]
fn gap_is_subscription_and_session_local_and_new_generation_restores_with_snapshot() {
    let mut harness = Harness::new();
    let primary = session(0, 0, 1);
    let peer = session(1, 1, 1);
    harness.start(primary, &["A", "B"]);
    harness.start(peer, &["A", "B"]);
    let mut decoder = AlternateDecoder::new(&["A", "B"]);
    let mut peer_decoder = AlternateDecoder::new(&["A", "B"]);
    harness
        .feed(
            &mut decoder,
            primary,
            "ALT/1\nBASE|A|10|42000000|B,12.5,7\nBASE|B|100|42000000|B,12.5,11",
        )
        .unwrap();
    harness
        .feed(
            &mut peer_decoder,
            peer,
            "ALT/1\nBASE|A|90|42000000|B,12.5,70\nBASE|B|500|42000000|B,12.5,110",
        )
        .unwrap();
    assert_eq!(harness.trace.len(), 4);
    assert!(
        harness.trace[2..]
            .iter()
            .all(|row| row["data"]["kind"] == "standby_baseline")
    );
    let before = harness.trace.len();
    harness
        .feed(
            &mut decoder,
            primary,
            "ALT/1\nMOVE|A|12|42000000|B,12.5,100\nMOVE|B|101|42000000|B,12.5,1",
        )
        .unwrap();
    assert_eq!(harness.trace.len(), before + 2);
    assert_eq!(harness.trace[before]["asset"], "A");
    assert_eq!(harness.trace[before]["data"]["kind"], "unavailable");
    assert_eq!(harness.trace[before + 1]["asset"], "B");
    assert_eq!(harness.trace[before + 1]["data"]["quantity"], 12_000_000);
    assert_eq!(
        harness.quantity(primary, "A", Side::Buy, 12_500_000),
        Some(7_000_000)
    );
    for (session, asset, state) in [
        (primary, "A", Validity::Stale),
        (primary, "B", Validity::Live),
        (peer, "A", Validity::Live),
        (peer, "B", Validity::Live),
    ] {
        assert_eq!(
            harness
                .processor
                .replica(session.adapter, asset, session.slot)
                .unwrap()
                .state,
            state
        );
    }
    // Sequence state did not leak across replicas; the standby still accepts 91.
    harness
        .feed(
            &mut peer_decoder,
            peer,
            "ALT/1\nMOVE|A|91|42000000|B,12.5,1",
        )
        .unwrap();
    assert_eq!(
        harness.quantity(peer, "A", Side::Buy, 12_500_000),
        Some(71_000_000)
    );
    harness
        .feed(
            &mut decoder,
            primary,
            "ALT/1\nBASE|A|13|42000000|B,12.5,999",
        )
        .unwrap();
    assert_eq!(
        harness.quantity(primary, "A", Side::Buy, 12_500_000),
        Some(7_000_000)
    );
    assert_eq!(harness.processor.take_recovery(), vec![primary]);
    harness.end(primary);
    let fresh = session(0, 0, 2);
    harness.start(fresh, &["A", "B"]);
    let mut fresh_decoder = AlternateDecoder::new(&["A", "B"]);
    harness
        .feed(
            &mut fresh_decoder,
            fresh,
            "ALT/1\nBASE|A|1|42000000|B,12.5,9\nMOVE|A|2|42000000|B,12.5,1",
        )
        .unwrap();
    assert_eq!(
        harness
            .processor
            .replica(fresh.adapter, "A", 0)
            .unwrap()
            .state,
        Validity::Live
    );
    assert_eq!(
        harness.quantity(fresh, "A", Side::Buy, 12_500_000),
        Some(10_000_000)
    );
    let (records, integrity) = harness.finish();
    let financial: Vec<_> = records
        .iter()
        .map(|record| {
            if record["kind"] == "snapshot" {
                json!(["snapshot", record["symbol"], record["bids"][0][1]])
            } else {
                json!(["level", record["symbol"], record["quantity"]])
            }
        })
        .collect();
    assert_eq!(
        financial,
        vec![
            json!(["snapshot", "polymarket:v1:A", 7000000]),
            json!(["snapshot", "polymarket:v1:B", 11000000]),
            json!(["level", "polymarket:v1:B", 12000000]),
            json!(["snapshot", "polymarket:v1:A", 9000000]),
            json!(["level", "polymarket:v1:A", 10000000]),
        ]
    );
    assert_eq!(
        integrity
            .iter()
            .filter(|row| row["kind"] == "standby_baseline")
            .count(),
        2
    );
    let gaps: Vec<_> = integrity
        .iter()
        .filter(|row| row["cause"] == "invalid_data")
        .collect();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["symbol"], "polymarket:v1:A");
    assert_eq!(gaps[0]["next_ordinal"], 2);
    let gap_position = integrity
        .iter()
        .position(|row| row["cause"] == "invalid_data")
        .unwrap();
    let replacement_position = integrity
        .iter()
        .position(|row| {
            row["kind"] == "snapshot" && row["symbol"] == "polymarket:v1:A" && row["ordinal"] == 3
        })
        .unwrap();
    assert!(gap_position < replacement_position);
}

#[test]
fn wider_generic_prices_do_not_relax_polymarket_validation() {
    assert_eq!(Price::new(12_500_000).unwrap().value(), 12_500_000);
    for text in [
        r#"{"event_type":"book","asset_id":"A","timestamp":"42000","bids":[{"price":"12.5","size":"7"}],"asks":[]}"#,
        r#"{"event_type":"price_change","timestamp":"42000","price_changes":[{"asset_id":"A","side":"BUY","price":"12.5","size":"7"}]}"#,
    ] {
        let limits = Limits::default();
        let events = market_data::envelope(text, &limits).unwrap();
        assert!(market_data::decode(events[0], &limits).is_err());
    }
}

#[test]
fn signed_fixture_parser_is_exact_at_boundaries() {
    for (text, expected) in [
        ("-2", -2_000_000),
        ("3", 3_000_000),
        ("-0.000001", -1),
        ("1.2300000", 1_230_000),
        ("9223372036854.775807", i64::MAX),
        ("-9223372036854.775808", i64::MIN),
    ] {
        assert_eq!(signed_scaled(text).unwrap(), expected);
    }
    for text in [
        "",
        "-",
        "+1",
        "--1",
        "1e2",
        " 1",
        "1.",
        ".1",
        "0.0000001",
        "9223372036854.775808",
        "-9223372036854.775809",
    ] {
        assert!(
            signed_scaled(text).is_err(),
            "unexpected acceptance: {text}"
        );
    }
}

#[test]
fn decoder_stops_at_sink_failure_and_does_not_consume_later_sequences() {
    let mut decoder = AlternateDecoder::new(&["A", "B"]);
    let mut callbacks = 0;
    let result = decoder.visit(
        "ALT/1\nBASE|A|1|42000000|B,12.5,7\nBASE|B|100|42000000|B,12.5,11",
        &Limits::default(),
        &mut |_| {
            callbacks += 1;
            anyhow::bail!("fixture sink failure")
        },
    );
    assert!(matches!(result, Err(VisitError::Sink(_))));
    assert_eq!(callbacks, 1);
    assert!(matches!(
        decoder.subscriptions["B"],
        Sequence::AwaitingBaseline
    ));
}

#[test]
fn duplicate_and_exhausted_sequences_never_wrap_or_refresh_a_book() {
    for (first, next) in [(10, 10), (u64::MAX, 0)] {
        let mut harness = Harness::new();
        let session = session(0, 0, 1);
        harness.start(session, &["A"]);
        let mut decoder = AlternateDecoder::new(&["A"]);
        harness
            .feed(
                &mut decoder,
                session,
                &format!("ALT/1\nBASE|A|{first}|42000000|B,12.5,7"),
            )
            .unwrap();
        harness
            .feed(
                &mut decoder,
                session,
                &format!("ALT/1\nMOVE|A|{next}|42000000|B,12.5,1"),
            )
            .unwrap();
        assert_eq!(
            harness.quantity(session, "A", Side::Buy, 12_500_000),
            Some(7_000_000)
        );
        assert_eq!(
            harness
                .processor
                .replica(session.adapter, "A", 0)
                .unwrap()
                .state,
            Validity::Stale
        );
        assert_eq!(harness.trace.len(), 2);
        assert_eq!(harness.trace[1]["data"]["kind"], "unavailable");
    }
}

#[test]
fn invalid_exchange_time_and_malformed_envelopes_follow_normal_error_scopes() {
    let mut harness = Harness::new();
    let session = session(0, 0, 1);
    harness.start(session, &["A", "B"]);
    let mut decoder = AlternateDecoder::new(&["A", "B"]);
    harness
        .feed(
            &mut decoder,
            session,
            "ALT/1\nBASE|A|1|42000000|B,12.5,7\nBASE|B|1|42000000|B,12.5,11",
        )
        .unwrap();
    harness
        .feed(&mut decoder, session, "ALT/1\nMOVE|A|2|not-time|B,12.5,1")
        .unwrap();
    assert_eq!(
        harness
            .processor
            .replica(session.adapter, "A", 0)
            .unwrap()
            .state,
        Validity::Stale
    );
    assert_eq!(
        harness
            .processor
            .replica(session.adapter, "B", 0)
            .unwrap()
            .state,
        Validity::Live
    );
    assert_eq!(
        harness.quantity(session, "A", Side::Buy, 12_500_000),
        Some(7_000_000)
    );
    harness
        .feed(&mut decoder, session, "broken envelope")
        .unwrap();
    assert_eq!(
        harness
            .processor
            .replica(session.adapter, "B", 0)
            .unwrap()
            .state,
        Validity::Stale
    );
    assert!(
        !harness
            .trace
            .iter()
            .any(|row| row["data"]["kind"] == "level")
    );
}

#[test]
fn unknown_exchange_time_normalizes_without_inventing_a_timestamp() {
    let mut decoder = AlternateDecoder::new(&["A"]);
    let mut callbacks = 0;
    let result = decoder.visit(
        "ALT/1\nBASE|A|1||B,12.5,7\nMOVE|A|2||B,12.5,1",
        &Limits::default(),
        &mut |event| {
            match event? {
                Event::Snapshot { exchange_us, .. } | Event::Changes { exchange_us, .. } => {
                    assert_eq!(exchange_us, None);
                    callbacks += 1;
                }
                _ => panic!("unexpected unknown-time event"),
            }
            Ok(())
        },
    );
    assert!(result.is_ok());
    assert_eq!(callbacks, 2);
}

#[test]
fn recorder_rejects_unknown_exchange_time_and_stops_later_events() {
    for missing_snapshot in [true, false] {
        let mut harness = Harness::new();
        let session = session(0, 0, 1);
        harness.start(session, &["A", "B"]);
        let mut decoder = AlternateDecoder::new(&["A", "B"]);
        if !missing_snapshot {
            harness
                .feed(&mut decoder, session, "ALT/1\nBASE|A|1|42000000|B,12.5,7")
                .unwrap();
        }
        let body = if missing_snapshot {
            "ALT/1\nBASE|A|1||B,12.5,7\nBASE|B|1|42000000|B,12.5,11"
        } else {
            "ALT/1\nMOVE|A|2||B,12.5,1\nBASE|B|1|42000000|B,12.5,11"
        };
        let error = harness.feed(&mut decoder, session, body).unwrap_err();
        assert!(
            error.to_string().contains("missing exchange time"),
            "{error:#}"
        );
        assert_eq!(harness.trace.last().unwrap()["exchange_us"], Value::Null);
        assert!(harness.recorder.check_output().is_err());
        assert!(matches!(
            decoder.subscriptions["B"],
            Sequence::AwaitingBaseline
        ));
        assert_eq!(
            harness
                .processor
                .replica(session.adapter, "B", 0)
                .unwrap()
                .state,
            Validity::AwaitingSnapshot
        );
        // A sink failure is terminal, not a malformed-data gap or a promise to
        // roll back a committed book. Do not call finish as if this run succeeded.
        assert_eq!(harness.processor.stats().rejected, 0);
    }
}

#[tokio::test]
async fn alternate_protocol_worker_ingress_decoder_and_recording_round_trip() {
    use crate::websocket::{self, WorkerCommand, ingress, run_worker_with_protocol};
    use futures_util::{SinkExt, StreamExt};
    use std::sync::Arc;
    use tokio::{
        net::TcpListener,
        sync::{mpsc, watch},
        time::{sleep, timeout},
    };
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::{
            Message as Frame,
            handshake::server::{Request, Response},
        },
    };

    const BASE: &str = "ALT/1\nBASE|A|1|42000000|B,12.5,7";
    const CHANGE: &str = "ALT/1\nMOVE|A|2|42000000|B,12.5,-2;B,12.5,3";
    const GAP: &str = "ALT/1\nMOVE|A|4|42000000|B,12.5,100";
    const FRESH: &str = "ALT/1\nBASE|A|1|42000000|B,12.5,9";
    const FOLLOWING: &str = "ALT/1\nMOVE|A|2|42000000|B,12.5,1";

    let (report, heartbeats, (harness, received_times, raw_order)) =
        timeout(Duration::from_secs(10), async {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let config = Arc::new(websocket::Config {
                endpoint: format!("ws://{}/alt", listener.local_addr()?),
                assets_per_socket: 1,
                max_sockets: 1,
                subscription_batch: 1,
                heartbeat: Duration::from_millis(20),
                pong_timeout: Duration::from_secs(2),
                io_timeout: Duration::from_secs(1),
                initialization_timeout: Duration::from_secs(5),
                reconnect_min: Duration::from_millis(1),
                reconnect_max: Duration::from_millis(5),
                connection_interval: Duration::from_millis(1),
                ..websocket::Config::default()
            });
            let (commands, command_receiver) = mpsc::channel(4);
            let (output, ingress) = ingress::channel(32, 64 << 10)?;
            let (stop, _) = watch::channel(false);
            let origin = std::time::Instant::now();
            let worker = run_worker_with_protocol(
                session(0, 0, 0),
                vec!["A".into()],
                command_receiver,
                output,
                config,
                stop.clone(),
                AlternateProtocol,
            );

            // The server advances on observed subscription/heartbeat/close frames,
            // never on sleeps or assumptions that the worker has processed a write.
            let server = async {
                let mut heartbeats = [0_u32; 2];
                for (generation, heartbeat_count) in heartbeats.iter_mut().enumerate() {
                    let (tcp, _) = listener.accept().await?;
                    let mut socket =
                        accept_hdr_async(tcp, |request: &Request, mut response: Response| {
                            assert_eq!(request.uri().path(), "/alt");
                            assert_eq!(request.headers()["sec-websocket-protocol"], "alt.l2.v1");
                            response
                                .headers_mut()
                                .insert("sec-websocket-protocol", "alt.l2.v1".parse().unwrap());
                            Ok(response)
                        })
                        .await?;
                    let mut subscribed = false;
                    while !subscribed || *heartbeat_count == 0 {
                        match socket
                            .next()
                            .await
                            .context("ALT socket ended before subscription/heartbeat")??
                        {
                            Frame::Text(text) if text == "ALIVE?" => {
                                *heartbeat_count += 1;
                                socket.send(Frame::Text("ALIVE!".into())).await?;
                            }
                            Frame::Text(text) => {
                                ensure!(
                                    !subscribed && text == "OPEN [\"A\"]",
                                    "unexpected ALT subscription: {text}"
                                );
                                subscribed = true;
                            }
                            other => anyhow::bail!("unexpected pre-baseline frame: {other:?}"),
                        }
                    }
                    let payloads: &[&str] = if generation == 0 {
                        &[BASE, CHANGE, GAP]
                    } else {
                        &[FRESH, FOLLOWING]
                    };
                    for payload in payloads {
                        socket.send(Frame::Text((*payload).into())).await?;
                    }
                    let mut unsubscribed = false;
                    loop {
                        match socket
                            .next()
                            .await
                            .context("ALT worker vanished without close")??
                        {
                            Frame::Text(text) if text == "ALIVE?" => {
                                *heartbeat_count += 1;
                                socket.send(Frame::Text("ALIVE!".into())).await?;
                            }
                            Frame::Text(text) => {
                                ensure!(
                                    generation == 0 && !unsubscribed && text == "UNWATCH [\"A\"]",
                                    "unexpected repair subscription: {text}"
                                );
                                unsubscribed = true;
                            }
                            Frame::Close(_) => {
                                socket.flush().await?;
                                break;
                            }
                            other => anyhow::bail!("unexpected post-baseline frame: {other:?}"),
                        }
                    }
                    // Exercise the preserved bounded unsubscribe-after-Ended block.
                    ensure!(
                        unsubscribed == (generation == 0),
                        "repair/shutdown unsubscribe mismatch"
                    );
                }
                Ok::<_, anyhow::Error>(heartbeats)
            };

            let consumer = async {
                let mut harness = Harness::new();
                let mut active = None;
                let mut decoder = None;
                let mut last_ordinal = 0;
                let mut received_times = Vec::new();
                let mut raw_order = Vec::new();
                let expected = [
                    (1, 1, BASE),
                    (1, 2, CHANGE),
                    (1, 3, GAP),
                    (2, 1, FRESH),
                    (2, 2, FOLLOWING),
                ];
                loop {
                    let envelope = match ingress.try_recv() {
                        Ok(envelope) => envelope,
                        Err(crossbeam::channel::TryRecvError::Empty) => {
                            // Adapt the real synchronous bounded queue without
                            // blocking the worker/server on this single-thread runtime.
                            sleep(Duration::from_millis(1)).await;
                            continue;
                        }
                        Err(crossbeam::channel::TryRecvError::Disconnected) => break,
                    };
                    let now = origin.elapsed();
                    match &envelope.message {
                        Message::Data {
                            session,
                            ordinal,
                            received_at,
                            received_mono,
                            payload,
                        } => {
                            ensure!(active == Some(*session), "data outside current session");
                            ensure!(*ordinal == last_ordinal + 1, "unordered raw ingress");
                            last_ordinal = *ordinal;
                            let expected = expected
                                .get(received_times.len())
                                .context("unexpected raw data (including leaked heartbeat)")?;
                            ensure!(
                                (session.generation, *ordinal, payload.as_str()) == *expected,
                                "ALT raw data changed"
                            );
                            let received = received_mono
                                .checked_duration_since(origin)
                                .context("receipt before test origin")?;
                            ensure!(
                                received <= now && received_at.timestamp_micros() > 0,
                                "invalid worker receive timestamps"
                            );
                            received_times.push(received_at.timestamp_micros());
                            raw_order.push((session.generation, "data", *ordinal));
                            harness.processor.clock(now)?;
                            harness.processor.received_time = received;
                            harness.processor.decode_message(
                                decoder.as_mut().context("missing session decoder")?,
                                *session,
                                *ordinal,
                                *received_at,
                                payload.as_str(),
                                &mut |observation| {
                                    capture(&mut harness.trace, &mut harness.recorder, observation)
                                },
                            )?;
                        }
                        message => {
                            match message {
                                Message::Started(session) => {
                                    ensure!(active.is_none(), "new generation before old Ended");
                                    ensure!(
                                        session.generation == 1 || session.generation == 2,
                                        "unexpected reconnect"
                                    );
                                    active = Some(*session);
                                    decoder = Some(AlternateDecoder::new(&["A"]));
                                    last_ordinal = 0;
                                    raw_order.push((session.generation, "started", 0));
                                }
                                Message::Ended { session, reason } => {
                                    ensure!(active == Some(*session), "unordered Ended");
                                    if session.generation == 1 {
                                        ensure!(
                                            reason == "processor requested fresh baseline",
                                            "unexpected repair reason: {reason}"
                                        );
                                    }
                                    active = None;
                                    decoder = None;
                                    raw_order.push((session.generation, "ended", 0));
                                }
                                Message::SubscriptionsRequested {
                                    session,
                                    add,
                                    remove,
                                    ..
                                } => {
                                    ensure!(
                                        active == Some(*session)
                                            && add.len() == 1
                                            && add[0] == "A"
                                            && remove.is_empty(),
                                        "unexpected raw subscription control"
                                    );
                                }
                                Message::RequestsComplete { session } => ensure!(
                                    active == Some(*session),
                                    "request marker outside session"
                                ),
                                _ => anyhow::bail!("unexpected worker control"),
                            }
                            harness
                                .processor
                                .process(message, now, &mut |observation| {
                                    capture(&mut harness.trace, &mut harness.recorder, observation)
                                })?;
                        }
                    }
                    drop(envelope); // Release actual byte-budget credit before administrative awaits.
                    for session in harness.processor.take_recovery() {
                        ensure!(
                            session.generation == 1 && received_times.len() == 3,
                            "unexpected repair"
                        );
                        commands
                            .send(WorkerCommand {
                                restart: Some(session),
                                ..WorkerCommand::default()
                            })
                            .await?;
                    }
                    if received_times.len() == expected.len() {
                        stop.send_replace(true);
                    }
                }
                ensure!(
                    active.is_none() && received_times.len() == expected.len(),
                    "incomplete accepted prefix"
                );
                ensure!(
                    harness.processor.stats().rejected == 1,
                    "gap was not isolated"
                );
                Ok::<_, anyhow::Error>((harness, received_times, raw_order))
            };
            tokio::try_join!(worker, server, consumer)
        })
        .await
        .expect("ALT loopback conformance exceeded its 10s deadline")
        .unwrap();

    assert_eq!(report.sessions, 2);
    assert_eq!(report.messages, 5); // Application heartbeats never enter raw ingress.
    assert_eq!(report.connection_attempts, 2);
    assert_eq!(report.initialization_expiries, 0);
    assert!(heartbeats.iter().all(|count| *count >= 1));
    assert_eq!(
        raw_order,
        vec![
            (1, "started", 0),
            (1, "data", 1),
            (1, "data", 2),
            (1, "data", 3),
            (1, "ended", 0),
            (2, "started", 0),
            (2, "data", 1),
            (2, "data", 2),
            (2, "ended", 0),
        ]
    );
    let (records, integrity) = harness.finish();
    assert_eq!(
        records,
        vec![
            json!({"kind":"snapshot","symbol":"polymarket:v1:A","received_us":received_times[0],
            "exchange_us":42000000,"bids":[[12500000,7000000]],"asks":[]}),
            json!({"kind":"level","symbol":"polymarket:v1:A","received_us":received_times[1],
            "exchange_us":42000000,"side":"B","price":12500000,"quantity":5000000}),
            json!({"kind":"level","symbol":"polymarket:v1:A","received_us":received_times[1],
            "exchange_us":42000000,"side":"B","price":12500000,"quantity":8000000}),
            json!({"kind":"snapshot","symbol":"polymarket:v1:A","received_us":received_times[3],
            "exchange_us":42000000,"bids":[[12500000,9000000]],"asks":[]}),
            json!({"kind":"level","symbol":"polymarket:v1:A","received_us":received_times[4],
            "exchange_us":42000000,"side":"B","price":12500000,"quantity":10000000}),
        ]
    );
    let gaps: Vec<_> = integrity
        .iter()
        .filter(|row| row["cause"] == "invalid_data")
        .collect();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["kind"], "unavailable");
    assert_eq!(gaps[0]["symbol"], "polymarket:v1:A");
    assert_eq!(gaps[0]["binding"]["generation"], 1);
    assert_eq!(gaps[0]["next_ordinal"], 3);
    let gap = integrity
        .iter()
        .position(|row| row["cause"] == "invalid_data")
        .unwrap();
    let replacement = integrity
        .iter()
        .position(|row| {
            row["kind"] == "snapshot" && row["binding"]["generation"] == 2 && row["ordinal"] == 3
        })
        .unwrap();
    assert!(gap < replacement);
}
