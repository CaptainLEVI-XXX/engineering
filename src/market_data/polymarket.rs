use super::{Change, DecodeError, Event, Level, Limits, Price, QuantityChange, ResourceLimit};
use crate::Side;
use anyhow::{Context, Result, ensure};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, SeqAccess, Visitor},
};
use serde_json::value::RawValue;
use std::{borrow::Cow, cell::Cell, fmt, marker::PhantomData};

struct List<'c, T> {
    max: usize,
    /// Set when the bound, not the input grammar, rejected the array.
    exceeded: &'c Cell<bool>,
    marker: PhantomData<T>,
}
impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for List<'_, T> {
    type Value = Vec<T>;
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Vec<T>, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de, T: Deserialize<'de>> Visitor<'de> for List<'_, T> {
    type Value = Vec<T>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
        let mut values = Vec::new();
        loop {
            if values.len() == self.max {
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    self.exceeded.set(true);
                    return Err(de::Error::custom("resource limit: array entries"));
                }
                break;
            }
            match seq.next_element()? {
                Some(v) => values.push(v),
                None => break,
            }
        }
        Ok(values)
    }
}
fn list<'a, T: Deserialize<'a>>(text: &'a str, max: usize) -> Result<Vec<T>> {
    let exceeded = Cell::new(false);
    let mut d = serde_json::Deserializer::from_str(text);
    let result = List {
        max,
        exceeded: &exceeded,
        marker: PhantomData,
    }
    .deserialize(&mut d)
    .map_err(|error| {
        // Keep serde's positioned message; classify by type, not by text.
        if exceeded.get() {
            anyhow::Error::new(ResourceLimit(error.to_string()))
        } else {
            error.into()
        }
    })?;
    d.end()?;
    Ok(result)
}

/// Full syntactic validation precedes exposing any top-level event to a caller.
pub fn envelope<'a>(text: &'a str, limits: &Limits) -> Result<Vec<&'a RawValue>> {
    ResourceLimit::check(text.len() <= limits.message_bytes, "message bytes")?;
    if text.trim_start().starts_with('[') {
        list(text, limits.events)
    } else {
        ensure!(
            limits.events > 0 && text.trim_start().starts_with('{'),
            "expected object or array"
        );
        Ok(vec![serde_json::from_str(text)?])
    }
}

fn optional_text<'de, D: de::Deserializer<'de>>(d: D) -> Result<Option<Cow<'de, str>>, D::Error> {
    // Serde's borrow specialization applies to a direct Cow, not Option<Cow>.
    // The transparent wrapper also preserves owned strings for JSON escapes.
    #[derive(Deserialize)]
    #[serde(transparent)]
    struct Text<'a>(#[serde(borrow)] Cow<'a, str>);
    Option::<Text<'de>>::deserialize(d).map(|value| value.map(|text| text.0))
}

#[derive(Deserialize)]
struct Wire<'a> {
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    #[serde(borrow, default, deserialize_with = "optional_text")]
    asset_id: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "optional_text")]
    timestamp: Option<Cow<'a, str>>,
    #[serde(borrow)]
    bids: Option<&'a RawValue>,
    #[serde(borrow)]
    asks: Option<&'a RawValue>,
    #[serde(borrow)]
    price_changes: Option<&'a RawValue>,
    #[serde(borrow)]
    new_tick_size: Option<&'a RawValue>,
    #[serde(borrow)]
    id: Option<&'a RawValue>,
    #[serde(borrow)]
    version: Option<&'a RawValue>,
}
#[derive(Deserialize)]
struct Entry<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
    #[serde(borrow)]
    side: Cow<'a, str>,
    price: Price,
    size: super::Quantity,
}
impl<'de> Deserialize<'de> for Change<'de> {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let entry = Entry::deserialize(d)?;
        let side = match entry.side.as_ref() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return Err(de::Error::custom("invalid book side")),
        };
        Ok(Self {
            asset: entry.asset_id,
            side,
            price: entry.price,
            quantity: QuantityChange::Set(entry.size),
        })
    }
}
#[derive(Deserialize)]
struct Identity<'a> {
    #[serde(borrow)]
    asset_id: Cow<'a, str>,
}
fn identity(id: &str, limits: &Limits) -> Result<()> {
    ensure!(
        !id.is_empty() && id.len() <= limits.identity_bytes,
        "invalid or oversized identity"
    );
    Ok(())
}
fn time(text: Option<&str>) -> Result<u64> {
    let text = text.context("missing exchange timestamp")?;
    ensure!(
        !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()),
        "invalid timestamp"
    );
    let micros = text
        .parse::<u64>()?
        .checked_mul(1000)
        .context("timestamp overflow")?;
    ensure!(
        chrono::DateTime::from_timestamp_micros(i64::try_from(micros)?).is_some(),
        "timestamp outside supplied reader range"
    );
    Ok(micros)
}

pub fn decode<'a>(raw: &'a RawValue, limits: &Limits) -> Result<Event<'a>, DecodeError> {
    if raw.get().len() > limits.message_bytes {
        return Err(DecodeError {
            assets: None,
            resource: true,
            reason: "resource limit: event bytes".into(),
        });
    }
    let wire = serde_json::from_str(raw.get()).map_err(|error| DecodeError {
        assets: None,
        resource: false,
        reason: error.to_string(),
    })?;
    decode_wire(wire, limits)
}

/// Decode Wire and check the input's end before exposing an event. Only a
/// typed-field failure needs a syntax-only scan to distinguish a malformed
/// envelope from a valid object with invalid fields.
pub(super) fn decode_object<'a>(
    text: &'a str,
    limits: &Limits,
) -> Result<Result<Event<'a>, DecodeError>> {
    ResourceLimit::check(text.len() <= limits.message_bytes, "message bytes")?;
    ensure!(limits.events > 0, "expected object or array");
    let mut deserializer = serde_json::Deserializer::from_str(text);
    match Wire::deserialize(&mut deserializer) {
        Ok(wire) => {
            // Once the object is decoded, a trailing-input failure is known to
            // be an envelope error. No second scan is needed to classify it.
            deserializer.end()?;
            Ok(decode_wire(wire, limits))
        }
        Err(error) => {
            let raw = serde_json::from_str::<&RawValue>(text)?;
            if text.starts_with([' ', '\t', '\r', '\n']) {
                // Preserve RawValue-relative typed-field error positions. Only
                // this whitespace-prefixed failure needs another typed parse.
                return Ok(decode(raw, limits));
            }
            Ok(Err(DecodeError {
                assets: None,
                resource: false,
                reason: error.to_string(),
            }))
        }
    }
}

fn decode_wire<'a>(mut wire: Wire<'a>, limits: &Limits) -> Result<Event<'a>, DecodeError> {
    let result = (|| -> Result<Event<'a>> {
        match wire.event_type.as_ref() {
            "book" => {
                let asset = wire.asset_id.as_deref().context("missing asset_id")?;
                identity(asset, limits)?;
                let bids: Vec<Level> =
                    list(wire.bids.context("missing bids")?.get(), limits.entries)?;
                let asks: Vec<Level> = list(
                    wire.asks.context("missing asks")?.get(),
                    limits.entries.saturating_sub(bids.len()),
                )?;
                ensure!(
                    bids.iter()
                        .chain(&asks)
                        .all(|l| l.price.value() <= 1_000_000),
                    "price outside probability range"
                );
                let exchange_us = Some(time(wire.timestamp.as_deref())?);
                Ok(Event::Snapshot {
                    asset: wire.asset_id.take().unwrap(),
                    exchange_us,
                    bids,
                    asks,
                })
            }
            "price_change" => {
                let changes: Vec<Change> = list(
                    wire.price_changes.context("missing changes")?.get(),
                    limits.entries,
                )?;
                for change in &changes {
                    identity(&change.asset, limits)?;
                    ensure!(
                        change.price.value() <= 1_000_000,
                        "price outside probability range"
                    );
                }
                Ok(Event::Changes {
                    exchange_us: Some(time(wire.timestamp.as_deref())?),
                    changes,
                })
            }
            "tick_size_change" => {
                let asset = wire.asset_id.as_deref().context("missing asset_id")?;
                identity(asset, limits)?;
                let tick: Price =
                    serde_json::from_str(wire.new_tick_size.context("missing tick")?.get())?;
                ensure!(
                    tick.value() > 0 && tick.value() <= 1_000_000,
                    "invalid probability tick size"
                );
                let exchange_us = Some(time(wire.timestamp.as_deref())?);
                Ok(Event::Tick {
                    asset: wire.asset_id.take().unwrap(),
                    exchange_us,
                    tick,
                })
            }
            "new_market" | "market_resolved" => {
                // `market` is a condition ID, not the Gamma key. Invalid hints
                // cannot invalidate financial books or manufacture an identity.
                let market_id = wire.id.and_then(|raw| {
                    let text = raw.get();
                    let id = if text.starts_with('"') {
                        Cow::Owned(serde_json::from_str::<String>(text).ok()?)
                    } else if text.bytes().all(|b| b.is_ascii_digit()) {
                        Cow::Borrowed(text)
                    } else {
                        return None;
                    };
                    identity(&id, limits).ok().map(|()| id)
                });
                let adapter = match wire.version.map(RawValue::get) {
                    Some("\"v1\"") => Some(crate::discovery::AdapterId::PolymarketV1),
                    Some("\"v2\"") => Some(crate::discovery::AdapterId::PolymarketV2),
                    _ => None,
                };
                let kind = if wire.event_type == "new_market" {
                    super::LifecycleKind::NewMarket
                } else {
                    super::LifecycleKind::Resolved
                };
                Ok(Event::Lifecycle {
                    kind,
                    market_id,
                    adapter,
                })
            }
            "last_trade_price" | "best_bid_ask" => Ok(Event::NonBook),
            _ => anyhow::bail!("unknown event type"),
        }
    })();
    result.map_err(|error| DecodeError {
        assets: scope(wire, limits),
        resource: error.is::<ResourceLimit>(),
        reason: error.to_string(),
    })
}

fn scope(wire: Wire<'_>, limits: &Limits) -> Option<Vec<String>> {
    let assets = match wire.event_type.as_ref() {
        "book" | "tick_size_change" => vec![wire.asset_id?.into_owned()],
        "price_change" => list::<Identity>(wire.price_changes?.get(), limits.entries)
            .ok()?
            .into_iter()
            .map(|e| e.asset_id.into_owned())
            .collect(),
        _ => return None,
    };
    if assets.iter().any(|id| identity(id, limits).is_err()) {
        return None;
    }
    Some(assets)
}
