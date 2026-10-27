use super::{Change, DecodeError, Event, Level, Limits, Price, QuantityChange};
use crate::Side;
use anyhow::{Context, Result, ensure};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, SeqAccess, Visitor},
};
use serde_json::value::RawValue;
use std::{borrow::Cow, fmt, marker::PhantomData};

struct List<T> {
    max: usize,
    marker: PhantomData<T>,
}
impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for List<T> {
    type Value = Vec<T>;
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Vec<T>, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de, T: Deserialize<'de>> Visitor<'de> for List<T> {
    type Value = Vec<T>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
        let mut values = Vec::new();
        loop {
            if values.len() == self.max {
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
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
    let mut d = serde_json::Deserializer::from_str(text);
    let result = List {
        max,
        marker: PhantomData,
    }
    .deserialize(&mut d)?;
    d.end()?;
    Ok(result)
}

/// Full syntactic validation precedes exposing any top-level event to a caller.
pub fn envelope<'a>(text: &'a str, limits: &Limits) -> Result<Vec<&'a RawValue>> {
    ensure!(
        text.len() <= limits.message_bytes,
        "resource limit: message bytes"
    );
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

#[derive(Deserialize)]
struct Wire<'a> {
    #[serde(borrow)]
    event_type: Cow<'a, str>,
    #[serde(borrow)]
    asset_id: Option<Cow<'a, str>>,
    #[serde(borrow)]
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
    let result = (|| -> Result<Event<'a>> {
        ensure!(
            raw.get().len() <= limits.message_bytes,
            "resource limit: event bytes"
        );
        let wire: Wire = serde_json::from_str(raw.get())?;
        match wire.event_type.as_ref() {
            "book" => {
                let asset = wire.asset_id.context("missing asset_id")?;
                identity(&asset, limits)?;
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
                Ok(Event::Snapshot {
                    asset,
                    exchange_us: Some(time(wire.timestamp.as_deref())?),
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
                let asset = wire.asset_id.context("missing asset_id")?;
                identity(&asset, limits)?;
                let tick: Price =
                    serde_json::from_str(wire.new_tick_size.context("missing tick")?.get())?;
                ensure!(
                    tick.value() > 0 && tick.value() <= 1_000_000,
                    "invalid probability tick size"
                );
                Ok(Event::Tick {
                    asset,
                    exchange_us: Some(time(wire.timestamp.as_deref())?),
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
    result.map_err(|error| {
        let reason = error.to_string();
        DecodeError {
            assets: scope(raw, limits),
            resource: reason.starts_with("resource limit:"),
            reason,
        }
    })
}

fn scope(raw: &RawValue, limits: &Limits) -> Option<Vec<String>> {
    if raw.get().len() > limits.message_bytes {
        return None;
    }
    let wire: Wire = serde_json::from_str(raw.get()).ok()?;
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
