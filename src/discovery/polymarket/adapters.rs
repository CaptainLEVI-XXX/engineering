//! Separate protocol adapters; shared Gamma envelope and lifecycle semantics.
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use crate::discovery::{AdapterId, Eligibility, Instrument, MarketRecord, Page, RecordError};

/// Gamma wire version, not a registry identity or exchange-neutral type.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Version {
    V1,
    V2,
}

impl Version {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

/// The shared transport asks each adapter for its query version and decoder.
/// Kalshi will have its own transport, not an implementation of this Gamma API.
pub trait GammaAdapter: Sync {
    fn id(&self) -> AdapterId;
    fn version(&self) -> Version;
    fn decode_page(&self, bytes: &[u8]) -> Result<Page>;
}

pub struct PolymarketV1;
pub struct PolymarketV2;

impl GammaAdapter for PolymarketV1 {
    fn id(&self) -> AdapterId {
        AdapterId::PolymarketV1
    }
    fn version(&self) -> Version {
        Version::V1
    }
    fn decode_page(&self, bytes: &[u8]) -> Result<Page> {
        decode_page(bytes, self.version(), |market| {
            serde_json::from_str(
                market
                    .clob_token_ids
                    .as_deref()
                    .context("missing token IDs")?,
            )
            .map_err(Into::into)
        })
    }
}

impl GammaAdapter for PolymarketV2 {
    fn id(&self) -> AdapterId {
        AdapterId::PolymarketV2
    }
    fn version(&self) -> Version {
        Version::V2
    }
    fn decode_page(&self, bytes: &[u8]) -> Result<Page> {
        decode_page(bytes, self.version(), |market| {
            market.position_ids.take().context("missing position IDs")
        })
    }
}

pub fn adapter(id: AdapterId) -> &'static dyn GammaAdapter {
    match id {
        AdapterId::PolymarketV1 => &PolymarketV1,
        AdapterId::PolymarketV2 => &PolymarketV2,
    }
}

#[derive(Deserialize)]
struct RawPage {
    markets: Vec<Box<RawValue>>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMarket {
    id: String,
    version: Option<Version>,
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
    outcomes: Option<String>,
    clob_token_ids: Option<String>,
    position_ids: Option<Vec<String>>,
}

fn decode_page(
    bytes: &[u8],
    version: Version,
    ids: fn(&mut RawMarket) -> Result<Vec<String>>,
) -> Result<Page> {
    let page: RawPage = serde_json::from_slice(bytes).context("invalid market page")?;
    ensure!(
        page.next_cursor.as_ref().is_none_or(|c| !c.is_empty()),
        "empty cursor"
    );
    let markets = page
        .markets
        .into_iter()
        .map(|raw| {
            normalize(&raw, version, ids).map_err(|error| {
                // Error path only: recover identities even when another required
                // field is malformed. No registry lookups or state mutations here.
                let value = serde_json::from_str::<Value>(raw.get()).unwrap_or(Value::Null);
                let market_id = value
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned);
                let instrument_ids = match version {
                    Version::V1 => value
                        .get("clobTokenIds")
                        .and_then(Value::as_str)
                        .and_then(|v| serde_json::from_str::<Vec<String>>(v).ok()),
                    Version::V2 => value
                        .get("positionIds")
                        .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok()),
                }
                .unwrap_or_default();
                RecordError {
                    market_id,
                    instrument_ids,
                    reason: format!("{error:#}"),
                }
            })
        })
        .collect();
    Ok(Page {
        markets,
        next_cursor: page.next_cursor,
    })
}

fn normalize(
    raw: &RawValue,
    version: Version,
    ids: fn(&mut RawMarket) -> Result<Vec<String>>,
) -> Result<MarketRecord> {
    let mut m: RawMarket = serde_json::from_str(raw.get()).context("malformed required field")?;
    ensure!(!m.id.is_empty(), "missing market ID");
    ensure!(
        m.version == Some(version),
        "missing or mismatched protocol version"
    );
    // Order entry is not a prerequisite for listening to an enabled, open book.
    let eligibility = if m.closed == Some(true) || m.archived == Some(true) {
        ensure!(
            m.accepting_orders != Some(true),
            "closed/archived market accepts orders"
        );
        Eligibility::Closed
    } else {
        ensure!(
            m.closed == Some(false) && m.archived == Some(false),
            "unknown lifecycle status"
        );
        match (m.active, m.enable_order_book) {
            (Some(true), Some(true)) => Eligibility::Eligible,
            (Some(false), _) | (_, Some(false)) => Eligibility::Unavailable,
            _ => anyhow::bail!("unknown deployment/book status"),
        }
    };
    if eligibility == Eligibility::Unavailable {
        return Ok(MarketRecord {
            id: m.id,
            instruments: Vec::new(),
            eligibility,
        });
    }
    let ids = ids(&mut m)?;
    let labels: Vec<String> =
        serde_json::from_str(m.outcomes.as_deref().context("missing outcomes")?)?;
    ensure!(
        ids.len() == 2 && labels.len() == 2,
        "unsupported or mismatched outcome mapping"
    );
    ensure!(
        ids[0] != ids[1]
            && ids
                .iter()
                .all(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())),
        "invalid or duplicate outcome IDs"
    );
    let instruments = ids
        .into_iter()
        .zip(labels)
        .map(|(id, label)| Instrument { id, label })
        .collect();
    Ok(MarketRecord {
        id: m.id,
        instruments,
        eligibility,
    })
}
