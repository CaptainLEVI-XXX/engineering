//! Borrowed views over validated state; generated codecs remain the wire authority.
use crate::{
    Side,
    market_data::{Price, Quantity},
    orderbook::Book,
    util::dtos::{
        SbeEncode,
        challenge_orderbook::{PriceLevelUpdateEncoder, SnapshotEncoder},
    },
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};

fn exchange_time(us: u64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_micros(i64::try_from(us)?)
        .context("exchange time outside reader range")
}
fn length(base: usize, symbol: &str) -> Result<usize> {
    ensure!(symbol.len() <= 1 << 30, "symbol exceeds SBE string limit");
    base.checked_add(symbol.len())
        .context("encoded length overflow")
}

pub struct SnapshotRecord<'a> {
    pub book: &'a Book,
    pub symbol: &'a str,
    pub received_at: DateTime<Utc>,
    pub exchange_us: u64,
}
impl SbeEncode for SnapshotRecord<'_> {
    fn encoded_length_with_header(&self) -> Result<usize> {
        u16::try_from(self.book.side_len(Side::Buy))?;
        u16::try_from(self.book.side_len(Side::Sell))?;
        length(
            self.book
                .len()
                .checked_mul(16)
                .and_then(|n| n.checked_add(36))
                .context("snapshot length overflow")?,
            self.symbol,
        )
    }
    fn encode(&self, buffer: &mut [u8]) -> Result<usize> {
        ensure!(
            buffer.len() >= self.encoded_length_with_header()?,
            "short encoding buffer"
        );
        let mut encoder = SnapshotEncoder::try_wrap_and_apply_header(buffer, 0)?;
        encoder
            .try_received_at(self.received_at)
            .map_err(anyhow::Error::msg)?;
        encoder
            .try_exchange_timestamp(exchange_time(self.exchange_us)?)
            .map_err(anyhow::Error::msg)?;
        let encoder = encoder.bids(u16::try_from(self.book.side_len(Side::Buy))?, |group| {
            for (p, q) in self.book.levels(Side::Buy) {
                group.add(|entry| {
                    entry.price_level(p.value()).quantity(q.value());
                    Ok(())
                })?;
            }
            Ok(())
        })?;
        let encoder = encoder.asks(u16::try_from(self.book.side_len(Side::Sell))?, |group| {
            for (p, q) in self.book.levels(Side::Sell) {
                group.add(|entry| {
                    entry.price_level(p.value()).quantity(q.value());
                    Ok(())
                })?;
            }
            Ok(())
        })?;
        Ok(encoder
            .symbol(self.symbol.as_bytes())?
            .encoded_length_with_header())
    }
}

pub struct LevelRecord<'a> {
    pub symbol: &'a str,
    pub received_at: DateTime<Utc>,
    pub exchange_us: u64,
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}
impl SbeEncode for LevelRecord<'_> {
    fn encoded_length_with_header(&self) -> Result<usize> {
        length(45, self.symbol)
    }
    fn encode(&self, buffer: &mut [u8]) -> Result<usize> {
        ensure!(
            buffer.len() >= self.encoded_length_with_header()?,
            "short encoding buffer"
        );
        let mut encoder = PriceLevelUpdateEncoder::try_wrap_and_apply_header(buffer, 0)?;
        encoder
            .try_backtest_timestamp(self.received_at)
            .map_err(anyhow::Error::msg)?;
        encoder
            .try_exchange_timestamp(exchange_time(self.exchange_us)?)
            .map_err(anyhow::Error::msg)?;
        encoder
            .price_level(self.price.value())
            .quantity(self.quantity.value());
        encoder.try_side(self.side)?;
        Ok(encoder
            .symbol(self.symbol.as_bytes())?
            .encoded_length_with_header())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{market_data::Level, util::dtos::challenge_orderbook::SnapshotDomain};
    #[test]
    fn maximum_groups_empty_books_and_timestamp_boundaries() {
        let mut book = Book::default();
        let at = DateTime::from_timestamp_micros(123).unwrap();
        let encode = |book: &Book| {
            SnapshotRecord {
                book,
                symbol: "polymarket:v1:00123",
                received_at: at,
                exchange_us: 42,
            }
            .to_bytes()
        };
        assert_eq!(encode(&book).unwrap().len(), 55);
        for side in [Side::Buy, Side::Sell] {
            for price in 1..=65535 {
                book.set(
                    side,
                    Level {
                        price: Price::new(price).unwrap(),
                        quantity: Quantity::new(price + side as i64).unwrap(),
                    },
                );
            }
        }
        let bytes = encode(&book).unwrap();
        assert_eq!(bytes.len(), 2_097_175);
        let decoded = SnapshotDomain::try_from_slice_with_header(&bytes, 0).unwrap();
        assert_eq!(decoded.bids.len(), 65535);
        assert_eq!(decoded.asks.len(), 65535);
        assert_eq!(
            (decoded.bids[0].price_level, decoded.bids[0].quantity),
            (65535, 65535)
        );
        assert_eq!(
            (decoded.asks[0].price_level, decoded.asks[0].quantity),
            (1, 2)
        );
        book.set(
            Side::Buy,
            Level {
                price: Price::new(65536).unwrap(),
                quantity: Quantity::new(1).unwrap(),
            },
        );
        assert!(encode(&book).is_err());
        let mut level = LevelRecord {
            symbol: "A",
            received_at: at,
            exchange_us: u64::MAX,
            side: Side::Buy,
            price: Price::new(1).unwrap(),
            quantity: Quantity::new(0).unwrap(),
        };
        assert!(level.to_bytes().is_err());
        level.exchange_us = 42;
        level.received_at = DateTime::from_timestamp_micros(-1).unwrap();
        assert!(level.to_bytes().is_err());
    }
}
