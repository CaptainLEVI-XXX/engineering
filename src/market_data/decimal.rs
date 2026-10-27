use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Deserializer, de};
use std::fmt;

/// Exact unsigned decimal grammar; values are stored in the signed SBE range.
pub fn parse_scaled(text: &str) -> Result<i64> {
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => {
            ensure!(!fraction.is_empty(), "empty fractional part");
            (whole, fraction)
        }
        None => (text, ""),
    };
    ensure!(
        !whole.is_empty()
            && whole.bytes().all(|b| b.is_ascii_digit())
            && fraction.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal grammar"
    );
    let mut value = 0i64;
    for digit in whole.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|n| n.checked_add(i64::from(digit - b'0')))
            .context("decimal overflow")?;
    }
    value = value.checked_mul(1_000_000).context("decimal overflow")?;
    let mut scale = 100_000;
    for digit in fraction.bytes() {
        let digit = i64::from(digit - b'0');
        if scale == 0 {
            ensure!(digit == 0, "unrepresentable decimal precision");
        } else {
            value = value
                .checked_add(digit * scale)
                .context("decimal overflow")?;
            scale /= 10;
        }
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Price(i64);
impl Price {
    pub fn new(value: i64) -> Result<Self> {
        ensure!(value >= 0, "negative price");
        Ok(Self(value))
    }
    pub fn value(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quantity(i64);
impl Quantity {
    pub fn new(value: i64) -> Result<Self> {
        ensure!(value >= 0, "negative quantity");
        Ok(Self(value))
    }
    pub fn value(self) -> i64 {
        self.0
    }
}

struct Decimal;
impl de::Visitor<'_> for Decimal {
    type Value = i64;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an exact nonnegative decimal string")
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<i64, E> {
        parse_scaled(value).map_err(E::custom)
    }
}
impl<'de> Deserialize<'de> for Price {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_str(Decimal).map(Self)
    }
}
impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_str(Decimal).map(Self)
    }
}
