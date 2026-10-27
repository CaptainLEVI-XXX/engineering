use crate::{
    Side,
    market_data::{Level, Price, Quantity},
};
use anyhow::{Result, ensure};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub struct Book {
    sides: [BTreeMap<Price, Quantity>; 2],
}
impl Book {
    pub fn levels(&self, side: Side) -> impl Iterator<Item = (Price, Quantity)> + '_ {
        let mut iter = self.sides[side as usize].iter();
        std::iter::from_fn(move || {
            let item = match side {
                Side::Buy => iter.next_back(),
                Side::Sell => iter.next(),
            };
            item.map(|(p, q)| (*p, *q))
        })
    }
    pub fn best(&self, side: Side) -> Option<(Price, Quantity)> {
        self.levels(side).next()
    }
    pub fn quantity(&self, side: Side, price: Price) -> Option<Quantity> {
        self.sides[side as usize].get(&price).copied()
    }
    pub fn len(&self) -> usize {
        self.sides.iter().map(BTreeMap::len).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn side_len(&self, side: Side) -> usize {
        self.sides[side as usize].len()
    }
    pub(crate) fn set(&mut self, side: Side, level: Level) {
        if level.quantity.value() == 0 {
            self.sides[side as usize].remove(&level.price);
        } else {
            self.sides[side as usize].insert(level.price, level.quantity);
        }
    }
    pub(crate) fn replacement(bids: Vec<Level>, asks: Vec<Level>) -> Result<Self> {
        let mut book = Self::default();
        for (side, levels) in [(Side::Buy, bids), (Side::Sell, asks)] {
            for level in levels {
                ensure!(
                    book.sides[side as usize]
                        .insert(level.price, level.quantity)
                        .is_none(),
                    "duplicate snapshot price"
                );
            }
            book.sides[side as usize].retain(|_, q| q.value() != 0);
        }
        Ok(book)
    }
}
