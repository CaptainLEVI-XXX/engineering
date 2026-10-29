//! A book's source replicas: at most one per redundant slot (two slots).
use super::Replica;

/// Slot numbers as stable storage, so iteration can yield `&u16` like a map.
const SLOTS: [u16; 2] = [0, 1];

/// Fixed two-slot storage replacing a `BTreeMap<u16, Replica>`: same
/// lookup and ascending-slot iteration, without tree nodes or allocation.
/// Session start rejects any slot outside `SLOTS` before a replica exists.
#[derive(Default)]
pub(super) struct Slots([Option<Replica>; 2]);

impl Slots {
    pub(super) fn get(&self, slot: &u16) -> Option<&Replica> {
        self.0.get(usize::from(*slot))?.as_ref()
    }
    pub(super) fn get_mut(&mut self, slot: &u16) -> Option<&mut Replica> {
        self.0.get_mut(usize::from(*slot))?.as_mut()
    }
    pub(super) fn insert(&mut self, slot: u16, replica: Replica) -> Option<Replica> {
        self.0
            .get_mut(usize::from(slot))
            .expect("session start bounds replica slots")
            .replace(replica)
    }
    pub(super) fn remove(&mut self, slot: &u16) -> Option<Replica> {
        self.0.get_mut(usize::from(*slot))?.take()
    }
    pub(super) fn len(&self) -> usize {
        self.0.iter().flatten().count()
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &Replica> {
        self.0.iter().flatten()
    }
    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut Replica> {
        self.0.iter_mut().flatten()
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = (&u16, &Replica)> {
        SLOTS
            .iter()
            .zip(&self.0)
            .filter_map(|(s, r)| Some((s, r.as_ref()?)))
    }
    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&u16, &mut Replica)> {
        SLOTS
            .iter()
            .zip(&mut self.0)
            .filter_map(|(s, r)| Some((s, r.as_mut()?)))
    }
}

impl std::ops::Index<&u16> for Slots {
    type Output = Replica;
    fn index(&self, slot: &u16) -> &Replica {
        self.get(slot).expect("no replica in slot")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        discovery::AdapterId,
        orderbook::{Binding, Book, BookId, Validity, health::Health},
        websocket::ingress::Session,
    };
    use std::time::Duration;

    fn replica(slot: u16, applied: u64) -> Replica {
        Replica {
            binding: Binding {
                book: BookId(0),
                revision: 1,
                session: Session {
                    worker: usize::from(slot),
                    generation: 1,
                    adapter: AdapterId::PolymarketV1,
                    slot,
                },
            },
            state: Validity::AwaitingSnapshot,
            book: Book::default(),
            last: None,
            tick: None,
            applied,
            observed: 0,
            last_received_at: None,
            last_valid_receive: None,
            suspect: false,
            suspicion_reported: false,
            health: Health::new(Duration::ZERO),
            unavailable_since: None,
            requested_at: Duration::ZERO,
            attempts: 0,
            next_retry: Duration::ZERO,
            cooldown: Duration::ZERO,
        }
    }

    #[test]
    fn behaves_like_an_ordered_two_slot_map() {
        let mut slots = Slots::default();
        assert_eq!(slots.len(), 0);
        assert!(slots.get(&0).is_none() && slots.get(&7).is_none());
        // Insertion order does not affect ascending-slot iteration.
        assert!(slots.insert(1, replica(1, 10)).is_none());
        assert!(slots.insert(0, replica(0, 20)).is_none());
        let order: Vec<_> = slots.iter().map(|(s, r)| (*s, r.applied)).collect();
        assert_eq!(order, [(0, 20), (1, 10)]);
        assert_eq!(
            slots.values().map(|r| r.applied).collect::<Vec<_>>(),
            [20, 10]
        );
        // Replacement returns the previous occupant, as a map insert does.
        assert_eq!(slots.insert(1, replica(1, 11)).map(|r| r.applied), Some(10));
        assert_eq!(slots[&1].applied, 11);
        for (_, r) in slots.iter_mut() {
            r.applied += 1;
        }
        slots.values_mut().for_each(|r| r.applied += 1);
        assert_eq!(slots.get_mut(&0).map(|r| r.applied), Some(22));
        assert_eq!(slots.remove(&0).map(|r| r.applied), Some(22));
        assert!(slots.remove(&0).is_none() && slots.remove(&9).is_none());
        assert_eq!(slots.len(), 1);
        assert_eq!(slots.iter().map(|(s, _)| *s).collect::<Vec<_>>(), [1]);
    }
}
