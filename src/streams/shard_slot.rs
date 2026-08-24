//! Assigns mints to fixed-capacity gRPC subscription workers ("shard slots").
//!
//! Yellowstone caps the number of account filters per stream, so the promoter
//! divides work into N workers, each carrying at most `per_slot_mints`
//! promoted mints (3 by default, since each mint installs 3 memcmp filters).
//!
//! Design goals:
//! - **Sticky assignment.** Once a mint lives in slot `s`, subsequent calls
//!   return the same slot. This keeps `dirty_slots(old, new)` = 1 per admit,
//!   so the promoter re-sends at most one `SubscribeRequest` per lifecycle
//!   transition.
//! - **Deterministic seed.** `preassign` accepts seed mints at bootstrap and
//!   spreads them across slots in pubkey order so restarts don't shuffle
//!   assignments (critical for staying inside per-slot filter budgets).
//! - **Least-loaded new admits.** Dynamic `assign` calls place new mints in
//!   the slot with the smallest current occupancy, breaking ties by slot
//!   index for determinism.
//!
//! The allocator is intentionally I/O-free; the orchestrator translates
//! assignments into `SubscriptionCommand::Replace` messages sent to the
//! matching worker (see `crate::promoter` in later phases).

use crate::promoter::ShardSlot;
use solana_program::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

/// Error returned when a mint cannot be placed.
#[derive(Debug, thiserror::Error)]
pub enum ShardSlotError {
    #[error("all shard slots are full ({0} slots, {1} mints/slot)")]
    Full(usize, usize),
    #[error("num_slots and per_slot_mints must be > 0")]
    InvalidDimensions,
}

pub struct ShardSlotAllocator {
    per_slot_mints: usize,
    num_slots: usize,
    assignments: HashMap<Pubkey, ShardSlot>,
    occupancy: Vec<HashSet<Pubkey>>,
}

impl ShardSlotAllocator {
    /// Create an allocator with `num_slots` workers, each capable of holding
    /// `per_slot_mints` mints.
    pub fn new(num_slots: usize, per_slot_mints: usize) -> anyhow::Result<Self> {
        if num_slots == 0 || per_slot_mints == 0 {
            anyhow::bail!(ShardSlotError::InvalidDimensions);
        }
        Ok(Self {
            per_slot_mints,
            num_slots,
            assignments: HashMap::new(),
            occupancy: (0..num_slots).map(|_| HashSet::new()).collect(),
        })
    }

    pub fn num_slots(&self) -> usize {
        self.num_slots
    }

    pub fn per_slot_capacity(&self) -> usize {
        self.per_slot_mints
    }

    /// Deterministically assign a seed set. Mints are sorted by pubkey so
    /// repeated boots produce identical assignments. Fails if capacity is
    /// insufficient.
    pub fn preassign_seed(&mut self, seed: &[Pubkey]) -> Result<(), ShardSlotError> {
        let capacity = self.num_slots * self.per_slot_mints;
        if seed.len() > capacity {
            return Err(ShardSlotError::Full(self.num_slots, self.per_slot_mints));
        }

        let mut ordered = seed.to_vec();
        ordered.sort();
        for (idx, mint) in ordered.into_iter().enumerate() {
            let slot = ShardSlot::new((idx % self.num_slots) as u16);
            self.place(mint, slot);
        }
        Ok(())
    }

    /// Assign or reuse a slot for `mint`. Returns the existing slot if already
    /// assigned; otherwise picks the least-loaded slot with free capacity.
    /// Returns `None` iff every slot is full.
    pub fn assign(&mut self, mint: Pubkey) -> Option<ShardSlot> {
        if let Some(slot) = self.assignments.get(&mint) {
            return Some(*slot);
        }
        let slot = self.least_loaded_slot_with_capacity()?;
        self.place(mint, slot);
        Some(slot)
    }

    /// Remove `mint` from its slot. Returns the slot it occupied, if any.
    pub fn release(&mut self, mint: Pubkey) -> Option<ShardSlot> {
        let slot = self.assignments.remove(&mint)?;
        if let Some(occupants) = self.occupancy.get_mut(slot.index() as usize) {
            occupants.remove(&mint);
        }
        Some(slot)
    }

    pub fn slot_of(&self, mint: &Pubkey) -> Option<ShardSlot> {
        self.assignments.get(mint).copied()
    }

    pub fn mints_for_slot(&self, slot: ShardSlot) -> Option<&HashSet<Pubkey>> {
        self.occupancy.get(slot.index() as usize)
    }

    /// Set of slots that differ between `old` and `new` allowlists. Each
    /// difference — added or removed mint — marks the mint's currently-known
    /// slot as dirty. Callers use this to decide which workers need a
    /// re-subscribe.
    pub fn dirty_slots(
        &self,
        old: &HashSet<Pubkey>,
        new: &HashSet<Pubkey>,
    ) -> HashSet<ShardSlot> {
        let mut dirty = HashSet::new();
        for mint in old.symmetric_difference(new) {
            if let Some(slot) = self.assignments.get(mint) {
                dirty.insert(*slot);
            }
        }
        dirty
    }

    /// Iterate all (mint, slot) assignments. Order is unspecified.
    pub fn iter(&self) -> impl Iterator<Item = (&Pubkey, ShardSlot)> {
        self.assignments.iter().map(|(m, s)| (m, *s))
    }

    fn place(&mut self, mint: Pubkey, slot: ShardSlot) {
        self.assignments.insert(mint, slot);
        self.occupancy[slot.index() as usize].insert(mint);
    }

    fn least_loaded_slot_with_capacity(&self) -> Option<ShardSlot> {
        (0..self.num_slots)
            .filter(|idx| self.occupancy[*idx].len() < self.per_slot_mints)
            .min_by_key(|idx| (self.occupancy[*idx].len(), *idx))
            .map(|idx| ShardSlot::new(idx as u16))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    #[test]
    fn invalid_dimensions_are_rejected() {
        assert!(ShardSlotAllocator::new(0, 3).is_err());
        assert!(ShardSlotAllocator::new(3, 0).is_err());
    }

    #[test]
    fn preassign_is_deterministic_over_pubkey_order() {
        let seed = vec![pk(3), pk(1), pk(2)];
        let mut a = ShardSlotAllocator::new(3, 3).unwrap();
        a.preassign_seed(&seed).unwrap();

        // Sort order is by pubkey: pk(1), pk(2), pk(3) → slots 0, 1, 2.
        assert_eq!(a.slot_of(&pk(1)), Some(ShardSlot::new(0)));
        assert_eq!(a.slot_of(&pk(2)), Some(ShardSlot::new(1)));
        assert_eq!(a.slot_of(&pk(3)), Some(ShardSlot::new(2)));
    }

    #[test]
    fn preassign_fails_if_seed_exceeds_capacity() {
        let seed: Vec<Pubkey> = (0..7).map(pk).collect(); // 7 > 2 * 3
        let mut a = ShardSlotAllocator::new(2, 3).unwrap();
        assert!(a.preassign_seed(&seed).is_err());
    }

    #[test]
    fn assign_is_sticky() {
        let mut a = ShardSlotAllocator::new(3, 3).unwrap();
        let s1 = a.assign(pk(10)).unwrap();
        let s2 = a.assign(pk(10)).unwrap();
        assert_eq!(s1, s2);
    }

    #[test]
    fn assign_picks_least_loaded_with_tie_break_on_slot_index() {
        let mut a = ShardSlotAllocator::new(3, 3).unwrap();
        // Pre-load slot 0 and slot 1 with one mint each; slot 2 empty.
        a.assign(pk(20));
        a.assign(pk(21));
        a.assign(pk(22)); // goes to slot 2

        // Now slot 2 has 1, others have 1 → tie; next assign goes to slot 0
        // (lowest index).
        let s = a.assign(pk(23)).unwrap();
        assert_eq!(s, ShardSlot::new(0));
    }

    #[test]
    fn assign_returns_none_when_full() {
        let mut a = ShardSlotAllocator::new(2, 2).unwrap();
        for byte in 0..4u8 {
            assert!(a.assign(pk(byte)).is_some());
        }
        assert!(a.assign(pk(99)).is_none());
    }

    #[test]
    fn release_frees_slot_and_returns_index() {
        let mut a = ShardSlotAllocator::new(2, 1).unwrap();
        let s = a.assign(pk(10)).unwrap();
        // Full: capacity is 2*1 = 2, only 1 mint used.
        a.assign(pk(11));
        assert!(a.assign(pk(12)).is_none());

        let released = a.release(pk(10));
        assert_eq!(released, Some(s));
        assert!(a.slot_of(&pk(10)).is_none());
        // Now there's capacity for one more.
        assert!(a.assign(pk(12)).is_some());
    }

    #[test]
    fn release_of_unknown_mint_is_none() {
        let mut a = ShardSlotAllocator::new(2, 2).unwrap();
        assert!(a.release(pk(99)).is_none());
    }

    #[test]
    fn dirty_slots_covers_added_and_removed() {
        let mut a = ShardSlotAllocator::new(3, 3).unwrap();
        a.assign(pk(1));
        a.assign(pk(2));

        let old: HashSet<_> = [pk(1), pk(2)].into_iter().collect();
        let new: HashSet<_> = [pk(1), pk(3)].into_iter().collect();
        // Add pk(3) → not yet assigned, so its slot is unknown; won't appear.
        // Remove pk(2) → slot of pk(2) marked dirty.
        let dirty = a.dirty_slots(&old, &new);
        assert!(dirty.contains(&a.slot_of(&pk(2)).unwrap()));

        // After we admit pk(3), a re-computation with the same old/new marks
        // its slot dirty as well.
        a.assign(pk(3));
        let dirty = a.dirty_slots(&old, &new);
        assert!(dirty.contains(&a.slot_of(&pk(3)).unwrap()));
        assert!(dirty.contains(&a.slot_of(&pk(2)).unwrap()));
    }

    #[test]
    fn admit_produces_exactly_one_dirty_slot() {
        // Property from M3b plan: single-mint admit dirty a single slot.
        let mut a = ShardSlotAllocator::new(4, 3).unwrap();
        a.assign(pk(1));
        a.assign(pk(2));
        a.assign(pk(3));

        let old: HashSet<_> = [pk(1), pk(2), pk(3)].into_iter().collect();
        let mut new = old.clone();
        new.insert(pk(4));
        a.assign(pk(4));

        let dirty = a.dirty_slots(&old, &new);
        assert_eq!(dirty.len(), 1);
    }

    #[test]
    fn mints_for_slot_matches_iter() {
        let mut a = ShardSlotAllocator::new(3, 3).unwrap();
        a.assign(pk(10));
        a.assign(pk(11));
        a.assign(pk(12));

        let mut all = HashSet::new();
        for idx in 0..3 {
            for mint in a.mints_for_slot(ShardSlot::new(idx)).unwrap() {
                all.insert(*mint);
            }
        }
        let via_iter: HashSet<Pubkey> = a.iter().map(|(m, _)| *m).collect();
        assert_eq!(all, via_iter);
    }
}
