//! Sliding-window hot-mint tracker.
//!
//! Records candidate mints observed in trigger streams (Axion/FOMO) and
//! answers "give me the top N most-active mints in the last `window_ms`".
//!
//! Recording happens BEFORE the allowlist filter so we discover mints that
//! aren't currently allowlisted — enabling future dynamic top-N rotation.
//!
//! Design:
//! - Ring buffer of `num_buckets` count maps, each covering `bucket_ms` of wall
//!   time. Total window = `num_buckets * bucket_ms`.
//! - Recording writes to the current bucket via DashMap (lockfree hot path).
//! - Rotation is driven externally (background task): call `rotate()` every
//!   `bucket_ms` to advance the ring and evict the oldest bucket.
//! - Query `top_n(n)` aggregates all live buckets, sorts by count, truncates.

use dashmap::DashMap;
use solana_program::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HotMintTracker {
    buckets: Vec<DashMap<Pubkey, u64>>,
    current: AtomicUsize,
}

impl HotMintTracker {
    /// Create a tracker with `num_buckets` slots. `num_buckets` must be >= 1.
    pub fn new(num_buckets: usize) -> Self {
        let num_buckets = num_buckets.max(1);
        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            buckets.push(DashMap::new());
        }
        Self {
            buckets,
            current: AtomicUsize::new(0),
        }
    }

    /// Record one occurrence of `mint` in the current bucket. Lockfree per shard.
    pub fn record(&self, mint: Pubkey) {
        let idx = self.current_index();
        let mut entry = self.buckets[idx].entry(mint).or_insert(0);
        *entry += 1;
    }

    /// Record a batch of mints. Useful when a single tx surfaces multiple
    /// candidate mints (token_balance_mints).
    pub fn record_all(&self, mints: impl IntoIterator<Item = Pubkey>) {
        for mint in mints {
            self.record(mint);
        }
    }

    /// Seed the tracker with a synthetic boost of `boost` hits per mint,
    /// applied in one shot to the current bucket. Used at boot by the
    /// wallet-seeded flow (`wallet_followers::seed_mints_from_wallets`) so
    /// the very first promoter tick observes the seed mints near the top
    /// of `top_n` even before any live trigger stream has fired. `boost=0`
    /// is a no-op. The boost decays naturally via ring rotation like any
    /// other recorded hit.
    pub fn seed_boost(&self, mints: impl IntoIterator<Item = Pubkey>, boost: u32) {
        if boost == 0 {
            return;
        }
        let idx = self.current_index();
        let bump = boost as u64;
        for mint in mints {
            let mut entry = self.buckets[idx].entry(mint).or_insert(0);
            *entry = entry.saturating_add(bump);
        }
    }

    /// Advance the ring by one slot and clear the incoming bucket. Must be
    /// called externally on a `bucket_ms` cadence.
    pub fn rotate(&self) {
        let old = self.current.fetch_add(1, Ordering::AcqRel);
        let new = (old + 1) % self.buckets.len();
        self.buckets[new].clear();
    }

    /// Aggregate all live buckets and return the top `n` mints by total count.
    pub fn top_n(&self, n: usize) -> Vec<(Pubkey, u64)> {
        if n == 0 {
            return Vec::new();
        }
        let mut totals: HashMap<Pubkey, u64> = HashMap::new();
        for bucket in &self.buckets {
            for entry in bucket.iter() {
                *totals.entry(*entry.key()).or_insert(0) += *entry.value();
            }
        }
        let mut sorted: Vec<(Pubkey, u64)> = totals.into_iter().collect();
        // Sort by count desc, then by pubkey asc for stable tie-breaking.
        sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        sorted.truncate(n);
        sorted
    }

    /// Current number of unique mints across all live buckets.
    pub fn unique_mint_count(&self) -> usize {
        let mut seen: std::collections::HashSet<Pubkey> = std::collections::HashSet::new();
        for bucket in &self.buckets {
            for entry in bucket.iter() {
                seen.insert(*entry.key());
            }
        }
        seen.len()
    }

    fn current_index(&self) -> usize {
        self.current.load(Ordering::Acquire) % self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(seed: u8) -> Pubkey {
        Pubkey::new_from_array([seed; 32])
    }

    #[test]
    fn record_and_top_n_returns_sorted_desc() {
        let tracker = HotMintTracker::new(3);
        tracker.record(pk(1));
        tracker.record(pk(2));
        tracker.record(pk(2));
        tracker.record(pk(3));
        tracker.record(pk(3));
        tracker.record(pk(3));

        let top = tracker.top_n(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, pk(3));
        assert_eq!(top[0].1, 3);
        assert_eq!(top[1].0, pk(2));
        assert_eq!(top[1].1, 2);
        assert_eq!(top[2].0, pk(1));
        assert_eq!(top[2].1, 1);
    }

    #[test]
    fn top_n_truncates() {
        let tracker = HotMintTracker::new(2);
        tracker.record(pk(1));
        tracker.record(pk(2));
        tracker.record(pk(3));

        assert_eq!(tracker.top_n(2).len(), 2);
        assert_eq!(tracker.top_n(10).len(), 3);
        assert!(tracker.top_n(0).is_empty());
    }

    #[test]
    fn rotate_evicts_after_full_window() {
        // 2 buckets; write into bucket 0, rotate, write into bucket 1, both
        // still live. Rotate again — bucket 0 gets cleared.
        let tracker = HotMintTracker::new(2);
        tracker.record(pk(1));
        tracker.record(pk(1));
        tracker.rotate();
        tracker.record(pk(2));

        let top = tracker.top_n(5);
        assert_eq!(top.len(), 2);
        // pk(1) still visible from bucket 0.
        assert!(top.iter().any(|(m, c)| *m == pk(1) && *c == 2));

        tracker.rotate();
        // Now bucket 0 was cleared. Only pk(2) remains.
        let top = tracker.top_n(5);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].0, pk(2));
    }

    #[test]
    fn record_all_accumulates_across_calls() {
        let tracker = HotMintTracker::new(3);
        tracker.record_all([pk(1), pk(2), pk(1)]);
        tracker.record_all([pk(2), pk(2)]);

        let top = tracker.top_n(2);
        assert_eq!(top[0].0, pk(2));
        assert_eq!(top[0].1, 3);
        assert_eq!(top[1].0, pk(1));
        assert_eq!(top[1].1, 2);
    }

    #[test]
    fn unique_mint_count_covers_all_buckets() {
        let tracker = HotMintTracker::new(2);
        tracker.record(pk(1));
        tracker.rotate();
        tracker.record(pk(2));
        assert_eq!(tracker.unique_mint_count(), 2);
    }

    #[test]
    fn empty_tracker_top_n_is_empty() {
        let tracker = HotMintTracker::new(3);
        assert!(tracker.top_n(10).is_empty());
        assert_eq!(tracker.unique_mint_count(), 0);
    }

    #[test]
    fn seed_boost_places_mints_at_top_of_ranking() {
        let tracker = HotMintTracker::new(3);
        // Prior live activity: pk(9) has 2 organic hits.
        tracker.record(pk(9));
        tracker.record(pk(9));

        // Seed boost pk(1)/pk(2) with 5 synthetic hits each.
        tracker.seed_boost([pk(1), pk(2)], 5);

        let top = tracker.top_n(3);
        // Seeded mints outrank pk(9) (count 5 > 2).
        assert_eq!(top[0].1, 5);
        assert_eq!(top[1].1, 5);
        assert!(top[0].0 == pk(1) || top[0].0 == pk(2));
        assert!(top[1].0 == pk(1) || top[1].0 == pk(2));
        assert_ne!(top[0].0, top[1].0);
        assert_eq!(top[2].0, pk(9));
        assert_eq!(top[2].1, 2);
    }

    #[test]
    fn seed_boost_zero_is_noop() {
        let tracker = HotMintTracker::new(2);
        tracker.seed_boost([pk(1), pk(2)], 0);
        assert!(tracker.top_n(5).is_empty());
        assert_eq!(tracker.unique_mint_count(), 0);
    }

    #[test]
    fn seed_boost_decays_via_rotation() {
        let tracker = HotMintTracker::new(2);
        tracker.seed_boost([pk(1)], 10);
        assert_eq!(tracker.top_n(1)[0].1, 10);
        // Bucket 0 → 1 rotate: seed still live (2-bucket window).
        tracker.rotate();
        assert_eq!(tracker.top_n(1)[0].1, 10);
        // Second rotate evicts bucket 0 (the one that held the boost).
        tracker.rotate();
        assert!(tracker.top_n(1).is_empty());
    }

    #[test]
    fn seed_boost_stacks_with_organic_hits() {
        let tracker = HotMintTracker::new(2);
        tracker.seed_boost([pk(1)], 3);
        tracker.record(pk(1));
        tracker.record(pk(1));
        let top = tracker.top_n(1);
        assert_eq!(top[0].0, pk(1));
        assert_eq!(top[0].1, 5);
    }
}
