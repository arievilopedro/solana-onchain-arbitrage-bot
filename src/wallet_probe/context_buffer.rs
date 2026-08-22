//! Rolling context buffer used to infer trigger causality.
//!
//! Every context tx (from `context_stream`) is inserted with a timestamp.
//! Entries older than `lookback_ms` are pruned lazily on insert / lookup.
//! When a wallet tx arrives, `score_candidates` walks the buffer and returns
//! the top-N entries that overlap the wallet tx by mint / pool / program.

use crate::wallet_probe::types::TriggerCandidate;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct ContextEntry {
    pub ts_ms: u128,
    pub signature: String,
    pub slot: u64,
    pub programs: Vec<String>,
    pub pools: Vec<String>,
    pub mints: Vec<String>,
    pub sol_volume_lamports: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ContextBuffer {
    entries: VecDeque<ContextEntry>,
    lookback_ms: u128,
    max_entries: usize,
}

impl ContextBuffer {
    pub fn new(lookback_ms: u128, max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            lookback_ms,
            max_entries: max_entries.max(1),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn push(&mut self, entry: ContextEntry) {
        self.prune_older_than(entry.ts_ms.saturating_sub(self.lookback_ms));
        self.entries.push_back(entry);
        while self.entries.len() > self.max_entries {
            self.entries.pop_front();
        }
    }

    fn prune_older_than(&mut self, cutoff_ms: u128) {
        while self
            .entries
            .front()
            .map(|e| e.ts_ms < cutoff_ms)
            .unwrap_or(false)
        {
            self.entries.pop_front();
        }
    }

    /// Return up to `max_candidates` best-scoring triggers for a wallet tx
    /// observed at `wallet_ts_ms` with the given programs / mints / pools.
    ///
    /// Score model (higher = more likely trigger):
    ///   * base: `1000 - min(time_delta_ms, lookback_ms)`
    ///   * +200 per matched mint
    ///   * +150 per matched pool
    ///   * +50 per matched DEX/trigger program
    ///   * -400 if candidate ts is AFTER the wallet tx (cause can't follow effect)
    pub fn score_candidates(
        &self,
        wallet_ts_ms: u128,
        wallet_programs: &[String],
        wallet_mints: &[String],
        wallet_pools: &[String],
        max_candidates: usize,
    ) -> Vec<TriggerCandidate> {
        let mut scored: Vec<TriggerCandidate> = self
            .entries
            .iter()
            .map(|e| {
                let time_delta_ms = wallet_ts_ms as i128 - e.ts_ms as i128;

                let matched_mints: Vec<String> = e
                    .mints
                    .iter()
                    .filter(|m| wallet_mints.iter().any(|wm| wm == *m))
                    .cloned()
                    .collect();
                let matched_pools: Vec<String> = e
                    .pools
                    .iter()
                    .filter(|p| wallet_pools.iter().any(|wp| wp == *p))
                    .cloned()
                    .collect();
                let matched_programs: Vec<String> = e
                    .programs
                    .iter()
                    .filter(|p| wallet_programs.iter().any(|wp| wp == *p))
                    .cloned()
                    .collect();

                let time_penalty = time_delta_ms.unsigned_abs().min(self.lookback_ms) as i64;
                let mut score = 1000i64 - time_penalty;
                score += 200 * matched_mints.len() as i64;
                score += 150 * matched_pools.len() as i64;
                score += 50 * matched_programs.len() as i64;
                if time_delta_ms < 0 {
                    // Candidate is AFTER the wallet tx — very unlikely to be a trigger.
                    score -= 400;
                }

                TriggerCandidate {
                    signature: e.signature.clone(),
                    slot: e.slot,
                    time_delta_ms,
                    matched_programs,
                    matched_mints,
                    matched_pools,
                    score,
                }
            })
            .filter(|c| {
                !c.matched_mints.is_empty()
                    || !c.matched_pools.is_empty()
                    || !c.matched_programs.is_empty()
            })
            .collect();

        scored.sort_by(|a, b| b.score.cmp(&a.score));
        scored.truncate(max_candidates);
        scored
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ts: u128, sig: &str, mint: &str) -> ContextEntry {
        ContextEntry {
            ts_ms: ts,
            signature: sig.to_string(),
            slot: 100,
            programs: vec!["prog1".to_string()],
            pools: Vec::new(),
            mints: vec![mint.to_string()],
            sol_volume_lamports: None,
        }
    }

    #[test]
    fn buffer_prunes_old_entries() {
        let mut buf = ContextBuffer::new(500, 100);
        buf.push(entry(1000, "old", "mint1"));
        buf.push(entry(1600, "new", "mint2"));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.entries.front().unwrap().signature, "new");
    }

    #[test]
    fn buffer_respects_max_entries() {
        let mut buf = ContextBuffer::new(10_000, 2);
        buf.push(entry(1000, "a", "m"));
        buf.push(entry(1001, "b", "m"));
        buf.push(entry(1002, "c", "m"));
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.entries.front().unwrap().signature, "b");
    }

    #[test]
    fn candidates_scored_by_recency_and_matches() {
        let mut buf = ContextBuffer::new(1000, 100);
        buf.push(entry(1000, "far_match", "hotmint"));
        buf.push(entry(1450, "near_no_match", "othermint"));
        buf.push(entry(1480, "near_match", "hotmint"));

        let cands = buf.score_candidates(
            1500,
            &["prog1".to_string()],
            &["hotmint".to_string()],
            &[],
            5,
        );
        assert_eq!(cands.len(), 2);
        assert_eq!(cands[0].signature, "near_match");
        assert!(cands[0].score > cands[1].score);
    }

    #[test]
    fn candidates_penalise_future_context() {
        let mut buf = ContextBuffer::new(1000, 100);
        buf.push(entry(1200, "past", "mint"));
        buf.push(entry(1600, "future", "mint"));
        let cands = buf.score_candidates(
            1500,
            &["prog1".to_string()],
            &["mint".to_string()],
            &[],
            2,
        );
        assert_eq!(cands[0].signature, "past");
    }
}
