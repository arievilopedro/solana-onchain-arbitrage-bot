//! Pre-compiled transaction cache for low-latency MEV execution.
//!
//! This module maintains pre-signed transactions for each monitored mint,
//! ready to be sent immediately when a trigger arrives. Transactions are
//! compiled in the background when pool state changes, respecting the
//! `max_dlmm_per_tx` limit by creating multiple transaction groups.
//!
//! Supports two modes:
//! - **Blockhash mode**: Transactions use recent blockhash, expire after TTL
//! - **Nonce mode**: Transactions use durable nonce, never expire

use crate::execution::{
    build_controlled_transaction, build_controlled_transaction_with_nonce, ControlledExecutionParams,
};
use crate::nonce::NonceManager;
use crate::registry::{DlmmRouteState, MintRuntimeState, PumpRouteState};
use crate::routes::{FixedDlmmRoutePacker, RouteGroup};
use dashmap::DashMap;
use solana_program::pubkey::Pubkey;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

/// Default TTL for pre-compiled transactions (25 seconds).
/// Blockhash is valid for ~60 seconds, but we use a shorter TTL
/// to ensure transactions don't expire during network delays.
const DEFAULT_TTL_MS: u64 = 25_000;

/// Statistics for cache monitoring.
#[derive(Debug, Clone, Default)]
pub struct PreCompiledCacheStats {
    pub total_mints: usize,
    pub total_transactions: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_expired: u64,
    pub refresh_count: u64,
}

/// A single pre-compiled transaction entry.
#[derive(Debug, Clone)]
pub struct PreCompiledEntry {
    /// The compiled and signed transaction, ready to send.
    pub transaction: VersionedTransaction,
    /// Index of the route group (0, 1, 2...) for this mint.
    pub route_group_index: usize,
    /// Number of DLMM pools in this transaction.
    pub dlmm_count: usize,
    /// When this entry was compiled.
    pub compiled_at: Instant,
    /// The blockhash or nonce hash used for this transaction.
    pub blockhash: Hash,
    /// When this entry expires (blockhash becomes invalid).
    /// For nonce mode, this is set far in the future.
    pub expires_at: Instant,
    /// If using durable nonce, the nonce account pubkey.
    pub nonce_pubkey: Option<Pubkey>,
}

impl PreCompiledEntry {
    /// Check if this entry is still valid (not expired).
    /// Nonce-based entries never expire by time.
    pub fn is_valid(&self) -> bool {
        self.nonce_pubkey.is_some() || Instant::now() < self.expires_at
    }

    /// Check if this entry uses a durable nonce.
    pub fn uses_nonce(&self) -> bool {
        self.nonce_pubkey.is_some()
    }

    /// Time until expiration in milliseconds.
    /// Returns u64::MAX for nonce-based entries.
    pub fn ttl_ms(&self) -> u64 {
        if self.nonce_pubkey.is_some() {
            u64::MAX
        } else {
            self.expires_at
                .saturating_duration_since(Instant::now())
                .as_millis() as u64
        }
    }
}

/// Cache of pre-compiled transactions for fast trigger response.
///
/// Each mint can have multiple pre-compiled transactions, one per route group.
/// Route groups are created by splitting DLMM pools according to `max_dlmm_per_tx`.
///
/// Supports two modes:
/// - Blockhash mode: TTL-based expiration
/// - Nonce mode: Never expires, uses durable nonce
pub struct PreCompiledTxCache {
    /// Pre-compiled transactions indexed by mint.
    cache: DashMap<Pubkey, Vec<PreCompiledEntry>>,
    /// Route packer for splitting DLMM pools into groups.
    packer: FixedDlmmRoutePacker,
    /// TTL for cached transactions in milliseconds (blockhash mode only).
    ttl_ms: u64,
    /// Whether nonce mode is enabled.
    nonce_enabled: AtomicBool,
    /// Nonce manager for durable nonce mode.
    nonce_manager: Option<Arc<NonceManager>>,
    /// Statistics counters.
    stats_hits: AtomicU64,
    stats_misses: AtomicU64,
    stats_expired: AtomicU64,
    stats_refreshes: AtomicU64,
}

impl PreCompiledTxCache {
    /// Create a new cache with the given route packer and default TTL.
    pub fn new(packer: FixedDlmmRoutePacker) -> Self {
        Self::with_ttl(packer, DEFAULT_TTL_MS)
    }

    /// Create a new cache with a custom TTL (blockhash mode).
    pub fn with_ttl(packer: FixedDlmmRoutePacker, ttl_ms: u64) -> Self {
        Self {
            cache: DashMap::new(),
            packer,
            ttl_ms,
            nonce_enabled: AtomicBool::new(false),
            nonce_manager: None,
            stats_hits: AtomicU64::new(0),
            stats_misses: AtomicU64::new(0),
            stats_expired: AtomicU64::new(0),
            stats_refreshes: AtomicU64::new(0),
        }
    }

    /// Create a new cache with durable nonce support.
    pub fn with_nonce(packer: FixedDlmmRoutePacker, nonce_manager: Arc<NonceManager>) -> Self {
        Self {
            cache: DashMap::new(),
            packer,
            ttl_ms: DEFAULT_TTL_MS, // Not used in nonce mode
            nonce_enabled: AtomicBool::new(true),
            nonce_manager: Some(nonce_manager),
            stats_hits: AtomicU64::new(0),
            stats_misses: AtomicU64::new(0),
            stats_expired: AtomicU64::new(0),
            stats_refreshes: AtomicU64::new(0),
        }
    }

    /// Check if nonce mode is enabled.
    pub fn is_nonce_enabled(&self) -> bool {
        self.nonce_enabled.load(Ordering::Relaxed)
    }

    /// Get the nonce manager (if in nonce mode).
    pub fn nonce_manager(&self) -> Option<&Arc<NonceManager>> {
        self.nonce_manager.as_ref()
    }

    /// Refresh pre-compiled transactions for a mint.
    ///
    /// This compiles transactions for all eligible route groups and stores them
    /// in the cache. Should be called when pool state changes or blockhash updates.
    ///
    /// In nonce mode, the blockhash parameter is ignored and nonces are used instead.
    pub fn refresh_mint(
        &self,
        mint: Pubkey,
        state: &MintRuntimeState,
        wallet: &Keypair,
        blockhash: Hash,
        token_program: Pubkey,
        lookup_tables: &[AddressLookupTableAccount],
        params: &ControlledExecutionParams,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
    ) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        // Pack routes into groups respecting max_dlmm_per_tx
        let route_groups = self.packer.pack_mint_state(
            state,
            min_base_liquidity_lamports,
            max_state_age_ms,
            now_ms,
        );

        if route_groups.is_empty() {
            // No eligible routes, clear cache for this mint
            self.cache.remove(&mint);
            debug!(
                "precompiled cache cleared: mint={} reason=no_eligible_routes",
                mint
            );
            return;
        }

        let now = Instant::now();
        let use_nonce = self.is_nonce_enabled();

        // For nonce mode, set expiry far in the future (effectively never expires)
        let expires_at = if use_nonce {
            now + std::time::Duration::from_secs(365 * 24 * 60 * 60) // 1 year
        } else {
            now + std::time::Duration::from_millis(self.ttl_ms)
        };

        let mut entries = Vec::with_capacity(route_groups.len());

        for (index, route) in route_groups.iter().enumerate() {
            let (tx_result, used_hash, nonce_pk) = if use_nonce {
                // Use durable nonce
                if let Some(nonce_manager) = &self.nonce_manager {
                    if let Some((nonce_pubkey, nonce_hash, _authority)) = nonce_manager.next_nonce() {
                        let result = build_controlled_transaction_with_nonce(
                            wallet,
                            route,
                            token_program,
                            nonce_hash,
                            nonce_pubkey,
                            lookup_tables,
                            params.clone(),
                        );
                        (result, nonce_hash, Some(nonce_pubkey))
                    } else {
                        warn!(
                            "precompiled tx skipped: mint={} group={} reason=no_nonce_available",
                            mint, index
                        );
                        continue;
                    }
                } else {
                    warn!(
                        "precompiled tx skipped: mint={} group={} reason=nonce_manager_missing",
                        mint, index
                    );
                    continue;
                }
            } else {
                // Use regular blockhash
                let result = build_controlled_transaction(
                    wallet,
                    route,
                    token_program,
                    blockhash,
                    lookup_tables,
                    params.clone(),
                );
                (result, blockhash, None)
            };

            match tx_result {
                Ok(tx) => {
                    entries.push(PreCompiledEntry {
                        transaction: tx,
                        route_group_index: index,
                        dlmm_count: route.dlmms.len(),
                        compiled_at: now,
                        blockhash: used_hash,
                        expires_at,
                        nonce_pubkey: nonce_pk,
                    });
                }
                Err(e) => {
                    warn!(
                        "precompiled tx compile failed: mint={} group={} nonce={} error={}",
                        mint, index, use_nonce, e
                    );
                }
            }
        }

        let tx_count = entries.len();
        self.cache.insert(mint, entries);
        self.stats_refreshes.fetch_add(1, Ordering::Relaxed);

        if use_nonce {
            debug!(
                "precompiled cache refreshed (nonce): mint={} transactions={} nonce_mode=true",
                mint, tx_count
            );
        } else {
            debug!(
                "precompiled cache refreshed: mint={} transactions={} ttl_ms={} blockhash={}",
                mint, tx_count, self.ttl_ms, blockhash
            );
        }
    }

    /// Get all valid pre-compiled transactions for a mint.
    ///
    /// Returns empty vec if no valid transactions are cached.
    /// Filters out expired transactions automatically.
    pub fn get_ready_txs(&self, mint: &Pubkey) -> Vec<VersionedTransaction> {
        let Some(entries) = self.cache.get(mint) else {
            self.stats_misses.fetch_add(1, Ordering::Relaxed);
            return Vec::new();
        };

        let valid_txs: Vec<_> = entries
            .iter()
            .filter(|entry| entry.is_valid())
            .map(|entry| entry.transaction.clone())
            .collect();

        if valid_txs.is_empty() {
            self.stats_expired.fetch_add(1, Ordering::Relaxed);
            debug!(
                "precompiled cache expired: mint={} cached_count={}",
                mint,
                entries.len()
            );
        } else {
            self.stats_hits.fetch_add(1, Ordering::Relaxed);
        }

        valid_txs
    }

    /// Check if a mint has valid pre-compiled transactions.
    pub fn has_valid_txs(&self, mint: &Pubkey) -> bool {
        self.cache
            .get(mint)
            .map(|entries| entries.iter().any(|e| e.is_valid()))
            .unwrap_or(false)
    }

    /// Get the number of valid transactions for a mint.
    pub fn valid_tx_count(&self, mint: &Pubkey) -> usize {
        self.cache
            .get(mint)
            .map(|entries| entries.iter().filter(|e| e.is_valid()).count())
            .unwrap_or(0)
    }

    /// Invalidate all cached transactions for a mint.
    pub fn invalidate_mint(&self, mint: &Pubkey) {
        if self.cache.remove(mint).is_some() {
            debug!("precompiled cache invalidated: mint={}", mint);
        }
    }

    /// Clear all cached transactions.
    pub fn clear(&self) {
        self.cache.clear();
        info!("precompiled cache cleared: all mints");
    }

    /// Remove expired entries from the cache.
    ///
    /// Returns the number of mints that were cleaned up.
    pub fn cleanup_expired(&self) -> usize {
        let mut cleaned = 0;

        self.cache.retain(|mint, entries| {
            let before = entries.len();
            entries.retain(|e| e.is_valid());

            if entries.is_empty() {
                debug!("precompiled cache evicted: mint={} reason=all_expired", mint);
                cleaned += 1;
                false
            } else {
                if entries.len() < before {
                    debug!(
                        "precompiled cache partial cleanup: mint={} removed={} remaining={}",
                        mint,
                        before - entries.len(),
                        entries.len()
                    );
                }
                true
            }
        });

        cleaned
    }

    /// Get cache statistics.
    pub fn stats(&self) -> PreCompiledCacheStats {
        let total_transactions: usize = self.cache.iter().map(|e| e.value().len()).sum();

        PreCompiledCacheStats {
            total_mints: self.cache.len(),
            total_transactions,
            cache_hits: self.stats_hits.load(Ordering::Relaxed),
            cache_misses: self.stats_misses.load(Ordering::Relaxed),
            cache_expired: self.stats_expired.load(Ordering::Relaxed),
            refresh_count: self.stats_refreshes.load(Ordering::Relaxed),
        }
    }

    /// Get detailed information about cached entries for a mint.
    pub fn get_mint_info(&self, mint: &Pubkey) -> Option<Vec<PreCompiledEntryInfo>> {
        self.cache.get(mint).map(|entries| {
            entries
                .iter()
                .map(|e| PreCompiledEntryInfo {
                    route_group_index: e.route_group_index,
                    dlmm_count: e.dlmm_count,
                    is_valid: e.is_valid(),
                    ttl_ms: e.ttl_ms(),
                    blockhash: e.blockhash,
                })
                .collect()
        })
    }
}

/// Information about a cached entry (for debugging/monitoring).
#[derive(Debug, Clone)]
pub struct PreCompiledEntryInfo {
    pub route_group_index: usize,
    pub dlmm_count: usize,
    pub is_valid: bool,
    pub ttl_ms: u64,
    pub blockhash: Hash,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::sol_mint;
    use crate::registry::{DlmmRouteState, PoolLiquidity, PumpRouteState};
    use solana_sdk::signature::Keypair;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn liquidity() -> Option<PoolLiquidity> {
        Some(PoolLiquidity {
            base_lamports: 2_000_000_000, // 2 SOL
            token_lamports: Some(2_000_000),
            updated_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        })
    }

    fn pump() -> PumpRouteState {
        PumpRouteState {
            pool: pk(1),
            base_mint: sol_mint(),
            token_vault: pk(2),
            base_vault: pk(3),
            fee_wallet: pk(4),
            fee_token_wallet: pk(5),
            coin_creator_vault_ata: pk(6),
            coin_creator_vault_authority: pk(7),
            coin_creator: pk(8),
            is_cashback_coin: false,
            liquidity: liquidity(),
            enabled: true,
            last_update_slot: 10,
        }
    }

    fn dlmm(byte: u8) -> DlmmRouteState {
        DlmmRouteState {
            program_id: pk(byte),
            base_mint: sol_mint(),
            event_authority: pk(byte + 1),
            memo_program: None,
            lb_pair: pk(byte + 2),
            token_vault: pk(byte + 3),
            base_vault: pk(byte + 4),
            oracle: pk(byte + 5),
            bin_array_bitmap_extension: None,
            bin_arrays: Vec::new(),
            active_id: 0,
            liquidity: liquidity(),
            enabled: true,
            last_update_slot: 10,
        }
    }

    #[test]
    fn cache_starts_empty() {
        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let cache = PreCompiledTxCache::new(packer);

        assert_eq!(cache.stats().total_mints, 0);
        assert_eq!(cache.stats().total_transactions, 0);
    }

    #[test]
    fn get_ready_txs_returns_empty_for_unknown_mint() {
        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let cache = PreCompiledTxCache::new(packer);

        let txs = cache.get_ready_txs(&pk(99));

        assert!(txs.is_empty());
        assert_eq!(cache.stats().cache_misses, 1);
    }

    #[test]
    fn invalidate_removes_mint_from_cache() {
        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let cache = PreCompiledTxCache::new(packer);
        let mint = pk(100);

        // Manually insert an entry for testing
        cache.cache.insert(
            mint,
            vec![PreCompiledEntry {
                transaction: VersionedTransaction::default(),
                route_group_index: 0,
                dlmm_count: 2,
                compiled_at: Instant::now(),
                blockhash: Hash::new_unique(),
                expires_at: Instant::now() + std::time::Duration::from_secs(30),
                nonce_pubkey: None,
            }],
        );

        assert!(cache.has_valid_txs(&mint));

        cache.invalidate_mint(&mint);

        assert!(!cache.has_valid_txs(&mint));
    }

    #[test]
    fn cleanup_removes_expired_entries() {
        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let cache = PreCompiledTxCache::with_ttl(packer, 1); // 1ms TTL
        let mint = pk(100);

        // Insert entry with very short TTL
        cache.cache.insert(
            mint,
            vec![PreCompiledEntry {
                transaction: VersionedTransaction::default(),
                route_group_index: 0,
                dlmm_count: 2,
                compiled_at: Instant::now(),
                blockhash: Hash::new_unique(),
                expires_at: Instant::now(), // Already expired
                nonce_pubkey: None,
            }],
        );

        std::thread::sleep(std::time::Duration::from_millis(2));

        let cleaned = cache.cleanup_expired();

        assert_eq!(cleaned, 1);
        assert!(!cache.has_valid_txs(&mint));
    }
}
