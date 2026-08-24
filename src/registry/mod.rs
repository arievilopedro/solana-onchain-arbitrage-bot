//! In-memory runtime state for allowlisted mints and candidate pools.

use crate::constants::sol_mint;
use arc_swap::ArcSwap;
use solana_program::instruction::AccountMeta;
use solana_program::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PoolKind {
    Pump,
    MeteoraDlmm,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PoolRejectReason {
    NonSolBase,
    BelowMinLiquidity,
    StaleState,
    MissingLiquidity,
    Disabled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PoolEligibility {
    Eligible,
    Rejected(PoolRejectReason),
}

impl PoolEligibility {
    pub fn is_eligible(self) -> bool {
        matches!(self, PoolEligibility::Eligible)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PoolLiquidity {
    pub base_lamports: u64,
    pub token_lamports: Option<u64>,
    pub updated_at_ms: u128,
}

#[derive(Debug, Clone)]
pub struct PumpRouteState {
    pub pool: Pubkey,
    pub base_mint: Pubkey,
    pub token_vault: Pubkey,
    pub base_vault: Pubkey,
    pub fee_wallet: Pubkey,
    pub fee_token_wallet: Pubkey,
    pub coin_creator_vault_ata: Pubkey,
    pub coin_creator_vault_authority: Pubkey,
    pub coin_creator: Pubkey,
    pub is_cashback_coin: bool,
    pub liquidity: Option<PoolLiquidity>,
    pub enabled: bool,
    pub last_update_slot: u64,
}

#[derive(Debug, Clone)]
pub struct DlmmRouteState {
    pub program_id: Pubkey,
    pub base_mint: Pubkey,
    pub event_authority: Pubkey,
    pub memo_program: Option<Pubkey>,
    pub lb_pair: Pubkey,
    pub token_vault: Pubkey,
    pub base_vault: Pubkey,
    pub oracle: Pubkey,
    pub bin_array_bitmap_extension: Option<Pubkey>,
    pub bin_arrays: Vec<AccountMeta>,
    pub active_id: i32,
    pub liquidity: Option<PoolLiquidity>,
    pub enabled: bool,
    pub last_update_slot: u64,
}

#[derive(Debug, Clone)]
pub struct MintRuntimeState {
    pub mint: Pubkey,
    pub token_program: Pubkey,
    pub pump: Vec<PumpRouteState>,
    pub dlmms: Vec<DlmmRouteState>,
    pub updated_slot: u64,
}

impl MintRuntimeState {
    pub fn new(mint: Pubkey, token_program: Pubkey) -> Self {
        Self {
            mint,
            token_program,
            pump: Vec::new(),
            dlmms: Vec::new(),
            updated_slot: 0,
        }
    }

    pub fn eligible_pumps(
        &self,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> Vec<&PumpRouteState> {
        self.pump
            .iter()
            .filter(|pool| {
                pool.eligibility(min_base_liquidity_lamports, max_state_age_ms, now_ms)
                    .is_eligible()
            })
            .collect()
    }

    pub fn eligible_dlmms(
        &self,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> Vec<&DlmmRouteState> {
        self.dlmms
            .iter()
            .filter(|pool| {
                pool.eligibility(min_base_liquidity_lamports, max_state_age_ms, now_ms)
                    .is_eligible()
            })
            .collect()
    }
}

impl PumpRouteState {
    pub fn eligibility(
        &self,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> PoolEligibility {
        pool_eligibility(
            self.enabled,
            self.base_mint,
            self.liquidity,
            min_base_liquidity_lamports,
            max_state_age_ms,
            now_ms,
        )
    }
}

impl DlmmRouteState {
    pub fn eligibility(
        &self,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> PoolEligibility {
        pool_eligibility(
            self.enabled,
            self.base_mint,
            self.liquidity,
            min_base_liquidity_lamports,
            max_state_age_ms,
            now_ms,
        )
    }
}

/// Result of `replace_active_set`: mints added to and removed from the
/// allowlist as part of a single atomic swap. Seed mints are never in
/// `removed` (invariant `A ⊇ S` enforced at swap time).
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ActiveSetDelta {
    pub added: Vec<Pubkey>,
    pub removed: Vec<Pubkey>,
}

/// Runtime allowlist + pool state.
///
/// The `allowed` set is exposed via an `ArcSwap` so hot-path readers (RPC
/// bootstrap loops, trigger streams) can snapshot without locking. The `mints`
/// HashMap continues to live behind the outer `Mutex<RuntimeRegistry>` that
/// callers already hold. `seed` is captured at construction time and never
/// mutated; `replace_active_set`/`evict_mint` refuse to drop seed entries.
#[derive(Debug, Clone)]
pub struct RuntimeRegistry {
    allowed: Arc<ArcSwap<HashSet<Pubkey>>>,
    mints: HashMap<Pubkey, MintRuntimeState>,
    seed: Arc<HashSet<Pubkey>>,
}

impl RuntimeRegistry {
    /// Constructs a registry whose seed and initial active set are identical.
    /// Backwards-compatible with the pre-M3b constructor.
    pub fn new(allowed_mints: impl IntoIterator<Item = Pubkey>) -> anyhow::Result<Self> {
        Self::new_with_seed(allowed_mints)
    }

    /// Constructs a registry with a permanent seed set. The active allowlist is
    /// initialised to the seed; `replace_active_set` may extend it later while
    /// preserving the seed as an invariant.
    pub fn new_with_seed(seed: impl IntoIterator<Item = Pubkey>) -> anyhow::Result<Self> {
        let seed = seed.into_iter().collect::<HashSet<_>>();
        if seed.is_empty() {
            anyhow::bail!("allowed_mints must not be empty");
        }

        let allowed = Arc::new(ArcSwap::from_pointee(seed.clone()));
        Ok(Self {
            allowed,
            mints: HashMap::new(),
            seed: Arc::new(seed),
        })
    }

    pub fn is_allowed(&self, mint: &Pubkey) -> bool {
        self.allowed.load().contains(mint)
    }

    /// Returns a materialised snapshot of the current active allowlist.
    /// Cheap: `Arc` clone + iteration over a small set.
    pub fn allowed_mints(&self) -> Vec<Pubkey> {
        self.allowed.load().iter().copied().collect()
    }

    /// Lock-free snapshot; use when a stable `Arc<HashSet<_>>` is preferable
    /// (e.g. to pass into a background task without cloning the set).
    pub fn allowed_snapshot(&self) -> Arc<HashSet<Pubkey>> {
        self.allowed.load_full()
    }

    /// Shared handle to the underlying `ArcSwap` for callers that need to read
    /// without going through the outer registry mutex (hot path).
    pub fn allowed_handle(&self) -> Arc<ArcSwap<HashSet<Pubkey>>> {
        Arc::clone(&self.allowed)
    }

    /// The permanent seed set. Never mutated after construction.
    pub fn seed(&self) -> &HashSet<Pubkey> {
        &self.seed
    }

    pub fn upsert_mint(&mut self, state: MintRuntimeState) -> anyhow::Result<()> {
        if !self.is_allowed(&state.mint) {
            anyhow::bail!("mint {} is not allowlisted", state.mint);
        }

        self.mints.insert(state.mint, state);
        Ok(())
    }

    /// Atomically admits `mint` to the allowlist and installs its initial
    /// pool state. Fails if the mint is already present (either in the
    /// allowlist or the pool map) to avoid overwriting live state.
    pub fn admit_mint_with_initial_state(
        &mut self,
        state: MintRuntimeState,
    ) -> anyhow::Result<()> {
        let mint = state.mint;
        if self.mints.contains_key(&mint) {
            anyhow::bail!("mint {} already has runtime state", mint);
        }

        let current = self.allowed.load();
        if current.contains(&mint) {
            anyhow::bail!("mint {} is already allowlisted", mint);
        }

        let mut next = HashSet::clone(&current);
        next.insert(mint);
        // Insert state first so any observer that sees the swapped allowlist
        // also finds the pool map populated (single-threaded writer path).
        self.mints.insert(mint, state);
        self.allowed.store(Arc::new(next));
        Ok(())
    }

    /// Removes `mint` from the allowlist and drops its pool state. Refuses to
    /// evict a seed mint (invariant `A ⊇ S`).
    pub fn evict_mint(&mut self, mint: Pubkey) -> anyhow::Result<Option<MintRuntimeState>> {
        if self.seed.contains(&mint) {
            anyhow::bail!("refusing to evict seed mint {}", mint);
        }

        let current = self.allowed.load();
        if !current.contains(&mint) {
            return Ok(self.mints.remove(&mint));
        }

        let mut next = HashSet::clone(&current);
        next.remove(&mint);
        self.allowed.store(Arc::new(next));
        Ok(self.mints.remove(&mint))
    }

    /// Replaces the active allowlist with `desired ∪ seed`. Any mints removed
    /// from the active set have their pool state dropped. Returns the delta
    /// so the caller can drive downstream reconciliation (gRPC re-subscribe,
    /// ALT retire, etc.).
    pub fn replace_active_set(
        &mut self,
        desired: HashSet<Pubkey>,
    ) -> anyhow::Result<ActiveSetDelta> {
        let mut next = desired;
        // Enforce seed invariant unconditionally.
        for mint in self.seed.iter() {
            next.insert(*mint);
        }

        let current = self.allowed.load_full();
        let added: Vec<Pubkey> = next.difference(&current).copied().collect();
        let removed: Vec<Pubkey> = current.difference(&next).copied().collect();

        if added.is_empty() && removed.is_empty() {
            return Ok(ActiveSetDelta::default());
        }

        // Drop pool state for evicted mints; new mints will be populated by
        // downstream discovery before their first hot-path use.
        for mint in &removed {
            self.mints.remove(mint);
        }

        self.allowed.store(Arc::new(next));
        Ok(ActiveSetDelta { added, removed })
    }

    pub fn get(&self, mint: &Pubkey) -> Option<&MintRuntimeState> {
        self.mints.get(mint)
    }

    pub fn get_mut(&mut self, mint: &Pubkey) -> Option<&mut MintRuntimeState> {
        self.mints.get_mut(mint)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Pubkey, &MintRuntimeState)> {
        self.mints.iter()
    }

    pub fn len(&self) -> usize {
        self.mints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mints.is_empty()
    }
}

fn pool_eligibility(
    enabled: bool,
    base_mint: Pubkey,
    liquidity: Option<PoolLiquidity>,
    min_base_liquidity_lamports: u64,
    max_state_age_ms: u64,
    now_ms: u128,
) -> PoolEligibility {
    if !enabled {
        return PoolEligibility::Rejected(PoolRejectReason::Disabled);
    }

    if base_mint != sol_mint() {
        return PoolEligibility::Rejected(PoolRejectReason::NonSolBase);
    }

    let Some(liquidity) = liquidity else {
        return PoolEligibility::Rejected(PoolRejectReason::MissingLiquidity);
    };

    if liquidity.base_lamports < min_base_liquidity_lamports {
        return PoolEligibility::Rejected(PoolRejectReason::BelowMinLiquidity);
    }

    if now_ms.saturating_sub(liquidity.updated_at_ms) > max_state_age_ms as u128 {
        return PoolEligibility::Rejected(PoolRejectReason::StaleState);
    }

    PoolEligibility::Eligible
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::usdc_mint;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn pump(base_mint: Pubkey, liquidity: Option<PoolLiquidity>) -> PumpRouteState {
        PumpRouteState {
            pool: pk(1),
            base_mint,
            token_vault: pk(2),
            base_vault: pk(3),
            fee_wallet: pk(4),
            fee_token_wallet: pk(5),
            coin_creator_vault_ata: pk(6),
            coin_creator_vault_authority: pk(7),
            coin_creator: pk(8),
            is_cashback_coin: false,
            liquidity,
            enabled: true,
            last_update_slot: 10,
        }
    }

    #[test]
    fn registry_rejects_non_allowlisted_mint() {
        let allowed = pk(10);
        let denied = pk(11);
        let mut registry = RuntimeRegistry::new([allowed]).unwrap();
        let state = MintRuntimeState::new(denied, spl_token::ID);

        assert!(registry.upsert_mint(state).is_err());
        assert!(registry.is_empty());
    }

    #[test]
    fn new_with_seed_freezes_seed_and_seeds_allowlist() {
        let a = pk(20);
        let b = pk(21);
        let registry = RuntimeRegistry::new_with_seed([a, b]).unwrap();

        assert!(registry.is_allowed(&a));
        assert!(registry.is_allowed(&b));
        assert_eq!(registry.seed().len(), 2);
        assert_eq!(registry.allowed_snapshot().len(), 2);
    }

    #[test]
    fn admit_mint_updates_allowlist_and_state_atomically() {
        let seed = pk(30);
        let hot = pk(31);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();

        let state = MintRuntimeState::new(hot, spl_token::ID);
        registry.admit_mint_with_initial_state(state).unwrap();

        assert!(registry.is_allowed(&hot));
        assert!(registry.get(&hot).is_some());
        assert_eq!(registry.allowed_mints().len(), 2);
    }

    #[test]
    fn admit_mint_rejects_duplicate() {
        let seed = pk(40);
        let hot = pk(41);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();

        registry
            .admit_mint_with_initial_state(MintRuntimeState::new(hot, spl_token::ID))
            .unwrap();
        assert!(registry
            .admit_mint_with_initial_state(MintRuntimeState::new(hot, spl_token::ID))
            .is_err());
    }

    #[test]
    fn admit_mint_rejects_seed_duplicate() {
        let seed = pk(50);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();
        assert!(registry
            .admit_mint_with_initial_state(MintRuntimeState::new(seed, spl_token::ID))
            .is_err());
    }

    #[test]
    fn evict_mint_refuses_seed() {
        let seed = pk(60);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();
        assert!(registry.evict_mint(seed).is_err());
        assert!(registry.is_allowed(&seed));
    }

    #[test]
    fn evict_mint_removes_allowlist_and_state() {
        let seed = pk(70);
        let hot = pk(71);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();
        registry
            .admit_mint_with_initial_state(MintRuntimeState::new(hot, spl_token::ID))
            .unwrap();

        let evicted = registry.evict_mint(hot).unwrap();
        assert!(evicted.is_some());
        assert!(!registry.is_allowed(&hot));
        assert!(registry.get(&hot).is_none());
    }

    #[test]
    fn evict_mint_missing_is_noop_ok() {
        let seed = pk(80);
        let missing = pk(81);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();
        assert!(registry.evict_mint(missing).unwrap().is_none());
    }

    #[test]
    fn replace_active_set_preserves_seed() {
        let seed_a = pk(90);
        let seed_b = pk(91);
        let hot = pk(92);
        let mut registry = RuntimeRegistry::new_with_seed([seed_a, seed_b]).unwrap();

        // Request an empty desired set: seed must still remain.
        let delta = registry.replace_active_set(HashSet::new()).unwrap();
        assert!(delta.added.is_empty());
        assert!(delta.removed.is_empty());
        assert!(registry.is_allowed(&seed_a));
        assert!(registry.is_allowed(&seed_b));

        // Admit a hot mint, then request replacing with an empty set: the hot
        // mint should be removed but seed must survive.
        registry
            .admit_mint_with_initial_state(MintRuntimeState::new(hot, spl_token::ID))
            .unwrap();
        let delta = registry.replace_active_set(HashSet::new()).unwrap();
        assert_eq!(delta.removed, vec![hot]);
        assert!(delta.added.is_empty());
        assert!(!registry.is_allowed(&hot));
        assert!(registry.get(&hot).is_none());
        assert!(registry.is_allowed(&seed_a));
    }

    #[test]
    fn replace_active_set_reports_added() {
        let seed = pk(100);
        let hot = pk(101);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();

        let mut desired = HashSet::new();
        desired.insert(hot);
        let delta = registry.replace_active_set(desired).unwrap();
        assert_eq!(delta.added, vec![hot]);
        assert!(delta.removed.is_empty());
        assert!(registry.is_allowed(&hot));
    }

    #[test]
    fn allowed_snapshot_is_lock_free_across_threads() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let seed = pk(110);
        let hot = pk(111);
        let mut registry = RuntimeRegistry::new_with_seed([seed]).unwrap();
        let handle = registry.allowed_handle();

        let stop = Arc::new(AtomicBool::new(false));
        let reader_stop = Arc::clone(&stop);
        let reader = thread::spawn(move || {
            let mut observed_hot = false;
            while !reader_stop.load(Ordering::Relaxed) {
                let snap = handle.load();
                // Seed must always be visible; hot may or may not be.
                assert!(snap.contains(&pk(110)));
                if snap.contains(&pk(111)) {
                    observed_hot = true;
                }
            }
            observed_hot
        });

        // Bounce the allowlist a few times.
        for _ in 0..64 {
            registry
                .admit_mint_with_initial_state(MintRuntimeState::new(hot, spl_token::ID))
                .unwrap();
            registry.evict_mint(hot).unwrap();
        }

        stop.store(true, Ordering::Relaxed);
        let _ = reader.join().unwrap();
    }

    #[test]
    fn pool_eligibility_requires_sol_base_liquidity_and_freshness() {
        let now_ms = 10_000;
        let min_liquidity = 1_000;
        let max_age_ms = 500;

        let eligible = pump(
            sol_mint(),
            Some(PoolLiquidity {
                base_lamports: 1_500,
                token_lamports: None,
                updated_at_ms: 9_900,
            }),
        );
        assert_eq!(
            eligible.eligibility(min_liquidity, max_age_ms, now_ms),
            PoolEligibility::Eligible
        );

        let non_sol = pump(
            usdc_mint(),
            Some(PoolLiquidity {
                base_lamports: 1_500,
                token_lamports: None,
                updated_at_ms: 9_900,
            }),
        );
        assert_eq!(
            non_sol.eligibility(min_liquidity, max_age_ms, now_ms),
            PoolEligibility::Rejected(PoolRejectReason::NonSolBase)
        );

        let low_liquidity = pump(
            sol_mint(),
            Some(PoolLiquidity {
                base_lamports: 999,
                token_lamports: None,
                updated_at_ms: 9_900,
            }),
        );
        assert_eq!(
            low_liquidity.eligibility(min_liquidity, max_age_ms, now_ms),
            PoolEligibility::Rejected(PoolRejectReason::BelowMinLiquidity)
        );

        let stale = pump(
            sol_mint(),
            Some(PoolLiquidity {
                base_lamports: 1_500,
                token_lamports: None,
                updated_at_ms: 9_000,
            }),
        );
        assert_eq!(
            stale.eligibility(min_liquidity, max_age_ms, now_ms),
            PoolEligibility::Rejected(PoolRejectReason::StaleState)
        );
    }
}
