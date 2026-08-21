//! In-memory runtime state for allowlisted mints and candidate pools.

use crate::constants::sol_mint;
use solana_program::instruction::AccountMeta;
use solana_program::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

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

#[derive(Debug, Clone)]
pub struct RuntimeRegistry {
    allowed_mints: HashSet<Pubkey>,
    mints: HashMap<Pubkey, MintRuntimeState>,
}

impl RuntimeRegistry {
    pub fn new(allowed_mints: impl IntoIterator<Item = Pubkey>) -> anyhow::Result<Self> {
        let allowed_mints = allowed_mints.into_iter().collect::<HashSet<_>>();
        if allowed_mints.is_empty() {
            anyhow::bail!("allowed_mints must not be empty");
        }

        Ok(Self {
            allowed_mints,
            mints: HashMap::new(),
        })
    }

    pub fn is_allowed(&self, mint: &Pubkey) -> bool {
        self.allowed_mints.contains(mint)
    }

    pub fn allowed_mints(&self) -> impl Iterator<Item = Pubkey> + '_ {
        self.allowed_mints.iter().copied()
    }

    pub fn upsert_mint(&mut self, state: MintRuntimeState) -> anyhow::Result<()> {
        if !self.is_allowed(&state.mint) {
            anyhow::bail!("mint {} is not allowlisted", state.mint);
        }

        self.mints.insert(state.mint, state);
        Ok(())
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
