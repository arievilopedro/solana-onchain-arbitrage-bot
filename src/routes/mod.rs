//! Route grouping and packing.

use crate::registry::{
    CpmmRouteState, DammV2RouteState, DlmmRouteState, MintRuntimeState, PumpRouteState,
};
use solana_program::pubkey::Pubkey;

/// A packed arb opportunity for `mint`. Contains an *optional* Pump AMM pool
/// plus a chunk of DLMMs (chunked by `FixedDlmmRoutePacker::max_dlmm_per_tx`)
/// plus any eligible Raydium CPMM / Meteora DAMM v2 pools, all sized to fit
/// in one v0 tx.
///
/// `pump` is `Option` because compositions like `Raydium CPMM + Meteora DLMM`,
/// `Raydium CPMM + Meteora DAMM v2` or `Meteora DAMM v2 + Meteora DLMM` are
/// valid arb targets that do not require a Pump AMM leg. Downstream consumers
/// (transaction builder, ATA preparation) MUST handle the `None` case.
///
/// `raydium_cps` and `damm_v2s` are concrete (non-generic) because in
/// practice they are only ever `CpmmRouteState`/`DammV2RouteState`; keeping
/// them off the generic parameter list avoids a 4-way ripple across every
/// consumer signature.
#[derive(Debug, Clone)]
pub struct RouteGroup<TPump, TDlmm> {
    pub mint: Pubkey,
    pub pump: Option<TPump>,
    pub dlmms: Vec<TDlmm>,
    pub raydium_cps: Vec<CpmmRouteState>,
    pub damm_v2s: Vec<DammV2RouteState>,
}

#[derive(Debug, Clone)]
pub struct FixedDlmmRoutePacker {
    pub max_dlmm_per_tx: usize,
}

impl FixedDlmmRoutePacker {
    pub fn new(max_dlmm_per_tx: usize) -> anyhow::Result<Self> {
        if max_dlmm_per_tx == 0 {
            anyhow::bail!("max_dlmm_per_tx must be greater than zero");
        }

        Ok(Self { max_dlmm_per_tx })
    }

    pub fn pack<T: Clone>(&self, dlmms: &[T]) -> Vec<Vec<T>> {
        dlmms
            .chunks(self.max_dlmm_per_tx)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    /// Build one or more [`RouteGroup`]s for `state`. A group is produced iff
    /// the mint has **at least two distinct pool types** among Pump / DLMM /
    /// CPMM / DAMM v2 — the Discovery gate rule (see
    /// `alt::StableMintRouteAccounts::from_mint_runtime_state`).
    ///
    /// Grouping rules:
    /// - If there are eligible DLMMs, they are chunked by `max_dlmm_per_tx`
    ///   and each chunk becomes a group carrying the (single) eligible pump
    ///   plus the full CPMM+DAMM v2 sets.
    /// - If there are no eligible DLMMs but there are ≥2 non-DLMM pool types
    ///   (or ≥1 non-DLMM pool type plus pump), a single DLMM-less group is
    ///   produced.
    /// - Otherwise (only one pool type present, or no pools at all), returns
    ///   empty.
    pub fn pack_mint_state(
        &self,
        state: &MintRuntimeState,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> Vec<RouteGroup<PumpRouteState, DlmmRouteState>> {
        let pump = state
            .eligible_pumps(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .next()
            .cloned();

        let dlmms = state
            .eligible_dlmms(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        let raydium_cps = state
            .eligible_raydium_cps(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        let damm_v2s = state
            .eligible_damm_v2s(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        // Discovery gate: need ≥2 distinct pool types for a valid arb.
        let types_present = (pump.is_some() as usize)
            + (!dlmms.is_empty() as usize)
            + (!raydium_cps.is_empty() as usize)
            + (!damm_v2s.is_empty() as usize);
        if types_present < 2 {
            return Vec::new();
        }

        if dlmms.is_empty() {
            // DLMM-less composition (e.g. Pump+CPMM, CPMM+DAMM v2, Pump+DAMM
            // v2). Produce a single group with all non-DLMM pools.
            return vec![RouteGroup {
                mint: state.mint,
                pump,
                dlmms: Vec::new(),
                raydium_cps,
                damm_v2s,
            }];
        }

        self.pack(&dlmms)
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(|dlmms| RouteGroup {
                mint: state.mint,
                pump: pump.clone(),
                dlmms,
                raydium_cps: raydium_cps.clone(),
                damm_v2s: damm_v2s.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::sol_mint;
    use crate::registry::{
        CpmmRouteState, DammV2RouteState, DlmmRouteState, PoolLiquidity, PumpRouteState,
    };

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn liquidity() -> Option<PoolLiquidity> {
        Some(PoolLiquidity {
            base_lamports: 2_000,
            token_lamports: Some(2_000_000),
            updated_at_ms: 1_000,
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

    fn cpmm(byte: u8) -> CpmmRouteState {
        CpmmRouteState {
            program_id: pk(byte),
            base_mint: sol_mint(),
            authority: pk(byte + 1),
            pool: pk(byte + 2),
            amm_config: pk(byte + 3),
            token_vault: pk(byte + 4),
            base_vault: pk(byte + 5),
            observation: pk(byte + 6),
            liquidity: liquidity(),
            enabled: true,
            last_update_slot: 10,
        }
    }

    fn damm_v2(byte: u8) -> DammV2RouteState {
        DammV2RouteState {
            program_id: pk(byte),
            base_mint: sol_mint(),
            event_authority: pk(byte + 1),
            pool_authority: pk(byte + 2),
            pool: pk(byte + 3),
            token_vault: pk(byte + 4),
            base_vault: pk(byte + 5),
            liquidity: liquidity(),
            enabled: true,
            last_update_slot: 10,
        }
    }

    #[test]
    fn fixed_packer_chunks_eligible_dlmms() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.pump.push(pump());
        state.dlmms = vec![dlmm(10), dlmm(20), dlmm(30), dlmm(40)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].dlmms.len(), 3);
        assert_eq!(groups[1].dlmms.len(), 1);
        assert!(groups[0].pump.is_some());
        assert!(groups[0].raydium_cps.is_empty());
        assert!(groups[0].damm_v2s.is_empty());
    }

    #[test]
    fn fixed_packer_propagates_cpmm_and_damm_v2_to_every_group() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.pump.push(pump());
        state.dlmms = vec![dlmm(10), dlmm(20), dlmm(30), dlmm(40)];
        state.raydium_cps = vec![cpmm(50), cpmm(60)];
        state.damm_v2s = vec![damm_v2(70)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        // 4 DLMMs / 3 per tx = 2 groups; each carries the full CPMM+DAMM v2 set.
        assert_eq!(groups.len(), 2);
        for group in &groups {
            assert_eq!(group.raydium_cps.len(), 2);
            assert_eq!(group.damm_v2s.len(), 1);
        }
    }

    #[test]
    fn fixed_packer_skips_disabled_cpmm_and_damm_v2() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.pump.push(pump());
        state.dlmms = vec![dlmm(10)];
        let mut disabled_cpmm = cpmm(50);
        disabled_cpmm.enabled = false;
        state.raydium_cps = vec![cpmm(60), disabled_cpmm];
        let mut disabled_damm = damm_v2(70);
        disabled_damm.enabled = false;
        state.damm_v2s = vec![disabled_damm];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].raydium_cps.len(), 1);
        assert!(groups[0].damm_v2s.is_empty());
    }

    #[test]
    fn fixed_packer_returns_no_groups_without_eligible_pump_and_only_dlmms() {
        // Only DLMMs eligible (pump disabled, no other pool types) → single
        // pool type → gate rejects.
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        let mut disabled_pump = pump();
        disabled_pump.enabled = false;
        state.pump.push(disabled_pump);
        state.dlmms = vec![dlmm(10), dlmm(20)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert!(groups.is_empty());
    }

    #[test]
    fn fixed_packer_accepts_cpmm_plus_dlmm_without_pump() {
        // No pump; CPMM + DLMM = 2 pool types → gate accepts.
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.dlmms = vec![dlmm(10), dlmm(20)];
        state.raydium_cps = vec![cpmm(50)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert_eq!(groups.len(), 1);
        assert!(groups[0].pump.is_none());
        assert_eq!(groups[0].dlmms.len(), 2);
        assert_eq!(groups[0].raydium_cps.len(), 1);
        assert!(groups[0].damm_v2s.is_empty());
    }

    #[test]
    fn fixed_packer_accepts_damm_v2_plus_dlmm_without_pump() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.dlmms = vec![dlmm(10)];
        state.damm_v2s = vec![damm_v2(70), damm_v2(80)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert_eq!(groups.len(), 1);
        assert!(groups[0].pump.is_none());
        assert_eq!(groups[0].dlmms.len(), 1);
        assert_eq!(groups[0].damm_v2s.len(), 2);
    }

    #[test]
    fn fixed_packer_accepts_cpmm_plus_damm_v2_without_dlmm_or_pump() {
        // No pump, no DLMM. CPMM + DAMM v2 = 2 pool types → single DLMM-less
        // group.
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.raydium_cps = vec![cpmm(50)];
        state.damm_v2s = vec![damm_v2(70)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert_eq!(groups.len(), 1);
        assert!(groups[0].pump.is_none());
        assert!(groups[0].dlmms.is_empty());
        assert_eq!(groups[0].raydium_cps.len(), 1);
        assert_eq!(groups[0].damm_v2s.len(), 1);
    }

    #[test]
    fn fixed_packer_accepts_pump_plus_cpmm_without_dlmm() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.pump.push(pump());
        state.raydium_cps = vec![cpmm(50)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert_eq!(groups.len(), 1);
        assert!(groups[0].pump.is_some());
        assert!(groups[0].dlmms.is_empty());
        assert_eq!(groups[0].raydium_cps.len(), 1);
    }

    #[test]
    fn fixed_packer_rejects_single_pool_type_cpmm_only() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        state.raydium_cps = vec![cpmm(50), cpmm(60)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert!(groups.is_empty());
    }

    #[test]
    fn fixed_packer_rejects_zero_pools() {
        let state = MintRuntimeState::new(pk(100), spl_token::ID);

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert!(groups.is_empty());
    }
}
