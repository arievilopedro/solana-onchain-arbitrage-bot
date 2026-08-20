//! Route grouping and packing.

use crate::registry::{DlmmRouteState, MintRuntimeState, PumpRouteState};
use solana_program::pubkey::Pubkey;

#[derive(Debug, Clone)]
pub struct RouteGroup<TPump, TDlmm> {
    pub mint: Pubkey,
    pub pump: TPump,
    pub dlmms: Vec<TDlmm>,
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

    pub fn pack_mint_state(
        &self,
        state: &MintRuntimeState,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> Vec<RouteGroup<PumpRouteState, DlmmRouteState>> {
        let Some(pump) = state
            .eligible_pumps(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .next()
            .cloned()
        else {
            return Vec::new();
        };

        let dlmms = state
            .eligible_dlmms(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        self.pack(&dlmms)
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(|dlmms| RouteGroup {
                mint: state.mint,
                pump: pump.clone(),
                dlmms,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::sol_mint;
    use crate::registry::{DlmmRouteState, PoolLiquidity, PumpRouteState};

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn liquidity() -> Option<PoolLiquidity> {
        Some(PoolLiquidity {
            base_lamports: 2_000,
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
    }

    #[test]
    fn fixed_packer_returns_no_groups_without_eligible_pump() {
        let mut state = MintRuntimeState::new(pk(100), spl_token::ID);
        let mut disabled_pump = pump();
        disabled_pump.enabled = false;
        state.pump.push(disabled_pump);
        state.dlmms = vec![dlmm(10), dlmm(20)];

        let packer = FixedDlmmRoutePacker::new(3).unwrap();
        let groups = packer.pack_mint_state(&state, 1_000, 500, 1_100);

        assert!(groups.is_empty());
    }
}
