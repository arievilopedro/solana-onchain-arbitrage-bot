use crate::dex::meteora::constants::{dlmm_program_id, BIN_ARRAY};
use anyhow::Result;
use solana_program::pubkey::Pubkey;
use std::mem::{offset_of, size_of};
use tracing::info;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ProtocolFee {
    pub amount_x: u64,
    pub amount_y: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RewardInfo {
    pub mint: Pubkey,
    pub vault: Pubkey,
    pub funder: Pubkey,
    pub reward_duration: u64,
    pub reward_duration_end: u64,
    pub reward_rate: u128,
    pub last_update_time: u64,
    pub cumulative_seconds_with_empty_liquidity_reward: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct StaticParameters {
    pub base_factor: u16,
    pub filter_period: u16,
    pub decay_period: u16,
    pub reduction_factor: u16,
    pub variable_fee_control: u32,
    pub max_volatility_accumulator: u32,
    pub min_bin_id: i32,
    pub max_bin_id: i32,
    pub protocol_share: u16,
    pub _padding: [u8; 6],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct VariableParameters {
    pub volatility_accumulator: u32,
    pub volatility_reference: u32,
    pub index_reference: i32,
    pub _padding: [u8; 4],
    pub last_update_timestamp: i64,
    pub _padding_1: [u8; 8],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct LbPair {
    pub parameters: StaticParameters,
    pub v_parameters: VariableParameters,
    pub bump_seed: [u8; 1],
    pub bin_step_seed: [u8; 2],
    pub pair_type: u8,
    pub active_id: i32,
    pub bin_step: u16,
    pub status: u8,
    pub require_base_factor_seed: u8,
    pub base_factor_seed: [u8; 2],
    pub activation_type: u8,
    pub _padding_0: u8,
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub reserve_x: Pubkey,
    pub reserve_y: Pubkey,
    pub protocol_fee: ProtocolFee,
    pub _padding_1: [u8; 32],
    pub reward_infos: [RewardInfo; 2],
    pub oracle: Pubkey,
    pub bin_array_bitmap: [u64; 16],
    pub last_updated_at: i64,
    pub _padding_2: [u8; 32],
    pub pre_activation_swap_address: Pubkey,
    pub base_key: Pubkey,
    pub activation_point: u64,
    pub pre_activation_duration: u64,
    pub _padding_3: [u8; 8],
    pub _padding_4: u64,
    pub creator: Pubkey,
    pub _reserved: [u8; 24],
}

#[derive(Debug)]
pub struct DlmmInfo {
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub token_x_vault: Pubkey,
    pub token_y_vault: Pubkey,
    pub oracle: Pubkey,
    pub active_id: i32,
    pub lb_pair: LbPair,
}

impl DlmmInfo {
    pub fn token_x_mint_gpa_offset() -> usize {
        8 + offset_of!(LbPair, token_x_mint)
    }

    pub fn token_y_mint_gpa_offset() -> usize {
        8 + offset_of!(LbPair, token_y_mint)
    }

    pub fn load_checked(data: &[u8]) -> Result<Self> {
        if data.len() < 8 + size_of::<LbPair>() {
            return Err(anyhow::anyhow!("Invalid data length for DlmmInfo"));
        }

        let raw_lb_pair = &data[8..8 + size_of::<LbPair>()];

        let lb_pair: LbPair = unsafe {
            assert!(raw_lb_pair.len() >= std::mem::size_of::<LbPair>());
            std::ptr::read_unaligned(raw_lb_pair.as_ptr() as *const LbPair)
        };

        Ok(Self {
            token_x_mint: lb_pair.token_x_mint,
            token_y_mint: lb_pair.token_y_mint,
            token_x_vault: lb_pair.reserve_x,
            token_y_vault: lb_pair.reserve_y,
            oracle: lb_pair.oracle,
            active_id: lb_pair.active_id,
            lb_pair,
        })
    }

    pub fn get_token_and_sol_vaults(&self, mint: &Pubkey, sol_mint: &Pubkey) -> (Pubkey, Pubkey) {
        let token_vault;
        let sol_vault;

        if sol_mint == &self.token_x_mint {
            sol_vault = self.token_x_vault;
            token_vault = self.token_y_vault;
        } else if sol_mint == &self.token_y_mint {
            sol_vault = self.token_y_vault;
            token_vault = self.token_x_vault;
        } else {
            if mint == &self.token_x_mint {
                token_vault = self.token_x_vault;
                sol_vault = self.token_y_vault;
            } else {
                token_vault = self.token_y_vault;
                sol_vault = self.token_x_vault;
            }
        }

        (token_vault, sol_vault)
    }

    pub fn calculate_bin_arrays(&self, pair_pubkey: &Pubkey) -> Result<Vec<Pubkey>> {
        let bin_array_index = self.bin_id_to_bin_array_index(self.active_id)?;

        // Emit 3 bin_arrays centred on the active bin (covers ~210 bins of
        // price range). Reference executors on-chain (SolanaMevBot) use the
        // same width; going wider costs 32 B per extra array and risks
        // exceeding the 1232 B packet limit on multi-DLMM triggers.
        let mut bin_arrays = Vec::new();
        let offsets = [-1, 0, 1];

        for offset in offsets {
            let array_idx = bin_array_index + offset;
            let array_pda = self.derive_bin_array_pda(pair_pubkey, array_idx as i64)?;
            bin_arrays.push(array_pda);
        }

        Ok(bin_arrays)
    }

    fn div_rem(&self, bin_id: i32, other: i32) -> (i32, i32) {
        (bin_id / other, bin_id % other)
    }

    pub fn bin_id_to_bin_array_index(&self, bin_id: i32) -> Result<i32> {
        const BIN_ARRAY_SIZE: i32 = 70;
        let (idx, rem) = self.div_rem(bin_id, BIN_ARRAY_SIZE);

        if bin_id.is_negative() && rem != 0 {
            Ok(idx - 1)
        } else {
            Ok(idx)
        }
    }

    fn derive_bin_array_pda(&self, lb_pair: &Pubkey, index: i64) -> Result<Pubkey> {
        let seeds = [BIN_ARRAY, lb_pair.as_ref(), &index.to_le_bytes()[0..8]];

        let (pda, _) = Pubkey::find_program_address(&seeds, &dlmm_program_id());

        Ok(pda)
    }
}

impl LbPair {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < size_of::<Self>() {
            return Err(anyhow::anyhow!("Data is too small for LbPair"));
        }

        let lb_pair = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const LbPair) };

        Ok(lb_pair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info_with_active_id(active_id: i32) -> DlmmInfo {
        // Only `active_id` matters for the bin_array PDA calculation; the
        // remaining fields can be zeroed.
        let mut lb_pair: LbPair = unsafe { std::mem::zeroed() };
        lb_pair.active_id = active_id;
        DlmmInfo {
            token_x_mint: Pubkey::default(),
            token_y_mint: Pubkey::default(),
            token_x_vault: Pubkey::default(),
            token_y_vault: Pubkey::default(),
            oracle: Pubkey::default(),
            active_id,
            lb_pair,
        }
    }

    #[test]
    fn calculate_bin_arrays_returns_three_pdas_centered_on_active_id() {
        let info = info_with_active_id(140); // bin_array_index = 2
        let pair = Pubkey::new_unique();
        let arrays = info.calculate_bin_arrays(&pair).unwrap();

        assert_eq!(arrays.len(), 3, "must emit exactly 3 bin_arrays");

        let expected: Vec<Pubkey> = [1i64, 2, 3]
            .iter()
            .map(|idx| {
                let seeds = [BIN_ARRAY, pair.as_ref(), &idx.to_le_bytes()[0..8]];
                Pubkey::find_program_address(&seeds, &dlmm_program_id()).0
            })
            .collect();
        assert_eq!(arrays, expected);
    }

    #[test]
    fn calculate_bin_arrays_handles_negative_active_id() {
        // Negative active_id with non-zero remainder floors toward -inf:
        // bin_id_to_bin_array_index(-100) = (-100/70=-1) - 1 = -2.
        // Offsets [-1, 0, 1] therefore yield indices [-3, -2, -1].
        let info = info_with_active_id(-100);
        let pair = Pubkey::new_unique();
        let arrays = info.calculate_bin_arrays(&pair).unwrap();

        assert_eq!(arrays.len(), 3);
        let expected: Vec<Pubkey> = [-3i64, -2, -1]
            .iter()
            .map(|idx| {
                let seeds = [BIN_ARRAY, pair.as_ref(), &idx.to_le_bytes()[0..8]];
                Pubkey::find_program_address(&seeds, &dlmm_program_id()).0
            })
            .collect();
        assert_eq!(arrays, expected);
    }
}
