//! Transaction assembly and execution pipeline.

use crate::pools::MintPoolData;
use crate::registry::{DlmmRouteState, PumpRouteState};
use crate::routes::RouteGroup;
use crate::sender::SenderTipConfig;
use crate::transaction::create_swap_instruction;
use solana_program::pubkey::Pubkey;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::message::v0::Message;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::system_instruction;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Debug, Clone)]
pub struct ControlledExecutionParams {
    pub compute_unit_limit: u32,
    pub compute_unit_price: u64,
    pub minimum_profit_lamports: u64,
    pub use_flashloan: bool,
    pub no_failure_mode: bool,
    pub sender_tip: Option<SenderTipConfig>,
}

pub fn build_controlled_transaction(
    wallet: &Keypair,
    route: &RouteGroup<PumpRouteState, DlmmRouteState>,
    token_program: Pubkey,
    recent_blockhash: Hash,
    lookup_tables: &[AddressLookupTableAccount],
    params: ControlledExecutionParams,
) -> anyhow::Result<VersionedTransaction> {
    build_controlled_transaction_internal(
        wallet,
        route,
        token_program,
        recent_blockhash,
        lookup_tables,
        params,
        None,
    )
}

/// Build a controlled transaction using a durable nonce.
///
/// The AdvanceNonceAccount instruction is added as the first instruction.
pub fn build_controlled_transaction_with_nonce(
    wallet: &Keypair,
    route: &RouteGroup<PumpRouteState, DlmmRouteState>,
    token_program: Pubkey,
    nonce_hash: Hash,
    nonce_pubkey: Pubkey,
    lookup_tables: &[AddressLookupTableAccount],
    params: ControlledExecutionParams,
) -> anyhow::Result<VersionedTransaction> {
    build_controlled_transaction_internal(
        wallet,
        route,
        token_program,
        nonce_hash,
        lookup_tables,
        params,
        Some(nonce_pubkey),
    )
}

fn build_controlled_transaction_internal(
    wallet: &Keypair,
    route: &RouteGroup<PumpRouteState, DlmmRouteState>,
    token_program: Pubkey,
    blockhash_or_nonce: Hash,
    lookup_tables: &[AddressLookupTableAccount],
    params: ControlledExecutionParams,
    nonce_pubkey: Option<Pubkey>,
) -> anyhow::Result<VersionedTransaction> {
    let mint_pool_data = route_group_to_mint_pool_data(route, &wallet.pubkey(), token_program);
    let mut instructions = Vec::new();

    // Durable nonce: AdvanceNonceAccount MUST be the first instruction
    if let Some(nonce_account) = nonce_pubkey {
        instructions.push(system_instruction::advance_nonce_account(
            &nonce_account,
            &wallet.pubkey(),
        ));
    }

    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(params.compute_unit_limit));
    if params.compute_unit_price > 0 {
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(params.compute_unit_price));
    }

    if let Some(tip) = &params.sender_tip {
        let lamports = tip.random_lamports();
        if lamports > 0 {
            if let Some(tip_account) = tip.random_account() {
                instructions.push(system_instruction::transfer(
                    &wallet.pubkey(),
                    &tip_account,
                    lamports,
                ));
            }
        }
    }
    instructions.push(create_swap_instruction(
        wallet,
        &mint_pool_data,
        params.compute_unit_limit,
        params.minimum_profit_lamports,
        params.use_flashloan,
        params.no_failure_mode,
    )?);

    let message = Message::try_compile(
        &wallet.pubkey(),
        &instructions,
        lookup_tables,
        blockhash_or_nonce,
    )?;

    Ok(VersionedTransaction::try_new(
        solana_sdk::message::VersionedMessage::V0(message),
        &[wallet],
    )?)
}

pub fn route_group_to_mint_pool_data(
    route: &RouteGroup<PumpRouteState, DlmmRouteState>,
    wallet: &Pubkey,
    token_program: Pubkey,
) -> MintPoolData {
    let mut pool_data = MintPoolData::new(route.mint, wallet, token_program);
    if let Some(pump) = &route.pump {
        pool_data.add_pump_pool(
            pump.pool,
            pump.token_vault,
            pump.base_vault,
            pump.fee_wallet,
            pump.fee_token_wallet,
            pump.coin_creator_vault_ata,
            pump.coin_creator_vault_authority,
            pump.coin_creator,
            route.mint,
            pump.base_mint,
            false,
            pump.is_cashback_coin,
        );
    }

    for dlmm in &route.dlmms {
        pool_data.add_dlmm_pool(
            dlmm.lb_pair,
            dlmm.token_vault,
            dlmm.base_vault,
            dlmm.oracle,
            dlmm.bin_array_bitmap_extension,
            dlmm.bin_arrays.iter().map(|meta| meta.pubkey).collect(),
            dlmm.memo_program,
            route.mint,
            dlmm.base_mint,
        );
    }

    for cp in &route.raydium_cps {
        pool_data.add_raydium_cp_pool(
            cp.pool,
            cp.token_vault,
            cp.base_vault,
            cp.amm_config,
            cp.observation,
            route.mint,
            cp.base_mint,
        );
    }

    for damm in &route.damm_v2s {
        pool_data.add_meteora_damm_v2_pool(
            damm.pool,
            damm.token_vault,
            damm.base_vault,
            route.mint,
            damm.base_mint,
        );
    }

    pool_data
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::sol_mint;
    use crate::registry::{CpmmRouteState, DammV2RouteState, PoolLiquidity, PumpRouteState};
    use solana_program::instruction::AccountMeta;
    use solana_sdk::message::VersionedMessage;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn pump() -> PumpRouteState {
        PumpRouteState {
            pool: pk(2),
            base_mint: sol_mint(),
            token_vault: pk(3),
            base_vault: pk(4),
            fee_wallet: pk(5),
            fee_token_wallet: pk(6),
            coin_creator_vault_ata: pk(7),
            coin_creator_vault_authority: pk(8),
            coin_creator: pk(9),
            is_cashback_coin: false,
            liquidity: Some(PoolLiquidity {
                base_lamports: 2_000,
                token_lamports: Some(2_000_000),
                updated_at_ms: 1_000,
            }),
            enabled: true,
            last_update_slot: 10,
        }
    }

    fn dlmm(byte: u8) -> DlmmRouteState {
        DlmmRouteState {
            program_id: pk(byte),
            base_mint: sol_mint(),
            event_authority: pk(byte + 1),
            memo_program: Some(pk(byte + 2)),
            lb_pair: pk(byte + 3),
            token_vault: pk(byte + 4),
            base_vault: pk(byte + 5),
            oracle: pk(byte + 6),
            bin_array_bitmap_extension: Some(pk(byte + 7)),
            bin_arrays: vec![
                AccountMeta::new(pk(byte + 8), false),
                AccountMeta::new(pk(byte + 9), false),
            ],
            active_id: 0,
            liquidity: Some(PoolLiquidity {
                base_lamports: 2_000,
                token_lamports: None,
                updated_at_ms: 1_000,
            }),
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
            liquidity: Some(PoolLiquidity {
                base_lamports: 2_000,
                token_lamports: None,
                updated_at_ms: 1_000,
            }),
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
            liquidity: Some(PoolLiquidity {
                base_lamports: 2_000,
                token_lamports: None,
                updated_at_ms: 1_000,
            }),
            enabled: true,
            last_update_slot: 10,
        }
    }

    #[test]
    fn route_group_conversion_preserves_pump_and_multiple_dlmms() {
        let mint = pk(1);
        let route = RouteGroup {
            mint,
            pump: Some(pump()),
            dlmms: vec![dlmm(20), dlmm(40)],
            raydium_cps: Vec::new(),
            damm_v2s: Vec::new(),
        };

        let pool_data = route_group_to_mint_pool_data(&route, &pk(99), spl_token::ID);

        assert_eq!(pool_data.mint, mint);
        assert_eq!(pool_data.token_program, spl_token::ID);
        assert_eq!(pool_data.pump_pools.len(), 1);
        assert_eq!(pool_data.pump_pools[0].pool, pk(2));
        assert_eq!(pool_data.dlmm_pairs.len(), 2);
        assert_eq!(pool_data.dlmm_pairs[0].pair, pk(23));
        assert_eq!(pool_data.dlmm_pairs[0].bin_arrays, vec![pk(28), pk(29)]);
        assert_eq!(pool_data.dlmm_pairs[1].pair, pk(43));
        assert!(pool_data.raydium_cp_pools.is_empty());
        assert!(pool_data.meteora_damm_v2_pools.is_empty());
    }

    #[test]
    fn route_group_conversion_copies_raydium_cp_and_damm_v2_pools() {
        let mint = pk(1);
        let route = RouteGroup {
            mint,
            pump: Some(pump()),
            dlmms: vec![dlmm(20)],
            raydium_cps: vec![cpmm(50), cpmm(60)],
            damm_v2s: vec![damm_v2(70)],
        };

        let pool_data = route_group_to_mint_pool_data(&route, &pk(99), spl_token::ID);

        // Raydium CPMM: pool, token_vault, sol_vault, amm_config, observation.
        assert_eq!(pool_data.raydium_cp_pools.len(), 2);
        assert_eq!(pool_data.raydium_cp_pools[0].pool, pk(52));
        assert_eq!(pool_data.raydium_cp_pools[0].token_vault, pk(54));
        assert_eq!(pool_data.raydium_cp_pools[0].sol_vault, pk(55));
        assert_eq!(pool_data.raydium_cp_pools[0].amm_config, pk(53));
        assert_eq!(pool_data.raydium_cp_pools[0].observation, pk(56));
        assert_eq!(pool_data.raydium_cp_pools[0].token_mint, mint);
        assert_eq!(pool_data.raydium_cp_pools[0].base_mint, sol_mint());
        assert_eq!(pool_data.raydium_cp_pools[1].pool, pk(62));

        // Meteora DAMM v2: pool, token_x_vault, token_sol_vault.
        assert_eq!(pool_data.meteora_damm_v2_pools.len(), 1);
        assert_eq!(pool_data.meteora_damm_v2_pools[0].pool, pk(73));
        assert_eq!(pool_data.meteora_damm_v2_pools[0].token_x_vault, pk(74));
        assert_eq!(pool_data.meteora_damm_v2_pools[0].token_sol_vault, pk(75));
        assert_eq!(pool_data.meteora_damm_v2_pools[0].token_mint, mint);
        assert_eq!(pool_data.meteora_damm_v2_pools[0].base_mint, sol_mint());
    }

    #[test]
    fn route_group_conversion_without_pump_produces_no_pump_pool() {
        // Composition CPMM + DLMM (no pump) — verify the MintPoolData carries
        // zero pump_pools while the CPMM and DLMM ones are populated. This
        // guards against a regression where `route.pump.X` panics or the
        // builder silently drops the composition.
        let mint = pk(1);
        let route = RouteGroup::<PumpRouteState, DlmmRouteState> {
            mint,
            pump: None,
            dlmms: vec![dlmm(20)],
            raydium_cps: vec![cpmm(50)],
            damm_v2s: Vec::new(),
        };

        let pool_data = route_group_to_mint_pool_data(&route, &pk(99), spl_token::ID);

        assert!(pool_data.pump_pools.is_empty());
        assert_eq!(pool_data.dlmm_pairs.len(), 1);
        assert_eq!(pool_data.raydium_cp_pools.len(), 1);
    }

    #[test]
    fn controlled_transaction_builds_without_pump_leg() {
        // End-to-end: build_controlled_transaction succeeds for a pump-absent
        // composition. Confirms the whole pipeline (route → MintPoolData →
        // create_swap_instruction → v0 message) tolerates `pump=None`.
        let wallet = Keypair::new();
        let mint = pk(1);
        let route = RouteGroup::<PumpRouteState, DlmmRouteState> {
            mint,
            pump: None,
            dlmms: vec![dlmm(20)],
            raydium_cps: vec![cpmm(50)],
            damm_v2s: Vec::new(),
        };
        let params = ControlledExecutionParams {
            compute_unit_limit: 450_000,
            compute_unit_price: 0,
            minimum_profit_lamports: 0,
            use_flashloan: false,
            no_failure_mode: true,
            sender_tip: None,
        };

        let tx = build_controlled_transaction(
            &wallet,
            &route,
            spl_token::ID,
            Hash::new_unique(),
            &[],
            params,
        )
        .unwrap();

        // v0 message with at least one instruction (compute budget + swap).
        let VersionedMessage::V0(message) = &tx.message else {
            panic!("expected v0 message");
        };
        assert!(!message.instructions.is_empty());
    }

    #[test]
    fn controlled_transaction_includes_sender_tip_when_configured() {
        let wallet = Keypair::new();
        let tip_account = pk(90);
        let mint = pk(1);
        let route = RouteGroup {
            mint,
            pump: Some(pump()),
            dlmms: vec![dlmm(20)],
            raydium_cps: Vec::new(),
            damm_v2s: Vec::new(),
        };
        let params = ControlledExecutionParams {
            compute_unit_limit: 450_000,
            compute_unit_price: 1_000,
            minimum_profit_lamports: 0,
            use_flashloan: false,
            no_failure_mode: true,
            sender_tip: Some(SenderTipConfig {
                min_lamports: 1_000_000,
                max_lamports: 1_000_000,
                accounts: vec![tip_account],
            }),
        };

        let tx = build_controlled_transaction(
            &wallet,
            &route,
            spl_token::ID,
            Hash::new_unique(),
            &[],
            params,
        )
        .unwrap();

        let VersionedMessage::V0(message) = &tx.message else {
            panic!("expected v0 message");
        };
        let system_program_index = message
            .account_keys
            .iter()
            .position(|key| *key == solana_sdk::system_program::ID)
            .unwrap();
        let tip_ix = message
            .instructions
            .iter()
            .find(|ix| ix.program_id_index as usize == system_program_index)
            .unwrap();

        assert!(tip_ix.accounts.len() >= 2);
        assert_eq!(
            message.account_keys[tip_ix.accounts[1] as usize],
            tip_account
        );
    }
}
