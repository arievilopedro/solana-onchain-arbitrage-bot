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
    let mint_pool_data = route_group_to_mint_pool_data(route, &wallet.pubkey(), token_program);
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(params.compute_unit_limit),
        ComputeBudgetInstruction::set_compute_unit_price(params.compute_unit_price),
    ];
    if let Some(tip) = &params.sender_tip {
        if tip.lamports > 0 {
            if let Some(tip_account) = tip.random_account() {
                instructions.push(system_instruction::transfer(
                    &wallet.pubkey(),
                    &tip_account,
                    tip.lamports,
                ));
            }
        }
    }
    instructions.push(create_swap_instruction(
        wallet,
        &mint_pool_data,
        params.compute_unit_limit,
        params.use_flashloan,
        params.no_failure_mode,
    )?);

    let message = Message::try_compile(
        &wallet.pubkey(),
        &instructions,
        lookup_tables,
        recent_blockhash,
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
    pool_data.add_pump_pool(
        route.pump.pool,
        route.pump.token_vault,
        route.pump.base_vault,
        route.pump.fee_wallet,
        route.pump.fee_token_wallet,
        route.pump.coin_creator_vault_ata,
        route.pump.coin_creator_vault_authority,
        route.pump.coin_creator,
        route.mint,
        route.pump.base_mint,
        false,
        route.pump.is_cashback_coin,
    );

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

    pool_data
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::sol_mint;
    use crate::registry::{PoolLiquidity, PumpRouteState};
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
            pump: pump(),
            dlmms: vec![dlmm(20), dlmm(40)],
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
    }

    #[test]
    fn controlled_transaction_includes_sender_tip_when_configured() {
        let wallet = Keypair::new();
        let tip_account = pk(90);
        let mint = pk(1);
        let route = RouteGroup {
            mint,
            pump: pump(),
            dlmms: vec![dlmm(20)],
        };
        let params = ControlledExecutionParams {
            compute_unit_limit: 450_000,
            compute_unit_price: 1_000,
            use_flashloan: false,
            no_failure_mode: true,
            sender_tip: Some(SenderTipConfig {
                lamports: 1_000_000,
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
