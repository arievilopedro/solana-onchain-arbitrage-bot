//! Controlled pool discovery for allowlisted mints.
//!
//! Bootstrap may use RPC. Runtime updates should come from Geyser/account streams.

use crate::constants::sol_mint;
use crate::dex::meteora::constants::{dlmm_event_authority, dlmm_program_id, memo_program_v2};
use crate::dex::meteora::dlmm_info::DlmmInfo;
use crate::dex::pump::amm_info::{
    PumpAmmInfo, PUMP_BASE_MINT_GPA_OFFSET, PUMP_QUOTE_MINT_GPA_OFFSET,
};
use crate::dex::pump::{pump_fee_wallet, pump_mayhem_fee_wallet, pump_program_id};
use crate::registry::{
    DlmmRouteState, MintRuntimeState, PoolLiquidity, PumpRouteState, RuntimeRegistry,
};
use anyhow::Context;
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_program::instruction::AccountMeta;
use solana_program::program_pack::Pack;
use solana_program::pubkey::Pubkey;
use solana_sdk::account::Account;
use spl_token::state::Account as TokenAccount;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RpcBootstrapConfig {
    pub min_pool_base_liquidity_lamports: u64,
}

#[derive(Debug)]
pub struct RpcBootstrapReport {
    pub registry: RuntimeRegistry,
    pub discovered_pump: usize,
    pub discovered_dlmm: usize,
    pub skipped_low_liquidity: usize,
}

pub struct ControlledRpcBootstrap {
    rpc: Arc<RpcClient>,
    config: RpcBootstrapConfig,
}

impl ControlledRpcBootstrap {
    pub fn new(rpc: Arc<RpcClient>, config: RpcBootstrapConfig) -> Self {
        Self { rpc, config }
    }

    pub fn bootstrap(&self, allowed_mints: &[Pubkey]) -> anyhow::Result<RpcBootstrapReport> {
        let mut registry = RuntimeRegistry::new(allowed_mints.iter().copied())?;
        let mut discovered_pump = 0usize;
        let mut discovered_dlmm = 0usize;
        let mut skipped_low_liquidity = 0usize;

        for mint in allowed_mints {
            let token_program = self.rpc.get_account(mint)?.owner;
            let mut state = MintRuntimeState::new(*mint, token_program);

            let pump_accounts = self.discover_pump_pool_accounts(*mint)?;
            for (pool, account) in pump_accounts {
                let Some(route_state) =
                    pump_route_state_from_account(*mint, pool, &account, |base_vault| {
                        self.base_vault_liquidity(base_vault).with_context(|| {
                            format!("failed to read Pump base vault liquidity for pool {}", pool)
                        })
                    })?
                else {
                    continue;
                };

                if route_state
                    .liquidity
                    .map(|l| l.base_lamports >= self.config.min_pool_base_liquidity_lamports)
                    .unwrap_or(false)
                {
                    discovered_pump += 1;
                    state.pump.push(route_state);
                } else {
                    skipped_low_liquidity += 1;
                }
            }

            let dlmm_accounts = self.discover_dlmm_accounts(*mint)?;
            for (pair, account) in dlmm_accounts {
                let Some(route_state) = dlmm_route_state_from_account(
                    *mint,
                    pair,
                    &account,
                    |base_vault| self.base_vault_liquidity(base_vault),
                    |pair| self.dlmm_bitmap_extension(pair),
                    |mint| mint_uses_token_2022(&self.rpc, mint),
                )?
                else {
                    continue;
                };

                if route_state
                    .liquidity
                    .map(|l| l.base_lamports >= self.config.min_pool_base_liquidity_lamports)
                    .unwrap_or(false)
                {
                    discovered_dlmm += 1;
                    state.dlmms.push(route_state);
                } else {
                    skipped_low_liquidity += 1;
                }
            }

            registry.upsert_mint(state)?;
        }

        Ok(RpcBootstrapReport {
            registry,
            discovered_pump,
            discovered_dlmm,
            skipped_low_liquidity,
        })
    }

    fn discover_pump_pool_accounts(&self, mint: Pubkey) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        let mut out = HashMap::new();
        for offset in [PUMP_BASE_MINT_GPA_OFFSET, PUMP_QUOTE_MINT_GPA_OFFSET] {
            for (pubkey, account) in
                self.get_program_accounts_by_mint(pump_program_id(), offset, mint)?
            {
                out.insert(pubkey, account);
            }
        }

        Ok(out.into_iter().collect())
    }

    fn discover_dlmm_accounts(&self, mint: Pubkey) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        let mut out = HashMap::new();
        for offset in [
            DlmmInfo::token_x_mint_gpa_offset(),
            DlmmInfo::token_y_mint_gpa_offset(),
        ] {
            for (pubkey, account) in
                self.get_program_accounts_by_mint(dlmm_program_id(), offset, mint)?
            {
                out.insert(pubkey, account);
            }
        }

        Ok(out.into_iter().collect())
    }

    fn get_program_accounts_by_mint(
        &self,
        program_id: Pubkey,
        mint_offset: usize,
        mint: Pubkey,
    ) -> anyhow::Result<Vec<(Pubkey, Account)>> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_base58_encoded(
                mint_offset,
                mint.as_ref(),
            ))]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(self
            .rpc
            .get_program_accounts_with_config(&program_id, config)?)
    }

    fn base_vault_liquidity(&self, base_vault: Pubkey) -> anyhow::Result<Option<PoolLiquidity>> {
        let account = self.rpc.get_account(&base_vault)?;
        let token_account = TokenAccount::unpack(&account.data)?;
        Ok(Some(PoolLiquidity {
            base_lamports: token_account.amount,
            updated_at_ms: now_ms(),
        }))
    }

    fn dlmm_bitmap_extension(&self, pair: Pubkey) -> anyhow::Result<Option<Pubkey>> {
        let (bitmap_extension, _) =
            Pubkey::find_program_address(&[b"bitmap", pair.as_ref()], &dlmm_program_id());
        match self.rpc.get_account(&bitmap_extension) {
            Ok(account) if account.owner == dlmm_program_id() => Ok(Some(bitmap_extension)),
            _ => Ok(None),
        }
    }
}

pub fn pump_route_state_from_account(
    mint: Pubkey,
    pool: Pubkey,
    account: &Account,
    liquidity: impl FnOnce(Pubkey) -> anyhow::Result<Option<PoolLiquidity>>,
) -> anyhow::Result<Option<PumpRouteState>> {
    let info = PumpAmmInfo::load_checked(&account.data)?;
    if !pool_contains_mint_and_sol(info.base_mint, info.quote_mint, mint) {
        return Ok(None);
    }

    let (token_vault, base_vault, base_mint) = if info.base_mint == mint {
        (
            info.pool_base_token_account,
            info.pool_quote_token_account,
            info.quote_mint,
        )
    } else {
        (
            info.pool_quote_token_account,
            info.pool_base_token_account,
            info.base_mint,
        )
    };

    if base_mint != sol_mint() {
        return Ok(None);
    }

    let fee_wallet = if info.is_mayhem_mode {
        pump_mayhem_fee_wallet()
    } else {
        pump_fee_wallet()
    };
    let fee_token_wallet =
        spl_associated_token_account::get_associated_token_address(&fee_wallet, &base_mint);
    let coin_creator_vault_ata = spl_associated_token_account::get_associated_token_address(
        &info.coin_creator_vault_authority,
        &base_mint,
    );

    Ok(Some(PumpRouteState {
        pool,
        base_mint,
        token_vault,
        base_vault,
        fee_wallet,
        fee_token_wallet,
        coin_creator_vault_ata,
        coin_creator_vault_authority: info.coin_creator_vault_authority,
        coin_creator: info.coin_creator,
        is_cashback_coin: info.is_cashback_coin,
        liquidity: liquidity(base_vault)?,
        enabled: true,
        last_update_slot: 0,
    }))
}

pub fn dlmm_route_state_from_account(
    mint: Pubkey,
    pair: Pubkey,
    account: &Account,
    liquidity: impl FnOnce(Pubkey) -> anyhow::Result<Option<PoolLiquidity>>,
    bitmap_extension: impl FnOnce(Pubkey) -> anyhow::Result<Option<Pubkey>>,
    mint_uses_token_2022: impl FnOnce(Pubkey) -> anyhow::Result<bool>,
) -> anyhow::Result<Option<DlmmRouteState>> {
    let info = DlmmInfo::load_checked(&account.data)?;
    if !pool_contains_mint_and_sol(info.token_x_mint, info.token_y_mint, mint) {
        return Ok(None);
    }

    let (token_vault, base_vault, base_mint) = if info.token_x_mint == mint {
        (info.token_x_vault, info.token_y_vault, info.token_y_mint)
    } else {
        (info.token_y_vault, info.token_x_vault, info.token_x_mint)
    };

    if base_mint != sol_mint() {
        return Ok(None);
    }

    let bin_arrays = info
        .calculate_bin_arrays(&pair)?
        .into_iter()
        .map(|pubkey| AccountMeta::new(pubkey, false))
        .collect::<Vec<_>>();

    Ok(Some(DlmmRouteState {
        program_id: dlmm_program_id(),
        base_mint,
        event_authority: dlmm_event_authority(),
        memo_program: dlmm_memo_program_for_mint(mint, mint_uses_token_2022)?,
        lb_pair: pair,
        token_vault,
        base_vault,
        oracle: info.oracle,
        bin_array_bitmap_extension: bitmap_extension(pair)?,
        bin_arrays,
        active_id: info.active_id,
        liquidity: liquidity(base_vault)?,
        enabled: true,
        last_update_slot: 0,
    }))
}

fn pool_contains_mint_and_sol(a: Pubkey, b: Pubkey, mint: Pubkey) -> bool {
    let sol = sol_mint();
    (a == mint && b == sol) || (b == mint && a == sol)
}

fn mint_uses_token_2022(rpc: &RpcClient, mint: Pubkey) -> anyhow::Result<bool> {
    Ok(rpc.get_account(&mint)?.owner == token_2022_program_id())
}

fn dlmm_memo_program_for_mint(
    mint: Pubkey,
    mint_uses_token_2022: impl FnOnce(Pubkey) -> anyhow::Result<bool>,
) -> anyhow::Result<Option<Pubkey>> {
    Ok(mint_uses_token_2022(mint)?.then_some(memo_program_v2()))
}

fn token_2022_program_id() -> Pubkey {
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
        .parse()
        .unwrap()
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::usdc_mint;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    #[test]
    fn pool_contains_mint_and_sol_requires_exact_allowed_mint_and_sol() {
        let mint = pk(1);
        assert!(pool_contains_mint_and_sol(mint, sol_mint(), mint));
        assert!(pool_contains_mint_and_sol(sol_mint(), mint, mint));
        assert!(!pool_contains_mint_and_sol(usdc_mint(), mint, mint));
        assert!(!pool_contains_mint_and_sol(sol_mint(), pk(2), mint));
    }

    #[test]
    fn pump_gpa_offsets_match_parser_layout() {
        assert_eq!(PUMP_BASE_MINT_GPA_OFFSET, 43);
        assert_eq!(PUMP_QUOTE_MINT_GPA_OFFSET, 75);
    }

    #[test]
    fn dlmm_gpa_offsets_are_ordered() {
        assert!(DlmmInfo::token_x_mint_gpa_offset() > 8);
        assert_eq!(
            DlmmInfo::token_y_mint_gpa_offset(),
            DlmmInfo::token_x_mint_gpa_offset() + 32
        );
    }

    #[test]
    fn dlmm_token_2022_uses_memo_program_v2_not_token_program() {
        let mint = pk(10);

        let memo = dlmm_memo_program_for_mint(mint, |_| Ok(true)).unwrap();
        let no_memo = dlmm_memo_program_for_mint(mint, |_| Ok(false)).unwrap();

        assert_eq!(memo, Some(memo_program_v2()));
        assert_ne!(memo, Some(token_2022_program_id()));
        assert_eq!(no_memo, None);
    }
}
