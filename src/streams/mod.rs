//! Stream clients for RPC bootstrap, Geyser gRPC updates, and RabbitStream shreds.

use crate::dex::meteora::constants::dlmm_program_id;
use crate::dex::pump::pump_program_id;
use crate::discovery::{dlmm_route_state_from_account, pump_route_state_from_account};
use crate::registry::{PoolKind, PoolLiquidity, RuntimeRegistry};
use solana_program::pubkey::Pubkey;
use solana_sdk::account::Account;
use std::collections::VecDeque;

pub mod grpc;
pub mod rabbitstream;
pub mod rpc;

#[derive(Debug, Clone)]
pub struct PoolAccountUpdate {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub account: Account,
    pub slot: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SlotUpdate {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: i32,
}

#[derive(Debug, Clone)]
pub struct SlotTracker {
    recent_slots: VecDeque<u64>,
    max_slots: usize,
}

impl Default for SlotTracker {
    fn default() -> Self {
        Self::new(150)
    }
}

impl SlotTracker {
    pub fn new(max_slots: usize) -> Self {
        Self {
            recent_slots: VecDeque::new(),
            max_slots: max_slots.max(1),
        }
    }

    pub fn record_processed_slot(&mut self, slot: u64) {
        if self.recent_slots.back().copied() == Some(slot) {
            return;
        }
        self.recent_slots.push_back(slot);
        while self.recent_slots.len() > self.max_slots {
            self.recent_slots.pop_front();
        }
    }

    pub fn recent_slot_candidates(&self) -> Vec<u64> {
        self.recent_slots.iter().rev().copied().collect()
    }

    pub fn latest(&self) -> Option<u64> {
        self.recent_slots.back().copied()
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct RegistryUpdateReport {
    pub applied: bool,
    pub applied_kind: Option<PoolKind>,
    pub applied_mint: Option<Pubkey>,
    pub applied_pool: Option<Pubkey>,
    pub applied_base_liquidity_lamports: Option<u64>,
    pub ignored_not_pool_program: bool,
    pub ignored_not_pool_account: bool,
    pub ignored_not_allowlisted: bool,
    pub ignored_missing_mint_state: bool,
    pub ignored_non_sol_route: bool,
}

pub fn apply_pool_account_update(
    registry: &mut RuntimeRegistry,
    update: PoolAccountUpdate,
    pump_liquidity: impl Fn(Pubkey, Pubkey) -> anyhow::Result<Option<PoolLiquidity>>,
    base_liquidity: impl Fn(Pubkey) -> anyhow::Result<Option<PoolLiquidity>>,
    mint_uses_token_2022: impl Fn(Pubkey) -> anyhow::Result<bool>,
    dlmm_bitmap_extension: impl Fn(Pubkey) -> anyhow::Result<Option<Pubkey>>,
) -> anyhow::Result<RegistryUpdateReport> {
    if update.owner == pump_program_id() {
        return apply_pump_update(registry, update, pump_liquidity);
    }

    if update.owner == dlmm_program_id() {
        return apply_dlmm_update(
            registry,
            update,
            base_liquidity,
            mint_uses_token_2022,
            dlmm_bitmap_extension,
        );
    }

    Ok(RegistryUpdateReport {
        ignored_not_pool_program: true,
        ..RegistryUpdateReport::default()
    })
}

fn apply_pump_update(
    registry: &mut RuntimeRegistry,
    update: PoolAccountUpdate,
    liquidity: impl Fn(Pubkey, Pubkey) -> anyhow::Result<Option<PoolLiquidity>>,
) -> anyhow::Result<RegistryUpdateReport> {
    for mint in registry.allowed_mints().collect::<Vec<_>>() {
        let route = match pump_route_state_from_account(
            mint,
            update.pubkey,
            &update.account,
            |token_vault, base_vault| liquidity(token_vault, base_vault),
        ) {
            Ok(Some(route)) => route,
            Ok(None) => continue,
            Err(_) => {
                return Ok(RegistryUpdateReport {
                    ignored_not_pool_account: true,
                    ..RegistryUpdateReport::default()
                });
            }
        };
        let mut route = route;

        route.last_update_slot = update.slot;
        let applied_pool = route.pool;
        let applied_base_liquidity_lamports =
            route.liquidity.map(|liquidity| liquidity.base_lamports);
        let Some(state) = registry.get_mut(&mint) else {
            return Ok(RegistryUpdateReport {
                ignored_missing_mint_state: true,
                ..RegistryUpdateReport::default()
            });
        };
        upsert_pump(&mut state.pump, route);
        state.updated_slot = update.slot;
        return Ok(RegistryUpdateReport {
            applied: true,
            applied_kind: Some(PoolKind::Pump),
            applied_mint: Some(mint),
            applied_pool: Some(applied_pool),
            applied_base_liquidity_lamports,
            ..RegistryUpdateReport::default()
        });
    }

    Ok(RegistryUpdateReport {
        ignored_not_allowlisted: true,
        ..RegistryUpdateReport::default()
    })
}

fn apply_dlmm_update(
    registry: &mut RuntimeRegistry,
    update: PoolAccountUpdate,
    liquidity: impl Fn(Pubkey) -> anyhow::Result<Option<PoolLiquidity>>,
    mint_uses_token_2022: impl Fn(Pubkey) -> anyhow::Result<bool>,
    dlmm_bitmap_extension: impl Fn(Pubkey) -> anyhow::Result<Option<Pubkey>>,
) -> anyhow::Result<RegistryUpdateReport> {
    for mint in registry.allowed_mints().collect::<Vec<_>>() {
        let route = match dlmm_route_state_from_account(
            mint,
            update.pubkey,
            &update.account,
            |base_vault| liquidity(base_vault),
            |pair| dlmm_bitmap_extension(pair),
            |mint| mint_uses_token_2022(mint),
        ) {
            Ok(Some(route)) => route,
            Ok(None) => continue,
            Err(_) => {
                return Ok(RegistryUpdateReport {
                    ignored_not_pool_account: true,
                    ..RegistryUpdateReport::default()
                });
            }
        };
        let mut route = route;

        route.last_update_slot = update.slot;
        let applied_pool = route.lb_pair;
        let applied_base_liquidity_lamports =
            route.liquidity.map(|liquidity| liquidity.base_lamports);
        let Some(state) = registry.get_mut(&mint) else {
            return Ok(RegistryUpdateReport {
                ignored_missing_mint_state: true,
                ..RegistryUpdateReport::default()
            });
        };
        upsert_dlmm(&mut state.dlmms, route);
        state.updated_slot = update.slot;
        return Ok(RegistryUpdateReport {
            applied: true,
            applied_kind: Some(PoolKind::MeteoraDlmm),
            applied_mint: Some(mint),
            applied_pool: Some(applied_pool),
            applied_base_liquidity_lamports,
            ..RegistryUpdateReport::default()
        });
    }

    Ok(RegistryUpdateReport {
        ignored_not_allowlisted: true,
        ..RegistryUpdateReport::default()
    })
}

fn upsert_pump(
    pools: &mut Vec<crate::registry::PumpRouteState>,
    route: crate::registry::PumpRouteState,
) {
    if let Some(existing) = pools.iter_mut().find(|pool| pool.pool == route.pool) {
        *existing = route;
    } else {
        pools.push(route);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::sol_mint;
    use crate::dex::pump::amm_info::{PUMP_BASE_MINT_GPA_OFFSET, PUMP_QUOTE_MINT_GPA_OFFSET};
    use crate::registry::MintRuntimeState;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn account(owner: Pubkey, data: Vec<u8>) -> Account {
        Account {
            lamports: 0,
            data,
            owner,
            executable: false,
            rent_epoch: 0,
        }
    }

    fn pump_account_data(mint: Pubkey, token_vault: Pubkey, base_vault: Pubkey) -> Vec<u8> {
        let pool_base_token_offset = PUMP_QUOTE_MINT_GPA_OFFSET + 32 + 32;
        let pool_quote_token_offset = pool_base_token_offset + 32;
        let mut data = vec![0u8; pool_quote_token_offset + 32];

        data[PUMP_BASE_MINT_GPA_OFFSET..PUMP_BASE_MINT_GPA_OFFSET + 32]
            .copy_from_slice(mint.as_ref());
        data[PUMP_QUOTE_MINT_GPA_OFFSET..PUMP_QUOTE_MINT_GPA_OFFSET + 32]
            .copy_from_slice(sol_mint().as_ref());
        data[pool_base_token_offset..pool_base_token_offset + 32]
            .copy_from_slice(token_vault.as_ref());
        data[pool_quote_token_offset..pool_quote_token_offset + 32]
            .copy_from_slice(base_vault.as_ref());

        data
    }

    fn fixed_pump_liquidity(
        _token_vault: Pubkey,
        _base_vault: Pubkey,
    ) -> anyhow::Result<Option<PoolLiquidity>> {
        Ok(Some(PoolLiquidity {
            base_lamports: 1_000_000,
            token_lamports: Some(2_000_000),
            updated_at_ms: 10,
        }))
    }

    fn fixed_base_liquidity(_base_vault: Pubkey) -> anyhow::Result<Option<PoolLiquidity>> {
        Ok(Some(PoolLiquidity {
            base_lamports: 1_000_000,
            token_lamports: None,
            updated_at_ms: 10,
        }))
    }

    fn no_token_2022(_mint: Pubkey) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn no_bitmap(_pair: Pubkey) -> anyhow::Result<Option<Pubkey>> {
        Ok(None)
    }

    #[test]
    fn ignores_updates_from_untracked_programs() {
        let mint = pk(1);
        let mut registry = RuntimeRegistry::new([mint]).unwrap();
        registry
            .upsert_mint(MintRuntimeState::new(mint, spl_token::ID))
            .unwrap();

        let report = apply_pool_account_update(
            &mut registry,
            PoolAccountUpdate {
                pubkey: pk(2),
                owner: pk(3),
                account: account(pk(3), Vec::new()),
                slot: 7,
            },
            fixed_pump_liquidity,
            fixed_base_liquidity,
            no_token_2022,
            no_bitmap,
        )
        .unwrap();

        assert_eq!(
            report,
            RegistryUpdateReport {
                ignored_not_pool_program: true,
                ..RegistryUpdateReport::default()
            }
        );
    }

    #[test]
    fn slot_tracker_keeps_recent_slots_newest_first_for_candidates() {
        let mut tracker = SlotTracker::new(3);
        tracker.record_processed_slot(10);
        tracker.record_processed_slot(11);
        tracker.record_processed_slot(11);
        tracker.record_processed_slot(12);
        tracker.record_processed_slot(13);

        assert_eq!(tracker.latest(), Some(13));
        assert_eq!(tracker.recent_slot_candidates(), vec![13, 12, 11]);
    }

    #[test]
    fn applies_allowlisted_pump_sol_update() {
        let mint = pk(10);
        let pool = pk(11);
        let token_vault = pk(12);
        let base_vault = pk(13);
        let mut registry = RuntimeRegistry::new([mint]).unwrap();
        registry
            .upsert_mint(MintRuntimeState::new(mint, spl_token::ID))
            .unwrap();

        let report = apply_pool_account_update(
            &mut registry,
            PoolAccountUpdate {
                pubkey: pool,
                owner: pump_program_id(),
                account: account(
                    pump_program_id(),
                    pump_account_data(mint, token_vault, base_vault),
                ),
                slot: 42,
            },
            fixed_pump_liquidity,
            fixed_base_liquidity,
            no_token_2022,
            no_bitmap,
        )
        .unwrap();

        assert_eq!(
            report,
            RegistryUpdateReport {
                applied: true,
                applied_kind: Some(PoolKind::Pump),
                applied_mint: Some(mint),
                applied_pool: Some(pool),
                applied_base_liquidity_lamports: Some(1_000_000),
                ..RegistryUpdateReport::default()
            }
        );

        let state = registry.get(&mint).unwrap();
        assert_eq!(state.updated_slot, 42);
        assert_eq!(state.pump.len(), 1);
        assert_eq!(state.pump[0].pool, pool);
        assert_eq!(state.pump[0].token_vault, token_vault);
        assert_eq!(state.pump[0].base_vault, base_vault);
        assert_eq!(state.pump[0].last_update_slot, 42);
    }

    #[test]
    fn reports_missing_mint_state_for_matching_allowlisted_update() {
        let mint = pk(20);
        let mut registry = RuntimeRegistry::new([mint]).unwrap();

        let report = apply_pool_account_update(
            &mut registry,
            PoolAccountUpdate {
                pubkey: pk(21),
                owner: pump_program_id(),
                account: account(pump_program_id(), pump_account_data(mint, pk(22), pk(23))),
                slot: 99,
            },
            fixed_pump_liquidity,
            fixed_base_liquidity,
            no_token_2022,
            no_bitmap,
        )
        .unwrap();

        assert_eq!(
            report,
            RegistryUpdateReport {
                ignored_missing_mint_state: true,
                ..RegistryUpdateReport::default()
            }
        );
    }

    #[test]
    fn ignores_program_owned_accounts_that_do_not_match_pool_layout() {
        let mint = pk(30);
        let mut registry = RuntimeRegistry::new([mint]).unwrap();
        registry
            .upsert_mint(MintRuntimeState::new(mint, spl_token::ID))
            .unwrap();

        let report = apply_pool_account_update(
            &mut registry,
            PoolAccountUpdate {
                pubkey: pk(31),
                owner: pump_program_id(),
                account: account(pump_program_id(), vec![0u8; 8]),
                slot: 100,
            },
            fixed_pump_liquidity,
            fixed_base_liquidity,
            no_token_2022,
            no_bitmap,
        )
        .unwrap();

        assert_eq!(
            report,
            RegistryUpdateReport {
                ignored_not_pool_account: true,
                ..RegistryUpdateReport::default()
            }
        );
    }
}

fn upsert_dlmm(
    pools: &mut Vec<crate::registry::DlmmRouteState>,
    route: crate::registry::DlmmRouteState,
) {
    if let Some(existing) = pools.iter_mut().find(|pool| pool.lb_pair == route.lb_pair) {
        *existing = route;
    } else {
        pools.push(route);
    }
}
