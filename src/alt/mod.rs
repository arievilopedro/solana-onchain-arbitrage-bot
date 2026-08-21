//! Address lookup table and route shard management.
//!
//! V1 operates only on mints listed in `runtime.allowed_mints`. Route shard
//! create/extend is allowed only in background workers, never in the trigger path.

use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSimulateTransactionConfig;
use solana_program::instruction::Instruction;
use solana_program::pubkey::Pubkey;
use solana_sdk::address_lookup_table::instruction::{create_lookup_table, extend_lookup_table};
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use crate::registry::MintRuntimeState;

pub const ROUTE_SHARD_STATE_VERSION: u8 = 1;
pub const ROUTE_SHARD_MAX_ADDRESSES: usize = 256;
const ROUTE_SHARD_EXTEND_ADDRESSES_PER_TX: usize = 19;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteShardStatus {
    Active,
    Full,
    Frozen,
    Deactivated,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteShardRecord {
    pub used: usize,
    pub capacity: usize,
    pub created_slot: u64,
    pub last_extended_slot: u64,
    pub status: RouteShardStatus,
}

impl RouteShardRecord {
    pub fn remaining_capacity(&self) -> usize {
        self.capacity.saturating_sub(self.used)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DlmmAltBlockRecord {
    pub lb_pair: String,
    pub indexes: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MintRouteBlockRecord {
    pub shard: String,
    pub base: RouteBaseBlockRecord,
    pub dlmm: Vec<DlmmAltBlockRecord>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteBaseBlockRecord {
    pub indexes: Vec<u8>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteShardStore {
    pub version: u8,
    pub active_shard: Option<String>,
    pub shards: BTreeMap<String, RouteShardRecord>,
    pub mints: BTreeMap<String, MintRouteBlockRecord>,
}

impl RouteShardStore {
    pub fn empty() -> Self {
        Self {
            version: ROUTE_SHARD_STATE_VERSION,
            active_shard: None,
            shards: BTreeMap::new(),
            mints: BTreeMap::new(),
        }
    }

    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::empty());
        }

        let contents = fs::read_to_string(path)?;
        let store: Self = serde_json::from_str(&contents)?;
        store.validate()?;
        Ok(store)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.validate()?;
        write_json(path, self)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.version != ROUTE_SHARD_STATE_VERSION {
            anyhow::bail!("unsupported route shard state version {}", self.version);
        }

        for (address, shard) in &self.shards {
            parse_pubkey(address)?;
            if shard.capacity == 0 || shard.capacity > ROUTE_SHARD_MAX_ADDRESSES {
                anyhow::bail!("invalid route shard capacity for {}", address);
            }
            if shard.used > shard.capacity {
                anyhow::bail!("route shard {} used exceeds capacity", address);
            }
        }

        if let Some(active_shard) = &self.active_shard {
            if !self.shards.contains_key(active_shard) {
                anyhow::bail!(
                    "active shard {} is missing from shard records",
                    active_shard
                );
            }
        }

        for (mint, block) in &self.mints {
            parse_pubkey(mint)?;
            parse_pubkey(&block.shard)?;
            if !self.shards.contains_key(&block.shard) {
                anyhow::bail!("mint {} references missing shard {}", mint, block.shard);
            }
            for dlmm in &block.dlmm {
                parse_pubkey(&dlmm.lb_pair)?;
                if dlmm.indexes.len() != 3 {
                    anyhow::bail!("mint {} dlmm {} must have 3 indexes", mint, dlmm.lb_pair);
                }
            }
        }

        Ok(())
    }

    pub fn active_shard_record(&self) -> Option<(&str, &RouteShardRecord)> {
        let active = self.active_shard.as_deref()?;
        let record = self.shards.get(active)?;
        Some((active, record))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingRouteShardOperation {
    pub version: u8,
    pub operation: PendingRouteShardOperationKind,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteShardPlanFile {
    pub version: u8,
    pub operations: Vec<PendingRouteShardOperation>,
}

impl RouteShardPlanFile {
    pub fn new(operations: Vec<PendingRouteShardOperation>) -> Self {
        Self {
            version: ROUTE_SHARD_STATE_VERSION,
            operations,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new(Vec::new()));
        }

        let contents = fs::read_to_string(path)?;
        let plan: Self = serde_json::from_str(&contents)?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.validate()?;
        write_json(path, self)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.version != ROUTE_SHARD_STATE_VERSION {
            anyhow::bail!("unsupported route shard plan version {}", self.version);
        }
        for operation in &self.operations {
            operation.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PendingRouteShardOperationKind {
    CreateShard {
        mint: String,
        addresses: Vec<String>,
    },
    ExtendShard {
        shard: String,
        mint: String,
        addresses: Vec<String>,
    },
}

impl PendingRouteShardOperation {
    pub fn save(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.validate()?;
        write_json(path, self)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.version != ROUTE_SHARD_STATE_VERSION {
            anyhow::bail!("unsupported route shard operation version {}", self.version);
        }

        match &self.operation {
            PendingRouteShardOperationKind::CreateShard { mint, addresses } => {
                parse_pubkey(mint)?;
                let addresses = parse_pubkeys(addresses)?;
                validate_mint_block_addresses(parse_pubkey(mint)?, &addresses)?;
            }
            PendingRouteShardOperationKind::ExtendShard {
                shard,
                mint,
                addresses,
            } => {
                parse_pubkey(shard)?;
                parse_pubkey(mint)?;
                let addresses = parse_pubkeys(addresses)?;
                if !addresses.is_empty() && addresses.len() % 3 == 0 {
                    for chunk in addresses.chunks(3) {
                        let _ = stable_dlmm_from_addresses(chunk)?;
                    }
                } else {
                    validate_mint_block_addresses(parse_pubkey(mint)?, &addresses)?;
                }
            }
        }
        Ok(())
    }

    pub fn build_instructions(
        &self,
        authority: Pubkey,
        payer: Pubkey,
        recent_slot: u64,
    ) -> anyhow::Result<RouteShardInstructions> {
        match &self.operation {
            PendingRouteShardOperationKind::CreateShard { mint: _, addresses } => {
                let addresses = parse_pubkeys(addresses)?;
                let (create_ix, shard) = create_lookup_table(authority, payer, recent_slot);
                let extend_ix = extend_lookup_table(shard, authority, Some(payer), addresses);
                Ok(RouteShardInstructions {
                    shard,
                    instructions: vec![create_ix, extend_ix],
                })
            }
            PendingRouteShardOperationKind::ExtendShard {
                shard,
                mint: _,
                addresses,
            } => {
                let shard = parse_pubkey(shard)?;
                let addresses = parse_pubkeys(addresses)?;
                let extend_ix = extend_lookup_table(shard, authority, Some(payer), addresses);
                Ok(RouteShardInstructions {
                    shard,
                    instructions: vec![extend_ix],
                })
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RouteShardInstructions {
    pub shard: Pubkey,
    pub instructions: Vec<Instruction>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RouteShardMaintenanceReport {
    pub attempted: usize,
    pub confirmed: Vec<ConfirmedRouteShardOperation>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfirmedRouteShardOperation {
    pub shard: Pubkey,
    pub signature: Signature,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct RouteShardLookupResolver {
    store: RouteShardStore,
}

impl RouteShardLookupResolver {
    pub fn new(store: RouteShardStore) -> anyhow::Result<Self> {
        store.validate()?;
        Ok(Self { store })
    }

    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::new(RouteShardStore::load(path)?)
    }

    pub fn shard_for_mint(&self, mint: Pubkey) -> anyhow::Result<Option<Pubkey>> {
        self.store
            .mints
            .get(&mint.to_string())
            .map(|block| parse_pubkey(&block.shard))
            .transpose()
    }

    pub fn block_for_mint(&self, mint: Pubkey) -> Option<&MintRouteBlockRecord> {
        self.store.mints.get(&mint.to_string())
    }

    pub fn load_lookup_for_mint(
        &self,
        rpc_client: &RpcClient,
        mint: Pubkey,
    ) -> anyhow::Result<Option<AddressLookupTableAccount>> {
        let Some(shard) = self.shard_for_mint(mint)? else {
            return Ok(None);
        };
        Ok(Some(load_lookup_table_account(rpc_client, shard)?))
    }

    pub fn load_all_confirmed_shards(
        &self,
        rpc_client: &RpcClient,
    ) -> anyhow::Result<Vec<AddressLookupTableAccount>> {
        self.store
            .shards
            .keys()
            .map(|shard| load_lookup_table_account(rpc_client, parse_pubkey(shard)?))
            .collect()
    }
}

pub fn load_lookup_table_account(
    rpc_client: &RpcClient,
    address: Pubkey,
) -> anyhow::Result<AddressLookupTableAccount> {
    let retry_delays_ms = [200, 400, 800, 1200, 1600];
    let mut last_error = None;
    let mut account = None;

    for delay_ms in retry_delays_ms {
        match rpc_client.get_account(&address) {
            Ok(loaded) => {
                account = Some(loaded);
                break;
            }
            Err(error) => {
                last_error = Some(error);
                sleep(Duration::from_millis(delay_ms));
            }
        }
    }

    let account = match account {
        Some(account) => account,
        None => rpc_client
            .get_account(&address)
            .map_err(|error| last_error.unwrap_or(error))?,
    };
    let lookup_table = AddressLookupTable::deserialize(&account.data)
        .map_err(|e| anyhow::anyhow!("failed to deserialize lookup table {}: {}", address, e))?;
    Ok(AddressLookupTableAccount {
        key: address,
        addresses: lookup_table.addresses.into_owned(),
    })
}

pub fn execute_route_shard_plan(
    rpc_client: &RpcClient,
    wallet: &Keypair,
    state_file: impl AsRef<Path>,
    plan_file: impl AsRef<Path>,
    allowed_mints: impl IntoIterator<Item = Pubkey>,
    shard_capacity: usize,
) -> anyhow::Result<RouteShardMaintenanceReport> {
    execute_route_shard_plan_with_recent_slots(
        rpc_client,
        wallet,
        state_file,
        plan_file,
        allowed_mints,
        shard_capacity,
        &[],
    )
}

pub fn execute_route_shard_plan_with_recent_slots(
    rpc_client: &RpcClient,
    wallet: &Keypair,
    state_file: impl AsRef<Path>,
    plan_file: impl AsRef<Path>,
    allowed_mints: impl IntoIterator<Item = Pubkey>,
    shard_capacity: usize,
    recent_slot_candidates: &[u64],
) -> anyhow::Result<RouteShardMaintenanceReport> {
    let plan = RouteShardPlanFile::load(plan_file)?;
    let mut planner = RouteShardPlanner::new(
        RouteShardStore::load(&state_file)?,
        allowed_mints,
        shard_capacity,
    )?;
    let mut report = RouteShardMaintenanceReport {
        attempted: 0,
        confirmed: Vec::new(),
    };

    for operation in plan.operations {
        for operation in split_route_shard_operation(operation)? {
            report.attempted += 1;
            let blockhash = rpc_client.get_latest_blockhash()?;
            let (built, tx) = build_simulated_route_shard_transaction(
                rpc_client,
                &operation,
                wallet,
                blockhash,
                recent_slot_candidates,
            )?;
            let signature = rpc_client.send_and_confirm_transaction(&tx)?;
            let confirmed_slot = rpc_client.get_slot()?;

            planner.apply_confirmed_operation(&operation, built.shard, confirmed_slot)?;
            planner.store().save(&state_file)?;
            report.confirmed.push(ConfirmedRouteShardOperation {
                shard: built.shard,
                signature,
                slot: confirmed_slot,
            });
        }
    }

    Ok(report)
}

fn split_route_shard_operation(
    operation: PendingRouteShardOperation,
) -> anyhow::Result<Vec<PendingRouteShardOperation>> {
    let PendingRouteShardOperationKind::ExtendShard {
        shard,
        mint,
        addresses,
    } = &operation.operation
    else {
        return Ok(vec![operation]);
    };

    if addresses.len() <= ROUTE_SHARD_EXTEND_ADDRESSES_PER_TX {
        return Ok(vec![operation]);
    }

    let mint_pubkey = parse_pubkey(mint)?;
    let parsed_addresses = parse_pubkeys(addresses)?;
    let mut split = Vec::new();
    if parsed_addresses.first() == Some(&mint_pubkey) {
        validate_mint_block_addresses(mint_pubkey, &parsed_addresses)?;
        let first_dlmm_capacity = (ROUTE_SHARD_EXTEND_ADDRESSES_PER_TX - 4) / 3;
        let first_end = 4 + first_dlmm_capacity * 3;
        split.push(PendingRouteShardOperation {
            version: operation.version,
            operation: PendingRouteShardOperationKind::ExtendShard {
                shard: shard.clone(),
                mint: mint.clone(),
                addresses: pubkeys_to_strings(&parsed_addresses[..first_end]),
            },
        });
        for chunk in parsed_addresses[first_end..].chunks(ROUTE_SHARD_EXTEND_ADDRESSES_PER_TX - 1) {
            split.push(PendingRouteShardOperation {
                version: operation.version,
                operation: PendingRouteShardOperationKind::ExtendShard {
                    shard: shard.clone(),
                    mint: mint.clone(),
                    addresses: pubkeys_to_strings(chunk),
                },
            });
        }
    } else {
        if parsed_addresses.len() % 3 != 0 {
            anyhow::bail!("allocated mint extension addresses must be DLMM triples");
        }
        for chunk in parsed_addresses.chunks(ROUTE_SHARD_EXTEND_ADDRESSES_PER_TX - 1) {
            split.push(PendingRouteShardOperation {
                version: operation.version,
                operation: PendingRouteShardOperationKind::ExtendShard {
                    shard: shard.clone(),
                    mint: mint.clone(),
                    addresses: pubkeys_to_strings(chunk),
                },
            });
        }
    }

    Ok(split)
}

fn build_simulated_route_shard_transaction(
    rpc_client: &RpcClient,
    operation: &PendingRouteShardOperation,
    wallet: &Keypair,
    blockhash: solana_sdk::hash::Hash,
    recent_slot_candidates: &[u64],
) -> anyhow::Result<(RouteShardInstructions, Transaction)> {
    let current_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::processed())?;
    let mut candidate_slots = recent_slot_candidates.to_vec();
    candidate_slots.extend((1..=512u64).map(|offset| current_slot.saturating_sub(offset)));
    let mut last_error = None;

    // AddressLookupTable create requires a slot present in the simulating
    // bank's SlotHashes sysvar. Current processed slot can be too new, while
    // finalized/default slots can be too old, so probe stream-observed slots
    // first and then recent parent slots from RPC.
    candidate_slots.sort_unstable();
    candidate_slots.dedup();
    for recent_slot in candidate_slots.into_iter().rev() {
        let built =
            operation.build_instructions(wallet.pubkey(), wallet.pubkey(), recent_slot)?;
        let tx = Transaction::new_signed_with_payer(
            &built.instructions,
            Some(&wallet.pubkey()),
            &[wallet],
            blockhash,
        );

        match ensure_route_shard_transaction_simulates(rpc_client, &tx, built.shard) {
            Ok(()) => return Ok((built, tx)),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no recent route shard slot candidates")))
}

fn ensure_route_shard_transaction_simulates(
    rpc_client: &RpcClient,
    tx: &Transaction,
    shard: Pubkey,
) -> anyhow::Result<()> {
    let simulation = rpc_client.simulate_transaction_with_config(
        tx,
        RpcSimulateTransactionConfig {
            sig_verify: true,
            replace_recent_blockhash: false,
            ..RpcSimulateTransactionConfig::default()
        },
    )?;

    if let Some(err) = simulation.value.err {
        let logs = simulation
            .value
            .logs
            .unwrap_or_default()
            .join(" | ");
        anyhow::bail!(
            "route shard transaction simulation failed: shard={} err={:?} logs={}",
            shard,
            err,
            logs
        );
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StableMintRouteAccounts {
    pub mint: Pubkey,
    pub pump_pool: Pubkey,
    pub pump_token_vault: Pubkey,
    pub pump_base_vault: Pubkey,
    pub dlmms: Vec<StableDlmmRouteAccounts>,
}

impl StableMintRouteAccounts {
    pub fn from_mint_runtime_state(
        state: &MintRuntimeState,
        min_base_liquidity_lamports: u64,
        max_state_age_ms: u64,
        now_ms: u128,
    ) -> Option<Self> {
        let pump = state
            .eligible_pumps(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .next()?;
        let dlmms = state
            .eligible_dlmms(min_base_liquidity_lamports, max_state_age_ms, now_ms)
            .into_iter()
            .map(|dlmm| StableDlmmRouteAccounts {
                lb_pair: dlmm.lb_pair,
                token_vault: dlmm.token_vault,
                base_vault: dlmm.base_vault,
            })
            .collect::<Vec<_>>();

        if dlmms.is_empty() {
            return None;
        }

        Some(Self {
            mint: state.mint,
            pump_pool: pump.pool,
            pump_token_vault: pump.token_vault,
            pump_base_vault: pump.base_vault,
            dlmms,
        })
    }

    pub fn stable_addresses(&self) -> Vec<Pubkey> {
        let mut out = vec![
            self.mint,
            self.pump_pool,
            self.pump_token_vault,
            self.pump_base_vault,
        ];
        for dlmm in &self.dlmms {
            out.extend(dlmm.stable_addresses());
        }
        out
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StableDlmmRouteAccounts {
    pub lb_pair: Pubkey,
    pub token_vault: Pubkey,
    pub base_vault: Pubkey,
}

impl StableDlmmRouteAccounts {
    pub fn stable_addresses(&self) -> [Pubkey; 3] {
        [self.lb_pair, self.token_vault, self.base_vault]
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PlannedRouteShardExtension {
    CreateShard {
        mint: Pubkey,
        addresses: Vec<Pubkey>,
    },
    ExtendShard {
        shard: Pubkey,
        mint: Pubkey,
        addresses: Vec<Pubkey>,
        start_index: u8,
    },
}

impl PlannedRouteShardExtension {
    pub fn pending_operation(&self) -> PendingRouteShardOperation {
        let operation = match self {
            PlannedRouteShardExtension::CreateShard { mint, addresses } => {
                PendingRouteShardOperationKind::CreateShard {
                    mint: mint.to_string(),
                    addresses: pubkeys_to_strings(addresses),
                }
            }
            PlannedRouteShardExtension::ExtendShard {
                shard,
                mint,
                addresses,
                ..
            } => PendingRouteShardOperationKind::ExtendShard {
                shard: shard.to_string(),
                mint: mint.to_string(),
                addresses: pubkeys_to_strings(addresses),
            },
        };

        PendingRouteShardOperation {
            version: ROUTE_SHARD_STATE_VERSION,
            operation,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouteShardPlanner {
    store: RouteShardStore,
    allowed_mints: HashSet<Pubkey>,
    shard_capacity: usize,
}

impl RouteShardPlanner {
    pub fn new(
        store: RouteShardStore,
        allowed_mints: impl IntoIterator<Item = Pubkey>,
        shard_capacity: usize,
    ) -> anyhow::Result<Self> {
        if shard_capacity == 0 || shard_capacity > ROUTE_SHARD_MAX_ADDRESSES {
            anyhow::bail!("route shard capacity must be between 1 and 256");
        }

        let allowed_mints = allowed_mints.into_iter().collect::<HashSet<_>>();
        if allowed_mints.is_empty() {
            anyhow::bail!("allowed_mints must not be empty");
        }

        Ok(Self {
            store,
            allowed_mints,
            shard_capacity,
        })
    }

    pub fn store(&self) -> &RouteShardStore {
        &self.store
    }

    pub fn plan_mint_block(
        &self,
        route: &StableMintRouteAccounts,
    ) -> anyhow::Result<Option<PlannedRouteShardExtension>> {
        self.ensure_allowed(route.mint)?;
        if self.store.mints.contains_key(&route.mint.to_string()) {
            return Ok(None);
        }

        let addresses = route.stable_addresses();
        if addresses.len() > self.shard_capacity {
            anyhow::bail!("mint route block does not fit into an empty route shard");
        }

        let Some((shard, record)) = self.store.active_shard_record() else {
            return Ok(Some(PlannedRouteShardExtension::CreateShard {
                mint: route.mint,
                addresses,
            }));
        };

        if record.status != RouteShardStatus::Active
            || record.remaining_capacity() < addresses.len()
        {
            return Ok(Some(PlannedRouteShardExtension::CreateShard {
                mint: route.mint,
                addresses,
            }));
        }

        Ok(Some(PlannedRouteShardExtension::ExtendShard {
            shard: parse_pubkey(shard)?,
            mint: route.mint,
            addresses,
            start_index: checked_u8(record.used)?,
        }))
    }

    pub fn plan_new_dlmm(
        &self,
        mint: Pubkey,
        dlmm: &StableDlmmRouteAccounts,
    ) -> anyhow::Result<Option<PlannedRouteShardExtension>> {
        self.ensure_allowed(mint)?;
        let Some(block) = self.store.mints.get(&mint.to_string()) else {
            anyhow::bail!("mint {} is not allocated in a route shard", mint);
        };

        if block
            .dlmm
            .iter()
            .any(|record| record.lb_pair == dlmm.lb_pair.to_string())
        {
            return Ok(None);
        }

        let shard = self
            .store
            .shards
            .get(&block.shard)
            .ok_or_else(|| anyhow::anyhow!("mint {} references missing shard", mint))?;
        let addresses = dlmm.stable_addresses().to_vec();
        if shard.status != RouteShardStatus::Active || shard.remaining_capacity() < addresses.len()
        {
            anyhow::bail!("route shard {} has no capacity for new DLMM", block.shard);
        }

        Ok(Some(PlannedRouteShardExtension::ExtendShard {
            shard: parse_pubkey(&block.shard)?,
            mint,
            addresses,
            start_index: checked_u8(shard.used)?,
        }))
    }

    pub fn apply_confirmed_mint_block(
        &mut self,
        shard: Pubkey,
        route: &StableMintRouteAccounts,
        slot: u64,
    ) -> anyhow::Result<()> {
        self.ensure_allowed(route.mint)?;
        let shard_key = shard.to_string();
        let addresses = route.stable_addresses();
        let shard_record = self
            .store
            .shards
            .entry(shard_key.clone())
            .or_insert(RouteShardRecord {
                used: 0,
                capacity: self.shard_capacity,
                created_slot: slot,
                last_extended_slot: 0,
                status: RouteShardStatus::Active,
            });

        let start = shard_record.used;
        let end = start + addresses.len();
        if end > shard_record.capacity {
            anyhow::bail!("confirmed mint block exceeds route shard capacity");
        }

        shard_record.used = end;
        shard_record.last_extended_slot = slot;
        if shard_record.used == shard_record.capacity {
            shard_record.status = RouteShardStatus::Full;
        }
        self.store.active_shard = Some(shard_key.clone());

        let base_indexes = index_range(start, 4)?;
        let mut cursor = start + 4;
        let mut dlmm_records = Vec::with_capacity(route.dlmms.len());
        for dlmm in &route.dlmms {
            dlmm_records.push(DlmmAltBlockRecord {
                lb_pair: dlmm.lb_pair.to_string(),
                indexes: index_range(cursor, 3)?,
            });
            cursor += 3;
        }

        self.store.mints.insert(
            route.mint.to_string(),
            MintRouteBlockRecord {
                shard: shard_key,
                base: RouteBaseBlockRecord {
                    indexes: base_indexes,
                },
                dlmm: dlmm_records,
            },
        );
        self.store.validate()
    }

    pub fn apply_confirmed_dlmm(
        &mut self,
        mint: Pubkey,
        dlmm: &StableDlmmRouteAccounts,
        slot: u64,
    ) -> anyhow::Result<()> {
        self.ensure_allowed(mint)?;
        let block =
            self.store.mints.get_mut(&mint.to_string()).ok_or_else(|| {
                anyhow::anyhow!("mint {} is not allocated in a route shard", mint)
            })?;
        let shard = self
            .store
            .shards
            .get_mut(&block.shard)
            .ok_or_else(|| anyhow::anyhow!("mint {} references missing shard", mint))?;

        let start = shard.used;
        let end = start + 3;
        if end > shard.capacity {
            anyhow::bail!("confirmed DLMM exceeds route shard capacity");
        }

        block.dlmm.push(DlmmAltBlockRecord {
            lb_pair: dlmm.lb_pair.to_string(),
            indexes: index_range(start, 3)?,
        });
        shard.used = end;
        shard.last_extended_slot = slot;
        if shard.used == shard.capacity {
            shard.status = RouteShardStatus::Full;
        }
        self.store.validate()
    }

    pub fn apply_confirmed_operation(
        &mut self,
        operation: &PendingRouteShardOperation,
        confirmed_shard: Pubkey,
        slot: u64,
    ) -> anyhow::Result<()> {
        operation.validate()?;

        match &operation.operation {
            PendingRouteShardOperationKind::CreateShard { mint: _, addresses } => {
                let route = stable_mint_route_from_addresses(addresses)?;
                self.apply_confirmed_mint_block(confirmed_shard, &route, slot)
            }
            PendingRouteShardOperationKind::ExtendShard {
                shard,
                mint,
                addresses,
            } => {
                let shard = parse_pubkey(shard)?;
                if shard != confirmed_shard {
                    anyhow::bail!(
                        "confirmed shard {} does not match planned shard {}",
                        confirmed_shard,
                        shard
                    );
                }
                let mint = parse_pubkey(mint)?;
                if self.store.mints.contains_key(&mint.to_string()) {
                    let addresses = parse_pubkeys(addresses)?;
                    if addresses.is_empty() || addresses.len() % 3 != 0 {
                        anyhow::bail!("allocated mint extensions must contain only DLMM triples");
                    }
                    for chunk in addresses.chunks(3) {
                        let dlmm = stable_dlmm_from_addresses(chunk)?;
                        self.apply_confirmed_dlmm(mint, &dlmm, slot)?;
                    }
                    Ok(())
                } else {
                    let route = stable_mint_route_from_addresses(addresses)?;
                    if route.mint != mint {
                        anyhow::bail!(
                            "planned mint {} does not match route mint {}",
                            mint,
                            route.mint
                        );
                    }
                    self.apply_confirmed_mint_block(shard, &route, slot)
                }
            }
        }
    }

    fn ensure_allowed(&self, mint: Pubkey) -> anyhow::Result<()> {
        if !self.allowed_mints.contains(&mint) {
            anyhow::bail!("mint {} is not allowlisted for route shards", mint);
        }
        Ok(())
    }
}

fn write_json<T: Serialize>(path: impl AsRef<Path>, value: &T) -> anyhow::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let tmp = path.with_extension("tmp");
    let json = serde_json::to_string_pretty(value)?;
    fs::write(&tmp, json)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn parse_pubkey(value: &str) -> anyhow::Result<Pubkey> {
    value
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid pubkey `{}`: {}", value, e))
}

fn pubkeys_to_strings(pubkeys: &[Pubkey]) -> Vec<String> {
    pubkeys.iter().map(ToString::to_string).collect()
}

fn parse_pubkeys(values: &[String]) -> anyhow::Result<Vec<Pubkey>> {
    values.iter().map(|value| parse_pubkey(value)).collect()
}

fn validate_mint_block_addresses(mint: Pubkey, addresses: &[Pubkey]) -> anyhow::Result<()> {
    if addresses.len() < 7 || (addresses.len() - 4) % 3 != 0 {
        anyhow::bail!(
            "mint route block must contain 4 base addresses plus one or more DLMM triples"
        );
    }
    if addresses[0] != mint {
        anyhow::bail!("mint route block first address must be the mint");
    }
    Ok(())
}

fn stable_mint_route_from_addresses(
    addresses: &[String],
) -> anyhow::Result<StableMintRouteAccounts> {
    let addresses = parse_pubkeys(addresses)?;
    validate_mint_block_addresses(addresses[0], &addresses)?;
    let dlmms = addresses[4..]
        .chunks(3)
        .map(stable_dlmm_from_addresses)
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(StableMintRouteAccounts {
        mint: addresses[0],
        pump_pool: addresses[1],
        pump_token_vault: addresses[2],
        pump_base_vault: addresses[3],
        dlmms,
    })
}

fn stable_dlmm_from_addresses(addresses: &[Pubkey]) -> anyhow::Result<StableDlmmRouteAccounts> {
    if addresses.len() != 3 {
        anyhow::bail!("DLMM route block must contain exactly 3 addresses");
    }
    Ok(StableDlmmRouteAccounts {
        lb_pair: addresses[0],
        token_vault: addresses[1],
        base_vault: addresses[2],
    })
}

fn checked_u8(value: usize) -> anyhow::Result<u8> {
    u8::try_from(value).map_err(|_| anyhow::anyhow!("route shard index exceeds u8 range"))
}

fn index_range(start: usize, len: usize) -> anyhow::Result<Vec<u8>> {
    (start..start + len).map(checked_u8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn mint_route(mint: Pubkey, dlmm_count: u8) -> StableMintRouteAccounts {
        StableMintRouteAccounts {
            mint,
            pump_pool: pk(2),
            pump_token_vault: pk(3),
            pump_base_vault: pk(4),
            dlmms: (0..dlmm_count)
                .map(|i| StableDlmmRouteAccounts {
                    lb_pair: pk(10 + i * 3),
                    token_vault: pk(11 + i * 3),
                    base_vault: pk(12 + i * 3),
                })
                .collect(),
        }
    }

    #[test]
    fn planner_creates_shard_when_no_active_shard_exists() {
        let mint = pk(1);
        let planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();
        let route = mint_route(mint, 1);

        let plan = planner.plan_mint_block(&route).unwrap().unwrap();

        assert!(matches!(
            plan,
            PlannedRouteShardExtension::CreateShard { .. }
        ));
        assert_eq!(plan.pending_operation().version, ROUTE_SHARD_STATE_VERSION);
    }

    #[test]
    fn pending_create_builds_create_and_extend_instructions() {
        let mint = pk(1);
        let planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();
        let route = mint_route(mint, 1);
        let plan = planner.plan_mint_block(&route).unwrap().unwrap();
        let authority = pk(80);
        let payer = pk(81);

        let built = plan
            .pending_operation()
            .build_instructions(authority, payer, 123)
            .unwrap();

        assert_eq!(built.instructions.len(), 2);
        assert!(built.instructions[0]
            .accounts
            .iter()
            .any(|meta| meta.pubkey == payer && meta.is_signer && meta.is_writable));
        assert!(built.instructions[1]
            .accounts
            .iter()
            .any(|meta| meta.pubkey == built.shard && meta.is_writable));
    }

    #[test]
    fn pending_extend_builds_extend_instruction_for_existing_shard() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 1);
        let mut planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();
        planner
            .apply_confirmed_mint_block(shard, &route, 100)
            .unwrap();
        let new_dlmm = StableDlmmRouteAccounts {
            lb_pair: pk(40),
            token_vault: pk(41),
            base_vault: pk(42),
        };
        let plan = planner.plan_new_dlmm(mint, &new_dlmm).unwrap().unwrap();

        let built = plan
            .pending_operation()
            .build_instructions(pk(80), pk(81), 123)
            .unwrap();

        assert_eq!(built.shard, shard);
        assert_eq!(built.instructions.len(), 1);
        assert!(built.instructions[0]
            .accounts
            .iter()
            .any(|meta| meta.pubkey == shard && meta.is_writable));
    }

    #[test]
    fn apply_confirmed_mint_block_records_base_and_dlmm_indexes() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 2);
        let mut planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();

        planner
            .apply_confirmed_mint_block(shard, &route, 100)
            .unwrap();

        let store = planner.store();
        let shard_record = store.shards.get(&shard.to_string()).unwrap();
        assert_eq!(shard_record.used, 10);
        let block = store.mints.get(&mint.to_string()).unwrap();
        assert_eq!(block.base.indexes, vec![0, 1, 2, 3]);
        assert_eq!(block.dlmm[0].indexes, vec![4, 5, 6]);
        assert_eq!(block.dlmm[1].indexes, vec![7, 8, 9]);
    }

    #[test]
    fn apply_confirmed_create_operation_records_mint_block() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 2);
        let plan = PlannedRouteShardExtension::CreateShard {
            mint,
            addresses: route.stable_addresses(),
        };
        let mut planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();

        planner
            .apply_confirmed_operation(&plan.pending_operation(), shard, 100)
            .unwrap();

        let block = planner.store().mints.get(&mint.to_string()).unwrap();
        assert_eq!(block.shard, shard.to_string());
        assert_eq!(block.base.indexes, vec![0, 1, 2, 3]);
        assert_eq!(block.dlmm.len(), 2);
    }

    #[test]
    fn apply_confirmed_extend_operation_records_new_dlmm() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 1);
        let mut planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();
        planner
            .apply_confirmed_mint_block(shard, &route, 100)
            .unwrap();
        let new_dlmm = StableDlmmRouteAccounts {
            lb_pair: pk(40),
            token_vault: pk(41),
            base_vault: pk(42),
        };
        let plan = PlannedRouteShardExtension::ExtendShard {
            shard,
            mint,
            addresses: new_dlmm.stable_addresses().to_vec(),
            start_index: 7,
        };

        planner
            .apply_confirmed_operation(&plan.pending_operation(), shard, 101)
            .unwrap();

        let block = planner.store().mints.get(&mint.to_string()).unwrap();
        assert_eq!(block.dlmm.len(), 2);
        assert_eq!(block.dlmm[1].lb_pair, pk(40).to_string());
        assert_eq!(block.dlmm[1].indexes, vec![7, 8, 9]);
    }

    #[test]
    fn validate_extend_accepts_multiple_dlmm_triples() {
        let mint = pk(1);
        let shard = pk(90);
        let addresses = vec![pk(40), pk(41), pk(42), pk(43), pk(44), pk(45)];
        let operation = PendingRouteShardOperation {
            version: ROUTE_SHARD_STATE_VERSION,
            operation: PendingRouteShardOperationKind::ExtendShard {
                shard: shard.to_string(),
                mint: mint.to_string(),
                addresses: pubkeys_to_strings(&addresses),
            },
        };

        operation.validate().unwrap();
    }

    #[test]
    fn split_large_mint_block_extend_keeps_first_chunk_as_mint_block() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 8);
        let operation = PlannedRouteShardExtension::ExtendShard {
            shard,
            mint,
            addresses: route.stable_addresses(),
            start_index: 0,
        }
        .pending_operation();

        let split = split_route_shard_operation(operation).unwrap();

        assert_eq!(split.len(), 2);
        match &split[0].operation {
            PendingRouteShardOperationKind::ExtendShard { addresses, .. } => {
                assert_eq!(addresses.len(), 19);
                assert_eq!(parse_pubkey(&addresses[0]).unwrap(), mint);
            }
            _ => panic!("expected extend shard operation"),
        }
        match &split[1].operation {
            PendingRouteShardOperationKind::ExtendShard { addresses, .. } => {
                assert_eq!(addresses.len(), 9);
            }
            _ => panic!("expected extend shard operation"),
        }
    }

    #[test]
    fn split_large_allocated_mint_extend_keeps_dlmm_triples() {
        let mint = pk(1);
        let shard = pk(90);
        let addresses = (0..8)
            .flat_map(|i| [pk(40 + i * 3), pk(41 + i * 3), pk(42 + i * 3)])
            .collect::<Vec<_>>();
        let operation = PendingRouteShardOperation {
            version: ROUTE_SHARD_STATE_VERSION,
            operation: PendingRouteShardOperationKind::ExtendShard {
                shard: shard.to_string(),
                mint: mint.to_string(),
                addresses: pubkeys_to_strings(&addresses),
            },
        };

        let split = split_route_shard_operation(operation).unwrap();

        assert_eq!(split.len(), 2);
        for operation in split {
            operation.validate().unwrap();
            match operation.operation {
                PendingRouteShardOperationKind::ExtendShard { addresses, .. } => {
                    assert_eq!(addresses.len() % 3, 0);
                    assert!(addresses.len() <= ROUTE_SHARD_EXTEND_ADDRESSES_PER_TX);
                }
                _ => panic!("expected extend shard operation"),
            }
        }
    }

    #[test]
    fn lookup_resolver_returns_confirmed_shard_for_mint() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 1);
        let mut planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();
        planner
            .apply_confirmed_mint_block(shard, &route, 100)
            .unwrap();
        let resolver = RouteShardLookupResolver::new(planner.store().clone()).unwrap();

        assert_eq!(resolver.shard_for_mint(mint).unwrap(), Some(shard));
        assert!(resolver.block_for_mint(mint).is_some());
        assert_eq!(resolver.shard_for_mint(pk(2)).unwrap(), None);
    }

    #[test]
    fn planner_extends_existing_mint_for_new_dlmm() {
        let mint = pk(1);
        let shard = pk(90);
        let route = mint_route(mint, 1);
        let mut planner = RouteShardPlanner::new(RouteShardStore::empty(), [mint], 256).unwrap();
        planner
            .apply_confirmed_mint_block(shard, &route, 100)
            .unwrap();
        let new_dlmm = StableDlmmRouteAccounts {
            lb_pair: pk(40),
            token_vault: pk(41),
            base_vault: pk(42),
        };

        let plan = planner.plan_new_dlmm(mint, &new_dlmm).unwrap().unwrap();

        assert_eq!(
            plan,
            PlannedRouteShardExtension::ExtendShard {
                shard,
                mint,
                addresses: new_dlmm.stable_addresses().to_vec(),
                start_index: 7,
            }
        );
        planner.apply_confirmed_dlmm(mint, &new_dlmm, 101).unwrap();
        let block = planner.store().mints.get(&mint.to_string()).unwrap();
        assert_eq!(block.dlmm[1].indexes, vec![7, 8, 9]);
    }

    #[test]
    fn planner_rejects_non_allowlisted_mint() {
        let allowed = pk(1);
        let denied = pk(2);
        let planner = RouteShardPlanner::new(RouteShardStore::empty(), [allowed], 256).unwrap();

        assert!(planner.plan_mint_block(&mint_route(denied, 1)).is_err());
    }

    #[test]
    fn stable_route_accounts_require_eligible_pump_and_dlmm() {
        use crate::constants::sol_mint;
        use crate::registry::{DlmmRouteState, MintRuntimeState, PoolLiquidity, PumpRouteState};

        let now_ms = 1_000;
        let mint = pk(1);
        let mut state = MintRuntimeState::new(mint, spl_token::ID);
        state.pump.push(PumpRouteState {
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
                updated_at_ms: now_ms,
            }),
            enabled: true,
            last_update_slot: 1,
        });
        state.dlmms.push(DlmmRouteState {
            program_id: pk(10),
            base_mint: sol_mint(),
            event_authority: pk(11),
            memo_program: None,
            lb_pair: pk(12),
            token_vault: pk(13),
            base_vault: pk(14),
            oracle: pk(15),
            bin_array_bitmap_extension: None,
            bin_arrays: Vec::new(),
            active_id: 0,
            liquidity: Some(PoolLiquidity {
                base_lamports: 2_000,
                token_lamports: None,
                updated_at_ms: now_ms,
            }),
            enabled: true,
            last_update_slot: 1,
        });

        let stable =
            StableMintRouteAccounts::from_mint_runtime_state(&state, 1_000, 500, now_ms).unwrap();

        assert_eq!(stable.mint, mint);
        assert_eq!(stable.pump_pool, pk(2));
        assert_eq!(stable.dlmms[0].lb_pair, pk(12));
    }
}
