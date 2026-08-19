use anyhow::Context;
use clap::{App, Arg};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_onchain_arbitrage_bot::ata::ensure_base_atas_exist;
use solana_onchain_arbitrage_bot::config::Config;
use solana_onchain_arbitrage_bot::pool_refreshers::PoolDataRefresher;
use solana_onchain_arbitrage_bot::pools::MintPoolData;
use solana_onchain_arbitrage_bot::refresh::initialize_pools_from_markets;
use solana_onchain_arbitrage_bot::transaction::create_swap_instruction;
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::address_lookup_table::AddressLookupTableAccount;
use solana_sdk::commitment_config::CommitmentLevel as RpcCommitmentLevel;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::message::v0::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair};
use solana_sdk::signer::Signer;
use solana_sdk::system_instruction;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const USD1: &str = "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB";
const FLASHX: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";
const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const SMB_LUT: &str = "4sKLJ1Qoudh8PJyqBeuKocYdsZvxTcRShUt9aKqwhgvC";
const DEFAULT_FAST_URL: &str = "https://fast.circular.fi/transactions";
const DEFAULT_HELIUS_SENDER_URL: &str = "http://fra-sender.helius-rpc.com/fast";
const CIRCULAR_TIP_ACCOUNTS: &str = "FAST3dMFZvESiEipBvLSiXq3QCV51o3xuoHScqRU6cB6,FASTHPW6akdGh9PFSdhMTbCuGkCSX7LsUjjnaB2RTQ4v,FASTYKWXRfAoty7SQCM1mGVrmPUyyNcF4tc3DUkLDAu9,FASTPB76TxKPMZ7Q29m8v4zJn8gUjbWyvTEQaaxhwN7M,FASTs6ctgbsuZegMzUs4DPUYhRSZUPCjgCVnttHbpQAp,FASTYmSidNfLwdwiQEhCTtzghxEtaipeNSDSwh9xDPs3,FASTCKnwwY6iL3CknRgg3Zqir7jeagDDhxSnBQQy5a1C,FASTKL1AamNKrwnvbKwo4PU8434BBdqVrTtugM6oDU71";
const HELIUS_TIP_ACCOUNTS: &str = "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE,D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ,9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta,5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn,2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD,2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ,wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF,3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT,4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey,4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FastSenderProvider {
    Circular,
    Helius,
}

#[derive(Clone)]
struct FastSender {
    provider: FastSenderProvider,
    client: reqwest::Client,
    url: String,
    api_key: String,
    cashback_address: String,
    tip_accounts: Vec<Pubkey>,
    tip_from: u64,
    tip_to: u64,
    timeout_ms: u64,
}

#[derive(Clone)]
struct SendConfig {
    fast: Option<FastSender>,
    send_rpc: bool,
}

#[derive(Clone, Debug)]
struct MintPools {
    pump: Vec<String>,
    dlmm: Vec<String>,
}

#[derive(Clone, Debug)]
struct PumpReserve {
    token_reserve: u64,
    sol_reserve: u64,
    updated_ms: u128,
}

#[derive(Clone)]
struct RouteState {
    config: Config,
    pool_data: MintPoolData,
}

#[derive(Clone, Debug)]
struct FlashEvent {
    at: Instant,
    volume_usd: f64,
}

#[derive(Clone, Debug)]
struct AxiomSwapSignal {
    mint: String,
    pump: String,
    side: &'static str,
    sol_amount: f64,
    volume_usd: f64,
    volume_source: &'static str,
}

struct ExecutorState {
    base_config: Config,
    rpc_client: Arc<RpcClient>,
    sending_rpc_clients: Vec<Arc<RpcClient>>,
    send_config: SendConfig,
    wallet: Arc<Keypair>,
    lookup_tables: Vec<AddressLookupTableAccount>,
    cached_blockhash: Hash,
    pools_file: String,
    pools_mtime: Option<SystemTime>,
    pump_reserves_file: String,
    pump_reserves_mtime: Option<SystemTime>,
    pump_reserves_max_age_ms: u128,
    max_dlmm: usize,
    pools_by_mint: HashMap<String, MintPools>,
    pump_reserves_by_pool: HashMap<String, PumpReserve>,
    route_cache: HashMap<String, RouteState>,
    flash_by_mint: HashMap<String, Vec<FlashEvent>>,
    last_signal_by_mint: HashMap<String, Instant>,
    last_signal_by_mint_slot: HashMap<String, Instant>,
    last_pool_touch_by_mint: HashMap<String, Instant>,
    pool_refresher: PoolDataRefresher,
}

fn load_keypair(private_key: &str) -> anyhow::Result<Keypair> {
    if let Ok(bytes) = bs58::decode(private_key).into_vec() {
        if let Ok(kp) = Keypair::from_bytes(&bytes) {
            return Ok(kp);
        }
    }
    read_keypair_file(private_key)
        .map_err(|e| anyhow::anyhow!("failed to load keypair {}: {}", private_key, e))
}

fn is_excluded_mint(mint: &str) -> bool {
    matches!(mint, WSOL | USDC | USDT | USD1)
}

fn pubkey_bytes_to_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 32 {
        return None;
    }
    Some(bs58::encode(bytes).into_string())
}

fn load_luts(rpc: &RpcClient, addresses: &[String]) -> Vec<AddressLookupTableAccount> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for addr in addresses {
        if !seen.insert(addr.clone()) {
            continue;
        }
        let Ok(pubkey) = Pubkey::from_str(addr) else {
            error!("invalid LUT: {}", addr);
            continue;
        };
        match rpc.get_account(&pubkey) {
            Ok(account) => match AddressLookupTable::deserialize(&account.data) {
                Ok(lut) => {
                    info!("loaded LUT {}", pubkey);
                    out.push(AddressLookupTableAccount {
                        key: pubkey,
                        addresses: lut.addresses.into_owned(),
                    });
                }
                Err(e) => error!("failed to deserialize LUT {}: {}", pubkey, e),
            },
            Err(e) => error!("failed to fetch LUT {}: {}", pubkey, e),
        }
    }
    out
}

fn sorted_pool_keys(side: &Value, max: usize) -> Vec<String> {
    let Some(obj) = side.as_object() else {
        return Vec::new();
    };
    let mut rows = obj
        .iter()
        .map(|(addr, meta)| {
            let count = meta.get("count").and_then(Value::as_u64).unwrap_or(0);
            let last_seen = meta
                .get("last_seen")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            (addr.clone(), count, last_seen)
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));
    rows.into_iter().take(max).map(|r| r.0).collect()
}

fn load_pools_by_mint(path: &str, max_dlmm: usize) -> anyhow::Result<HashMap<String, MintPools>> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path))?;
    let v: Value = serde_json::from_str(&raw)?;
    let mut out = HashMap::new();
    let Some(obj) = v.as_object() else {
        return Ok(out);
    };
    for (mint, rec) in obj {
        if is_excluded_mint(mint) {
            continue;
        }
        let pump = sorted_pool_keys(rec.get("pump").unwrap_or(&Value::Null), 1);
        let dlmm = sorted_pool_keys(rec.get("dlmm").unwrap_or(&Value::Null), max_dlmm);
        if !pump.is_empty() && !dlmm.is_empty() {
            out.insert(mint.clone(), MintPools { pump, dlmm });
        }
    }
    Ok(out)
}

fn load_pump_reserves(path: &str) -> anyhow::Result<HashMap<String, PumpReserve>> {
    if path.is_empty() {
        return Ok(HashMap::new());
    }
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path))?;
    let v: Value = serde_json::from_str(&raw)?;
    let mut out = HashMap::new();
    let Some(obj) = v.as_object() else {
        return Ok(out);
    };
    for (pool, rec) in obj {
        let token_reserve = rec
            .get("token_reserve")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let sol_reserve = rec.get("sol_reserve").and_then(Value::as_u64).unwrap_or(0);
        let updated_ms = rec
            .get("updated_ms")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<u128>().ok())
            .or_else(|| rec.get("updated_ms").and_then(Value::as_u64).map(u128::from))
            .unwrap_or(0);
        if token_reserve > 0 && sol_reserve > 0 && updated_ms > 0 {
            out.insert(
                pool.clone(),
                PumpReserve {
                    token_reserve,
                    sol_reserve,
                    updated_ms,
                },
            );
        }
    }
    Ok(out)
}

fn file_mtime(path: &str) -> Option<SystemTime> {
    fs::metadata(path).and_then(|m| m.modified()).ok()
}

fn reload_pools_if_changed(state: &mut ExecutorState) {
    let mtime = file_mtime(&state.pools_file);
    if mtime.is_some() && mtime != state.pools_mtime {
        match load_pools_by_mint(&state.pools_file, state.max_dlmm) {
            Ok(pools) => {
                state.pools_by_mint = pools;
                state.pools_mtime = mtime;
                info!(
                    "reloaded {} actionable mints from {}",
                    state.pools_by_mint.len(),
                    state.pools_file
                );
            }
            Err(e) => error!("failed to reload pools_by_mint: {}", e),
        }
    }
}

fn reload_pump_reserves_if_changed(state: &mut ExecutorState) {
    if state.pump_reserves_file.is_empty() {
        return;
    }
    let mtime = file_mtime(&state.pump_reserves_file);
    if mtime.is_some() && mtime != state.pump_reserves_mtime {
        match load_pump_reserves(&state.pump_reserves_file) {
            Ok(reserves) => {
                state.pump_reserves_by_pool = reserves;
                state.pump_reserves_mtime = mtime;
                info!(
                    "reloaded {} pump reserves from {}",
                    state.pump_reserves_by_pool.len(),
                    state.pump_reserves_file
                );
            }
            Err(e) => error!("failed to reload pump reserves: {}", e),
        }
    }
}

fn token_amount(amount: &str, decimals: u32) -> f64 {
    let raw = amount.parse::<f64>().unwrap_or(0.0);
    raw / 10f64.powi(decimals as i32)
}

fn estimate_wsol_volume_usd(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    sol_usd: f64,
) -> f64 {
    estimate_wsol_volume_sol(meta) * sol_usd
}

fn estimate_wsol_volume_sol(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
) -> f64 {
    let mut pre = HashMap::new();
    let mut post = HashMap::new();
    for b in &meta.pre_token_balances {
        if b.mint == WSOL {
            if let Some(ui) = &b.ui_token_amount {
                pre.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    for b in &meta.post_token_balances {
        if b.mint == WSOL {
            if let Some(ui) = &b.ui_token_amount {
                post.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    let mut max_abs = 0.0;
    for idx in pre.keys().chain(post.keys()) {
        let a = pre.get(idx).copied().unwrap_or(0.0);
        let b = post.get(idx).copied().unwrap_or(0.0);
        let diff = (b - a).abs();
        if diff > max_abs {
            max_abs = diff;
        }
    }
    max_abs
}

fn owner_wsol_volume_sol(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    owner: &str,
) -> Option<f64> {
    let mut pre = HashMap::new();
    let mut post = HashMap::new();
    for b in &meta.pre_token_balances {
        if b.mint == WSOL && b.owner == owner {
            if let Some(ui) = &b.ui_token_amount {
                pre.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    for b in &meta.post_token_balances {
        if b.mint == WSOL && b.owner == owner {
            if let Some(ui) = &b.ui_token_amount {
                post.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    let max_abs = pre
        .keys()
        .chain(post.keys())
        .map(|idx| {
            let a = pre.get(idx).copied().unwrap_or(0.0);
            let b = post.get(idx).copied().unwrap_or(0.0);
            (b - a).abs()
        })
        .fold(0.0, f64::max);
    if max_abs > 0.0 {
        Some(max_abs)
    } else {
        None
    }
}

fn keyed_wsol_volume_sol(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    candidate_owners: &HashSet<String>,
) -> Option<f64> {
    let mut pre = HashMap::new();
    let mut post = HashMap::new();
    for b in &meta.pre_token_balances {
        if b.mint == WSOL && candidate_owners.contains(&b.owner) {
            if let Some(ui) = &b.ui_token_amount {
                pre.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    for b in &meta.post_token_balances {
        if b.mint == WSOL && candidate_owners.contains(&b.owner) {
            if let Some(ui) = &b.ui_token_amount {
                post.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    let max_abs = pre
        .keys()
        .chain(post.keys())
        .map(|idx| {
            let a = pre.get(idx).copied().unwrap_or(0.0);
            let b = post.get(idx).copied().unwrap_or(0.0);
            (b - a).abs()
        })
        .fold(0.0, f64::max);
    if max_abs > 0.0 {
        Some(max_abs)
    } else {
        None
    }
}

fn instruction_account_wsol_volume_sol(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    instruction_accounts: &HashSet<u32>,
) -> Option<f64> {
    let mut pre = HashMap::new();
    let mut post = HashMap::new();
    for b in &meta.pre_token_balances {
        if b.mint == WSOL && instruction_accounts.contains(&b.account_index) {
            if let Some(ui) = &b.ui_token_amount {
                pre.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    for b in &meta.post_token_balances {
        if b.mint == WSOL && instruction_accounts.contains(&b.account_index) {
            if let Some(ui) = &b.ui_token_amount {
                post.insert(b.account_index, token_amount(&ui.amount, ui.decimals));
            }
        }
    }
    let max_abs = pre
        .keys()
        .chain(post.keys())
        .map(|idx| {
            let a = pre.get(idx).copied().unwrap_or(0.0);
            let b = post.get(idx).copied().unwrap_or(0.0);
            (b - a).abs()
        })
        .fold(0.0, f64::max);
    if max_abs > 0.0 {
        Some(max_abs)
    } else {
        None
    }
}

fn instruction_account_native_volume_sol(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    instruction_accounts: &HashSet<u32>,
) -> Option<f64> {
    let len = meta.pre_balances.len().min(meta.post_balances.len());
    let max_abs = (0..len)
        .filter(|idx| instruction_accounts.contains(&(*idx as u32)))
        .map(|idx| {
            let pre = meta.pre_balances[idx] as i128;
            let post = meta.post_balances[idx] as i128;
            (post - pre).abs() as f64 / 1_000_000_000.0
        })
        .fold(0.0, f64::max);
    if max_abs > 0.0 {
        Some(max_abs)
    } else {
        None
    }
}

fn token_balance_mints(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
) -> Vec<String> {
    let mut out = Vec::new();
    for b in meta.pre_token_balances.iter().chain(meta.post_token_balances.iter()) {
        if !b.mint.is_empty() && !is_excluded_mint(&b.mint) && !out.contains(&b.mint) {
            out.push(b.mint.clone());
        }
    }
    out
}

fn account_keys(
    tx: &yellowstone_grpc_proto::solana::storage::confirmed_block::Transaction,
    meta: Option<&yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta>,
) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(msg) = &tx.message {
        for k in &msg.account_keys {
            if let Some(s) = pubkey_bytes_to_string(k) {
                keys.push(s);
            }
        }
    }
    if let Some(meta) = meta {
        for k in &meta.loaded_writable_addresses {
            if let Some(s) = pubkey_bytes_to_string(k) {
                keys.push(s);
            }
        }
        for k in &meta.loaded_readonly_addresses {
            if let Some(s) = pubkey_bytes_to_string(k) {
                keys.push(s);
            }
        }
    }
    keys
}

fn read_le_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.len() < 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    Some(u64::from_le_bytes(buf))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn estimate_cached_pump_sell_sol(
    reserves_by_pool: &HashMap<String, PumpReserve>,
    pump: &str,
    token_in_raw: u64,
    max_age_ms: u128,
) -> Option<f64> {
    if token_in_raw == 0 {
        return None;
    }
    let reserve = reserves_by_pool.get(pump)?;
    let age_ms = now_ms().saturating_sub(reserve.updated_ms);
    if max_age_ms > 0 && age_ms > max_age_ms {
        return None;
    }
    let token_reserve = reserve.token_reserve as u128;
    let sol_reserve = reserve.sol_reserve as u128;
    if token_reserve == 0 || sol_reserve == 0 {
        return None;
    }

    let token_in = token_in_raw as u128;
    let out_lamports =
        sol_reserve.saturating_mul(token_in) / token_reserve.saturating_add(token_in);
    let conservative_sol = (out_lamports as f64) * 0.97 / 1_000_000_000.0;
    (conservative_sol > 0.0).then_some(conservative_sol)
}

fn axiom_swap_signals(
    tx: &yellowstone_grpc_proto::solana::storage::confirmed_block::Transaction,
    meta: Option<&yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta>,
    keys: &[String],
    pools_by_mint: &HashMap<String, MintPools>,
    pump_reserves_by_pool: &HashMap<String, PumpReserve>,
    pump_reserves_max_age_ms: u128,
    sol_usd: f64,
    min_sol: f64,
) -> Vec<AxiomSwapSignal> {
    let Some(msg) = &tx.message else {
        return Vec::new();
    };
    msg.instructions
        .iter()
        .filter_map(|ix| {
            let program = keys.get(ix.program_id_index as usize)?;
            if program != FLASHX || ix.accounts.len() < 14 || ix.data.len() < 17 {
                return None;
            }
            let account_at = |pos: usize| -> Option<String> {
                ix.accounts
                    .get(pos)
                    .and_then(|account_idx| keys.get(*account_idx as usize))
                    .cloned()
            };
            let mint = account_at(12)?;
            let quote_mint = account_at(13)?;
            if is_excluded_mint(&mint) || quote_mint != WSOL {
                return None;
            }
            let pools = pools_by_mint.get(&mint)?;
            if pools.dlmm.is_empty() {
                return None;
            }
            let user = account_at(1)?;
            let side = match ix.data[0] {
                0 => "sell",
                1 => "buy",
                _ => return None,
            };
            let amount_0 = read_le_u64(&ix.data[1..9]).unwrap_or(0);
            let sol_amount = amount_0 as f64 / 1_000_000_000.0;
            let pump = pools.pump.first().cloned()?;
            let candidate_owners = ix
                .accounts
                .iter()
                .filter_map(|account_idx| keys.get(*account_idx as usize).cloned())
                .collect::<HashSet<_>>();
            let instruction_accounts = ix
                .accounts
                .iter()
                .copied()
                .map(u32::from)
                .collect::<HashSet<_>>();
            let measured_from_meta = meta.and_then(|meta| {
                owner_wsol_volume_sol(meta, &user)
                    .map(|sol| (sol, "meta_wsol_owner"))
                    .or_else(|| {
                        instruction_account_wsol_volume_sol(meta, &instruction_accounts)
                            .map(|sol| (sol, "meta_wsol_ix_account"))
                    })
                    .or_else(|| {
                        keyed_wsol_volume_sol(meta, &candidate_owners)
                            .map(|sol| (sol, "meta_wsol_ix_owner"))
                    })
                    .or_else(|| {
                        let sol = estimate_wsol_volume_sol(meta);
                        if sol > 0.0 {
                            Some((sol, "meta_wsol_tx"))
                        } else {
                            None
                        }
                    })
                    .or_else(|| {
                        instruction_account_native_volume_sol(meta, &instruction_accounts)
                            .map(|sol| (sol, "meta_native_ix_account"))
                    })
            });
            let (measured_sol, volume_source) = if let Some(measured) = measured_from_meta {
                measured
            } else if side == "buy" {
                (sol_amount, "instruction_bytes_buy")
            } else if let Some(sol) = estimate_cached_pump_sell_sol(
                pump_reserves_by_pool,
                &pump,
                amount_0,
                pump_reserves_max_age_ms,
            ) {
                (sol, "pump_sell_cache")
            } else {
                info!(
                    "AXIOM_ESTIMATE_FAIL mint={} signal={} raw_amount={} volume_source=pump_sell_cache pump={} dlmms={} min_sol={:.6}",
                    mint,
                    side,
                    amount_0,
                    pump,
                    pools.dlmm.len(),
                    min_sol
                );
                return None;
            };
            if measured_sol < min_sol {
                info!(
                    "AXIOM_FILTERED mint={} signal={} sol={:.6} raw_amount={} volume_source={} pump={} dlmms={} min_sol={:.6}",
                    mint,
                    side,
                    measured_sol,
                    amount_0,
                    volume_source,
                    pump,
                    pools.dlmm.len(),
                    min_sol
                );
                return None;
            }
            Some(AxiomSwapSignal {
                mint,
                pump,
                side,
                sol_amount: measured_sol,
                volume_usd: measured_sol * sol_usd,
                volume_source,
            })
        })
        .collect()
}

fn signal_candidates_from_keys(
    pools_by_mint: &HashMap<String, MintPools>,
    keys: &[String],
) -> Vec<(String, String, Vec<String>)> {
    let keyset = keys.iter().cloned().collect::<HashSet<_>>();
    let mut out = Vec::new();
    for (mint, pools) in pools_by_mint {
        let pump = pools
            .pump
            .iter()
            .find(|p| keyset.contains(*p))
            .cloned();
        let Some(pump) = pump else {
            continue;
        };
        if pools.dlmm.is_empty() {
            continue;
        }
        out.push((mint.clone(), pump, pools.dlmm.clone()));
    }
    out
}

fn signal_candidates_from_axiom_swaps(
    pools_by_mint: &HashMap<String, MintPools>,
    swaps: &[AxiomSwapSignal],
) -> Vec<(String, String, Vec<String>, f64, &'static str, f64, &'static str)> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for swap in swaps {
        let Some(pools) = pools_by_mint.get(&swap.mint) else {
            continue;
        };
        if pools.dlmm.is_empty() {
            continue;
        }
        let key = format!("{}:{}", swap.mint, swap.pump);
        if seen.insert(key) {
            out.push((
                swap.mint.clone(),
                swap.pump.clone(),
                pools.dlmm.clone(),
                swap.volume_usd,
                swap.side,
                swap.sol_amount,
                swap.volume_source,
            ));
        }
    }
    out
}

fn signal_candidates(
    pools_by_mint: &HashMap<String, MintPools>,
    keys: &[String],
    mints: &[String],
) -> Vec<(String, String, Vec<String>)> {
    let keyset = keys.iter().cloned().collect::<HashSet<_>>();
    let mut out = Vec::new();
    for mint in mints {
        let Some(pools) = pools_by_mint.get(mint) else {
            continue;
        };
        let pump = pools
            .pump
            .iter()
            .find(|p| keyset.contains(*p))
            .cloned()
            .or_else(|| pools.pump.first().cloned());
        let Some(pump) = pump else {
            continue;
        };
        if pools.dlmm.is_empty() {
            continue;
        }
        out.push((mint.clone(), pump, pools.dlmm.clone()));
    }
    out
}

fn subscription_accounts(
    pools_by_mint: &HashMap<String, MintPools>,
    rabbit_pool_filter: bool,
    pump_program_filter: bool,
    max_filter_accounts: usize,
) -> Vec<String> {
    let mut out = vec![FLASHX.to_string()];
    if pump_program_filter {
        out.push(PUMP_AMM_PROGRAM.to_string());
    }
    if !rabbit_pool_filter {
        return out;
    }
    let mut seen = HashSet::new();
    seen.insert(FLASHX.to_string());
    seen.insert(PUMP_AMM_PROGRAM.to_string());
    for pools in pools_by_mint.values() {
        for pool in &pools.pump {
            if out.len() >= max_filter_accounts {
                return out;
            }
            if seen.insert(pool.clone()) {
                out.push(pool.clone());
            }
        }
    }
    out
}

fn parse_pubkey_list(raw: &str) -> anyhow::Result<Vec<Pubkey>> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| Pubkey::from_str(s).with_context(|| format!("invalid pubkey {}", s)))
        .collect()
}

fn parse_csv_strings(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn random_lamports(from: u64, to: u64) -> u64 {
    if to <= from {
        from
    } else {
        from + (rand::random::<u64>() % (to - from + 1))
    }
}

fn random_tip_account(accounts: &[Pubkey]) -> Option<Pubkey> {
    if accounts.is_empty() {
        None
    } else {
        Some(accounts[rand::random::<usize>() % accounts.len()])
    }
}

fn build_versioned_transaction(
    wallet_kp: &Keypair,
    config: &Config,
    mint_pool_data: &MintPoolData,
    blockhash: Hash,
    address_lookup_table_accounts: &[AddressLookupTableAccount],
    fast_sender: Option<&FastSender>,
    no_failure_mode: bool,
) -> anyhow::Result<VersionedTransaction> {
    let enable_flashloan = config.flashloan.as_ref().map_or(false, |k| k.enabled);
    let compute_unit_limit = config.bot.compute_unit_limit;
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(
            compute_unit_limit + rand::random::<u32>() % 1000,
        ),
    ];
    let compute_unit_price = config.spam.as_ref().map_or(1000, |s| s.compute_unit_price);
    instructions.push(ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price));

    if let Some(sender) = fast_sender {
        if let Some(tip_account) = random_tip_account(&sender.tip_accounts) {
            let lamports = random_lamports(sender.tip_from, sender.tip_to);
            if lamports > 0 {
                instructions.push(system_instruction::transfer(
                    &wallet_kp.pubkey(),
                    &tip_account,
                    lamports,
                ));
            }
        }
    }

    instructions.push(create_swap_instruction(
        wallet_kp,
        mint_pool_data,
        compute_unit_limit,
        enable_flashloan,
        no_failure_mode,
    )?);

    let message = Message::try_compile(
        &wallet_kp.pubkey(),
        &instructions,
        address_lookup_table_accounts,
        blockhash,
    )?;
    Ok(VersionedTransaction::try_new(
        solana_sdk::message::VersionedMessage::V0(message),
        &[wallet_kp],
    )?)
}

async fn send_fast(sender: &FastSender, tx: &VersionedTransaction) -> anyhow::Result<String> {
    let bytes = bincode::serialize(tx)?;
    let tx64 = base64::encode(bytes);
    let body = match sender.provider {
        FastSenderProvider::Circular => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx64,
                {
                    "cashbackAddress": sender.cashback_address
                }
            ]
        }),
        FastSenderProvider::Helius => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx64,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        }),
    };
    let mut request = sender
        .client
        .post(&sender.url)
        .header("content-type", "application/json")
        .json(&body);
    if sender.provider == FastSenderProvider::Circular {
        request = request.header("x-api-key", &sender.api_key);
    }
    let resp = request.send().await?;
    let status = resp.status();
    let value: Value = resp.json().await?;
    if !status.is_success() || value.get("error").is_some() {
        anyhow::bail!("fast sender status={} body={}", status, value);
    }
    extract_signature_from_response(&value)
        .with_context(|| format!("fast sender missing result signature body={}", value))
}

fn extract_signature_from_response(value: &Value) -> Option<String> {
    if let Some(s) = value.get("result").and_then(Value::as_str) {
        return Some(s.to_string());
    }
    if let Some(arr) = value.get("result").and_then(Value::as_array) {
        if let Some(s) = arr.first().and_then(Value::as_str) {
            return Some(s.to_string());
        }
    }
    let result = value.get("result").unwrap_or(value);
    for key in ["signature", "txid", "txId", "transactionSignature", "hash"] {
        if let Some(s) = result.get(key).and_then(Value::as_str) {
            return Some(s.to_string());
        }
    }
    if let Some(data) = result.get("data") {
        for key in ["signature", "txid", "txId", "transactionSignature", "hash"] {
            if let Some(s) = data.get(key).and_then(Value::as_str) {
                return Some(s.to_string());
            }
        }
    }
    None
}

async fn send_signed_transaction(
    rpc_clients: &[Arc<RpcClient>],
    send_config: &SendConfig,
    tx: VersionedTransaction,
) -> Vec<(&'static str, anyhow::Result<String>)> {
    let mut tasks = Vec::new();
    if let Some(sender) = &send_config.fast {
        let sender = sender.clone();
        let tx = tx.clone();
        tasks.push(tokio::spawn(async move {
            let res = tokio::time::timeout(
                Duration::from_millis(sender.timeout_ms),
                send_fast(&sender, &tx),
            )
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "fast sender timeout after {}ms",
                    sender.timeout_ms
                ))
            });
            ("fast", res)
        }));
    }
    if send_config.send_rpc {
        for client in rpc_clients {
            let client = client.clone();
            let tx = tx.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let res = client
                    .send_transaction_with_config(
                        &tx,
                        RpcSendTransactionConfig {
                            skip_preflight: true,
                            max_retries: Some(0),
                            preflight_commitment: Some(RpcCommitmentLevel::Processed),
                            ..Default::default()
                        },
                    )
                    .map(|sig| sig.to_string())
                    .map_err(anyhow::Error::from);
                ("rpc", res)
            }));
        }
    }
    if tasks.is_empty() {
        return vec![("none", Err(anyhow::anyhow!("no transaction senders configured")))];
    }

    let mut out = Vec::new();
    for task in tasks {
        match task.await {
            Ok(res) => out.push(res),
            Err(e) => out.push(("sender", Err(anyhow::anyhow!("sender task failed: {}", e)))),
        }
    }
    out
}

async fn route_for(
    state: &mut ExecutorState,
    mint: &str,
    pump: &str,
    dlmm: &str,
    process_delay_ms: u64,
) -> anyhow::Result<RouteState> {
    let key = format!("{}|{}|{}", mint, pump, dlmm);
    if let Some(route) = state.route_cache.get(&key) {
        return Ok(route.clone());
    }

    let mut cfg = state.base_config.clone();
    cfg.routing.markets.markets = vec![pump.to_string(), dlmm.to_string()];
    cfg.routing.markets.process_delay = process_delay_ms;

    let mut map =
        initialize_pools_from_markets(&cfg.routing.markets, &state.wallet.pubkey(), state.rpc_client.clone())
            .await?;
    let (_, pool_data) = map.drain().next().context("no pool data initialized")?;
    let route = RouteState {
        config: cfg,
        pool_data,
    };
    state.route_cache.insert(key, route.clone());
    Ok(route)
}

async fn prewarm_routes(state: Arc<Mutex<ExecutorState>>, limit: usize, process_delay_ms: u64) {
    let routes = {
        let guard = state.lock().await;
        guard
            .pools_by_mint
            .iter()
            .flat_map(|(mint, pools)| {
                pools.pump.iter().flat_map(move |pump| {
                    pools
                        .dlmm
                        .iter()
                        .map(move |dlmm| (mint.clone(), pump.clone(), dlmm.clone()))
                })
            })
            .take(limit)
            .collect::<Vec<_>>()
    };
    info!("prewarming {} routes", routes.len());
    let mut ok = 0usize;
    let mut failed = 0usize;
    for (mint, pump, dlmm) in routes {
        let mut guard = state.lock().await;
        match route_for(&mut guard, &mint, &pump, &dlmm, process_delay_ms).await {
            Ok(_) => ok += 1,
            Err(e) => {
                failed += 1;
                warn!(
                    "prewarm route failed mint={} pump={} dlmm={}: {}",
                    mint, pump, dlmm, e
                );
            }
        }
    }
    info!("prewarm complete ok={} failed={}", ok, failed);
}

async fn fire_route(
    state: Arc<Mutex<ExecutorState>>,
    mint: String,
    pump: String,
    dlmms: Vec<String>,
    src_sig: String,
    src_slot: u64,
    volume_usd: f64,
    max_txs: u64,
    delay_ms: u64,
    refresh_on_send: bool,
    no_failure_mode: bool,
    parallel_sends: bool,
) {
    for i in 0..max_txs {
        let dlmm = dlmms[(i as usize) % dlmms.len()].clone();
        let (wallet, rpc_client, sending_rpc_clients, send_config, lookup_tables, route_config, mut pool_data, blockhash) = {
            let mut guard = state.lock().await;
            let route = match route_for(&mut guard, &mint, &pump, &dlmm, delay_ms).await {
                Ok(r) => r,
                Err(e) => {
                    error!("route init failed mint={} dlmm={}: {}", mint, dlmm, e);
                    return;
                }
            };
            (
                guard.wallet.clone(),
                guard.rpc_client.clone(),
                guard.sending_rpc_clients.clone(),
                guard.send_config.clone(),
                guard.lookup_tables.clone(),
                route.config.clone(),
                route.pool_data.clone(),
                guard.cached_blockhash,
            )
        };

        if refresh_on_send {
            let pool_refresher = PoolDataRefresher::new();
            if let Err(e) = pool_refresher.refresh_all_pools(&mut pool_data, &rpc_client, false) {
                error!("pool refresh failed mint={} dlmm={}: {}", mint, dlmm, e);
                return;
            }
        }
        info!(
            "SEND attempt={} mint={} vol=${:.0} src_slot={} src_sig={} pump={} dlmm={}",
            i + 1,
            mint,
            volume_usd,
            src_slot,
            src_sig,
            pump,
            dlmm
        );
        let tx = match build_versioned_transaction(
            &wallet,
            &route_config,
            &pool_data,
            blockhash,
            &lookup_tables,
            send_config.fast.as_ref(),
            no_failure_mode,
        ) {
            Ok(tx) => tx,
            Err(e) => {
                error!("build tx failed src_sig={}: {}", src_sig, e);
                return;
            }
        };

        if parallel_sends {
            let send_src_sig = src_sig.clone();
            tokio::spawn(async move {
                for (transport, res) in
                    send_signed_transaction(&sending_rpc_clients, &send_config, tx).await
                {
                    match res {
                        Ok(sig) => info!(
                            "sent transport={} sig={} for src_sig={}",
                            transport, sig, send_src_sig
                        ),
                        Err(e) => {
                            error!(
                                "send failed transport={} src_sig={}: {}",
                                transport, send_src_sig, e
                            );
                        }
                    }
                }
            });
        } else {
            for (transport, res) in
                send_signed_transaction(&sending_rpc_clients, &send_config, tx).await
            {
                match res {
                    Ok(sig) => info!(
                        "sent transport={} sig={} for src_sig={}",
                        transport, sig, src_sig
                    ),
                    Err(e) => {
                        error!(
                            "send failed transport={} src_sig={}: {}",
                            transport, src_sig, e
                        );
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).ok();

    let matches = App::new("FLASHX direct Rust executor")
        .arg(Arg::with_name("base-config").long("base-config").takes_value(true).required(true))
        .arg(Arg::with_name("geyser-url").long("geyser-url").takes_value(true).required(true))
        .arg(Arg::with_name("geyser-token").long("geyser-token").takes_value(true).default_value(""))
        .arg(Arg::with_name("pools-by-mint-file").long("pools-by-mint-file").takes_value(true).required(true))
        .arg(Arg::with_name("pump-reserves-file").long("pump-reserves-file").takes_value(true).default_value(""))
        .arg(Arg::with_name("pump-reserves-max-age-ms").long("pump-reserves-max-age-ms").takes_value(true).default_value("30000"))
        .arg(Arg::with_name("sol-usd").long("sol-usd").takes_value(true).default_value("180"))
        .arg(Arg::with_name("axiom-min-sol").long("axiom-min-sol").takes_value(true).default_value("0.01"))
        .arg(Arg::with_name("min-usd").long("min-usd").takes_value(true).default_value("50"))
        .arg(Arg::with_name("cluster-window-ms").long("cluster-window-ms").takes_value(true).default_value("10000"))
        .arg(Arg::with_name("cluster-min-usd").long("cluster-min-usd").takes_value(true).default_value("400"))
        .arg(Arg::with_name("cluster-min-count").long("cluster-min-count").takes_value(true).default_value("2"))
        .arg(Arg::with_name("cooldown-ms").long("cooldown-ms").takes_value(true).default_value("0"))
        .arg(Arg::with_name("max-dlmm").long("max-dlmm").takes_value(true).default_value("1"))
        .arg(Arg::with_name("max-txs").long("max-txs").takes_value(true).default_value("3"))
        .arg(Arg::with_name("delay-ms").long("delay-ms").takes_value(true).default_value("10"))
        .arg(Arg::with_name("parallel-sends").long("parallel-sends"))
        .arg(Arg::with_name("refresh-on-send").long("refresh-on-send"))
        .arg(Arg::with_name("no-failure-mode").long("no-failure-mode"))
        .arg(Arg::with_name("prewarm-routes").long("prewarm-routes"))
        .arg(Arg::with_name("prewarm-limit").long("prewarm-limit").takes_value(true).default_value("0"))
        .arg(Arg::with_name("rabbit-pool-filter").long("rabbit-pool-filter"))
        .arg(Arg::with_name("pump-program-filter").long("pump-program-filter"))
        .arg(Arg::with_name("trigger-on-pool-touch").long("trigger-on-pool-touch"))
        .arg(Arg::with_name("pool-touch-require-pump-program").long("pool-touch-require-pump-program"))
        .arg(Arg::with_name("pool-touch-cooldown-ms").long("pool-touch-cooldown-ms").takes_value(true).default_value("1000"))
        .arg(Arg::with_name("pool-touch-min-keys").long("pool-touch-min-keys").takes_value(true).default_value("0"))
        .arg(Arg::with_name("pool-touch-max-keys").long("pool-touch-max-keys").takes_value(true).default_value("0"))
        .arg(Arg::with_name("pool-touch-required-accounts").long("pool-touch-required-accounts").takes_value(true).default_value(""))
        .arg(Arg::with_name("pool-touch-reject-accounts").long("pool-touch-reject-accounts").takes_value(true).default_value(""))
        .arg(Arg::with_name("log-actionable-keys").long("log-actionable-keys"))
        .arg(Arg::with_name("max-filter-accounts").long("max-filter-accounts").takes_value(true).default_value("200"))
        .arg(Arg::with_name("send-rpc").long("send-rpc"))
        .arg(Arg::with_name("sender-provider").long("sender-provider").takes_value(true).default_value("circular"))
        .arg(Arg::with_name("fast-url").long("fast-url").takes_value(true).default_value(DEFAULT_FAST_URL))
        .arg(Arg::with_name("fast-api-key").long("fast-api-key").takes_value(true).default_value(""))
        .arg(Arg::with_name("cashback-address").long("cashback-address").takes_value(true).default_value(""))
        .arg(Arg::with_name("fast-tip-from").long("fast-tip-from").takes_value(true).default_value("1000000"))
        .arg(Arg::with_name("fast-tip-to").long("fast-tip-to").takes_value(true).default_value("1000000"))
        .arg(Arg::with_name("fast-timeout-ms").long("fast-timeout-ms").takes_value(true).default_value("700"))
        .arg(
            Arg::with_name("fast-tip-accounts")
                .long("fast-tip-accounts")
                .takes_value(true)
                .default_value(""),
        )
        .get_matches();

    let base_config_path = matches.value_of("base-config").unwrap();
    let geyser_url = matches.value_of("geyser-url").unwrap();
    let geyser_token = matches.value_of("geyser-token").unwrap();
    let pools_file = matches.value_of("pools-by-mint-file").unwrap();
    let pump_reserves_file = matches.value_of("pump-reserves-file").unwrap();
    let pump_reserves_max_age_ms: u128 = matches.value_of("pump-reserves-max-age-ms").unwrap().parse()?;
    let sol_usd: f64 = matches.value_of("sol-usd").unwrap().parse()?;
    let axiom_min_sol: f64 = matches.value_of("axiom-min-sol").unwrap().parse()?;
    let min_usd: f64 = matches.value_of("min-usd").unwrap().parse()?;
    let cluster_window_ms: u64 = matches.value_of("cluster-window-ms").unwrap().parse()?;
    let cluster_min_usd: f64 = matches.value_of("cluster-min-usd").unwrap().parse()?;
    let cluster_min_count: usize = matches.value_of("cluster-min-count").unwrap().parse()?;
    let cooldown_ms: u64 = matches.value_of("cooldown-ms").unwrap().parse()?;
    let max_dlmm: usize = matches.value_of("max-dlmm").unwrap().parse()?;
    let max_txs: u64 = matches.value_of("max-txs").unwrap().parse()?;
    let delay_ms: u64 = matches.value_of("delay-ms").unwrap().parse()?;
    let parallel_sends = matches.is_present("parallel-sends");
    let refresh_on_send = matches.is_present("refresh-on-send");
    let no_failure_mode = matches.is_present("no-failure-mode");
    let prewarm_routes_enabled = matches.is_present("prewarm-routes");
    let prewarm_limit: usize = matches.value_of("prewarm-limit").unwrap().parse()?;
    let rabbit_pool_filter = matches.is_present("rabbit-pool-filter");
    let pump_program_filter = matches.is_present("pump-program-filter");
    let trigger_on_pool_touch = matches.is_present("trigger-on-pool-touch");
    let pool_touch_require_pump_program = matches.is_present("pool-touch-require-pump-program");
    let pool_touch_cooldown_ms: u64 = matches.value_of("pool-touch-cooldown-ms").unwrap().parse()?;
    let pool_touch_min_keys: usize = matches.value_of("pool-touch-min-keys").unwrap().parse()?;
    let pool_touch_max_keys: usize = matches.value_of("pool-touch-max-keys").unwrap().parse()?;
    let pool_touch_required_accounts =
        parse_csv_strings(matches.value_of("pool-touch-required-accounts").unwrap());
    let pool_touch_reject_accounts =
        parse_csv_strings(matches.value_of("pool-touch-reject-accounts").unwrap());
    let log_actionable_keys = matches.is_present("log-actionable-keys");
    let max_filter_accounts: usize = matches.value_of("max-filter-accounts").unwrap().parse()?;
    let send_rpc = matches.is_present("send-rpc");
    let sender_provider = match matches.value_of("sender-provider").unwrap() {
        "circular" => FastSenderProvider::Circular,
        "helius" => FastSenderProvider::Helius,
        other => anyhow::bail!("unsupported sender-provider {}; use circular or helius", other),
    };
    let fast_url_arg = matches.value_of("fast-url").unwrap();
    let fast_api_key = matches.value_of("fast-api-key").unwrap();
    let mut fast_url = if sender_provider == FastSenderProvider::Helius
        && fast_url_arg == DEFAULT_FAST_URL
    {
        DEFAULT_HELIUS_SENDER_URL.to_string()
    } else {
        fast_url_arg.to_string()
    };
    if sender_provider == FastSenderProvider::Helius
        && !fast_api_key.is_empty()
        && !fast_url.contains("api-key=")
    {
        let sep = if fast_url.contains('?') { '&' } else { '?' };
        fast_url = format!("{}{}api-key={}", fast_url, sep, fast_api_key);
    }
    let cashback_address_arg = matches.value_of("cashback-address").unwrap();
    let fast_tip_from: u64 = matches.value_of("fast-tip-from").unwrap().parse()?;
    let fast_tip_to: u64 = matches.value_of("fast-tip-to").unwrap().parse()?;
    let fast_timeout_ms: u64 = matches.value_of("fast-timeout-ms").unwrap().parse()?;
    let fast_tip_accounts_arg = matches.value_of("fast-tip-accounts").unwrap();
    let fast_tip_accounts_raw = if fast_tip_accounts_arg.is_empty() {
        match sender_provider {
            FastSenderProvider::Circular => CIRCULAR_TIP_ACCOUNTS,
            FastSenderProvider::Helius => HELIUS_TIP_ACCOUNTS,
        }
    } else {
        fast_tip_accounts_arg
    };

    let base_config = Config::load(base_config_path)?;
    let rpc_client = Arc::new(RpcClient::new(base_config.rpc.url.clone()));
    let sending_rpc_clients = if let Some(spam_config) = &base_config.spam {
        if spam_config.enabled {
            spam_config
                .sending_rpc_urls
                .iter()
                .map(|url| Arc::new(RpcClient::new(url.clone())))
                .collect::<Vec<_>>()
        } else {
            vec![rpc_client.clone()]
        }
    } else {
        vec![rpc_client.clone()]
    };

    let wallet = Arc::new(load_keypair(&base_config.wallet.private_key)?);
    info!("wallet={}", wallet.pubkey());
    ensure_base_atas_exist(&rpc_client, &wallet)?;

    let fast = if sender_provider == FastSenderProvider::Circular && fast_api_key.is_empty() {
        None
    } else {
        let cashback_address = if cashback_address_arg.is_empty() {
            wallet.pubkey().to_string()
        } else {
            cashback_address_arg.to_string()
        };
        Some(FastSender {
            provider: sender_provider,
            client: reqwest::Client::builder()
                .timeout(Duration::from_millis(fast_timeout_ms))
                .build()?,
            url: fast_url.clone(),
            api_key: fast_api_key.to_string(),
            cashback_address,
            tip_accounts: parse_pubkey_list(fast_tip_accounts_raw)?,
            tip_from: fast_tip_from,
            tip_to: fast_tip_to,
            timeout_ms: fast_timeout_ms,
        })
    };
    let send_config = SendConfig { fast, send_rpc };
    let fast_url_log = if fast_url.contains("api-key=") {
        fast_url
            .split("api-key=")
            .next()
            .map(|prefix| format!("{}api-key=<redacted>", prefix))
            .unwrap_or_else(|| "<redacted>".to_string())
    } else {
        fast_url.clone()
    };
    info!(
        "senders provider={:?} fast={} rpc={} fast_url={} fast_tip={}..{} axiom_min_sol={:.6}",
        sender_provider,
        send_config.fast.is_some(),
        send_config.send_rpc,
        fast_url_log,
        fast_tip_from,
        fast_tip_to,
        axiom_min_sol
    );

    let mut lut_addresses = base_config
        .routing
        .markets
        .lookup_table_accounts
        .clone()
        .unwrap_or_default();
    lut_addresses.push(SMB_LUT.to_string());
    let lookup_tables = load_luts(&rpc_client, &lut_addresses);
    let initial_blockhash = rpc_client.get_latest_blockhash()?;

    let pools_by_mint = load_pools_by_mint(pools_file, max_dlmm)?;
    let pools_mtime = file_mtime(pools_file);
    info!("loaded {} actionable mints from {}", pools_by_mint.len(), pools_file);
    let pump_reserves_by_pool = load_pump_reserves(pump_reserves_file).unwrap_or_else(|e| {
        if !pump_reserves_file.is_empty() {
            warn!("failed to load pump reserves from {}: {}", pump_reserves_file, e);
        }
        HashMap::new()
    });
    let pump_reserves_mtime = file_mtime(pump_reserves_file);
    if !pump_reserves_file.is_empty() {
        info!(
            "loaded {} pump reserves from {} max_age_ms={}",
            pump_reserves_by_pool.len(),
            pump_reserves_file,
            pump_reserves_max_age_ms
        );
    }
    let subscribe_account_include =
        subscription_accounts(&pools_by_mint, rabbit_pool_filter, pump_program_filter, max_filter_accounts);
    info!(
        "stream filter accounts={} rabbit_pool_filter={} pump_program_filter={} trigger_on_pool_touch={}",
        subscribe_account_include.len(),
        rabbit_pool_filter,
        pump_program_filter,
        trigger_on_pool_touch
    );

    let state = Arc::new(Mutex::new(ExecutorState {
        base_config,
        rpc_client,
        sending_rpc_clients,
        send_config,
        wallet,
        lookup_tables,
        cached_blockhash: initial_blockhash,
        pools_file: pools_file.to_string(),
        pools_mtime,
        pump_reserves_file: pump_reserves_file.to_string(),
        pump_reserves_mtime,
        pump_reserves_max_age_ms,
        max_dlmm,
        pools_by_mint,
        pump_reserves_by_pool,
        route_cache: HashMap::new(),
        flash_by_mint: HashMap::new(),
        last_signal_by_mint: HashMap::new(),
        last_signal_by_mint_slot: HashMap::new(),
        last_pool_touch_by_mint: HashMap::new(),
        pool_refresher: PoolDataRefresher::new(),
    }));

    if prewarm_routes_enabled {
        let route_count = {
            let guard = state.lock().await;
            let total = guard
                .pools_by_mint
                .values()
                .map(|pools| pools.pump.len() * pools.dlmm.len())
                .sum::<usize>();
            if prewarm_limit == 0 {
                total
            } else {
                total.min(prewarm_limit)
            }
        };
        prewarm_routes(state.clone(), route_count, delay_ms).await;
    }

    {
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                let rpc_client = {
                    let guard = state.lock().await;
                    guard.rpc_client.clone()
                };
                match rpc_client.get_latest_blockhash() {
                    Ok(blockhash) => {
                        let mut guard = state.lock().await;
                        guard.cached_blockhash = blockhash;
                    }
                    Err(e) => error!("blockhash refresh failed: {}", e),
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        });
    }

    loop {
        info!("connecting geyser {}", geyser_url);
        let mut client = GeyserGrpcClient::build_from_shared(geyser_url.to_string())?
            .x_token(if geyser_token.is_empty() {
                None
            } else {
                Some(geyser_token.to_string())
            })?
            .max_decoding_message_size(64 * 1024 * 1024)
            .connect()
            .await?;

        let (mut tx, mut stream) = client.subscribe().await?;
        let mut transactions = HashMap::new();
        transactions.insert(
            "flashx".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: subscribe_account_include.clone(),
                account_exclude: vec![],
                account_required: vec![],
                ..Default::default()
            },
        );
        tx.send(SubscribeRequest {
            transactions,
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        })
        .await?;
        info!(
            "subscribed FLASHX min_tx_usd={} cluster_min_usd={} cluster_min_count={} window_ms={} max_txs={} delay_ms={}",
            min_usd, cluster_min_usd, cluster_min_count, cluster_window_ms, max_txs, delay_ms
        );

        while let Some(update) = stream.next().await {
            let update = match update {
                Ok(u) => u,
                Err(e) => {
                    error!("geyser stream error: {}", e);
                    break;
                }
            };
            let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else {
                continue;
            };
            let Some(info_tx) = tx_update.transaction else {
                continue;
            };
            let Some(txn) = info_tx.transaction else {
                continue;
            };
            let meta = info_tx.meta.as_ref();
            if meta.is_some_and(|m| m.err.is_some()) {
                continue;
            }
            let sig = bs58::encode(info_tx.signature).into_string();
            let keys = account_keys(&txn, meta);
            let has_flashx = keys.iter().any(|k| k == FLASHX);
            let has_pump_program = keys.iter().any(|k| k == PUMP_AMM_PROGRAM);
            if !has_flashx && !trigger_on_pool_touch {
                continue;
            }
            let is_pool_touch_candidate = trigger_on_pool_touch && !has_flashx;
            if is_pool_touch_candidate {
                if pool_touch_require_pump_program && !has_pump_program {
                    continue;
                }
                if pool_touch_min_keys > 0 && keys.len() < pool_touch_min_keys {
                    continue;
                }
                if pool_touch_max_keys > 0 && keys.len() > pool_touch_max_keys {
                    continue;
                }
                if !pool_touch_required_accounts.is_empty()
                    && !pool_touch_required_accounts.iter().all(|required| keys.contains(required))
                {
                    continue;
                }
                if pool_touch_reject_accounts.iter().any(|rejected| keys.contains(rejected)) {
                    continue;
                }
            }
            let (volume_usd, mints, candidates) = {
                let mut guard = state.lock().await;
                reload_pools_if_changed(&mut guard);
                reload_pump_reserves_if_changed(&mut guard);
                if has_flashx {
                    let axiom_swaps = axiom_swap_signals(
                        &txn,
                        meta,
                        &keys,
                        &guard.pools_by_mint,
                        &guard.pump_reserves_by_pool,
                        guard.pump_reserves_max_age_ms,
                        sol_usd,
                        axiom_min_sol,
                    );
                    let mints = axiom_swaps
                        .iter()
                        .map(|swap| swap.mint.clone())
                        .collect::<Vec<_>>();
                    let candidates =
                        signal_candidates_from_axiom_swaps(&guard.pools_by_mint, &axiom_swaps);
                    let volume_usd = candidates
                        .iter()
                        .map(|(_, _, _, volume_usd, _, _, _)| *volume_usd)
                        .fold(0.0, f64::max);
                    (volume_usd, mints, candidates)
                } else if trigger_on_pool_touch {
                    let candidates = signal_candidates_from_keys(&guard.pools_by_mint, &keys);
                    let mints = candidates.iter().map(|(mint, _, _)| mint.clone()).collect::<Vec<_>>();
                    let candidates = candidates
                        .into_iter()
                        .map(|(mint, pump, dlmms)| {
                            (mint, pump, dlmms, min_usd.max(1.0), "pool_touch", 0.0, "pool_touch")
                        })
                        .collect::<Vec<_>>();
                    (min_usd.max(1.0), mints, candidates)
                } else if let Some(meta) = meta {
                    let volume_usd = estimate_wsol_volume_usd(meta, sol_usd);
                    if volume_usd < min_usd {
                        continue;
                    }
                    let mints = token_balance_mints(meta);
                    let candidates = signal_candidates(&guard.pools_by_mint, &keys, &mints);
                    let candidates = candidates
                        .into_iter()
                        .map(|(mint, pump, dlmms)| {
                            (mint, pump, dlmms, volume_usd, "meta", 0.0, "meta_wsol_tx")
                        })
                        .collect::<Vec<_>>();
                    (volume_usd, mints, candidates)
                } else {
                    let candidates = signal_candidates_from_keys(&guard.pools_by_mint, &keys);
                    let mints = candidates.iter().map(|(mint, _, _)| mint.clone()).collect::<Vec<_>>();
                    let candidates = candidates
                        .into_iter()
                        .map(|(mint, pump, dlmms)| {
                            (mint, pump, dlmms, min_usd.max(1.0), "keys", 0.0, "keys")
                        })
                        .collect::<Vec<_>>();
                    (min_usd.max(1.0), mints, candidates)
                }
            };

            if candidates.is_empty() {
                info!(
                    "WATCH_ONLY vol=${:.0} mints={:?} keys={} meta={} flashx={} sig={}",
                    volume_usd,
                    mints,
                    keys.len(),
                    meta.is_some(),
                    has_flashx,
                    sig
                );
                continue;
            }

            for (mint, pump, dlmms, candidate_volume_usd, source, sol_amount, volume_source) in candidates {
                if source == "sell" && sol_amount > 0.0 && sol_amount < axiom_min_sol {
                    info!(
                        "AXIOM_FILTERED_LATE mint={} signal={} sol={:.6} vol=${:.0} volume_source={} min_sol={:.6} sig={}",
                        mint,
                        source,
                        sol_amount,
                        candidate_volume_usd,
                        volume_source,
                        axiom_min_sol,
                        sig
                    );
                    continue;
                }
                let should_fire = {
                    let mut guard = state.lock().await;
                    let now = Instant::now();
                    guard
                        .last_signal_by_mint_slot
                        .retain(|_, at| now.duration_since(*at) <= Duration::from_secs(3));

                    let rows = guard.flash_by_mint.entry(mint.clone()).or_default();
                    rows.retain(|r| now.duration_since(r.at) <= Duration::from_millis(cluster_window_ms));
                    rows.push(FlashEvent {
                        at: now,
                        volume_usd: candidate_volume_usd,
                    });
                    let cluster_usd = rows.iter().map(|r| r.volume_usd).sum::<f64>();
                    let cluster_count = rows.len();
                    if cluster_count < cluster_min_count || cluster_usd < cluster_min_usd {
                        info!(
                            "WATCH_CLUSTER mint={} vol=${:.0} cluster=${:.0} count={} need=${:.0}/{} sig={}",
                            mint,
                            volume_usd,
                            cluster_usd,
                            cluster_count,
                            cluster_min_usd,
                            cluster_min_count,
                            sig
                        );
                        false
                    } else {
                        let mint_slot_key = format!("{}:{}", mint, tx_update.slot);
                        if guard.last_signal_by_mint_slot.contains_key(&mint_slot_key) {
                            false
                        } else {
                            if is_pool_touch_candidate {
                                if let Some(last) = guard.last_pool_touch_by_mint.get(&mint).copied() {
                                    if now.duration_since(last) < Duration::from_millis(pool_touch_cooldown_ms) {
                                        false
                                    } else {
                                        guard.last_pool_touch_by_mint.insert(mint.clone(), now);
                                        guard.last_signal_by_mint.insert(mint.clone(), now);
                                        guard.last_signal_by_mint_slot.insert(mint_slot_key, now);
                                        true
                                    }
                                } else {
                                    guard.last_pool_touch_by_mint.insert(mint.clone(), now);
                                    guard.last_signal_by_mint.insert(mint.clone(), now);
                                    guard.last_signal_by_mint_slot.insert(mint_slot_key, now);
                                    true
                                }
                            } else {
                            let last = guard.last_signal_by_mint.get(&mint).copied();
                            if let Some(last) = last {
                                if now.duration_since(last) < Duration::from_millis(cooldown_ms) {
                                    false
                                } else {
                                    guard.last_signal_by_mint.insert(mint.clone(), now);
                                    guard.last_signal_by_mint_slot.insert(mint_slot_key, now);
                                    true
                                }
                            } else {
                                guard.last_signal_by_mint.insert(mint.clone(), now);
                                guard.last_signal_by_mint_slot.insert(mint_slot_key, now);
                                true
                            }
                            }
                        }
                    }
                };
                if !should_fire {
                    continue;
                }
                info!(
                    "ACTIONABLE_FLASHX mint={} signal={} sol={:.6} vol=${:.0} volume_source={} slot={} pump={} dlmms={} meta={} flashx={} sig={}",
                    mint,
                    source,
                    sol_amount,
                    candidate_volume_usd,
                    volume_source,
                    tx_update.slot,
                    pump,
                    dlmms.len(),
                    meta.is_some(),
                    has_flashx,
                    sig
                );
                if log_actionable_keys {
                    info!(
                        "ACTIONABLE_KEYS mint={} slot={} keys={:?}",
                        mint,
                        tx_update.slot,
                        keys
                    );
                }
                tokio::spawn(fire_route(
                    state.clone(),
                    mint,
                    pump,
                    dlmms,
                    sig.clone(),
                    tx_update.slot,
                    volume_usd,
                    max_txs,
                    delay_ms,
                    refresh_on_send,
                    no_failure_mode,
                    parallel_sends,
                ));
            }
        }

        warn!("geyser disconnected, reconnecting in 500ms");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
