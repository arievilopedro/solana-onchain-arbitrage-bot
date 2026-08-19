use clap::{App, Arg};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_onchain_arbitrage_bot::dex::meteora::constants::dlmm_program_id;
use solana_onchain_arbitrage_bot::dex::meteora::dlmm_info::DlmmInfo;
use solana_onchain_arbitrage_bot::dex::pump::constants::pump_program_id;
use solana_onchain_arbitrage_bot::dex::pump::PumpAmmInfo;
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const FLASHX: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";
const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
const DLMM_TOKEN_X_MINT_OFFSET: usize = 96;
const DLMM_TOKEN_Y_MINT_OFFSET: usize = 128;

#[derive(Debug)]
struct DiscoveredPool {
    mint: String,
    side: &'static str,
    pool: String,
}

#[derive(Debug)]
struct PumpReserve {
    pool: String,
    mint: String,
    token_reserve: u64,
    sol_reserve: u64,
}

fn pubkey_bytes_to_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 32 {
        return None;
    }
    Some(bs58::encode(bytes).into_string())
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

fn now_seen() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn load_json(path: &str) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}))
}

fn atomic_write_json(path: &str, value: &Value) -> anyhow::Result<()> {
    let tmp = format!("{}.tmp", path);
    let raw = serde_json::to_string_pretty(value)?;
    fs::write(&tmp, raw)?;
    if cfg!(windows) && Path::new(path).exists() {
        let _ = fs::remove_file(path);
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn ensure_object(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_object() {
        *value = json!({});
    }
    value.as_object_mut().unwrap()
}

fn upsert_pool(root: &mut Value, pool: &DiscoveredPool) -> bool {
    let root_obj = ensure_object(root);
    let mint_rec = root_obj
        .entry(pool.mint.clone())
        .or_insert_with(|| json!({"pump": {}, "dlmm": {}}));
    let mint_obj = ensure_object(mint_rec);
    let side_rec = mint_obj
        .entry(pool.side.to_string())
        .or_insert_with(|| json!({}));
    let side_obj = ensure_object(side_rec);
    let existed = side_obj.contains_key(&pool.pool);
    let rec = side_obj
        .entry(pool.pool.clone())
        .or_insert_with(|| json!({"count": 0, "last_seen": ""}));
    let rec_obj = ensure_object(rec);
    let count = rec_obj.get("count").and_then(Value::as_u64).unwrap_or(0);
    rec_obj.insert("count".to_string(), json!(count + 1));
    rec_obj.insert("last_seen".to_string(), json!(now_seen()));
    !existed
}

fn known_mints(root: &Value, limit: usize) -> Vec<String> {
    let Some(obj) = root.as_object() else {
        return Vec::new();
    };
    let mut rows = obj
        .iter()
        .filter_map(|(mint, rec)| {
            if Pubkey::from_str(mint).is_err() {
                return None;
            }
            let pump_count = rec
                .get("pump")
                .and_then(Value::as_object)
                .map(|pools| {
                    pools
                        .values()
                        .map(|meta| meta.get("count").and_then(Value::as_u64).unwrap_or(0))
                        .sum::<u64>()
                })
                .unwrap_or(0);
            let dlmm_count = rec
                .get("dlmm")
                .and_then(Value::as_object)
                .map(|pools| {
                    pools
                        .values()
                        .map(|meta| meta.get("count").and_then(Value::as_u64).unwrap_or(0))
                        .sum::<u64>()
                })
                .unwrap_or(0);
            Some((mint.clone(), pump_count.saturating_add(dlmm_count)))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    rows.into_iter()
        .take(if limit == 0 { usize::MAX } else { limit })
        .map(|(mint, _)| mint)
        .collect()
}

fn token_account_amount(data: &[u8]) -> Option<u64> {
    let amount = data.get(64..72)?;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(amount);
    Some(u64::from_le_bytes(buf))
}

fn upsert_pump_reserve(root: &mut Value, reserve: &PumpReserve) {
    let root_obj = ensure_object(root);
    root_obj.insert(
        reserve.pool.clone(),
        json!({
            "mint": reserve.mint,
            "token_reserve": reserve.token_reserve,
            "sol_reserve": reserve.sol_reserve,
            "updated_ms": now_ms().to_string(),
        }),
    );
}

fn mint_for_pair(a: &Pubkey, b: &Pubkey) -> Option<String> {
    let wsol = Pubkey::from_str(WSOL).ok()?;
    if a == &wsol && b != &wsol {
        Some(b.to_string())
    } else if b == &wsol && a != &wsol {
        Some(a.to_string())
    } else {
        None
    }
}

fn validate_account(pool: &str, owner: &Pubkey, data: &[u8]) -> Option<DiscoveredPool> {
    let pump_program = pump_program_id();
    let dlmm_program = dlmm_program_id();
    if owner == &pump_program {
        if data.len() < 8 {
            return None;
        }
        let info = PumpAmmInfo::load_checked(data).ok()?;
        let mint = mint_for_pair(&info.base_mint, &info.quote_mint)?;
        return Some(DiscoveredPool {
            mint,
            side: "pump",
            pool: pool.to_string(),
        });
    }
    if owner == &dlmm_program {
        if data.len() < 8 {
            return None;
        }
        let info = DlmmInfo::load_checked(data).ok()?;
        let mint = mint_for_pair(&info.token_x_mint, &info.token_y_mint)?;
        return Some(DiscoveredPool {
            mint,
            side: "dlmm",
            pool: pool.to_string(),
        });
    }
    None
}

fn candidate_pool_keys(
    tx: &yellowstone_grpc_proto::solana::storage::confirmed_block::Transaction,
    keys: &[String],
    max_accounts: usize,
    full_scan: bool,
) -> Vec<String> {
    let Some(msg) = &tx.message else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for ix in &msg.instructions {
        let program = keys
            .get(ix.program_id_index as usize)
            .map(String::as_str)
            .unwrap_or_default();
        if program == PUMP_AMM_PROGRAM {
            if let Some(pool) = ix.accounts.get(0).and_then(|idx| keys.get(*idx as usize)) {
                if seen.insert(pool.clone()) {
                    out.push(pool.clone());
                }
            }
        } else if program == DLMM_PROGRAM || full_scan {
            for account_idx in &ix.accounts {
                if out.len() >= max_accounts {
                    return out;
                }
                if let Some(account) = keys.get(*account_idx as usize) {
                    if seen.insert(account.clone()) {
                        out.push(account.clone());
                    }
                }
            }
        }
        if out.len() >= max_accounts {
            return out;
        }
    }
    out
}

async fn validate_candidates(
    rpc: Arc<RpcClient>,
    candidates: Vec<String>,
) -> anyhow::Result<(Vec<DiscoveredPool>, Vec<PumpReserve>)> {
    task::spawn_blocking(move || {
        let keyed_candidates = candidates
            .iter()
            .filter_map(|s| Pubkey::from_str(s).ok().map(|p| (s.clone(), p)))
            .collect::<Vec<_>>();
        let pubkeys = keyed_candidates.iter().map(|(_, p)| *p).collect::<Vec<_>>();
        let accounts = rpc.get_multiple_accounts(&pubkeys)?;
        let mut out = Vec::new();
        let mut reserves = Vec::new();
        for ((pool, pubkey), account) in keyed_candidates.iter().zip(accounts) {
            let Some(account) = account else {
                continue;
            };
            if let Some(found) = validate_account(pool, &account.owner, &account.data) {
                if found.side == "pump" {
                    if let Ok(info) = PumpAmmInfo::load_checked(&account.data) {
                        let wsol = Pubkey::from_str(WSOL)?;
                        if let Some(mint) = mint_for_pair(&info.base_mint, &info.quote_mint) {
                            let (token_vault, sol_vault) = if info.base_mint == wsol {
                                (info.pool_quote_token_account, info.pool_base_token_account)
                            } else {
                                (info.pool_base_token_account, info.pool_quote_token_account)
                            };
                            if let Ok(vault_accounts) =
                                rpc.get_multiple_accounts(&[token_vault, sol_vault])
                            {
                                if let (Some(Some(token_account)), Some(Some(sol_account))) =
                                    (vault_accounts.get(0), vault_accounts.get(1))
                                {
                                    if let (Some(token_reserve), Some(sol_reserve)) = (
                                        token_account_amount(&token_account.data),
                                        token_account_amount(&sol_account.data),
                                    ) {
                                        reserves.push(PumpReserve {
                                            pool: pool.to_string(),
                                            mint,
                                            token_reserve,
                                            sol_reserve,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                if pubkey.to_string() == found.pool {
                    out.push(found);
                }
            }
        }
        anyhow::Ok((out, reserves))
    })
    .await?
}

fn memcmp_pubkey(offset: usize, pubkey: &Pubkey) -> RpcFilterType {
    RpcFilterType::Memcmp(Memcmp::new_raw_bytes(offset, pubkey.to_bytes().to_vec()))
}

fn dlmm_program_accounts_config(x_mint: &Pubkey, y_mint: &Pubkey) -> RpcProgramAccountsConfig {
    RpcProgramAccountsConfig {
        filters: Some(vec![
            memcmp_pubkey(DLMM_TOKEN_X_MINT_OFFSET, x_mint),
            memcmp_pubkey(DLMM_TOKEN_Y_MINT_OFFSET, y_mint),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn discover_dlmm_pools_for_mints(
    rpc: Arc<RpcClient>,
    mints: Vec<String>,
    max_per_mint: usize,
) -> anyhow::Result<Vec<DiscoveredPool>> {
    task::spawn_blocking(move || {
        let program = dlmm_program_id();
        let wsol = Pubkey::from_str(WSOL)?;
        let mut out = Vec::new();
        for mint_raw in mints {
            let Ok(mint) = Pubkey::from_str(&mint_raw) else {
                continue;
            };
            let mut seen = HashSet::new();
            let configs = [
                dlmm_program_accounts_config(&mint, &wsol),
                dlmm_program_accounts_config(&wsol, &mint),
            ];
            for config in configs {
                let accounts = match rpc.get_program_accounts_with_config(&program, config) {
                    Ok(accounts) => accounts,
                    Err(e) => {
                        warn!("dlmm discovery failed mint={}: {}", mint_raw, e);
                        continue;
                    }
                };
                for (pool_pubkey, account) in accounts {
                    if max_per_mint > 0 && seen.len() >= max_per_mint {
                        break;
                    }
                    let pool = pool_pubkey.to_string();
                    if !seen.insert(pool.clone()) {
                        continue;
                    }
                    let Some(found) = validate_account(&pool, &account.owner, &account.data) else {
                        continue;
                    };
                    if found.side == "dlmm" && found.mint == mint_raw {
                        out.push(found);
                    }
                }
            }
        }
        anyhow::Ok(out)
    })
    .await?
}

fn parse_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let matches = App::new("pools_by_mint_updater")
        .arg(Arg::with_name("geyser-url").long("geyser-url").takes_value(true).required(true))
        .arg(Arg::with_name("geyser-token").long("geyser-token").takes_value(true).default_value(""))
        .arg(Arg::with_name("rpc-url").long("rpc-url").takes_value(true).required(true))
        .arg(Arg::with_name("pools-by-mint-file").long("pools-by-mint-file").takes_value(true).required(true))
        .arg(Arg::with_name("pump-reserves-file").long("pump-reserves-file").takes_value(true).default_value(""))
        .arg(Arg::with_name("include-accounts").long("include-accounts").takes_value(true).default_value(""))
        .arg(Arg::with_name("max-candidate-accounts").long("max-candidate-accounts").takes_value(true).default_value("80"))
        .arg(Arg::with_name("write-interval-ms").long("write-interval-ms").takes_value(true).default_value("500"))
        .arg(Arg::with_name("dlmm-discovery-interval-ms").long("dlmm-discovery-interval-ms").takes_value(true).default_value("60000"))
        .arg(Arg::with_name("dlmm-discovery-max-mints").long("dlmm-discovery-max-mints").takes_value(true).default_value("300"))
        .arg(Arg::with_name("dlmm-discovery-max-per-mint").long("dlmm-discovery-max-per-mint").takes_value(true).default_value("8"))
        .arg(Arg::with_name("full-scan").long("full-scan"))
        .get_matches();

    let geyser_url = matches.value_of("geyser-url").unwrap();
    let geyser_token = matches.value_of("geyser-token").unwrap();
    let rpc = Arc::new(RpcClient::new(matches.value_of("rpc-url").unwrap().to_string()));
    let pools_file = matches.value_of("pools-by-mint-file").unwrap().to_string();
    let pump_reserves_file = matches.value_of("pump-reserves-file").unwrap().to_string();
    let max_candidate_accounts = matches
        .value_of("max-candidate-accounts")
        .unwrap()
        .parse::<usize>()?;
    let write_interval_ms = matches
        .value_of("write-interval-ms")
        .unwrap()
        .parse::<u64>()?;
    let dlmm_discovery_interval_ms = matches
        .value_of("dlmm-discovery-interval-ms")
        .unwrap()
        .parse::<u64>()?;
    let dlmm_discovery_max_mints = matches
        .value_of("dlmm-discovery-max-mints")
        .unwrap()
        .parse::<usize>()?;
    let dlmm_discovery_max_per_mint = matches
        .value_of("dlmm-discovery-max-per-mint")
        .unwrap()
        .parse::<usize>()?;
    let full_scan = matches.is_present("full-scan");

    let mut include_accounts = vec![
        FLASHX.to_string(),
        PUMP_AMM_PROGRAM.to_string(),
        DLMM_PROGRAM.to_string(),
    ];
    include_accounts.extend(parse_csv(matches.value_of("include-accounts").unwrap()));
    include_accounts.sort();
    include_accounts.dedup();

    let pools_json = Arc::new(Mutex::new(load_json(&pools_file)));
    let pump_reserves_json = Arc::new(Mutex::new(if pump_reserves_file.is_empty() {
        json!({})
    } else {
        load_json(&pump_reserves_file)
    }));
    let dirty = Arc::new(Mutex::new(false));
    {
        let pools_json = pools_json.clone();
        let pump_reserves_json = pump_reserves_json.clone();
        let dirty = dirty.clone();
        let pools_file = pools_file.clone();
        let pump_reserves_file = pump_reserves_file.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(write_interval_ms)).await;
                let should_write = {
                    let mut guard = dirty.lock().unwrap();
                    let changed = *guard;
                    *guard = false;
                    changed
                };
                if !should_write {
                    continue;
                }
                let snapshot = pools_json.lock().unwrap().clone();
                let pump_reserves_snapshot = pump_reserves_json.lock().unwrap().clone();
                if let Err(e) = atomic_write_json(&pools_file, &snapshot) {
                    error!("failed writing {}: {}", pools_file, e);
                } else {
                    info!("updated {}", pools_file);
                }
                if !pump_reserves_file.is_empty() {
                    if let Err(e) =
                        atomic_write_json(&pump_reserves_file, &pump_reserves_snapshot)
                    {
                        error!("failed writing {}: {}", pump_reserves_file, e);
                    } else {
                        info!("updated {}", pump_reserves_file);
                    }
                }
            }
        });
    }
    if dlmm_discovery_interval_ms > 0 {
        let pools_json = pools_json.clone();
        let dirty = dirty.clone();
        let rpc = rpc.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(dlmm_discovery_interval_ms)).await;
                let mints = {
                    let root = pools_json.lock().unwrap();
                    known_mints(&root, dlmm_discovery_max_mints)
                };
                if mints.is_empty() {
                    continue;
                }
                match discover_dlmm_pools_for_mints(
                    rpc.clone(),
                    mints.clone(),
                    dlmm_discovery_max_per_mint,
                )
                .await
                {
                    Ok(discovered) => {
                        if discovered.is_empty() {
                            info!("DLMM_DISCOVERY mints={} discovered=0 added=0", mints.len());
                            continue;
                        }
                        let mut added = 0usize;
                        {
                            let mut root = pools_json.lock().unwrap();
                            for pool in &discovered {
                                if upsert_pool(&mut root, pool) {
                                    added += 1;
                                }
                            }
                        }
                        if added > 0 {
                            *dirty.lock().unwrap() = true;
                        }
                        info!(
                            "DLMM_DISCOVERY mints={} discovered={} added={}",
                            mints.len(),
                            discovered.len(),
                            added
                        );
                    }
                    Err(e) => warn!("DLMM_DISCOVERY failed: {}", e),
                }
            }
        });
    }

    info!(
        "updater loaded {}; subscribing accounts={} full_scan={} max_candidate_accounts={} dlmm_discovery_interval_ms={} dlmm_discovery_max_mints={} dlmm_discovery_max_per_mint={}",
        pools_file,
        include_accounts.len(),
        full_scan,
        max_candidate_accounts,
        dlmm_discovery_interval_ms,
        dlmm_discovery_max_mints,
        dlmm_discovery_max_per_mint
    );

    let mut last_seen_sig = HashMap::<String, Instant>::new();
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
            "pool-discovery".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: include_accounts.clone(),
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
        info!("subscribed pool discovery");

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
            if info_tx.meta.as_ref().is_some_and(|m| m.err.is_some()) {
                continue;
            }
            let sig = bs58::encode(info_tx.signature).into_string();
            let now = Instant::now();
            last_seen_sig.retain(|_, at| now.duration_since(*at) <= Duration::from_secs(10));
            if last_seen_sig.insert(sig.clone(), now).is_some() {
                continue;
            }

            let keys = account_keys(&txn, info_tx.meta.as_ref());
            let candidates = candidate_pool_keys(&txn, &keys, max_candidate_accounts, full_scan);
            if candidates.is_empty() {
                continue;
            }

            let (discovered, reserves) = match validate_candidates(rpc.clone(), candidates).await {
                Ok(rows) => rows,
                Err(e) => {
                    warn!("pool validation failed sig={}: {}", sig, e);
                    continue;
                }
            };
            if discovered.is_empty() && reserves.is_empty() {
                continue;
            }

            let mut added = 0usize;
            {
                let mut root = pools_json.lock().unwrap();
                for pool in &discovered {
                    if upsert_pool(&mut root, pool) {
                        added += 1;
                    }
                }
            }
            if !pump_reserves_file.is_empty() && !reserves.is_empty() {
                let mut root = pump_reserves_json.lock().unwrap();
                for reserve in &reserves {
                    upsert_pump_reserve(&mut root, reserve);
                }
            }
            *dirty.lock().unwrap() = true;
            info!(
                "POOL_UPDATE slot={} sig={} discovered={} reserves={} added={}",
                tx_update.slot,
                sig,
                discovered.len(),
                reserves.len(),
                added
            );
            for pool in discovered {
                info!("  {} mint={} pool={}", pool.side, pool.mint, pool.pool);
            }
        }

        warn!("geyser disconnected, reconnecting in 500ms");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
