use anyhow::Context;
use clap::{App, Arg};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
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

#[derive(Clone, Debug)]
struct MintPools {
    pump: Vec<String>,
    dlmm: Vec<String>,
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

fn token_amount(amount: &str, decimals: u32) -> f64 {
    let raw = amount.parse::<f64>().unwrap_or(0.0);
    raw / 10f64.powi(decimals as i32)
}

fn estimate_wsol_volume_usd(
    meta: &yellowstone_grpc_proto::solana::storage::confirmed_block::TransactionStatusMeta,
    sol_usd: f64,
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
    pre.keys()
        .chain(post.keys())
        .map(|idx| {
            let a = pre.get(idx).copied().unwrap_or(0.0);
            let b = post.get(idx).copied().unwrap_or(0.0);
            (b - a).abs()
        })
        .fold(0.0, f64::max)
        * sol_usd
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

fn program_ids(
    tx: &yellowstone_grpc_proto::solana::storage::confirmed_block::Transaction,
    keys: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    let Some(msg) = &tx.message else {
        return out;
    };
    for ix in &msg.instructions {
        let idx = ix.program_id_index as usize;
        if let Some(program) = keys.get(idx) {
            if !out.contains(program) {
                out.push(program.clone());
            }
        }
    }
    out
}

fn candidates_from_keys(
    pools_by_mint: &HashMap<String, MintPools>,
    keys: &[String],
    mints: &[String],
) -> Vec<Value> {
    let keyset = keys.iter().cloned().collect::<HashSet<_>>();
    let mint_filter = mints.iter().cloned().collect::<HashSet<_>>();
    let mut out = Vec::new();
    for (mint, pools) in pools_by_mint {
        if !mint_filter.is_empty() && !mint_filter.contains(mint) {
            continue;
        }
        let pump = pools.pump.iter().find(|p| keyset.contains(*p)).cloned();
        let dlmm_touched = pools
            .dlmm
            .iter()
            .filter(|p| keyset.contains(*p))
            .cloned()
            .collect::<Vec<_>>();
        if pump.is_some() || !dlmm_touched.is_empty() || mint_filter.contains(mint) {
            out.push(json!({
                "mint": mint,
                "pump": pump.or_else(|| pools.pump.first().cloned()),
                "dlmm": pools.dlmm.first(),
                "dlmm_touched": dlmm_touched,
                "route_complete": !pools.pump.is_empty() && !pools.dlmm.is_empty()
            }));
        }
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
    let mut seen = out.iter().cloned().collect::<HashSet<_>>();
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

fn parse_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let matches = App::new("rabbitstream_probe")
        .arg(Arg::with_name("geyser-url").long("geyser-url").takes_value(true).required(true))
        .arg(Arg::with_name("geyser-token").long("geyser-token").takes_value(true).default_value(""))
        .arg(Arg::with_name("pools-by-mint-file").long("pools-by-mint-file").takes_value(true).required(true))
        .arg(Arg::with_name("rabbit-pool-filter").long("rabbit-pool-filter"))
        .arg(Arg::with_name("pump-program-filter").long("pump-program-filter"))
        .arg(Arg::with_name("max-filter-accounts").long("max-filter-accounts").takes_value(true).default_value("200"))
        .arg(Arg::with_name("max-dlmm").long("max-dlmm").takes_value(true).default_value("1"))
        .arg(Arg::with_name("sol-usd").long("sol-usd").takes_value(true).default_value("180"))
        .arg(Arg::with_name("min-usd").long("min-usd").takes_value(true).default_value("0"))
        .arg(Arg::with_name("only-actionable").long("only-actionable"))
        .arg(Arg::with_name("require-flashx").long("require-flashx"))
        .arg(Arg::with_name("required-accounts").long("required-accounts").takes_value(true).default_value(""))
        .arg(Arg::with_name("reject-accounts").long("reject-accounts").takes_value(true).default_value(""))
        .arg(Arg::with_name("log-keys").long("log-keys"))
        .arg(Arg::with_name("log-programs").long("log-programs"))
        .arg(Arg::with_name("json").long("json"))
        .get_matches();

    let geyser_url = matches.value_of("geyser-url").unwrap();
    let geyser_token = matches.value_of("geyser-token").unwrap();
    let pools_file = matches.value_of("pools-by-mint-file").unwrap();
    let max_dlmm = matches.value_of("max-dlmm").unwrap().parse::<usize>()?;
    let sol_usd = matches.value_of("sol-usd").unwrap().parse::<f64>()?;
    let min_usd = matches.value_of("min-usd").unwrap().parse::<f64>()?;
    let max_filter_accounts = matches.value_of("max-filter-accounts").unwrap().parse::<usize>()?;
    let rabbit_pool_filter = matches.is_present("rabbit-pool-filter");
    let pump_program_filter = matches.is_present("pump-program-filter");
    let only_actionable = matches.is_present("only-actionable");
    let require_flashx = matches.is_present("require-flashx");
    let log_keys = matches.is_present("log-keys");
    let log_programs = matches.is_present("log-programs");
    let json_logs = matches.is_present("json");
    let required_accounts = parse_csv(matches.value_of("required-accounts").unwrap());
    let reject_accounts = parse_csv(matches.value_of("reject-accounts").unwrap());

    let pools_by_mint = load_pools_by_mint(pools_file, max_dlmm)?;
    let subscribe_account_include = subscription_accounts(
        &pools_by_mint,
        rabbit_pool_filter,
        pump_program_filter,
        max_filter_accounts,
    );
    info!(
        "loaded {} actionable mints; subscribing accounts={} rabbit_pool_filter={} pump_program_filter={}",
        pools_by_mint.len(),
        subscribe_account_include.len(),
        rabbit_pool_filter,
        pump_program_filter
    );

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
            "probe".to_string(),
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
        info!("subscribed probe min_usd={} require_flashx={}", min_usd, require_flashx);

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
            let keyset = keys.iter().cloned().collect::<HashSet<_>>();
            let has_flashx = keyset.contains(FLASHX);
            let has_pump_program = keyset.contains(PUMP_AMM_PROGRAM);
            if require_flashx && !has_flashx {
                continue;
            }
            if !required_accounts.iter().all(|a| keyset.contains(a)) {
                continue;
            }
            if reject_accounts.iter().any(|a| keyset.contains(a)) {
                continue;
            }

            let (volume_usd, mints) = if let Some(meta) = meta {
                (estimate_wsol_volume_usd(meta, sol_usd), token_balance_mints(meta))
            } else {
                (0.0, Vec::new())
            };
            if volume_usd < min_usd {
                continue;
            }
            let programs = program_ids(&txn, &keys);
            let candidates = candidates_from_keys(&pools_by_mint, &keys, &mints);
            if only_actionable && candidates.is_empty() {
                continue;
            }

            let row = json!({
                "ts_ms": now_ms(),
                "slot": tx_update.slot,
                "signature": sig,
                "meta": meta.is_some(),
                "flashx": has_flashx,
                "pump_program": has_pump_program,
                "volume_usd": volume_usd,
                "keys_count": keys.len(),
                "programs_count": programs.len(),
                "mints": mints,
                "candidates": candidates,
                "programs": if log_programs { json!(programs) } else { Value::Null },
                "keys": if log_keys { json!(keys) } else { Value::Null },
            });

            if json_logs {
                println!("{}", row);
            } else {
                info!(
                    "PROBE slot={} sig={} flashx={} pump_program={} meta={} vol=${:.0} keys={} mints={} candidates={}",
                    row["slot"],
                    row["signature"].as_str().unwrap_or_default(),
                    has_flashx,
                    has_pump_program,
                    meta.is_some(),
                    volume_usd,
                    keys.len(),
                    row["mints"].as_array().map(|a| a.len()).unwrap_or(0),
                    row["candidates"].as_array().map(|a| a.len()).unwrap_or(0)
                );
                if log_programs {
                    info!("PROGRAMS sig={} programs={:?}", row["signature"].as_str().unwrap_or_default(), programs);
                }
                if log_keys {
                    info!("KEYS sig={} keys={:?}", row["signature"].as_str().unwrap_or_default(), keys);
                }
            }
        }

        warn!("geyser disconnected, reconnecting in 500ms");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
