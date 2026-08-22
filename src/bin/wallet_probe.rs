//! `wallet_probe` — passive competitor-wallet observation binary.
//!
//! Reads its endpoints from `config.toml` (`rpc`, `grpc`, `rabbitstream`) and
//! runs three concurrent subscribers:
//!
//!   A. RabbitStream (Shyft shred) filtered by the target wallets — captures
//!      every tx they broadcast, **including dropped / non-landed ones**.
//!   B. RabbitStream filtered by a broad DEX/trigger program list — feeds a
//!      rolling context buffer used to score causality candidates.
//!   C. Yellowstone gRPC filtered by the target wallets, plus RPC fallback —
//!      reconciles broadcast signatures with landing outcomes.
//!
//! Output: single JSONL file (default `state/wallet_probe.jsonl`).
//!
//! Example:
//!
//! ```text
//! cargo run --release --features geyser --bin wallet_probe -- \
//!     --config config.toml \
//!     --target-wallets 5xy...,7ab... \
//!     --out state/wallet_probe.jsonl \
//!     --duration-sec 3600 \
//!     --context-lookback-ms 500
//! ```

use anyhow::Context;
use clap::{App, Arg};
use solana_onchain_arbitrage_bot::config::AppConfig;
use solana_onchain_arbitrage_bot::streams::rabbitstream::RabbitStreamPlan;
use solana_onchain_arbitrage_bot::wallet_probe::context_stream::run_context_stream;
use solana_onchain_arbitrage_bot::wallet_probe::landing_stream::LandingReconciler;
use solana_onchain_arbitrage_bot::wallet_probe::tx_parse::{
    FLASHX_PROGRAM, KNOWN_DEX_PROGRAMS, MEVI_PROGRAM,
};
use solana_onchain_arbitrage_bot::wallet_probe::types::{ProbeStatusEvent, WalletProbeEvent};
use solana_onchain_arbitrage_bot::wallet_probe::wallet_stream::{
    run_wallet_stream, WalletBroadcast,
};
use solana_onchain_arbitrage_bot::wallet_probe::{ContextBuffer, JsonlWriter};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn parse_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn validate_pubkeys(field: &str, values: &[String]) -> anyhow::Result<()> {
    for v in values {
        Pubkey::from_str(v)
            .with_context(|| format!("invalid {} pubkey `{}`", field, v))?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let matches = App::new("wallet_probe")
        .about("Passive competitor-wallet observation via RabbitStream + Yellowstone")
        .arg(
            Arg::with_name("config")
                .long("config")
                .takes_value(true)
                .default_value("config.toml")
                .help("Path to AppConfig TOML (reuses rpc/grpc/rabbitstream)"),
        )
        .arg(
            Arg::with_name("target-wallets")
                .long("target-wallets")
                .takes_value(true)
                .required(true)
                .help("Comma-separated pubkeys to monitor (1-N)"),
        )
        .arg(
            Arg::with_name("out")
                .long("out")
                .takes_value(true)
                .default_value("state/wallet_probe.jsonl")
                .help("Output JSONL path (appended)"),
        )
        .arg(
            Arg::with_name("duration-sec")
                .long("duration-sec")
                .takes_value(true)
                .default_value("0")
                .help("Stop after N seconds (0 = run forever)"),
        )
        .arg(
            Arg::with_name("context-lookback-ms")
                .long("context-lookback-ms")
                .takes_value(true)
                .default_value("750")
                .help("Rolling context buffer window (ms)"),
        )
        .arg(
            Arg::with_name("context-max-entries")
                .long("context-max-entries")
                .takes_value(true)
                .default_value("4000")
                .help("Rolling context buffer max entries"),
        )
        .arg(
            Arg::with_name("max-candidates")
                .long("max-candidates")
                .takes_value(true)
                .default_value("5")
                .help("Max trigger candidates per wallet tx"),
        )
        .arg(
            Arg::with_name("landing-deadline-ms")
                .long("landing-deadline-ms")
                .takes_value(true)
                .default_value("15000")
                .help("Deadline before we flag a broadcast as dropped"),
        )
        .arg(
            Arg::with_name("context-programs")
                .long("context-programs")
                .takes_value(true)
                .default_value("")
                .help(
                    "Comma-separated program IDs for context stream. \
                     Empty = default DEX/trigger set.",
                ),
        )
        .arg(
            Arg::with_name("log-context-events")
                .long("log-context-events")
                .help("Also emit `context` events to JSONL (verbose)"),
        )
        .arg(
            Arg::with_name("disable-landing-yellowstone")
                .long("disable-landing-yellowstone")
                .help("Skip Yellowstone subscriber; rely on RPC deadline only"),
        )
        .get_matches();

    let config_path = matches.value_of("config").unwrap();
    let target_wallets = parse_csv(matches.value_of("target-wallets").unwrap());
    validate_pubkeys("target-wallets", &target_wallets)?;
    if target_wallets.is_empty() {
        anyhow::bail!("--target-wallets must contain at least one pubkey");
    }
    let out_path = matches.value_of("out").unwrap();
    let duration_sec = matches.value_of("duration-sec").unwrap().parse::<u64>()?;
    let context_lookback_ms = matches
        .value_of("context-lookback-ms")
        .unwrap()
        .parse::<u128>()?;
    let context_max_entries = matches
        .value_of("context-max-entries")
        .unwrap()
        .parse::<usize>()?;
    let max_candidates = matches.value_of("max-candidates").unwrap().parse::<usize>()?;
    let landing_deadline_ms = matches
        .value_of("landing-deadline-ms")
        .unwrap()
        .parse::<u64>()?;
    let log_context_events = matches.is_present("log-context-events");
    let disable_landing_yellowstone = matches.is_present("disable-landing-yellowstone");

    let mut context_programs = parse_csv(matches.value_of("context-programs").unwrap());
    if context_programs.is_empty() {
        context_programs = default_context_programs();
    }
    validate_pubkeys("context-programs", &context_programs)?;

    let config = AppConfig::load(config_path)
        .with_context(|| format!("loading AppConfig from {}", config_path))?;

    if !config.rabbitstream.enabled {
        anyhow::bail!(
            "config.rabbitstream must be enabled for wallet_probe (set [rabbitstream].enabled = true)"
        );
    }
    let rabbit_plan = RabbitStreamPlan::controlled_v1(&config.rabbitstream)?
        .ok_or_else(|| anyhow::anyhow!("rabbitstream plan not available (check config)"))?;

    info!(
        "wallet_probe starting: wallets={} out={} duration_sec={} lookback_ms={} context_programs={} landing_yellowstone={}",
        target_wallets.len(),
        out_path,
        duration_sec,
        context_lookback_ms,
        context_programs.len(),
        !disable_landing_yellowstone && config.grpc.enabled,
    );

    let writer = JsonlWriter::open(out_path)
        .await
        .with_context(|| format!("opening JSONL writer at {}", out_path))?;
    writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
        ts_ms: now_ms(),
        event: "probe_start".to_string(),
        detail: Some(format!(
            "wallets={} context_programs={} lookback_ms={}",
            target_wallets.len(),
            context_programs.len(),
            context_lookback_ms
        )),
    }));

    let context_buffer = Arc::new(RwLock::new(ContextBuffer::new(
        context_lookback_ms,
        context_max_entries,
    )));

    let (broadcast_tx, broadcast_rx) = mpsc::unbounded_channel::<WalletBroadcast>();

    // Landing reconciler (Yellowstone + RPC fallback)
    let reconciler = Arc::new(LandingReconciler::new(
        writer.clone(),
        landing_deadline_ms,
        config.rpc.http.clone(),
    ));
    reconciler.spawn_deadline_task(broadcast_rx);

    let landing_task = if disable_landing_yellowstone {
        info!("landing_stream (Yellowstone) disabled via CLI flag");
        None
    } else {
        let reconciler = reconciler.clone();
        let grpc_endpoint = config.grpc.clone();
        let wallets = target_wallets.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = reconciler.run_yellowstone(grpc_endpoint, wallets).await {
                error!("landing_stream task exited: {}", e);
            }
        }))
    };

    // Context stream (RabbitStream, broad DEX filter)
    let context_task = {
        let plan = rabbit_plan.clone();
        let buffer = context_buffer.clone();
        let writer = writer.clone();
        let programs = context_programs.clone();
        tokio::spawn(async move {
            if let Err(e) =
                run_context_stream(plan, programs, buffer, writer, log_context_events).await
            {
                error!("context_stream task exited: {}", e);
            }
        })
    };

    // Wallet stream (RabbitStream, wallet filter)
    let wallet_task = {
        let plan = rabbit_plan.clone();
        let buffer = context_buffer.clone();
        let writer = writer.clone();
        let wallets = target_wallets.clone();
        let broadcast_tx = broadcast_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = run_wallet_stream(
                plan,
                wallets,
                buffer,
                writer,
                broadcast_tx,
                max_candidates,
            )
            .await
            {
                error!("wallet_stream task exited: {}", e);
            }
        })
    };

    // Coordinate shutdown.
    if duration_sec > 0 {
        info!("wallet_probe will stop after {}s", duration_sec);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(duration_sec)) => {
                info!("wallet_probe duration reached, shutting down");
            }
            _ = tokio::signal::ctrl_c() => {
                info!("wallet_probe received Ctrl-C, shutting down");
            }
        }
    } else {
        info!("wallet_probe running until Ctrl-C");
        let _ = tokio::signal::ctrl_c().await;
        info!("wallet_probe received Ctrl-C, shutting down");
    }

    writer.write(WalletProbeEvent::ProbeStatus(ProbeStatusEvent {
        ts_ms: now_ms(),
        event: "probe_stop".to_string(),
        detail: None,
    }));

    // Best-effort: abort background loops. Stream tasks are infinite loops with
    // internal reconnect, so we don't await them.
    wallet_task.abort();
    context_task.abort();
    if let Some(t) = landing_task {
        t.abort();
    }

    // Give the writer a moment to flush its channel.
    drop(broadcast_tx);
    drop(writer);
    tokio::time::sleep(Duration::from_millis(500)).await;

    Ok(())
}

/// Default context program set: known DEXes + FlashX/MEVi triggers.
fn default_context_programs() -> Vec<String> {
    let mut v: Vec<String> = KNOWN_DEX_PROGRAMS.iter().map(|s| s.to_string()).collect();
    for p in [FLASHX_PROGRAM, MEVI_PROGRAM] {
        let s = p.to_string();
        if !v.contains(&s) {
            v.push(s);
        }
    }
    v
}
