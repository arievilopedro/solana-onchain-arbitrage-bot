//! Wallet-follower loop: polls `getSignaturesForAddress` for one or more
//! trader wallets, extracts mints from `postTokenBalances`, filters by
//! target programs (pump-amm / DLMM), and feeds mints into
//! `HotMintTracker::record_all` weighted by `weight` (equivalent to N
//! synthetic hits per new tx observed).
//!
//! Two entry points:
//! - `bootstrap_wallet_followers`: **blocking**, one-shot scan called at
//!   boot (via `spawn_blocking`). Populates the tracker synchronously so
//!   the promoter's first tick already sees the wallet's recent mints.
//!   Returns the per-wallet dedup set that `run_wallet_follower_loop`
//!   must inherit to avoid reprocessing the same sigs.
//! - `run_wallet_follower_loop`: **async**, infinite polling loop for
//!   subsequent cycles.

use crate::axion::pump_amm_pubkey;
use crate::config::{WalletFollowerEntry, WalletFollowersConfig};
use crate::constants::{sol_mint, usd1_mint, usdc_mint};
use crate::dex::meteora::constants::dlmm_program_id;
use crate::hot_mints::HotMintTracker;
use anyhow::Context;
use solana_client::rpc_client::{
    GetConfirmedSignaturesForAddress2Config, RpcClient,
};
use solana_client::rpc_config::RpcTransactionConfig;
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, UiMessage,
    UiTransactionEncoding, UiTransactionStatusMeta,
};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::sleep;

/// Common quote tokens that appear as the counterparty in every pump-amm /
/// DLMM swap (WSOL, USDC, USD1). If we let them into the tracker they
/// dominate the top-N because every followed trade records them, which then
/// makes the promoter try to discover WSOL / USDC pools — WSOL alone
/// returns a >32 MB `getProgramAccounts` reply (RPC -32008) and burns the
/// budget for the real target mints. Filter at extraction time.
fn quote_token_mints() -> &'static HashSet<Pubkey> {
    static CELL: OnceLock<HashSet<Pubkey>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut s = HashSet::new();
        s.insert(sol_mint());
        s.insert(usdc_mint());
        s.insert(usd1_mint());
        s
    })
}

#[derive(Debug, Clone)]
pub struct WalletTarget {
    pub address: Pubkey,
    pub label: String,
}

#[derive(Debug, Clone)]
pub struct WalletFollowerRuntimeConfig {
    pub poll_interval: Duration,
    pub lookback_signatures: usize,
    pub weight: u32,
    pub wallets: Vec<WalletTarget>,
    /// Empty = no program filter (accept every tx).
    pub target_programs: HashSet<Pubkey>,
}

/// Per-wallet outcome of a single scan cycle (sync or async).
#[derive(Debug, Clone, Default)]
pub struct WalletScanOutcome {
    pub signatures_seen: usize,
    pub new_signatures: usize,
    pub matched_transactions: usize,
    pub mints_recorded: usize,
    pub tx_errors: usize,
    /// Full current batch of sigs so the caller can seed dedup for the
    /// next cycle.
    pub current_batch: HashSet<Signature>,
}

/// Aggregate report returned by `bootstrap_wallet_followers`.
#[derive(Debug, Clone, Default)]
pub struct WalletFollowerBootstrapReport {
    pub wallets_scanned: usize,
    pub signatures_examined: usize,
    pub matched_transactions: usize,
    pub mints_recorded: usize,
    /// Per-wallet dedup sets. Feed into `run_wallet_follower_loop`.
    pub seen: HashMap<Pubkey, HashSet<Signature>>,
}

/// Parse the raw TOML config into a validated runtime shape.
pub fn parse_config(
    cfg: &WalletFollowersConfig,
) -> anyhow::Result<WalletFollowerRuntimeConfig> {
    let mut wallets = Vec::with_capacity(cfg.wallets.len());
    for entry in &cfg.wallets {
        wallets.push(parse_wallet(entry)?);
    }

    let mut target_programs: HashSet<Pubkey> = HashSet::new();
    for alias in &cfg.programs {
        match alias.as_str() {
            "pump_amm" | "pump" => {
                target_programs.insert(pump_amm_pubkey());
            }
            "dlmm" => {
                target_programs.insert(dlmm_program_id());
            }
            other => anyhow::bail!(
                "wallet_followers: unknown program alias `{}`",
                other
            ),
        }
    }

    Ok(WalletFollowerRuntimeConfig {
        poll_interval: Duration::from_millis(cfg.poll_interval_ms),
        lookback_signatures: cfg.lookback_signatures,
        weight: cfg.weight,
        wallets,
        target_programs,
    })
}

fn parse_wallet(entry: &WalletFollowerEntry) -> anyhow::Result<WalletTarget> {
    let address = Pubkey::from_str(entry.address.trim())
        .with_context(|| format!("invalid wallet_followers address `{}`", entry.address))?;
    let label = if entry.label.trim().is_empty() {
        entry.address.trim().to_string()
    } else {
        entry.label.trim().to_string()
    };
    Ok(WalletTarget { address, label })
}

/// Blocking one-shot scan of every configured wallet. Called at boot from
/// `tokio::task::spawn_blocking` so the tracker is populated **before** the
/// promoter's first tick, letting it promote the wallet's recent mints
/// immediately instead of waiting one `tick_ms` for the async loop.
pub fn bootstrap_wallet_followers(
    rpc: &RpcClient,
    tracker: &HotMintTracker,
    cfg: &WalletFollowerRuntimeConfig,
) -> anyhow::Result<WalletFollowerBootstrapReport> {
    let mut report = WalletFollowerBootstrapReport::default();
    if cfg.wallets.is_empty() {
        return Ok(report);
    }

    let empty: HashSet<Signature> = HashSet::new();
    for target in &cfg.wallets {
        report.wallets_scanned += 1;
        match process_wallet_sync(rpc, target, cfg, &empty, tracker) {
            Ok(outcome) => {
                report.signatures_examined += outcome.signatures_seen;
                report.matched_transactions += outcome.matched_transactions;
                report.mints_recorded += outcome.mints_recorded;
                report.seen.insert(target.address, outcome.current_batch);

                tracing::info!(
                    wallet = %target.address,
                    label = %target.label,
                    signatures = outcome.signatures_seen,
                    matched = outcome.matched_transactions,
                    mints = outcome.mints_recorded,
                    tx_errors = outcome.tx_errors,
                    "wallet_followers bootstrap: wallet scan complete"
                );
            }
            Err(err) => {
                tracing::warn!(
                    wallet = %target.address,
                    label = %target.label,
                    error = %err,
                    "wallet_followers bootstrap: wallet scan failed"
                );
            }
        }
    }

    Ok(report)
}

/// Main async entry: infinite loop. Each iteration polls every configured
/// wallet in sequence, feeds new mints into the tracker, then sleeps
/// `poll_interval`. Blocking RPC calls are dispatched via `spawn_blocking`
/// so this runs on the shared tokio runtime.
///
/// `initial_seen` should be the `WalletFollowerBootstrapReport::seen` map
/// from the bootstrap phase so we don't re-record the sigs we already
/// processed synchronously.
pub async fn run_wallet_follower_loop(
    rpc: Arc<RpcClient>,
    tracker: Arc<HotMintTracker>,
    cfg: WalletFollowerRuntimeConfig,
    initial_seen: HashMap<Pubkey, HashSet<Signature>>,
) {
    if cfg.wallets.is_empty() {
        tracing::warn!("wallet_followers: no wallets configured, exiting");
        return;
    }

    tracing::info!(
        wallets = cfg.wallets.len(),
        poll_interval_ms = cfg.poll_interval.as_millis() as u64,
        lookback = cfg.lookback_signatures,
        weight = cfg.weight,
        target_programs = cfg.target_programs.len(),
        seeded_wallets = initial_seen.len(),
        "wallet_followers: starting loop"
    );

    // Per-wallet dedup: last batch of sigs we already processed. Replaced
    // (not merged) on every poll so memory stays bounded at
    // `lookback_signatures` entries per wallet.
    let mut seen: HashMap<Pubkey, HashSet<Signature>> = initial_seen;

    loop {
        // Sleep FIRST when we already had a bootstrap; otherwise the loop
        // would re-scan the same sigs immediately. `seen` non-empty means
        // bootstrap ran; empty means either no bootstrap or bootstrap
        // returned nothing (fresh wallet).
        if !seen.is_empty() {
            sleep(cfg.poll_interval).await;
        }

        for target in &cfg.wallets {
            let previous = seen.entry(target.address).or_default().clone();
            match poll_wallet_async(&rpc, target, &cfg, &previous, &tracker).await {
                Ok(current_batch) => {
                    seen.insert(target.address, current_batch);
                }
                Err(err) => {
                    tracing::warn!(
                        wallet = %target.address,
                        label = %target.label,
                        error = %err,
                        "wallet_followers: poll failed"
                    );
                }
            }
        }

        // Sleep AFTER a fresh (no bootstrap) first pass.
        if seen.values().all(|s| s.is_empty()) {
            sleep(cfg.poll_interval).await;
        }
    }
}

/// Async wrapper: dispatches the blocking work to a `spawn_blocking`
/// worker so the runtime stays responsive.
async fn poll_wallet_async(
    rpc: &Arc<RpcClient>,
    target: &WalletTarget,
    cfg: &WalletFollowerRuntimeConfig,
    previous: &HashSet<Signature>,
    tracker: &Arc<HotMintTracker>,
) -> anyhow::Result<HashSet<Signature>> {
    let rpc_clone = Arc::clone(rpc);
    let tracker_clone = Arc::clone(tracker);
    let target_clone = target.clone();
    let cfg_clone = cfg.clone();
    let previous_clone = previous.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        process_wallet_sync(
            &rpc_clone,
            &target_clone,
            &cfg_clone,
            &previous_clone,
            &tracker_clone,
        )
    })
    .await
    .map_err(|e| anyhow::anyhow!("spawn_blocking join: {e}"))??;

    if outcome.matched_transactions > 0 {
        tracing::info!(
            wallet = %target.address,
            label = %target.label,
            new_signatures = outcome.new_signatures,
            matched_transactions = outcome.matched_transactions,
            mints = outcome.mints_recorded,
            weight = cfg.weight,
            "wallet_followers: recorded batch"
        );
    } else {
        tracing::debug!(
            wallet = %target.address,
            label = %target.label,
            new_signatures = outcome.new_signatures,
            "wallet_followers: no matching transactions"
        );
    }

    Ok(outcome.current_batch)
}

/// Core blocking logic: fetch signatures, dedup vs `previous`, fetch tx
/// details for new sigs, filter by target_programs, record weighted hits
/// into the tracker. Used by both `bootstrap_wallet_followers` (direct)
/// and `poll_wallet_async` (via `spawn_blocking`).
fn process_wallet_sync(
    rpc: &RpcClient,
    target: &WalletTarget,
    cfg: &WalletFollowerRuntimeConfig,
    previous: &HashSet<Signature>,
    tracker: &HotMintTracker,
) -> anyhow::Result<WalletScanOutcome> {
    let mut outcome = WalletScanOutcome::default();

    let sigs = fetch_signatures(rpc, &target.address, cfg.lookback_signatures)?;
    outcome.signatures_seen = sigs.len();
    outcome.current_batch = sigs.iter().copied().collect();

    let new_sigs: Vec<Signature> = sigs
        .iter()
        .copied()
        .filter(|s| !previous.contains(s))
        .collect();
    outcome.new_signatures = new_sigs.len();

    if new_sigs.is_empty() {
        return Ok(outcome);
    }

    for sig in &new_sigs {
        let details = match fetch_tx_details(rpc, sig) {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(err) => {
                outcome.tx_errors += 1;
                tracing::debug!(
                    wallet = %target.address,
                    signature = %sig,
                    error = %err,
                    "wallet_followers: fetch tx failed"
                );
                continue;
            }
        };

        if !cfg.target_programs.is_empty()
            && !details.programs.iter().any(|p| cfg.target_programs.contains(p))
        {
            continue;
        }

        if details.mints.is_empty() {
            continue;
        }

        outcome.matched_transactions += 1;
        outcome.mints_recorded += details.mints.len();

        for _ in 0..cfg.weight {
            tracker.record_all(details.mints.iter().copied());
        }
    }

    Ok(outcome)
}

fn fetch_signatures(
    rpc: &RpcClient,
    wallet: &Pubkey,
    limit: usize,
) -> anyhow::Result<Vec<Signature>> {
    let limit = limit.min(1000);
    let config = GetConfirmedSignaturesForAddress2Config {
        before: None,
        until: None,
        limit: Some(limit),
        commitment: Some(CommitmentConfig::confirmed()),
    };
    let result = rpc.get_signatures_for_address_with_config(wallet, config)?;
    let mut sigs = Vec::with_capacity(result.len());
    for entry in result {
        if entry.err.is_some() {
            continue;
        }
        if let Ok(sig) = Signature::from_str(&entry.signature) {
            sigs.push(sig);
        }
    }
    Ok(sigs)
}

struct TxDetails {
    mints: Vec<Pubkey>,
    programs: Vec<Pubkey>,
}

fn fetch_tx_details(
    rpc: &RpcClient,
    signature: &Signature,
) -> anyhow::Result<Option<TxDetails>> {
    let config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let tx = rpc.get_transaction_with_config(signature, config)?;
    let Some(meta) = tx.transaction.meta else {
        return Ok(None);
    };

    let mut mints = Vec::new();
    extract_mints_from_meta(&meta, &mut mints);
    dedup_preserving_order(&mut mints);

    // CRITICAL: v0 transactions load most of their accounts (including the
    // pump-amm and DLMM program pubkeys) via Address Lookup Tables, so they
    // never appear in `UiMessage::Raw.account_keys`. We MUST also read
    // `meta.loaded_addresses.{writable,readonly}` to correctly identify
    // which programs the tx invoked. Without this the program filter drops
    // essentially every real trader tx (routers, MEV bots, etc.).
    let mut programs: Vec<Pubkey> = Vec::new();
    if let EncodedTransaction::Json(ui_tx) = tx.transaction.transaction {
        if let UiMessage::Raw(raw) = ui_tx.message {
            for key in &raw.account_keys {
                if let Ok(pk) = Pubkey::from_str(key) {
                    programs.push(pk);
                }
            }
        }
    }
    if let OptionSerializer::Some(loaded) = &meta.loaded_addresses {
        for key in loaded.writable.iter().chain(loaded.readonly.iter()) {
            if let Ok(pk) = Pubkey::from_str(key) {
                programs.push(pk);
            }
        }
    }

    Ok(Some(TxDetails { mints, programs }))
}

fn extract_mints_from_meta(meta: &UiTransactionStatusMeta, out: &mut Vec<Pubkey>) {
    let quote = quote_token_mints();
    if let OptionSerializer::Some(balances) = &meta.post_token_balances {
        for b in balances {
            if let Ok(pk) = Pubkey::from_str(&b.mint) {
                if !quote.contains(&pk) {
                    out.push(pk);
                }
            }
        }
    }
    if let OptionSerializer::Some(balances) = &meta.pre_token_balances {
        for b in balances {
            if let Ok(pk) = Pubkey::from_str(&b.mint) {
                if !quote.contains(&pk) {
                    out.push(pk);
                }
            }
        }
    }
}

fn dedup_preserving_order(mints: &mut Vec<Pubkey>) {
    let mut seen = HashSet::new();
    mints.retain(|pk| seen.insert(*pk));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WalletFollowerEntry;

    #[test]
    fn parse_config_resolves_aliases_and_wallets() {
        let cfg = WalletFollowersConfig {
            enabled: true,
            poll_interval_ms: 60_000,
            lookback_signatures: 100,
            weight: 20,
            programs: vec!["pump_amm".into(), "dlmm".into()],
            wallets: vec![WalletFollowerEntry {
                address: "11111111111111111111111111111111".into(),
                label: "system".into(),
            }],
        };
        let runtime = parse_config(&cfg).unwrap();
        assert_eq!(runtime.wallets.len(), 1);
        assert_eq!(runtime.wallets[0].label, "system");
        assert!(runtime.target_programs.contains(&pump_amm_pubkey()));
        assert!(runtime.target_programs.contains(&dlmm_program_id()));
        assert_eq!(runtime.weight, 20);
        assert_eq!(runtime.poll_interval, Duration::from_millis(60_000));
    }

    #[test]
    fn parse_config_rejects_unknown_program_alias() {
        let cfg = WalletFollowersConfig {
            enabled: true,
            poll_interval_ms: 60_000,
            lookback_signatures: 100,
            weight: 20,
            programs: vec!["mango".into()],
            wallets: vec![WalletFollowerEntry {
                address: "11111111111111111111111111111111".into(),
                label: "".into(),
            }],
        };
        assert!(parse_config(&cfg).is_err());
    }

    #[test]
    fn parse_config_rejects_invalid_pubkey() {
        let cfg = WalletFollowersConfig {
            enabled: true,
            poll_interval_ms: 60_000,
            lookback_signatures: 100,
            weight: 20,
            programs: vec!["pump_amm".into()],
            wallets: vec![WalletFollowerEntry {
                address: "not-a-pubkey".into(),
                label: "".into(),
            }],
        };
        assert!(parse_config(&cfg).is_err());
    }

    #[test]
    fn quote_token_mints_covers_wsol_usdc_usd1() {
        let quote = quote_token_mints();
        assert!(quote.contains(&sol_mint()));
        assert!(quote.contains(&usdc_mint()));
        assert!(quote.contains(&usd1_mint()));
        assert_eq!(quote.len(), 3);
    }

    #[test]
    fn extract_mints_drops_quote_tokens_keeps_targets() {
        use solana_account_decoder::parse_token::UiTokenAmount;
        use solana_transaction_status::{
            TransactionStatusMeta, TransactionTokenBalance,
        };

        let target = "GPzpoXpD74E2C4CJNayuoyBqPQJEsPtdse3nhntrpump";
        let amount = UiTokenAmount {
            ui_amount: Some(0.0),
            decimals: 6,
            amount: "0".into(),
            ui_amount_string: "0".into(),
        };
        let mk = |mint: &str| TransactionTokenBalance {
            account_index: 0,
            mint: mint.into(),
            ui_token_amount: amount.clone(),
            owner: String::new(),
            program_id: String::new(),
        };
        let raw = TransactionStatusMeta {
            post_token_balances: Some(vec![
                mk(crate::constants::SOL_MINT),
                mk(target),
                mk(crate::constants::USDC_MINT),
            ]),
            pre_token_balances: Some(vec![mk(crate::constants::USD1_MINT)]),
            ..TransactionStatusMeta::default()
        };
        let meta: UiTransactionStatusMeta = raw.into();
        let mut out = Vec::new();
        extract_mints_from_meta(&meta, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], Pubkey::from_str(target).unwrap());
    }

    #[test]
    fn parse_config_defaults_label_to_address() {
        let cfg = WalletFollowersConfig {
            enabled: true,
            poll_interval_ms: 60_000,
            lookback_signatures: 100,
            weight: 20,
            programs: vec![],
            wallets: vec![WalletFollowerEntry {
                address: "11111111111111111111111111111111".into(),
                label: "".into(),
            }],
        };
        let runtime = parse_config(&cfg).unwrap();
        assert_eq!(runtime.wallets[0].label, "11111111111111111111111111111111");
        assert!(runtime.target_programs.is_empty());
    }
}
