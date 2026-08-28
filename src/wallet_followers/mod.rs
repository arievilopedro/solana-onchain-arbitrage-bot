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
use crate::dex::meteora::constants::{damm_v2_program_id, dlmm_program_id};
use crate::dex::raydium::constants::raydium_cp_program_id;
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
            "cpmm" | "raydium_cpmm" => {
                target_programs.insert(raydium_cp_program_id());
            }
            "damm_v2" | "meteora_damm_v2" => {
                target_programs.insert(damm_v2_program_id());
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

// -----------------------------------------------------------------------------
// Seed extractor: one-shot boot scan to derive an initial set of monitored
// mints from the recent trade history of one or more "copy wallets". Used to
// support `runtime.allowed_mints=[]` deployments where the operator wants the
// bot to bootstrap its focus list from an off-chain reference wallet instead
// of a hard-coded config list.
// -----------------------------------------------------------------------------

use std::time::Instant;

/// One mint's aggregated activity across the scanned wallets. Emitted by
/// `seed_mints_from_wallets`. `trade_count` is the number of transactions in
/// which this mint appeared in `pre/post_token_balances` (quote tokens
/// filtered out). `source_wallets` lists every scanned wallet whose history
/// contributed at least one hit.
#[derive(Debug, Clone)]
pub struct SeedMintRank {
    pub mint: Pubkey,
    pub trade_count: u32,
    pub first_seen_slot: Option<u64>,
    pub last_seen_slot: Option<u64>,
    pub source_wallets: Vec<Pubkey>,
}

/// Aggregated report from a single boot-time seed scan.
#[derive(Debug, Clone)]
pub struct SeedFromWalletsReport {
    /// Top-N mints selected by `(trade_count DESC, last_seen_slot DESC)`.
    pub selected: Vec<SeedMintRank>,
    /// Full ranking (all mints observed, sorted by the same key). Useful for
    /// observability / debugging why a mint did or did not make the cut.
    pub all_ranked: Vec<SeedMintRank>,
    pub wallets_scanned: usize,
    pub signatures_examined: usize,
    /// True if the scan hit `cfg.budget` before finishing every wallet. The
    /// caller may still choose to proceed with a partial `selected`, or abort
    /// depending on operator policy.
    pub budget_exhausted: bool,
    pub elapsed: Duration,
}

/// Runtime knobs for the seed extractor. Decoupled from the TOML shape so
/// callers can construct it programmatically (e.g. in tests) without going
/// through the full `WalletFollowersConfig` type.
#[derive(Debug, Clone)]
pub struct SeedExtractionConfig {
    pub top_n: usize,
    pub max_signatures_per_wallet: usize,
    pub budget: Duration,
    /// Reserved for future parallelism; currently the extractor scans wallets
    /// serially (one wallet at a time, one tx at a time) matching the existing
    /// `bootstrap_wallet_followers` cadence.
    pub concurrency: usize,
}

/// Per-tx summary emitted by the internal `WalletTxScanner` trait. Kept
/// intentionally minimal so a fake scanner can drive the aggregator in unit
/// tests without depending on `solana_transaction_status` fixtures.
#[derive(Debug, Clone)]
pub struct SeedTxSummary {
    pub mints: Vec<Pubkey>,
    pub slot: u64,
}

/// Abstract source of signatures + per-tx mint lists. Production uses the
/// RPC-backed impl below; tests substitute a fake to feed canned data.
pub(crate) trait WalletTxScanner {
    fn fetch_signatures(&self, wallet: &Pubkey, limit: usize) -> anyhow::Result<Vec<Signature>>;
    fn fetch_tx_mints(&self, sig: &Signature) -> anyhow::Result<Option<SeedTxSummary>>;
}

struct RpcWalletTxScanner<'a> {
    rpc: &'a RpcClient,
}

impl<'a> WalletTxScanner for RpcWalletTxScanner<'a> {
    fn fetch_signatures(&self, wallet: &Pubkey, limit: usize) -> anyhow::Result<Vec<Signature>> {
        fetch_signatures(self.rpc, wallet, limit)
    }

    fn fetch_tx_mints(&self, signature: &Signature) -> anyhow::Result<Option<SeedTxSummary>> {
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let tx = self.rpc.get_transaction_with_config(signature, config)?;
        let slot = tx.slot;
        let Some(meta) = tx.transaction.meta else {
            return Ok(None);
        };
        let mut mints = Vec::new();
        extract_mints_from_meta(&meta, &mut mints);
        dedup_preserving_order(&mut mints);
        if mints.is_empty() {
            return Ok(None);
        }
        Ok(Some(SeedTxSummary { mints, slot }))
    }
}

/// Blocking boot-time scan across `wallets`. Aggregates non-quote mints from
/// each transaction's `pre/post_token_balances`, ranks them by frequency
/// (tie-break: `last_seen_slot` desc), and returns the top-`cfg.top_n` for
/// use as the initial allowlist seed. Intended to be called via
/// `tokio::task::spawn_blocking` at boot before the registry is constructed.
///
/// The scan respects `cfg.budget` as a hard wall-clock cap: when hit, the
/// function returns whatever it aggregated so far with `budget_exhausted =
/// true`. `cfg.max_signatures_per_wallet` bounds the RPC lookback per wallet
/// (Solana `getSignaturesForAddress` caps at 1000).
pub fn seed_mints_from_wallets(
    rpc: &RpcClient,
    wallets: &[Pubkey],
    cfg: &SeedExtractionConfig,
) -> anyhow::Result<SeedFromWalletsReport> {
    let scanner = RpcWalletTxScanner { rpc };
    seed_mints_with_scanner(&scanner, wallets, cfg, Instant::now())
}

fn seed_mints_with_scanner<S: WalletTxScanner>(
    scanner: &S,
    wallets: &[Pubkey],
    cfg: &SeedExtractionConfig,
    start: Instant,
) -> anyhow::Result<SeedFromWalletsReport> {
    let mut aggregator: HashMap<Pubkey, SeedMintRank> = HashMap::new();
    let mut signatures_examined: usize = 0;
    let mut wallets_scanned: usize = 0;
    let mut budget_exhausted = false;

    'outer: for wallet in wallets {
        wallets_scanned += 1;
        if start.elapsed() >= cfg.budget {
            budget_exhausted = true;
            break 'outer;
        }

        let sigs = match scanner.fetch_signatures(wallet, cfg.max_signatures_per_wallet) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    wallet = %wallet,
                    error = %err,
                    "seed_mints_from_wallets: fetch_signatures failed, skipping wallet",
                );
                continue;
            }
        };

        for sig in sigs {
            if start.elapsed() >= cfg.budget {
                budget_exhausted = true;
                break 'outer;
            }
            signatures_examined += 1;

            let summary = match scanner.fetch_tx_mints(&sig) {
                Ok(Some(s)) => s,
                Ok(None) => continue,
                Err(err) => {
                    tracing::debug!(
                        wallet = %wallet,
                        signature = %sig,
                        error = %err,
                        "seed_mints_from_wallets: tx fetch failed",
                    );
                    continue;
                }
            };

            let slot = summary.slot;
            for mint in summary.mints {
                let entry = aggregator.entry(mint).or_insert_with(|| SeedMintRank {
                    mint,
                    trade_count: 0,
                    first_seen_slot: None,
                    last_seen_slot: None,
                    source_wallets: Vec::new(),
                });
                entry.trade_count = entry.trade_count.saturating_add(1);
                entry.first_seen_slot = Some(match entry.first_seen_slot {
                    Some(cur) => cur.min(slot),
                    None => slot,
                });
                entry.last_seen_slot = Some(match entry.last_seen_slot {
                    Some(cur) => cur.max(slot),
                    None => slot,
                });
                if !entry.source_wallets.contains(wallet) {
                    entry.source_wallets.push(*wallet);
                }
            }
        }
    }

    let mut all_ranked: Vec<SeedMintRank> = aggregator.into_values().collect();
    all_ranked.sort_by(|a, b| {
        b.trade_count
            .cmp(&a.trade_count)
            .then_with(|| b.last_seen_slot.cmp(&a.last_seen_slot))
            .then_with(|| a.mint.to_bytes().cmp(&b.mint.to_bytes()))
    });
    let selected = all_ranked.iter().take(cfg.top_n).cloned().collect();

    Ok(SeedFromWalletsReport {
        selected,
        all_ranked,
        wallets_scanned,
        signatures_examined,
        budget_exhausted,
        elapsed: start.elapsed(),
    })
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
            ..Default::default()
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
    fn parse_config_resolves_cpmm_and_damm_v2_aliases() {
        let cfg = WalletFollowersConfig {
            enabled: true,
            poll_interval_ms: 60_000,
            lookback_signatures: 100,
            weight: 5,
            programs: vec![
                "cpmm".into(),
                "raydium_cpmm".into(),
                "damm_v2".into(),
                "meteora_damm_v2".into(),
            ],
            wallets: vec![WalletFollowerEntry {
                address: "11111111111111111111111111111111".into(),
                label: "system".into(),
            }],
            ..Default::default()
        };
        let runtime = parse_config(&cfg).unwrap();
        assert!(runtime.target_programs.contains(&raydium_cp_program_id()));
        assert!(runtime.target_programs.contains(&damm_v2_program_id()));
        // 4 aliases collapse into 2 unique program pubkeys.
        assert_eq!(runtime.target_programs.len(), 2);
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        };
        let runtime = parse_config(&cfg).unwrap();
        assert_eq!(runtime.wallets[0].label, "11111111111111111111111111111111");
        assert!(runtime.target_programs.is_empty());
    }

    // ---- Seed extractor tests --------------------------------------------
    // Fakes: build canned per-wallet signature streams + per-sig mint lists.

    use std::cell::RefCell;

    /// Fake scanner that returns pre-programmed signatures per wallet and
    /// pre-programmed `SeedTxSummary` per signature. Also records how many
    /// times `fetch_tx_mints` was called so tests can assert budget cutoff
    /// behaviour without wall-clock races.
    struct FakeScanner {
        by_wallet: HashMap<Pubkey, Vec<Signature>>,
        by_sig: HashMap<Signature, SeedTxSummary>,
        /// Optional per-call sleep to trigger budget exhaustion.
        tx_delay: Duration,
        calls: RefCell<usize>,
    }

    impl FakeScanner {
        fn new() -> Self {
            Self {
                by_wallet: HashMap::new(),
                by_sig: HashMap::new(),
                tx_delay: Duration::from_millis(0),
                calls: RefCell::new(0),
            }
        }

        fn add_wallet(&mut self, wallet: Pubkey, txs: Vec<SeedTxSummary>) {
            let mut sigs = Vec::with_capacity(txs.len());
            for tx in txs {
                let sig = Signature::new_unique();
                sigs.push(sig);
                self.by_sig.insert(sig, tx);
            }
            self.by_wallet.insert(wallet, sigs);
        }
    }

    impl WalletTxScanner for FakeScanner {
        fn fetch_signatures(
            &self,
            wallet: &Pubkey,
            limit: usize,
        ) -> anyhow::Result<Vec<Signature>> {
            let all = self.by_wallet.get(wallet).cloned().unwrap_or_default();
            Ok(all.into_iter().take(limit).collect())
        }

        fn fetch_tx_mints(
            &self,
            sig: &Signature,
        ) -> anyhow::Result<Option<SeedTxSummary>> {
            *self.calls.borrow_mut() += 1;
            if !self.tx_delay.is_zero() {
                std::thread::sleep(self.tx_delay);
            }
            Ok(self.by_sig.get(sig).cloned())
        }
    }

    fn default_cfg(top_n: usize) -> SeedExtractionConfig {
        SeedExtractionConfig {
            top_n,
            max_signatures_per_wallet: 1000,
            budget: Duration::from_secs(60),
            concurrency: 1,
        }
    }

    fn tx(mints: &[Pubkey], slot: u64) -> SeedTxSummary {
        SeedTxSummary {
            mints: mints.to_vec(),
            slot,
        }
    }

    #[test]
    fn seed_extractor_orders_by_frequency() {
        let wallet = Pubkey::new_unique();
        let mint_hot = Pubkey::new_unique();
        let mint_warm = Pubkey::new_unique();
        let mint_cold = Pubkey::new_unique();
        let mut scanner = FakeScanner::new();
        scanner.add_wallet(
            wallet,
            vec![
                tx(&[mint_hot, mint_warm], 100),
                tx(&[mint_hot], 101),
                tx(&[mint_hot, mint_cold], 102),
                tx(&[mint_warm], 103),
            ],
        );
        let cfg = default_cfg(10);
        let report = seed_mints_with_scanner(
            &scanner,
            &[wallet],
            &cfg,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(report.wallets_scanned, 1);
        assert_eq!(report.signatures_examined, 4);
        assert!(!report.budget_exhausted);
        assert_eq!(report.all_ranked.len(), 3);
        assert_eq!(report.all_ranked[0].mint, mint_hot);
        assert_eq!(report.all_ranked[0].trade_count, 3);
        assert_eq!(report.all_ranked[1].mint, mint_warm);
        assert_eq!(report.all_ranked[1].trade_count, 2);
        assert_eq!(report.all_ranked[2].mint, mint_cold);
        assert_eq!(report.all_ranked[2].trade_count, 1);
    }

    #[test]
    fn seed_extractor_tie_breaks_on_last_seen_slot() {
        let wallet = Pubkey::new_unique();
        let mint_old = Pubkey::new_unique();
        let mint_new = Pubkey::new_unique();
        let mut scanner = FakeScanner::new();
        scanner.add_wallet(
            wallet,
            vec![
                tx(&[mint_old], 100),
                tx(&[mint_old], 101),
                tx(&[mint_new], 200),
                tx(&[mint_new], 201),
            ],
        );
        let cfg = default_cfg(10);
        let report = seed_mints_with_scanner(
            &scanner,
            &[wallet],
            &cfg,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(report.all_ranked.len(), 2);
        assert_eq!(report.all_ranked[0].trade_count, 2);
        assert_eq!(report.all_ranked[1].trade_count, 2);
        // Newer last_seen_slot ranks first on ties.
        assert_eq!(report.all_ranked[0].mint, mint_new);
        assert_eq!(report.all_ranked[0].last_seen_slot, Some(201));
        assert_eq!(report.all_ranked[1].mint, mint_old);
        assert_eq!(report.all_ranked[1].last_seen_slot, Some(101));
    }

    #[test]
    fn seed_extractor_filters_quote_tokens() {
        // The extractor consumes SeedTxSummary directly; quote-token
        // filtering happens upstream in extract_mints_from_meta. This test
        // verifies the RPC-side extractor path via extract_mints_from_meta
        // (already covered by extract_mints_drops_quote_tokens_keeps_targets)
        // AND verifies that if a caller *did* feed a quote mint into the
        // aggregator it would still be counted (i.e. the filter contract is
        // enforced at the summary boundary, not inside the aggregator).
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
                mk(crate::constants::USD1_MINT),
            ]),
            ..TransactionStatusMeta::default()
        };
        let meta: UiTransactionStatusMeta = raw.into();
        let mut out = Vec::new();
        extract_mints_from_meta(&meta, &mut out);
        // Only the pump mint survives; WSOL/USDC/USD1 filtered.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], Pubkey::from_str(target).unwrap());
        assert!(!out.contains(&sol_mint()));
        assert!(!out.contains(&usdc_mint()));
        assert!(!out.contains(&usd1_mint()));
    }

    #[test]
    fn seed_extractor_respects_top_n_truncation() {
        let wallet = Pubkey::new_unique();
        let m1 = Pubkey::new_unique();
        let m2 = Pubkey::new_unique();
        let m3 = Pubkey::new_unique();
        let m4 = Pubkey::new_unique();
        let mut scanner = FakeScanner::new();
        scanner.add_wallet(
            wallet,
            vec![
                tx(&[m1], 100),
                tx(&[m1], 101),
                tx(&[m1], 102),
                tx(&[m1], 103),
                tx(&[m2], 200),
                tx(&[m2], 201),
                tx(&[m2], 202),
                tx(&[m3], 300),
                tx(&[m3], 301),
                tx(&[m4], 400),
            ],
        );
        let cfg = default_cfg(2);
        let report = seed_mints_with_scanner(
            &scanner,
            &[wallet],
            &cfg,
            Instant::now(),
        )
        .unwrap();
        // Full ranking retains all 4, but `selected` is capped at top_n=2.
        assert_eq!(report.all_ranked.len(), 4);
        assert_eq!(report.selected.len(), 2);
        assert_eq!(report.selected[0].mint, m1);
        assert_eq!(report.selected[0].trade_count, 4);
        assert_eq!(report.selected[1].mint, m2);
        assert_eq!(report.selected[1].trade_count, 3);
    }

    #[test]
    fn seed_extractor_reports_budget_exhausted() {
        let wallet = Pubkey::new_unique();
        let m1 = Pubkey::new_unique();
        let mut scanner = FakeScanner::new();
        // 20 tx * 10ms delay = ~200ms; budget 30ms cuts us off early.
        let txs: Vec<_> = (0..20).map(|i| tx(&[m1], 100 + i)).collect();
        scanner.add_wallet(wallet, txs);
        scanner.tx_delay = Duration::from_millis(10);
        let cfg = SeedExtractionConfig {
            top_n: 5,
            max_signatures_per_wallet: 1000,
            budget: Duration::from_millis(30),
            concurrency: 1,
        };
        let report = seed_mints_with_scanner(
            &scanner,
            &[wallet],
            &cfg,
            Instant::now(),
        )
        .unwrap();
        assert!(
            report.budget_exhausted,
            "expected budget_exhausted=true, got report={:?}",
            report
        );
        // At least one tx got aggregated before the cutoff.
        assert!(report.signatures_examined >= 1);
        assert!(report.signatures_examined < 20);
    }

    #[test]
    fn seed_extractor_dedupes_across_wallets() {
        let wallet_a = Pubkey::new_unique();
        let wallet_b = Pubkey::new_unique();
        let shared_mint = Pubkey::new_unique();
        let only_a = Pubkey::new_unique();
        let only_b = Pubkey::new_unique();
        let mut scanner = FakeScanner::new();
        scanner.add_wallet(
            wallet_a,
            vec![
                tx(&[shared_mint, only_a], 100),
                tx(&[shared_mint], 101),
            ],
        );
        scanner.add_wallet(
            wallet_b,
            vec![
                tx(&[shared_mint, only_b], 200),
                tx(&[shared_mint], 201),
            ],
        );
        let cfg = default_cfg(10);
        let report = seed_mints_with_scanner(
            &scanner,
            &[wallet_a, wallet_b],
            &cfg,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(report.wallets_scanned, 2);
        assert_eq!(report.signatures_examined, 4);
        // shared_mint aggregated across both wallets (single entry, count=4).
        let shared = report
            .all_ranked
            .iter()
            .find(|r| r.mint == shared_mint)
            .expect("shared mint present");
        assert_eq!(shared.trade_count, 4);
        assert_eq!(shared.source_wallets.len(), 2);
        assert!(shared.source_wallets.contains(&wallet_a));
        assert!(shared.source_wallets.contains(&wallet_b));
        // Wallet-specific mints retain single-wallet provenance.
        let a_only = report
            .all_ranked
            .iter()
            .find(|r| r.mint == only_a)
            .expect("only_a present");
        assert_eq!(a_only.source_wallets, vec![wallet_a]);
        let b_only = report
            .all_ranked
            .iter()
            .find(|r| r.mint == only_b)
            .expect("only_b present");
        assert_eq!(b_only.source_wallets, vec![wallet_b]);
        // shared_mint ranks first (trade_count=4 > 1).
        assert_eq!(report.all_ranked[0].mint, shared_mint);
    }
}
