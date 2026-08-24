//! Wallet-follower loop: polls `getSignaturesForAddress` for one or more
//! trader wallets, extracts mints from `postTokenBalances`, filters by
//! target programs (pump-amm / DLMM), and feeds mints into
//! `HotMintTracker::record_all` weighted by `weight` (equivalent to N
//! synthetic hits per new tx observed).
//!
//! Design goals:
//! - Simple polling loop; blocking RPC via `tokio::task::spawn_blocking`.
//! - Bounded memory: per-wallet `seen: HashSet<Signature>` is replaced with
//!   the current lookback batch on every poll.
//! - No state persistence: on restart, the first poll may re-record recent
//!   sigs (idempotent from the tracker's perspective).
//! - Isolated failures: a single wallet's RPC failure does not abort the
//!   loop; per-tx failures are logged at debug and skipped.

use crate::axion::pump_amm_pubkey;
use crate::config::{WalletFollowerEntry, WalletFollowersConfig};
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
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

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

/// Main entry: infinite loop. Each iteration polls every configured wallet
/// in sequence, feeds new mints into the tracker, then sleeps
/// `poll_interval`. Blocking RPC calls are dispatched via `spawn_blocking`
/// so this can run on the shared tokio runtime.
pub async fn run_wallet_follower_loop(
    rpc: Arc<RpcClient>,
    tracker: Arc<HotMintTracker>,
    cfg: WalletFollowerRuntimeConfig,
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
        "wallet_followers: starting loop"
    );

    // Per-wallet dedup: last batch of sigs we already processed. Replaced
    // (not merged) on every poll so memory stays bounded at
    // `lookback_signatures` entries per wallet.
    let mut seen: HashMap<Pubkey, HashSet<Signature>> = HashMap::new();

    loop {
        for target in &cfg.wallets {
            let previous = seen.entry(target.address).or_default().clone();
            match poll_wallet(&rpc, target, &cfg, &previous, &tracker).await {
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

        sleep(cfg.poll_interval).await;
    }
}

/// Fetch the last `lookback_signatures` sigs for one wallet, filter for
/// new-since-last-poll, fetch each tx, feed passing mints into the tracker.
/// Returns the full current batch (regardless of "new" status) so the
/// caller can update its dedup set.
async fn poll_wallet(
    rpc: &Arc<RpcClient>,
    target: &WalletTarget,
    cfg: &WalletFollowerRuntimeConfig,
    previous: &HashSet<Signature>,
    tracker: &Arc<HotMintTracker>,
) -> anyhow::Result<HashSet<Signature>> {
    let rpc_clone = Arc::clone(rpc);
    let wallet = target.address;
    let limit = cfg.lookback_signatures;
    let sigs = tokio::task::spawn_blocking(move || fetch_signatures(&rpc_clone, &wallet, limit))
        .await
        .map_err(|e| anyhow::anyhow!("spawn_blocking join: {e}"))??;

    let current_batch: HashSet<Signature> = sigs.iter().copied().collect();
    let new_sigs: Vec<Signature> = sigs.iter().copied().filter(|s| !previous.contains(s)).collect();

    if new_sigs.is_empty() {
        return Ok(current_batch);
    }

    let mut tx_count = 0usize;
    let mut mint_hits = 0usize;
    for sig in &new_sigs {
        let rpc_clone = Arc::clone(rpc);
        let sig_owned = *sig;
        let details = match tokio::task::spawn_blocking(move || {
            fetch_tx_details(&rpc_clone, &sig_owned)
        })
        .await
        {
            Ok(Ok(details)) => details,
            Ok(Err(err)) => {
                tracing::debug!(
                    wallet = %target.address,
                    signature = %sig,
                    error = %err,
                    "wallet_followers: fetch tx failed"
                );
                continue;
            }
            Err(join_err) => {
                tracing::debug!(
                    wallet = %target.address,
                    signature = %sig,
                    error = %join_err,
                    "wallet_followers: spawn_blocking failed"
                );
                continue;
            }
        };

        let Some(details) = details else {
            continue;
        };

        if !cfg.target_programs.is_empty()
            && !details.programs.iter().any(|p| cfg.target_programs.contains(p))
        {
            continue;
        }

        if details.mints.is_empty() {
            continue;
        }

        tx_count += 1;
        mint_hits += details.mints.len();

        for _ in 0..cfg.weight {
            tracker.record_all(details.mints.iter().copied());
        }
    }

    if tx_count > 0 {
        tracing::info!(
            wallet = %target.address,
            label = %target.label,
            new_signatures = new_sigs.len(),
            matched_transactions = tx_count,
            mints_per_match = mint_hits,
            weight = cfg.weight,
            "wallet_followers: recorded batch"
        );
    } else {
        tracing::debug!(
            wallet = %target.address,
            label = %target.label,
            new_signatures = new_sigs.len(),
            "wallet_followers: no matching transactions"
        );
    }

    Ok(current_batch)
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

    Ok(Some(TxDetails { mints, programs }))
}

fn extract_mints_from_meta(meta: &UiTransactionStatusMeta, out: &mut Vec<Pubkey>) {
    if let OptionSerializer::Some(balances) = &meta.post_token_balances {
        for b in balances {
            if let Ok(pk) = Pubkey::from_str(&b.mint) {
                out.push(pk);
            }
        }
    }
    if let OptionSerializer::Some(balances) = &meta.pre_token_balances {
        for b in balances {
            if let Ok(pk) = Pubkey::from_str(&b.mint) {
                out.push(pk);
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
