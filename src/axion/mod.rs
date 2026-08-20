//! Axion trigger parsing and filtering.
//!
//! This module is intentionally independent from pool storage. RabbitStream
//! emits candidate transaction triggers; the runtime registry decides whether a
//! mint is currently executable.

use solana_program::pubkey::Pubkey;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq)]
pub struct AxionTriggerSignal {
    pub signature: String,
    pub slot: u64,
    pub mint: Pubkey,
    pub sol_amount: f64,
    pub volume_source: &'static str,
    pub axion_program_seen: bool,
}

pub fn pubkey_bytes_to_pubkey(bytes: &[u8]) -> Option<Pubkey> {
    if bytes.len() != 32 {
        return None;
    }
    Pubkey::try_from(bytes).ok()
}

pub fn collect_allowlisted_mints_from_strings<'a>(
    keys: impl IntoIterator<Item = &'a Pubkey>,
    token_balance_mints: impl IntoIterator<Item = &'a Pubkey>,
    allowed_mints: &HashSet<Pubkey>,
) -> Vec<Pubkey> {
    let mut out = Vec::new();
    for mint in token_balance_mints.into_iter().chain(keys) {
        if allowed_mints.contains(mint) && !out.contains(mint) {
            out.push(*mint);
        }
    }
    out
}

#[cfg(feature = "geyser")]
pub mod yellowstone {
    use super::{
        collect_allowlisted_mints_from_strings, pubkey_bytes_to_pubkey, AxionTriggerSignal,
    };
    use solana_program::pubkey::Pubkey;
    use std::collections::HashSet;
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        Transaction, TransactionStatusMeta,
    };

    const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

    pub fn account_keys(tx: &Transaction, meta: Option<&TransactionStatusMeta>) -> Vec<Pubkey> {
        let mut keys = Vec::new();
        if let Some(msg) = &tx.message {
            for key in &msg.account_keys {
                if let Some(pubkey) = pubkey_bytes_to_pubkey(key) {
                    keys.push(pubkey);
                }
            }
        }
        if let Some(meta) = meta {
            for key in &meta.loaded_writable_addresses {
                if let Some(pubkey) = pubkey_bytes_to_pubkey(key) {
                    keys.push(pubkey);
                }
            }
            for key in &meta.loaded_readonly_addresses {
                if let Some(pubkey) = pubkey_bytes_to_pubkey(key) {
                    keys.push(pubkey);
                }
            }
        }
        keys
    }

    pub fn token_balance_mints(meta: Option<&TransactionStatusMeta>) -> Vec<Pubkey> {
        let Some(meta) = meta else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for balance in meta
            .pre_token_balances
            .iter()
            .chain(meta.post_token_balances.iter())
        {
            let Ok(mint) = balance.mint.parse::<Pubkey>() else {
                continue;
            };
            if !out.contains(&mint) {
                out.push(mint);
            }
        }
        out
    }

    pub fn sol_volume(meta: Option<&TransactionStatusMeta>) -> (f64, &'static str) {
        let Some(meta) = meta else {
            return (0.0, "missing_meta");
        };

        let wsol = wsol_volume(meta);
        if wsol > 0.0 {
            return (wsol, "meta_wsol_delta");
        }

        let native = native_sol_volume(meta);
        if native > 0.0 {
            return (native, "meta_native_delta");
        }

        (0.0, "none")
    }

    fn token_amount(amount: &str, decimals: u32) -> f64 {
        let raw = amount.parse::<f64>().unwrap_or(0.0);
        raw / 10f64.powi(decimals as i32)
    }

    fn wsol_volume(meta: &TransactionStatusMeta) -> f64 {
        let mut pre = std::collections::HashMap::new();
        let mut post = std::collections::HashMap::new();
        for balance in &meta.pre_token_balances {
            if balance.mint == WSOL_MINT {
                if let Some(ui) = &balance.ui_token_amount {
                    pre.insert(balance.account_index, token_amount(&ui.amount, ui.decimals));
                }
            }
        }
        for balance in &meta.post_token_balances {
            if balance.mint == WSOL_MINT {
                if let Some(ui) = &balance.ui_token_amount {
                    post.insert(balance.account_index, token_amount(&ui.amount, ui.decimals));
                }
            }
        }

        pre.keys()
            .chain(post.keys())
            .map(|idx| {
                let before = pre.get(idx).copied().unwrap_or(0.0);
                let after = post.get(idx).copied().unwrap_or(0.0);
                (after - before).abs()
            })
            .fold(0.0, f64::max)
    }

    fn native_sol_volume(meta: &TransactionStatusMeta) -> f64 {
        let len = meta.pre_balances.len().min(meta.post_balances.len());
        (0..len)
            .map(|idx| {
                let before = meta.pre_balances[idx] as i128;
                let after = meta.post_balances[idx] as i128;
                (after - before).abs() as f64 / 1_000_000_000.0
            })
            .fold(0.0, f64::max)
    }

    pub fn axion_trigger_signals(
        signature: String,
        slot: u64,
        tx: &Transaction,
        meta: Option<&TransactionStatusMeta>,
        axion_program: Pubkey,
        allowed_mints: &HashSet<Pubkey>,
    ) -> Vec<AxionTriggerSignal> {
        let keys = account_keys(tx, meta);
        let axion_program_seen = keys.contains(&axion_program);
        if !axion_program_seen {
            return Vec::new();
        }

        let token_balance_mints = token_balance_mints(meta);
        let (sol_amount, volume_source) = sol_volume(meta);
        collect_allowlisted_mints_from_strings(
            keys.iter(),
            token_balance_mints.iter(),
            allowed_mints,
        )
        .into_iter()
        .map(|mint| AxionTriggerSignal {
            signature: signature.clone(),
            slot,
            mint,
            sol_amount,
            volume_source,
            axion_program_seen,
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    #[test]
    fn pubkey_bytes_require_exact_length() {
        assert_eq!(pubkey_bytes_to_pubkey(&[1; 31]), None);
        assert_eq!(pubkey_bytes_to_pubkey(&[2; 32]), Some(pk(2)));
    }

    #[test]
    fn collect_allowlisted_mints_dedupes_token_balances_and_keys() {
        let allowed = HashSet::from([pk(1), pk(2)]);
        let keys = vec![pk(2), pk(3), pk(1)];
        let token_balances = vec![pk(1), pk(2), pk(2)];

        let mints =
            collect_allowlisted_mints_from_strings(keys.iter(), token_balances.iter(), &allowed);

        assert_eq!(mints, vec![pk(1), pk(2)]);
    }
}
