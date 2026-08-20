//! Axion trigger parsing and filtering.
//!
//! This module is intentionally independent from pool storage. RabbitStream
//! emits candidate transaction triggers; the runtime registry decides whether a
//! mint is currently executable.

use solana_program::pubkey::Pubkey;
use std::collections::HashSet;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AxionTriggerSignal {
    pub signature: String,
    pub slot: u64,
    pub mint: Pubkey,
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
