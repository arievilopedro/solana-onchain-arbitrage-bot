//! Transaction parsing helpers for wallet_probe.
//!
//! Extract from a Yellowstone `Transaction` + optional `TransactionStatusMeta`:
//! priority fee / CU limit (ComputeBudget), tip (SystemProgram::transfer to
//! known tip accounts), touched mints / pools / programs, and a few flags.
//!
//! All extraction is best-effort and defensive — inputs come from an untrusted
//! upstream stream and can be malformed.

use crate::sender::{HELIUS_TIP_ACCOUNTS, JITO_TIP_ACCOUNTS};
use crate::wallet_probe::types::ParsedTipRow;
use serde::Serialize;
use solana_sdk::compute_budget;
use solana_sdk::system_program;

// Well-known program IDs of DEXes / trigger programs relevant to the study.
// Keep this list conservative: we log what's touched, not judge.
pub const FLASHX_PROGRAM: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";
pub const MEVI_PROGRAM: &str = "MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz";
pub const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
pub const METEORA_DAMM_PROGRAM: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";
pub const RAYDIUM_CLMM_PROGRAM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
pub const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
pub const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const PHOENIX_PROGRAM: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";
pub const OPENBOOK_V2: &str = "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb";

pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const USD1_MINT: &str = "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB";

/// Programs recognised as pool / DEX programs for `pools`/`programs` extraction.
pub const KNOWN_DEX_PROGRAMS: &[&str] = &[
    PUMP_AMM_PROGRAM,
    METEORA_DLMM_PROGRAM,
    METEORA_DAMM_PROGRAM,
    RAYDIUM_CLMM_PROGRAM,
    RAYDIUM_CPMM_PROGRAM,
    RAYDIUM_AMM_V4,
    WHIRLPOOL_PROGRAM,
    PHOENIX_PROGRAM,
    OPENBOOK_V2,
    FLASHX_PROGRAM,
];

/// Programs recognised as "trigger" or MEV-related programs.
pub const TRIGGER_PROGRAMS: &[&str] = &[FLASHX_PROGRAM, MEVI_PROGRAM];

/// Stable mints excluded from `mints` (quote assets, stables).
pub const STABLE_QUOTE_MINTS: &[&str] = &[WSOL_MINT, USDC_MINT, USDT_MINT, USD1_MINT];

pub fn is_known_dex_program(pubkey: &str) -> bool {
    KNOWN_DEX_PROGRAMS.iter().any(|p| *p == pubkey)
}

pub fn is_stable_quote_mint(mint: &str) -> bool {
    STABLE_QUOTE_MINTS.iter().any(|m| *m == mint)
}

#[derive(Debug, Clone, Copy, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TipKind {
    Jito,
    Helius,
    Other,
}

impl TipKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TipKind::Jito => "jito",
            TipKind::Helius => "helius",
            TipKind::Other => "other",
        }
    }
}

pub fn classify_tip_account(pubkey: &str) -> TipKind {
    if JITO_TIP_ACCOUNTS.iter().any(|p| *p == pubkey) {
        TipKind::Jito
    } else if HELIUS_TIP_ACCOUNTS.iter().any(|p| *p == pubkey) {
        TipKind::Helius
    } else {
        TipKind::Other
    }
}

#[derive(Debug, Clone)]
pub struct ParsedTip {
    pub account: String,
    pub amount_lamports: u64,
    pub kind: TipKind,
}

impl From<ParsedTip> for ParsedTipRow {
    fn from(tip: ParsedTip) -> Self {
        ParsedTipRow {
            account: tip.account,
            amount_lamports: tip.amount_lamports,
            kind: tip.kind.as_str().to_string(),
        }
    }
}

/// Aggregated parse result for a monitored wallet's transaction.
#[derive(Debug, Clone)]
pub struct ParsedWalletTx {
    pub programs: Vec<String>,
    pub pools: Vec<String>,
    pub mints: Vec<String>,
    pub tip: Option<ParsedTip>,
    pub priority_fee_micro_lamports: Option<u64>,
    pub cu_limit: Option<u32>,
    pub instruction_count: usize,
    pub has_advance_nonce: bool,
    pub alt_writable_count: usize,
    pub alt_readonly_count: usize,
    pub flashx_axion_seen: bool,
    pub mevi_program_seen: bool,
}

#[cfg(feature = "geyser")]
pub use with_geyser::*;

#[cfg(feature = "geyser")]
mod with_geyser {
    use super::*;
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        Transaction, TransactionStatusMeta,
    };

    /// Convert a 32-byte pubkey slice to a base58 string. Returns `None` on wrong length.
    pub fn pubkey_bytes_to_string(bytes: &[u8]) -> Option<String> {
        if bytes.len() != 32 {
            return None;
        }
        Some(bs58::encode(bytes).into_string())
    }

    /// All account keys visible to the tx (message keys + ALT resolved keys).
    pub fn account_keys(tx: &Transaction, meta: Option<&TransactionStatusMeta>) -> Vec<String> {
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

    /// True when the message is versioned v0 (uses address lookup tables).
    pub fn is_versioned_v0(tx: &Transaction) -> bool {
        tx.message
            .as_ref()
            .map(|m| m.versioned)
            .unwrap_or(false)
    }

    /// Read a little-endian u32 from a byte slice.
    fn read_le_u32(bytes: &[u8]) -> Option<u32> {
        if bytes.len() < 4 {
            return None;
        }
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&bytes[..4]);
        Some(u32::from_le_bytes(buf))
    }

    /// Read a little-endian u64 from a byte slice.
    fn read_le_u64(bytes: &[u8]) -> Option<u64> {
        if bytes.len() < 8 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[..8]);
        Some(u64::from_le_bytes(buf))
    }

    /// Extract SetComputeUnitLimit and SetComputeUnitPrice from ComputeBudget ixs.
    ///
    /// ComputeBudget instruction encoding:
    ///   RequestUnits (deprecated)          — tag 0
    ///   RequestHeapFrame                    — tag 1  ( + u32 )
    ///   SetComputeUnitLimit                 — tag 2  ( + u32 )
    ///   SetComputeUnitPrice                 — tag 3  ( + u64 micro-lamports )
    ///   SetLoadedAccountsDataSizeLimit      — tag 4  ( + u32 )
    fn extract_compute_budget(data: &[u8]) -> (Option<u32>, Option<u64>) {
        if data.is_empty() {
            return (None, None);
        }
        match data[0] {
            2 => (read_le_u32(&data[1..]), None),
            3 => (None, read_le_u64(&data[1..])),
            _ => (None, None),
        }
    }

    fn is_advance_nonce(data: &[u8]) -> bool {
        // system_instruction::SystemInstruction::AdvanceNonceAccount tag = 4 (u32 LE)
        data.len() >= 4 && data[0] == 4 && data[1] == 0 && data[2] == 0 && data[3] == 0
    }

    /// Returns `Some((dest_pubkey, lamports))` for a system_program::transfer.
    ///
    /// SystemInstruction::Transfer tag = 2 (u32 LE) followed by u64 lamports.
    fn parse_system_transfer(data: &[u8]) -> Option<u64> {
        if data.len() < 12 {
            return None;
        }
        if !(data[0] == 2 && data[1] == 0 && data[2] == 0 && data[3] == 0) {
            return None;
        }
        read_le_u64(&data[4..12])
    }

    /// Parse a wallet transaction into aggregated fields.
    ///
    /// `signer` is the wallet address we're monitoring (used to disambiguate which
    /// system transfers count as "tips" — must originate from the signer).
    pub fn parse_wallet_tx(
        tx: &Transaction,
        meta: Option<&TransactionStatusMeta>,
        signer: &str,
    ) -> ParsedWalletTx {
        let keys = account_keys(tx, meta);
        let mut result = ParsedWalletTx {
            programs: Vec::new(),
            pools: Vec::new(),
            mints: Vec::new(),
            tip: None,
            priority_fee_micro_lamports: None,
            cu_limit: None,
            instruction_count: 0,
            has_advance_nonce: false,
            alt_writable_count: 0,
            alt_readonly_count: 0,
            flashx_axion_seen: false,
            mevi_program_seen: false,
        };

        if let Some(meta) = meta {
            result.alt_writable_count = meta.loaded_writable_addresses.len();
            result.alt_readonly_count = meta.loaded_readonly_addresses.len();
        }

        let Some(msg) = &tx.message else {
            return result;
        };

        result.instruction_count = msg.instructions.len();
        let compute_budget_id = compute_budget::ID.to_string();
        let system_program_id = system_program::ID.to_string();

        for ix in &msg.instructions {
            let program_idx = ix.program_id_index as usize;
            let program = match keys.get(program_idx) {
                Some(p) => p.clone(),
                None => continue,
            };

            if !result.programs.contains(&program) {
                result.programs.push(program.clone());
            }
            if program == FLASHX_PROGRAM {
                result.flashx_axion_seen = true;
            }
            if program == MEVI_PROGRAM {
                result.mevi_program_seen = true;
            }

            if program == compute_budget_id {
                let (limit, price) = extract_compute_budget(&ix.data);
                if let Some(v) = limit {
                    result.cu_limit = Some(v);
                }
                if let Some(v) = price {
                    result.priority_fee_micro_lamports = Some(v);
                }
                continue;
            }

            if program == system_program_id {
                if is_advance_nonce(&ix.data) {
                    result.has_advance_nonce = true;
                    continue;
                }
                // Look for tips: system transfer from signer to a known tip account.
                if let Some(lamports) = parse_system_transfer(&ix.data) {
                    // account[0] = from, account[1] = to
                    let from = ix
                        .accounts
                        .get(0)
                        .and_then(|i| keys.get(*i as usize))
                        .map(String::as_str);
                    let to = ix
                        .accounts
                        .get(1)
                        .and_then(|i| keys.get(*i as usize))
                        .cloned();
                    if from == Some(signer) {
                        if let Some(to_pk) = to {
                            let kind = classify_tip_account(&to_pk);
                            // Prefer classified tips over "other"; keep first "other" as fallback.
                            let should_replace = match &result.tip {
                                None => true,
                                Some(existing) => {
                                    matches!(existing.kind, TipKind::Other)
                                        && !matches!(kind, TipKind::Other)
                                }
                            };
                            if should_replace {
                                result.tip = Some(ParsedTip {
                                    account: to_pk,
                                    amount_lamports: lamports,
                                    kind,
                                });
                            }
                        }
                    }
                }
                continue;
            }

            if is_known_dex_program(program.as_str()) {
                // account[0] is (almost always) the pool for DEX programs — take it as a hint.
                if let Some(pool) = ix
                    .accounts
                    .get(0)
                    .and_then(|i| keys.get(*i as usize))
                    .cloned()
                {
                    if !result.pools.contains(&pool) {
                        result.pools.push(pool);
                    }
                }
            }
        }

        // Mints: from meta.pre_token_balances / post_token_balances if present.
        if let Some(meta) = meta {
            for b in meta
                .pre_token_balances
                .iter()
                .chain(meta.post_token_balances.iter())
            {
                if b.mint.is_empty() || is_stable_quote_mint(b.mint.as_str()) {
                    continue;
                }
                if !result.mints.contains(&b.mint) {
                    result.mints.push(b.mint.clone());
                }
            }
        }

        result
    }

    /// Return the wallet signer (index 0 of account_keys) for a tx, if present.
    pub fn tx_signer(tx: &Transaction) -> Option<String> {
        tx.message
            .as_ref()
            .and_then(|m| m.account_keys.first())
            .and_then(|b| pubkey_bytes_to_string(b))
    }

    /// Extract distinct pool candidates and mint hints for a **context** tx
    /// (not restricted to a specific signer).
    pub fn parse_context_tx(
        tx: &Transaction,
        meta: Option<&TransactionStatusMeta>,
    ) -> (Vec<String>, Vec<String>, Vec<String>) {
        let keys = account_keys(tx, meta);
        let mut programs = Vec::new();
        let mut pools = Vec::new();
        let mut mints = Vec::new();

        if let Some(msg) = &tx.message {
            for ix in &msg.instructions {
                let program_idx = ix.program_id_index as usize;
                if let Some(program) = keys.get(program_idx).cloned() {
                    if !programs.contains(&program) {
                        programs.push(program.clone());
                    }
                    if is_known_dex_program(program.as_str()) {
                        if let Some(pool) = ix
                            .accounts
                            .get(0)
                            .and_then(|i| keys.get(*i as usize))
                            .cloned()
                        {
                            if !pools.contains(&pool) {
                                pools.push(pool);
                            }
                        }
                    }
                }
            }
        }
        if let Some(meta) = meta {
            for b in meta
                .pre_token_balances
                .iter()
                .chain(meta.post_token_balances.iter())
            {
                if b.mint.is_empty() || is_stable_quote_mint(b.mint.as_str()) {
                    continue;
                }
                if !mints.contains(&b.mint) {
                    mints.push(b.mint.clone());
                }
            }
        }

        (programs, pools, mints)
    }

    /// Rough SOL volume estimate for a context tx: max absolute lamport delta
    /// across all account balances. Zero when meta is absent or partial.
    pub fn context_sol_volume_lamports(meta: Option<&TransactionStatusMeta>) -> Option<u64> {
        let meta = meta?;
        let len = meta.pre_balances.len().min(meta.post_balances.len());
        if len == 0 {
            return None;
        }
        let max_delta = (0..len)
            .map(|i| {
                let pre = meta.pre_balances[i] as i128;
                let post = meta.post_balances[i] as i128;
                (post - pre).unsigned_abs() as u64
            })
            .max()
            .unwrap_or(0);
        if max_delta == 0 {
            None
        } else {
            Some(max_delta)
        }
    }

    /// Serialized wire size of the tx (approximation of what went to the network).
    pub fn tx_size_bytes(tx: &Transaction) -> usize {
        // yellowstone_grpc_proto::Transaction has no `signatures` size we can trust
        // as-is (it's a repeated field), so we approximate by summing sizes.
        let mut n = 0usize;
        for s in &tx.signatures {
            n += s.len();
        }
        if let Some(msg) = &tx.message {
            n += 3; // header (3 u8)
            for k in &msg.account_keys {
                n += k.len();
            }
            n += msg.recent_blockhash.len();
            for ix in &msg.instructions {
                n += 1 + ix.accounts.len() + ix.data.len() + 1;
            }
            for lut in &msg.address_table_lookups {
                n += lut.account_key.len()
                    + lut.writable_indexes.len()
                    + lut.readonly_indexes.len();
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helius_tip_classified() {
        assert_eq!(
            classify_tip_account("4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE"),
            TipKind::Helius
        );
    }

    #[test]
    fn jito_tip_classified() {
        assert_eq!(
            classify_tip_account("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
            TipKind::Jito
        );
    }

    #[test]
    fn unknown_tip_classified_as_other() {
        assert_eq!(
            classify_tip_account("11111111111111111111111111111111"),
            TipKind::Other
        );
    }
}
