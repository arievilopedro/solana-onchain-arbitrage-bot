//! Axion trigger parsing and filtering.
//!
//! This module is intentionally independent from pool storage. RabbitStream
//! emits candidate transaction triggers; the runtime registry decides whether a
//! mint is currently executable.

use solana_program::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::OnceLock;

pub const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";
pub const AXIOM_ALT: &str = "4vX5U9XsiY11infmC13d6VFPjvUqtuRw744r4o94dyow";
pub const AXIOM_VANITY_PREFIX: &str = "jitodontfront";
pub const AXIOM_VANITY_SUFFIX: &str = "1111111TradeWithAxiomDotTrade";
pub const AXION_PREPARE_IX_LEN: usize = 10;
pub const AXION_PREPARE_OPCODE: u8 = 0x01;
pub const AXION_SWAP_IX_LEN: usize = 23;
pub const AXION_SWAP_OPCODE: u8 = 0x00;
pub const AXIOM_CU_LIMIT: u32 = 275_000;
const BASE58_ALPHABET: &[u8] =
    b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn cached_pubkey(cell: &'static OnceLock<Pubkey>, source: &str) -> Pubkey {
    *cell.get_or_init(|| source.parse().expect("valid static pubkey"))
}

pub fn pump_amm_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    cached_pubkey(&CELL, PUMP_AMM_PROGRAM)
}

pub fn compute_budget_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    cached_pubkey(&CELL, COMPUTE_BUDGET_PROGRAM)
}

pub fn axiom_alt_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    cached_pubkey(&CELL, AXIOM_ALT)
}

/// Precomputes the family of Axiom vanity tip addresses observed on-chain.
///
/// The pattern is `jitodontfront?1111111TradeWithAxiomDotTrade` where `?`
/// varies across the base58 alphabet. Only strings that decode to valid
/// pubkeys are retained.
pub fn axiom_vanity_family() -> &'static HashSet<Pubkey> {
    static CELL: OnceLock<HashSet<Pubkey>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut out = HashSet::new();
        for ch in BASE58_ALPHABET {
            let candidate = format!(
                "{}{}{}",
                AXIOM_VANITY_PREFIX, *ch as char, AXIOM_VANITY_SUFFIX
            );
            if let Ok(pk) = candidate.parse::<Pubkey>() {
                out.insert(pk);
            }
        }
        out
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct AxionTriggerSignal {
    pub signature: String,
    pub slot: u64,
    pub mint: Pubkey,
    pub sol_amount: f64,
    pub volume_source: &'static str,
    pub side: Option<&'static str>,
    pub raw_amount: Option<u64>,
    pub axion_program_seen: bool,
    /// `SetComputeUnitLimit(275_000)` present (Axiom-typical value).
    pub cu_limit_axiom: bool,
    /// Axiom canonical ALT referenced in `addressTableLookups`.
    pub axiom_alt_seen: bool,
    /// Any address in the Axiom vanity tip family is a static key.
    pub axiom_vanity_seen: bool,
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
        axiom_alt_pubkey, axiom_vanity_family, collect_allowlisted_mints_from_strings,
        compute_budget_pubkey, pubkey_bytes_to_pubkey, pump_amm_pubkey, AxionTriggerSignal,
        AXIOM_CU_LIMIT, AXION_PREPARE_IX_LEN, AXION_PREPARE_OPCODE, AXION_SWAP_IX_LEN,
        AXION_SWAP_OPCODE,
    };
    use solana_program::pubkey::Pubkey;
    use std::collections::HashSet;
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        CompiledInstruction, Transaction, TransactionStatusMeta,
    };

    const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
    const PUMP_BUY_DISCRIMINATOR: [u8; 8] = [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];
    const PUMP_SELL_DISCRIMINATOR: [u8; 8] = [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];
    const CB_SET_COMPUTE_UNIT_LIMIT: u8 = 0x02;

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

    #[derive(Debug, Clone, Copy)]
    pub struct AxionVolume {
        pub sol_amount: f64,
        pub source: &'static str,
        pub side: Option<&'static str>,
        pub raw_amount: Option<u64>,
    }

    pub fn sol_volume(
        tx: &Transaction,
        keys: &[Pubkey],
        meta: Option<&TransactionStatusMeta>,
        axion_program: Pubkey,
    ) -> AxionVolume {
        let pump_instruction = pump_amm_instruction_volume(tx, keys);
        let axion_instruction = axion_instruction_volume(tx, keys, axion_program, meta);
        let instruction = pump_instruction.or(axion_instruction);
        if let Some(meta) = meta {
            let wsol = wsol_volume(meta);
            if wsol > 0.0 {
                return AxionVolume {
                    sol_amount: wsol,
                    source: "meta_wsol_delta",
                    side: instruction.and_then(|volume| volume.side),
                    raw_amount: instruction.and_then(|volume| volume.raw_amount),
                };
            }

            let native = native_sol_volume(meta);
            if native > 0.0 {
                return AxionVolume {
                    sol_amount: native,
                    source: "meta_native_delta",
                    side: instruction.and_then(|volume| volume.side),
                    raw_amount: instruction.and_then(|volume| volume.raw_amount),
                };
            }
        }

        if let Some(instruction) = instruction {
            return instruction;
        }

        AxionVolume {
            sol_amount: 0.0,
            source: "none",
            side: None,
            raw_amount: None,
        }
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

    fn axion_instruction_volume(
        tx: &Transaction,
        keys: &[Pubkey],
        axion_program: Pubkey,
        meta: Option<&TransactionStatusMeta>,
    ) -> Option<AxionVolume> {
        let msg = tx.message.as_ref()?;
        msg.instructions
            .iter()
            .filter(|ix| keys.get(ix.program_id_index as usize) == Some(&axion_program))
            .filter_map(|ix| {
                axion_volume_from_instruction(ix.accounts.as_slice(), &ix.data, keys, meta)
            })
            .max_by_key(|volume| volume.raw_amount.unwrap_or(0))
    }

    fn axion_volume_from_instruction(
        account_indexes: &[u8],
        data: &[u8],
        keys: &[Pubkey],
        meta: Option<&TransactionStatusMeta>,
    ) -> Option<AxionVolume> {
        // Only decode the swap ix; prepare ix carries no volume.
        if data.len() != AXION_SWAP_IX_LEN || data[0] != AXION_SWAP_OPCODE {
            return None;
        }
        let amount_in = read_le_u64(&data[1..9]).unwrap_or(0);
        let amount_out = read_le_u64(&data[9..17]).unwrap_or(0);
        let byte_side = match data[17] {
            0 => Some("buy"),
            1 => Some("sell"),
            _ => None,
        };
        let user = account_indexes
            .get(1)
            .and_then(|index| keys.get(*index as usize));
        let side = meta
            .and_then(|meta| user.and_then(|user| owner_wsol_delta_sol(meta, user)))
            .map(|delta| if delta < 0.0 { "buy" } else { "sell" })
            .or(byte_side);
        let lamports = match side {
            Some("buy") => amount_in,
            Some("sell") => amount_out,
            _ => 0,
        };
        let raw_amount = if lamports > 0 {
            lamports
        } else {
            amount_in.max(amount_out)
        };
        if raw_amount == 0 {
            return None;
        }
        Some(AxionVolume {
            sol_amount: lamports as f64 / 1_000_000_000.0,
            source: "axion_instruction_amount",
            side,
            raw_amount: Some(raw_amount),
        })
    }

    fn pump_amm_instruction_volume(tx: &Transaction, keys: &[Pubkey]) -> Option<AxionVolume> {
        let msg = tx.message.as_ref()?;
        let pump_program = pump_amm_pubkey();
        msg.instructions
            .iter()
            .filter(|ix| keys.get(ix.program_id_index as usize) == Some(&pump_program))
            .filter_map(|ix| pump_amm_volume_from_instruction(&ix.data))
            .max_by_key(|volume| volume.raw_amount.unwrap_or(0))
    }

    fn pump_amm_volume_from_instruction(data: &[u8]) -> Option<AxionVolume> {
        if data.len() < 16 {
            return None;
        }
        let (side, source) = if data.starts_with(&PUMP_BUY_DISCRIMINATOR) {
            ("buy", "pump_swap_buy_bytes")
        } else if data.starts_with(&PUMP_SELL_DISCRIMINATOR) {
            ("sell", "pump_swap_sell_bytes")
        } else {
            return None;
        };
        let amount = read_le_u64(&data[8..16]).unwrap_or(0);
        if amount == 0 {
            return None;
        }
        Some(AxionVolume {
            sol_amount: if side == "buy" {
                amount as f64 / 1_000_000_000.0
            } else {
                0.0
            },
            source,
            side: Some(side),
            raw_amount: Some(amount),
        })
    }

    fn owner_wsol_delta_sol(meta: &TransactionStatusMeta, owner: &Pubkey) -> Option<f64> {
        let owner = owner.to_string();
        let mut pre = std::collections::HashMap::new();
        let mut post = std::collections::HashMap::new();
        for balance in &meta.pre_token_balances {
            if balance.mint == WSOL_MINT && balance.owner == owner {
                if let Some(ui) = &balance.ui_token_amount {
                    pre.insert(balance.account_index, token_amount(&ui.amount, ui.decimals));
                }
            }
        }
        for balance in &meta.post_token_balances {
            if balance.mint == WSOL_MINT && balance.owner == owner {
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
                after - before
            })
            .max_by(|left, right| {
                left.abs()
                    .partial_cmp(&right.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .filter(|delta| delta.abs() > 0.0)
    }

    fn read_le_u64(bytes: &[u8]) -> Option<u64> {
        if bytes.len() < 8 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[..8]);
        Some(u64::from_le_bytes(buf))
    }

    /// Verifies the Axion trigger shape (2 ixs: prepare + swap) and the swap
    /// touches pump-amm. Rejects everything else (single-ix probes, wrong
    /// opcodes, wrong lengths, no pump-amm).
    pub fn validate_axion_structure(
        tx: &Transaction,
        keys: &[Pubkey],
        axion_program: Pubkey,
    ) -> bool {
        let Some(msg) = tx.message.as_ref() else {
            return false;
        };
        let axion_ixs: Vec<&CompiledInstruction> = msg
            .instructions
            .iter()
            .filter(|ix| keys.get(ix.program_id_index as usize) == Some(&axion_program))
            .collect();
        if axion_ixs.len() != 2 {
            return false;
        }
        let prepare = axion_ixs[0];
        let swap = axion_ixs[1];
        if prepare.data.len() != AXION_PREPARE_IX_LEN
            || prepare.data.first().copied() != Some(AXION_PREPARE_OPCODE)
        {
            return false;
        }
        if swap.data.len() != AXION_SWAP_IX_LEN
            || swap.data.first().copied() != Some(AXION_SWAP_OPCODE)
        {
            return false;
        }
        let pump = pump_amm_pubkey();
        swap.accounts
            .iter()
            .any(|idx| keys.get(*idx as usize) == Some(&pump))
    }

    /// True iff the tx contains a `SetComputeUnitLimit(target)` ComputeBudget ix.
    pub fn cu_limit_matches(tx: &Transaction, keys: &[Pubkey], target: u32) -> bool {
        let Some(msg) = tx.message.as_ref() else {
            return false;
        };
        let cb = compute_budget_pubkey();
        msg.instructions
            .iter()
            .filter(|ix| keys.get(ix.program_id_index as usize) == Some(&cb))
            .any(|ix| {
                ix.data.len() >= 5
                    && ix.data[0] == CB_SET_COMPUTE_UNIT_LIMIT
                    && {
                        let mut buf = [0u8; 4];
                        buf.copy_from_slice(&ix.data[1..5]);
                        u32::from_le_bytes(buf) == target
                    }
            })
    }

    /// True iff `addressTableLookups` references the canonical Axiom ALT.
    pub fn axiom_alt_referenced(tx: &Transaction) -> bool {
        let Some(msg) = tx.message.as_ref() else {
            return false;
        };
        let target = axiom_alt_pubkey();
        msg.address_table_lookups
            .iter()
            .filter_map(|lookup| pubkey_bytes_to_pubkey(&lookup.account_key))
            .any(|pk| pk == target)
    }

    /// True iff any static/loaded key belongs to the Axiom vanity tip family.
    pub fn axiom_vanity_present(keys: &[Pubkey]) -> bool {
        let family = axiom_vanity_family();
        keys.iter().any(|k| family.contains(k))
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

        if !validate_axion_structure(tx, &keys, axion_program) {
            return Vec::new();
        }

        let cu_limit_axiom = cu_limit_matches(tx, &keys, AXIOM_CU_LIMIT);
        let axiom_alt_seen = axiom_alt_referenced(tx);
        let axiom_vanity_seen = axiom_vanity_present(&keys);

        let token_balance_mints = token_balance_mints(meta);
        let volume = sol_volume(tx, &keys, meta, axion_program);
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
            sol_amount: volume.sol_amount,
            volume_source: volume.source,
            side: volume.side,
            raw_amount: volume.raw_amount,
            axion_program_seen,
            cu_limit_axiom,
            axiom_alt_seen,
            axiom_vanity_seen,
        })
        .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn swap_ix_bytes(amount_in: u64, amount_out: u64, side: u8) -> Vec<u8> {
            let mut d = Vec::with_capacity(AXION_SWAP_IX_LEN);
            d.push(AXION_SWAP_OPCODE);
            d.extend_from_slice(&amount_in.to_le_bytes());
            d.extend_from_slice(&amount_out.to_le_bytes());
            d.push(side);
            d.push(0x02);
            d.extend_from_slice(&31u16.to_le_bytes());
            d.extend_from_slice(&0u16.to_le_bytes());
            debug_assert_eq!(d.len(), AXION_SWAP_IX_LEN);
            d
        }

        #[test]
        fn pump_amm_discriminator_detects_buy() {
            let mut data = Vec::from(PUMP_BUY_DISCRIMINATOR);
            data.extend_from_slice(&1_500_000_000u64.to_le_bytes());

            let volume = pump_amm_volume_from_instruction(&data).unwrap();

            assert_eq!(volume.side, Some("buy"));
            assert_eq!(volume.raw_amount, Some(1_500_000_000));
            assert_eq!(volume.source, "pump_swap_buy_bytes");
            assert_eq!(volume.sol_amount, 1.5);
        }

        #[test]
        fn pump_amm_discriminator_detects_sell() {
            let mut data = Vec::from(PUMP_SELL_DISCRIMINATOR);
            data.extend_from_slice(&42_000_000u64.to_le_bytes());

            let volume = pump_amm_volume_from_instruction(&data).unwrap();

            assert_eq!(volume.side, Some("sell"));
            assert_eq!(volume.raw_amount, Some(42_000_000));
            assert_eq!(volume.source, "pump_swap_sell_bytes");
            assert_eq!(volume.sol_amount, 0.0);
        }

        #[test]
        fn axion_swap_decodes_buy_side_and_amount_in() {
            let data = swap_ix_bytes(2_500_000_000, 999_999, 0);
            let volume =
                axion_volume_from_instruction(&[0, 1], &data, &[], None).expect("volume");
            assert_eq!(volume.side, Some("buy"));
            assert_eq!(volume.raw_amount, Some(2_500_000_000));
            assert_eq!(volume.sol_amount, 2.5);
            assert_eq!(volume.source, "axion_instruction_amount");
        }

        #[test]
        fn axion_swap_decodes_sell_side_and_amount_out() {
            let data = swap_ix_bytes(4_240_000_000_000, 18_870_000_000, 1);
            let volume =
                axion_volume_from_instruction(&[0, 1], &data, &[], None).expect("volume");
            assert_eq!(volume.side, Some("sell"));
            assert_eq!(volume.raw_amount, Some(18_870_000_000));
            assert_eq!(volume.sol_amount, 18.87);
        }

        #[test]
        fn axion_prepare_ix_is_not_decoded_as_volume() {
            let prepare = vec![AXION_PREPARE_OPCODE, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            assert!(axion_volume_from_instruction(&[], &prepare, &[], None).is_none());
        }

        #[test]
        fn axion_swap_with_wrong_len_rejected() {
            let mut data = swap_ix_bytes(1, 1, 0);
            data.pop();
            assert!(axion_volume_from_instruction(&[], &data, &[], None).is_none());
        }

        #[test]
        fn vanity_family_contains_observed_keys() {
            let family = axiom_vanity_family();
            for observed in [
                "jitodontfront91111111TradeWithAxiomDotTrade",
                "jitodontfront81111111TradeWithAxiomDotTrade",
                "jitodontfrontP1111111TradeWithAxiomDotTrade",
                "jitodontfrontB1111111TradeWithAxiomDotTrade",
                "jitodontfront31111111TradeWithAxiomDotTrade",
            ] {
                let pk = observed.parse::<Pubkey>().expect("valid pubkey");
                assert!(
                    family.contains(&pk),
                    "vanity family missing observed key {observed}"
                );
            }
        }
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
    fn axiom_vanity_family_is_non_empty() {
        let family = axiom_vanity_family();
        assert!(!family.is_empty(), "expected at least one vanity variant");
        assert!(family.len() <= 58, "family cannot exceed base58 alphabet");
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
