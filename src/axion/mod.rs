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
    pub side: Option<&'static str>,
    pub raw_amount: Option<u64>,
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
    const PUMP_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
    const PUMP_BUY_DISCRIMINATOR: [u8; 8] =
        [0x33, 0xe6, 0x85, 0xa4, 0x01, 0x7f, 0x83, 0xad];
    const PUMP_SELL_DISCRIMINATOR: [u8; 8] =
        [0x66, 0x06, 0x3d, 0x12, 0x01, 0xda, 0xeb, 0xea];

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
        if data.len() < 17 {
            return None;
        }
        let byte_side = match data[0] {
            0 => Some("sell"),
            1 => Some("buy"),
            _ => None,
        };
        let user = account_indexes
            .get(1)
            .and_then(|index| keys.get(*index as usize));
        let side = meta
            .and_then(|meta| user.and_then(|user| owner_wsol_delta_sol(meta, user)))
            .map(|delta| if delta < 0.0 { "buy" } else { "sell" })
            .or(byte_side);
        let amount_0 = read_le_u64(&data[1..9]).unwrap_or(0);
        let amount_1 = read_le_u64(&data[9..17]).unwrap_or(0);
        let raw_amount = if amount_0 > 0 { amount_0 } else { amount_1 };
        let lamports = match side {
            Some("buy") => amount_0,
            Some("sell") => amount_1,
            _ => [amount_0, amount_1]
                .iter()
                .copied()
                .filter(|amount| *amount > 0)
                .min()
                .unwrap_or(0),
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
        let pump_program = PUMP_AMM_PROGRAM.parse::<Pubkey>().ok()?;
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
        })
        .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

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
