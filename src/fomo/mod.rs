//! FOMO/TradeonFomo trigger parsing and filtering.
//!
//! FOMO is a competing MEV bot that arbitrages the same pump-amm mints as
//! Axion but via a triangular strategy (pump-amm ⇄ USDC ⇄ SOL via Meteora
//! DLMM), using either DFLOW or Jupiter V6 as its router. Because router
//! choice varies per tx but the fee-collecting signer (`FOMO_SIGNER`) is
//! constant, we identify FOMO transactions by matching `account_keys[0]`
//! against that signer instead of matching a program.

use solana_program::pubkey::Pubkey;
use std::collections::HashSet;
use std::sync::OnceLock;

/// FOMO/TradeonFomo fee wallet — signs every FOMO transaction as fee payer.
pub const FOMO_SIGNER: &str = "AgmLJBMDCqWynYnQiPCuj9ewsNNsBJXyzoUhD9LJzN51";
/// DFLOW router — observed in 5/7 FOMO samples.
pub const DFLOW_PROGRAM: &str = "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH";
/// Jupiter V6 router — observed in 2/7 FOMO samples.
pub const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
/// Meteora DLMM program — leg of FOMO's triangular arb path.
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

pub fn fomo_signer_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    *CELL.get_or_init(|| FOMO_SIGNER.parse().expect("valid FOMO_SIGNER pubkey"))
}

pub fn dflow_program_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    *CELL.get_or_init(|| DFLOW_PROGRAM.parse().expect("valid DFLOW_PROGRAM pubkey"))
}

pub fn jupiter_v6_program_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    *CELL.get_or_init(|| {
        JUPITER_V6_PROGRAM
            .parse()
            .expect("valid JUPITER_V6_PROGRAM pubkey")
    })
}

pub fn meteora_dlmm_pubkey() -> Pubkey {
    static CELL: OnceLock<Pubkey> = OnceLock::new();
    *CELL.get_or_init(|| {
        METEORA_DLMM_PROGRAM
            .parse()
            .expect("valid METEORA_DLMM_PROGRAM pubkey")
    })
}

#[derive(Debug, Clone, PartialEq)]
pub struct FomoTriggerSignal {
    pub signature: String,
    pub slot: u64,
    pub mint: Pubkey,
    pub sol_amount: f64,
    pub volume_source: &'static str,
    pub side: Option<&'static str>,
    pub raw_amount: Option<u64>,
    /// Best-effort router detection: `"dflow"`, `"jupiter"`, or `"other"`.
    pub router_kind: &'static str,
}

#[cfg(feature = "geyser")]
pub mod yellowstone {
    use super::{
        dflow_program_pubkey, jupiter_v6_program_pubkey, meteora_dlmm_pubkey, FomoTriggerSignal,
    };
    use crate::axion::yellowstone::{account_keys, sol_volume, token_balance_mints};
    use crate::axion::{collect_allowlisted_mints_from_strings, pump_amm_pubkey};
    use solana_program::pubkey::Pubkey;
    use std::collections::HashSet;
    use yellowstone_grpc_proto::solana::storage::confirmed_block::{
        Transaction, TransactionStatusMeta,
    };

    /// Detects the router used at the top level (if any). Returned string is
    /// stored on the signal for logging only; it is NOT part of the acceptance
    /// filter (a FOMO tx that switches to a new router still passes as
    /// `"other"` as long as the signer + AMM invariants hold).
    fn detect_router_kind(tx: &Transaction, keys: &[Pubkey]) -> &'static str {
        let Some(msg) = tx.message.as_ref() else {
            return "other";
        };
        let dflow = dflow_program_pubkey();
        let jupiter = jupiter_v6_program_pubkey();
        for ix in &msg.instructions {
            let Some(program) = keys.get(ix.program_id_index as usize) else {
                continue;
            };
            if *program == dflow {
                return "dflow";
            }
            if *program == jupiter {
                return "jupiter";
            }
        }
        "other"
    }

    /// Verifies the FOMO trigger shape:
    ///   1. `account_keys[0]` (fee-payer / signer) equals `fomo_signer`.
    ///   2. The tx touches pump-amm OR Meteora DLMM.
    ///
    /// We deliberately do NOT validate the router program or its discriminator:
    /// FOMO has been observed swapping between DFLOW and Jupiter V6, and may
    /// add new routers in the future. The signer-plus-AMM invariant is more
    /// stable and observed in 100% of samples.
    pub fn validate_fomo_structure(
        tx: &Transaction,
        keys: &[Pubkey],
        fomo_signer: Pubkey,
    ) -> bool {
        let Some(msg) = tx.message.as_ref() else {
            return false;
        };
        // account_keys[0] is the fee-payer/signer for legacy and v0 messages.
        let signer_matches = msg
            .account_keys
            .first()
            .and_then(|bytes| Pubkey::try_from(bytes.as_slice()).ok())
            == Some(fomo_signer);
        if !signer_matches {
            return false;
        }
        let pump = pump_amm_pubkey();
        let dlmm = meteora_dlmm_pubkey();
        keys.iter().any(|k| *k == pump || *k == dlmm)
    }

    pub fn fomo_trigger_signals(
        signature: String,
        slot: u64,
        tx: &Transaction,
        meta: Option<&TransactionStatusMeta>,
        fomo_signer: Pubkey,
        allowed_mints: &HashSet<Pubkey>,
    ) -> Vec<FomoTriggerSignal> {
        let keys = account_keys(tx, meta);
        if !validate_fomo_structure(tx, &keys, fomo_signer) {
            return Vec::new();
        }
        let router_kind = detect_router_kind(tx, &keys);
        let token_balance_mints = token_balance_mints(meta);
        // `sol_volume` needs an "axion program" pubkey to look up ixs by
        // program. FOMO has no analogous program — passing a sentinel that
        // never matches an ix skips the axion decode path and lets the
        // WSOL/native/pump fallbacks do the work.
        let volume = sol_volume(tx, &keys, meta, Pubkey::default());
        collect_allowlisted_mints_from_strings(
            keys.iter(),
            token_balance_mints.iter(),
            allowed_mints,
        )
        .into_iter()
        .map(|mint| FomoTriggerSignal {
            signature: signature.clone(),
            slot,
            mint,
            sol_amount: volume.sol_amount,
            volume_source: volume.source,
            side: volume.side,
            raw_amount: volume.raw_amount,
            router_kind,
        })
        .collect()
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::axion::PUMP_AMM_PROGRAM;
        use yellowstone_grpc_proto::solana::storage::confirmed_block::{
            CompiledInstruction, Message,
        };

        fn signer_bytes() -> Vec<u8> {
            super::super::fomo_signer_pubkey().to_bytes().to_vec()
        }

        fn pubkey_bytes(s: &str) -> Vec<u8> {
            s.parse::<Pubkey>().unwrap().to_bytes().to_vec()
        }

        fn make_tx(account_keys: Vec<Vec<u8>>, instructions: Vec<CompiledInstruction>) -> Transaction {
            Transaction {
                signatures: vec![vec![0u8; 64]],
                message: Some(Message {
                    header: None,
                    account_keys,
                    recent_blockhash: vec![0u8; 32],
                    instructions,
                    versioned: true,
                    address_table_lookups: vec![],
                }),
            }
        }

        #[test]
        fn accepts_signer_plus_pump_amm() {
            // account_keys: [fomo_signer, pump-amm, some_program]
            let keys = vec![
                signer_bytes(),
                pubkey_bytes(PUMP_AMM_PROGRAM),
                pubkey_bytes("DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH"),
            ];
            let ix = CompiledInstruction {
                program_id_index: 2,
                accounts: vec![0, 1],
                data: vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8],
            };
            let tx = make_tx(keys.clone(), vec![ix]);
            let parsed_keys: Vec<Pubkey> = keys
                .iter()
                .map(|k| Pubkey::try_from(k.as_slice()).unwrap())
                .collect();
            assert!(validate_fomo_structure(
                &tx,
                &parsed_keys,
                super::super::fomo_signer_pubkey()
            ));
            assert_eq!(detect_router_kind(&tx, &parsed_keys), "dflow");
        }

        #[test]
        fn accepts_signer_plus_dlmm_only() {
            let keys = vec![
                signer_bytes(),
                pubkey_bytes(super::super::METEORA_DLMM_PROGRAM),
                pubkey_bytes(super::super::JUPITER_V6_PROGRAM),
            ];
            let ix = CompiledInstruction {
                program_id_index: 2,
                accounts: vec![0, 1],
                data: vec![0xbb, 0x64, 0xfa, 0xcc, 0x31, 0xc4, 0xaf, 0x14],
            };
            let tx = make_tx(keys.clone(), vec![ix]);
            let parsed_keys: Vec<Pubkey> = keys
                .iter()
                .map(|k| Pubkey::try_from(k.as_slice()).unwrap())
                .collect();
            assert!(validate_fomo_structure(
                &tx,
                &parsed_keys,
                super::super::fomo_signer_pubkey()
            ));
            assert_eq!(detect_router_kind(&tx, &parsed_keys), "jupiter");
        }

        #[test]
        fn rejects_wrong_signer() {
            let wrong_signer = Pubkey::new_from_array([9u8; 32]).to_bytes().to_vec();
            let keys = vec![wrong_signer, pubkey_bytes(PUMP_AMM_PROGRAM)];
            let tx = make_tx(keys.clone(), vec![]);
            let parsed_keys: Vec<Pubkey> = keys
                .iter()
                .map(|k| Pubkey::try_from(k.as_slice()).unwrap())
                .collect();
            assert!(!validate_fomo_structure(
                &tx,
                &parsed_keys,
                super::super::fomo_signer_pubkey()
            ));
        }

        #[test]
        fn rejects_signer_without_pump_or_dlmm() {
            let keys = vec![
                signer_bytes(),
                pubkey_bytes("DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH"),
            ];
            let tx = make_tx(keys.clone(), vec![]);
            let parsed_keys: Vec<Pubkey> = keys
                .iter()
                .map(|k| Pubkey::try_from(k.as_slice()).unwrap())
                .collect();
            assert!(!validate_fomo_structure(
                &tx,
                &parsed_keys,
                super::super::fomo_signer_pubkey()
            ));
        }

        #[test]
        fn detect_router_kind_returns_other_when_no_known_router() {
            let keys = vec![
                signer_bytes(),
                pubkey_bytes(PUMP_AMM_PROGRAM),
            ];
            let ix = CompiledInstruction {
                program_id_index: 1,
                accounts: vec![0],
                data: vec![],
            };
            let tx = make_tx(keys.clone(), vec![ix]);
            let parsed_keys: Vec<Pubkey> = keys
                .iter()
                .map(|k| Pubkey::try_from(k.as_slice()).unwrap())
                .collect();
            assert_eq!(detect_router_kind(&tx, &parsed_keys), "other");
        }

        #[test]
        fn signals_produced_per_allowlisted_mint() {
            let mint_a = Pubkey::new_from_array([7u8; 32]);
            let mint_b = Pubkey::new_from_array([8u8; 32]);
            let mut allowed = HashSet::new();
            allowed.insert(mint_a);
            // mint_b intentionally NOT in allowlist

            let keys = vec![
                signer_bytes(),
                pubkey_bytes(PUMP_AMM_PROGRAM),
                mint_a.to_bytes().to_vec(),
                mint_b.to_bytes().to_vec(),
            ];
            let tx = make_tx(keys, vec![]);

            let signals = fomo_trigger_signals(
                "sig".to_string(),
                123,
                &tx,
                None,
                super::super::fomo_signer_pubkey(),
                &allowed,
            );

            assert_eq!(signals.len(), 1);
            assert_eq!(signals[0].mint, mint_a);
            assert_eq!(signals[0].signature, "sig");
            assert_eq!(signals[0].slot, 123);
        }

        #[test]
        fn signals_empty_when_mint_not_allowlisted() {
            let allowed = HashSet::new();
            let keys = vec![signer_bytes(), pubkey_bytes(PUMP_AMM_PROGRAM)];
            let tx = make_tx(keys, vec![]);
            let signals = fomo_trigger_signals(
                "sig".to_string(),
                0,
                &tx,
                None,
                super::super::fomo_signer_pubkey(),
                &allowed,
            );
            assert!(signals.is_empty());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_parse_as_valid_pubkeys() {
        // Would panic on init if any const were malformed.
        assert_ne!(fomo_signer_pubkey(), Pubkey::default());
        assert_ne!(dflow_program_pubkey(), Pubkey::default());
        assert_ne!(jupiter_v6_program_pubkey(), Pubkey::default());
        assert_ne!(meteora_dlmm_pubkey(), Pubkey::default());
    }

    #[test]
    fn signal_struct_fields_round_trip() {
        let mint = Pubkey::new_from_array([1u8; 32]);
        let signal = FomoTriggerSignal {
            signature: "sig".to_string(),
            slot: 42,
            mint,
            sol_amount: 1.5,
            volume_source: "meta_wsol_delta",
            side: Some("buy"),
            raw_amount: Some(1_500_000_000),
            router_kind: "dflow",
        };
        assert_eq!(signal.mint, mint);
        assert_eq!(signal.router_kind, "dflow");
    }

    // Silence unused-code warnings on non-geyser builds where the yellowstone
    // helpers below are not compiled.
    #[allow(dead_code)]
    fn _hashset_is_reachable() -> HashSet<Pubkey> {
        HashSet::new()
    }
}
