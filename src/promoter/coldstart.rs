//! Cold-start scanner: seeds `HotMintTracker` from recent on-chain activity so
//! promoter decisions don't have to wait for the full `window_ms` to elapse
//! from a cold boot.
//!
//! Approach (per M3b plan):
//! 1. Ask RPC for the most recent `max_signatures` transactions that touched
//!    the configured programs (typically pump-amm).
//! 2. Fetch each transaction's meta and extract the mints from
//!    `pre/postTokenBalances`.
//! 3. Feed the mints into the tracker via `record_all`.
//!
//! The RPC layer is abstracted behind `TransactionScanner` so unit tests can
//! run without network. The real implementation ships in `RpcTransactionScanner`.

use crate::hot_mints::HotMintTracker;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::time::{Duration, Instant};

/// Configuration for a single cold-start scan.
#[derive(Debug, Clone)]
pub struct ColdStartScanConfig {
    /// Max number of signatures to fetch per program. Also bounded by RPC
    /// (Solana JSON-RPC caps `getSignaturesForAddress` at 1000).
    pub max_signatures: usize,
    /// Wall-clock budget for the entire scan. Once exceeded the scan stops
    /// early and reports `budget_exhausted`.
    pub budget: Duration,
    /// Programs to scan. Defaults to `[pump_amm_pubkey()]` in the caller.
    pub programs: Vec<Pubkey>,
}

impl Default for ColdStartScanConfig {
    fn default() -> Self {
        Self {
            max_signatures: 1000,
            budget: Duration::from_secs(30),
            programs: Vec::new(),
        }
    }
}

/// Structured report returned by a completed (or budget-truncated) scan.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ColdStartScanReport {
    pub signatures_examined: usize,
    pub mints_recorded: usize,
    pub budget_exhausted: bool,
    pub elapsed: Duration,
    /// Errors encountered while fetching individual transactions. The scan
    /// continues past per-transaction errors so a few flaky lookups don't
    /// discard the whole batch.
    pub transaction_errors: usize,
}

/// Trait abstracting the RPC calls this scanner needs. Tests swap in a mock;
/// production wires in `RpcTransactionScanner`.
pub trait TransactionScanner {
    /// Return up to `limit` recent confirmed signatures that touched `program`,
    /// most recent first.
    fn signatures_for_program(
        &self,
        program: &Pubkey,
        limit: usize,
    ) -> anyhow::Result<Vec<Signature>>;

    /// Extract the mints referenced by the confirmed transaction identified by
    /// `signature`. Implementations may return an empty vec for transactions
    /// with no token balances.
    fn transaction_mints(&self, signature: &Signature) -> anyhow::Result<Vec<Pubkey>>;
}

/// Seed the tracker by draining recent transactions across the configured
/// programs. Blocking: intended to be called from `tokio::task::spawn_blocking`.
pub fn seed_hot_mint_tracker<S: TransactionScanner>(
    scanner: &S,
    tracker: &HotMintTracker,
    config: &ColdStartScanConfig,
) -> anyhow::Result<ColdStartScanReport> {
    let start = Instant::now();
    let mut report = ColdStartScanReport::default();

    if config.programs.is_empty() {
        anyhow::bail!("ColdStartScanConfig.programs must not be empty");
    }

    for program in &config.programs {
        if start.elapsed() >= config.budget {
            report.budget_exhausted = true;
            break;
        }

        let signatures = match scanner.signatures_for_program(program, config.max_signatures) {
            Ok(sigs) => sigs,
            Err(err) => {
                tracing::warn!(
                    program = %program,
                    error = %err,
                    "cold-start: signatures_for_program failed"
                );
                continue;
            }
        };

        for signature in signatures {
            if start.elapsed() >= config.budget {
                report.budget_exhausted = true;
                break;
            }

            report.signatures_examined += 1;
            match scanner.transaction_mints(&signature) {
                Ok(mints) => {
                    if !mints.is_empty() {
                        let count = mints.len();
                        tracker.record_all(mints);
                        report.mints_recorded += count;
                    }
                }
                Err(err) => {
                    report.transaction_errors += 1;
                    tracing::debug!(
                        signature = %signature,
                        error = %err,
                        "cold-start: transaction_mints failed"
                    );
                }
            }
        }

        if report.budget_exhausted {
            break;
        }
    }

    report.elapsed = start.elapsed();
    Ok(report)
}

/// Real `TransactionScanner` backed by `solana_client::rpc_client::RpcClient`.
/// Kept in a separate impl so unit tests need not link RPC transport.
pub mod rpc {
    use super::*;
    use solana_client::rpc_client::RpcClient;
    use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
    use solana_client::rpc_config::RpcTransactionConfig;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_transaction_status::{
        option_serializer::OptionSerializer, EncodedTransaction, UiMessage,
        UiTransactionEncoding, UiTransactionStatusMeta,
    };
    use std::str::FromStr;
    use std::sync::Arc;

    pub struct RpcTransactionScanner {
        rpc: Arc<RpcClient>,
    }

    impl RpcTransactionScanner {
        pub fn new(rpc: Arc<RpcClient>) -> Self {
            Self { rpc }
        }
    }

    impl TransactionScanner for RpcTransactionScanner {
        fn signatures_for_program(
            &self,
            program: &Pubkey,
            limit: usize,
        ) -> anyhow::Result<Vec<Signature>> {
            // Solana JSON-RPC caps limit at 1000. Clamp defensively.
            let limit = limit.min(1000);
            let config = GetConfirmedSignaturesForAddress2Config {
                before: None,
                until: None,
                limit: Some(limit),
                commitment: Some(CommitmentConfig::confirmed()),
            };
            let result = self
                .rpc
                .get_signatures_for_address_with_config(program, config)?;
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

        fn transaction_mints(&self, signature: &Signature) -> anyhow::Result<Vec<Pubkey>> {
            let config = RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Json),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            };
            let tx = self.rpc.get_transaction_with_config(signature, config)?;
            let Some(meta) = tx.transaction.meta else {
                return Ok(Vec::new());
            };

            let mut mints = Vec::new();
            extract_from_meta(&meta, &mut mints);

            // Also scan the message account keys for any mint-shaped pubkeys.
            // Cheaper than nothing but coarse; token balances above are the
            // authoritative signal — this is a fallback for edge cases where
            // meta omits them.
            if mints.is_empty() {
                if let EncodedTransaction::Json(ui_tx) = tx.transaction.transaction {
                    if let UiMessage::Raw(raw) = ui_tx.message {
                        for key in raw.account_keys {
                            if let Ok(pk) = Pubkey::from_str(&key) {
                                mints.push(pk);
                            }
                        }
                    }
                }
            }

            dedup_preserving_order(&mut mints);
            Ok(mints)
        }
    }

    fn extract_from_meta(meta: &UiTransactionStatusMeta, out: &mut Vec<Pubkey>) {
        if let OptionSerializer::Some(balances) = &meta.pre_token_balances {
            for b in balances {
                if let Ok(pk) = Pubkey::from_str(&b.mint) {
                    out.push(pk);
                }
            }
        }
        if let OptionSerializer::Some(balances) = &meta.post_token_balances {
            for b in balances {
                if let Ok(pk) = Pubkey::from_str(&b.mint) {
                    out.push(pk);
                }
            }
        }
    }

    fn dedup_preserving_order(mints: &mut Vec<Pubkey>) {
        let mut seen = std::collections::HashSet::new();
        mints.retain(|pk| seen.insert(*pk));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn sig(byte: u8) -> Signature {
        Signature::from(<[u8; 64]>::from([byte; 64]))
    }

    struct MockScanner {
        sigs: Vec<Signature>,
        mints_per_sig: HashMap<Signature, Vec<Pubkey>>,
        transaction_err_after: Option<usize>,
        transaction_calls: AtomicUsize,
        // sleeps injected per get_transaction call, in the order they arrive
        transaction_delays_us: RefCell<Vec<u64>>,
    }

    impl MockScanner {
        fn new(sigs: Vec<Signature>) -> Self {
            Self {
                sigs,
                mints_per_sig: HashMap::new(),
                transaction_err_after: None,
                transaction_calls: AtomicUsize::new(0),
                transaction_delays_us: RefCell::new(Vec::new()),
            }
        }

        fn with_mints(mut self, sig: Signature, mints: Vec<Pubkey>) -> Self {
            self.mints_per_sig.insert(sig, mints);
            self
        }

        fn fail_after(mut self, n: usize) -> Self {
            self.transaction_err_after = Some(n);
            self
        }
    }

    impl TransactionScanner for MockScanner {
        fn signatures_for_program(
            &self,
            _program: &Pubkey,
            limit: usize,
        ) -> anyhow::Result<Vec<Signature>> {
            Ok(self.sigs.iter().take(limit).copied().collect())
        }

        fn transaction_mints(&self, signature: &Signature) -> anyhow::Result<Vec<Pubkey>> {
            let call_no = self.transaction_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(delay) = self.transaction_delays_us.borrow_mut().pop() {
                std::thread::sleep(Duration::from_micros(delay));
            }
            if let Some(threshold) = self.transaction_err_after {
                if call_no >= threshold {
                    anyhow::bail!("simulated rpc failure at call {}", call_no);
                }
            }
            Ok(self.mints_per_sig.get(signature).cloned().unwrap_or_default())
        }
    }

    fn cfg(programs: Vec<Pubkey>, budget_ms: u64) -> ColdStartScanConfig {
        ColdStartScanConfig {
            max_signatures: 1000,
            budget: Duration::from_millis(budget_ms),
            programs,
        }
    }

    #[test]
    fn scan_records_mints_from_all_signatures() {
        let scanner = MockScanner::new(vec![sig(1), sig(2), sig(3)])
            .with_mints(sig(1), vec![pk(10), pk(11)])
            .with_mints(sig(2), vec![pk(11)])
            .with_mints(sig(3), vec![pk(12)]);
        let tracker = HotMintTracker::new(1);
        let report =
            seed_hot_mint_tracker(&scanner, &tracker, &cfg(vec![pk(100)], 60_000)).unwrap();

        assert_eq!(report.signatures_examined, 3);
        assert_eq!(report.mints_recorded, 4);
        assert!(!report.budget_exhausted);
        // pk(11) appeared twice → count = 2, top of tracker.
        let top = tracker.top_n(5);
        assert_eq!(top[0].0, pk(11));
        assert_eq!(top[0].1, 2);
    }

    #[test]
    fn empty_programs_is_error() {
        let scanner = MockScanner::new(vec![]);
        let tracker = HotMintTracker::new(1);
        assert!(seed_hot_mint_tracker(&scanner, &tracker, &cfg(vec![], 1_000)).is_err());
    }

    #[test]
    fn per_transaction_error_does_not_abort_scan() {
        let scanner = MockScanner::new(vec![sig(1), sig(2), sig(3), sig(4)])
            .with_mints(sig(1), vec![pk(10)])
            .with_mints(sig(2), vec![pk(11)])
            .fail_after(2); // call 0 and 1 succeed, 2+ fail
        let tracker = HotMintTracker::new(1);
        let report =
            seed_hot_mint_tracker(&scanner, &tracker, &cfg(vec![pk(100)], 60_000)).unwrap();

        assert_eq!(report.signatures_examined, 4);
        assert_eq!(report.mints_recorded, 2);
        assert_eq!(report.transaction_errors, 2);
        assert!(!report.budget_exhausted);
    }

    #[test]
    fn budget_short_circuits_scan() {
        // Small budget, artificial delay per tx forces early exit.
        let scanner = MockScanner::new(vec![sig(1), sig(2), sig(3), sig(4), sig(5)])
            .with_mints(sig(1), vec![pk(10)])
            .with_mints(sig(2), vec![pk(11)]);
        // Each get_transaction sleeps 15ms; budget 20ms → at most one tx.
        *scanner.transaction_delays_us.borrow_mut() = vec![15_000; 5];
        let tracker = HotMintTracker::new(1);
        let report =
            seed_hot_mint_tracker(&scanner, &tracker, &cfg(vec![pk(100)], 20)).unwrap();

        assert!(report.budget_exhausted, "expected budget to short-circuit");
        assert!(
            report.signatures_examined >= 1 && report.signatures_examined <= 5,
            "examined = {}",
            report.signatures_examined
        );
    }

    #[test]
    fn multiple_programs_are_scanned_sequentially() {
        let scanner = MockScanner::new(vec![sig(1), sig(2)])
            .with_mints(sig(1), vec![pk(10)])
            .with_mints(sig(2), vec![pk(11)]);
        let tracker = HotMintTracker::new(1);
        let report = seed_hot_mint_tracker(
            &scanner,
            &tracker,
            &cfg(vec![pk(100), pk(101)], 60_000),
        )
        .unwrap();

        // Both programs return the same sig list from the mock, so we scan the
        // same 2 sigs twice → 4 examined, 4 mints recorded (with dedup only
        // inside tracker's aggregation, not per-scan).
        assert_eq!(report.signatures_examined, 4);
        assert_eq!(report.mints_recorded, 4);
    }
}
