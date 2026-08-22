//! JSONL event schema for wallet_probe output.
//!
//! All events are serialized to a single JSONL file with a `type` discriminator.

use serde::Serialize;

/// Discriminated union of all event kinds emitted by the probe.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalletProbeEvent {
    WalletTx(WalletTxEvent),
    Landing(LandingEvent),
    Context(ContextEvent),
    ProbeStatus(ProbeStatusEvent),
}

/// A transaction observed being broadcast by a monitored wallet (via RabbitStream shred).
#[derive(Debug, Clone, Serialize)]
pub struct WalletTxEvent {
    pub ts_ms: u128,
    pub signature: String,
    pub slot: u64,
    pub wallet: String,
    pub mints: Vec<String>,
    pub pools: Vec<String>,
    pub programs: Vec<String>,
    pub tip: Option<ParsedTipRow>,
    pub priority_fee_micro_lamports: Option<u64>,
    pub cu_limit: Option<u32>,
    pub tx_size_bytes: usize,
    pub is_versioned_v0: bool,
    pub has_advance_nonce: bool,
    pub uses_alt: bool,
    pub alt_writable_count: usize,
    pub alt_readonly_count: usize,
    pub flashx_axion_seen: bool,
    pub mevi_program_seen: bool,
    pub instruction_count: usize,
    pub trigger_candidates: Vec<TriggerCandidate>,
    /// True when meta.err is definitively `Some` (not just missing).
    pub meta_err_present: bool,
}

/// Landing / drop reconciliation for a signature previously seen in wallet_stream.
#[derive(Debug, Clone, Serialize)]
pub struct LandingEvent {
    pub ts_ms: u128,
    pub signature: String,
    pub wallet: String,
    /// Slot from wallet_stream (shred / propagation slot).
    pub broadcast_slot: Option<u64>,
    /// Slot where the tx landed (from Yellowstone or getSignatureStatuses).
    pub landed_slot: Option<u64>,
    /// True when no landing was observed within the deadline window.
    pub dropped: bool,
    /// True when the tx landed but with an execution error.
    pub landed_with_err: bool,
    pub err_debug: Option<String>,
    /// `landed_slot - broadcast_slot` when both known.
    pub slot_gap: Option<i64>,
    pub confirmation_ms: Option<u128>,
    /// Source that produced this landing observation.
    pub source: LandingSource,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LandingSource {
    /// Observed in the landing_stream (Yellowstone, wallet-filtered).
    Yellowstone,
    /// Determined via HTTP `getSignatureStatuses` polling.
    RpcStatus,
    /// Deadline expired with no landing signal.
    Deadline,
}

/// A context tx observed touching a DEX/trigger program (used for causality scoring).
#[derive(Debug, Clone, Serialize)]
pub struct ContextEvent {
    pub ts_ms: u128,
    pub signature: String,
    pub slot: u64,
    pub programs: Vec<String>,
    pub mints: Vec<String>,
    pub pools: Vec<String>,
    pub sol_volume_lamports: Option<u64>,
    pub flashx_axion_seen: bool,
}

/// A candidate trigger, i.e. a context tx that plausibly caused the wallet tx.
#[derive(Debug, Clone, Serialize)]
pub struct TriggerCandidate {
    pub signature: String,
    pub slot: u64,
    /// Milliseconds between the candidate ctx tx and the wallet tx (positive = ctx first).
    pub time_delta_ms: i128,
    /// Wallet-visible programs that matched this candidate.
    pub matched_programs: Vec<String>,
    /// Wallet-visible mints that matched this candidate.
    pub matched_mints: Vec<String>,
    /// Wallet-visible pools that matched this candidate.
    pub matched_pools: Vec<String>,
    /// Higher = more likely trigger. See `context_buffer::score`.
    pub score: i64,
}

/// Parsed tip record embedded inside a WalletTxEvent.
#[derive(Debug, Clone, Serialize)]
pub struct ParsedTipRow {
    pub account: String,
    pub amount_lamports: u64,
    pub kind: String,
}

/// One-off status event emitted at startup / shutdown / stream reconnect.
#[derive(Debug, Clone, Serialize)]
pub struct ProbeStatusEvent {
    pub ts_ms: u128,
    pub event: String,
    pub detail: Option<String>,
}
