//! Passive wallet observation module.
//!
//! Watches 1-N target wallets across three subscribers:
//!
//! * `wallet_stream` — RabbitStream (shred) filtered by target wallet accounts.
//!   Captures txs at propagation time, **including those that never land**.
//!   Meta is partial on Shyft RabbitStream (ALTs resolved, no logs/balances/err).
//! * `context_stream` — RabbitStream filtered by DEX / trigger programs. Feeds a
//!   short rolling buffer used to score causality candidates for each wallet tx.
//! * `landing_stream` — Yellowstone gRPC filtered by target wallet accounts.
//!   Provides landed slot / err / slot gap to reconcile with wallet_stream sigs.
//!
//! All events are written as JSONL to a single output file (async, non-blocking).
//! This module is **read-only** with respect to the chain — no signing, no
//! transaction submission.

pub mod context_buffer;
pub mod tx_parse;
pub mod types;
pub mod writer;

pub use context_buffer::{ContextBuffer, ContextEntry};
pub use tx_parse::{classify_tip_account, ParsedTip, ParsedWalletTx, TipKind};
pub use types::{
    ContextEvent, LandingEvent, LandingSource, ProbeStatusEvent, TriggerCandidate,
    WalletProbeEvent, WalletTxEvent,
};
pub use writer::JsonlWriter;

#[cfg(feature = "geyser")]
pub mod context_stream;
#[cfg(feature = "geyser")]
pub mod landing_stream;
#[cfg(feature = "geyser")]
pub mod wallet_stream;

#[cfg(feature = "geyser")]
pub use tx_parse::parse_wallet_tx;
