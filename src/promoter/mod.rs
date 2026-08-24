//! Hot-mint promoter: drives the `HotMintTracker` top-N into the runtime
//! registry, ATA/ALT infrastructure, and gRPC subscriptions.
//!
//! Structure:
//! - `lifecycle`: pure per-mint state machine (no I/O).
//! - later phases: cold-start scanner, shard slot allocator, orchestrator.

pub mod coldstart;
pub mod lifecycle;
pub mod metrics;
pub mod orchestrator;

use solana_program::pubkey::Pubkey;

/// Opaque handle for one of the fixed-size gRPC subscription workers. Values
/// are allocated by the shard slot allocator (Phase 4). The lifecycle FSM
/// carries this only as an opaque tag; it never inspects the numeric value.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ShardSlot(pub u16);

impl ShardSlot {
    pub const fn new(index: u16) -> Self {
        Self(index)
    }

    pub const fn index(self) -> u16 {
        self.0
    }
}

/// Convenience alias for the mint identifier tracked by the promoter.
pub type PromotedMint = Pubkey;
