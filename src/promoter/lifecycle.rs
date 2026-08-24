//! Per-mint lifecycle state machine.
//!
//! Pure reducer: `step(state, event, now_ms)` performs no I/O. The
//! orchestrator (Phase 6) is responsible for translating side-effectful
//! results (RPC completion, gRPC ack, tracker deltas) into `LifecycleEvent`s
//! and applying them here.
//!
//! Invariants (see M3b plan):
//! - Seed mints never enter `Cooling`, `Retiring`, or `Retired`. Callers must
//!   short-circuit demotion for seed mints; the reducer enforces this by
//!   ignoring `LeftTopN` events for seed-flagged lifecycles.
//! - Phase progression is monotonic within a "generation" (advancement or
//!   `Failed`). `RetryFromFailure` resets to `Discovered` and increments the
//!   generation counter so metrics can distinguish retries.
//! - `attempts` is monotonic per phase (never decreases) and is used to gate
//!   retry policy.

use crate::promoter::ShardSlot;
use solana_program::pubkey::Pubkey;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Coarse-grained lifecycle phase used both as FSM state and metric label.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum LifecyclePhase {
    Discovered,
    PoolsDiscovered,
    AtasReady,
    AltReady,
    RegistryLive,
    GrpcSubscribed,
    Active,
    Cooling,
    Retiring,
    Retired,
    Failed(FailureKind),
}

/// Reason a mint entered the `Failed` phase. Retry policy in the orchestrator
/// can inspect this to decide backoff / permanent skip.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FailureKind {
    Discovery,
    AtaCreation,
    AltExtension,
    RegistryAdmit,
    GrpcSubscribe,
}

/// Events consumed by the reducer. Each variant maps to a completion (Ok/Err)
/// of a side-effectful task the orchestrator kicked off, or a tracker signal.
#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    /// Discovery finished successfully.
    DiscoveryOk,
    /// Discovery failed. Retry count is incremented; caller decides when the
    /// retry budget is exhausted and issues `Fail` explicitly.
    DiscoveryErr(Arc<str>),

    AtaOk,
    AtaErr(Arc<str>),

    /// ALT extension confirmed; carries the primary shard pubkey.
    AltOk { primary_shard: Pubkey },
    AltErr(Arc<str>),

    /// Registry admit succeeded; carries the shard slot allocation.
    RegistryAdmitOk { shard_slot: ShardSlot },
    RegistryAdmitErr(Arc<str>),

    /// gRPC `SubscribeRequest` was applied by the worker.
    GrpcAckOk,
    GrpcAckErr(Arc<str>),

    /// First stream update observed for the mint (fast path to `Active`).
    FirstUpdateSeen,
    /// Warmup timer elapsed since entering `GrpcSubscribed`.
    WarmupElapsed,

    /// Tracker says the mint dropped out of top-N. Ignored for seed mints.
    LeftTopN,
    /// Tracker says the mint re-entered top-N while cooling.
    ReturnedToTopN,
    /// Grace period expired in `Cooling`; caller wants to retire.
    GraceElapsed,

    /// Retire flow (unsubscribe + evict) finished.
    RetireDone,

    /// Push a failed lifecycle back into `Discovered` for another attempt.
    RetryFromFailure,
}

/// Per-mint FSM state. The orchestrator owns a `HashMap<Pubkey, MintLifecycle>`
/// and drives one step per tick.
#[derive(Debug, Clone)]
pub struct MintLifecycle {
    pub mint: Pubkey,
    pub phase: LifecyclePhase,
    pub entered_phase_at_ms: u128,
    /// Retry attempts per phase. Incremented on `*Err` events; consulted by
    /// the orchestrator to enforce max-retry budget.
    pub attempts: BTreeMap<LifecyclePhase, u32>,
    pub last_error: Option<Arc<str>>,
    /// Assigned by `RegistryAdmitOk`; cleared on retire.
    pub shard_slot: Option<ShardSlot>,
    /// Populated by `AltOk`; identifies the primary ALT holding the mint's
    /// route accounts.
    pub primary_alt_shard: Option<Pubkey>,
    /// Retries across the full FSM (each `RetryFromFailure` increments).
    pub generation: u32,
    /// Whether this mint is part of the permanent seed. Seed mints ignore
    /// `LeftTopN` / `GraceElapsed` (never demoted).
    pub is_seed: bool,
}

impl MintLifecycle {
    pub fn new(mint: Pubkey, now_ms: u128, is_seed: bool) -> Self {
        Self {
            mint,
            phase: LifecyclePhase::Discovered,
            entered_phase_at_ms: now_ms,
            attempts: BTreeMap::new(),
            last_error: None,
            shard_slot: None,
            primary_alt_shard: None,
            generation: 0,
            is_seed,
        }
    }

    /// Time spent in the current phase.
    pub fn age_ms(&self, now_ms: u128) -> u128 {
        now_ms.saturating_sub(self.entered_phase_at_ms)
    }

    /// Number of attempts recorded for the current phase.
    pub fn current_phase_attempts(&self) -> u32 {
        self.attempts.get(&self.phase).copied().unwrap_or(0)
    }

    fn set_phase(&mut self, phase: LifecyclePhase, now_ms: u128) {
        if self.phase != phase {
            self.phase = phase;
            self.entered_phase_at_ms = now_ms;
        }
    }

    fn record_error(&mut self, err: Arc<str>) {
        let counter = self.attempts.entry(self.phase).or_insert(0);
        *counter = counter.saturating_add(1);
        self.last_error = Some(err);
    }
}

/// Outcome of a `step` call. Callers can use this to drive metrics without
/// re-inspecting the state.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StepOutcome {
    /// Phase changed to a new value.
    Advanced,
    /// Event applied but phase unchanged (e.g. attempts counter incremented).
    NoTransition,
    /// Event was invalid for the current phase and ignored. Callers may log
    /// this at debug level; it should be rare in practice and never on the
    /// happy path.
    Ignored,
}

/// Apply `event` to `state`, returning the outcome. Pure with respect to
/// `state`: no I/O, no clock reads (caller supplies `now_ms`).
pub fn step(state: &mut MintLifecycle, event: LifecycleEvent, now_ms: u128) -> StepOutcome {
    use LifecycleEvent as E;
    use LifecyclePhase as P;

    match (state.phase, event) {
        (P::Discovered, E::DiscoveryOk) => {
            state.set_phase(P::PoolsDiscovered, now_ms);
            state.last_error = None;
            StepOutcome::Advanced
        }
        (P::Discovered, E::DiscoveryErr(err)) => {
            state.record_error(err);
            StepOutcome::NoTransition
        }

        (P::PoolsDiscovered, E::AtaOk) => {
            state.set_phase(P::AtasReady, now_ms);
            state.last_error = None;
            StepOutcome::Advanced
        }
        (P::PoolsDiscovered, E::AtaErr(err)) => {
            state.record_error(err);
            StepOutcome::NoTransition
        }

        (P::AtasReady, E::AltOk { primary_shard }) => {
            state.primary_alt_shard = Some(primary_shard);
            state.set_phase(P::AltReady, now_ms);
            state.last_error = None;
            StepOutcome::Advanced
        }
        (P::AtasReady, E::AltErr(err)) => {
            state.record_error(err);
            StepOutcome::NoTransition
        }

        (P::AltReady, E::RegistryAdmitOk { shard_slot }) => {
            state.shard_slot = Some(shard_slot);
            state.set_phase(P::RegistryLive, now_ms);
            state.last_error = None;
            StepOutcome::Advanced
        }
        (P::AltReady, E::RegistryAdmitErr(err)) => {
            state.record_error(err);
            StepOutcome::NoTransition
        }

        (P::RegistryLive, E::GrpcAckOk) => {
            state.set_phase(P::GrpcSubscribed, now_ms);
            state.last_error = None;
            StepOutcome::Advanced
        }
        (P::RegistryLive, E::GrpcAckErr(err)) => {
            state.record_error(err);
            StepOutcome::NoTransition
        }

        (P::GrpcSubscribed, E::FirstUpdateSeen) | (P::GrpcSubscribed, E::WarmupElapsed) => {
            state.set_phase(P::Active, now_ms);
            StepOutcome::Advanced
        }

        // Demotion path. Seed mints are held permanent.
        (P::Active, E::LeftTopN) => {
            if state.is_seed {
                StepOutcome::Ignored
            } else {
                state.set_phase(P::Cooling, now_ms);
                StepOutcome::Advanced
            }
        }
        (P::Cooling, E::ReturnedToTopN) => {
            state.set_phase(P::Active, now_ms);
            StepOutcome::Advanced
        }
        (P::Cooling, E::GraceElapsed) => {
            if state.is_seed {
                StepOutcome::Ignored
            } else {
                state.set_phase(P::Retiring, now_ms);
                StepOutcome::Advanced
            }
        }
        (P::Retiring, E::RetireDone) => {
            state.set_phase(P::Retired, now_ms);
            state.shard_slot = None;
            StepOutcome::Advanced
        }

        // Retry from any Failed phase back to Discovered. Bumps generation and
        // clears per-phase attempts so the new attempt starts fresh.
        (P::Failed(_), E::RetryFromFailure) => {
            state.generation = state.generation.saturating_add(1);
            state.attempts.clear();
            state.last_error = None;
            state.set_phase(P::Discovered, now_ms);
            StepOutcome::Advanced
        }

        // Anything else is invalid for this phase and ignored.
        _ => StepOutcome::Ignored,
    }
}

/// Move a lifecycle to `Failed(kind)`. Kept as a helper so the orchestrator
/// can express "retry budget exhausted" without a special event variant.
pub fn fail(state: &mut MintLifecycle, kind: FailureKind, err: Arc<str>, now_ms: u128) {
    state.last_error = Some(err);
    state.set_phase(LifecyclePhase::Failed(kind), now_ms);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    fn new_hot(now: u128) -> MintLifecycle {
        MintLifecycle::new(pk(1), now, false)
    }

    fn new_seed(now: u128) -> MintLifecycle {
        MintLifecycle::new(pk(2), now, true)
    }

    #[test]
    fn happy_path_progresses_to_active() {
        let mut s = new_hot(0);
        assert_eq!(step(&mut s, LifecycleEvent::DiscoveryOk, 10), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::PoolsDiscovered);
        assert_eq!(s.entered_phase_at_ms, 10);
        assert_eq!(step(&mut s, LifecycleEvent::AtaOk, 20), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::AtasReady);
        assert_eq!(
            step(&mut s, LifecycleEvent::AltOk { primary_shard: pk(9) }, 30),
            StepOutcome::Advanced
        );
        assert_eq!(s.phase, LifecyclePhase::AltReady);
        assert_eq!(s.primary_alt_shard, Some(pk(9)));
        assert_eq!(
            step(
                &mut s,
                LifecycleEvent::RegistryAdmitOk { shard_slot: ShardSlot::new(2) },
                40,
            ),
            StepOutcome::Advanced
        );
        assert_eq!(s.phase, LifecyclePhase::RegistryLive);
        assert_eq!(s.shard_slot, Some(ShardSlot::new(2)));
        assert_eq!(step(&mut s, LifecycleEvent::GrpcAckOk, 50), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::GrpcSubscribed);
        assert_eq!(step(&mut s, LifecycleEvent::FirstUpdateSeen, 60), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::Active);
    }

    #[test]
    fn warmup_can_promote_without_first_update() {
        let mut s = new_hot(0);
        s.phase = LifecyclePhase::GrpcSubscribed;
        assert_eq!(step(&mut s, LifecycleEvent::WarmupElapsed, 100), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::Active);
    }

    #[test]
    fn err_events_increment_attempts_without_transition() {
        let mut s = new_hot(0);
        for i in 1..=3 {
            let outcome = step(&mut s, LifecycleEvent::DiscoveryErr("boom".into()), 5);
            assert_eq!(outcome, StepOutcome::NoTransition);
            assert_eq!(s.current_phase_attempts(), i);
            assert_eq!(s.phase, LifecyclePhase::Discovered);
        }
        assert!(s.last_error.is_some());
    }

    #[test]
    fn seed_ignores_left_top_n_and_grace() {
        let mut s = new_seed(0);
        s.phase = LifecyclePhase::Active;
        assert_eq!(step(&mut s, LifecycleEvent::LeftTopN, 10), StepOutcome::Ignored);
        assert_eq!(s.phase, LifecyclePhase::Active);

        // Force-drive a seed lifecycle into Cooling to prove GraceElapsed is
        // still ignored (belt-and-suspenders; upstream should never do this).
        s.phase = LifecyclePhase::Cooling;
        assert_eq!(step(&mut s, LifecycleEvent::GraceElapsed, 20), StepOutcome::Ignored);
        assert_eq!(s.phase, LifecyclePhase::Cooling);
    }

    #[test]
    fn hot_mint_demotion_flow() {
        let mut s = new_hot(0);
        s.phase = LifecyclePhase::Active;
        assert_eq!(step(&mut s, LifecycleEvent::LeftTopN, 100), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::Cooling);
        assert_eq!(s.entered_phase_at_ms, 100);

        // Returning to top-N reactivates without going through discovery.
        assert_eq!(step(&mut s, LifecycleEvent::ReturnedToTopN, 150), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::Active);

        // Second demotion, this time grace elapses.
        assert_eq!(step(&mut s, LifecycleEvent::LeftTopN, 200), StepOutcome::Advanced);
        assert_eq!(step(&mut s, LifecycleEvent::GraceElapsed, 300), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::Retiring);
        assert_eq!(step(&mut s, LifecycleEvent::RetireDone, 310), StepOutcome::Advanced);
        assert_eq!(s.phase, LifecyclePhase::Retired);
        assert!(s.shard_slot.is_none());
    }

    #[test]
    fn fail_helper_moves_to_failed_and_retry_resets() {
        let mut s = new_hot(0);
        // Simulate 3 discovery failures then explicit fail.
        for _ in 0..3 {
            step(&mut s, LifecycleEvent::DiscoveryErr("net".into()), 10);
        }
        fail(&mut s, FailureKind::Discovery, "budget".into(), 20);
        assert_eq!(s.phase, LifecyclePhase::Failed(FailureKind::Discovery));
        assert_eq!(s.last_error.as_deref(), Some("budget"));

        assert_eq!(
            step(&mut s, LifecycleEvent::RetryFromFailure, 100),
            StepOutcome::Advanced
        );
        assert_eq!(s.phase, LifecyclePhase::Discovered);
        assert_eq!(s.generation, 1);
        assert_eq!(s.current_phase_attempts(), 0);
        assert!(s.last_error.is_none());
    }

    #[test]
    fn invalid_events_are_ignored() {
        let mut s = new_hot(0);
        // AtaOk before DiscoveryOk shouldn't advance.
        assert_eq!(step(&mut s, LifecycleEvent::AtaOk, 5), StepOutcome::Ignored);
        assert_eq!(s.phase, LifecyclePhase::Discovered);
    }

    #[test]
    fn set_phase_updates_entry_time_only_on_change() {
        let mut s = new_hot(50);
        s.set_phase(LifecyclePhase::Discovered, 500);
        assert_eq!(s.entered_phase_at_ms, 50, "same phase should not reset timer");
        s.set_phase(LifecyclePhase::PoolsDiscovered, 500);
        assert_eq!(s.entered_phase_at_ms, 500);
    }

    #[test]
    fn attempts_are_monotonic_across_multiple_errs() {
        let mut s = new_hot(0);
        step(&mut s, LifecycleEvent::DiscoveryErr("a".into()), 1);
        step(&mut s, LifecycleEvent::DiscoveryOk, 2);
        // Advancing does not clear attempts map from prior phase.
        assert_eq!(*s.attempts.get(&LifecyclePhase::Discovered).unwrap(), 1);
        step(&mut s, LifecycleEvent::AtaErr("b".into()), 3);
        step(&mut s, LifecycleEvent::AtaErr("c".into()), 4);
        assert_eq!(*s.attempts.get(&LifecyclePhase::PoolsDiscovered).unwrap(), 2);
    }

    #[test]
    fn age_ms_saturates_on_time_regression() {
        let s = new_hot(1000);
        assert_eq!(s.age_ms(500), 0, "clock going backwards yields zero age");
        assert_eq!(s.age_ms(1500), 500);
    }
}
