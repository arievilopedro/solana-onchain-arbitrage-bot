//! Cross-module integration test for the M3b promoter stack.
//!
//! Exercises the public API surface end-to-end without spinning up RPC or a
//! real gRPC endpoint:
//!
//! 1. Seed the `HotMintTracker` via a fake `TransactionScanner` (cold-start).
//! 2. Compute the desired active set (`A = S ∪ top_(K−|S|)(D)`).
//! 3. Preassign seed mints into a `ShardSlotAllocator` and dynamically place
//!    the promoted hot mints (asserts sticky, least-loaded, "1 admit → 1 dirty
//!    slot" property).
//! 4. Walk one hot mint through the full `MintLifecycle` FSM to `Active`, and
//!    a second hot mint through `Cooling → Retiring → Retired`.
//! 5. Record the resulting events on `PromoterMetrics` and snapshot the
//!    counters to confirm they line up with the FSM outcomes.
//!
//! The intent is to catch API drift between the modules — signature changes to
//! `LifecycleEvent`, `ShardSlot`, or `PromoterMetrics` will break the compile
//! or the assertions here.

use solana_onchain_arbitrage_bot::hot_mints::HotMintTracker;
use solana_onchain_arbitrage_bot::promoter::coldstart::{
    seed_hot_mint_tracker, ColdStartScanConfig, TransactionScanner,
};
use solana_onchain_arbitrage_bot::promoter::lifecycle::{
    fail, step, FailureKind, LifecycleEvent, LifecyclePhase, MintLifecycle, StepOutcome,
};
use solana_onchain_arbitrage_bot::promoter::metrics::PromoterMetrics;
use solana_onchain_arbitrage_bot::promoter::ShardSlot;
use solana_onchain_arbitrage_bot::streams::shard_slot::ShardSlotAllocator;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

fn pk(byte: u8) -> Pubkey {
    Pubkey::new_from_array([byte; 32])
}

fn sig(byte: u8) -> Signature {
    Signature::from(<[u8; 64]>::from([byte; 64]))
}

/// Deterministic in-memory scanner: no RPC, no clocks, no sleeps.
struct FakeScanner {
    signatures: Vec<Signature>,
    mints_per_sig: HashMap<Signature, Vec<Pubkey>>,
}

impl TransactionScanner for FakeScanner {
    fn signatures_for_program(
        &self,
        _program: &Pubkey,
        limit: usize,
    ) -> anyhow::Result<Vec<Signature>> {
        Ok(self.signatures.iter().take(limit).copied().collect())
    }

    fn transaction_mints(&self, signature: &Signature) -> anyhow::Result<Vec<Pubkey>> {
        Ok(self.mints_per_sig.get(signature).cloned().unwrap_or_default())
    }
}

/// Desired active set = seed ∪ top_(target − |seed|)(tracker).
fn desired_active_set(
    tracker: &HotMintTracker,
    seed: &HashSet<Pubkey>,
    target: usize,
) -> HashSet<Pubkey> {
    let mut out = seed.clone();
    let hot_budget = target.saturating_sub(seed.len());
    for (mint, _count) in tracker.top_n(hot_budget * 4).into_iter() {
        if out.len() >= target {
            break;
        }
        out.insert(mint);
    }
    out
}

#[test]
fn end_to_end_promoter_flow_drives_seed_and_hot_mints_through_public_api() {
    // ---- Fixtures ------------------------------------------------------
    // Seed = 2 permanent mints (pk 1, 2). Hot mints = pk 10..14, in
    // decreasing frequency across the cold-start fixture.
    let seed_a = pk(1);
    let seed_b = pk(2);
    let hot_top = pk(10); // observed 3x
    let hot_mid = pk(11); // observed 2x
    let hot_low = pk(12); // observed 1x
    let hot_noise = pk(13); // observed 1x, tie-broken by pubkey
    let seed: HashSet<Pubkey> = [seed_a, seed_b].into_iter().collect();

    // Cold-start fixture: 5 signatures, per-tx mint lists chosen to yield
    // deterministic tracker frequencies.
    let mut mints_per_sig: HashMap<Signature, Vec<Pubkey>> = HashMap::new();
    mints_per_sig.insert(sig(1), vec![hot_top, hot_mid, hot_low]);
    mints_per_sig.insert(sig(2), vec![hot_top, hot_mid, hot_noise]);
    mints_per_sig.insert(sig(3), vec![hot_top]);
    mints_per_sig.insert(sig(4), vec![hot_noise]);
    mints_per_sig.insert(sig(5), vec![]);
    let scanner = FakeScanner {
        signatures: vec![sig(1), sig(2), sig(3), sig(4), sig(5)],
        mints_per_sig,
    };

    // ---- Step 1: cold-start seeds tracker ------------------------------
    let tracker = HotMintTracker::new(1);
    let cfg = ColdStartScanConfig {
        max_signatures: 100,
        budget: Duration::from_secs(30),
        programs: vec![pk(200)],
    };
    let report = seed_hot_mint_tracker(&scanner, &tracker, &cfg).expect("scan ok");
    assert_eq!(report.signatures_examined, 5);
    assert!(!report.budget_exhausted);
    assert!(report.mints_recorded >= 4);

    // Tracker top-1 should be the mint we injected 3x.
    let top = tracker.top_n(1);
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].0, hot_top);
    assert_eq!(top[0].1, 3);

    // ---- Step 2: desired active set ------------------------------------
    // Target |A| = 4: seed (2) + top-2 hot mints.
    let desired = desired_active_set(&tracker, &seed, 4);
    assert!(desired.contains(&seed_a));
    assert!(desired.contains(&seed_b));
    assert!(desired.contains(&hot_top));
    assert!(desired.contains(&hot_mid));
    assert_eq!(desired.len(), 4);

    // ---- Step 3: shard slot allocation ---------------------------------
    // 2 slots × 3 mints/slot = capacity 6 → fits |A|=4 comfortably.
    let mut alloc = ShardSlotAllocator::new(2, 3).expect("allocator");
    alloc
        .preassign_seed(&[seed_a, seed_b])
        .expect("seed preassign fits");
    // Seed is preassigned deterministically: sorted-by-pubkey, round-robin.
    let seed_a_slot = alloc.slot_of(&seed_a).expect("seed_a placed");
    let seed_b_slot = alloc.slot_of(&seed_b).expect("seed_b placed");
    assert_ne!(seed_a_slot, seed_b_slot, "seed spread across slots");

    // Snapshot current active set (seed only), then admit hot mints and
    // check the "1 admit → 1 dirty slot" property.
    let before: HashSet<Pubkey> = seed.clone();
    let hot_top_slot = alloc.assign(hot_top).expect("hot_top assigned");
    let mut after: HashSet<Pubkey> = seed.clone();
    after.insert(hot_top);
    let dirty = alloc.dirty_slots(&before, &after);
    assert_eq!(dirty.len(), 1, "single admit dirties exactly one slot");
    assert!(dirty.contains(&hot_top_slot));

    let hot_mid_slot = alloc.assign(hot_mid).expect("hot_mid assigned");
    // Sticky: re-assigning returns the same slot.
    assert_eq!(alloc.assign(hot_top), Some(hot_top_slot));
    assert_eq!(alloc.assign(hot_mid), Some(hot_mid_slot));

    // Every mint in the desired set now has a slot.
    for mint in &desired {
        assert!(
            alloc.slot_of(mint).is_some(),
            "mint {mint} unassigned after admit"
        );
    }

    // ---- Step 4a: FSM happy path for hot_top ---------------------------
    let metrics = PromoterMetrics::new();
    let mut hot_top_fsm = MintLifecycle::new(hot_top, 0, false);
    let hot_top_alt = pk(90);

    let stages: &[(LifecycleEvent, LifecyclePhase, u128)] = &[
        (LifecycleEvent::DiscoveryOk, LifecyclePhase::PoolsDiscovered, 10),
        (LifecycleEvent::AtaOk, LifecyclePhase::AtasReady, 20),
        (
            LifecycleEvent::AltOk {
                primary_shard: hot_top_alt,
            },
            LifecyclePhase::AltReady,
            30,
        ),
        (
            LifecycleEvent::RegistryAdmitOk {
                shard_slot: hot_top_slot,
            },
            LifecyclePhase::RegistryLive,
            40,
        ),
        (LifecycleEvent::GrpcAckOk, LifecyclePhase::GrpcSubscribed, 50),
        (LifecycleEvent::FirstUpdateSeen, LifecyclePhase::Active, 60),
    ];
    for (event, expected_phase, now_ms) in stages.iter().cloned() {
        let out = step(&mut hot_top_fsm, event.clone(), now_ms);
        assert_eq!(
            out,
            StepOutcome::Advanced,
            "event {event:?} should advance FSM from {:?}",
            hot_top_fsm.phase
        );
        assert_eq!(hot_top_fsm.phase, expected_phase);
        if matches!(event, LifecycleEvent::RegistryAdmitOk { .. }) {
            metrics.record_promoted();
        }
        if matches!(event, LifecycleEvent::GrpcAckOk) {
            metrics.record_resubscribe_ok();
        }
    }
    assert_eq!(hot_top_fsm.shard_slot, Some(hot_top_slot));
    assert_eq!(hot_top_fsm.primary_alt_shard, Some(hot_top_alt));

    // ---- Step 4b: FSM demote path for hot_mid --------------------------
    let mut hot_mid_fsm = MintLifecycle::new(hot_mid, 100, false);
    // Fast-forward through the happy path via events; we already verified
    // per-transition correctness above.
    step(&mut hot_mid_fsm, LifecycleEvent::DiscoveryOk, 110);
    step(&mut hot_mid_fsm, LifecycleEvent::AtaOk, 120);
    step(
        &mut hot_mid_fsm,
        LifecycleEvent::AltOk {
            primary_shard: pk(91),
        },
        130,
    );
    step(
        &mut hot_mid_fsm,
        LifecycleEvent::RegistryAdmitOk {
            shard_slot: hot_mid_slot,
        },
        140,
    );
    metrics.record_promoted();
    step(&mut hot_mid_fsm, LifecycleEvent::GrpcAckOk, 150);
    metrics.record_resubscribe_ok();
    step(&mut hot_mid_fsm, LifecycleEvent::FirstUpdateSeen, 160);
    assert_eq!(hot_mid_fsm.phase, LifecyclePhase::Active);

    // Demote it.
    assert_eq!(
        step(&mut hot_mid_fsm, LifecycleEvent::LeftTopN, 200),
        StepOutcome::Advanced
    );
    assert_eq!(hot_mid_fsm.phase, LifecyclePhase::Cooling);
    assert_eq!(
        step(&mut hot_mid_fsm, LifecycleEvent::GraceElapsed, 300),
        StepOutcome::Advanced
    );
    assert_eq!(hot_mid_fsm.phase, LifecyclePhase::Retiring);
    assert_eq!(
        step(&mut hot_mid_fsm, LifecycleEvent::RetireDone, 310),
        StepOutcome::Advanced
    );
    assert_eq!(hot_mid_fsm.phase, LifecyclePhase::Retired);
    assert!(hot_mid_fsm.shard_slot.is_none(), "retire clears slot");
    metrics.record_demoted();
    // Slot allocator must release too, mirroring the FSM.
    assert_eq!(alloc.release(hot_mid), Some(hot_mid_slot));
    assert!(alloc.slot_of(&hot_mid).is_none());

    // ---- Step 4c: seed mint refuses to be demoted ----------------------
    let mut seed_fsm = MintLifecycle::new(seed_a, 0, true);
    seed_fsm.phase = LifecyclePhase::Active;
    assert_eq!(
        step(&mut seed_fsm, LifecycleEvent::LeftTopN, 200),
        StepOutcome::Ignored,
        "seed mint must never enter Cooling"
    );
    assert_eq!(seed_fsm.phase, LifecyclePhase::Active);

    // ---- Step 4d: failure path exercises record_failure ----------------
    let mut failing = MintLifecycle::new(pk(50), 0, false);
    // Simulate 3 discovery errors then a terminal fail — mirrors the
    // orchestrator's "retry budget exhausted → record_failure" behavior.
    for _ in 0..3 {
        step(
            &mut failing,
            LifecycleEvent::DiscoveryErr("timeout".into()),
            5,
        );
    }
    fail(&mut failing, FailureKind::Discovery, "budget".into(), 10);
    assert_eq!(
        failing.phase,
        LifecyclePhase::Failed(FailureKind::Discovery)
    );
    metrics.record_failure(FailureKind::Discovery);

    // Also record a gRPC ack failure for coverage of failure slot 4.
    metrics.record_resubscribe_err();
    metrics.record_failure(FailureKind::GrpcSubscribe);

    // ---- Step 5: metrics snapshot reflects FSM outcomes ----------------
    metrics.record_tick(17);
    metrics.set_active_count(2);
    metrics.set_cooling_count(0);
    let snap = metrics.snapshot();

    assert_eq!(snap.ticks_total, 1);
    assert_eq!(snap.mints_promoted_total, 2, "hot_top + hot_mid promoted");
    assert_eq!(snap.mints_demoted_total, 1, "hot_mid demoted");
    assert_eq!(snap.grpc_resubscribes_total, 2);
    assert_eq!(snap.grpc_resubscribe_errors_total, 1);
    assert_eq!(snap.lifecycle_failures_total[0], 1, "Discovery");
    assert_eq!(snap.lifecycle_failures_total[4], 1, "GrpcSubscribe");
    assert_eq!(snap.current_active_count, 2);
    assert_eq!(snap.current_cooling_count, 0);
    assert_eq!(snap.last_tick_duration_ms, 17);

    // ---- Cross-module invariant: slot capacity respected ---------------
    for slot_idx in 0..alloc.num_slots() {
        let slot = ShardSlot::new(slot_idx as u16);
        let mints = alloc
            .mints_for_slot(slot)
            .expect("slot exists");
        assert!(
            mints.len() <= alloc.per_slot_capacity(),
            "slot {slot_idx} over-capacity: {}",
            mints.len()
        );
    }
}
