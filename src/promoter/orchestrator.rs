//! Promoter orchestrator: drives the `HotMintTracker` top-N into the
//! runtime registry, ATA/ALT infrastructure and gRPC subscription workers.
//!
//! Design summary (M3b Phase 6):
//! - **Tick loop**: every `tick_ms` recompute `A = seed ∪ top_(K−|S|)` and
//!   update lifecycles (create for additions, `LeftTopN` for removals).
//! - **Event loop**: spawned per-mint tasks (discovery / ATA / ALT / gRPC
//!   Replace) send `InternalEvent`s back over an unbounded channel. The
//!   orchestrator applies each event to the FSM and, when a phase advances,
//!   kicks off the next stage.
//! - **Serialisation**: one op in-flight per mint (`pending_ops`) and one
//!   Replace in-flight per shard slot (`in_flight_by_slot`). Different slots
//!   run in parallel.
//! - **Seed invariant**: seed mints never see `LeftTopN`/`GraceElapsed`
//!   (enforced both here and in the lifecycle reducer).

use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{debug, error, info, warn};

use crate::alt::{promote_mint_into_shard_async, PromoteMintReport, StableMintRouteAccounts};
use crate::ata::ensure_ata_async;
use crate::config::PromoterConfig;
use crate::discovery::{discover_mint_async, ControlledRpcBootstrap, MintDiscoveryResult};
use crate::hot_mints::HotMintTracker;
use crate::promoter::lifecycle::{
    fail, step, FailureKind, LifecycleEvent, LifecyclePhase, MintLifecycle,
};
use crate::promoter::metrics::PromoterMetrics;
use crate::promoter::ShardSlot;
use crate::registry::{MintRuntimeState, RuntimeRegistry};
use crate::streams::grpc::{GrpcWorkerHandle, SubscriptionAck, SubscriptionCommand};
use crate::streams::shard_slot::ShardSlotAllocator;

/// Result reported back to the tick loop by a spawned side-effect task.
#[derive(Debug)]
pub enum InternalEvent {
    Discovery {
        mint: Pubkey,
        result: Result<MintDiscoveryResult>,
    },
    Ata {
        mint: Pubkey,
        result: Result<Pubkey>,
    },
    Alt {
        mint: Pubkey,
        result: Result<PromoteMintReport>,
    },
    /// Local admit result: the orchestrator performs this synchronously in
    /// the event loop, but wraps the outcome as an event so all lifecycle
    /// transitions flow through one path.
    RegistryAdmit {
        mint: Pubkey,
        result: Result<ShardSlot>,
    },
    GrpcAck {
        slot: ShardSlot,
        mints: Vec<Pubkey>,
        result: Result<SubscriptionAck>,
    },
}

/// Immutable dependency bundle passed into the orchestrator.
pub struct PromoterInputs {
    pub config: PromoterConfig,
    pub rpc: Arc<RpcClient>,
    pub wallet: Arc<Keypair>,
    pub tracker: Arc<HotMintTracker>,
    pub registry: Arc<Mutex<RuntimeRegistry>>,
    pub bootstrap: Arc<ControlledRpcBootstrap>,
    pub grpc_workers: Vec<GrpcWorkerHandle>,
    pub shard_alloc: Arc<Mutex<ShardSlotAllocator>>,
    pub seed: Arc<HashSet<Pubkey>>,
    /// Route shard state file (single-mint promotion writes to it).
    pub state_file: PathBuf,
    /// Global lock serializing every writer that mutates `state_file`.
    /// The promoter's ALT phase, the startup maintenance path in main.rs and
    /// the live gRPC-triggered `maintain_live_route_shards` all share this
    /// mutex. Without it, concurrent `load -> reconcile -> plan -> send ->
    /// apply -> save` cycles race and produce `local used > on-chain len`
    /// corruption (see `RouteShardPlanner::reconcile_with_chain`).
    pub state_file_lock: Arc<Mutex<()>>,
    pub shard_capacity: usize,
    pub auto_create: bool,
    pub auto_extend: bool,
    /// Passed through to `StableMintRouteAccounts::from_mint_runtime_state`.
    pub min_pool_base_liquidity_lamports: u64,
    pub max_pool_state_age_ms: u64,
}

pub struct PromoterOrchestrator {
    inputs: PromoterInputs,
    lifecycles: Arc<Mutex<HashMap<Pubkey, MintLifecycle>>>,
    /// Post-discovery `MintRuntimeState` cached until ALT + registry admit
    /// consume it. Keyed by mint; entries dropped after admit.
    pending_states: Arc<Mutex<HashMap<Pubkey, MintRuntimeState>>>,
    /// Gate: at most one side-effect task per mint at a time.
    pending_ops: Arc<Mutex<HashSet<Pubkey>>>,
    /// Gate: at most one Replace in flight per slot.
    in_flight_slots: Arc<Mutex<HashSet<ShardSlot>>>,
    /// Global throttle: bounds the number of concurrent RPC-heavy phase
    /// tasks (Discovery / ATA / ALT) so a big promotion batch cannot
    /// burst past the shared RPC quota. Sized from
    /// `PromoterConfig::max_concurrent_rpc_ops`.
    rpc_semaphore: Arc<Semaphore>,
    event_tx: mpsc::UnboundedSender<InternalEvent>,
    metrics: Arc<PromoterMetrics>,
}

impl PromoterOrchestrator {
    /// Construct the orchestrator and return the paired event receiver. The
    /// receiver is passed back into `run`.
    pub fn new(inputs: PromoterInputs) -> (Self, mpsc::UnboundedReceiver<InternalEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let permits = inputs.config.max_concurrent_rpc_ops.max(1);
        let this = Self {
            inputs,
            lifecycles: Arc::new(Mutex::new(HashMap::new())),
            pending_states: Arc::new(Mutex::new(HashMap::new())),
            pending_ops: Arc::new(Mutex::new(HashSet::new())),
            in_flight_slots: Arc::new(Mutex::new(HashSet::new())),
            rpc_semaphore: Arc::new(Semaphore::new(permits)),
            event_tx,
            metrics: Arc::new(PromoterMetrics::new()),
        };
        (this, event_rx)
    }

    /// Expose the metrics collector so callers (e.g. a periodic logger)
    /// can read snapshots.
    pub fn metrics(&self) -> Arc<PromoterMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Run the promoter until `cancel_rx` fires. `event_rx` is the receiver
    /// paired to this orchestrator's internal event sender.
    pub async fn run(
        self: Arc<Self>,
        mut event_rx: mpsc::UnboundedReceiver<InternalEvent>,
        mut cancel_rx: oneshot::Receiver<()>,
    ) -> Result<()> {
        let tick_period = Duration::from_millis(self.inputs.config.tick_ms.max(1));
        let mut interval = tokio::time::interval(tick_period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            tick_ms = self.inputs.config.tick_ms,
            top_n_target = self.inputs.config.top_n_target,
            seed_size = self.inputs.seed.len(),
            "promoter orchestrator started"
        );

        loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => {
                    info!("promoter orchestrator cancelled");
                    return Ok(());
                }
                _ = interval.tick() => {
                    let now_ms = now_ms();
                    if let Err(e) = self.tick(now_ms).await {
                        warn!(error = %e, "promoter tick error");
                    }
                }
                Some(event) = event_rx.recv() => {
                    let now_ms = now_ms();
                    self.apply_event(event, now_ms).await;
                    // After every event, re-drive so newly-advanced phases
                    // can kick off their next stage without waiting a tick.
                    self.drive_pending(now_ms).await;
                }
            }
        }
    }

    // ----- tick logic -----

    /// One reconciliation cycle: recompute desired active set, refresh
    /// lifecycles, drive per-mint work.
    async fn tick(&self, now_ms: u128) -> Result<()> {
        let tick_start = std::time::Instant::now();
        let seed = Arc::clone(&self.inputs.seed);
        let top_needed = self
            .inputs
            .config
            .top_n_target
            .saturating_sub(seed.len());
        let top_n = self.inputs.tracker.top_n(top_needed);

        let mut desired: HashSet<Pubkey> = seed.iter().copied().collect();
        for (mint, _count) in top_n {
            desired.insert(mint);
        }

        let current = {
            let registry = self.inputs.registry.lock().unwrap();
            (*registry.allowed_snapshot()).clone()
        };

        // Additions: create lifecycles.
        for mint in desired.difference(&current).copied().collect::<Vec<_>>() {
            let mut lifecycles = self.lifecycles.lock().unwrap();
            let is_seed = seed.contains(&mint);
            lifecycles
                .entry(mint)
                .or_insert_with(|| MintLifecycle::new(mint, now_ms, is_seed));
        }

        // Removals: send LeftTopN to non-seed mints.
        for mint in current.difference(&desired).copied().collect::<Vec<_>>() {
            if seed.contains(&mint) {
                continue;
            }
            let mut lifecycles = self.lifecycles.lock().unwrap();
            if let Some(lc) = lifecycles.get_mut(&mint) {
                step(lc, LifecycleEvent::LeftTopN, now_ms);
            }
        }

        // Returns to top-N: if a Cooling mint is back in desired, send ReturnedToTopN.
        for mint in &desired {
            if seed.contains(mint) {
                continue;
            }
            let mut lifecycles = self.lifecycles.lock().unwrap();
            if let Some(lc) = lifecycles.get_mut(mint) {
                if lc.phase == LifecyclePhase::Cooling {
                    step(lc, LifecycleEvent::ReturnedToTopN, now_ms);
                }
            }
        }

        // Timer-driven transitions: warmup and grace elapsed.
        self.drive_timers(now_ms);

        // Kick off per-mint work.
        self.drive_pending(now_ms).await;

        // Refresh gauges + tick counter.
        let (active, cooling) = {
            let lifecycles = self.lifecycles.lock().unwrap();
            let mut active = 0u64;
            let mut cooling = 0u64;
            for lc in lifecycles.values() {
                match lc.phase {
                    LifecyclePhase::Active | LifecyclePhase::GrpcSubscribed => active += 1,
                    LifecyclePhase::Cooling => cooling += 1,
                    _ => {}
                }
            }
            (active, cooling)
        };
        self.metrics.set_active_count(active);
        self.metrics.set_cooling_count(cooling);
        self.metrics
            .record_tick(tick_start.elapsed().as_millis() as u64);

        Ok(())
    }

    /// Apply warmup + grace timers to lifecycles that entered `GrpcSubscribed`
    /// or `Cooling` long enough ago.
    fn drive_timers(&self, now_ms: u128) {
        let warmup = self.inputs.config.warmup_ms as u128;
        let cooling = self.inputs.config.cooling_ms as u128;
        let mut lifecycles = self.lifecycles.lock().unwrap();
        for lc in lifecycles.values_mut() {
            match lc.phase {
                LifecyclePhase::GrpcSubscribed => {
                    if lc.age_ms(now_ms) >= warmup {
                        step(lc, LifecycleEvent::WarmupElapsed, now_ms);
                    }
                }
                LifecyclePhase::Cooling => {
                    if lc.age_ms(now_ms) >= cooling {
                        step(lc, LifecycleEvent::GraceElapsed, now_ms);
                    }
                }
                _ => {}
            }
        }
    }

    /// Walk the lifecycle table and dispatch async work for phases whose next
    /// step is I/O-bound. Respects `pending_ops` and `in_flight_slots` gates.
    async fn drive_pending(&self, now_ms: u128) {
        // Snapshot phases + mints under the lock, then drop the lock before
        // spawning tasks so we never hold a std::sync::Mutex across await.
        let dispatch: Vec<(Pubkey, LifecyclePhase)> = {
            let lifecycles = self.lifecycles.lock().unwrap();
            lifecycles
                .values()
                .map(|lc| (lc.mint, lc.phase))
                .collect()
        };

        for (mint, phase) in dispatch {
            match phase {
                LifecyclePhase::Discovered => self.spawn_discovery(mint),
                LifecyclePhase::PoolsDiscovered => self.spawn_ata(mint),
                LifecyclePhase::AtasReady => self.spawn_alt(mint, now_ms),
                LifecyclePhase::AltReady => self.perform_registry_admit(mint),
                LifecyclePhase::RegistryLive => self.spawn_grpc_replace_for_mint(mint),
                LifecyclePhase::Retiring => self.perform_retire(mint),
                _ => {}
            }
        }
    }

    // ----- event handling -----

    async fn apply_event(&self, event: InternalEvent, now_ms: u128) {
        match event {
            InternalEvent::Discovery { mint, result } => {
                self.clear_pending(mint);
                match result {
                    Ok(disc) => {
                        // Early eligibility gate: reject mints that have no
                        // SOL-paired pump+dlmm pair BEFORE any rent is spent.
                        // Uses the same route-shape check as `spawn_alt`
                        // (`StableMintRouteAccounts::from_mint_runtime_state`)
                        // but passes `u64::MAX` for `max_state_age_ms` because
                        // pool freshness is irrelevant for ALT promotion — we
                        // only need to know that a SOL-paired pump+dlmm pair
                        // exists so its pubkeys can be added to the lookup
                        // table (pubkeys don't stale). Using the hot-path
                        // freshness bound here would race the ATA-creation
                        // latency (~12s for a fresh ATA) against the 1.5s
                        // default staleness window and reject every mint that
                        // needs a new ATA.
                        let route_opt = StableMintRouteAccounts::from_mint_runtime_state(
                            &disc.state,
                            self.inputs.min_pool_base_liquidity_lamports,
                            u64::MAX,
                            now_ms,
                        );
                        if route_opt.is_none() {
                            let msg = format!(
                                "no eligible pump+dlmm pair: pump_pools={} dlmm_pools={}",
                                disc.state.pump.len(),
                                disc.state.dlmms.len()
                            );
                            warn!(
                                mint = %mint,
                                kind = ?FailureKind::Discovery,
                                error = %msg,
                                "promoter phase permanent failure (pre-ATA gate)"
                            );
                            let err: Arc<str> = Arc::from(msg);
                            let mut lifecycles = self.lifecycles.lock().unwrap();
                            if let Some(lc) = lifecycles.get_mut(&mint) {
                                fail(lc, FailureKind::Discovery, err, now_ms);
                                self.metrics.record_failure(FailureKind::Discovery);
                            }
                            return;
                        }
                        self.pending_states
                            .lock()
                            .unwrap()
                            .insert(mint, disc.state);
                        let mut lifecycles = self.lifecycles.lock().unwrap();
                        if let Some(lc) = lifecycles.get_mut(&mint) {
                            step(lc, LifecycleEvent::DiscoveryOk, now_ms);
                        }
                    }
                    Err(e) => {
                        self.record_err(mint, FailureKind::Discovery, format!("{:#}", e), now_ms);
                    }
                }
            }
            InternalEvent::Ata { mint, result } => {
                self.clear_pending(mint);
                match result {
                    Ok(_ata) => {
                        let mut lifecycles = self.lifecycles.lock().unwrap();
                        if let Some(lc) = lifecycles.get_mut(&mint) {
                            step(lc, LifecycleEvent::AtaOk, now_ms);
                        }
                    }
                    Err(e) => {
                        self.record_err(mint, FailureKind::AtaCreation, format!("{:#}", e), now_ms);
                    }
                }
            }
            InternalEvent::Alt { mint, result } => {
                self.clear_pending(mint);
                match result {
                    Ok(report) => {
                        let mut lifecycles = self.lifecycles.lock().unwrap();
                        if let Some(lc) = lifecycles.get_mut(&mint) {
                            step(
                                lc,
                                LifecycleEvent::AltOk {
                                    primary_shard: report.primary_shard,
                                },
                                now_ms,
                            );
                        }
                    }
                    Err(e) => {
                        self.record_err(mint, FailureKind::AltExtension, format!("{:#}", e), now_ms);
                    }
                }
            }
            InternalEvent::RegistryAdmit { mint, result } => {
                self.clear_pending(mint);
                match result {
                    Ok(shard_slot) => {
                        self.metrics.record_promoted();
                        let mut lifecycles = self.lifecycles.lock().unwrap();
                        if let Some(lc) = lifecycles.get_mut(&mint) {
                            step(
                                lc,
                                LifecycleEvent::RegistryAdmitOk { shard_slot },
                                now_ms,
                            );
                        }
                    }
                    Err(e) => {
                        self.record_err(
                            mint,
                            FailureKind::RegistryAdmit,
                            format!("{:#}", e),
                            now_ms,
                        );
                    }
                }
            }
            InternalEvent::GrpcAck {
                slot,
                mints,
                result,
            } => {
                self.in_flight_slots.lock().unwrap().remove(&slot);
                let (event, log_err) = match result {
                    Ok(SubscriptionAck::Applied { subscriptions, .. }) => {
                        debug!(slot = slot.index(), subscriptions, "grpc replace applied");
                        self.metrics.record_resubscribe_ok();
                        (LifecycleEvent::GrpcAckOk, None)
                    }
                    Ok(SubscriptionAck::Failed(msg)) => {
                        self.metrics.record_resubscribe_err();
                        (LifecycleEvent::GrpcAckErr(msg.clone().into()), Some(msg))
                    }
                    Err(e) => {
                        let msg = format!("{}", e);
                        self.metrics.record_resubscribe_err();
                        (LifecycleEvent::GrpcAckErr(msg.clone().into()), Some(msg))
                    }
                };
                if let Some(err) = log_err {
                    warn!(slot = slot.index(), error = %err, "grpc replace failed");
                }
                let mut lifecycles = self.lifecycles.lock().unwrap();
                for mint in &mints {
                    let Some(lc) = lifecycles.get_mut(mint) else {
                        continue;
                    };
                    if lc.phase == LifecyclePhase::RegistryLive {
                        step(lc, event.clone(), now_ms);
                        if let LifecycleEvent::GrpcAckErr(_) = &event {
                            if lc.current_phase_attempts()
                                >= self.inputs.config.max_lifecycle_retries
                            {
                                fail(
                                    lc,
                                    FailureKind::GrpcSubscribe,
                                    "ack retries exhausted".into(),
                                    now_ms,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    fn record_err(&self, mint: Pubkey, kind: FailureKind, msg: String, now_ms: u128) {
        warn!(mint = %mint, kind = ?kind, error = %msg, "promoter phase error");
        let err: Arc<str> = msg.into();
        let mut lifecycles = self.lifecycles.lock().unwrap();
        let Some(lc) = lifecycles.get_mut(&mint) else {
            return;
        };
        let event = match kind {
            FailureKind::Discovery => LifecycleEvent::DiscoveryErr(Arc::clone(&err)),
            FailureKind::AtaCreation => LifecycleEvent::AtaErr(Arc::clone(&err)),
            FailureKind::AltExtension => LifecycleEvent::AltErr(Arc::clone(&err)),
            FailureKind::RegistryAdmit => LifecycleEvent::RegistryAdmitErr(Arc::clone(&err)),
            FailureKind::GrpcSubscribe => LifecycleEvent::GrpcAckErr(Arc::clone(&err)),
        };
        step(lc, event, now_ms);
        if lc.current_phase_attempts() >= self.inputs.config.max_lifecycle_retries {
            fail(lc, kind, err, now_ms);
            self.metrics.record_failure(kind);
        }
    }

    // ----- spawn helpers -----

    fn try_take_pending(&self, mint: Pubkey) -> bool {
        let mut pending = self.pending_ops.lock().unwrap();
        pending.insert(mint)
    }

    fn clear_pending(&self, mint: Pubkey) {
        self.pending_ops.lock().unwrap().remove(&mint);
    }

    fn spawn_discovery(&self, mint: Pubkey) {
        if !self.try_take_pending(mint) {
            return;
        }
        let bootstrap = Arc::clone(&self.inputs.bootstrap);
        let tx = self.event_tx.clone();
        let sem = Arc::clone(&self.rpc_semaphore);
        tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            let result = discover_mint_async(bootstrap, mint).await;
            let _ = tx.send(InternalEvent::Discovery { mint, result });
        });
    }

    fn spawn_ata(&self, mint: Pubkey) {
        if !self.try_take_pending(mint) {
            return;
        }
        let rpc = Arc::clone(&self.inputs.rpc);
        let wallet = Arc::clone(&self.inputs.wallet);
        let tx = self.event_tx.clone();
        let sem = Arc::clone(&self.rpc_semaphore);
        tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            let result = ensure_ata_async(rpc, wallet, mint, mint.to_string()).await;
            let _ = tx.send(InternalEvent::Ata { mint, result });
        });
    }

    fn spawn_alt(&self, mint: Pubkey, now_ms: u128) {
        if !self.try_take_pending(mint) {
            return;
        }

        // Build StableMintRouteAccounts from the discovered state.
        let state_opt = self.pending_states.lock().unwrap().get(&mint).cloned();
        let Some(state) = state_opt else {
            warn!(mint = %mint, "spawn_alt: no cached discovery state; skipping");
            self.clear_pending(mint);
            return;
        };
        // ALT promotion doesn't care about pool freshness (pubkeys are stable);
        // pass `u64::MAX` for `max_state_age_ms` to avoid racing ATA-creation
        // latency against the hot-path staleness bound. The Discovery event
        // gate already validated the route shape (pump+dlmm SOL-paired with
        // min liquidity) before an ATA was ever requested.
        let route_opt = StableMintRouteAccounts::from_mint_runtime_state(
            &state,
            self.inputs.min_pool_base_liquidity_lamports,
            u64::MAX,
            now_ms,
        );
        let Some(route) = route_opt else {
            // Should not happen: Discovery gate already validated this. If it
            // does, treat as permanent (retrying won't change anything without
            // fresh discovery).
            let msg = "no eligible pump+dlmm pair for ALT promotion (post-Discovery)";
            warn!(mint = %mint, kind = ?FailureKind::AltExtension, error = %msg, "promoter phase permanent failure");
            let err: Arc<str> = Arc::from(msg);
            self.clear_pending(mint);
            let mut lifecycles = self.lifecycles.lock().unwrap();
            if let Some(lc) = lifecycles.get_mut(&mint) {
                fail(lc, FailureKind::AltExtension, err, now_ms);
                self.metrics.record_failure(FailureKind::AltExtension);
            }
            return;
        };

        // Widen the allowlist snapshot to include this mint so the planner
        // treats its pools as first-class.
        let mut allowed: Vec<Pubkey> = self
            .inputs
            .registry
            .lock()
            .unwrap()
            .allowed_mints();
        if !allowed.contains(&mint) {
            allowed.push(mint);
        }

        let rpc = Arc::clone(&self.inputs.rpc);
        let wallet = Arc::clone(&self.inputs.wallet);
        let state_file = self.inputs.state_file.clone();
        let state_file_lock = Arc::clone(&self.inputs.state_file_lock);
        let shard_capacity = self.inputs.shard_capacity;
        let auto_create = self.inputs.auto_create;
        let auto_extend = self.inputs.auto_extend;
        let alt_timeout = Duration::from_millis(self.inputs.config.alt_timeout_ms);
        let tx = self.event_tx.clone();
        let sem = Arc::clone(&self.rpc_semaphore);

        tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.ok();
            let fut = promote_mint_into_shard_async(
                rpc,
                wallet,
                state_file,
                state_file_lock,
                allowed,
                shard_capacity,
                route,
                auto_create,
                auto_extend,
                Vec::new(),
            );
            let result = match tokio::time::timeout(alt_timeout, fut).await {
                Ok(inner) => inner,
                Err(_) => Err(anyhow::anyhow!("alt_timeout_ms exceeded")),
            };
            let _ = tx.send(InternalEvent::Alt { mint, result });
        });
    }

    fn perform_registry_admit(&self, mint: Pubkey) {
        if !self.try_take_pending(mint) {
            return;
        }
        let state_opt = self.pending_states.lock().unwrap().remove(&mint);
        let Some(state) = state_opt else {
            self.clear_pending(mint);
            self.record_err(
                mint,
                FailureKind::RegistryAdmit,
                "missing cached discovery state".into(),
                now_ms(),
            );
            return;
        };

        // 1. Admit the mint atomically.
        let admit_result = self.inputs.registry.lock().unwrap().admit_mint_with_initial_state(state);
        let event = match admit_result {
            Ok(()) => {
                // 2. Allocate a shard slot.
                let slot_opt = self.inputs.shard_alloc.lock().unwrap().assign(mint);
                match slot_opt {
                    Some(slot) => Ok(slot),
                    None => Err(anyhow::anyhow!("shard slot allocator full")),
                }
            }
            Err(e) => Err(e),
        };
        let _ = self.event_tx.send(InternalEvent::RegistryAdmit {
            mint,
            result: event,
        });
    }

    fn spawn_grpc_replace_for_mint(&self, mint: Pubkey) {
        let slot = match self.inputs.shard_alloc.lock().unwrap().slot_of(&mint) {
            Some(s) => s,
            None => {
                warn!(mint = %mint, "spawn_grpc_replace: no slot assignment");
                return;
            }
        };
        // Serialise per slot: skip if another Replace is in flight.
        {
            let mut in_flight = self.in_flight_slots.lock().unwrap();
            if !in_flight.insert(slot) {
                return;
            }
        }
        let mints: Vec<Pubkey> = self
            .inputs
            .shard_alloc
            .lock()
            .unwrap()
            .mints_for_slot(slot)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();

        let Some(handle) = self
            .inputs
            .grpc_workers
            .iter()
            .find(|h| h.slot == slot)
            .cloned()
        else {
            self.in_flight_slots.lock().unwrap().remove(&slot);
            warn!(slot = slot.index(), "spawn_grpc_replace: no worker handle for slot");
            return;
        };

        let tx = self.event_tx.clone();
        let ack_timeout = Duration::from_millis(self.inputs.config.grpc_ack_timeout_ms);
        let mints_for_send = mints.clone();

        tokio::spawn(async move {
            let (ack_tx, ack_rx) = oneshot::channel();
            let send_res = handle
                .command_tx
                .send(SubscriptionCommand::Replace {
                    mints: mints_for_send,
                    ack: ack_tx,
                })
                .await;
            let result: Result<SubscriptionAck> = match send_res {
                Ok(()) => match tokio::time::timeout(ack_timeout, ack_rx).await {
                    Ok(Ok(ack)) => Ok(ack),
                    Ok(Err(_)) => Err(anyhow::anyhow!("grpc ack channel dropped")),
                    Err(_) => Err(anyhow::anyhow!("grpc ack timeout")),
                },
                Err(e) => Err(anyhow::anyhow!("grpc command send failed: {}", e)),
            };
            let _ = tx.send(InternalEvent::GrpcAck {
                slot,
                mints,
                result,
            });
        });
    }

    /// Currently retire = unsubscribe (Replace without the mint) + evict from
    /// registry + release shard slot. Rent parked (ATA/ALT retained per
    /// `retire_ata_on_evict` / `retire_alt_on_evict` defaults = false).
    fn perform_retire(&self, mint: Pubkey) {
        // Evict from registry (drops pool state).
        let evict_res = self.inputs.registry.lock().unwrap().evict_mint(mint);
        if let Err(e) = evict_res {
            error!(mint = %mint, error = %e, "retire: evict failed");
            // Do not advance FSM: we'll try again next tick.
            return;
        }
        // Release the slot and re-subscribe with the reduced mint list.
        let slot = self.inputs.shard_alloc.lock().unwrap().release(mint);
        if let Some(slot) = slot {
            // Fire a Replace with the new mint set (mint no longer present).
            self.spawn_grpc_replace_for_slot(slot);
        }
        // FSM advance.
        let mut lifecycles = self.lifecycles.lock().unwrap();
        if let Some(lc) = lifecycles.get_mut(&mint) {
            step(lc, LifecycleEvent::RetireDone, now_ms());
        }
        self.metrics.record_demoted();
    }

    fn spawn_grpc_replace_for_slot(&self, slot: ShardSlot) {
        {
            let mut in_flight = self.in_flight_slots.lock().unwrap();
            if !in_flight.insert(slot) {
                return;
            }
        }
        let mints: Vec<Pubkey> = self
            .inputs
            .shard_alloc
            .lock()
            .unwrap()
            .mints_for_slot(slot)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();

        let Some(handle) = self
            .inputs
            .grpc_workers
            .iter()
            .find(|h| h.slot == slot)
            .cloned()
        else {
            self.in_flight_slots.lock().unwrap().remove(&slot);
            return;
        };
        let tx = self.event_tx.clone();
        let ack_timeout = Duration::from_millis(self.inputs.config.grpc_ack_timeout_ms);
        let mints_for_send = mints.clone();
        tokio::spawn(async move {
            let (ack_tx, ack_rx) = oneshot::channel();
            let send_res = handle
                .command_tx
                .send(SubscriptionCommand::Replace {
                    mints: mints_for_send,
                    ack: ack_tx,
                })
                .await;
            let result: Result<SubscriptionAck> = match send_res {
                Ok(()) => match tokio::time::timeout(ack_timeout, ack_rx).await {
                    Ok(Ok(ack)) => Ok(ack),
                    Ok(Err(_)) => Err(anyhow::anyhow!("grpc ack channel dropped")),
                    Err(_) => Err(anyhow::anyhow!("grpc ack timeout")),
                },
                Err(e) => Err(anyhow::anyhow!("grpc command send failed: {}", e)),
            };
            let _ = tx.send(InternalEvent::GrpcAck {
                slot,
                mints,
                result,
            });
        });
    }
}

pub(crate) fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(byte: u8) -> Pubkey {
        Pubkey::new_from_array([byte; 32])
    }

    #[test]
    fn now_ms_is_positive() {
        assert!(now_ms() > 0);
    }

    #[test]
    fn pending_ops_gate_is_exclusive() {
        // Direct unit test of the gate primitive without the full orchestrator.
        let gate: Arc<Mutex<HashSet<Pubkey>>> = Arc::new(Mutex::new(HashSet::new()));
        let mint = pk(1);
        let first = gate.lock().unwrap().insert(mint);
        let second = gate.lock().unwrap().insert(mint);
        assert!(first);
        assert!(!second);
        gate.lock().unwrap().remove(&mint);
        assert!(gate.lock().unwrap().insert(mint));
    }
}
