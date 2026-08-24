# Promoter Runbook (M3b)

The promoter drives the `HotMintTracker` top-N into the runtime allowlist end-to-end: discovery + ATA + ALT + registry admit + gRPC subscription hot-swap. The seed set (`runtime.allowed_mints`) is preserved as an invariant `A ⊇ S`.

## Enabling

```toml
[runtime.hot_mints]
enabled = true          # required
top_n = 27
window_ms = 900000
rotate_ms = 300000

[runtime.promoter]
enabled = true          # off by default
tick_ms = 300000        # reconciliation cadence
cooling_ms = 600000     # grace before evict after LeftTopN
warmup_ms = 5000
alt_timeout_ms = 60000
max_lifecycle_retries = 3
top_n_target = 27       # |A| = seed ∪ top_(top_n_target − |seed|)
grpc_ack_timeout_ms = 5000

[runtime.promoter.coldstart]
enabled = true          # scan pump-amm signatures at boot
max_signatures = 1000
budget_ms = 30000

[grpc]
enabled = true          # required
```

Validation refuses `promoter.enabled=true` without `hot_mints.enabled` and `grpc.enabled`. `top_n_target < |seed|` is also rejected.

## Lifecycle

Each promoted mint traverses:

```
Discovered → PoolsDiscovered → AtasReady → AltReady
           → RegistryLive → GrpcSubscribed → Active
           → Cooling → Retiring → Retired
```

Seed mints never enter `Cooling`/`Retiring`/`Retired`.

## Operational signals

`PromoterOrchestrator::metrics()` exposes `PromoterMetrics`. Snapshot fields:

| Counter | Meaning |
|---|---|
| `ticks_total` | Reconciliation cycles executed |
| `mints_promoted_total` | Successful `RegistryLive` admits |
| `mints_demoted_total` | Successful `Retired` transitions |
| `grpc_resubscribes_total` | `Replace` acks with `Applied` |
| `grpc_resubscribe_errors_total` | `Replace` acks failed or timed out |
| `lifecycle_failures_total[FailureKind]` | Retries exhausted per phase (`Discovery`, `AtaCreation`, `AltExtension`, `RegistryAdmit`, `GrpcSubscribe`) |
| `current_active_count_gauge` | Snapshot of `Active`+`GrpcSubscribed` mints |
| `current_cooling_count_gauge` | Snapshot of `Cooling` mints |
| `last_tick_duration_ms_gauge` | Wall-clock duration of last tick |

## Diagnosing common issues

### Mint stuck in `Failed{Discovery}`

Signal: `lifecycle_failures_total[0]` climbs; specific mint never reaches `PoolsDiscovered`.
Cause: no eligible pump-amm or DLMM pool for this mint above `execution.min_pool_base_liquidity_lamports`.
Fix: raise/lower liquidity threshold, or accept that the mint is illiquid and let it park.

### Mint stuck in `Failed{AtaCreation}`

Signal: `lifecycle_failures_total[1]` climbs; wallet log shows `insufficient funds`.
Cause: wallet SOL exhausted (each ATA costs ~0.00204 SOL rent).
Fix: fund the wallet or set `top_n_target` lower.

### Mint stuck in `Failed{AltExtension}`

Signal: `lifecycle_failures_total[2]` climbs after ~60s of `AtasReady`.
Cause: RPC extend budget insufficient, ALT state file locked, or on-chain reconciliation mismatch.
Fix: inspect `lookup_tables.route_shards.state_file` and reconcile manually; a bot restart re-runs `promote_mint_into_shard` idempotently.

### Growing `grpc_resubscribe_errors_total`

Signal: `Replace` acks failing.
Cause: Yellowstone endpoint down, upstream x_token invalid, or filter budget exceeded per slot.
Fix: verify `grpc.url`/`x_token`. Check per-slot mint count: `top_n_target / 3` slots, up to 3 mints per slot.

### `last_tick_duration_ms_gauge` growing

Signal: ticks slow (> 1s typical is fine; > 30s is a bug).
Cause: too many mints in `Discovered`+`PoolsDiscovered` awaiting slow RPCs.
Fix: reduce `top_n_target`, or check RPC latency (`rpc.http`).

## Manual force-retire

There is no runtime API. Kill the bot, remove the mint from `runtime.allowed_mints` if present, and restart. The promoter re-scans the tracker and only promotes what's currently hot.

## ATA rent accounting

`retire_ata_on_evict=false` (default) leaves ATA rent (~0.00204 SOL each) parked on demote. Rationale: closing an ATA costs one tx and re-creating it costs rent again if the mint returns to hot list. Parked rent is recoverable manually via `spl-token close`.

## Design notes

- **Serial per slot, parallel across slots.** Each ShardSlot has at most one `Replace` in flight (`in_flight_slots` gate). Different slots run in parallel.
- **Single op per mint.** `pending_ops` gate ensures Discovery/ATA/ALT/etc. don't overlap for the same mint.
- **Seed invariant enforced at 4 layers.** Validation, `RegistryHandle::evict_mint` refusal, allocator `preassign_seed`, and the lifecycle reducer.
- **Cold-start.** `getSignaturesForAddress(pump_program, 1000)` populates the tracker before `HotMintTracker` sees any live trigger traffic. Runs blocking on `spawn_blocking` with a 30s budget.
