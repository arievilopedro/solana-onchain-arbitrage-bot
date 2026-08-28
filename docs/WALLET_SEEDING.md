# Wallet-Seeded Boot

Runbook for bootstrapping the bot's initial monitored mint set from a
copy-wallet's recent trade history instead of a hard-coded
`runtime.allowed_mints` list.

## Why

The default flow requires operators to hand-curate `allowed_mints`. That's
fine for a static portfolio but painful when the target strategy is "shadow
what wallet X is trading right now". Wallet-seeded boot removes that
manual step: at start-up the bot scans the last N signatures of one or
more configured copy wallets, ranks the touched mints by trade frequency
(tie-break by most-recent slot), and uses the top-K as the initial
allowlist.

## Two modes

### Pin mode (`pin_seeded_mints = true`)

Seeded mints are appended to `runtime.allowed_mints` **before** the boot
pipeline runs. Each seeded mint is discovered synchronously (pool
enumeration + per-mint ATA + route-shard maintenance + geyser
subscription) exactly like a hand-configured mint. When boot finishes
every seeded mint is Active and can trigger immediately.

Best when:
- The copy wallet's target set is stable across restarts.
- Seed size is small (≤ ~30 mints) so boot latency stays bounded.
- You want deterministic, all-or-nothing boot semantics — one failed
  seed mint fails the whole boot.

### Non-pin mode (`pin_seeded_mints = false`)

Seeded mints are held in-memory and planted into `HotMintTracker` via
`seed_boost` right after the tracker is constructed. The promoter FSM
then discovers them lazily on the next tick: Discovered →
PoolsDiscovered → AtasReady → AltReady → RegistryLive → GrpcSubscribed
→ Active. Boot itself remains fast because the seed set never touches
the synchronous bootstrap pipeline.

Best when:
- Seed size is large or you don't want boot to block on discovery.
- The copy wallet churns targets frequently (a failed discovery on one
  mint should not fail the whole boot).
- You already run the promoter for other reasons.

Requires `runtime.promoter.enabled = true` **and**
`runtime.hot_mints.enabled = true`. Enforced at config-validation time.

## Config surface

```toml
[runtime]
# May be [] when the seeding block below is enabled with seed_top_n > 0.
allowed_mints = []

[runtime.hot_mints]
enabled = true          # required by non-pin path
top_n = 27
window_ms = 900000
rotate_ms = 300000

[runtime.promoter]
enabled = true          # required by non-pin path
top_n_target = 27
# ...

[runtime.wallet_followers]
enabled = true
seed_top_n = 3          # 0 = disable seeding
pin_seeded_mints = false
seed_max_signatures_per_wallet = 500     # <= 1000 (Solana RPC cap)
seed_budget_ms = 30000                   # hard wall-clock cap
seed_concurrency = 1                     # reserved (currently serial)
seed_boost_weight = 100                  # ignored when pin=true

[[runtime.wallet_followers.wallets]]
address = "TraderWalletPubkeyBase58"
label = "trader_alpha"
```

## Boot log timeline

Look for these lines in `RUST_LOG=info` output:

```
wallet_seed_extraction: selected=3 all_ranked=17 wallets_scanned=1 signatures_examined=482 budget_exhausted=false elapsed_ms=8213 pin_seeded_mints=false
wallet_seed_planted_in_tracker: count=3 boost_weight=100          # non-pin only
bootstrap OK: registry_mints=0 pump=0 dlmm=0 raydium_cp=0 damm_v2=0 skipped_low_liquidity=0
```

Pinned path additionally logs:

```
wallet_seed_extraction: pinned effective_allowed_mints=3 (base=0 + seed_added=3)
bootstrap OK: registry_mints=3 pump=1 dlmm=3 raydium_cp=1 damm_v2=1 skipped_low_liquidity=0
```

Follow with the usual promoter FSM traces (non-pin path):

```
promoter tick: candidates=3 admitting=3 ...
promoter phase=Discovered mint=... → PoolsDiscovered
promoter phase=PoolsDiscovered mint=... → AtasReady
...
promoter phase=GrpcSubscribed mint=... → Active
```

Time to first Active on the non-pin path is roughly one promoter tick
plus discovery/ATA/ALT latency (~30-90 s in typical conditions).

## Debug

- **Zero mints selected**: check `report.all_ranked` — if it's also
  empty, the wallets have no recent trades touching the configured
  `programs` filter, or the RPC returned no signatures. Try raising
  `seed_max_signatures_per_wallet` or widening `programs`.
- **`budget_exhausted=true`**: bump `seed_budget_ms`. Wall-clock cap
  hit before the scan finished every wallet.
- **Discovery fails permanently for a seed mint** (non-pin path): the
  mint didn't meet the ≥ 2-distinct-pool-types Discovery gate. This is
  expected and correct — arb requires at least two pool types.
- **Boot bails with `runtime.allowed_mints must not be empty ...`**:
  validation V1 tripped. You have `allowed_mints=[]` but either
  `wallet_followers.enabled=false` or `seed_top_n=0`.

## Force re-seed

There is no periodic re-scan; the seed extraction runs exactly once at
boot. To re-seed, restart the bot. Persisting the extracted set across
restarts and periodic re-scanning are explicitly out of scope for the
first cut (see `docs/PLAN_RAYDIUM_CPMM_ROUTES.md` deferred list).

## Failure modes

| Scenario | Behaviour |
|---|---|
| `allowed_mints=[]` && `seed_top_n=0` | validation bail (V1) |
| `allowed_mints=[]` && `seed_top_n>0` && `wallets=[]` | validation bail (V2) |
| RPC timeout during seed scan | `budget_exhausted=true`; if `selected=[]` && `allowed_mints=[]` → bail |
| Seed returns zero mints, but `allowed_mints` non-empty | warn + continue with static allowlist |
| Discovery fails on pinned seed mint | fatal (matches static-allowlist behaviour) |
| Discovery fails on non-pinned seed mint | promoter FSM parks the mint after `max_lifecycle_retries` |
| Duplicate mint between `allowed_mints` and seed | dedupped silently in the pin path |
