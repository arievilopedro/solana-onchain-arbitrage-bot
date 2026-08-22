# wallet_probe

Passive observation binary for studying competitor MEV wallets on Solana.

Uses the endpoints already declared in `config.toml` (Shyft RabbitStream shred
stream + Shyft Yellowstone gRPC + HTTP RPC) — no new credentials required.

**Read-only.** The probe never signs, never sends, never touches any wallet.

---

## What it captures

For every transaction broadcast by one of the target wallets, the probe writes a
`wallet_tx` JSONL row containing:

- `signature`, `slot`, `wallet` (fee payer / signer)
- `mints`, `pools`, `programs` touched
- `tip` (`{account, amount_lamports, kind}` where `kind ∈ jito|helius|other`)
- `priority_fee_micro_lamports`, `cu_limit`
- `tx_size_bytes`, `is_versioned_v0`, `has_advance_nonce`, `uses_alt`,
  `alt_writable_count`, `alt_readonly_count`
- `flashx_axion_seen`, `mevi_program_seen`, `instruction_count`
- `trigger_candidates` — top-N causally-plausible context txs seen within
  `context-lookback-ms`, scored by mint/pool/program overlap and recency.

For every observed signature it later writes a `landing` row:

- `broadcast_slot`, `landed_slot`, `slot_gap`, `confirmation_ms`
- `dropped` (true if no landing observed before `landing-deadline-ms`)
- `landed_with_err`, `err_debug` (base58 of raw error bytes)
- `source` (`yellowstone` | `rpc_status` | `deadline`)

With `--log-context-events`, also emits `context` rows for each DEX-touching
tx observed on the context stream.

---

## Why three subscribers

1. **Wallet stream** — RabbitStream (shred) filtered by the target wallet
   accounts. Sees the tx **before** consensus, so it captures both landed and
   dropped transactions. Shyft RabbitStream ships partial meta (ALTs resolved,
   no logs / balances / err) which the parser handles.
2. **Context stream** — same shred stream, but filtered by a broad DEX +
   trigger program list. Feeds a small rolling buffer (`ContextBuffer`) used
   to infer what likely triggered each wallet tx.
3. **Landing stream** — Yellowstone gRPC filtered by the target wallets, plus
   a single `getSignatureStatuses` RPC probe after the deadline. Reconciles
   each broadcast signature with a landing outcome.

---

## Usage

```bash
cargo build --release --features geyser --bin wallet_probe

./target/release/wallet_probe \
    --config config.toml \
    --target-wallets <pubkey1>,<pubkey2> \
    --out state/wallet_probe.jsonl \
    --duration-sec 3600 \
    --context-lookback-ms 750 \
    --landing-deadline-ms 15000
```

### Flags

| Flag | Default | Purpose |
| --- | --- | --- |
| `--config` | `config.toml` | AppConfig path (reuses `rpc`, `grpc`, `rabbitstream`). |
| `--target-wallets` | *(required)* | Comma-separated pubkeys to monitor. |
| `--out` | `state/wallet_probe.jsonl` | Output JSONL path (appended). |
| `--duration-sec` | `0` | Stop after N seconds (`0` = run until Ctrl-C). |
| `--context-lookback-ms` | `750` | Rolling context buffer window. |
| `--context-max-entries` | `4000` | Cap on context buffer size. |
| `--max-candidates` | `5` | Max trigger candidates emitted per wallet tx. |
| `--landing-deadline-ms` | `15000` | Deadline before flagging a broadcast as dropped. |
| `--context-programs` | *(default DEX set)* | Override the context filter program list. |
| `--log-context-events` | *(off)* | Also emit `context` events to JSONL. |
| `--disable-landing-yellowstone` | *(off)* | Skip Yellowstone subscriber; rely on RPC deadline only. |

### Default context programs

`FLASHX`, `MEVi`, `pump-amm`, `Meteora DLMM`, `Meteora DAMM`, `Raydium CLMM`,
`Raydium CPMM`, `Raydium AMM v4`, `Whirlpool`, `Phoenix`, `OpenBook v2`.

Override via `--context-programs` if you want to narrow / broaden the scope.

---

## Config prerequisites

The probe reuses the existing `config.toml`. Required sections:

```toml
[rpc]
http = "${RPC_HTTP_URL}"        # for getSignatureStatuses fallback

[grpc]
enabled = true                   # Yellowstone (Shyft) for landing reconciliation
url = "${YELLOWSTONE_URL}"
x_token = "${YELLOWSTONE_TOKEN}"

[rabbitstream]
enabled = true                   # Shyft RabbitStream (shred) for both A and B subscribers
url = "${RABBIT_URL}"
x_token = "${RABBIT_TOKEN}"
```

If you don't have a Yellowstone endpoint, pass
`--disable-landing-yellowstone`; landing rows will come exclusively from the
deadline / RPC fallback path.

---

## JSONL schema at a glance

```json
{"type":"probe_status","ts_ms":..., "event":"probe_start","detail":"..."}

{"type":"wallet_tx","ts_ms":..., "signature":"...","slot":...,"wallet":"...",
 "mints":[...],"pools":[...],"programs":[...],
 "tip":{"account":"...","amount_lamports":...,"kind":"jito"},
 "priority_fee_micro_lamports":...,"cu_limit":...,
 "tx_size_bytes":...,"is_versioned_v0":true,"has_advance_nonce":false,
 "uses_alt":true,"alt_writable_count":8,"alt_readonly_count":2,
 "flashx_axion_seen":true,"mevi_program_seen":false,
 "instruction_count":6,
 "trigger_candidates":[
   {"signature":"...","slot":...,"time_delta_ms":123,
    "matched_programs":["..."],"matched_mints":["..."],"matched_pools":[],
    "score":1230}
 ],
 "meta_err_present":false}

{"type":"landing","ts_ms":..., "signature":"...","wallet":"...",
 "broadcast_slot":...,"landed_slot":...,"dropped":false,
 "landed_with_err":false,"err_debug":null,
 "slot_gap":0,"confirmation_ms":420,"source":"yellowstone"}

{"type":"context","ts_ms":..., "signature":"...","slot":...,
 "programs":[...],"mints":[...],"pools":[...],
 "sol_volume_lamports":123456,"flashx_axion_seen":true}
```

---

## Trigger scoring

For each wallet tx observed at `t`, every context entry `c` within the
lookback window is scored:

```
base       = 1000 - min(|t - c.ts|, lookback_ms)
matches    = 200 * matched_mints + 150 * matched_pools + 50 * matched_programs
future_pen = -400 if c.ts > t (candidate can't be a trigger for a past event)
score      = base + matches + future_pen
```

Entries with no matches are dropped. Top-N by score are emitted.

The scoring is intentionally simple and additive — treat scores as an ordinal
ranking within a single wallet tx, not as a probability.

---

## Notes on Shyft RabbitStream meta

RabbitStream is a shred stream and delivers partial meta:

- **populated**: `loaded_writable_addresses`, `loaded_readonly_addresses` (ALTs)
- **empty**: `logs`, `inner_instructions`, `pre_balances`, `post_balances`,
  `pre_token_balances`, `post_token_balances`, `err`

The parser handles this: mints are best-effort (from token balances if
available, otherwise inferred from tx accounts), and `meta_err_present` is
only `true` when meta is present AND has a non-empty err.

The landing stream uses Yellowstone (Confirmed commitment) which ships full
meta, so `landed_with_err` / `err_debug` are reliable there.
