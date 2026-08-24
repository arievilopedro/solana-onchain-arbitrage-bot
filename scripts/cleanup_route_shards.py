#!/usr/bin/env python3
"""
Cleanup script for route_shards.json state file corruption.

Fixes the `local used > on-chain len` case introduced by the pre-mutex
concurrent writers race (see promote_mint_into_shard_async in src/alt/mod.rs).

Two modes:

  --mode=truncate   For each corrupt shard, drop any mint block record whose
                    indexes reference positions >= on-chain length. Also drop
                    those positions from the shard's `used` counter so it
                    matches the on-chain length. Keeps the shard live.

  --mode=drop-shard For each shard listed with --shard, remove it from
                    `shards`, clear `active_shard` if pointing at it, remove
                    every mint block record that references it (primary or
                    extension), and drop `active_shard` entirely if empty.
                    Use when the shard is beyond salvage and you'd rather
                    rebuild from scratch.

Usage:

  # Truncate mode: pass on-chain lengths per shard.
  python scripts/cleanup_route_shards.py route_shards.json \\
    --mode=truncate \\
    --shard HwQdYZkrLENRawamW9T3KhcFAYu6TXMAh4tC1kzAgLEW=212

  # Drop-shard mode: nuke listed shards entirely.
  python scripts/cleanup_route_shards.py route_shards.json \\
    --mode=drop-shard \\
    --shard HwQdYZkrLENRawamW9T3KhcFAYu6TXMAh4tC1kzAgLEW

Always writes to `<input>.cleaned.json` and prints a summary. Never mutates the
input file — copy over manually after inspecting the output.
"""

import argparse
import json
import sys
from pathlib import Path


def parse_shard_specs(raw, expects_length):
    out = {}
    for spec in raw:
        if expects_length:
            if "=" not in spec:
                sys.exit(
                    f"--shard argument {spec!r} must be SHARD=ONCHAIN_LEN in truncate mode"
                )
            shard, length_str = spec.split("=", 1)
            try:
                length = int(length_str)
            except ValueError:
                sys.exit(f"invalid on-chain length {length_str!r} for shard {shard}")
            out[shard] = length
        else:
            out[spec] = None
    return out


def mint_touches_shard(record, shard):
    if record["shard"] == shard:
        return True
    for ext in record.get("extensions", []) or []:
        if ext["shard"] == shard:
            return True
    return False


def mint_has_out_of_range_index(record, shard, on_chain_len):
    def bad(indexes):
        return any(idx >= on_chain_len for idx in indexes)

    if record["shard"] == shard:
        if bad(record["base"]["indexes"]):
            return True
        for dl in record["dlmm"]:
            if bad(dl["indexes"]):
                return True
    for ext in record.get("extensions", []) or []:
        if ext["shard"] != shard:
            continue
        for dl in ext["dlmm"]:
            if bad(dl["indexes"]):
                return True
    return False


def truncate_mode(store, shard_lengths):
    dropped_mints = []
    for shard, on_chain_len in shard_lengths.items():
        if shard not in store["shards"]:
            sys.exit(f"shard {shard} not present in state file")
        record = store["shards"][shard]
        used_before = record["used"]
        if on_chain_len > record["capacity"]:
            sys.exit(
                f"on-chain length {on_chain_len} exceeds capacity "
                f"{record['capacity']} for shard {shard}"
            )
        if used_before < on_chain_len:
            print(
                f"[warn] shard {shard} used ({used_before}) already <= on-chain "
                f"({on_chain_len}); no truncation needed"
            )
            continue

        to_drop = []
        for mint, mrec in store["mints"].items():
            if mint_touches_shard(mrec, shard) and mint_has_out_of_range_index(
                mrec, shard, on_chain_len
            ):
                to_drop.append(mint)

        for mint in to_drop:
            del store["mints"][mint]
            dropped_mints.append((mint, shard))

        record["used"] = on_chain_len
        print(
            f"[ok] shard {shard}: used {used_before} -> {on_chain_len}; "
            f"dropped {len(to_drop)} mint blocks"
        )
    return dropped_mints


def drop_shard_mode(store, shards_to_drop):
    dropped_mints = []
    for shard in shards_to_drop:
        if shard not in store["shards"]:
            print(f"[warn] shard {shard} not present in state file; skipping")
            continue
        del store["shards"][shard]

        if store.get("active_shard") == shard:
            store["active_shard"] = None

        # Remove any mint whose primary shard is this one; strip extensions
        # pointing at this shard from mints that only had it as extension.
        to_delete = []
        for mint, mrec in store["mints"].items():
            if mrec["shard"] == shard:
                to_delete.append(mint)
                continue
            if mrec.get("extensions"):
                mrec["extensions"] = [
                    ext for ext in mrec["extensions"] if ext["shard"] != shard
                ]
        for mint in to_delete:
            del store["mints"][mint]
            dropped_mints.append((mint, shard))

        print(
            f"[ok] shard {shard}: dropped from shards; removed "
            f"{len(to_delete)} mint blocks whose primary shard was this one"
        )
    # If we deleted the active_shard reference and other active shards remain,
    # pick the newest active as the new active_shard (matches planner heuristic
    # of appending to the last active shard).
    if store.get("active_shard") is None:
        active_candidates = [
            (addr, rec)
            for addr, rec in store["shards"].items()
            if rec["status"] == "active"
        ]
        if active_candidates:
            active_candidates.sort(key=lambda kv: kv[1]["created_slot"], reverse=True)
            store["active_shard"] = active_candidates[0][0]
            print(f"[ok] active_shard reset to {store['active_shard']}")
    return dropped_mints


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", type=Path, help="path to route_shards.json")
    parser.add_argument(
        "--mode",
        choices=("truncate", "drop-shard"),
        required=True,
    )
    parser.add_argument(
        "--shard",
        action="append",
        default=[],
        help="target shard; truncate mode requires SHARD=ONCHAIN_LEN",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="output path (default: <input>.cleaned.json)",
    )
    args = parser.parse_args()

    if not args.shard:
        sys.exit("at least one --shard must be provided")

    store = json.loads(args.input.read_text(encoding="utf-8"))
    shards_before = {k: v["used"] for k, v in store["shards"].items()}
    mints_before = len(store["mints"])

    if args.mode == "truncate":
        specs = parse_shard_specs(args.shard, expects_length=True)
        dropped = truncate_mode(store, specs)
    else:
        specs = parse_shard_specs(args.shard, expects_length=False)
        dropped = drop_shard_mode(store, list(specs.keys()))

    shards_after = {k: v["used"] for k, v in store["shards"].items()}
    mints_after = len(store["mints"])

    output = args.output or args.input.with_suffix(args.input.suffix + ".cleaned.json")
    output.write_text(json.dumps(store, indent=2), encoding="utf-8")

    print()
    print(f"input:  {args.input}")
    print(f"output: {output}")
    print(f"mints:  {mints_before} -> {mints_after}  (dropped {len(dropped)})")
    print("shards:")
    for addr in sorted(set(shards_before) | set(shards_after)):
        before = shards_before.get(addr, "-")
        after = shards_after.get(addr, "-")
        print(f"  {addr}: used {before} -> {after}")
    if dropped:
        print("dropped mints:")
        for mint, shard in dropped:
            print(f"  {mint}  (from shard {shard})")


if __name__ == "__main__":
    main()
