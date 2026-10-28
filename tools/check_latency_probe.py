#!/usr/bin/env python3
"""Independent, bounded offline audit of completed paired-probe raw evidence.

Uses Python Decimal and sorted full-content groups, not the Rust parser or its
online hash/window implementation. Requires logged timer watermarks. Resource
resets and incomplete captures fail closed as unsupported, not as verified loss.
No network, writes to evidence, source selection or performance recommendations.
A terminal row does not prove the process exited zero or its final sync succeeded;
the caller must retain/check exit status separately.
"""
import argparse
from collections import defaultdict
from decimal import Decimal, localcontext
from itertools import groupby
import json
from pathlib import Path
import re


def require(condition, message):
    if not condition:
        raise ValueError(message)


def scaled(value, price=False):
    require(isinstance(value, str) and
            re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value), "invalid decimal")
    with localcontext() as context:
        context.prec = max(40, len(value) + 6)
        number = Decimal(value) * 1_000_000
        require(number == number.to_integral_value(), "inexact financial amount")
        require(0 <= number <= (1_000_000 if price else 2**63 - 1), "amount range")
        return int(number)


def checked_hash(value):
    require(value is None or isinstance(value, str) and len(value.encode()) <= 256,
            "invalid hash")
    return value


def canonical_events(payload, assets, initialized):
    """Returns independent financial keys in wire order; no timestamp sorting."""
    wire = json.loads(payload)
    messages = wire if isinstance(wire, list) else [wire]
    events = []
    seen = initialized.copy()
    for message in messages:
        require(isinstance(message, dict), "nonobject event")
        kind = message["event_type"]
        event_hash = checked_hash(message.get("hash"))
        if kind in {"last_trade_price", "best_bid_ask", "new_market", "market_resolved"}:
            continue
        if kind == "tick_size_change":
            require(message["asset_id"] in assets and
                    scaled(message["new_tick_size"], price=True) > 0, "invalid tick")
            continue
        stamp = message["timestamp"]
        require(isinstance(stamp, str) and re.fullmatch(r"[0-9]+", stamp),
                "invalid exchange time")
        exchange = int(stamp) * 1000
        require(exchange <= 2**63 - 1, "exchange time range")
        if kind == "book":
            asset = message["asset_id"]
            require(asset in assets, "unrequested snapshot")
            entries = []
            for side, name in enumerate(("bids", "asks")):
                prices = set()
                for level in message[name]:
                    price = scaled(level["price"], price=True)
                    require(price not in prices, "duplicate snapshot price")
                    prices.add(price)
                    entries.append([side, price, scaled(level["size"]), None])
            snapshot_kind = "ReplacementSnapshot" if asset in seen else "InitialSnapshot"
            events.append([asset, snapshot_kind, exchange, event_hash, entries])
            seen.add(asset)
        elif kind == "price_change":
            grouped = {}
            require(message["price_changes"], "empty change batch")
            for change in message["price_changes"]:
                asset = change["asset_id"]
                require(asset in assets and asset in seen, "change before requested baseline")
                require(change["side"] in {"BUY", "SELL"}, "invalid side")
                grouped.setdefault(asset, []).append([
                    int(change["side"] == "SELL"), scaled(change["price"], price=True),
                    scaled(change["size"]), checked_hash(change.get("hash"))])
            events.extend([asset, "Change", exchange, event_hash, entries]
                          for asset, entries in grouped.items())
        else:
            raise ValueError(f"unknown event type: {kind}")
    initialized.clear()
    initialized.update(seen)
    return [(event[0], event[1], json.dumps(event, separators=(",", ":")))
            for event in events]


def pair(left, right):
    return {
        "asset": left["asset"], "kind": left["kind"],
        "a_ordinal": left["ordinal"], "b_ordinal": right["ordinal"],
        "a_event_index": left["event_index"], "b_event_index": right["event_index"],
        "a_generation": left["generation"], "b_generation": right["generation"],
        "a_ns": left["ns"], "b_ns": right["ns"],
        "delta_ns": right["ns"] - left["ns"],
    }


def epoch_pairs(observations, start, watermark, window):
    """Offline block partition + sort/group oracle; retains original source order."""
    blocks = defaultdict(list)
    for observation in observations:
        block = (observation["ns"] - start) // window
        require(block >= 0, "observation before epoch")
        blocks[block].append(observation)
    expected = []
    # A middle block needs its entire trailing guard to have advanced.
    for middle in range(1, (watermark - start) // window - 1):
        neighborhood = [o for block in (middle - 1, middle, middle + 1)
                        for o in blocks[block]]
        candidates = []
        for _, group in groupby(sorted(neighborhood, key=lambda o: o["key"]),
                                key=lambda o: o["key"]):
            copies = list(group)
            if len(copies) != 2 or {o["source"] for o in copies} != {0, 1}:
                continue
            left, right = sorted(copies, key=lambda o: o["source"])
            if all((o["ns"] - start) // window == middle for o in copies):
                candidates.append((left, right))
        candidates.sort(key=lambda copies: copies[0]["sequence"])
        previous = {}
        inverted = set()
        for left, right in candidates:
            asset = left["asset"]
            if asset in previous and right["sequence"] <= previous[asset]:
                inverted.add(asset)
            previous[asset] = right["sequence"]
        expected.extend(pair(left, right) for left, right in candidates
                        if left["asset"] not in inverted)
    return expected


def audit(path):
    path = Path(path)
    require(path.stat().st_size <= 256 << 20, "offline evidence limit is 256 MiB")
    config = None
    assets = set()
    initialized = [set(), set()]
    generations = [None, None]
    ordinals = {}
    observed = []
    expected = []
    actual = []
    measured_start = epoch_start = watermark = end = None
    window = sequence = received = changes = 0
    complete = None
    last_receive = 0
    with path.open() as stream:
        for row_number in range(1_000_000):
            line = stream.readline((16 << 20) + 1)
            if not line:
                break
            require(len(line) <= 16 << 20, "offline row limit is 16 MiB")
            require(complete is None, "data after completion")
            row = json.loads(line)
            kind = row["type"]
            if kind == "metadata":
                require(row_number == 0 and row["format_version"] == 1, "unknown format")
                require(row["policy"].get("matcher_advance_logged") is True,
                        "timer watermarks absent: exact offline replay unsupported")
                config = row["config"]
                assets = set(config["assets"])
                window = config["window_ms"] * 1_000_000
                require(window > 0, "zero window")
                require(0 < config["seconds"] <= 3600 and
                        config["seconds"] * 1_000_000_000 // window <= 100_000,
                        "offline window count limit exceeded")
                continue
            require(config is not None, "missing metadata")
            if kind == "connection" and row["details"]["phase"] == "attempt":
                source, generation = row["source"], row["generation"]
                require(source in (0, 1), "invalid source")
                require(generation == (0 if generations[source] is None else
                                       generations[source] + 1), "generation discontinuity")
                generations[source] = generation
                initialized[source].clear()
                ordinals[source, generation] = 0
            elif kind == "measurement_schedule":
                require(measured_start is None, "multiple measurement schedules")
                require(all(s == assets for s in initialized), "paired baselines incomplete")
                measured_start = epoch_start = watermark = row["start_mono_ns"]
                end = measured_start + config["seconds"] * 1_000_000_000
            elif kind == "received":
                source, generation = row["source"], row["generation"]
                require(source in (0, 1) and generations[source] == generation,
                        "received outside active generation")
                identity = source, generation
                require(row["ordinal"] == ordinals[identity] + 1, "ordinal discontinuity")
                ordinals[identity] = row["ordinal"]
                ns = row["stamp"]["mono_ns"]
                require(ns >= last_receive, "regressing application receive clock")
                last_receive = ns
                received += 1
                require(row["frame"] in {"text", "ping", "pong"},
                        "unexpected data/close in successful capture")
                if row["frame"] != "text" or row["payload"] == "PONG":
                    continue
                events = canonical_events(row["payload"], assets, initialized[source])
                if measured_start is None or not measured_start <= ns < end:
                    continue
                for event_index, (asset, event_kind, key) in enumerate(events):
                    require(sequence < 200_000, "offline canonical event limit is 200000")
                    require(ns >= watermark, "arrival behind logged matcher watermark")
                    observed.append(dict(asset=asset, kind=event_kind, key=key,
                                         source=source, generation=generation,
                                         ordinal=row["ordinal"], event_index=event_index,
                                         ns=ns, sequence=sequence))
                    sequence += 1
                    changes += int(event_kind == "Change")
                    watermark = ns
            elif kind == "matcher_advance":
                ns = row["mono_ns"]
                require(watermark is not None and watermark <= ns <= end,
                        "invalid timer watermark")
                watermark = ns
            elif kind == "renewal_cutoff":
                require(epoch_start is not None, "renewal before measurement schedule")
                require(watermark <= row["stamp"]["mono_ns"] <= end,
                        "renewal outside active measurement")
                expected.extend(epoch_pairs(observed, epoch_start, watermark, window))
                observed.clear()
                epoch_start = watermark = row["stamp"]["mono_ns"]
                initialized[row["source"]].clear()
            elif kind == "match":
                actual.append(row["pair"])
            elif kind == "matcher_resource_reset":
                raise ValueError("resource reset: exact event cutoff unsupported by offline audit")
            elif kind == "complete":
                complete = row
            else:
                require(kind in {"connection", "sent", "all_initial_snapshots_observed"},
                        f"unsupported evidence row {kind}")
        else:
            raise ValueError("offline row count exceeded")
    require(complete is not None and complete["successful"], "incomplete/failed capture")
    require(complete["raw_observations"] == complete["captured_observations"] == received,
            "raw observation accounting mismatch")
    require(complete["undrained_inputs"] == 0 and complete["resource_resets"] == 0,
            "unverified input/reset boundary")
    require(epoch_start is not None, "no measurement")
    expected.extend(epoch_pairs(observed, epoch_start, watermark, window))
    require(actual == expected, f"match decisions differ: actual={len(actual)}, expected={len(expected)}")
    require(complete["matched_pairs"] == len(actual), "pair count mismatch")
    matched_changes = sum(p["kind"] == "Change" for p in actual)
    return dict(verified_pairs=len(actual), measured_canonical_events=sequence,
                change_pairs=matched_changes, change_observations=changes,
                matched_change_fraction=(2 * matched_changes / changes if changes else None),
                raw_observations=received,
                process_exit_verified=False,
                interpretation="observed-content decisions verified; not exchange event identity or losslessness")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_jsonl", type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(audit(args.raw_jsonl), indent=2))
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.exit(1, f"NOT VERIFIED: {error}\n")
