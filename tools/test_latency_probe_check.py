"""Public offline-audit contract, including deliberately corrupted evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from check_latency_probe import audit


def fixture():
    rows = [{"type": "metadata", "format_version": 1,
             "policy": {"matcher_advance_logged": True},
             "config": {"assets": ["A"], "window_ms": 100, "seconds": 1}}]
    for source in (0, 1):
        rows.append(dict(type="connection", source=source, generation=0,
                         details={"phase": "attempt"}))
        rows.append(dict(type="received", source=source, generation=0, ordinal=1,
                         stamp={"mono_ns": (source + 1) * 10_000_000}, frame="text",
                         payload=json.dumps([
                             dict(event_type="book", asset_id="A", timestamp="50",
                                  bids=[], asks=[]),
                             dict(event_type="tick_size_change", asset_id="A",
                                  new_tick_size="0.01")]))
                    )
    rows.append(dict(type="measurement_schedule", start_mono_ns=1_000_000_000))
    ordinals = [1, 1]
    # One unique pair; repeated content; an unmatched quantity; same-asset
    # inversion in another block; and a pair straddling a window boundary.
    for source, ms, quantity in [(0, 1110, 7), (1, 1120, 7),
                                 (0, 1130, 8), (1, 1140, 8),
                                 (0, 1150, 8), (1, 1160, 8), (0, 1170, 9),
                                 (0, 1210, 11), (0, 1220, 12),
                                 (1, 1230, 12), (1, 1240, 11),
                                 (0, 1399, 13), (1, 1401, 13)]:
        ordinals[source] += 1
        payload = {"event_type": "price_change", "timestamp": "10", "hash": "same",
                   "price_changes": [{"asset_id": "A", "side": "BUY",
                                      "price": "0.125", "size": str(quantity)}]}
        rows.append(dict(type="received", source=source, generation=0,
                         ordinal=ordinals[source], stamp={"mono_ns": ms * 1_000_000},
                         frame="text", payload=json.dumps(payload)))
    rows.extend([
        {"type": "matcher_advance", "mono_ns": 2_000_000_000},
        {"type": "match", "pair": {
            "asset": "A", "kind": "Change", "a_ordinal": 2, "b_ordinal": 2,
            "a_event_index": 0, "b_event_index": 0, "a_generation": 0,
            "b_generation": 0, "a_ns": 1_110_000_000, "b_ns": 1_120_000_000,
            "delta_ns": 10_000_000}},
        {"type": "complete", "successful": True, "raw_observations": 15,
         "captured_observations": 15, "undrained_inputs": 0,
         "resource_resets": 0, "matched_pairs": 1},
    ])
    return rows


class Audit(unittest.TestCase):
    def check(self, rows):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            return audit(path)

    def test_complete_capture_rejects_ambiguous_unmatched_inverted_and_edge_pairs(self):
        result = self.check(fixture())
        self.assertEqual(result["verified_pairs"], 1)
        self.assertEqual(result["change_observations"], 13)
        self.assertEqual(result["matched_change_fraction"], 2 / 13)

    def test_renewal_starts_new_guards_and_requires_a_fresh_generation_baseline(self):
        rows = fixture()
        rows[0]["config"]["seconds"] = 2
        terminal = rows.pop()
        rows.extend([
            {"type": "renewal_cutoff", "source": 1, "stamp": {"mono_ns": 2_000_000_000}},
            {"type": "connection", "source": 1, "generation": 1,
             "details": {"phase": "attempt"}},
            {"type": "received", "source": 1, "generation": 1, "ordinal": 1,
             "stamp": {"mono_ns": 2_010_000_000}, "frame": "text",
             "payload": json.dumps(dict(event_type="book", asset_id="A", timestamp="1",
                                        bids=[], asks=[]))},
        ])
        for source, generation, ordinal, ns in [(0, 0, 9, 2_110_000_000),
                                               (1, 1, 2, 2_120_000_000)]:
            rows.append(dict(type="received", source=source, generation=generation,
                             ordinal=ordinal, stamp={"mono_ns": ns}, frame="text",
                             payload=json.dumps({"event_type": "price_change", "timestamp": "10",
                                                 "hash": "same", "price_changes": [{
                                                     "asset_id": "A", "side": "BUY",
                                                     "price": "0.125", "size": "7"}]})))
        rows.extend([
            {"type": "matcher_advance", "mono_ns": 3_000_000_000},
            {"type": "match", "pair": {
                "asset": "A", "kind": "Change", "a_ordinal": 9, "b_ordinal": 2,
                "a_event_index": 0, "b_event_index": 0, "a_generation": 0,
                "b_generation": 1, "a_ns": 2_110_000_000, "b_ns": 2_120_000_000,
                "delta_ns": 10_000_000}},
            terminal | {"raw_observations": 18, "captured_observations": 18, "matched_pairs": 2},
        ])
        self.assertEqual(self.check(rows)["verified_pairs"], 2)
        without_baseline = copy.deepcopy(rows)
        row = next(row for row in without_baseline if
                   row.get("stamp", {}).get("mono_ns") == 2_010_000_000)
        row["payload"] = json.dumps({"event_type": "last_trade_price"})
        with self.assertRaisesRegex(ValueError, "baseline"):
            self.check(without_baseline)

    def test_tampered_amount_arrival_and_missing_or_duplicate_matches_fail(self):
        base = fixture()
        mutations = []
        amount = copy.deepcopy(base)
        receipt = next(row for row in amount if row.get("stamp", {}).get("mono_ns") == 1_120_000_000)
        wire = json.loads(receipt["payload"])
        wire["price_changes"][0]["size"] = "7.000001"
        receipt["payload"] = json.dumps(wire)
        mutations.append(amount)
        wrong_sign = copy.deepcopy(base)
        wrong_sign[-2]["pair"]["delta_ns"] = -10_000_000
        mutations.append(wrong_sign)
        omitted = copy.deepcopy(base)
        del omitted[-2]
        omitted[-1]["matched_pairs"] = 0
        mutations.append(omitted)
        duplicated = copy.deepcopy(base)
        duplicated.insert(-1, copy.deepcopy(duplicated[-2]))
        duplicated[-1]["matched_pairs"] = 2
        mutations.append(duplicated)
        for rows in mutations:
            with self.subTest(rows=rows[-2:]), self.assertRaises(ValueError):
                self.check(rows)

    def test_incomplete_unknown_progress_or_reset_capture_is_not_verified(self):
        base = fixture()
        old = copy.deepcopy(base)
        del old[0]["policy"]["matcher_advance_logged"]
        reset = copy.deepcopy(base)
        reset.insert(-1, {"type": "matcher_resource_reset"})
        failed = copy.deepcopy(base)
        failed[-1]["successful"] = False
        for rows in [base[:-1], old, reset, failed]:
            with self.subTest(rows=rows[-1:]), self.assertRaises(ValueError):
                self.check(rows)


if __name__ == "__main__":
    unittest.main()
