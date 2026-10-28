"""Safety and comparison contracts; no Cargo, exchange calls or performance runs."""
import copy
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from latency_harness import (Failure, Runner, apply_candidate_patch, compare, count_processor_allocations, git,
                             validate_decoder_profile, validate_result)


def result(latency=100):
    distribution = {key: latency for key in
                    ("p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns")}
    distribution["samples"] = 5000
    return {
        "books": 100, "mode": "ingress", "destination": "file",
        "offered_rate": 1000, "initialization_messages": 100,
        "warmup_messages": 2000, "measured_messages": 5000, "messages": 7100,
        "records_compared": 32123, "periodic_snapshots": 0,
        "measured_offered_rate": 1000, "measured_offer_seconds": 5,
        "drain_seconds": 0.05, "max_measured_offer_lag_us": 100,
        "ingress_age": distribution.copy(),
        "processing_including_encoding": distribution.copy(),
        "output": {"error": None, "records_confirmed": 32123, "records_written": 32123},
    }


def trial(arm, pair, latency=100):
    return {"arm": arm, "pair": pair, "result": result(latency),
            "resources": {"max_rss_kib": 10000}}


class Contracts(unittest.TestCase):
    def test_explicit_patch_preserves_base_and_rejects_measurement_changes(self):
        for name, allowed in [("src/book.rs", True), ("tests/oracle.rs", False),
                              ("src/util/sbe.rs", False), ("Cargo.toml", False)]:
            with self.subTest(path=name), tempfile.TemporaryDirectory() as d:
                root = Path(d)
                git(root, "init", "--quiet")
                source = root / name
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("before\n")
                git(root, "add", ".")
                git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                    "commit", "--quiet", "-m", "disposable test baseline")
                head = git(root, "rev-parse", "HEAD")
                source.write_text("after\n")
                patch = root / "candidate.patch"
                patch.write_text(git(root, "diff") + "\n")
                git(root, "checkout", "--", name)
                if allowed:
                    # A baseline-only patch is not A/A and must not bypass the
                    # research gate just because both commit refs are HEAD.
                    command = [sys.executable, str(Path(__file__).with_name("latency_harness.py").resolve()),
                               "--baseline", "HEAD", "--candidate", "HEAD", "--baseline-patch", str(patch)]
                    gated = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=10)
                    self.assertNotEqual(gated.returncode, 0)
                    self.assertIn("different trees require a researched hypothesis", gated.stderr)
                    tree = apply_candidate_patch(root, patch)
                    self.assertEqual(tree, git(root, "write-tree"))
                    self.assertEqual(source.read_text(), "after\n")
                    self.assertFalse(git(root, "diff", "--name-only"))
                    patch.write_text("diff --git a/tests/shared.rs b/tests/shared.rs\n"
                                     "new file mode 100644\n--- /dev/null\n+++ b/tests/shared.rs\n"
                                     "@@ -0,0 +1 @@\n+// shared measurement\n")
                    shared_tree = apply_candidate_patch(root, patch, measurement=True)
                    self.assertNotEqual(tree, shared_tree)
                    self.assertEqual(source.read_text(), "after\n")
                    self.assertEqual((root / "tests/shared.rs").read_text(), "// shared measurement\n")
                else:
                    with self.assertRaises(Failure):
                        apply_candidate_patch(root, patch)
                self.assertEqual(git(root, "rev-parse", "HEAD"), head)

    def test_rename_cannot_move_a_frozen_test_into_candidate_source(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            git(root, "init", "--quiet")
            (root / "tests").mkdir()
            (root / "src").mkdir()
            original = root / "tests/oracle.rs"
            original.write_text("// original oracle\n")
            git(root, "add", ".")
            git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                "commit", "--quiet", "-m", "disposable rename baseline")
            original.rename(root / "src/moved.rs")
            git(root, "add", "--all")
            patch = root / "rename.patch"
            patch.write_text(git(root, "diff", "--cached", "--find-renames") + "\n")
            git(root, "reset", "--hard", "HEAD")
            with self.assertRaises(Failure):
                apply_candidate_patch(root, patch)

    def test_result_requires_exact_workload_and_complete_oracle(self):
        validate_result(result(), 5, 1000, "ingress", "file")
        for field, wrong in [("measured_messages", 4999), ("mode", "socket"),
                             ("warmup_messages", 0), ("measured_offered_rate", 900)]:
            with self.subTest(field=field), self.assertRaises(Failure):
                changed = result()
                changed[field] = wrong
                validate_result(changed, 5, 1000, "ingress", "file")
        for output in [{"error": "broken pipe"}, {"records_written": 32122}]:
            changed = result()
            changed["output"].update(output)
            with self.assertRaises(Failure):
                validate_result(changed, 5, 1000, "ingress", "file")

    def test_missing_and_nonfinite_measurements_fail_closed(self):
        with self.assertRaises(Failure):
            validate_result(result(), 5, 1000, "ingress", "file", "snapshot-burst")
        for value in [None, -1, float("nan"), float("inf")]:
            changed = result()
            changed["processing_including_encoding"]["p99_ns"] = value
            with self.assertRaises(Failure):
                validate_result(changed, 5, 1000, "ingress", "file")
        changed = result()
        del changed["output"]
        with self.assertRaises(Failure):
            validate_result(changed, 5, 1000, "ingress", "file")

    def test_arrival_counts_units_and_pacing_are_validated(self):
        paced = result()
        lag = paced["ingress_age"].copy()
        lag["max_ns"] = 100000
        gaps = lag | {"samples": 4999}
        paced["offered_arrivals"] = {
            "pacing": "thread_sleep_absolute", "lag": lag, "interarrival": gaps,
            "short_gaps": 3, "short_gap_threshold_ns": 500000,
        }
        validate_result(paced, 5, 1000, "ingress", "file")
        for path, wrong in [(('lag', 'samples'), 4999), (('interarrival', 'samples'), 5000),
                            (('lag', 'p99_ns'), float('nan')), (('lag', 'max_ns'), 99999),
                            (('short_gaps',), -1), (('short_gaps',), 5000),
                            (('short_gap_threshold_ns',), 500),
                            (('pacing',), 'tokio_sleep_absolute')]:
            changed = copy.deepcopy(paced)
            target = changed['offered_arrivals']
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = wrong
            with self.subTest(path=path, wrong=wrong), self.assertRaises(Failure):
                validate_result(changed, 5, 1000, "ingress", "file")
        a, b = trial("baseline", 0), trial("candidate", 0)
        a["result"] = paced
        b["result"] = copy.deepcopy(paced)
        b["result"]["offered_arrivals"]["pacing"] = "tokio_sleep_absolute"
        with self.assertRaises(Failure):
            compare([a, b], True)

    def test_shape_samples_match_offered_trace_not_just_total(self):
        shaped = result()
        # Measured offsets 2000..6999: one replacement at 4000; remaining
        # offsets partition by modulo three into one/four/sixteen changes.
        counts = {"replacement_array": 1, "delta_1": 1667,
                  "delta_4": 1665, "delta_16": 1667}
        shaped["local_stages"] = {"processing_by_shape": {
            name: shaped["processing_including_encoding"] | {"samples": count}
            for name, count in counts.items()}}
        validate_result(shaped, 5, 1000, "ingress", "file")
        changed = copy.deepcopy(shaped)
        changed["local_stages"]["processing_by_shape"]["delta_1"]["samples"] -= 1
        changed["local_stages"]["processing_by_shape"]["delta_4"]["samples"] += 1
        with self.assertRaises(Failure):
            validate_result(changed, 5, 1000, "ingress", "file")
        changed = copy.deepcopy(shaped)
        changed["local_stages"]["processing_by_shape"]["replacement_array"]["p99_ns"] = float("nan")
        with self.assertRaises(Failure):
            validate_result(changed, 5, 1000, "ingress", "file")

        def samples(n):
            return {key: (n if key == "samples" else 100 if n else None)
                    for key in shaped["processing_including_encoding"]}

        stages = shaped["local_stages"]
        stages["shape_order"] = list(counts)
        stages["owner_by_shape"] = [samples(n) for n in counts.values()]
        stages["recorder_by_shape"] = [samples(n) for n in counts.values()]
        stages["snapshot_neighborhood_order"] = ["replacement_array", "next_three_affected",
                                                 "next_three_unrelated", "later_updates"]
        stages["ingress_age_by_snapshot_neighborhood"] = [samples(n) for n in [1, 3, 0, 4996]]
        validate_result(shaped, 5, 1000, "ingress", "file")
        for group in ("owner_by_shape", "recorder_by_shape", "ingress_age_by_snapshot_neighborhood"):
            changed = copy.deepcopy(shaped)
            changed["local_stages"][group][0]["samples"] += 1
            changed["local_stages"][group][1]["samples"] -= 1
            with self.subTest(group=group), self.assertRaises(Failure):
                validate_result(changed, 5, 1000, "ingress", "file")
        changed = copy.deepcopy(shaped)
        changed["local_stages"]["ingress_age_by_snapshot_neighborhood"][2]["p99_ns"] = 0
        with self.assertRaises(Failure):
            validate_result(changed, 5, 1000, "ingress", "file")

    def test_pairing_is_by_identity_not_execution_order(self):
        trials = [trial("baseline", 0), trial("candidate", 0, 80),
                  trial("candidate", 1, 80), trial("baseline", 1),
                  trial("baseline", 2), trial("candidate", 2, 80)]
        report = compare(trials, same_tree=False)
        self.assertEqual(report["decision"], "promising_requires_qualification")
        self.assertAlmostEqual(report["processing_p99_improvement_percent"][0], 20)
        self.assertEqual(compare(trials, same_tree=True)["decision"], "control_only")
        noisy = copy.deepcopy(trials)
        noisy[-1]["result"]["processing_including_encoding"]["p99_ns"] = 105
        self.assertEqual(compare(noisy, False)["decision"], "inconclusive_or_regression")
        different_measurement = copy.deepcopy(trials)
        different_measurement[-1]["result"]["local_stages"] = {
            "oracle_placement": "after_output_drain"}
        with self.assertRaises(Failure):
            compare(different_measurement, False)

    def test_decoder_profile_requires_each_case_and_finite_measurements(self):
        cases = [("control", "clock")] + [(case, stage)
            for case in ("retained_snapshots", "retained_deltas", "synthetic_depth_8",
                         "synthetic_depth_256", "synthetic_depth_2048", "synthetic_changes_1",
                         "synthetic_changes_4", "synthetic_changes_16", "synthetic_changes_64")
            for stage in ("envelope", "decode_prevalidated", "envelope_and_decode", "session_decoder")]
        reports = [dict(case=case, stage=stage, samples=10000,
                        p50_ns=100, p99_ns=180, total_ns=1200000) for case, stage in cases]
        validate_decoder_profile(reports)
        for malformed in [reports[:-1], reports + [reports[0]],
                          [reports[0] | {"total_ns": float("nan")}] + reports[1:],
                          [reports[0] | {"samples": 9999}] + reports[1:]]:
            with self.assertRaises(Failure):
                validate_decoder_profile(malformed)

    def test_queue_windows_preserve_counts_even_with_uneven_partitions(self):
        value = result()
        # Seven inputs split by floor(index * 5 / 7): 0,0,1,2,2,3,4.
        value.update(offered_rate=7, warmup_messages=14, measured_messages=7,
                     messages=121, measured_offered_rate=7, measured_offer_seconds=1)
        for stage in ("ingress_age", "processing_including_encoding"):
            value[stage]["samples"] = 7
        value["local_stages"] = {"ingress_age_by_window": [
            value["ingress_age"] | {"samples": count} for count in (2, 1, 2, 1, 1)]}
        validate_result(value, 1, 7, "ingress", "file")
        changed = copy.deepcopy(value)
        changed["local_stages"]["ingress_age_by_window"][0]["samples"] = 1
        changed["local_stages"]["ingress_age_by_window"][1]["samples"] = 2
        with self.assertRaises(Failure):
            validate_result(changed, 1, 7, "ingress", "file")

    def test_allocation_counts_reject_unfiltered_or_incomplete_capture(self):
        self.assertEqual(count_processor_allocations(
            "root;Processor::process;decode; 17\nroot;Processor::process;encode; 3\n"), 20)
        for text in ("", "root;oracle; 17\n", "Processor::process; -1\n",
                     "Processor::process; nan\n", "Processor::process; 0\n"):
            with self.subTest(text=text), self.assertRaises(Failure):
                count_processor_allocations(text)

    def test_fast_but_incomplete_or_regressed_candidate_cannot_win(self):
        trials = [t for i in range(3) for t in
                  (trial("baseline", i), trial("candidate", i, 80))]
        changed = copy.deepcopy(trials)
        changed[-1]["result"]["records_compared"] -= 1
        with self.assertRaises(Failure):
            compare(changed, False)
        with self.assertRaises(Failure):
            compare(trials[:-1], False)
        changed = copy.deepcopy(trials)
        changed[-1]["resources"]["max_rss_kib"] = 12000
        self.assertEqual(compare(changed, False)["decision"], "inconclusive_or_regression")

    def test_commands_record_failure_and_preserve_bounded_logs(self):
        with tempfile.TemporaryDirectory() as d:
            runner = Runner(Path(d), time.monotonic() + 5)
            with self.assertRaises(Failure):
                runner.run("failed", [sys.executable, "-c", "print('evidence');exit(7)"], Path(d))
            self.assertIn("evidence", (Path(d) / "failed.log").read_text())
            self.assertEqual(runner.steps[-1]["exit_code"], 7)
            with self.assertRaises(Failure):
                runner.run("large", [sys.executable, "-c", "print('x'*100000)"],
                           Path(d), log_limit=128)
            self.assertLessEqual((Path(d) / "large.log").stat().st_size, 128)

    def test_budget_and_cancellation_do_not_leave_a_running_command(self):
        with tempfile.TemporaryDirectory() as d:
            runner = Runner(Path(d), time.monotonic() + 0.2)
            began = time.monotonic()
            with self.assertRaises(Failure):
                runner.run("timeout", [sys.executable, "-c", "import time;time.sleep(30)"], Path(d))
            self.assertLess(time.monotonic() - began, 3)
            self.assertIsNotNone(runner.steps[-1]["exit_code"])
            self.assertIn("budget", runner.steps[-1]["error"])

    def test_timeout_also_cancels_descendant_after_parent_exits(self):
        with tempfile.TemporaryDirectory() as d:
            marker = Path(d) / "survived"
            child = f"import time;from pathlib import Path;print('child ready',flush=True);time.sleep(0.7);Path({str(marker)!r}).touch()"
            parent = f"import subprocess,sys;subprocess.Popen([sys.executable,'-c',{child!r}])"
            runner = Runner(Path(d), time.monotonic() + 0.2)
            with self.assertRaises(Failure):
                runner.run("descendant", [sys.executable, "-c", parent], Path(d))
            self.assertIn("child ready", (Path(d) / "descendant.log").read_text())
            time.sleep(0.8)
            self.assertFalse(marker.exists(), "a descendant survived the campaign cutoff")


if __name__ == "__main__":
    unittest.main()
