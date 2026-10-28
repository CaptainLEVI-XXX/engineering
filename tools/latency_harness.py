#!/usr/bin/env python3
"""Bounded, local-only A/B runner for the existing independent SBE replay oracle.

No model calls, live-feed commands, commits, pushes or automatic code promotion.
Candidates are explicit Git commits plus an optional preserved source-only patch.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import selectors
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time

TEST = "mixed_recording_matches_every_oracle_record"
DECODER_TEST = "profile_decoder_stages_against_retained_oracle"
TIERS = {"smoke": 5, "integration": 20, "qualification": 120}
FROZEN = ["Cargo.toml", "Cargo.lock", "sbe_schemas", "src/util", "tests",
          "rust-toolchain", "rust-toolchain.toml", ".cargo"]
QUANTILES = ["p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns"]


class Failure(Exception):
    pass


def require(condition, message):
    if not condition:
        raise Failure(message)


def digest(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


class Runner:
    def __init__(self, output, deadline, env=None):
        self.output, self.deadline = output, deadline
        self.env = os.environ.copy() if env is None else env.copy()
        self.steps = []

    def run(self, name, command, cwd, extra_env=None, log_limit=8 << 20):
        require(time.monotonic() < self.deadline, "campaign time budget exhausted")
        step = {"name": name, "command": list(map(str, command)), "cwd": str(cwd)}
        self.steps.append(step)
        began = time.monotonic()
        process = None
        print(name, flush=True)
        try:
            with (self.output / f"{name}.log").open("xb") as log:
                process = subprocess.Popen(command, cwd=cwd,
                    env=self.env | (extra_env or {}), stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, start_new_session=True)
                with process.stdout, selectors.DefaultSelector() as selector:
                    selector.register(process.stdout, selectors.EVENT_READ)
                    written = 0
                    while selector.get_map():
                        require(time.monotonic() < self.deadline, "campaign time budget exhausted")
                        for key, _ in selector.select(0.1):
                            chunk = os.read(key.fd, 8192)
                            if not chunk:
                                selector.unregister(key.fileobj)
                                continue
                            log.write(chunk[:max(0, log_limit - written)])
                            written += len(chunk)
                            require(written <= log_limit, "command log byte budget exhausted")
                    process.wait(timeout=max(0.001, self.deadline - time.monotonic()))
                require(process.returncode == 0, f"{name} failed: exit {process.returncode}")
        except BaseException as error:
            step["error"] = str(error) or type(error).__name__
            raise
        finally:
            if process is not None:
                # Kill the whole owned group, including a stalled output consumer.
                # Also clean up descendants left behind by an exited parent.
                for sig in (signal.SIGTERM, signal.SIGKILL):
                    try:
                        os.killpg(process.pid, sig)
                    except ProcessLookupError:
                        break
                    if sig == signal.SIGTERM:
                        try:
                            process.wait(timeout=1)
                        except subprocess.TimeoutExpired:
                            pass
                process.wait()
                step["exit_code"] = process.returncode
            step["elapsed_seconds"] = time.monotonic() - began
        return (self.output / f"{name}.log").read_text(errors="replace")


def validate_result(value, seconds, rate, mode, destination, workload="mixed"):
    try:
        require(value.get("workload", "mixed") == workload, "workload scenario mismatch")
        for key, expected in {
            "books": 100, "mode": mode, "destination": destination, "offered_rate": rate,
            "initialization_messages": 100, "warmup_messages": rate * 2,
            "measured_messages": seconds * rate, "messages": 100 + (seconds + 2) * rate,
        }.items():
            require(value[key] == expected, f"workload mismatch: {key}")
        count = value["records_compared"]
        require(count > 0 and value["output"]["error"] is None, "incomplete output oracle")
        require(value["output"]["records_confirmed"] == count == value["output"]["records_written"],
                "output record count mismatch")
        for stage in ("ingress_age", "processing_including_encoding"):
            require(value[stage]["samples"] == seconds * rate, "missing latency samples")
            for key in QUANTILES:
                number = value[stage][key]
                require(isinstance(number, (int, float)) and math.isfinite(number) and number >= 0,
                        f"invalid latency: {stage}.{key}")
        shapes = value.get("local_stages", {}).get("processing_by_shape")
        if shapes is not None:
            names = ["replacement_array", "delta_1", "delta_4", "delta_16"]
            require(set(shapes) == set(names), "unknown or missing input shape")
            expected = dict.fromkeys(names, 0)
            period = 20 if workload == "snapshot-burst" else 4000
            for n in range(2 * rate, (seconds + 2) * rate):
                expected[names[0 if n % period == 0 else 1 + n % 3]] += 1
            for name, samples in expected.items():
                require(shapes[name]["samples"] == samples, f"input shape sample mismatch: {name}")
                for key in QUANTILES:
                    number = shapes[name][key]
                    require(number is None if samples == 0 else
                            isinstance(number, (int, float)) and math.isfinite(number) and number >= 0,
                            f"invalid shape latency: {name}.{key}")
        stages = value.get("local_stages", {})
        if any(name in stages for name in ("owner_by_shape", "recorder_by_shape",
                                           "ingress_age_by_snapshot_neighborhood")):
            names = ["replacement_array", "delta_1", "delta_4", "delta_16"]
            require(shapes is not None and stages["shape_order"] == names, "stage shape order mismatch")
            counts = [0, 0, 0, 0]
            period = 20 if workload == "snapshot-burst" else 4000
            for offset in range(2 * rate, (seconds + 2) * rate):
                distance = offset % period
                if distance == 0:
                    group = 0
                elif distance <= 3:
                    first = offset // period if workload == "snapshot-burst" else 0
                    affected = {(first + i) % 100 for i in range(8)}
                    group = 1 if offset % 100 in affected else 2
                else:
                    group = 3
                counts[group] += 1
            require(stages["snapshot_neighborhood_order"] == ["replacement_array", "next_three_affected",
                    "next_three_unrelated", "later_updates"], "snapshot neighborhood order mismatch")
            for label, expected_counts in [
                ("owner_by_shape", [shapes[name]["samples"] for name in names]),
                ("recorder_by_shape", [shapes[name]["samples"] for name in names]),
                ("ingress_age_by_snapshot_neighborhood", counts),
            ]:
                require(len(stages[label]) == 4, f"missing stage groups: {label}")
                for report, samples in zip(stages[label], expected_counts):
                    require(report["samples"] == samples, f"stage sample count mismatch: {label}")
                    for key in QUANTILES:
                        number = report[key]
                        require(number is None if samples == 0 else
                                isinstance(number, (int, float)) and math.isfinite(number) and number >= 0,
                                f"invalid stage latency: {label}.{key}")
        windows = value.get("local_stages", {}).get("ingress_age_by_window")
        if windows is not None:
            require(len(windows) == 5, "missing queue-age windows")
            total = seconds * rate
            for index, window in enumerate(windows):
                samples = ((index + 1) * total + 4) // 5 - (index * total + 4) // 5
                require(window["samples"] == samples, "queue-age window count mismatch")
                for key in QUANTILES:
                    number = window[key]
                    require(number is None if samples == 0 else
                            isinstance(number, (int, float)) and math.isfinite(number) and number >= 0,
                            f"invalid queue-age window: {index}.{key}")
        arrivals = value.get("offered_arrivals")
        if arrivals is not None:
            require(arrivals["pacing"] == ("thread_sleep_absolute" if mode == "ingress"
                                          else "tokio_sleep_absolute"), "arrival pacing mismatch")
            for name, samples in [("lag", seconds * rate), ("interarrival", seconds * rate - 1)]:
                require(arrivals[name]["samples"] == samples, f"missing arrival samples: {name}")
                for key in QUANTILES:
                    number = arrivals[name][key]
                    require(number is None if samples == 0 else
                            isinstance(number, (int, float)) and math.isfinite(number) and number >= 0,
                            f"invalid arrival timing: {name}.{key}")
            require(type(arrivals["short_gaps"]) is int
                    and 0 <= arrivals["short_gaps"] <= seconds * rate - 1,
                    "invalid short-gap count")
            require(arrivals["short_gap_threshold_ns"] == 1_000_000_000 // (2 * rate),
                    "short-gap threshold mismatch")
            require(arrivals["lag"]["max_ns"] // 1000 == value["max_measured_offer_lag_us"],
                    "offered-lag maximum mismatch")
        for key in ("measured_offered_rate", "measured_offer_seconds", "drain_seconds",
                    "max_measured_offer_lag_us"):
            require(math.isfinite(value[key]) and value[key] >= 0, f"invalid timing: {key}")
        require(value["measured_offer_seconds"] > 0, "zero measurement duration")
        require(0.95 * rate <= value["measured_offered_rate"] <= 1.05 * rate,
                "offered rate outside predeclared 5% tolerance; not comparable")
        if seconds >= 120:
            require(value["periodic_snapshots"] >= 184, "minute snapshots absent")
    except (KeyError, TypeError, ValueError) as error:
        raise Failure(f"invalid benchmark report: {error}") from error


def compare(trials, same_tree):
    pairs = {}
    for trial in trials:
        pair = pairs.setdefault(trial["pair"], {})
        require(trial["arm"] not in pair, "duplicate pair member")
        pair[trial["arm"]] = trial
    require(pairs, "no trial evidence")
    gains, guards = [], []
    for pair in pairs.values():
        require(set(pair) == {"baseline", "candidate"}, "incomplete pair")
        a, b = pair["baseline"]["result"], pair["candidate"]["result"]
        require(a.get("workload", "mixed") == b.get("workload", "mixed"), "paired scenario differs")
        require(a.get("offered_arrivals", {}).get("pacing") ==
                b.get("offered_arrivals", {}).get("pacing"), "paired arrival pacing differs")
        require(a.get("local_stages", {}).get("oracle_placement") ==
                b.get("local_stages", {}).get("oracle_placement"), "paired oracle placement differs")
        for key in ("records_compared", "periodic_snapshots", "messages", "measured_messages",
                    "books", "offered_rate", "mode", "destination"):
            require(a[key] == b[key], f"paired workload differs: {key}")
        before, after = (x["processing_including_encoding"]["p99_ns"] for x in (a, b))
        require(before > 0, "zero baseline processing duration")
        gains.append(100 * (1 - after / before))
        guards.append(
            b["ingress_age"]["p99_ns"] <= a["ingress_age"]["p99_ns"] * 1.05
            and b["processing_including_encoding"]["p999_ns"] <= a["processing_including_encoding"]["p999_ns"] * 1.05
            and b["measured_offer_seconds"] + b["drain_seconds"] <=
                (a["measured_offer_seconds"] + a["drain_seconds"]) * 1.05
            and pair["candidate"]["resources"]["max_rss_kib"] <=
                pair["baseline"]["resources"]["max_rss_kib"] * 1.10)
    decision = "inconclusive_or_regression"
    if same_tree:
        decision = "control_only"
    elif len(pairs) >= 3 and min(gains) > 0 and statistics.median(gains) >= 5 and all(guards):
        decision = "promising_requires_qualification"
    return {
        "decision": decision, "processing_p99_improvement_percent": gains,
        "decision_scope": "Tail-latency screen only; not an efficiency verdict or promotion decision.",
        "median_processing_p99_improvement_percent": statistics.median(gains),
        "regression_guards_passed": guards,
        "inference": "Exploratory paired screening, not statistical confidence or permission to promote.",
    }


def validate_decoder_profile(reports):
    expected = {("control", "clock")} | {
        (case, stage)
        for case in ("retained_snapshots", "retained_deltas", "synthetic_depth_8",
                     "synthetic_depth_256", "synthetic_depth_2048", "synthetic_changes_1",
                     "synthetic_changes_4", "synthetic_changes_16", "synthetic_changes_64")
        for stage in ("envelope", "decode_prevalidated", "envelope_and_decode", "session_decoder")}
    observed = set()
    for report in reports:
        key = (report.get("case"), report.get("stage"))
        require(key in expected and key not in observed, "unexpected or duplicate decoder case")
        observed.add(key)
        require(report.get("samples") == 10000, "decoder sample count mismatch")
        for field in ("p50_ns", "p99_ns", "total_ns"):
            value = report.get(field)
            require(isinstance(value, (int, float)) and math.isfinite(value) and value > 0,
                    f"invalid decoder measurement: {field}")
        require(report["p50_ns"] <= report["p99_ns"], "unordered decoder percentiles")
    require(observed == expected, "missing decoder cases")


def count_processor_allocations(text):
    total = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        stack, separator, count = line.rpartition(" ")
        require(separator and "Processor::process" in stack and count.isdecimal(),
                "invalid or unfiltered allocation stack")
        total += int(count)
    require(total > 0, "empty allocation stack capture")
    return total


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True, timeout=15).strip()


def apply_candidate_patch(worktree, patch, measurement=False):
    """Only in an owned disposable checkout; stage to fingerprint its exact tree."""
    before = git(worktree, "write-tree")
    git(worktree, "apply", "--index", "--whitespace=error-all", str(patch))
    paths = git(worktree, "diff", "--cached", before, "--no-renames", "--name-only", "-z").split("\0")
    allowed = (lambda p: p.startswith("tests/") and p.endswith(".rs")) if measurement else (
        lambda p: p.startswith("src/") and p.endswith(".rs") and not p.startswith("src/util/"))
    require(paths and all(allowed(p) for p in paths if p), "patch changes files outside its allowed boundary")
    require(bool(git(worktree, "diff", "--cached", before, "--stat")), "empty patch")
    return git(worktree, "write-tree")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, help="existing Git commit/ref")
    parser.add_argument("--candidate", required=True, help="existing Git commit/ref; same ref runs A/A")
    parser.add_argument("--baseline-patch", type=Path,
                        help="explicit source-only patch for the disposable baseline, e.g. a previous experimental candidate")
    parser.add_argument("--candidate-patch", type=Path,
                        help="explicit source-only patch applied only to disposable candidate; never auto-committed")
    parser.add_argument("--measurement-patch", type=Path,
                        help="explicit test-only patch applied identically to both arms")
    parser.add_argument("--hypothesis", type=Path, help="evidence-gate JSON; required for different trees")
    parser.add_argument("--tier", choices=TIERS, default="smoke")
    parser.add_argument("--mode", choices=["ingress", "socket"], default="ingress")
    parser.add_argument("--destination", choices=["file", "pipe", "throttled"], default="file")
    parser.add_argument("--workload", choices=["mixed", "snapshot-burst", "native-ids"], default="mixed")
    parser.add_argument("--pairs", type=int, choices=range(1, 6), default=3)
    rate_options = parser.add_mutually_exclusive_group()
    rate_options.add_argument("--rate", type=int, default=1000)
    rate_options.add_argument("--rates", type=int, nargs="+",
                              help="up to three distinct ascending rates; one shared build per arm")
    parser.add_argument("--decoder-profile", action="store_true",
                        help="also compare the identical public-API decoder diagnostic in both release builds")
    parser.add_argument("--allocation-profile", action="store_true",
                        help="after timing, run bounded heaptrack replay per arm; instrumented timings never enter comparisons")
    parser.add_argument("--budget-seconds", type=int, default=900, help="includes builds/checks/trials; cleanup may add seconds")
    parser.add_argument("--output", type=Path, help="new results directory; never overwrites")
    args = parser.parse_args()
    require(sys.platform == "linux", "runner requires Linux and GNU /usr/bin/time")
    rates = args.rates or [args.rate]
    require(1 <= len(rates) <= 3 and rates == sorted(set(rates))
            and all(1 <= rate <= 5000 for rate in rates)
            and 1 <= args.budget_seconds <= 7200, "invalid resource budget")
    require(Path("/usr/bin/time").is_file(), "GNU time is required")
    if args.allocation_profile:
        require(shutil.which("heaptrack") and shutil.which("heaptrack_print"), "heaptrack tools are required")
    root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel"))
    refs = {arm: git(root, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}")
            for arm, ref in (("baseline", args.baseline), ("candidate", args.candidate))}
    source_patches = {}
    for arm, path in (("baseline", args.baseline_patch), ("candidate", args.candidate_patch)):
        if path is not None:
            require(0 < path.stat().st_size <= 1 << 20, f"{arm} patch must be 1 byte to 1MiB")
        source_patches[arm] = path.read_bytes() if path is not None else None
    measurement_bytes = None
    if args.measurement_patch is not None:
        require(0 < args.measurement_patch.stat().st_size <= 1 << 20, "measurement patch must be 1 byte to 1MiB")
        measurement_bytes = args.measurement_patch.read_bytes()
    same = source_patches["baseline"] == source_patches["candidate"] and git(root, "rev-parse", refs["baseline"] + "^{tree}") == git(root, "rev-parse", refs["candidate"] + "^{tree}")
    hypothesis = None
    if not same:
        require(args.hypothesis is not None, "different trees require a researched hypothesis JSON")
        require(args.hypothesis.stat().st_size <= 65536, "hypothesis exceeds 64KiB")
        hypothesis = json.loads(args.hypothesis.read_text())
        require(isinstance(hypothesis, dict), "hypothesis must be an object")
        require(hypothesis.get("status") == "ready-for-experiment", "hypothesis is not ready")
        for key in ("id", "observed_cost", "implementation_evidence", "applicability",
                    "change", "correctness_risks", "falsifier", "verification"):
            require(isinstance(hypothesis.get(key), str) and hypothesis[key].strip(), f"missing evidence: {key}")
        require(isinstance(hypothesis.get("references"), list) and hypothesis["references"]
                and all(isinstance(x, str) and x.startswith("https://")
                for x in hypothesis["references"]), "missing research references")
    require(not git(root, "diff", "--name-only", refs["baseline"], refs["candidate"], "--", *FROZEN),
            "manifest, lockfile, schema, supplied helpers, tests or build config changed; establish a new shared baseline first")
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("C5_"):
            del env[key]
    forbidden = [k for k in env if k in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC", "CARGO_BUILD_TARGET")
                 or k.startswith("CARGO_PROFILE_")]
    require(not forbidden, "remove ambient build overrides before comparison: " + ", ".join(forbidden))
    env["PATH"] = str(Path.home() / ".cargo/bin") + os.pathsep + env.get("PATH", "")
    env["CARGO_NET_OFFLINE"] = "true"
    env["CARGO_TARGET_DIR"] = str(root / "target")
    # The same advisory lock as the existing repository build/benchmark workflow.
    # It excludes cooperating commands, not arbitrary other processes on the host.
    with open("/tmp/voluntas-cargo.lock", "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Failure("another build or experiment owns the benchmark lock") from error
        run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}"
        output = (args.output or root / ".amp/in/artifacts/harness" / run_id).resolve()
        output.mkdir(parents=True, exist_ok=False)
        patches = {arm: output / f"{arm}.patch" for arm in refs}
        for arm, data in source_patches.items():
            if data is not None:
                patches[arm].write_bytes(data)
        measurement_patch = output / "measurement.patch"
        if measurement_bytes is not None:
            measurement_patch.write_bytes(measurement_bytes)
        (root / "target").mkdir(exist_ok=True)
        scratch = Path(tempfile.mkdtemp(prefix="latency-harness-", dir=root / "target"))
        env["TMPDIR"] = str(scratch)
        runner = Runner(output, time.monotonic() + args.budget_seconds, env)
        manifest = {"format_version": 1, "status": "running", "refs": refs, "hypothesis": hypothesis,
                    "baseline_patch_sha256": digest(patches["baseline"]) if source_patches["baseline"] is not None else None,
                    "candidate_patch_sha256": digest(patches["candidate"]) if source_patches["candidate"] is not None else None,
                    "measurement_patch_sha256": digest(measurement_patch) if measurement_bytes is not None else None,
                    "runner_sha256": digest(__file__), "host": platform.uname()._asdict(),
                    "logical_cpus": os.cpu_count(), "load_start": os.getloadavg(),
                    "tier": args.tier, "seconds": TIERS[args.tier], "pairs": args.pairs,
                    "mode": args.mode, "destination": args.destination, "rates": rates,
                    "rate": rates[0] if len(rates) == 1 else None, "workload": args.workload,
                    "decoder_profile": args.decoder_profile, "decoder_trials": [],
                    "allocation_profile": args.allocation_profile, "allocation_trials": [],
                    "budget_seconds": args.budget_seconds, "steps": runner.steps, "trials": [],
                    "limits": {"command_log_bytes": 8 << 20, "minimum_free_disk_bytes": 2 << 30},
                    "not_measured": ["live network latency", "per-record consumer visibility",
                                     "collector-only CPU cycles/cache misses",
                                     "dual-source full-pipeline latency", "Codex quota"],
                    "builds": {}}
        if not args.allocation_profile:
            manifest["not_measured"].append("allocations")

        def save():
            temporary = output / "manifest.tmp"
            temporary.write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
            temporary.replace(output / "manifest.json")

        worktrees, builds = [], {}
        build_keys = {arm: (commit, source_patches[arm])
                      for arm, commit in refs.items()}
        save()
        try:
            manifest["toolchain"] = runner.run("toolchain", ["rustc", "-vV"], root)
            for arm, commit in refs.items():
                require(shutil.disk_usage(root).free >= 2 << 30, "insufficient disk headroom")
                key = build_keys[arm]
                if key in builds:
                    manifest["builds"][arm] = manifest["builds"]["baseline"]
                    continue
                worktree = scratch / arm
                worktrees.append(worktree)
                runner.run(f"{arm}-checkout", ["git", "worktree", "add", "--detach", str(worktree), commit], root)
                tree = (apply_candidate_patch(worktree, patches[arm]) if key[1] is not None
                        else git(worktree, "rev-parse", "HEAD^{tree}"))
                if measurement_bytes is not None:
                    tree = apply_candidate_patch(worktree, measurement_patch, measurement=True)
                source_status = git(worktree, "status", "--porcelain")
                commands = [["cargo", "test", "--locked", "--release", "--lib",
                             "--test", "decoder", "--test", "book_replay", "--test", "book_lifecycle", "--test", "recording"]]
                if args.tier == "integration":
                    commands[0] += ["--test", "websocket", "--test", "collector"]
                elif args.tier == "qualification":
                    commands = [["cargo", "fmt", "--all", "--", "--check"],
                                ["cargo", "check", "--locked", "--all-targets"],
                                ["cargo", "test", "--locked"], ["cargo", "test", "--locked", "--release"],
                                ["cargo", "clippy", "--locked", "--all-targets", "--", "-D", "warnings"]]
                for index, command in enumerate(commands):
                    runner.run(f"{arm}-correctness-{index}", command, worktree)
                    save()
                targets = ["recording_load"] + (["decoder"] if args.decoder_profile else [])
                command = ["cargo", "test", "--locked", "--release", "--no-run", "--message-format=json"]
                for target in targets:
                    command += ["--test", target]
                log = runner.run(f"{arm}-build", command, worktree)
                artifacts = [json.loads(line) for line in log.splitlines() if line.startswith("{")]
                binaries = {}
                for target in targets:
                    matches = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact"
                               and a.get("target", {}).get("name") == target and a.get("executable")]
                    require(len(matches) == 1, f"expected one {target} test executable")
                    binaries[target] = scratch / f"{arm}-{target}"
                    shutil.copy2(matches[0], binaries[target])
                require(git(worktree, "status", "--porcelain") == source_status
                        and not git(worktree, "diff", "--name-only")
                        and git(worktree, "write-tree") == tree,
                        "build/check modified its source worktree")
                builds[key] = (worktree, binaries)
                manifest["builds"][arm] = {"executable_sha256": digest(binaries["recording_load"]),
                    "decoder_executable_sha256": digest(binaries["decoder"]) if args.decoder_profile else None,
                    "decoder_benchmark_sha256": digest(worktree / "tests/decoder.rs"),
                    "source_tree": tree,
                    "benchmark_sha256": digest(worktree / "tests/recording_load.rs"),
                    "lock_sha256": digest(worktree / "Cargo.lock")}
                save()
            # All compilation/checking finishes before any timed run. Alternate
            # AB then BA to expose order effects; retain individual pairs, not pooled percentiles.
            same = manifest["builds"]["baseline"]["source_tree"] == manifest["builds"]["candidate"]["source_tree"]
            if args.decoder_profile:
                for pair in range(args.pairs):
                    for arm in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                        worktree, binaries = builds[build_keys[arm]]
                        text = runner.run(f"decoder-{pair}-{arm}", [str(binaries["decoder"]),
                            DECODER_TEST, "--exact", "--ignored", "--nocapture"], worktree)
                        require("test result: ok. 1 passed; 0 failed" in text, "decoder diagnostic did not pass")
                        reports = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
                        validate_decoder_profile(reports)
                        manifest["decoder_trials"].append({"arm": arm, "pair": pair, "results": reports})
                        save()
            for pair, rate in ((pair, rate) for pair in range(args.pairs)
                               for rate in (rates if pair % 2 == 0 else list(reversed(rates)))):
                for arm in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                    require(shutil.disk_usage(root).free >= 2 << 30, "insufficient disk headroom")
                    worktree, binaries = builds[build_keys[arm]]
                    name = f"pair-{pair}-{arm}" + (f"-rate-{rate}" if len(rates) > 1 else "")
                    metrics = output / f"{name}.resources.json"
                    text = runner.run(name, ["/usr/bin/time", "-f",
                        '{"user_seconds":%U,"system_seconds":%S,"max_rss_kib":%M,"wall_seconds":%e}',
                        "-o", str(metrics), str(binaries["recording_load"]), TEST, "--exact", "--ignored", "--nocapture"],
                        worktree, {"C5_SECONDS": str(TIERS[args.tier]), "C5_RATE": str(rate),
                                   "C5_MODE": args.mode, "C5_OUTPUT": args.destination,
                                   "C5_WORKLOAD": args.workload})
                    reports = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
                    require(len(reports) == 1 and "test result: ok. 1 passed; 0 failed" in text,
                            "missing benchmark success/report; zero tests is not success")
                    validate_result(reports[0], TIERS[args.tier], rate, args.mode, args.destination, args.workload)
                    manifest["trials"].append({"arm": arm, "pair": pair, "result": reports[0],
                                               "resources": json.loads(metrics.read_text())})
                    save()
            if args.allocation_profile:
                # Fixed small input, after all latency timing. Instrumentation
                # changes timing; its samples must never join the A/B trials.
                for arm in ("baseline", "candidate"):
                    require(shutil.disk_usage(root).free >= 2 << 30, "insufficient disk headroom")
                    worktree, binaries = builds[build_keys[arm]]
                    prefix = output / f"allocations-{arm}"
                    text = runner.run(f"allocations-{arm}", ["heaptrack", "-o", str(prefix),
                        str(binaries["recording_load"]), TEST, "--exact", "--ignored", "--nocapture"],
                        worktree, {"C5_SECONDS": "1", "C5_RATE": "1000", "C5_MODE": "ingress",
                                   "C5_OUTPUT": "file", "C5_WORKLOAD": args.workload})
                    reports = [json.loads(line) for line in text.splitlines() if line.startswith("{")]
                    require(len(reports) == 1 and "test result: ok. 1 passed; 0 failed" in text,
                            "allocation replay did not pass")
                    validate_result(reports[0], 1, 1000, "ingress", "file", args.workload)
                    profiles = [p for suffix in (".zst", ".gz") if (p := Path(str(prefix) + suffix)).is_file()]
                    require(len(profiles) == 1, "missing or ambiguous heaptrack profile")
                    stacks = output / f"allocations-{arm}.stacks"
                    runner.run(f"allocations-{arm}-export", ["heaptrack_print", "-f", str(profiles[0]),
                        "--filter-bt-function", "Processor::process", "--flamegraph-cost-type", "allocations",
                        "-F", str(stacks), "-p", "0", "-a", "0", "-T", "0"], worktree)
                    require(stacks.stat().st_size <= 8 << 20, "allocation stack budget exhausted")
                    calls = count_processor_allocations(stacks.read_text())
                    manifest["allocation_trials"].append({"arm": arm, "result": reports[0],
                        "processor_allocation_calls": calls, "profile": profiles[0].name, "stacks": stacks.name,
                        "boundary": "Processor-containing stacks; includes initialization, warmup and callbacks; instrumented timings are not latency evidence"})
                    save()
            comparisons = {str(rate): compare([t for t in manifest["trials"]
                           if t["result"]["offered_rate"] == rate], same) for rate in rates}
            manifest["comparison"] = comparisons[str(rates[0])] if len(rates) == 1 else {
                "decision": "load_curve_screen_only", "by_rate": comparisons,
                "inference": "Finite load points, not maximum sustainable capacity or automatic promotion."}
            manifest["status"] = "completed"
        except BaseException as error:
            manifest["status"] = "incomplete"
            manifest["error"] = str(error) or type(error).__name__
        finally:
            cleanup_errors = []
            for worktree in worktrees:
                try:
                    git(root, "worktree", "remove", "--force", str(worktree))
                except (subprocess.SubprocessError, OSError) as error:
                    cleanup_errors.append(str(error))
            if cleanup_errors:
                manifest["status"] = "incomplete"
                manifest["cleanup_errors"] = cleanup_errors
                manifest["retained_scratch"] = str(scratch)
            else:
                shutil.rmtree(scratch)
            manifest["load_end"] = os.getloadavg()
            save()
        print(json.dumps({"status": manifest["status"], "manifest": str(output / "manifest.json"),
                          "comparison": manifest.get("comparison"), "error": manifest.get("error")}), flush=True)
        return 0 if manifest["status"] == "completed" else 1


if __name__ == "__main__":
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt("campaign interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except (Failure, OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        print(f"Harness stopped: {error}", file=sys.stderr)
        sys.exit(1)
