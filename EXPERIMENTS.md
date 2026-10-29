# Bounded latency experiment harness

`tools/latency_harness.py` orchestrates the existing `recording_load` oracle;
it does not replace the benchmark, generate hypotheses, invoke models or edit
production code. Research decisions live in `RESEARCH.md` and `HYPOTHESES.md`.

## Frozen baseline after E21

The user paused further optimization after E21. Keep the current tested working
tree on `harness-latency`, including H15/H08/H05; do not integrate H17 or start
the proposed H18/H19 experiments. Earlier "next experiment" sections are history
and backlog, not active instructions. This checkpoint does not resolve the
documented pipeline latency regressions or establish production qualification.

Local checkpoint: `.amp/in/artifacts/frozen-e21/`. `source.tar.gz` preserves
repository source, tests, harness tools and ledgers (including uncommitted files).
`experiments.tar.gz` preserves the existing harness evidence separately.
`manifest.json` records the Git base, working-tree status, file hashes, archive
hashes and verification; `working-tree.patch` also records tracked changes.
Build caches, Git credentials and Python bytecode are excluded. The checkpoint
is local to this orb, not a commit, remote backup, merge or deployment.

## Run contract

- Linux, Python 3.11+, GNU `time`, the repository Rust toolchain and cached locked
  dependencies. Cargo runs offline. No ignored live probes are selected.
- Explicit baseline/candidate **commits**, copied into owned detached worktrees.
  Optional `--baseline-patch` / `--candidate-patch` admit explicitly supplied
  Rust-source-only experiments without committing them. Optional
  `--measurement-patch` applies the same test-only changes to both arms.
  All patches are retained and hashed;
  source tree IDs are recorded. Other uncommitted work is excluded. The caller's
  worktree/branch remains untouched.
- A/B requires a researched hypothesis record. Identical-tree A/A is calibration.
- Manifest, lockfile, Cargo/toolchain config, supplied helpers/schema and all
  integration tests/fixtures must match. Measurement patches may change Rust test
  files, not fixtures, supplied helpers or implementation. Candidate patches may
  change implementation, not tests/helpers/configuration. Both arms use exactly
  the same measurement patch; do not compare changed oracles as a speedup.
- All builds/checks precede timing. Copied executables cannot be replaced by a
  later Cargo build. One shared advisory lock excludes cooperating builds/runs.
  It cannot prevent unrelated host activity; start/end load is recorded.
- Alternating AB/BA pairs; no simultaneous benchmarks. No automatic escalation,
  retries, infinite loop or accepted optimization. Each invocation is one bounded
  campaign, restartable from its exact refs and settings—not from a partial pair.
- Default three pairs, 1,000 messages/s, 100 books, file output, 900-second total
  budget including compilation. Rate cap 5,000/s; pair cap five. Every trial has
  the benchmark's two-second warmup and its independent full-record comparison.
- `--rates 1000 3000 5000` compares up to three bounded load points using the same
  copied builds. Rate order reverses between pairs; AB/BA order still alternates.
  `--decoder-profile` also runs the identical public-API decoder diagnostic against
  both release libraries, with exact case/count validation and preserved raw data.
- `--allocation-profile` requires heaptrack and heaptrack_print. After timing it
  captures one measured second plus two warmup seconds at 1k/s, ingress/file,
  per arm. Filtered Processor allocation calls include initialization/callbacks.
  Instrumented latency is not benchmark latency; raw profiles remain separate.
- SIGINT/SIGTERM, timeout, failing correctness/oracle, output failure, malformed
  measurements, log exhaustion or incomplete pairs stop the campaign nonzero.
  Raw logs and incremental manifest remain; incomplete data is never a win.
- Each command log is capped at 8MiB. Require 2GiB free disk before each build/run.
  This is a headroom check, not a filesystem quota or process RAM limit. Preserve
  dependency build cache; remove owned worktrees/binaries/temp archives on exit.
  Uninterruptible OS I/O/SIGKILL/host loss can prevent final cleanup; inspect an
  unfinished manifest rather than assuming success. Cleanup may outlast the budget.
- No network market-data work, key generation, unsafe Rust, Git commit/push/merge
  or Codex allowance monitoring. Pure test execution does not invoke an LLM.

| Tier | Measured seconds per arm | Correctness scope |
|---|---:|---|
| `smoke` | 5 | Release library, decoder, book replay/lifecycle and recording |
| `integration` | 20 | Smoke plus local WebSocket and executable collector tests |
| `qualification` | 120 | Full debug/release suites, fmt, check, strict Clippy; selected recording workload |

Qualification is still **not** full-universe/live acceptance or all workload
combinations. Run both transport modes and relevant destinations separately.
Microbenchmarks and dual-source/health/visibility profiling remain separate
existing test targets; the runner does not pretend the one-source recording trace
measures those paths. Add only measurements a researched hypothesis needs.

## Commands

Unchanged-code calibration (no production candidate):

```sh
python3 tools/latency_harness.py --baseline HEAD --candidate HEAD \
  --tier smoke --pairs 3 --budget-seconds 600
```

Use different existing commit refs and `--hypothesis reviewed.json` for A/B.
Choose `--mode socket` for localhost WebSockets and `--destination pipe` or
`throttled` for existing output controls. `--output NEW_DIRECTORY` rejects an
existing path; otherwise results go under `.amp/in/artifacts/harness/`.
`--workload snapshot-burst` selects eight replacement snapshots every twenty
messages, rotating over 100 books with 128–383 bid levels plus one ask. It needs
the shared measurement patch until that benchmark version is committed. Default
`mixed` preserves the earlier workload exactly. Ambient `C5_*` settings are
removed; the runner sets every supported workload option explicitly.
For a deliberately longer unattended campaign, use the orb's supervised service
launcher with this same bounded command, not an unbounded shell loop. The agent
does not start a recurring schedule or paid research agents implicitly.

Harness tests (no Cargo or network):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools -p 'test_latency_harness.py'
```

## Interpretation

`manifest.json` records exact refs, runner/binary/fixture/lock digests, toolchain,
host, commands, CPU/RSS, individual reports, configuration and the comparison.
RSS/CPU include generator/oracle/readers, not just collector code. Reported
throughput is **achieved offered rate**, not maximum sustainable capacity. Since
E14, the oracle runs after drain, but timing/reference-metadata instrumentation
still perturbs processing. Physical write/flush calls are not
consumer-visible record latency. Do not add/subtract independently computed
percentiles; small p99.9 samples are exploratory.

The tail-latency screen uses processing-including-recorder p99. Its result is
only `promising_requires_qualification` when at least three complete pairs all
improve, median improvement is at least 5%, and every pair stays within 5% of
baseline queue p99, processing p99.9 and offer-plus-drain duration, and 10% of
baseline process peak RSS. These are predeclared screening tolerances, **not
statistical confidence**, business latency SLAs or automatic promotion. A/A is
always `control_only`. Keep individual runs; never cherry-pick or pool percentiles.
This is one dimension, not an overall accept/reject decision. Computational
efficiency and capacity need their own evidence. Keep unresolved regression flags
alongside promising mechanisms. Five chronological offered-count queue-age windows
help expose drift; they do not measure queue occupancy or prove sustainable load.

## E00 — initial A/A calibration

Baseline/candidate: `015703231785b0ae93fb1be0bb36ff4c14382a2c`.
Run: `--tier smoke --pairs 3 --budget-seconds 600 --output
.amp/in/artifacts/harness/initial-aa`. **Completed: control_only.** The earlier
120s trials are historical evidence in `docs/revision-check.md`, not comparable
speedup baselines for 5s runs.

All 96 selected release correctness tests passed. All six replay runs matched
49,105 SBE records each, with 5,000 measured messages per run. Both arms used
the identical copied executable (SHA-256
`c8a7d20b6a8d56d1825ec3eb1844e064825ef285875c01548f27f4c464ba137a`).
Measured command time including build/checks was 67.03s. Each replay took about
7.16s including warmup and validation. No other builds/benchmarks or subagents
ran concurrently; short source inspection and documentation work continued.

| Pair / order | Baseline processing p99 (µs) | Other label p99 (µs) | Apparent improvement |
|---|---:|---:|---:|
| 0 / AB | 46.835 | 50.319 | −7.44% |
| 1 / BA | 47.551 | 45.178 | +4.99% |
| 2 / AB | 48.985 | 40.277 | +17.78% |

**Finding:** unchanged code can appear nearly 18% faster in one short pair.
This disproves using a single small run as optimization evidence. It does not
measure a confidence interval or justify choosing 18% as a universal threshold.
No production change was made or promoted. All source-tree worktrees were removed.

Harness validation: seven Python tests passed, including failed commands, bounded
logs, invalid/missing metrics, pairing, incomplete or regressed candidates and
deadline cancellation of a descendant after its parent exits. CLI checks reject
a differing source tree without research evidence and refuse output overwrite.
An actual one-second campaign exited nonzero with `status: incomplete` and
`campaign time budget exhausted`; no worktree remained. Its evidence is retained
at `.amp/in/artifacts/harness/budget-stop/manifest.json`.

This established the smoke end-to-end path. The subsequent E02 campaigns below
exercise patched candidates, the 20s integration tier and socket input. The 120s
qualification tier remains separate; earlier collector tests are not runner proof.

## E01 — decoder cost attribution

Executed `cargo test --offline --locked --release --test decoder --
--include-ignored --nocapture`. Seven tests passed, including the explicit
profiling test. Before timing, the decoder matched all 66 independently generated
retained-fixture records exactly. Synthetic asymmetric snapshots independently
checked each price and quantity. Every stage used 10,000 samples after 1,000
warmup operations; black-box outputs were dropped inside the timed span.

| Case | Envelope median | Decode prevalidated median | Combined median / p99 |
|---|---:|---:|---:|
| Retained two-entry deltas | 0.493 µs | 1.152 µs | 1.588 / 2.145 µs |
| Retained four-snapshot array | 33.442 µs | 108.083 µs | 138.639 / 226.314 µs |
| Synthetic 8 bids + 1 ask | 0.568 µs | 1.628 µs | 2.137 / 4.035 µs |
| Synthetic 256 bids + 1 ask | 11.323 µs | 35.189 µs | 47.817 / 74.180 µs |
| Synthetic 2048 bids + 1 ask | 89.009 µs | 282.055 µs | 369.657 / 588.029 µs |

Empty clock/control median/p99: 32/34 ns. This is **hot-cache isolated cost**,
not exchange latency, end-to-end latency, or a decomposition of earlier pipeline
percentiles. No allocator attribution yet. The results favor investigating bursts
before a parser rewrite; they do not prove deltas never matter. Raw output:
`.amp/in/artifacts/harness/e01/baseline.log`.

## E02 — validated bulk snapshot construction, parked

The Rust standard library's bulk BTreeMap builder is applicable only after
explicit duplicate validation. The isolated candidate sorts the owned levels,
rejects equal prices **before** filtering zeros, and collects into the same book
representation. It preserves state/output contracts; it does not change deltas.

An executed temporary unit experiment compared insertion against the candidate
in three alternating pairs, 2,000 samples each after 200 warmups, at depths
8/64/256/2048 with descending and permuted prices. Both checked independently
specified bid/ask output and duplicate-zero rejection. Input clones were outside
the timed interval; construction and destruction were inside.

| Depth/order | Insert median (ns) | Bulk median (ns) |
|---|---:|---:|
| 8 / descending | 211 | 143 |
| 64 / descending | 1,751 | 442 |
| 256 / descending | 7,982 | 1,837 |
| 256 / permuted | 8,477 | 3,563 |
| 2048 / descending | 76,264 | 16,292 |
| 2048 / permuted | 147,708 | 31,599 |

Values are medians of the three run medians, not pooled samples. Raw log and
reproducible temporary experiment patch are preserved under
`.amp/in/artifacts/harness/e02/`; the temporary duplicate implementation was
removed from production source. This is sufficient to **test**, not promote.

Predeclared pipeline experiment: three 20-second AB/BA pairs, 100 books,
1,000 messages/s, snapshot-burst primary and mixed regression control, independent
full-record SBE replay, unchanged five-percent tail/queue/drain and ten-percent
RSS screening guards. Small-workload, local-only evidence. Correctness tests and
measurement changes are identical on baseline and candidate. No commits required;
the hypothesis and exact patches are preserved with each campaign.

### Initial pipeline results: burst gain, mixed control gate not satisfied

Each arm passed 114 release correctness tests before timing. Each burst run
matched **155,191** independently expected SBE records; all runs offered 20,000
measured messages after two seconds of warmup. Same baseline commit and exact
test patch on both arms, no concurrent builds or other benchmarks during timing.

| Mode/workload | Pair | Baseline p99 (µs) | Candidate p99 (µs) | Improvement | All regression guards |
|---|---:|---:|---:|---:|---|
| Ingress / burst | 0 | 637.851 | 518.806 | +18.66% | pass |
| Ingress / burst | 1 | 647.339 | 543.583 | +16.03% | pass |
| Ingress / burst | 2 | 651.822 | 536.269 | +17.73% | pass |
| Socket / burst | 0 | 627.467 | 524.856 | +16.35% | pass |
| Socket / burst | 1 | 685.425 | 548.237 | +20.02% | pass |
| Socket / burst | 2 | 699.770 | 549.517 | +21.47% | pass |
| Ingress / mixed | 0 | 36.407 | 35.213 | +3.28% | pass |
| Ingress / mixed | 1 | 32.200 | 35.013 | −8.74% | **fail** |
| Ingress / mixed | 2 | 32.848 | 34.072 | −3.73% | pass |

Mixed pair 1 queue p99 also increased from 104.050 to 110.720µs, exceeding the
unchanged 5% guard. Its p99.9, drain and RSS guards passed. This is **not a clean
promotion**. The subsequent matched-duration mixed A/A calibration produced
apparent p99 improvements of +8.45%, −11.88% and −15.66% on identical binaries.
That establishes substantial noise in these 20s trials, but cannot retroactively
erase the failed gate. The earlier 5s calibration alone was insufficient.

Evidence manifests: `.amp/in/artifacts/harness/e02/{burst,mixed-control,socket-burst}/manifest.json`.
The source patch is 23 changed lines in `src/orderbook/book.rs`, not a new book
representation, parser or queue. Full fmt/check/debug/release/strict Clippy passed
on this candidate (160 tests in each profile; ignored live/performance tests
excluded), recorded in `e02/full-checks.log`. The candidate is **parked**, and
`book.rs` was restored to the unchanged baseline before E03. Its exact patch
remains in `e02/candidate.patch`; it is not stacked into later experiments. No
live-exchange latency, full-universe capacity, consumer visibility or statistical
confidence is established by these results.

## E03 — reuse existing binding identity, parked

`Processor::resolve` currently hashes every accepted asset in both the adapter
registry and the session request map. H05 uses the existing stable BookId in a
requested binding, retaining namespace-specific registry validation on misses
and all current binding/state checks. No interner, new cache or unsafe indexing.
The isolated experiment measured amortized short-ID lookup cost of 58 → 27ns,
and synthetic 78-character IDs of 133 → 51ns. These are batch-derived costs,
not per-call latency percentiles or a pipeline improvement claim.

The public identity-scope regression passed on the baseline before editing:
leading zeros, overlapping adapter namespaces, known-unrequested versus unknown
input, and preservation of unaffected books. E03 applies that same test patch to
both arms. Its measurement-only change moves synthetic JSON generation before
the simulated receive timestamp and offer-lag observation. A one-second smoke
matched 2,207 independent expected records; this fixes instrumentation, not
production latency. E02 and E03 stage values therefore are not interchangeable.

Predeclared screening remains unchanged: three alternating 20s pairs at 100 books
and 1,000 messages/s, native-width IDs primary and short-ID mixed control, exact
full-record replay, ≥5% median processing-p99 improvement with every pair improved,
and unchanged tail/queue/drain/RSS guards. Both completed campaigns failed:

| Workload | Pair 0 p99 gain | Pair 1 p99 gain | Pair 2 p99 gain | Guards |
|---|---:|---:|---:|---|
| Native-width IDs | +4.43% | +15.16% | −19.34% | pass / pass / fail |
| Mixed short IDs | +4.69% | −20.24% | +8.10% | pass / fail / pass |

Every run matched 154,100 independent expected records. Each arm passed 115
release correctness tests (one explicitly ignored profiling test). The native-ID
pair 2 queue p99 rose 132.407 → 150.992µs; mixed pair 1 rose 120.600 → 136.255µs.
The exact lookup change is parked and removed from production source; isolated
nanosecond savings do not establish pipeline improvement. Evidence under `e03/`.

Latest short-ID processing percentiles, median of three run percentiles (µs):

| Percentile | Baseline | Candidate |
|---|---:|---:|
| p50 | 10.416 | 10.505 |
| p95 | 27.860 | 26.972 |
| p99 | 37.225 | 36.600 |
| p99.9 | 67.041 | 67.132 |

Do not use a favorable median column to conceal the failed paired screen.
These are processing including encoding, not exchange/network/durable visibility.

## E04 — single-lookup numeric delta preparation, parked

H06 replaces provisional quantities `get` plus `insert` with an occupied/vacant
Entry held through the existing arithmetic and budget checks. The previous book
fallback, intermediate zero, ordered prepared entries and later commit loop are
unchanged. This is independent of the parked E02/E03 changes.

Executed an isolated synthetic preparation-map fragment with matching key/hash
representations, shared RandomState and retained capacities, three alternating
pairs of 10,000 measured samples after 1,000 warmups. Independently checked
`7 → 0 → 3`, intermediate negative rejection and overflow; timing includes clear,
map operations, checked arithmetic and prepared-vector append, but excludes
actual book fallback, parsing, budget checks and output. Median of run medians:

| Entries | Distinct keys, get+insert → Entry | Two repeating keys, get+insert → Entry |
|---|---:|---:|
| 2 | 96 → 78ns | 108 → 84ns |
| 16 | 725 → 442ns | 767 → 423ns |
| 64 | 2,597 → 1,532ns | 2,678 → 1,481ns |

At length two the two-key pattern has no repeated key yet; it is not a separate
collision stress case. This is isolated mechanism evidence, not Processor latency.
The candidate then passed fmt plus 91 focused release correctness tests. Pipeline
screening uses the unchanged E03 measurement patch and thresholds, three 20s
mixed pairs followed by native-width IDs. Source, logs and patches: `e04/`.

Both completed campaigns failed screening; each run still matched 154,100
independent expected records and each arm passed 115 release correctness tests.

| Workload | Pair 0 p99 gain | Pair 1 p99 gain | Pair 2 p99 gain | Guards |
|---|---:|---:|---:|---|
| Mixed short IDs | +17.88% | −1.61% | −5.20% | pass / fail / fail |
| Native-width IDs | −18.61% | −0.69% | +11.21% | fail / pass / pass |

The candidate was removed from production source and its patch preserved. Current
production code is unchanged from the starting baseline. Latest mixed processing
percentiles (median of run percentiles, µs): baseline p50/p95/p99/p99.9 =
10.781/28.509/40.776/80.379; candidate = 11.357/28.605/41.705/86.210. No gain claimed.

## E05 — fuse decimal validation and accumulation, deferred after isolation

Source-backed mechanism: the locked serde_json integer parser classifies digits
while accumulating. A temporary candidate adopts only this traversal, retaining
checked fixed-point arithmetic, leading zeros and exact excess-zero precision.
It does not adopt JSON exponents or floating-point overflow. Invalid input still
rejects, but diagnostic precedence can differ for multiply-invalid strings.

The real public parser and temporary candidate passed independently specified
valid values, overflow/precision/grammar cases, and every Latin-1 character in
whole and fractional positions. Three alternating pairs, 10,000 batches of 64
calls after 1,000 warmup batches; opaque function pointers and inputs, expected
sums checked outside timing. Amortized median ns/call, not per-call percentiles:

| Input | Current | Fused |
|---|---:|---:|
| `0` | 4.97 | 4.34 |
| `0.37` | 8.41 | 7.02 |
| `0001.000000` | 20.66 | 13.91 |
| `6389.012345` | 20.64 | 13.83 |
| `0.00000100000` | 19.48 | 14.08 |
| `9223372036854.775807` | 33.39 | 23.28 |

The savings are too small to justify another pipeline campaign at present: even
32 values × 10ns is only about 0.32µs, versus mixed p99 around 40µs. That is a
scale estimate, not a measured pipeline effect. No production decimal edit was
made; the temporary test was removed, retaining source/log under `e05/`.

## E06 — sample the unchanged pipeline before another optimization

Three small mechanisms have not established robust pipeline tail improvement.
Use Linux perf CPU-clock user-space sampling to locate larger costs, keeping
oracle/generator/output activity distinct from Processor stacks. This is an
instrumented diagnostic, not a candidate benchmark or a latency acceptance run.
The optional orb profiling package was installed; software-counter access worked.
The setup change is local, not active in the remote project's default branch;
two warm setup runs took 0.75s and 0.37s. Full current-tree checks precede profiles.

Full fmt/check/debug/release/strict Clippy passed, with 161 Rust tests per profile
and nine Python harness tests. No ignored live campaign ran. Short profiles then
matched 49,105 mixed and 49,441 burst records. Zero samples were reported lost.
Among 636 burst CPU samples, 384 had the synthetic payload generator in the stack;
121 had Processor. This justified removing temporary JSON DOM construction from
the **test generator**, not altering collector validation or serialization.

The direct ASCII/numeric generator produced byte-identical output across 4,016
messages and all four depth/identity combinations: 13,550,839 length-framed bytes,
SHA-256 `ffbefeb3a389a6ab6150006e18ae998e7fe5860f052cff8b86eee97ec3b1dc3f`.
The temporary capture test/files were removed. The independent action/SBE oracle
is unchanged. Afterward, short profiles matched the same 49,105/49,441 records;
a socket/native-ID smoke matched 2,207. Format, targeted debug test and all-target
strict Clippy passed again after this measurement-only edit.

Burst profiles changed from 384/636 generator-containing samples to 57/285;
Processor-containing samples were 121 in both. Mixed changed from 90/214 to
24/156 generator-containing samples. Small, instrumented, whole-run profiles
include initialization/warmup/output and potentially truncated call stacks. They
support the generator diagnosis but are not precise CPU shares, allocation counts
or collector latency improvements. Original and updated profiles/logs remain in
`e06/`. A fresh three-pair 20s identical-binary calibration completed: apparent
p99 gains +7.81%, −10.07%, +2.21%, guards fail/pass/fail, correctly labeled
`control_only`. Generator interference was reduced, but short-run noise remains;
never compare earlier candidates across the changed measurement baseline.

## E07 — optional-text borrowing, allocation evidence and pipeline evaluation

Heaptrack identified a real discrepancy with the earlier code audit: direct Cow
borrowing does not extend through Option in the locked Serde derive. The local
transparent-wrapper candidate changes only optional timestamp/asset parsing.
It preserves escaped owned strings, missing/default/null handling and exact values.
The new public borrowing regression failed on the baseline and passed afterward;
98 focused release correctness tests passed (one profiling test ignored).

One-second measured traces plus two-second warmup, 100 books/1,000 msg/s, same
replay oracle and compiler flags. Allocation calls with Processor in their stack:

| Workload | Baseline calls | Candidate calls | Baseline string-deserialization calls | Candidate string calls |
|---|---:|---:|---:|---:|
| Mixed | 26,570 | 23,355 | 3,215 | 0 |
| Snapshot burst | 142,948 | 137,498 | 5,450 | 0 |

Counts include initialization/callbacks, not just financial deltas; do not confuse
the whole-process footer with filtered counts. Exported collapsed stacks preserve
all sites, rather than inferring zero from absence in a top-N table. Heaptrack
changes timing; no instrumented latency is an acceptance result. Exit-time retained
allocations are not automatically application leaks. Raw profiles and counts live
under `e07/`; no Rust dependency or production allocator hook was added.

Paired mixed/native-ID pipeline tests retain the unchanged screening thresholds
and the identical E06 test patch on both arms. The new borrowing-only assertion
is separately validated, not applied to a baseline that intentionally allocates.
No allocation reduction is presented as an established pipeline latency gain.

Both three-pair 20-second screens completed with exact replay of 154,100 records
per arm/run. Mixed p99 gains were +10.22%, −10.61%, −0.84%, with guards
pass/fail/fail; native-ID gains were +15.21%, +3.12%, −0.94%, with guards
pass/pass/fail. Both decisions are `inconclusive_or_regression`. H08 is parked,
not promoted: its source change and borrowing-only assertion were removed from
the working tree. Candidate/measurement patches, profiles, correctness logs and
manifests remain under `e07/` for reproduction.

Latest baseline median run percentiles (microseconds, not pooled quantiles):

| Workload | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| Mixed | 11.359 | 28.601 | 39.461 | 67.543 |
| Native-width IDs | 9.863 | 30.093 | 43.431 | 78.180 |

These measure `Processor::process` including recorder callbacks at 100 books,
1,000 messages/second, two-second warmup, synthetic ingress and file output.
They exclude exchange/network latency, oracle work before processing, consumer
visibility, and automatic policy tick/maintenance. Short runs do not establish
reliable extreme-tail behavior or a production latency SLA.

## E08 — assess the proposed lookup and inactive-health directions

The retained real V1 trace has 31 price-change events, each with two entries:
62 entries and zero adjacent repetitions of the same asset. A consecutive-asset
cache cannot save a resolution in that trace. This does not rule out other live
batch shapes; it does rule out claiming applicability from synthetic repeated
entries alone. E03 already tested the distinct second-lookup elimination.

Preparation vectors/maps already retain capacity via clear/drain. Decoder lists
still allocate; changing their lifetime contract is separate work, not merely
adding `clear()`. Existing health scheduling skips inactive replicas and stops
rescheduling retired books. The recording workload calls capture/publish, but not
health tick or automatic maintenance, so its previous percentiles cannot answer
the health-overhead question. Measure that boundary separately without quietly
changing the baseline or skipping quiet-but-live books.

Executed a temporary public-API lifecycle diagnostic with 100 books, two replicas,
automatic selection/monitoring, and three 600-second logical-clock phases (no
real-time waits). After a bounded retirement-settling sweep, health checks were
6,000 with all books desired, 3,000 with half retired, and **zero** with all retired.
Quiet desired replicas stayed Live with quantity 7,000,000; retired replicas were
Inactive; no quiet-peer recovery occurred. The test passed and completed in 0.01s
after compilation. Its source and output are preserved in `e08/`; the temporary
test was removed from the suite.

With 100 desired books, the 120 due-tick calls had p50 16.378µs/p99 25.828µs,
versus about 0.103µs median for no-work ticks. Due ticks check batches, not one
book each. This is a tiny hot-cache scheduling diagnostic, not traffic-interleaved
pipeline latency or a large-universe qualification. It confirms the existing
retirement optimization; it does not justify skipping quiet live books or rewriting
the scheduler. Decoder-buffer ownership remains the more relevant investigation.

## E09 — isolate bounded decoder-list storage reuse

Research basis: the locked Serde `VecInPlaceVisitor` retains capacity, and our E07
profiles identify bounded-list vector growth. The experiment uses the same bounded
Serde visitor in both arms, with either a fresh vector or a retained cleared vector.
Exact decimal types, list limits, trailing-input rejection and malformed-input reset
checks remain. Snapshot depths 8/256/2048 and change counts 2/16 separate the real
trace's common two-entry shape from larger synthetic cases. Three alternating pairs
measure hot-cache parse/drop mean cost; no timer per parsed item, network or output.

This is an applicability screen, not a production candidate. Changing `Event`
ownership requires more than a local scratch vector. Before paying that complexity,
look for consistent savings of at least 10% and 0.5µs per representative list;
smaller savings would be hard to distinguish in the current noisy pipeline screen.
Never infer a pipeline gain from this isolated cost, reuse references across freed
messages, or claim allocator-stall tails from average parser timings.

The release diagnostic passed in 2.64s. Two-entry changes saved 21–52ns/list;
eight-level snapshots saved 29–60ns. Sixteen-entry changes ranged from a 30ns
regression to a 224ns saving; 256-level results were mixed and all three 2048-level
pairs regressed by 1.1–2.5%. None met the predeclared applicability screen. Do not
rewrite the decoder ownership interface for these results. This also does not
prove reuse never helps under allocator contention; that workload was not tested.
Source/logs remain under `e09/`; the temporary test was removed. No production
code changed. Next measurement work should distinguish input shapes: aggregate
p99 can hide rare snapshots and mix single-entry updates with 16-entry batches.

## E10 — input-shape attribution, unchanged collector

The same measured owner spans are now partitioned by the offered trace into one-,
four-, and sixteen-entry changes and eight-snapshot replacement arrays. No new
per-message clocks or production hooks were added. Classification happens outside
owner timing; additional sample storage can still perturb queueing, so this is a
new shared measurement patch, not a collector improvement. Python validation
independently checks each class's expected count, not just their sum.

Ten harness tests, all-target strict Clippy and a three-pair five-second identical-
binary calibration passed. Each calibration run replayed 49,105 exact records.
A socket/native-ID one-second smoke replayed 317 records and validated empty-class
null quantiles. A five-second burst run replayed 49,441 records with 250 replacement
arrays. No exchange, full-universe or reliable extreme-tail claim follows.

Mixed A/A median run quantiles, microseconds (six runs, not pooled samples):

| Message shape | Samples/run | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| One change | 1,667 | 4.438 | 12.613 | 17.780 |
| Four changes | 1,665 | 7.521 | 16.922 | 23.850 |
| Sixteen changes | 1,667 | 17.727 | 32.015 | 48.440 |

The mixed run has only one replacement array: do not interpret its reported
quantiles as a distribution. In the separate burst run, replacement-array p50 was
594.904µs and p99 921.297µs (250 samples, eight snapshots each, exploratory).
The overall A/A p99 apparent gains remained +11.44%, −10.46%, +9.61%. Noise still
prevents treating single small differences as improvements. Primary acceptance
and regression thresholds remain unchanged; per-shape metrics are diagnostic.

This supplies a reason to revisit the previously measured bulk-snapshot candidate
once: E06 removed generator interference and E10 attributes replacement cost.
Reuse the exact E02 source patch, no stacked changes, on this new shared baseline;
retain the earlier failed mixed gate rather than overwriting its history.

The user subsequently requested deeper component-by-component horizontal research,
including non-default challenge-fork branches, before another hypothesis. That
retest is paused: no E11 campaign or new production candidate was started. The
snapshot replacement source audit and compatibility comparison are in RESEARCH.md.

## E11 — researched H02 retest: burst gain, mixed regression

Completed the fork/non-default-branch audit, Rust 1.93 construction/search review,
and unchanged-code retained-snapshot stage attribution before reopening H02.
Reused the exact E02 candidate; no stacked changes. E10's shared measurement patch
was identical across arms. Each workload ran three alternating 20-second pairs,
100 books, 1,000 messages/s, local ingress/file, plus two seconds warmup. Each arm
passed 115 release correctness tests (one explicit ignored test remained ignored)
before timing. Every timed run passed complete supplied-reader output comparison:
155,191 records/run for snapshot bursts; 154,100 for mixed traffic.

| Workload | Baseline p99 µs | Candidate p99 µs | Paired p99 improvements | Regression guards |
|---|---:|---:|---|---|
| Snapshot burst | 662.226 | 533.924 | +19.11%, +23.10%, +19.37% | 3/3 pass |
| Mixed | 38.552 | 44.327 | −10.15%, −14.98%, −14.69% | 0/3 pass |

Table latencies are medians of three run p99s, not pooled samples. Mixed median
p50/p95/p99/p99.9 were 10.487/27.864/38.552/78.017µs for baseline and
10.977/29.440/44.327/85.913µs for candidate. These include decode/book/recorder work,
not live network or consumer visibility. Mixed replacement-array quantiles have
only five samples/run and are not meaningful tail estimates. Burst runs had
1,000 replacement arrays/run. Full manifests retain all samples' summary counts,
per-shape quantiles, resource figures, binary/source hashes and commands.

Decision: **do not promote; H02 stays parked.** A burst-only win does not satisfy
the mixed-workload contract. Do not rerun until a favorable outcome or select a
depth cutoff from these samples. No production files changed. Source trees and
measurement patches matched across campaigns, but rebuilt binary hashes differed
with their distinct worktree build paths; no across-campaign binary identity claim.

Follow-up mechanism check, not a new optimization: bulk tree building and repeated
insertion produce different node layouts, and Rust searches each node linearly.
A deterministic counted-key replay with an independent Vec book oracle checked
all mutations under both construction strategies. Mixed measured changes needed
615,113 → 619,144 get comparisons (+0.66%) and 615,503 → 625,395 mutation comparisons
(+1.61%). Burst changes needed +14.81%/+15.84% respectively despite their faster
overall pipeline. This establishes a real construction/update tradeoff, but does
**not** establish that comparison counts caused the observed 10–15% mixed latency
regression. The probe uses atomic counting and is deliberately not a timing test.
Compiler/layout, cache and scheduling effects remain unattributed.

Evidence: `e11/{snapshot-burst,mixed}/manifest.json`,
`fork-research/snapshot-stages.{patch,log}`, and `e11/snapshot-layout.{rs,log}`.
Temporary instrumentation was removed. Further candidate work needs additional
cost attribution, not a blanket claim that bulk construction is faster.

## E12 — avoid a redundant successful-object scan; not promoted

Profiled the unchanged collector, audited hot-path ownership/types and inspected
locked serde_json source before changing C3. A successful typed object parse
already consumes and validates the full JSON input. The candidate avoided the
earlier RawValue scan/one-element Vec on success, retaining the original array
and error paths. No financial, queue, storage or shared-pointer representation
changed. A baseline contract test passed before implementation. After the change,
10,556 differential truncation/mutation/bounds cases matched the old decode path;
existing public malformed-tail/error-scope/sink-failure checks also passed.

Three isolated retained two-entry pairs improved decode from 1.588–1.816µs to
1.158–1.284µs. Invalid semantic inputs slowed from 2.546–2.761µs to 4.629–4.958µs
because of deliberate fallback. This tradeoff was recorded before pipeline runs.

| Workload | Paired processing-p99 improvements | Guards |
|---|---|---|
| Mixed, 20s/run | +16.67%, +16.65%, +9.08% | 3/3 pass |
| Native IDs, 20s/run | +16.52%, +6.83%, +8.02% | 2/3 pass |
| Native IDs, fixed 120s qualification | −5.09%, +16.97%, +16.18% | 2/3 pass |

Short native pair 1 queue p99 rose 122.635 → 130.577µs (+6.48%). Qualification
pair 0 processing p99 rose 39.295 → 41.295µs and queue p99 137.867 → 153.809µs.
These failures remain failures; do not average them away. Both qualification arms
passed full debug/release suites (161 tests each), fmt, all-target check and strict
Clippy. Each long run replayed 854,312 exact records and 184 periodic snapshots.
Production candidate was removed; `src/` matches HEAD. Its patch, test patch,
hypothesis, diagnostics and manifests are preserved in `e12/`. Public contract
assertions remain useful for future decoder work. No overall speedup claimed.

The independent producer's worst offered lag varied from milliseconds to 106ms,
followed by catch-up bursts. Mean rate/count equality does not establish equal
arrival patterns. This motivated E13; it does not prove pacing caused every
failed E12 result. Don't retest candidates unchanged until one passes by chance.

## E13 — correct benchmark pacing before another candidate

Tokio 1.54.1 `time/sleep.rs:14–15` explicitly documents millisecond granularity,
inappropriate for the benchmark's intended 1ms/200µs intervals. A separate
one-second timer-only diagnostic, three alternating pairs at each rate, found:

| Rate | Tokio gaps below half intended interval | Thread sleep gaps below half interval |
|---|---:|---:|
| 1,000/s | 169–188 / 999 (17–19%) | 0 / 999 in each run |
| 5,000/s | 4,159–4,174 / 4,999 (~83%) | 0 / 4,999 in each run |

This is observed timing, not a universal scheduling guarantee. The synchronous
ingress test producer now runs once on a blocking thread using absolute std sleep
deadlines; no spinning, feedback-driven pacing or dropped late messages. Socket
mode retains async I/O and Tokio timing, explicitly labeled. Both modes report
actual offered-lag and interarrival distributions and short-gap counts; Python
checks sample counts, units and pacing identity. The output oracle is unchanged.
Do not compare collector latencies against pre-E13 runs as an optimization gain.

Initial real-stack smoke: five seconds/1,000 messages/s, 49,105 exact records,
zero short gaps, gap p50 999.460µs, offered-lag p99 187.562µs. Socket/native-ID
one-second/10 messages/s smoke replayed 317 exact records and validated its nine
measured interarrival gaps. Eleven Python tests, Rust format and all-target strict
Clippy passed. Timer-only temporary source was archived then removed. Establish
a fresh identical-binary calibration before judging another production candidate.

The completed calibration (`e13/mixed-aa/manifest.json`) used the exact same
copied binary, three alternating 20-second pairs. Apparent processing-p99 changes
were −47.12%, −3.39%, +28.30%; p99 ranged 31.580–51.566µs across six runs. All six
passed the 154,100-record oracle. Corrected pacing still had 0–22 short gaps per
19,999 measured gaps. Pacing correction therefore did **not** establish stable
measurement or explain every E12 failure. Host inspection found eight virtual
CPUs, KVM/kvm-clock and no reported cgroup throttling; that is not proof of host
contention or its absence. Do not retest H12 until a suitable control is established.

## E14 — remove reference reconstruction from the timed pipeline

The old reference engine rebuilt Vec books, allocated symbols and encoded a
second SBE stream on the owner before each processing span. Its p99 was
26.476–34.990µs in E13. Excluding that work from the processing timer did not
exclude its scheduling, cache or allocator effects. This is a demonstrated
benchmark mismatch with production, not a proven explanation for all variance.

The benchmark now captures only bounded input ordinals/receipt times and periodic
snapshot cutoffs, then reconstructs the independent reference after output drain.
No expected financial values come from production observations. Input continuity,
complete record counts, supplied-reader field equality and order remain mandatory.
Trace capacity is fixed to the input count plus periodic intervals and two
intervals of slack; overflow fails instead of dropping evidence. Both arms get the
same test patch; the runner rejects differing oracle placements. Existing
quantile thresholds are unchanged. No production optimization is being tested.

A deterministic 33-record check covers periodic state before/after deltas, zero
deletion, replacement, exact exchange times and ordered levels. It passed, as did
all 11 Python harness checks. One predeclared identical-binary calibration uses
three alternating 20-second mixed pairs at 100 books/1,000 messages/s. Its results
must be reported even if they remain noisy; no retry-until-pass campaign.

Completed result: all six runs compared 154,100 records exactly; apparent paired
p99 changes were −65.86%, −66.41%, +4.39%. Removing the oracle did **not** stabilize
timing. Median run quantiles (not pooled distributions) were:

| Stage | p50 µs | p95 µs | p99 µs | p99.9 µs | Individual run p99 range µs |
|---|---:|---:|---:|---:|---:|
| Processing including recorder | 11.735 | 27.405 | 34.034 | 71.162 | 32.870–56.064 |
| Ingress queue age | 44.634 | 95.044 | 127.047 | 281.489 | 109.044–167.781 |

These are local synthetic observations, not a collector speedup, live latency or
consumer visibility. The correction is retained for measurement fidelity, not
because it makes the numbers smaller. H12 remains parked.

Verification: format, all-target strict Clippy, debug/release recording-load tests
passed. Additional bounded replays passed: 21,250 snapshot-burst records, 317
socket/native-ID/pipe records, and a 61-second/10-message-per-second run with 4,617
records including 100 real periodic captures. The small periodic run used 24,720
bytes of reference metadata. No live feed or production source was changed.

### Scheduling attribution: don't assume the guest scheduler caused the tails

A separate 10-second/1,000-message-per-second diagnostic recorded every measured
processing start/end against `perf record --switch-events --clockid mono`, using
the same monotonic clock. The analysis checked all 10,000 spans, 26,119 owner
switch records and exact agreement with the reported p99. Only **2/10,000**
processing spans overlapped recorded task descheduling; those two contributed
50,537ns of the slowest 100 spans' 5,295,269ns. Subtracting recorded descheduling
left p99 unchanged at 33.875µs. Its 84,103 output records matched the oracle.

This contradicts blaming ordinary guest task descheduling for most processing
tails in this diagnostic. It does not measure IRQ time, hypervisor steal, cache
effects, frequency or memory-layout effects, nor establish the cause of the
between-run variation. Hardware `cycles`/`instructions` counters report
`<not supported>` in this orb. No lost-record marker appeared in the perf script.
Instrumentation was removed; the retained test patch matches the calibrated
patch byte-for-byte. Evidence: `e14/mixed-aa/manifest.json`, smoke/check logs,
`scheduling-{summary.json,spans.csv,analysis.py,switches.txt.gz}` and diagnostic
patch/log. Promotion remains blocked by measurement uncertainty, not by a
demonstrated financial-correctness failure.

## E15 — direct object decoding with reusable error context

Implemented at the user's request after comparing the pinned NautilusTrader
Polymarket parser. Production scope is `src/market_data/{mod,polymarket}.rs`;
public decoder contracts are extended in the existing `tests/decoder.rs`.
Valid objects bypass the redundant RawValue envelope scan and one-element Vec.
Wire parsing remains order-independent, bounded and complete before emission.
Arrays retain their existing prevalidation. Error scope reuses the parsed Wire;
there is no whole-pipeline retry after invalid quantities/timestamps. A raw
syntax scan remains necessary after early typed-field failures. Whitespace-
prefixed typed failures alone reparse to preserve exact diagnostic positions.

Validation against the original HEAD decoder, compiled separately in a temporary
test module, matched **43,827 cases**, comparing returned error categories/reasons,
every emitted event/error, affected identities and resource flags. Inputs include
retained V1 messages, all 120 snapshot field permutations, invalid late timestamps,
escaped identities/keys, truncations, mutations, malformed tails, nested ignored
data and byte/event/entry/identity limits. The public `decode` API was compared too.
Existing independent book/recording oracles remain the financial correctness check;
agreement with the original alone would not prove the original was correct.

Isolated hot-cache diagnostic, three alternating pairs (median ns/message):

| Input | Original | Revised | Observation |
|---|---:|---:|---|
| Retained valid two-entry object | 1,417.84 | 979.52 | 30.91% lower |
| Invalid side in retained object | 2,483.52 | 1,544.58 | 37.81% lower |
| Wrong-typed asset field | 863.24 | 520.58 | 39.69% lower |
| Valid object followed by garbage | 462.44 | 500.15 | 8.16% higher (~38ns) |
| Retained snapshot array | 123,795.85 | 121,372.40 | Unchanged path; individual pairs mixed |

This is isolated decoding, not pipeline latency; the original is compiled as a
test module and the revised path through the library. The first revision reparsed
trailing-garbage inputs, costing ~981ns. Separating Wire deserialization from the
end-of-input check removed that redundant error scan; both diagnostic versions
are preserved. No malformed-input rejection was skipped to improve timings.

Full `cargo test --locked` and `cargo test --locked --release` each passed 162 tests
with 10 explicit ignored probes. Format, all-target check and strict Clippy passed.
The temporary differential test was archived under `e15/diagnostic.rs` then removed.
The permanent stage diagnostic now also measures `SessionDecoder::visit`, so it
does not accidentally bypass the improved entry path.

Predeclared pipeline checks: three alternating 20-second pairs each for mixed and
native-width identities, 100 books/1,000 messages/s, unchanged regression guards,
identical shared measurement patch and full independent SBE comparison. Interpret
these alongside the noisy E14 A/A control; do not retry until a favorable result.

Both campaigns completed. Processing includes encoding, but excludes ingress
queue residence and consumer visibility. Per-pair processing p99 in microseconds:

| Workload / pair | Original | Revised | Improvement | All regression guards pass |
|---|---:|---:|---:|---|
| Mixed / 0 | 31.676 | 32.113 | −1.38% | yes |
| Mixed / 1 | 35.731 | 52.797 | −47.76% | yes |
| Mixed / 2 | 35.356 | 49.040 | −38.70% | no |
| Native IDs / 0 | 39.339 | 36.275 | +7.79% | yes |
| Native IDs / 1 | 37.888 | 33.707 | +11.04% | no |
| Native IDs / 2 | 39.048 | 58.541 | −49.92% | no |

Every run matched all 154,100 SBE records against the independent oracle
(1,849,200 comparisons across 12 runs). Both screens returned
`inconclusive_or_regression`. Mixed processing p99 worsened in every pair; native
IDs improved in two pairs but failed regression guards in two. Prior A/A variation
does not establish that these failures are noise. No unchanged retry was run.

The candidate remains locally implemented and uncommitted for inspection, but is
**not qualified for promotion as an overall latency improvement**. Its isolated
decoder saving and correctness evidence do not override the pipeline failures.
Before another performance hypothesis, attribute the remaining tail by event
class and input size, using identical instrumentation on both arms, rather than
assuming every mixed p99 sample is an object-parser cost. This next diagnostic has
not been executed; live latency and per-record consumer visibility remain unmeasured.

Evidence: `.amp/in/artifacts/harness/e15/{mixed,native-ids}/manifest.json`,
`differential-and-cost.log`, archived diagnostic/reference source and full check
logs. Production patch SHA-256:
`4749a79718f254730f0e98b026550360085a7397d33052a402478249b604c6b3`;
shared measurement patch SHA-256:
`991a2840ef7d10887d7ff807d4d7d89ed4af6227d0212c15c59f3544d5e4e5ef`.

## E16 — equivalent-build mechanism cost and bounded load curve

The adopted workflow separates computational efficiency, sustainable capacity and
tail latency. Keep H15 unchanged while checking its computational claim through
equivalent compilation boundaries. Both release libraries are now called by the
same `tests/decoder.rs` public-API diagnostic; no frozen implementation is compiled
inside one arm's test module. Build/check once per arm, copy binaries, then run
all timing serially with matching benchmark/lock digests.

Predeclared campaign: three alternating decoder pairs plus three 20-second pairs
at each of 1,000, 3,000 and 5,000 messages/s; 100 books, mixed direct ingress, file
output, unchanged replay oracle. Reverse rate order on alternating pairs. Preserve
per-shape latency, add five chronological offered-count queue-age windows, and keep
the existing tail regression tolerances unchanged. Fixed 1,800-second total budget;
no rerun-until-pass. This establishes finite load response, not maximum throughput.
Hardware cycles/cache counters, dual-feed maintenance and live exchange latency
remain outside this measurement. Process CPU includes the post-drain oracle.

Before new timing, inspect E15's already-recorded shape data. Delta p99s in µs:

| E15 mixed pair | One entry, original → revised | Four entries | Sixteen entries |
|---|---:|---:|---:|
| 0 | 15.153 → 14.771 | 19.575 → 19.405 | 35.018 → 34.378 |
| 1 | 18.207 → 34.851 | 23.314 → 46.266 | 40.074 → 64.094 |
| 2 | 16.568 → 31.988 | 22.089 → 37.392 | 39.478 → 55.090 |

The two large regressions span all delta sizes; they are not merely a few slow
replacement arrays. This localizes the observation but does not attribute cause
to parsing, allocator, cache, scheduling or the book structure. Sparse replacement
samples cannot support a reliable extreme percentile. Do not rewrite BTreeMap on
this evidence alone.

Runner contract tests: 13 passed, including missing/duplicate decoder cases,
non-finite results and asymmetric queue-window partitions. The recording-load
non-ignored tests and strict all-target Clippy passed before the campaign.
Evidence and exact hypothesis: `.amp/in/artifacts/harness/e16/`.

### Completed results

Equivalent-build public `SessionDecoder::visit` medians, in µs (median of three
per-run p50s; not pooled samples or hardware CPU cycles):

| Input | Original | Revised | Observation |
|---|---:|---:|---|
| Retained two-entry updates | 1.561 | 1.054 | Per-pair reductions 35.25%, 32.48%, 26.75% |
| Eight-level snapshot object | 2.077 | 1.554 | Every pair lower |
| 256-level snapshot object | 47.074 | 32.389 | Every pair lower |
| 2,048-level snapshot object | 374.403 | 261.542 | Every pair lower |
| Retained four-snapshot array | 139.740 | 128.524 | Every pair lower; outer array path unchanged, so do not attribute all savings to scan removal |

The equivalent-build comparison supports computational savings without the E15
test-module/library asymmetry. It remains a repeated hot-cache workload, not a
prediction for every input or a claim that each savings percentage is causal.

| Offered messages/s | Processing p99 improvement, pairs 0/1/2 | Regression guards | Tail screen |
|---|---|---|---|
| 1,000 | +4.65%, +36.62%, +22.46% | all passed | Promising, requires qualification |
| 3,000 | +17.93%, −20.83%, −112.89% | pairs 1 and 2 failed | Inconclusive/regression |
| 5,000 | +20.30%, +45.21%, +12.08% | pair 2 failed | Inconclusive/regression |

At 3k/s, pair 1 processing p99.9 regressed 19.20%; pair 2 regressed 116.52% and
queue p99 regressed 23.11%. At 5k/s, pair 2 processing p99.9 regressed 9.95% despite
lower processing p99 and queue p99. These failures remain open; the favorable
1k/s screen does not erase E15's failed 1k/s result. No unchanged rerun followed.

All 18 pipeline trials met offered-rate tolerance, drained successfully and
matched **8,318,106 SBE records** in total. At 5k/s the candidate's five queue-age
window medians stayed between 22.62 and 23.47µs, and final drain took 21–34ms.
This screen did not demonstrate sustained backlog growth or reach a measured
capacity limit. It does not prove full-universe or long-duration sustainability.
Whole-benchmark CPU at 5k/s was lower in all three candidate runs (9.15→8.73,
9.31→9.02, 9.50→9.12 CPU-seconds), but includes generator/oracle/readers and must
not be presented as collector-only CPU savings. Peak RSS stayed within 0.14% of
baseline at every load point.

Decision: **retain H15 as a computationally promising collector implementation
with unresolved tail regressions**. Do not discard it simply because one combined
p99 screen fails; equally, do not promote it into a fully qualified release.
The next independent candidate is H08's already-demonstrated optional-string
allocation reduction, measured alone before any combination. Before promotion,
the mixed-load tail warnings still require bounded attribution/qualification.
The production patch is unchanged from E15; this campaign changed measurement
and runner code only. Full E15 debug/release correctness evidence still applies,
and both E16 arms also passed the integration-tier release suites.

## E17 — optional-string borrowing alone, then alongside H15

Restore H08's small transparent Cow wrapper for optional wire IDs/timestamps.
Unescaped strings now borrow; escaped strings still own their decoded contents.
Missing/null/wrong-type behavior, exact decimals, message lifetimes and error
scope remain unchanged. The locked Serde implementation and published BorrowCow
mechanism establish why the existing Option annotation alone did not borrow.
No dependency, shared pointer, allocator, queue or book change is involved.

First compare the allocation-only patch against HEAD using three alternating
five-second pairs at 1k/5k messages/s. Then compare parser-only H15 against H15+H08
using three twenty-second pairs at 1k/3k/5k messages/s. This second comparison
isolates the incremental allocation change rather than conflating two changes.
Both campaigns use equivalent release-library builds, identical tests, decoder
diagnostics and exact SBE replay. Heaptrack runs separately after timing: one
measured second plus two warmup seconds, 100 books, 1k messages/s. Instrumented
latencies never enter timing comparisons. Budgets are 900/1,800 seconds; no
unchanged rerun-until-pass. Exact inputs and logs: `.amp/in/artifacts/harness/e17/`.

Before timing, 43,827 differential cases matched the frozen parser-only source.
Full debug and release suites each passed 163 tests, with 10 opt-in tests ignored;
all-target check, strict Clippy and formatting passed. Public decoder tests cover
escaped IDs and timestamps through object/array input plus default/null/type/error
scope behavior. Fourteen runner tests passed. The runner now supports explicit
`--baseline-patch` for combinations without creating commits and optional bounded
`--allocation-profile` captures, kept separate from its timing trials.

### Allocation-only screen completed

Filtered Processor allocation calls fell **26,570 → 23,355**, exactly 3,215 fewer
(12.10%). This includes initialization, warmup and observation callbacks; it is
neither total-process allocations nor a per-delta count. All twelve pipeline
trials drained and matched **1,765,230 SBE records** in total.

| Offered messages/s | Processing p99 improvement, pairs 0/1/2 | Regression guards |
|---|---|---|
| 1,000 | +8.01%, −48.85%, +35.71% | pass / fail / pass |
| 5,000 | −4.13%, +30.61%, +8.49% | fail / pass / fail |

The allocation count reduction reproduced; a consistent decoder-time reduction
did not. Median-of-run-p50 retained-update decoding was 1.439 → 1.507µs; shallow
eight-level snapshots were 1.894 → 1.875µs. Retained snapshot-array cost increased
in every pair (median 127.462 → 132.530µs). Preserve that warning rather than
inferring that fewer allocations guarantee faster execution. At 5k/s, pair 2's
processing p99.9 increased 32.454 → 60.083µs despite its lower p99.

### Combined evaluation completed

H15 → H15+H08 removed the same 3,215 allocation calls, **20,572 → 17,357**
(15.63%). All eighteen pipeline trials matched **8,318,106 SBE records**.
Median-of-run-p50 decoder cost was 0.984 → 0.995µs for retained updates and
121.390 → 119.177µs for retained snapshot arrays; the extra borrowing change
does not establish a consistent decoder-time win beyond H15.

| Offered messages/s | Processing p99 improvement, pairs 0/1/2 | Regression guards |
|---|---|---|
| 1,000 | +5.54%, +4.04%, +2.29% | pass / fail / pass |
| 3,000 | +2.63%, −11.94%, +24.12% | pass / pass / pass |
| 5,000 | −5.30%, −4.84%, +25.68% | pass / fail / pass |

At 1k/s pair 1 queue p99 increased 117.728 → 125.859µs. At 5k/s pair 1 queue
p99 increased 40.456 → 59.146µs and processing p99.9 increased 35.947 → 41.250µs.
These remain failed guards; no unchanged rerun followed. All three 3k/s CPU
measurements fell, but whole-process CPU includes generator/oracle/output work.
At 5k/s candidate chronological queue-window medians stayed 19.73–20.60µs,
with 18–21ms final drains. No sustained backlog or saturation was established.

Latest combined processing percentiles (median of three run percentiles, µs;
decode/book/recorder callbacks, excluding queue, network and consumer visibility):

| Offered messages/s | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| 1,000 | 8.775 | 24.487 | 28.901 | 41.556 |
| 3,000 | 5.752 | 19.797 | 27.167 | 42.670 |
| 5,000 | 4.425 | 12.805 | 18.476 | 27.570 |

Additional existing opt-in checks passed on the combined code. The 100,000-message
replica replay (32 instruments, 64 replicas, prebuilt burst input, no SBE or
maintenance) achieved 39,930 msg/s and checked every selected update and final
replica. Processing p50/p95/p99/p99.9 was 2/3/5/61µs; queue was 81/141/230/297µs.
This is a bounded different workload, not a paired speedup or full-collector
capacity claim. The separate pipe visibility fixture observed the 84-byte record
at 3.257ms and the full 64MiB chunk at 256.048ms, both measured since consumer
startup, with a test-only 200ms chunk deadline. It round-tripped the supplied
reader; these are not producer-to-consumer latency or production-default timings.

Decision: retain H15+H08 experimentally: parser computational benefit and fewer
allocations are established; consistent overall tail improvement is not. All
production and harness changes remain local/uncommitted. Follow-up E18 profiles
remaining mixed processing before selecting another change.

## E18 — profile remaining work and reuse validated instrument IDs

The H15+H08 software CPU profile captured 1,547 samples, including 420 containing
Processor and 65 containing resolve. It replayed 420,115 exact records. Read the
locked Crossbeam receive path and current binding/identity construction before
choosing a change: high receive CPU is not proof that a queue rewrite helps.
Research applicability and limits are recorded in RESEARCH.md; exact source,
profile exports and predeclared hypothesis are under `.amp/in/artifacts/harness/e18/`.

Restore only H05's previously researched resolve change alongside H15+H08:
look up the requested Binding first and index its stable BookId. Request misses
still validate namespace-specific registry membership; replica binding equality
and inactive-state guards remain. No new map/cache/hasher, unsafe indexing,
ownership change or public API is introduced. Existing public replay tests cover
namespaces, exact IDs, unknown versus unsubscribed input and failure scope;
lifecycle tests cover old generations, ownership and retirement. Full debug and
release each passed 163 tests (10 opt-in tests ignored), plus fmt/check/Clippy.

Three alternating five-second pairs at each of 1k/5k, identical measurement/source
controls, 100 books, ingress/file; 900-second cap per campaign. Mixed short-ID
screening failed. The distinct native-width applicability control uses synthetic
78-character IDs, not live instruments. Its favorable result cannot erase the
short-ID failures or E03's earlier failed screens.

| Workload / messages/s | Processing p99 improvement, pairs 0/1/2 | Guards |
|---|---|---|
| Mixed / 1,000 | −7.27%, −10.57%, −30.73% | fail / fail / fail |
| Mixed / 5,000 | −0.72%, −19.69%, +31.08% | pass / fail / fail |
| Native-width / 1,000 | −6.13%, +23.46%, −0.99% | fail / pass / fail |
| Native-width / 5,000 | +22.84%, +23.79%, +14.14% | pass / pass / pass |

All 24 smoke trials matched **3,530,460 SBE records**. A separate post-change
profile matched another 420,115 records: 1,518 total samples, 390 containing
Processor, 30 containing resolve. That reduction supports the removed-work
mechanism but is not a confidence interval or a wall-time speedup estimate.
Instrumented timings are excluded from every latency comparison.

Confirm the positive native-width 5k/s regime using three twenty-second pairs
at the existing integration tier, same patches and 900-second ceiling. This is
a longer confirmation of a passed screen, not a retry-until-pass of failed cases.
Lower-rate/short-ID warnings remain; the three-change combination is experimental,
not a promoted release.

### Longer confirmation failed; no retry followed

All six twenty-second trials matched **4,620,750 records**. Processing p99 pairs
in µs were **20.344 → 36.122**, **28.882 → 26.695**, **35.767 → 21.783**:
−77.56%, +7.57%, +39.10% improvements respectively. Pair 0 also regressed p99.9
31.513 → 60.149µs and queue p99 48.514 → 60.434µs; the other guards passed.
The short-run positive signal did not survive the longer confirmation.

Latest native-width 5k/s processing percentiles, median of three run percentiles
(µs, not pooled distributions, excluding ingress/network/consumer visibility):

| Implementation | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| H15+H08 | 5.313 | 18.105 | 28.882 | 49.190 |
| H15+H08+H05 | 4.938 | 16.742 | 26.695 | 50.328 |

The favorable median p99 is not a pass: retain the individual failed pair and
the prior mixed failures. Fewer required lookups and reduced resolve samples
support computational benefit, but do not attribute the wall-time regressions
or establish a general latency improvement. Keep the third change experimental,
with its exact two-change baseline and source patch preserved. No more unchanged
confirmation, fourth stacked optimization, queue rewrite or sharding is justified
by this evidence. Current production changes remain local/uncommitted on
`harness-latency`, not promoted, pushed or deployed.

Final current-tree replica replay passed 100,000 messages at 39,920 msg/s,
checking selected history and every final replica; p50/p95/p99/p99.9 processing
was 2/3/6/66µs (no SBE or maintenance, not an A/B claim). Runner tests passed 14/14
again, fmt/diff checks passed, and only the main worktree remains. The final
production diff matches archived `candidate.patch` SHA-256
`525b0d5fa15e68f0e4cc86752a2dadb5b964ac4f6171d56144147dbdaa605697`.

## E19 — bounded 16-entry tail diagnosis, not another acceptance campaign

Instrumented only disposable worktrees: A=H15+H08, B adds H05. Split the owner
span into dispatch/decode, preparation, mutation, and publication; record ingress
occupancy, free output batches, writer intervals, and same-clock Linux switches.
Six serialized 20-second replays at 5k messages/s followed a two-second pilot:
A/A unchanged-code controls, then A/B and B/A. All six matched 770,125 SBE
records each. No lost perf events; analysis checks counts, ordinals, clock order,
phase sums and reconstructed report p99. Instrumentation changes timings: these
are attribution results, not comparable release qualification numbers.

Normal (≤40µs) 16-entry preparation averaged 3.82–4.01µs in the paired A runs
and 2.92–3.06µs in B. That supports H05's removed-work mechanism. It does not
explain the historical failed confirmation. The same A executable ranged from
30.385 to 47.317µs p99 across controls/pairs. Only 12 of 1,492 >40µs batches
overlapped guest descheduling; output capacity and ingress samples did not show
sustained saturation. Slow work appeared in multiple phases, not one proven
collector bottleneck. Guest scheduling cannot expose host steal, IRQ time,
frequency or cache effects; hardware counters are unavailable here.

### Proven harness artifact: sample-vector growth and first-touch faults

`encoding.push()` ran inside the timed Processor callback with an initially empty
vector. At repeatable sample-capacity crossings, publication took roughly
66–247µs although Recorder callbacks took only about 2–3µs. This required a
discriminating intervention rather than labeling all outliers environmental.

Built one new A executable with a setup-only runtime toggle: either the original
empty vector or a preallocated, nonzero-filled and cleared vector. Same binary
for off/on then on/off, four 20-second runs; add direct capacity observations
and every minor-fault timestamp. All four matched another 3,080,500 records.
The faulting instruction at closure+0xb8 is the sample-vector store following
`RawVec::grow_one`, not the SBE encoder. Disassembly and trace artifacts retained.

| Sample storage | 16-entry p99 µs | p99.9 µs | Batches >40µs | Batches with minor faults |
|---|---:|---:|---:|---:|
| Original, run 1 | 33.180 | 113.026 | 163 | 2,119 |
| Prepared, run 1 | 32.272 | 66.903 | 169 | 4 |
| Prepared, run 2 | 36.120 | 53.178 | 158 | 24 |
| Original, run 2 | 36.695 | 48.282 | 256 | 2,099 |

At ordinals 12439, 19462, 28822, 47548 and 84994, original publication spans
were 77.595–123.350µs; with prepared storage the corresponding spans were
3.377–15.153µs, with no capacity growth. The intervention moves those sample
page faults before intake, not out of total program work. It is a harness repair,
not a collector speedup. Tracing each fault adds overhead to faulting runs.

**Remaining uncertainty:** the prepared runs still had 327 >40µs batches. None
contained minor faults, only two overlapped guest descheduling, only five
overlapped writer calls, and every slow batch had at least 14 free output batches.
This does not establish that those overlaps caused the delay, or rule out other
memory/system effects. Most remaining tail attribution is unresolved; do not
erase E18's failed confirmation or claim the benchmark artifact explains it all.

Retain only the bounded sample-storage repair in `tests/recording_load.rs`:
preallocate and touch at most 16 u128 samples per offered measured message,
then fail if that fixture bound is exceeded. This uses 25.6 MB for 100k measured
messages; no sample truncation or production source changes. The three production
changes retain the exact E18 diff hash. Release recording-load tests passed
(2 passed, 2 opt-in ignored); explicit two-second/5k mixed, native-ID and
snapshot-burst replays matched another 421,307 records. fmt and scoped strict
release Clippy passed. No full-suite rerun or latency acceptance claim.

Evidence: `.amp/in/artifacts/harness/e19/` contains patches, binary hashes,
plans, raw CSV/perf traces, join analysis, summaries and check logs. A shared
Cargo-cache reuse was caught before initial A/B timing by identical binary
hashes; the rejected build was preserved and B rebuilt after release-package
cleaning. Accepted binaries differed. Future cross-worktree builds must isolate
targets or explicitly invalidate release artifacts. Diagnostic worktrees were
removed after archival. Changes remain local/uncommitted on `harness-latency`.

Next useful evidence is a bounded attribution of the remaining decode-heavy
outliers using the repaired harness and per-thread CPU-time/system evidence,
with clock-read overhead controlled. Do not stack another optimization or retry
the old confirmation unchanged. Any future qualification needs both arms to
use this same repaired measurement source.

## E20 — specialize nested price-change decoding, not instrument hashing

User scope: horizontal C1–C5 specialization audit plus evidence for the strongest
applicable candidate. The ranked map and pinned implementation references are
in RESEARCH.md E20. Main production source remained byte-for-byte unchanged
from the E18/E19 diff. No new dependency, unsafe code, architecture change or
live traffic. One read-only research subagent inspected external source; no
CPU-heavy research ran alongside timings.

### Candidate and predeclared controls

Baseline is current H15+H08+H05, not HEAD alone. A disposable candidate makes
Wire generic over raw versus bounded typed `price_changes`. Default-limit
single objects can decode entries during the first traversal; arrays and custom
limits remain on the existing path. Successful snapshot objects reuse their raw
book fields. Every failed attempt falls back before emission, preserving original
error precedence, source/asset scope and whole-envelope validation. This is a
restricted mechanism experiment, not a production choice to hard-code limits.

The fixed hypothesis and complete patches are in `.amp/in/artifacts/harness/e20/`:

- Baseline source SHA-256: `525b0d5fa15e68f0e4cc86752a2dadb5b964ac4f6171d56144147dbdaa605697`.
- Candidate source SHA-256: `7b7d1908596be72d468d4fcdda1d0dddabcfeee57adfe07208c1e230c21f1f3b`.
- Identical measurement patch: `64ae1bf49037051cf5fe7e16ac0dd057d5e1efe9f2e5fa44fe769f76a6589e59`.

The first runner attempt stopped before pipeline timing because the decoder
validator correctly rejected four newly added batch-width cases. Retained its
incomplete manifest, corrected the expected suite in runner and contract tests,
then ran the unchanged candidate once through the complete campaign. This was
not a latency retry: the incomplete attempt had no pipeline comparisons.

Correctness: **51,037** differential cases plus **3** explicit 65,534/65,535/65,536
entry-boundary cases matched the frozen current parser, including error strings,
affected IDs, sink results, malformed tails, duplicate/null fields, arbitrary
field order, ignored fields, escapes and resource limits. Release lib, decoder,
book replay, lifecycle and recording suites passed **98 tests per arm** (one
opt-in profile ignored until explicitly run). Synthetic 1/4/16/64-entry decoder
cases check values, IDs, sides and ordering against independent integer formulas;
retained messages use their separate fixture oracle.

### Computational evidence is favorable for larger update batches

Equivalent release library builds, identical profile source; three alternating
pairs. Each case has 10k calls, with warmup outside timing. Results below use
total loop elapsed time divided by calls, including timer/sample overhead:

| Case | Baseline → candidate ns/call, pairs 0 / 1 / 2 | Change |
|---|---|---|
| Retained small deltas | 1173→859 / 1068→784 / 1113→1225 | 26.82%, 26.57% faster; 10.01% slower |
| Synthetic 16-entry changes | 4243→2997 / 4413→3100 / 4739→3663 | 22.70–29.75% faster |
| Synthetic 64-entry changes | 17766→11902 / 17435→11562 / 20054→14635 | 27.02–33.69% faster |
| Retained snapshot array | 130167→136009 / 131181→132191 / 128785→147198 | 0.77–14.30% slower |

The mechanism removes a real list traversal; these are elapsed microbenchmarks,
not hardware instruction counts. The snapshot array algorithm was not optimized
and its regression cannot be dismissed because the source path looks similar.
No cause is assigned from these measurements alone.

The separate same-binary frozen-module diagnostic measured fallback costs:
semantic-error decoding rose 24.7–42.1%, typed errors 49.2–70.6%, and malformed
tails 137.9–146.4%. A second parse is real added work. Valid-input savings do not
justify hiding this tradeoff. Irrelevant `price_changes` on other event types can
also cause speculative typed allocation before fallback; it remains bounded by
the default cap, but is not free. Generalizing limits/error handling requires
a bounded seeded visitor and fresh correctness/cost evidence, not deleting guards.

### Controlled pipeline screening still fails

Three alternating five-second pairs at each of 1k and 5k messages/s, 100 books,
native-width synthetic IDs, ingress/file; one 900-second campaign ceiling.
E19's prepared timing storage is identical in both arms. All **1,765,230 SBE
records** matched exactly; both builds used the same schema/lockfile and distinct
binary hashes. No profile ran during pipeline timings.

| Rate | Processing p99 µs, baseline → candidate (pairs 0 / 1 / 2) | Guards |
|---|---|---|
| 1k/s | 73.467→37.366 / 28.896→29.376 / 31.390→26.233 | pass / fail / fail |
| 5k/s | 36.146→17.923 / 24.625→19.829 / 18.434→21.862 | pass / pass / fail |

The last 5k pair worsened processing p99 by **18.60%**, queue p99
28.609→41.792µs and processing p99.9 24.682→30.842µs. Preserve it alongside the
favorable pairs. Completion at these finite offered rates is not maximum
sustainable capacity, full-universe coverage or a tail-latency qualification.
Whole-process CPU/RSS include generator, oracle and output, not only the owner.

### Decision and retained work

**Archive H16; do not integrate or promote it.** Nested traversal elimination is
promising computationally, especially for larger batches, but fallback cost,
snapshot regression and pipeline tails remain unresolved. No fourth production
optimization was stacked. Do not rerun the same failed screen until it passes.

Keep only the new opt-in 1/4/16/64 decoder cases and the runner's matching
validation contract in `tests/decoder.rs`, `tools/latency_harness.py` and
`tools/test_latency_harness.py`, plus research/experiment/hypothesis notes.
Runner tests passed 14/14; main release decoder tests passed 7 (one ignored);
fmt, strict all-target Clippy and diff checks passed. Compiler control confirmed
that a safe equivalent event-name match already lowers to length dispatch,
integer compares and SSE2 for 16-byte names; its membership test passed.
The temporary prototype and frozen baseline are archived with E20, and the
disposable checkout is removed after validation. Nothing committed or pushed.

## E21 — C4 attribution and occupancy-preserving update specialization

Completed the new campaign's four initial deliverables: deep-snapshot stage
breakdown, C4 representation audit, three ranked hypotheses, and one isolated
implementation with correctness/performance evidence. Full research and the
stage table are in RESEARCH.md E21. C3 experiments remain paused; H17 changes
only C4 count bookkeeping. H18/H19 are hypotheses, not extra implementations.

### Measurement improvements retained

`tests/recording_load.rs` now reports owner and recorder time separately for each
offered message shape. Recorder spans are summed per message; no subtraction of
independently ranked percentiles. Queue age distinguishes replacement arrays,
the next three affected-book updates, the next three unrelated-book updates,
and later updates. Groups follow offered ordinals/fixture identities, not
consumer wall time or observed delay. An explicit wraparound test and runner
validation check group identity and exact counts. Empty groups report null
quantiles, not zero latency. All observations still feed the unchanged SBE oracle.

Additional grouping/sample pushes occur outside owner timing, but can perturb
queue scheduling; this is identical across arms, not zero-overhead telemetry.
The bounded recorder-sample storage from E19 remains preallocated/page-touched.
Fine-grained C4 source clocks exist only in an archived disposable diagnostic,
not the production candidate or the pipeline comparison binaries.

### Baseline attribution precedes implementation

Two three-second burst replays and one mixed replay at 5k/s matched **527,789**
records. Each burst measured 750 replacement arrays, eight books per array,
128–383 bids plus one ask per book. Decode/validation outside event callbacks
accounted for 70–72% of array time; construction including duplicate validation
15–16%; installation/old-tree destruction 3–4%; recorder callback 8–9%.
These are shares of matched elapsed spans, not CPU instruction counts.
Slowest-array subsets still have substantial decode cost. Duplicate validation
is fused with tree insertion; it is not assigned a fictional standalone duration.
Remaining clocks, event dispatch and scheduling explain the unassigned residual.

The first three unrelated updates after an array had queue p99 about
0.69–1.06ms, compared with 0.086–0.323ms for later updates in those diagnostic
runs. This motivates retained head-of-line measurements, not a C2 architecture
rewrite. Full-universe and long-duration sustainable capacity remain unmeasured.

### One isolated candidate and its falsifiers

H17 moves `counts.entry` and the side/global budget check into
`occupied != present`. Quantity arithmetic/provisional storage, ordered prepared
entries, source/health state and output remain unchanged. The initial committed
book is valid; a count-preserving change cannot violate occupancy limits. Every
transition is checked immediately, so add-before-delete at capacity still fails.
No new retained state, dependency or unsafe code. Existing H15/H08/H05 remain
the baseline, with source patch hash
`525b0d5fa15e68f0e4cc86752a2dadb5b964ac4f6171d56144147dbdaa605697`.

The new public-API oracle exhausts **9,604** four-entry sequences, independently
recounting two-book/two-side arrays after each entry. It checks intermediate
limits, repeated prices, zero no-ops, all emitted values and atomic rejection.
Both arms passed **99 focused release tests**, including signed-relative,
overflow/negative and source-isolation cases. A test-authoring JSON macro error
was fixed before timing; no failed behavior check was ignored.

A separate equivalent-build predecoded diagnostic covers widths 1/16/64,
zero/mixed/all occupancy transitions, Set/Add, 1k warmup + 10k measured events,
three alternating pairs. It verifies every observed quantity against input
formulas outside timing. Real event preparation/mutation runs, but only a small
sink—not parsing, SBE output or ingress. Width-one mixed mode necessarily has
one transitioning entry (100%); actual transition counts are reported.

- 16-entry no-transition Set: **5.8–11.9%** less elapsed event time; Add:
  **7.6–11.6%** less, across all three pairs.
- 16-entry all-transition Set: **7.7–54.3% slower**; Add **7.4–36.5% slower**.
  The unfavorable control is a real gate, not discarded noise.
- Single-entry results are mixed/adverse; 64-entry all-transition results improve.
  Width-dependent regressions rule out claiming a universal cost reduction.

### Pipeline result: do not integrate H17

Three alternating five-second pairs per workload and rate (1k/5k), 100 books,
ingress/file, identical repaired measurement patch, serialized builds/timing.
All **3,542,958** timed SBE records matched. Each campaign has a fixed 900s ceiling.

| Workload/rate | Pairwise processing-p99 savings (%) | Regression guards |
|---|---|---|
| Mixed 1k/s | −7.80, +35.98, +39.16 | fail/pass/pass |
| Mixed 5k/s | +0.11, −7.84, −37.90 | fail/fail/fail |
| Snapshot burst 1k/s | −12.87, +12.52, −17.86 | fail/pass/fail |
| Snapshot burst 5k/s | +17.46, +10.74, +3.28 | pass/pass/pass |

One favorable short burst screen does not override failures elsewhere or prove
the unchanged snapshot algorithm faster. Median run quantiles at 5k/s (µs):

| Workload/arm | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| Mixed baseline | 4.522 | 13.434 | 19.167 | 30.623 |
| Mixed candidate | 4.480 | 14.080 | 19.146 | 44.317 |
| Burst baseline | 5.079 | 71.037 | 631.758 | 973.959 |
| Burst candidate | 4.879 | 100.215 | 605.116 | 864.655 |

These are medians of run quantiles, not pooled data or paired effect estimates.
Mixed pair 2 worsened p99 **20.666→28.498µs**. Shape attribution localized its
largest update tail to 16-entry messages: processing p99 **22.770→35.900µs**,
owner **18.250→32.150µs**, recorder **2.890→14.600µs**. Owner and recorder
quantiles need not belong to the same messages and must not be subtracted.
Offered lag p99 improved 140.021→113.810µs and queue p99 improved
54.174→49.267µs in that pair; worse offered pacing alone therefore does not
explain the processing regression. No unique hash-layout/cache/CPU-scheduling
cause is established. C4's transition-heavy control also needs explanation.

All finite offered rates completed/drained, but burst baseline pair 0 had queue
p99 9.235ms with a late-window spike. Do not infer stable capacity from completion
or hide this run. Other burst baseline queue p99s were 0.769/0.827ms; candidate
0.741/0.666/0.697ms. Full production health/replica maintenance is excluded.

Separate one-second/1k heaptrack replays found identical Processor-stack
allocation calls: **17,345** per mixed arm, **131,786** per burst arm (includes
startup, warmup and callbacks). No per-operation allocation reduction claimed.
Whole-process 5k CPU-time averages were 2.767→2.747s mixed and 4.430→4.263s burst;
peak RSS maxima 137628→137500KiB and 140892→140264KiB respectively. These include
generator, oracle and output; they do not isolate owner CPU or book peak bytes.
The candidate does not alter old-plus-new replacement limits or tree allocation.

### Delivery and next experiment

Retain H17's patch, microbench and all favorable/unfavorable results as an
experiment. **Do not integrate, promote or retry for a green screen.** The next
independent snapshot hypothesis is H18's zero-filter pass, with a limited upside;
before revisiting H17, discriminate hash-seed/code-layout and CPU-time effects
on all-transition 16-entry events. No architecture change is justified here.

Persistent changes in this campaign: `tests/recording_load.rs`,
`tests/book_replay.rs`, `tools/latency_harness.py`, `tools/test_latency_harness.py`,
README.md and the three research ledgers. Earlier user/source edits are preserved.
Main passed fmt, all-target check, full debug and release suites (**165 tests
each**, opt-in probes ignored), strict all-target Clippy, diff checks and **14**
Python runner tests. Source clocks and candidates are archived, not integrated.
No live runs, commits or pushes. Results, exact patches, binaries' hashes,
commands and `summary.json` are under `.amp/in/artifacts/harness/e21/`.

## Full-owner capacity repair after frozen E21 (2026-10-28)

User-authorized correctness/capacity follow-up, not resumption of H17/H18.
Scope: preserve C1–C5 ownership and financial contracts; reproduce the production
owner's missing load conditions before changing architecture or resource budgets.

### Two localized changes, not full-universe qualification

- C4 readiness feedback previously scanned every retained source after each
  input. A bounded set now indexes active sources awaiting acknowledgment.
  All original readiness predicates remain authoritative; retries, subscription
  churn, disconnect and generation-matched acknowledgment preserve membership.
  The public lifecycle regression also checks invalid-data readiness retraction.
- C5's soft size watermark could publish the last partly used batch during a
  writer pause, making the next record fail despite unused bounded storage.
  Size publication now follows the existing age policy: retain the final batch
  when other batches are outstanding. Hard byte/descriptor limits, single-batch
  behavior, FIFO output and terminal failure remain unchanged. No pool enlargement.
  The extended writer-pause regression failed before this fix; afterwards all
  100 ordered records and control ordinals survived. The prefix-exhaustion test
  now reaches a hard two-descriptor bound rather than assuming a soft watermark
  must consume all capacity; its two-record prefix/failure assertions are intact.

### Production-owner diagnostic and controls

`src/owner_load_tests.rs` exercises actual `process_books`, bounded ingress,
health maintenance, 60-second snapshots, recorder and file output. Initialization
is paced by readiness; offered arrivals then use independent absolute deadlines.
550,000 books have native-width IDs and 0–8 levels per side. Offered messages
contain 80% single-level absolute changes/deletes with regressing exchange times
and 20% ignored trade events. An independent arithmetic oracle checks every
output delta and snapshot after drain. Runs of at least 65 seconds require a
periodic capture for every initialized book. REST, sockets, ongoing discovery
churn and live burst distributions are excluded. This is not a p99 benchmark.

Evidence: `.amp/in/artifacts/owner-capacity/`.

- Frozen owner at 550k books / 10k messages/s / 70s passed: 1,660,000 records,
  including 550,000 periodic captures. A 50k/s profile attributed about 13.3%
  sampled owner user CPU to readiness scanning/collection.
- Two completed 50k/s / 20s pairs had whole-process CPU 31.82→29.92s and
  30.90→30.30s. A third baseline failed during initialization; do not count it as
  a successful pair. The initial harness omitted production's writer-startup
  barrier; that omission was corrected rather than hidden.
- At 100k/s / 70s, both frozen and readiness-only versions failed output-pool
  exhaustion during periodic captures. This is not a replay of the live ingress
  failure, and these timing-dependent failures alone do not prove causality.
- Combined changes passed twice at 550k / 100k/s / 70s: 7,000,000 offered inputs,
  **6,700,000 matching financial records**, including 550,000 periodic captures
  in each run. The second run also enforced per-book periodic coverage. Maximum
  producer lateness was 24.647/16.751ms; owner-reported maximum ingress age was
  104.161/85.783ms. These maxima are not quantiles or sustainable-capacity claims.
- A scratch oracle altered by one scaled quantity unit failed as intended
  (1,000,000 versus 1,000,001); no negative-control code entered production.
- Full debug and release suites passed **166 tests each**, with opt-in probes
  ignored. Format, all-target check and strict Clippy passed; the strengthened
  harness subsequently passed its full-size run and static checks again.

### Live evidence still fails the acceptance gate

All attempts used unfiltered V1/V2, one replica, at most 640 sockets, 10 GiB RSS
and 20 GiB output. Market traffic differs between attempts; duration comparisons
are not controlled speedups.

- Frozen: ingress queue full at 637.25s. All 12,254,647 decoded archive records
  were internally consistent; 2,107,858 periodic captures matched replay.
- Readiness-only: output-pool exhaustion at 389.46s; 5,952,483 confirmed records.
  Not independently archive-validated in this follow-up.
- Combined: ingress queue full at **736.98s**, not recorder exhaustion. Peak RSS
  4,610,256,896 bytes; 17,187,614 confirmed records in 46 completed chunks. All
  **2,849,838 periodic snapshots** matched independent absolute-delta replay.
  Recorder failure position was null; upstream intake/drain was incomplete and
  the footer correctly reports failure. This does **not** fix full-universe
  capture. Final desired count was 551,718, with 44 unresolved V1 records and
  missing live identities; rolling health counts are not coverage evidence.

Artifacts: `.amp/in/artifacts/full-universe-20261028-1631/`,
`full-universe-readiness-20261028/` and `full-universe-capacity-fixed-20261028/`
under the same artifact root. They retain manifests, binary hashes, resources,
stderr, archives and sidecars; the first and third also have replay/coverage
analysis. No raw-feed oracle or exchange-wide losslessness claim.

### Measured burst allowance: completed intake/drain, coverage still incomplete

A separate temporary diagnostic build retained up to 4,096 owner timing rows
and the 64 slowest data payloads over 5ms, writing them only after processing
finished. Five wall-time spans cover process, control provenance, tick/maintain,
periodic capture and feedback/hints. One-second windows include queue high-water,
queue age and processed counts. These measurements perturb execution and are
not latency benchmarks. The profiler service restarted and overwrote its main
capture; no CPU attribution from that attempt is used here.

The diagnostic used **65,536 items with the unchanged 64 MiB byte budget**.
Six pre-cutoff windows exceeded 8,192 queued messages. The largest reached
**23,404** at 933.75s, then ended with four messages queued. It processed 36,466
messages in that window; maximum processing span was 336µs and queue age
123.954ms. Across complete pre-cutoff windows, maximum queue age was 141.678ms.
A separate 91.722ms desired-state operation occurred earlier, but is not the
cause of that largest burst. The trace does not identify whether exchange/network
batching or worker scheduling produced the burst. It does show recovery rather
than sustained owner saturation in these observations; sharding is not justified
by the original failure alone. Shutdown age maxima are reported separately and
must not be mistaken for pre-cutoff processing stalls.

The **1,000-second** bounded run completed and drained in 1,003.29s, exit 0,
without forced termination. 553 workers / 559 sessions received and drained
**11,689,868 messages**. All **22,799,660** financial records decoded with the
supplied reader; all **4,028,901** periodic snapshots matched independent replay.
Peak RSS was 4,581,314,560 bytes; recorder error was absent and run footer success
was true. These establish local intake/drain and archive consistency, not feed
completeness. The build uses the same production library plus diagnostic timing;
the final uninstrumented executable has not had a second full-universe run.

The executable now defaults to the tested 65,536-item allowance, with headroom
above the observed burst. Byte accounting, message limits, terminal overload,
FIFO and per-session ordering are unchanged. Queue-slot allocation increases
outside the byte-credit accounting; this is a finite burst budget, **not a
computational optimization**. `--queue-items` remains configurable. The new
deterministic regression with an unscheduled consumer fails at the old default
and preserves 25,000 payloads, timestamps and ordinals at the new default.
The independent steady-load harness intentionally retains its stricter 8,192
item queue so earlier comparisons remain like-for-like.
Final `cargo test --locked` and `cargo test --locked --release` each passed
**167 tests**, with opt-in probes ignored. Format, all-target check and strict
Clippy passed. Exact commands/output are in `owner-capacity/burst-final-checks.log`.

**Full-universe coverage still fails:** desired 551,878; the exact owner-ledger
cutoff has 21,024 V1 and 2,886 V2 desired identities not live. Discovery retained
44 unresolved V1 records; its final V1 sweep was incomplete. Neither passing
drain nor historical initialization totals certify current live coverage. Health
revisit time also exceeded the provisional 10-second target; monitoring was off.

Evidence: `.amp/in/artifacts/full-universe-owner-diagnostic-20261028/` contains
the manifest, temporary diagnostic source, resource samples, bounded raw samples,
timing trace, supplied-reader validation and exact coverage ledger. Production
changes are in `src/main.rs`, `src/orderbook/store.rs`, `src/recording/mod.rs`;
checks are in `src/owner_load_tests.rs`, `tests/book_lifecycle.rs`,
`tests/recording.rs`; documentation is in README.md, docs/architecture.md and this
ledger. No diagnostic tracing, raw capture, unsafe code, dependency, sharding,
commit or push is included in the production change.

### Coverage follow-up: fair admission, upstream missing books, shutdown race

The diagnostic cutoff's 23,910 missing desired instruments separates into
17,000 V1 never requested, 3,993 V1 and 2,886 V2 previously initialized but
unavailable, and 31 V1 requested without any initialized snapshot. The 44 Gamma
listings without token IDs are a separate discovery error: they were never
admitted as subscriptions. Exact histories and current API responses are under
`.amp/in/artifacts/coverage-gaps/`.

**Confirmed C2 bug:** 17 workers repeatedly canceled semaphore admission after
30 seconds, lost their FIFO position, and entered cold cooldowns without ever
starting a connection. Tokio's fair semaphore does not preserve canceled queue
positions. A bounded worker now retains one pending acquisition across diagnostic
ticks. Retirement and shutdown remain cancellable; a superseding mandatory
request cancels planned admission. Cold recovery starts at actual admission,
not local queuing. Concurrency, pacing, handshake/initialization deadlines,
financial validity and genuine failed-attempt escalation are unchanged.

The real-worker regression failed before the fix: a later waiter acquired the
released permit first, after the worker entered a 60-second cooldown. Afterwards,
the worker retained priority through 155 seconds of virtual time, and retirement
released its reservation without dialing. A second test holds local admission
for 155 virtual seconds, resumes real time, rejects a localhost handshake, and
observes the next attempt within two seconds instead of a 60-second cooldown.
Tokio `test-util` is enabled only as a dev feature; no dependency version changed.

**Independent upstream check:** a separate Node WebSocket client requested the
31 absent IDs plus one control. During 30 seconds it received the control book
and two PONGs but no snapshot for any of the 31. Public CLOB REST requests using
curl returned `404: No orderbook exists for the requested token id` for every
missing ID and 200 for the control. The first Python urllib probe received 403
for every request and is not used as evidence of book absence. Gamma returned
active/open/book-enabled records for 21 queried IDs and no result for ten.
These are current observations, not proof of historical or permanent absence.
Do not remove valid desired identities or create empty books from these errors.

**Uninstrumented 1,000-second full-universe attempt:** all 554 workers started.
At the exact pre-shutdown owner-ledger cutoff, desired was 552,148 and live was
548,140 (**99.274%**). V1 had 4,008 missing live identities; V2 had zero. Every
missing identity had been requested at least once: the never-requested category
fell from 17,000 to zero. Current requested coverage is distinct: 3,186 desired
V1 identities were not requested on a live session at cutoff. The remaining live
gaps were 3,975 previously initialized books on four recovering workers and 33
never initialized. The latter comprise 21 of the previously checked 404 IDs and
12 additional IDs, all also returning 404. Whole-session recovery still creates
collateral gaps for initialized books; this change does not implement isolated
per-instrument recovery or filter out missing books.

The run processed 14,377,942 messages with no ingress/output-capacity failure;
peak RSS was 4,682,924,032 bytes. The supplied reader decoded **28,033,578**
records; all **4,728,354** periodic snapshots matched independent absolute-delta
replay. This validates internal archive consistency, not raw-exchange history.
Live traffic and discovery changed between attempts, so the coverage difference
is not a controlled throughput/latency improvement.

**The run still exited 1:** shutdown raced with preflight of an empty desired
reconciliation. Begin revision 738 was accepted, but the manager rejected
Complete solely because shutdown was set. The footer honestly reports incomplete
intake. The follow-up manager fix checks shutdown after preflight/before Begin,
and closes an already fully delivered reconciliation even if shutdown arrives.
Interrupted partial delivery still fails. A deterministic public-manager test
stops at the existing per-chunk fairness yield: final-chunk completion failed
before the fix and passes afterwards, while an undelivered second chunk still
fails. This final shutdown change has not had another full-universe live run.

Evidence: `coverage-gaps/{admission-before,admission-after,transport-checks,
shutdown-before,shutdown-after,full-checks,final-checks}.log`, plus
`full-universe-admission-fixed-20261028/` for the manifest, binary hash, resource
samples, stderr, archive, sidecar, identity ledger and independent replay output.
Full-universe acceptance remains **failed**: missing desired coverage, discovery
uncertainty, the recorded shutdown failure, and a 44.138-second maximum health
revisit versus the provisional 10-second target remain explicit limitations.

After the shutdown fix, `cargo test --locked` and `cargo test --locked --release`
each passed **170 tests**, with opt-in live/load probes ignored. Formatting,
all-target checking, strict Clippy and the release executable build passed.
The final source changes remain local and uncommitted; all live services from
this follow-up have stopped. Isolating unavailable instruments from their
initialized socket neighbors remains a separate recovery-architecture decision.

## Dual-feed failure acceptance and health scheduling (2026-10-28)

Scope: correct failover, safe monitoring/recovery, then synthetic capacity and
local processing latency. No new architecture, financial representation, parser,
production exchange, live socket budget, or automatic latency-based switching.
Evidence directory: `.amp/in/artifacts/dual-feed-acceptance/`.

`tests/dual_feed.rs` drives real localhost WebSocket workers, bounded ingress,
C4, C5 and the supplied SBE reader. Six fixtures cover abrupt selected disconnect;
symbol silence despite observed PING/PONG; both-quiet negative control; delayed
equal-rate batches with regressing exchange times; a missing book on both feeds
beside initialized neighbors; and staggered fresh-generation renewal. Expected
quantities and record order are explicit and asymmetric, and raw ingress receipt
timestamps are compared with recorded delta timestamps. Health windows use an
explicit logical clock, not a claimed network latency. The harness bypasses the
manager/shared dial budget and drains its small output at the end: it is neither
a throughput test nor full-process failure isolation.

The first run had a fixture configuration error (replicas defaulted to one); its
six failures are not production evidence. After setting two replicas, five
cases passed and missing-book recovery failed: both sessions restarted and
suspended initialized A/B neighbors. The C4 repair guard now defers removing a
neighbor's last eligible redundant copy. Single-copy and already-invalid-source
recovery remain possible. Merely serializing exposed a second defect: slot zero
could repeatedly win when both X replicas remained missing. The strengthened
fixture reproduced that starvation. Due repairs now prefer the replica with
fewer granted attempts, breaking ties by slot. All six cases pass, including a
cold replacement blocking peer repair and restored A/B baselines allowing the
previously deferred peer to try, while X remains honestly unavailable.
See `failover-before-valid.log`, `fairness-before.log`, `fairness-after.log`.

The actual-owner replay at 550,000 books, one replica, 10,000 messages/s for 30s
reproduced a separate scheduling failure: maximum revisit **25.568909s**, health
lateness **15.874047s**. All 790,000 financial records still matched the oracle.
C4 bounded each slice to 1ms, but the owner waited another 100ms even with overdue
work. The owner now schedules another slice after 1ms when work remains, keeping
100ms idle polling and an ingress opportunity between slices. Identical replay
passed with revisit **10.102103s**, lateness **312.290ms**, and the same 790,000
matching records. This is a scheduling-contract repair, not a financial hot-path
speedup or proof of live universe coverage. `OWNER_CHECK_HEALTH=1` enforces ≤11s
revisit and ≤1s lateness. See `health-before.log` and `health-after.log`.

The capacity harness now accepts `OWNER_REPLICAS=2`: 550,000 instruments become
1,100,000 independent replicas across 1,100 synthetic sessions, with automatic
selection and monitoring enabled. Input rate counts physical deliveries;
alternating copies carry equivalent histories, with only the selected copy
recorded. Independent arithmetic replay verifies every selected delta and every
minute snapshot, not just record totals.

An initial 100,000-message/s, 70s dual trial passed with 7,000,000 accepted inputs,
3,900,000 matching output records and all 550,000 periodic snapshots. Its repeat
failed at 52.03s with `ingress queue full` after 5,200,551 accepted inputs; the
2,928,220-record accepted prefix matched, but missing Ended boundaries correctly
made completion unsuccessful. Both trials still used the harness's old 8,192-item
queue, not production's previously measured 65,536-item allowance. The harness
now references `DEFAULT_QUEUE_ITEMS` directly, retaining the 64 MiB byte cap, and
prints both limits. The failed smaller-queue control remains in
`dual-capacity-{1,2}.log`; it must not be counted as a passing qualification.

**Final production-limit runs:** `OWNER_BOOKS=550000 OWNER_REPLICAS=2
OWNER_RATE=100000 OWNER_SECONDS=70 OWNER_CHECK_HEALTH=1 cargo test --locked
--release --bin engineering-challenge production_owner_capacity -- --ignored
--nocapture` passed twice, serialized without concurrent builds or benchmarks.
Each processed 7,000,000 offered messages and independently matched 3,900,000
selected-stream records, including all 550,000 periodic snapshots. Neither had
rejection, false health suspicion, ingress exhaustion or output failure.

| Metric | Trial 1 | Trial 2 |
|---|---:|---:|
| Full synthetic initialization | 14.902s | 15.270s |
| Maximum health revisit | 10.111480s | 10.118199s |
| Maximum health lateness | 232.495ms | 275.413ms |
| Maximum ingress age | 90.466ms | 85.171ms |
| Maximum periodic-snapshot lateness | 17.997ms | 9.107ms |
| Maximum offered-message lateness | 116.210ms | 59.878ms |
| Maximum process RSS (including oracle) | 5,132,104 KiB | 4,956,060 KiB |

See `production-capacity-{1,2}.log` and `final-manifest.txt` (source hashes,
Rust 1.93/LLVM 21.1.8, x86-64 orb, 8 CPUs, approximately 16 GiB RAM). This
qualifies only the declared 0–8-level-per-side synthetic envelope, not maximum
sustainable capacity, startup at live socket speed, or 1,100 actual sockets.
The one-copy before/after health experiment retained identical 8,192-item queues;
do not attribute the separate production-limit capacity results solely to the
health scheduling change.

Final `cargo test --locked` and `cargo test --locked --release` each passed
176 tests (11 opt-in tests ignored). Formatting, all-target checking and strict
Clippy passed; Clippy's one new harness style warning was fixed, not suppressed.
Logs are `final-debug.log`, `final-release.log`, `fairness-after.log` and
`clippy-fixed.log`. No commit or push was made by this follow-up.

**Latest local latency:** three final release trials per mode with
`C5_SECONDS=30 C5_RATE=1000 C5_WORKLOAD=native-ids C5_OUTPUT=file`, running
`mixed_recording_matches_every_oracle_record` in `tests/recording_load.rs`.
Each uses 100 instruments, a two-second warmup, 30,000 measured messages and
224,108 independently matching SBE records. All six passed. Values below are
medians of the three per-run percentiles, in microseconds, not pooled percentiles.

| Mode / span | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| Ingress replay, processing including encoding | 8.755 | 24.230 | 29.676 | 50.024 |
| Local WebSocket, processing including encoding | 9.098 | 24.704 | 33.542 | 57.327 |
| Ingress replay, ingress queue residence | 42.607 | 90.198 | 113.748 | 251.779 |
| Local WebSocket, ingress queue residence | 42.643 | 93.884 | 122.230 | 240.829 |

Socket processing p99 was 33.542/34.861/33.357us across the three trials;
ingress replay p99 was 29.561/29.676/38.878us. All measured offered rates were
within 0.03 messages/s of 1,000. The two modes use different pacers; subtracting
their percentiles would not establish transport overhead. Queue and processing
percentiles cannot be added into an end-to-end percentile. These instrumented
spans exclude physical file visibility and exchange/network arrival latency;
30-second measurements do not include a minute boundary. The separate 70-second
capacity replay covers periodic snapshots. p99.9 remains exploratory with only
30,000 samples per run. No controlled before/after hot-path speedup is claimed.
See `final-latency-{ingress,socket}-{1,2,3}.log`; earlier `latency-*` runs preceded
the final repair-fairness change and are not the final figures.

**Remaining gates:** no full-universe live rerun, real two-replica universe, or
live paired-feed connection experiment ran in this follow-up. Existing live
coverage/discovery gaps remain unclosed; synthetic success cannot remove them.
The R7 local preflight below is now executed; live work still needs an explicitly
approved sample/cohort/resource budget as specified in `docs/latency-research.md`.
The current approved one-copy universe budget does not authorize approximately
1,108 live sockets. For scoping a small future V1
pilot only, the previously checked control asset again returned a public REST
book (54 bids, 4 asks); `pilot-control-book.json` preserves that response. It is
not a WebSocket readiness or activity guarantee.

### R7 probe preflight — local evidence, not live latency

The real loopback feed/probe/evidence path now covers handshake/initialization
deadlines, malformed/oversized/closed connections, PING/PONG, cancellation and
actual isolated-process SIGINT, requested source ports/conflicts, full ingress,
staggered renewal and incomplete-peer refusal. Output-fault fixtures reproduce
the old `BufWriter` destructor retry after partial failure. The writer now
disarms its buffer on error and flushes the raw prefix before terminal status;
final flush/sync or report failure must return an unsuccessful process. A final
sync failure can leave a success-looking raw marker: always check process exit.

Release load qualification: 1,000 batches/source × 20 unique changes/batch,
5ms absolute schedule, two sources, seven-second measurement. All 2,002 received
frames and 40,000 changes captured; zero resource resets. Generator maximum
lateness 2.917ms (250ms failure bound); maximum queue age 5.382ms, processing
5.364ms and timer lateness 1.902ms; 11,381,516 evidence bytes and 8,820 KiB maximum
RSS. This short one-asset fixture includes fake venue/report/verification in the
same process. It is not saturation, worst-case memory, or live latency evidence.

The independent Python Decimal + offline grouping audit verified all 20,000
pair decisions in that capture. A separate actual-socket fixture verified one
unique pair out of six observations, rejecting repeated/unmatched controls.
Audit mutation tests reject a one-scaled-unit discrepancy, wrong signed delay,
omitted/duplicate matches, missing fresh baselines, incomplete output and
unsupported reset/old-format captures. The checker validates full-content
matching, not exchange occurrence identity; it refuses captures over 256MiB or
200,000 canonical observations. Effective matcher timer advances are now logged
so quiet-window decisions can be independently reproduced.

Evidence: `probe-debug.log`, `probe-fixture.log`, `probe-fixture-audit.json`,
`probe-load.log`, `probe-load-audit.json`, and the two retained `raw.jsonl` files
under the same dual-feed acceptance directory. Implementation/tests are in
`src/bin/latency_probe.rs`, `tools/check_latency_probe.py` and
`tools/test_latency_probe_check.py`; protocol/limits are in
`docs/latency-research.md`. No collector hot-path change was made during this
probe preflight, so the collector capacity/latency results above remain the
latest measurements; no live connection-treatment improvement is claimed.

Final combined validation: formatting, locked all-target check and strict Clippy
passed; `cargo test --locked` and `cargo test --locked --release` each passed
191 tests with 12 opt-in cases ignored. The new load case was separately executed
as described above. Python discovery passed all 18 tool tests. See
`integrated-final-checks.log`, `python-checks-final.log` and
`probe-final-manifest.txt`. All changes remain local, uncommitted and unpushed.

## V1 live pilot and probe sample filtering (2026-10-28)

The preceding collector/probe baseline was subsequently committed and pushed as
`1b7e394` on `harness-latency`. The approved single-asset, two-socket baseline
pilot used 60s warmup plus 300s measurement, an eight-minute wall budget, 256MiB
raw cap and monitored 1GiB RSS ceiling. Both initial books arrived, but the probe
stopped at 32.872s with `unexpected change asset`, before measurement. Six frames
were retained, zero pairs matched and no resource reset occurred. Sampled peak
RSS was 5,828,608 bytes. No live latency or live collector-redundancy qualification
can be inferred. Evidence: `.amp/in/artifacts/live-v1-baseline-01/`.

The final frame contains the subscribed token and a second token under the same
market. `tests/fixtures/polymarket-probe-mixed-change.json` preserves the exact
received payload plus a final newline (verified against source 0, ordinal 5 in
`trial/raw.jsonl`). This is a real public V1 fixture, not a synthetic identity
replacement. Official market-channel documentation describes an array of
per-asset changes but does not establish exclusive single-token delivery; the
capture disproves the probe's assumption for this attempt.

The probe now validates every price-change identity/side/price/quantity/hash and
charges all entries to input bounds before excluding unrequested entries from
matching. It preserves full raw messages, requested entry order and repeated
quantities; requested changes still require a baseline. No extra instrument is
initialized/subscribed or added to the sample denominator. Unexpected snapshot
and tick assets remain terminal. The collector's separate known-unrequested vs
unknown-identity contract is unchanged. The independent Python auditor applies
the same sample definition with its own Decimal/grouping implementation; its tick
timestamp check now also matches the Rust contract.

The strengthened local two-socket fixture failed on the old code with the same
`unexpected change asset` error, then passed with differing excluded-token
quantities on the two feeds. Independent audit verified exactly one requested
pair from six requested observations, with the repeated/unmatched controls
unchanged and both excluded entries still in raw evidence. Regression checks
also cover the captured native-ID frame, interleaved repeated-price/delete
entries, outside-only input, missing requested initialization, invalid excluded
amounts/sides/identities and late malformed entries.

Validation: 31 probe cases passed in both debug and release (one opt-in load
case not rerun); all-target check, strict Clippy and 19 Python tool tests passed.
Release collector/dual-feed/recording tests and the real alternate-protocol
worker-to-reader case also passed. Logs: `.amp/in/artifacts/probe-scope-fix/`
(`before.log`, `audit-before.log`, `checks.log`, `python.log`, `after.log`,
`after-audit.json`, `acceptance.log`). The original live capture still fails audit
as incomplete; no replacement live attempt was run. This follow-up changes only
the research probe/auditor, tests and documentation; it is not a collector
latency optimization or production Kalshi implementation.

## Stage latency histograms, Phase 1 (2026-10-29)

Opt-in diagnostics answering "receive to stdout, where does the time go", not an
optimization. Off by default (`recording.latency_histograms`, or
`--latency-histograms BOOL`). Each stage has one owning thread, so no merging:

| Stage | Thread | Sample | Boundary |
|---|---|---|---|
| S1 | owner | data message | `queued_at - received_mono` |
| S2 | owner | data message | ingress enqueue to owner dequeue |
| S3 | owner | data message | dequeue to `process` + control provenance returned |
| S4 | owner | data-derived SBE record | receive to its batch's publication |
| S5 | writer | batch | publication to writer receipt (`max_batch_age` used creation) |
| S6 | writer | batch | writer receipt to all entries written / to next flush returned |
| S7 | writer | data-derived SBE record | receive to the first flush returning after its write |
| slices | owner | `capture_due` call with captures | duration, levels, snapshots |

Histograms are log-linear (16 sub-buckets per power of two, at most 6.25%
bucket width) in a fixed `[u64; 976]` allocated before intake. Each thread writes
one `{"kind":"control","event":{"kind":"latency",...}}` sidecar row per 60s
window plus a final `last:true` row, with dense bucket counts so windows can be
merged exactly. Periodic snapshot records carry no receive instant and are absent
from S4/S7. Owner rows are deferred, never fatal, when the output pool lacks space.
The writer's pending-flush instants are preallocated; overflow is counted as
`unmeasured_*`, not grown. Disabled cost: one branch per hook. `Entry` grew 16→32
bytes, so the default pool reports 69,206,016 instead of 68,157,440 bytes.

Tests: the bucket math is checked against an exact sorted-sample oracle (200
heavy-tailed cases). Off and on produce byte-identical SBE, with zero latency
rows when off, and the enabled rows are well-formed with counts tied to records.
The executable `books-budget` test enables the rows through the config file and
checks S1–S3 per message and S4/S7 equal to the 6 confirmed records. Other modes
must emit no rows.

Overhead: one release `recording_load` binary toggled by `C5_LATENCY`. The
harness cannot toggle it because flipping the code default breaks the
config-drift test, and the harness strips `C5_*`. Runs were 5k msg/s, 10s,
ingress/file, three alternating pairs of A/A (off/off) and A/B (off/on) per
workload, 50,000 samples per run, with no other process above 20% CPU. Median
pairwise change in processing-including-recorder p99:

| Workload | A/A pairs (%) | A/B pairs (%) | A/B median p99 / p50 / CPU |
|---|---|---|---|
| mixed | +13.6, −28.3, −9.8 | +5.8, −42.9, −0.9 | −0.9% / +3.8% / +3.8% |
| snapshot-burst | +5.4, +25.0, +8.1 | −6.7, +7.8, −10.1 | −6.7% / −2.1% / −4.0% |

The A/B effect is inside A/A noise. A possible ~0.2µs p50 cost for mixed cannot
be separated from noise. Evidence: `.amp/in/artifacts/latency-p1/overhead-quiet/`.

**Contaminated first attempt, retained:** `latency-p1/overhead/` ran while Amp's
change capture hashed the untracked 5.8GB `runs/r1` sidecar (`git hash-object`
at ~100% CPU plus disk writes). One burst-on run failed with
`output pool exhausted`; another showed queue p99 16ms. On the quiet host, all 24
campaign runs plus 5 extra burst-on repeats passed (queue p99 0.51–0.57ms). The
contention is a likely cause, **not a proven one**. It does show that a writer
stall exhausts the fixed pool, which is the existing terminal policy. Local
`.git/info/exclude` now ignores `runs/` and `collector.yaml`.

Phase 0 (HEAD `5269cf6`, smoke, 3 pairs, 1k/s, 5s, 5,000 samples per run; quiet
reruns `harness/p0-aa-{mixed,burst}-quiet/`, contaminated first runs retained).
Medians of run quantiles in µs (p50/p95/p99/p99.9/max; the harness reports p95,
not p90):

| Workload | processing incl. recorder | ingress age | encode callback |
|---|---|---|---|
| mixed | 13.0/30.0/37.9/63.5/109 | 52.9/116.9/158.1/340.7/589 | 0.1/1.0/1.9/4.3/36.5 |
| snapshot-burst | 14.2/105.4/646.8/1012.5/1196 | 54.4/124.8/216.7/610.2/1820 | 0.1/3.8/9.3/21.6/68.1 |

Both were `control_only`. A/A p99 pairs were +6.0/−36.4/−3.4% (mixed) and
−7.8/+28.4/−10.1% (burst), which sets the noise floor of 5-second screens. No live
run; Phase 2 awaits approval.

### Phase 2 live run (2026-10-29)

25-minute full-universe run on defaults plus `latency_histograms: true`. Exit 0
and `success=true`, with 35,141,365 records and peak RSS 2,496,856 KiB. No pool
exhaustion, so there is no H20 evidence. Steady state (11 windows): S7
receive→flush p50/p99/max 6.0/11.0/58.8 ms. S2 owner queue wait p99/max
2.4/130.5 ms. S5 publish→writer p99 0.26 ms. S6 received→flushed p99 5.8 ms.
`nr_throttled_written` stayed 0 and `nr_dirty` peaked at 77k of about 778k
pages. The ~100 ms tail is an owner-thread stall: S2/S4/S7 and `max_batch_age`
114.8 ms coincide, while the writer, disk and writer CPU are clear. The stalling
operation is not identified, because non-message owner work is unmeasured. The
per-minute depth sample is a hypothesis for the 24–40 ms per-window floor. Owner
CPU mean/max-10s was 20.6/33.2% of a core. Report and evidence:
`.amp/in/artifacts/live-latency-p2/REPORT.md`. `runs/r2/` keeps run.sbe and the
sidecar locally.

### Phase 3 owner-operation attribution (2026-10-29)

Added opt-in per-operation owner histograms (with max wall time) and capped
`s3_outlier` rows (≤1,000 per run) behind `latency_histograms`. Offline H28 (550k
books, 120 s) passed its oracle. The depth sample takes ~50 ms per minute, and a
`DesiredChunk` took 115 ms. Live r3 (25 min) attributes the steady ~50 ms S2 floor
to `DepthSummary::of`: it matches S2 max within ~1 ms in 6 of 11 steady windows.
The ramp spikes up to 112 ms match slow `process.desired_chunk`, whose chunk
times roughly double (5→117 ms), so container growth is hypothesised (H29). The
last-window ~100 ms spike is only partly attributed, with ~37 ms residual. r3
ended `success=false` (exit 1) because worker 77 failed a heartbeat write during
shutdown. Recording was clean with 38,338,474 confirmed records. Network
interruptions numbered 164 (r2: 4). Report:
`.amp/in/artifacts/live-latency-p3/REPORT.md`. No fixes applied.
