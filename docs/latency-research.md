# R7 — bounded paired-feed latency research

## Status and authorization

**Implemented; local socket/failure preflight and independent capture audit now
executed.** The probe has 31 regular cases and one opt-in local load case. The
first approved V1 baseline attempt stopped after 32.9 seconds, before measurement:
a price-change batch included both the requested token and an unrequested token
under the same market. Both sockets had initialized, but the probe's strict
membership check rejected the batch. The sample-filter correction below is
locally verified; no fresh live attempt has run with it.
There are **no valid live arrival-latency results** or evidence of a reliable
improvement. R7's live-research acceptance gate remains open. Local synthetic
pipeline measurements belong in `docs/revision-check.md`, not this live gate.

The implementation is isolated in `src/bin/latency_probe.rs`. It does not change
the collector, discover instruments, reconstruct/publish books, select sources,
or write SBE. It uses the R1 wire protocol API and the shared exact decimal and
envelope parsers; it deliberately does not depend on the changing normalized
`Change` representation. Rust tests are beside the binary; independent Python
audit tests live beside their checker in `tools/`.

R0's reported 556,974 desired assets, 44 unresolved V1 IDs, and two-copy capacity
exceeding 1,024 sockets are **not authorization for a universe run**. This tool
accepts only an explicit 1–100 asset sample, one adapter namespace, and two feeds.
Sampling is research, not full-universe coverage.

Before any live run, obtain explicit approval for:

- exact adapter and opaque asset IDs, host/region and destination;
- cohort, trial IDs, treatment assignment, warmup and measured duration;
- at most two simultaneous sockets and one connection start per 30 seconds;
- evidence/memory limits, sufficient disk headroom, other collectors sharing the
  public IP, and stop criteria below;
- whether the controlled single-source coverage gap during renewal is permitted.

The `--approve-live` flag is a deliberate operator gate, **not a substitute for
that approval**. No arguments fail closed before networking. `--help` does not
connect. There is no automatic campaign, retry, discovery or duration extension.

## Predeclared cohorts

| Cohort | Implemented treatment | Control and limitations |
|---|---|---|
| `baseline` | Same assets, endpoint, TLS and subscription on A and B | Both feeds run concurrently during measurement. Handshakes start at least 30s apart to respect the connection budget; this is not simultaneous connection establishment. |
| `renewal` | Close exactly one source, drop its old TCP connection, connect a fresh generation and resubscribe once | Stable peer remains running. The owner must grant renewal after observing all initial snapshots on both feeds. New source needs fresh snapshots within 30s. No same-session remove/re-add or automatic failback. |
| `source-port` | Bind one nonprivileged local port before TCP connect; other source binds port 0 | Records requested/actual local endpoint, DNS results and remote endpoint. NAT can rewrite ports; different DNS destinations confound port attribution. No port scanning or connection spam. |

Trial 1 treats A, trial 2 B, trial 3 A, etc. The first-created connection is always
A, so alternate treatment across trials rather than choosing the observed fast
connection. Baseline has no treatment even though the parity label is retained.
The renewal schedule is fixed at `run origin + 60s + warmup + measured_duration/2`,
not chosen after viewing latency. Actual start/cutoff and full-initialization
times are retained. This need not be the exact midpoint of measured time.
Renewal trials require at least 120 measured seconds.

Optional maintenance-window cohorts are **not implemented**: they require an
actual documented venue maintenance schedule and separate approval. No schedule
or maintenance behavior is inferred from Kalshi or another Polymarket API.

Candidate research minimum is **three 10-minute paired trials per treatment**,
with a fixed cohort and 60s warmup after both feeds have complete initial
snapshots. Repeat on an independent time/cohort before promoting a production
change. Extending a trial or adding trials when samples are inadequate requires
approval; short exploratory runs are allowed but cannot satisfy this minimum.

### Predeclared sample adequacy

Before inspecting live timing, require zero process errors, complete initial
coverage, no matcher resource resets, a successful independent capture audit,
and at least 80% matched change observations **on each source for each asset**.
Require at least 1,000 change pairs per asset and three completed nonempty 30s
blocks for a descriptive central estimate. These are conservative operational
screening thresholds, not a proof of unbiased sampling or statistical confidence.
Include guard-censored observations in the denominator; report inadequate and
quiet strata rather than silently removing them or extending the run. The
100,000-match p99.9 threshold and repeated-trial requirements still apply.

Inspect matched fraction and ambiguity/censoring before comparing treatments.
Even 80% coverage can selectively omit delayed observations; a material change
in missingness between cohorts makes a timing improvement inconclusive. The
offline checker verifies pair decisions, not these per-stratum campaign gates;
apply the gates to the report's per-source/asset counts and retain all failures.

### Command template — do not execute without approval

After the final implementation/testing gate, build the binary in release mode.
An approved operator can substitute real values into this template (the IDs
below are placeholders, not a proposed live cohort):

```sh
target/release/latency_probe \
  --approve-live \
  --adapter v2 --assets 'APPROVED_ASSET_A,APPROVED_ASSET_B' \
  --destination '.amp/in/artifacts/approved-latency-baseline-01' \
  --cohort baseline --trial 1 \
  --region 'APPROVED_HOST_REGION' --revision 'EXACT_SOURCE_REVISION_AND_DIRTY_STATE' \
  --toolchain 'EXACT_RUSTC_VERSION' \
  --warmup 60 --seconds 600 \
  --raw-bytes 268435456 --raw-observations 1000000 \
  --matcher-entries 100000 --matcher-bytes 67108864 --window-ms 5000
```

For a separately approved port trial use `--cohort source-port --local-port N`;
for a renewal trial use `--cohort renewal`. Never reuse an output directory.
Stdout remains empty; diagnostics go to stderr. The parent directory must exist.
Source revision/dirty state, toolchain and region labels are operator-supplied,
not independently verified by the probe. Preserve the exact source tree and
lockfile with a campaign when the revision is not a clean reproducible commit.

## Canonical matching contract

1. Timestamp a complete WebSocket delivery with `Instant` and `SystemTime`
   immediately after `socket.next()` resolves, before classification/queueing.
   One monotonic origin is shared by both feed futures. Wall time is provenance,
   not the subtraction clock; clock jumps cannot reverse the reported sign.
2. Retain the original complete text as a JSON string before decoding. JSON
   escapes round-trip original whitespace and batching. Validate the whole
   message before changing the probe's initialization state. Price-change
   batches may include tokens outside the requested sample: validate their
   identities, sides, prices, quantities and hashes, and count all entries
   against input limits, then exclude unrequested entries from canonical
   matching. They remain in raw evidence. Do not expand the sample or require
   unrequested snapshots; sample denominators include requested observations only.
3. Canonical content is exact asset ID, event kind, exchange timestamp converted
   from digit-string milliseconds to checked reader-compatible microseconds,
   ordered levels/entries and every available book/event/entry hash. V1 and V2
   are separate runs, never a collapsed identity namespace.
4. Price/size use the shared checked decimal parser at scale 1,000,000: no floats,
   exponents, signs, nonzero excess precision or rounding. Prices must be 0–1;
   quantities must fit nonnegative i64. Duplicate snapshot prices are invalid.
   Zero/no-op quantities remain canonical observations. BUY/SELL and order are
   preserved. Missing times are not replaced by receive time.
5. Per-asset groups preserve entry order within each price-change event. Different
   assets can reorder independently. Object versus array envelopes and whitespace
   do not alter identity. Repacking the same entries into different *financial
   events* is not assumed equivalent: event boundaries remain meaningful.
6. Initial snapshots (per generation), later replacement snapshots and ordinary
   changes are distinct strata. Trades, BBO, lifecycle, ticks and PONGs are not
   L2 change matches; raw evidence retains them. Ticks validate their relevant
   fields but do not count as progress. Unexpected snapshot/tick assets,
   requested changes before their baseline, malformed financial fields (including
   excluded change entries) and unknown event types still stop the trial.

The matcher uses a conservative **three-block guard window**, not eager pairing:

- Default block/candidate span is 5s. Retain leading, middle and trailing blocks
  (at most 15s of canonical history by timestamp, plus bounded scheduling delay).
  Only decide the middle after its trailing guard is complete. All three blocks
  share the one entry/byte budget; guards do not triple that budget.
- Require exactly one copy from each source across all three blocks, full
  canonical equality, and both candidates in the middle block. The hash map
  accelerates lookup but equality verifies all content. A repeated hash alone
  never establishes event identity.
- Repeated equal content is ambiguous even when occurrence counts agree.
  Candidate order inversions invalidate that asset's candidate set for the
  block. Source order is never repaired by sorting exchange timestamps.
- A missing counterpart is unmatched; a counterpart across a block edge is
  censored. The initial guard and undecided trailing blocks are censored.
  Edges deliberately lose usable samples and may bias the retained distribution
  toward short delays; report matched fractions and censorship, not just tails.
- Entry/byte pressure censors **the entire retained undecided window**, records
  a resource reset and starts a full new leading guard. No eviction makes an
  old repeated key newly unique. A renewal also censors the retained window.
  Already decided observations remain only as guard evidence, never counted twice.
- Each match identifies source generation, raw message ordinal, per-message
  canonical event index, asset, kind, both monotonic arrival times and signed
  `B_receive − A_receive` nanoseconds. Positive means A arrived first.

This is **heuristic observed-content matching** without a documented exchange
event ID/sequence guarantee. Even unique full content does not prove that two
observations are the same underlying exchange occurrence. Missing matches are
not a proven packet-loss count. Replacement state cannot restore missing history.

## Bounds, failure and output contract

| Resource | Default / hard bound | Exhaustion policy |
|---|---|---|
| Explicit assets | 1–100, each <=256 bytes | Reject CLI; no discovery or silent sampling |
| Physical sockets | 2; initial starts >=30s apart; one new start/30s globally within process | Wait for shared start budget; no retries |
| Connect / control send | 15s / 5s | Stop incomplete; no uncertain-write retry |
| Complete snapshot coverage | 30s from each connection attempt | Stop incomplete, stable quiet books are not presumed stale |
| Measured duration | 60–3600s; renewal >=120s | One bounded trial; total deadline 90s + warmup + measurement |
| Warmup | 30–300s, default 60s | Starts after both initial asset sets are complete |
| Message / frame | 2MiB each | Tungstenite rejects oversize; incomplete trial |
| Input handoff | 8 entries, each at most one bounded message or small control | `try_send` full/closed is terminal; no overwrite/coalescing |
| Socket write buffer | 256KiB each | Terminal failure |
| Matcher | 100k observations, 64MiB charged canonical storage; hard 100k / 128MiB | Whole-window censor/reset, explicit reset row and counts |
| Matcher scratch | O(100k entries), shared full-content reference index | Bounded by retained entry count, not included in canonical-byte charge |
| Match samples | 200k pairs with bounded IDs | Stop incomplete; no reservoir or silent tail truncation |
| Raw evidence | 256MiB / 1m received frames; hard 2GiB / 5m | Stop incomplete; reserve 16KiB for terminal status |
| Report | 2MiB | Stop incomplete |
| Local stall | >1s input processing, queue age or timer lateness | Stop when control returns; not a hard filesystem cancellation deadline |

Canonical parsing temporaries are bounded by the 2MiB input, 4,096 canonical
events and 65,535 accepted entries per complete message. Typed temporary vectors
are allocated before the entry-count validation; their transient size remains
input-bounded, not bounded by the matcher byte charge. The canonical-byte charge
is not RSS: allocator overhead, bounded input/wire buffers, parsing temporaries,
matcher scratch, retained pair IDs and report vectors add memory. Qualify actual
RSS and service rates before increasing a sample/budget. Counters fit u64 under
these hard observation/event bounds.

Raw data and reports are separate **create-new** files in a new directory.
`raw.jsonl` contains metadata, sends, complete received frames, connection
attempts/endpoints, readiness, renewal cutoff, effective timer-driven matcher
advances, matching/resource diagnostics and terminal status. Financial raw bytes
are written before matching. An I/O error poisons further evidence appends and
disarms the buffered writer's destructor: it must not implicitly retry a failed
prefix. Finalization flushes the prefix before appending terminal status. A
possibly partial line is never presented as a successful complete capture. A
missing terminal record is incomplete.
Caps and failed output can prevent retaining the final delivered frame/prefix;
the process fails and reports undrained input rather than claiming full capture.
Terminal status separates handled observations from successfully captured raw
observations. The report remains provisional until finalization and zero exit;
a terminal record alone cannot certify a later flush/sync succeeded.

The 64KiB buffered file is flushed and `sync_all` is requested at completion;
buffer visibility is delayed, and neither a successful flush nor this report is
a crash-durability proof. Blocking filesystem calls run on the same research
runtime and can perturb receive timing; a permanently blocked syscall cannot
be interrupted by its timer. This tool is not a production latency benchmark.

On a deadline, SIGINT, error or resource failure, both feed futures are dropped,
establishing intake cutoff and closing their TCP streams. Accepted bounded input
is drained where possible, undecided observations are censored, and finalization
errors are returned. Final shutdown does **not** verify the peer close handshake.
Renewal sends a bounded close before dropping the old stream. Ordinary disconnect,
TLS, HTTP/throttling or control errors stop rather than retrying; there is no
rate-limit evasion, authentication, TLS verification bypass, or market mutation.

## Reporting and interpretation

The generated `report.md` provides per-trial, per-asset and event-kind counts,
matched fractions (all measured observations in the denominator), signed
p50/p95/p99/p99.9/min/max and sample sizes. Snapshot distributions are not pooled
with change distributions. p99.9 estimates below **100,000 unambiguous matches
per stratum** are explicitly exploratory; exceeding that threshold is necessary,
not sufficient, for a credible tail result.

It also reports 30s change-only block distributions and a fixed-seed,
1,000-resample bootstrap interval for the **median of completed nonempty block
medians**, requiring at least three blocks. This is not a p99.9 confidence
interval. Independence/stationarity across 30s blocks may fail; changing asset
mix, renewal transitions and matched-only selection can bias it. Empty blocks
are not samples. The campaign must report each trial and its blocks, investigate
correlation and negative/failed trials, and compare pre/post treatment periods
against the stable peer. Automated cross-trial aggregation and tail-confidence
inference are not implemented.

Metadata includes actual DNS answers/selected remote, requested and actual local
port, default verified rustls/WebPKI+SNI policy, TCP_NODELAY, host/CPU/OS/architecture,
region/revision/toolchain labels, load at beginning/end, clocks, configuration,
generation/cutoffs and trial parity. Negotiated TLS cipher is not captured.
The collector already uses `disable_nagle=true`; this is not a new optimization.

Arrival differences are separate from receive-to-probe-consumer queue age, probe
processing/evidence time, timer lateness and reconnection-to-complete-initialization
time. They are neither NIC timestamps nor receive-minus-exchange-time network
latency. The tool does not measure collector end-to-end/SBE physical visibility,
selection hysteresis or a useful latency-switching policy. R3 remains sticky.

## Executed local preflight and remaining plan items

The final implementation-first pass executed these checks successfully (the
probe tests ran inside the full debug/release suites):

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked
cargo test --locked --release
cargo clippy --locked --all-targets -- -D warnings
```

Passing cases cover delayed/signed arrivals despite wall-clock direction,
repetition ambiguity, same timestamps/distinct quantities, repeated hashes,
independent-asset reorder versus same-asset inversions, missing events, boundary
censoring, ambiguity after expiry/pressure, exact decimal boundaries,
whitespace/array batching, snapshot kinds and entry order, atomic malformed
message rejection, create-new evidence limits, approval/cohort validation,
full/closed control handoff, expired-window rejection and uncertainty labels.
Logs: `.amp/in/artifacts/r1-r8-debug-2.log`, `r1-r8-release-checks.log`
and `r1-r8-small-smoke-2.log`. These tests do not exercise real paired feeds.

The subsequent preflight exercised real loopback TCP/WebSockets through the
actual feed, probe, evidence writer and finalization paths. Private endpoint and
timing injection leaves production CLI/TLS/timing limits unchanged. Cases cover
PING/PONG, abrupt EOF/close, malformed/oversized messages, initialization and
handshake deadlines, cancellation, actual Linux SIGINT in an isolated process,
source-port binding/conflict, bounded full ingress, renewal with both treatment
parities, and refusal to renew without complete peer initialization. Renewal
must close the old stream before replacement and require a fresh baseline.

Fault-injected output covers initial/partial writes, final flush/sync, failed
report output and a >1s evidence stall. The legacy buffered-writer reproducer
shows a destructor retry after a failed partial flush; the corrected writer
disarms that destructor and its regressions observe no retry. A sync failure
after writing terminal status still returns failure: the terminal row alone is
not a successful-process or durability certificate. Blocking filesystem calls
remain non-cancellable until control returns.

`tools/check_latency_probe.py` independently parses raw evidence using Python
Decimal and offline sorting/grouping over full canonical content, rather than
the Rust online hash/window implementation. It verifies exact financial keys,
source/generation/order, pair decisions and signed arrival differences. Timer
watermarks make quiet-window decisions reproducible. Deliberately corrupted
amounts, signed differences, omitted/duplicate matches, missing baselines,
incomplete captures and unsupported reset/old-format captures fail its tests.
The auditor is bounded to 256MiB input, 200,000 canonical observations and
100,000 windows; it is not a validator for arbitrary larger probe configurations.
It refuses resource resets because the old reset row lacks an exact event index.
Its `process_exit_verified: false` is intentional: retain and check zero exit
and final sync separately. It does not validate exchange event identity.

Two release fixtures were retained in
`.amp/in/artifacts/dual-feed-acceptance/`:

- `probe-fixture/raw.jsonl`: actual controls, one unique pair, repeated content
  and an unmatched observation. Independent audit found exactly one pair out of
  six measured canonical observations (matched fraction 1/3, intentionally
  inadequate for live timing).
- `probe-load/raw.jsonl`: 1,000 batches/source, 20 unique changes/batch, scheduled
  every 5ms (4,000 changes/s/source). Seven-second measurement, about five seconds
  of offered load, one asset, two loopback sources. All 2,002 received frames and
  40,000 changes were retained; the independent checker verified all 20,000
  pairs. Zero resource resets, ambiguity, unmatched or censored observations in
  this deliberately unique middle-window workload. Raw evidence: 11,381,516
  bytes; maximum process RSS: 8,820 KiB; generator lateness: 2.917ms; maximum
  queue age: 5.382ms; processing: 5.364ms; timer lateness: 1.902ms. The fixture
  fails if offered-load lateness exceeds 250ms; observed max was below it.

Commands: `PROBE_TEST_EVIDENCE_DIR=<new-absolute-directory> cargo test --locked
--release --bin latency_probe local_orchestration_matches_controls --
--test-threads=1`; for load, run the compiled release test executable alone under
`/usr/bin/time -v`, setting `PROBE_TEST_LOAD_EVIDENCE_DIR=<new-directory>` and
passing `--exact tests::local_bounded_load_retains_every_offered_event --ignored
--nocapture --test-threads=1`. Audit either with `python3 -B
tools/check_latency_probe.py <directory>/raw.jsonl`. Logs are `probe-debug.log`,
`probe-fixture.log`, `probe-fixture-audit.json`, `probe-load.log`, and
`probe-load-audit.json`.

Final combined debug/release suites each passed 191 tests (12 opt-in cases
ignored); format, all-target check and strict Clippy passed. All 18 Python tool
tests passed. See `integrated-final-checks.log`, `python-checks-final.log` and
`probe-final-manifest.txt` in the same directory. The opt-in probe load above
ran separately, without concurrent builds or other measurements.

These are synthetic correctness/tool-service measurements, **not live arrival
latency, maximum capacity or qualification of worst-case 100-asset input**.
The fake venue shares the process/runtime; RSS includes generation, report and
streaming evidence verification. Starts/measurement/windows are accelerated,
with zero warmup; production timing text in fixture reports is explicitly
overridden by their synthetic header. This removes the local-preflight blocker
only for proposing a small live sample. The probe still cannot prove freshness
or diagnose silently stale initialized subscriptions. Quiet feeds can yield no
useful samples.

`tests/recording_load.rs` now measures local receive/enqueue, queue, owner,
recorder callback and destination-call spans separately, with explicit oracle
and instrumentation costs. No local-loopback result can satisfy R7's live
objective. Approved live campaigns, cross-trial
analysis, independent replication, hysteresis/switch-cost measurements, and an
actual evidence-backed Markdown conclusion remain unperformed. **“No reliable
improvement” is a valid eventual outcome; parameter support is not an experiment.**

Protocol basis: repository `docs/protocol.md`, market WebSocket semantics and
wire-interpretation sections, and `6.md` R7 (official documentation previously
rechecked 2026-10-27). No new external lookup or live fixture was required here.
