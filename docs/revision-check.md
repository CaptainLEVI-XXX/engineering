# Full-project revision checklist

**Created:** 2026-10-27. **Status:** open; not full-collector sign-off.

Use this after the separately approved components form a working collector,
before claiming production readiness or low latency. It records deferred checks,
not permission to implement later components now. Component 1 owns discovery;
Component 2 owns assignments and raw transport. Components 3/4 now own exact
decoding, replica reconstruction and bounded recovery. Component 5 now owns SBE
recording and selected-output integrity provenance; production qualification
and later extensions remain open.

Fix a demonstrated correctness failure when found; do not defer it just because
this checklist is scheduled for integration. Optimize only after the correctness
gates pass and the actual bottleneck is measured.

## R1–R8 integration ledger (implementation-first continuation)

The user requested implementation before the final test pass, then correctness
and latency on small data before optimization. All rows below distinguish code
from executed evidence; the earlier R0 checks do **not** validate these changes.
No additional universe campaign or live latency treatment is authorized here.

| Requirement | Implementation | Final local evidence | Live/capacity acceptance |
|---|---|---|---|
| R1 protocol seams | Neutral discovery commands, wire protocol and session decoder ownership, adapter-owned namespace | Combined debug/release pass; actual alternate-protocol local worker fixture | Existing Polymarket fixtures only |
| Extension 1 / R2–R3 | Independent slots, physical packing cap, desired fences, sticky snapshot-before-delta selection, bounded degraded recovery | Dual-slot executable deadline/SIGINT, lifecycle interleavings, failure/selection replay and SBE tests passed | Not qualified for observed universe; 1,116 projected sockets exceed 1,024 cap |
| Extension 2 / R4 | Fixed accepted-activity windows, indexed examinations, health suspicion, peer reservations, bounded planned renewal and shared connection leases | Logical-clock activity/policy tests and local connection-budget tests passed | Revisit/initialization/renewal service rates unqualified at universe scale; small results will not qualify 500k+ assets |
| R5 discovery hints | Optional payload flag, exact Gamma ID, two-namespace verification fallback, bounded dedup and coalesced reconciliation | Decoder, bounded hint and collector fixtures passed | Global notification delivery scope unobserved |
| Extension 3 / R6 | Exact signed-relative normalized operations, venue price bounds, optional timestamps, synthetic alternate protocol/sequence fixtures | Alternate worker→decoder→recorder→supplied reader replay passed, including sequence gap and new generation | No production Kalshi or second-live-exchange claim |
| Extension 4 / R7 | Bounded paired-feed probe, conservative canonical matching, renewal/port cohorts, uncertainty reporting; local stage attribution | All 15 matcher/output/CLI fixtures passed in debug and release | Approved live trials not run; no measured network-arrival improvement |
| R8 integrated acceptance | Existing failure campaign extended with fences, replicas, standby provenance and automatic policy | 160 default tests passed in each profile; fmt/check/strict Clippy passed; 100-cycle churn fixture passed | Full-universe two reconciliations + 30min steady interval, full-target health load and 30min soak deferred |

Executed `cargo test --locked` and `cargo test --locked --release`: each passed
160 tests, with nine opt-in tests ignored. `cargo check --locked --all-targets`,
`cargo clippy --locked --all-targets -- -D warnings`, and
`cargo fmt --all -- --check` also passed. These are local orb results, not CI.
The first debug run found duplicate invalidation after a global retirement fence
and late physical unsubscribe. The owner now ignores obsolete removal revisions;
the existing interleaving oracle passed without relaxing its expected history.
Strict Clippy found one collapsible conditional, fixed by Clippy and formatted.
Logs under `.amp/in/artifacts/`: `r1-r8-debug.log` (retained failure),
`r1-r8-debug-2.log`, `r1-r8-release-checks.log` (release pass and first lint failure),
`r1-r8-clippy-fix.log`, and `r1-r8-small-smoke-2.log` (clean static checks).

The first benchmark uses 100 varied-depth books and explicitly labeled synthetic
offered traffic. Local queue/owner/encoding/write-call timing is not network
latency, consumer visibility, durability or evidence of exchange-wide losslessness.
No optimization is accepted without repeated same-input measurements and the
same financial oracle. Larger load and live campaigns require a separate budget.

Remaining design limitation: generation-only all-ready acknowledgments become
ambiguous if assignments change after the worker marker. The worker conservatively
retains its lease until expiry/end. Marker-token acknowledgment can improve this
case later; it must not be presented as already measured full-ready throughput.
Two sockets in one process do not protect against process death. Independent
collectors/hosts and their separate archives remain optional, separately approved
hardening, not silently combined writers.

### Small-data results; longer trials stopped on request

On the same Linux orb/Rust 1.93.0 host, four release recording trials completed:
100 varied-depth books, 100 initialization messages, 2,000 warmup messages and
120,000 measured messages at 1,000/s for 120 seconds. Every trial compared all
854,312 decoded SBE records against the independent oracle, including 184 minute
snapshots, with zero mismatches and successful drain. These trials use one source
and default monitoring-off behavior; they do not measure dual-feed health cost.

| Path / trial | Achieved offered messages/s | Queue p99 (µs) | Processing including recorder p99 (µs) |
|---|---:|---:|---:|
| Direct ingress / 1 | 1,000.001 | 126.72 | 37.15 |
| Direct ingress / 2 | 999.997 | 129.52 | 38.19 |
| Direct ingress / 3 | 1,000.000 | 127.48 | 37.20 |
| Local WebSocket / 1 | 1,000.003 | 136.23 | 43.10 |

Commands: `C5_SECONDS=120 C5_RATE=1000 C5_MODE=ingress C5_OUTPUT=file cargo test
--locked --release --test recording_load mixed_recording_matches_every_oracle_record
-- --exact --ignored --nocapture`; repeat with `C5_MODE=socket`. Logs are
`.amp/in/artifacts/r1-r8-latency-{ingress-1,ingress-2,ingress-3,socket-1}.log`.
They retain p50/p95/p99/p99.9/max, sample counts, stage boundaries, output calls,
offer lag, drain and `/usr/bin/time -v` data. No builds or heavy agents ran during
these trials. Reference checking and timing instrumentation remain workload costs.
This is a diagnostic baseline, not an optimization comparison or maximum capacity.

The user then stopped the time-consuming work. Socket trial 2 was interrupted
and has **no accepted result**; trial 3 never ran. Its partial log remains as
`r1-r8-latency-socket-2.log`. No full-universe or live latency campaign started.

The separate accelerated-clock health test passed with 100 instruments / 200
replicas, 16,000 accepted entries and 8,000 independently checked selected entries.
Health state measured 288 bytes/replica; maximum revisit was 10s and scheduling
lateness 100ms. The first harness skipped all startup ticks for one logical second
and reported 1.1s lateness. Servicing startup at the same declared 100ms cadence
as steady state removed that artificial stall; the test now also asserts the 1s
lateness limit. No production scheduler change was made. This is not a constant-
ingress or universe-scale qualification. Final command:
`cargo test --locked --release --test health_load -- --ignored --nocapture`.
Log `r1-r8-final-small-check.log` also records final fmt and strict Clippy success.

## R0 orb baseline (2026-10-27)

Checkout: `component-2-ingestion`, starting at
`b76a70f0d6013daa9cff392a0ce895e783378f26`, not either repository's main.
The transferred `6.md` governs stages R0–R8; its original contents and the
user's deletion of `3&4.md` are preserved. This is R0 evidence work, not
redundant-feed enablement or challenge sign-off.

Host: Linux x86_64 orb, eight vCPUs, approximately 16 GiB RAM and 60 GiB disk;
Intel Xeon 2.60 GHz. `.agents/setup` installs stable Rust 1.93.0 and the locked
dependencies. No nightly features, custom allocator or compiler flags were
added. Mac measurements below are historical, not comparative speedups.

The unchanged Linux baseline passed all 73 release tests but failed one debug
test: elapsed silence did not establish that an unsubscribe write was pending.
The correction is test-only. Real WebSockets over an eight-byte duplex transport
now explicitly reach `Poll::Pending` before shutdown, peer-drain and timeout
controls. Removing the cancellation branch in a temporary copy failed the
250 ms shutdown assertion; both controls still passed. The loopback test retains
accepted-prefix, session, timeout and drain assertions, accepting either exact
ordinary-shutdown or interrupted-unsubscribe-shutdown reason. Production worker
behavior was not changed.

Combined verification passed **84 tests in each of debug and release**, plus
formatting, locked all-target check and strict all-target Clippy. Ignored live
and performance tests are not included in that count. Logs:
`.amp/in/artifacts/r0-baseline.log` and `r0-local-checks.log`.

| R0 contract | Local evidence | Limits |
|---|---|---|
| Identity-level coverage | `tests/collector.rs` distinguishes desired A/B/C, requested A/B/X and live A/X; identifies missing C/unexpected X, retirement of B and 29/30-second initialization deadline boundary | Independently sampled owner views are not an atomic full-universe ledger; R2 supplies the ordered desired fence |
| Existing owner state | Borrowed `Assignments::current`, `Processor::requested` and `Processor::coverage` expose exact identities, admission revisions, input cutoffs and initialization status without cloning books | A successful request is not acknowledgement; selected affinity alone is not live eligibility |
| Recording replay | Existing recording oracle now compares complete selected records and unavailable boundaries through source loss, recovery, manual selection, deletion and retirement | Automatic failover remains R3; synthetic replay does not recover missing exchange history |
| Discovery evidence | Capped create-new journal retains decisions, exact failed/expired/overflow identities, all sweep reports and final desired/pending state; short-write, cap and flush faults are tested | Evidence completion and coverage acceptance are separate; expiry does not resolve an identity |
| Benchmark accounting | Initialization, warmup, measured offered messages and drain are separate; sample membership follows trace ordinals | No synchronous-versus-dedicated topology qualification or speedup claim |
| Physical visibility | Separate pipe consumer timestamps record-end and complete-chunk byte offsets; supplied reader round-trips the snapshot | Consumer-local timing, not receive-to-output latency; one diagnostic sample |

### Current discovery fails strict coverage; two copies exceed the socket cap

The bounded public run completed in 556,365 ms: V1 2,754 pages / 275,308
records, V2 33 pages / 3,277 records. It observed **556,974 desired assets**
(550,420 V1; 6,554 V2), not the historical 484,704. Both traversals completed
without listing errors or conflicts. Strict exit status was **1** because 44 V1
markets lacked token IDs. All 44 exact identities/reasons remain in
`.amp/in/artifacts/r0-discovery-run/unresolved.json` and the original journal.
The six targeted requests did not resolve them; all 44 jobs expired. Final
pending=0 therefore does **not** mean no unresolved identities. No anonymous
errors, overflow, failed verification batches or known-market uncertainty were
observed. Stdout was zero bytes; the complete journal was 242,095,659 bytes.

Command: `target/release/discovery --seconds 1770 --once --strict --evidence
NEW_FILE --evidence-max-bytes 536870912`, under an outer 1,800-second watchdog.
The evidence file has a terminal completion marker and successful evidence
status, while discovery coverage remains unaccepted. Pagination is not an
atomic exchange snapshot and says nothing about unobserved historical updates.

The offline `project_current_universe_capacity` test feeds that final snapshot
through the actual initial assignment algorithm. At 1,000 assets/socket:

| Adapter | Assets | Workers per slot | Final worker load | Two-slot workers |
|---|---:|---:|---:|---:|
| V1 | 550,420 | 551 | 420 | 1,102 |
| V2 | 6,554 | 7 | 554 | 14 |
| Total | 556,974 | 558 | — | **1,116** |

Two independent copies would require **1,113,948 replicas** and exceed the
configured 1,024 physical-socket cap by 92. The 10,000,000-level aggregate
budget permits only **8.98 levels/replica before replacement construction**.
The initial-packing projection does not include later churn/fragmentation.
No full-universe redundancy was enabled or approved.

Resource worksheet, not an admission certificate:

- Replica structs alone: 294,082,272 bytes, excluding heap books, identities,
  maps, retained tombstones and allocator overhead. Full-universe depth and
  replacement-burst distributions remain unmeasured.
- Each socket permits a 2 MiB reassembled message and a 256 KiB write buffer;
  multiplication by 1,116 is a potential envelope, not resident/preallocated
  memory. TCP/TLS/read buffers and tasks add further memory.
- Existing ingress accounting: 64 MiB; fixed C5 pool: 68,157,440 bytes, plus
  output buffers and symbol retention. Health state is not implemented yet.
- This orb had approximately 56 GiB disk free before probes. No universe-scale
  RSS, all-book-ready latency, backlog age, sustainable service rate or minute
  snapshot completion time has been established. Raising caps alone would not
  qualify this host.

### Synthetic output baseline, not topology qualification

Sequential release runs used 100 books, 100 initialization messages, 2,000
warmup messages and 120,000 measured messages over 120 seconds at 1,000/s.
Each compared **854,312 complete decoded records**, including 184 generated
minute snapshots; both output reports had no error and drained successfully.

| Mode / destination | Measured offered/s | Ingress age p50 / p99 / p99.9 (µs) | Processing p50 / p99 / p99.9 (µs) | Drain (ms) | Peak RSS (KiB) |
|---|---:|---|---|---:|---:|
| Direct ingress / file | 999.999 | 58.5 / 154.8 / 266.2 | 12.5 / 46.2 / 73.8 | 39.6 | 134,680 |
| Loopback socket / draining pipe | 1,000.006 | 62.5 / 155.5 / 295.2 | 12.9 / 43.4 / 78.9 | 63.8 | 135,224 |

Both wrote three 64 MiB chunks (201,326,592 physical bytes). User+system CPU
was 24.80 s and 31.45 s respectively; maximum snapshot lateness was 1,160 µs
and 1,121 µs. Processing includes encoding and test instrumentation; producer
lag reached 7,287/8,749 µs. No builds, other benchmarks or CPU-heavy agents ran
concurrently; the public discovery service was active, so these are diagnostic
shared-host runs, not isolated comparative latency trials. Logs:
`r0-recording-ingress.log`, `r0-recording-socket.log` under `.amp/in/artifacts/`.

The separate-consumer visibility probe observed the 84-byte snapshot record at
4.083 ms and the complete 64 MiB chunk at 260.913 ms from consumer startup,
using a diagnostic 200 ms chunk-age setting. The supplied reader recovered
exactly one snapshot. These timestamps are not producer-to-consumer latency.
Log: `r0-visibility.log`.

The three-trial synchronous-versus-dedicated comparison, throttled/paused/stalled
matrix and representative population/depth measurements remain open. These
baseline trials neither select a topology nor authorize full-universe capture.

## Component 2 evidence and limits (historical baseline)

- Unsubscribe work now returns to the receive/control loop after each bounded
  batch, as additions already did. Pending removals are deduplicated, and additions
  wait behind them. Removing the last assignment drains pending unsubscribe
  requests before closing, unless shutdown, failure or the existing session-rotation
  policy requires an earlier cutoff.
- The regression in `tests/websocket.rs` injects ordered messages during 511
  single-asset removals. Before the fix, reception waited for all 511 batches;
  the first fixed run received them after two. This is a scheduling regression,
  **not a latency SLA or representative production benchmark**.
- A separate local TCP test stops reading unsubscribe requests while continuing
  to send numbered messages. It checks actual write backpressure, ordered accepted
  data, session termination on timeout, and cancellation on shutdown. Its oversized
  synthetic assignment and restricted receive window are fault injection, not
  exchange capacity evidence.
- **A single awaited write still prevents receive polling until it completes,
  times out, or is cancelled.** Returning between batches does not fix that.
  Application-receive timestamps and queue age exclude time waiting unread in
  socket/TLS buffers. Unread messages at a failed session's end are not recovered.
- Keep the existing queue/byte accounting, payload ownership, timestamp type,
  and provisional 2 MiB message limit until measurements justify a change.
  Accounted ingress bytes are not a total process-memory ceiling.
- The Component 2 executable validated and discarded raw messages. It did not persist
  history, reconstruct books, or emit SBE. See [protocol.md](protocol.md) for
  historical live-probe and load-test evidence and its limitations.

Local validation on 2026-10-27, macOS ARM64 / Rust 1.93.0-nightly: formatting,
locked all-target check, strict Clippy and all 35 default tests passed in debug
and release. The opt-in release raw-load test preserved 100,000 messages in order
and drained in 2.502728 seconds (39,956 messages/s). Queue-age p50/p95/p99/p99.9/max
were 6/206/960/8,294/9,985 µs; maximum generator lag was 13,707 µs. This is a single
loopback run, not proof of a performance improvement. Live universe/size coverage
was not rerun. Local log: `.amp/in/artifacts/component-2-revision-checks.log`.

## C3/C4 execution evidence and remaining sign-off gaps

**2026-10-27, local uncommitted implementation based on `95b45c7`.** C1 is
unchanged. C2 changes carry revisions, authoritative retirement cutoffs and
bounded session-targeted recovery. `src/market_data/` owns exact decoding;
`src/orderbook/` owns the single processing owner and independent BTreeMap
replicas. The executable defaults to book validation, retains transport-only
mode, and still emits no SBE or persisted history.

| Plan milestone | Executed evidence | Remaining limitation |
|---|---|---|
| 1. Protocol contracts | Retained genuine V1/V2 snapshots with provenance/checksums in protocol.md; current bounded TLS probe | Current V1 handshake failed HTTP 525; no genuine delta captured for either version. Numeric/schema and recovery limits remain provisional at full-universe scale. |
| 2. Exact bounded decoding | Six decoder tests: decimal boundaries, escapes, timestamp overflow and supplied-reader range, array-tail validation, bounded arrays, unknown/ambiguous identities and genuine snapshots | Not every exchange message variant is observed live; unused metadata is not strictly schema-validated. |
| 3. Atomic reconstruction | Six replay tests; 3,000 generated valid events against an independent linear integer oracle, comparing every intermediate selected update and both book sides after each event; malformed batches, duplicates, zero deletion, replacement and construction/intermediate budgets | No alternative book structure has been established as faster. |
| 4. Lifecycle/recovery | Injected-clock attempt/deadline tests; cross-worker revision races; retirement while disconnected; real local socket recovery and obsolete-generation restart; full/closed recovery controls | Default policies require universe-scale calibration; per-asset same-socket resynchronization is deliberately not used. |
| 5. Replicas/selection | Divergent replica state, non-selected updates, identical-book switch boundaries, stale/uninitialized selection rejection, provenance/input cursors and terminal sink failures | Redundant live socket provisioning and automatic failover are later work. |
| 6. Integration/drain | All 53 deterministic tests passed in debug and release, including preserved C1/C2 tests; real local HTTP/WS executable cases with overlapping adapter IDs, deadline and SIGINT in both modes; empty stdout | This verifies reconstruction and accepted-input behavior, not persistence, exchange history or full desired-universe initialization. |
| 7. Measurements | Raw socket load plus paced two-source C3/C4 replay; repeated release runs with CPU/RSS and queue/completion/processing tails | No allocation profile (`xcrun xctrace` unavailable), representative recorded delta workload, TLS book-load run, saturation search, or full-system recovery/churn performance result. Not low-latency sign-off. |

Fresh commands passed: `cargo fmt --all -- --check`,
`cargo check --locked --all-targets`, `cargo clippy --locked --all-targets -- -D warnings`,
`cargo test --locked`, `cargo test --locked --release`, and
`cargo test --locked --release --test ingestion_load -- --ignored --nocapture --test-threads=1`.
The large-discovery benchmark and 120-second live-universe test were not rerun.
Logs: `.amp/in/artifacts/c34-{debug-final,release-final,load-final}.log`.

### Reproducible bounded workload, not maximum capacity

Apple M2, macOS 25.6.0 ARM64; Rust 1.93.0-nightly (`cc3eee7fb`, LLVM 21.1.5),
ordinary Cargo release profile, no custom compiler/allocator flags. Workload:
100,000 messages / 30,344,212 payload bytes, 32 instruments, two independent
sources (64 replicas), bid depth 1–16 at each replacement, four ordered repeated
absolute changes/message between snapshots. Every 2,000 messages both sources
receive full 32-book replacement bursts. Offer schedule: 40 messages/ms,
independent of consumer acknowledgements. Expected totals: 3,200 snapshots,
399,600 applied changes, 199,800 selected changes; every selected change is
checked and final quantities are checked independently for both replicas.

The replay injects at ingress, **not through TLS/WebSockets**. Completion time
starts at a synthetic admission timestamp; it excludes actual socket buffering.
Processing time includes decode, preparation, commit and checking sink assertions.
Snapshot/input construction occurs before pacing; the producer still clones
payloads. Samples and test assertions add memory/CPU overhead. This is not an
isolated parser/book microbenchmark or receive-to-SBE measurement.

Workload source SHA-256 (`tests/ingestion_load.rs`):
`f292f470fecd5f84b00166d11b1710eb99e30a1d3be1cc05fcdedf7ef2efd0f5`.
Cargo.lock SHA-256:
`8e2511a814e11f02f856dc223be9e863fd59ed49146cd19d51616bda8bbc630c`.
The source completely specifies the generated workload; genuine fixture hashes
are separately recorded in protocol.md.

After building, three sequential direct runs of the release test binary under
`/usr/bin/time -l` produced the following. Each run verified all expected records;
percentiles are from 100,000 message samples, in µs.

| Run | Achieved messages/s | Completion p50 / p95 / p99 / p99.9 / max | Processing p50 / p99 / p99.9 | Max producer lag | User+system CPU | Peak RSS bytes |
|---|---:|---|---|---:|---|---:|
| 1 | 39,851 | 127 / 2,343 / 9,234 / 14,277 / 14,487 | 5 / 15 / 138 | 15,747 | 0.64 + 0.05 s | 14,516,224 |
| 2 | 39,836 | 160 / 4,952 / 8,916 / 10,181 / 10,678 | 5 / 16 / 177 | 7,356 | 0.70 + 0.05 s | 13,484,032 |
| 3 | 39,798 | 159 / 51,411 / 83,591 / 94,970 / 98,929 | 4 / 19 / 256 | 42,779 | 0.60 + 0.05 s | 14,417,920 |

Monotonic queue p50/p95/p99/p99.9/max respectively:
run 1: 122/2330/9225/14274/14484;
run 2: 153/4945/8909/10172/10671;
run 3: 152/51404/83576/94965/98921 µs.
Logs: `.amp/in/artifacts/c34-load-repeat-{1,2,3}.log`.
All runs drained in approximately 2.51 seconds, but **the tail variability is
unacceptable evidence for a predictable low-latency claim**. This shared laptop
experiment does not identify the cause. Do not discard run 3 or infer improvement
from the earlier single baseline. Scratch reuse and direct normalized-entry
construction remove redundant work; their isolated performance benefit is not
established by these noisy runs.

The separate raw loopback run preserved 100,000 messages at 39,947 messages/s;
queue p50/p95/p99/p99.9/max: 5/47/659/9810/11451 µs, producer lag 18037 µs.
It is a different workload and must not be used as a decoder speed comparison.

### Mixed workload and stage breakdown — 2026-10-27

An isolated copy of the current uncommitted C3/C4 sources was instrumented;
**production Rust sources were not changed**. Same M2/toolchain/lockfile as above;
`sw_vers` reports macOS 26.6.2 (25.6.0 above is the Darwin kernel version).
One workload was run twice: stage timers enabled, then disabled in the same binary.
The disabled run still contains the instrumentation fields/branches and benchmark
wrapper; it is not an untouched-production-binary control. No custom build flags.

Synthetic mix, not yet calibrated against recorded Polymarket deltas:

- 256 instruments in two adapter namespaces, 512 independent replicas, four
  logical sources; exact 78-character IDs. Single producer, single consumer.
- Both sides: replacement bid depths 8/32/72/256, ask depths 5/17/37/129;
  1/4/16-entry absolute-update batches, multiple assets, repeated levels and zeros.
- 170,184 delta messages; 11,932 single snapshots; 2,048 eight-snapshot arrays
  (including 48 reconnect arrays); 13,880 non-book trades; 2,004 tick events.
- 200,048 measured messages, 245,394,352 payload bytes, three reconnects and a
  clean four-session drain. Another 64 initialization messages precede timing.
  Message sizes p50/p99/max: 220/46,001/46,001 bytes.
- Independent offered schedule: ten messages/ms for about 20 seconds; no consumer
  acknowledgements during pacing. Ingress budget: 8,192 items and 64 MiB accounted
  bytes. A finite payload pool is constructed before timing, then Utf8Bytes cloned.

Both runs matched **every selected level and full replacement snapshot** against
generated numeric expectations, and all 512 final replica books against a separate
linear-ladder oracle. Totals including initialization: 528,804 applied changes,
28,828 snapshots, 264,402 selected changes, 14,478 selected snapshots and 896
source invalidations. No rejected events, enqueue failures or unfinished drain.

Stage timings below are per complete data message, summing events within arrays.
All durations are monotonic elapsed time, **not CPU time**; descheduling is included.
Book processing includes resolution, prevalidation, mutation and snapshot replacement,
less that same message's checking-sink time. Residual time includes the benchmark
wrapper and instrumentation. No subtraction of independently computed percentiles.

| Instrumented stage | Mean µs | p50 µs | p99 µs | p99.9 µs |
|---|---:|---:|---:|---:|
| Enqueue/accounting | 0.575 | 0.208 | 7.167 | 12.167 |
| Queue wait | 218.692 | 41.084 | 3,974.334 | 11,901.916 |
| Envelope validation | 3.384 | 0.750 | 71.125 | 161.625 |
| Typed decoding/normalization | 8.081 | 1.375 | 185.749 | 412.290 |
| Book processing, excluding sink | 4.534 | 1.458 | 59.459 | 189.500 |
| Checking sink | 0.264 | 0 | 2.877 | 15.082 |
| Other processing | 0.452 | 0.335 | 1.331 | 3.333 |
| Whole processing | 16.715 | 4.167 | 319.166 | 709.042 |
| Queue timestamp → processing completion | 235.406 | 47.958 | 4,069.959 | 11,917.916 |

| Run | Completed messages/s | Completion p50 / p99 / p99.9 / max µs | Maximum offer lag µs | Sampled peak queued items | User + system CPU seconds | Peak process RSS bytes |
|---|---:|---|---:|---:|---|---:|
| Stage timers enabled | 10,000.16 | 47.958 / 4,069.959 / 11,917.916 / 18,787.625 | 16,410.625 | 210 | 3.68 + 0.43 | 72,761,344 |
| Stage timers disabled | 10,000.16 | 49.375 / 3,797.833 / 16,242.833 / 38,191.958 | 34,672.125 | 379 | 3.83 + 0.47 | 70,615,040 |

Parsing accounts for approximately 69% of instrumented processing elapsed time;
book processing accounts for 27%. Eight-snapshot arrays have processing p50/p99
555/1,105 µs, versus 4.167/39.667 µs for deltas, so large replacements are a
concrete head-of-line workload to investigate. The timing-disabled run still has
millisecond tails; these two noisy runs cannot isolate instrumentation overhead
or establish predictable latency. Do not infer maximum capacity from the achieved
offered rate, or compare this heavier mix with the earlier 40k/s replay as a regression.

CPU/RSS include generator construction, the independent oracle, samples and checks;
RSS is not collector-only memory or allocation profiling. Completion begins at the
ingress queue timestamp, **after permit acquisition**, not socket receive. No network,
TLS, discovery churn, recovery-timer scan, SBE serialization, output visibility or
automatic failover is measured. Preparation and mutation are not separately profiled.
Keep BTreeMap and existing queues; next isolate parsing/replacement costs and OS
scheduling on a controlled host before selecting an optimization.

Logs: `.amp/in/artifacts/c34-mixed-{breakdown,control}.log`. Reproducer:
`.amp/in/artifacts/c34-mixed-reproducer.tar.gz` contains the exact instrumented
source, deterministic generator, manifest, lockfile and schemas. Extract into a
scratch directory, run `cargo build --locked --release --example c34_mixed`, then
`BENCH_BREAKDOWN=1 /usr/bin/time -l target/release/examples/c34_mixed`; repeat with
`BENCH_BREAKDOWN` unset for the timing-disabled control. Artifacts are local and
gitignored. Generator SHA-256:
`3774c4d419744d878520fc776fd41b38e13c9cac6ec2ef8003699f97cc26c523`.

### Review follow-up: diagnostic drain and retirement cutoff

The book consumer now performs no diagnostic I/O. It counts every invalidation
observation and retains only the first four diagnostic samples, with asset/reason
previews capped at 128 characters. The main thread prints its bounded report after
joining the consumer; processing errors retain the same counters/samples as error
context. Omitted diagnostic detail is counted explicitly. Library integrity
observations, replica state transitions and recovery behavior are unchanged.
This is a final diagnostic summary, not live monitoring or a persistent gap archive.

- **Reproduced before the fix:** a subprocess requested 50,000 books on one source,
  initialized a separate healthy source, then accepted a source-end burst followed
  by a healthy-source update and shutdown. With stderr unread, the old consumer
  stalled partway through invalidation and exceeded the 10-second test deadline.
  Local evidence: `.amp/in/artifacts/diagnostics-before.log`.
- **After:** the regression drains both data messages with 50,001 invalidation
  observations, one applied/selected healthy-source change, bounded diagnostic
  size and zero stderr writes by the book consumer. The final test also uses a
  long Unicode reason to exercise preview bounds. It passed debug and release;
  this is fault injection, not a throughput or latency-SLA benchmark.
- **Retirement:** 111 valid interleavings of old-worker data, manager retirement,
  delayed unsubscribe/end and new-worker readmission verify the exact selected
  history and final revision. Independently injected scratch mutations that ignore
  `Retired` or allow obsolete removes each fail this test. Logs:
  `.amp/in/artifacts/retirement-{cutoff,revision}-mutation.log`.
- **Real local sockets:** a manager/worker/processor test verifies retirement is
  observed before the successful unsubscribe notification, preserves pre-cutoff
  updates, ignores delayed retired-asset changes, and still applies a healthy peer's
  change in the same batch without reconnecting. This proves the collector's ingress
  cutoff, not exchange acknowledgement or universal chronological order.
- All 56 default tests passed in debug and release; all-target check and strict
  Clippy passed. Existing deadline/SIGINT executable tests still pass. Live probes,
  full-universe memory/timer measurements and the mixed-load benchmark were not rerun.

**Remaining limitation:** other control-plane/transport stderr writes remain
synchronous. This change prevents book-owner I/O stalls; it does not guarantee
process exit against a permanently blocked stderr or eliminate CPU cost of a large
invalidation. Revisit process-wide diagnostic delivery alongside monitoring.
Also retain the unresolved recovery-blast-radius, unknown-asset policy,
50k/100k/500k timer-scan and repeated retirement/readmission memory measurements.

### Revisit after completing the collector and extensions

- [ ] Capture genuine V1/V2 changes and event variants; replay raw fixtures through
  reconstruction and the supplied SBE reader. Do not call synthetic deltas live evidence.
- [ ] Calibrate message/entry/level/identity budgets and 30s/120s/three-attempt
  recovery policy against the full desired universe and reconnect snapshot bursts.
- [ ] Profile allocation counts/bytes and separate parser, preparation and mutation
  costs on a suitable profiling host. Measure retained scratch after an unusually
  large batch, not only its logical length. Check the O(retained replicas) recovery
  scan (every 100 ms) and synchronous diagnostic I/O at universe scale.
- [ ] Repeat independently paced load over longer runs with representative depths,
  actual TLS sockets, slow output, discovery churn, failures and recovery bursts.
  Diagnose tails and find sustainable capacity instead of raising the offered rate
  without accounting for scheduling lag or accumulating backlog.
- [x] Implement C5: namespaced exact external symbols, source-switch/periodic snapshots,
  timestamps, chunk framing, short writes, broken pipe, SIGINT drain and tail flush.
  Broader performance/coverage qualification remains open below.
- [ ] Finish Extension 1: provision independent live sources and test policy-driven
  failover. Never combine their deltas into one mutable replica.
- [ ] Finish Extension 2: activity windows, quiet-versus-unavailable distinctions,
  staggered rotations and subscription probes. Repeated PONGs are not book coverage.
- [ ] Verify Extension 3 at integrated boundaries; keep venue subscription/recovery
  semantics adapter-owned. Kalshi implementation remains separately authorized.
- [ ] Finish Extension 4: controlled connection experiments with defensible event
  matching, ambiguous/unmatched accounting and results in Markdown. Application
  receive deltas are not absolute network latency.

## Correctness and coverage gates — before performance tuning

### Component 5 local evidence — 2026-10-27

The default executable now records SBE stdout plus a mandatory create-new JSONL
sidecar. Local HTTP/WebSocket tests replay overlapping V1/V2 IDs, initialization,
repeated updates/deletes, deadline shutdown and SIGINT through the supplied reader.
They also exercise broken/unread stdout with unread stderr and require a bounded
nonzero exit without a successful footer. Library tests cover selected versus
standby gaps, selection/periodic provenance, timestamp regression, fixed-pool
exhaustion, temporary writer pauses, encoding failure, partial writes, WriteZero,
sidecar errors and output panic. Exact-clock C4 tests cover deadline fairness,
replacement/retirement cancellation, whole-interval failure and cutoff accounting.
Schema, generated codecs and supplied reader are unchanged.

**A long run found a real batching weakness.** The initial 120-second replay
failed closed at 60.12s when the age policy exhausted batch count around minute
chunk completion. A deterministic two-batch test reproduced the capacity waste
with three tiny records and a paused writer. The revised age policy retains the
last batch while all others are outstanding, appending exact records within the
same memory/descriptor limits. Size limits still force publication/fail closed;
the 5ms age target is not guaranteed while output is behind. No memory budget was
raised and no update was dropped/coalesced to make the benchmark pass.

Release results on Apple M2 / aarch64 macOS 25.6.0, Rust
`1.93.0-nightly (cc3eee7fb 2025-11-28)`, locked dependencies:

| Synthetic path | Direct ingress → file | Local unencrypted WS → file |
|---|---:|---:|
| Offered duration + warmup | 120s + 2s | 120s + 2s |
| Offered rate | 1,000 messages/s | 1,000 messages/s |
| Received / exact records compared | 122,100 / 854,312 | 122,100 / 854,312 |
| Periodic snapshots | 184 | 184 |
| Ingress age p99 / p99.9 | 1.447 / 10.362 ms | 0.318 / 9.815 ms |
| Decode/book/encode p99 / p99.9 | 63.500 / 264.708 µs | 62.875 / 180.416 µs |
| Encode callback p99.9 | 4.625 µs | 4.542 µs |
| Maximum snapshot lateness | 0.894 ms | 1.385 ms |
| Maximum batch creation → dequeue | 82.436 ms | 117.695 ms |
| Maximum generator scheduling lag | 60.456 ms | 35.485 ms |
| Physical output / complete chunks | 192 MiB / 3 | 192 MiB / 3 |

Both runs matched **every** output record in order against an independent linear
book oracle, not just the final state. Workload: 100 books, unequal initial depths,
1/4/16-entry updates, deletes/no-ops, eight-snapshot replacement bursts and minute
captures. Post-warmup samples: 120,000 ingress ages, 120,001 processing durations
and 840,130 encoding callbacks per run. The oracle/generator and sampling impose
additional work; processing excludes oracle construction but includes callback
instrumentation. Archive timestamps were not changed for instrumentation.

Reproduce with `cargo test --locked --release --test recording_load -- --ignored
--nocapture`; set `C5_MODE=socket` for local WebSockets. Raw logs are in
`.amp/in/artifacts/c5-{ingress,socket}-120s-revised.log`; the initial failed run is
preserved as `c5-ingress-120s.log`. The older logs call creation-to-dequeue age
`max_queue_age_us`; this has been renamed `max_batch_age_us` to avoid implying a
pure queue-wait measurement. Pool capacity is 68,157,440 bytes (payload plus
descriptors); it is not RSS. `/usr/bin/time -l` around Cargo includes compilation
and test/reference readers, so its peak is not a collector-only memory result.

Additional output-consumer checks used the same independent record oracle:

- Draining pipe (`C5_SECONDS=3 C5_OUTPUT=pipe`, plus 2s warmup): 5,100 messages,
  35,102 matching records, one complete 64 MiB chunk. This short run tests routing
  and drain, not minute cadence or sustained capacity.
- Throttled pipe (`C5_RATE=100 C5_OUTPUT=throttled`, 120s plus 2s warmup):
  12,300 messages, 85,685 matching records, 184 periodic snapshots and three
  complete chunks. Processing p99/p99.9 was 91.833/745.875 µs; ingress age
  p99/p99.9 was 1.294/9.686 ms. Maximum snapshot lateness was 8.823 ms and
  maximum batch creation-to-dequeue age was **1.530 seconds**. Correct output
  under this consumer does not establish low-latency delivery. Post-warmup
  samples: 12,000 ingress ages, 12,001 processing durations, 84,103 encode
  callbacks. Generator lag reached 41.713 ms. Measured process peak RSS was
  142,000,128 bytes, including the test/oracle/readers, not collector-only memory.

Logs: `.amp/in/artifacts/c5-pipe-short.log` and `c5-throttled-120s.log`.
These runs predate only the subsequent failure-accounting correction: an
upstream-only error now marks `input_complete=false` without inventing a C5
`failure_input` from the last successful observation. That failure path has its
own regression test; the successful encoding/output path is unchanged.

Final local verification after that correction: `cargo fmt --all -- --check`,
`cargo check --locked --all-targets`, `cargo test --locked`,
`cargo test --locked --release`, and
`cargo clippy --locked --all-targets -- -D warnings` all passed. Each default
test suite passed 73 tests with zero failures; five opt-in tests remain ignored
by those commands. Recording benchmarks were run separately as described above.
Full test logs are `c5-debug-tests-final.log` and `c5-release-tests-final.log`
under `.amp/in/artifacts/`. This is local verification, not CI or deployment.

**Still open:** representative live TLS/full-universe recording; direct-output
versus dedicated-writer comparison; repeated saturation curves; actual consumer
visibility percentiles (successful writes are not acknowledgements); allocation
counts and multi-day retained-memory behavior. Short local runs do not certify
full-universe capacity, complete exchange history, or sub-millisecond delivery.
Use `C5_OUTPUT=pipe` for a draining `cat` subprocess; `C5_OUTPUT=throttled` requires
Python 3 and sleeps 1ms after each ≤64KiB pipe read. These are synthetic consumers,
not disk-durability tests. Preserve failed overload trials rather than raising
budgets or calling an unsustained offered rate achieved throughput.

| Status | Revisit | Required evidence / exit condition |
|---|---|---|
| [ ] | Complete desired universe | Complete current V1/V2 scans; distinguish known desired, successfully requested, exchange-confirmed where supported, and book-initialized counts. Account for every unresolved identity and failed traversal. Do not manufacture missing IDs or turn a partial scan into successful coverage. |
| [ ] | Discovery-to-subscription delay | Measure new-market and retirement delays during a full scan and targeted verification. Evaluate lifecycle hints against short-lived markets; `custom_feature_enabled` also enables other event types, so setting the flag alone is not integration. Resolve adapter namespaces explicitly; retain REST reconciliation and one registry. |
| [ ] | Wire limits and snapshot sizes | Record complete-message and frame-size distributions by adapter, subscription batch and market depth, including initial/reconnect bursts. Recheck the 2 MiB limit, subscription budget and 256 KiB write buffer against observed maxima plus justified headroom. Test boundary-sized and fragmented messages; do not silently truncate or blindly adopt library defaults. |
| [ ] | Exact parsing and book reconstruction | Replay recorded and adversarial fixtures against an independent reference model: checked fixed-point decimals, absolute sizes, zero deletion, replacement snapshots, whole-event validation, asymmetric sides and multi-asset batches. Assert exact state and emitted updates, not just counts. |
| [ ] | Source/session validity | Replay disconnects, remove/re-add, delayed old-generation messages and malformed input. A new source needs a validated snapshot before publishing deltas. Preserve accepted per-source order; expose uncertain history. A snapshot restores state, not missing events. |
| [ ] | SBE archive contract | Round-trip binary stdout through the supplied reader. Verify IDs/mapping persistence, units, sides, timestamp conversion, chunk boundaries, large snapshots, minute snapshots, short writes and tail flush. Reconstruct from the archive and compare with the independent oracle. |
| [ ] | Overload and final drain | Stall and fail each downstream stage, exhaust message and byte budgets, break the output pipe, and signal SIGINT during reads/writes/reconnects/discovery. Count accepted, processed, emitted and unfinished work independently. Preserve the accepted prefix or explicitly fail its drain; never claim flush proves durability. |

## Latency and resource gates — measure the complete collector

| Status | Revisit | Experiment and decision rule |
|---|---|---|
| [ ] | Receive stalls during control writes | Offer numbered/timestamped traffic while slowing subscription writes, at normal and burst churn. Measure peer-send-to-receive as well as receive-to-processing/output. Set an explicit acceptable stall budget. Compare smaller batches/write deadlines with concurrent read/write designs only if needed; preserve single ownership, ordered subscription context, uncertain-send recovery and bounded shutdown. |
| [ ] | Full-pipeline capacity and tails | Replay representative recorded traffic with TLS where relevant, multiple sockets, discovery churn, reconnect snapshots and slow stdout. Use an independently paced producer; report scheduled, actually sent, accepted, processed and emitted counts/rates, generator lag and drain time. Report p50/p95/p99/p99.9/max with sample counts and repeated runs. Do not hide delayed delivery behind fast queue-age numbers. |
| [ ] | Queue/handoff cost | Profile Crossbeam enqueue, semaphore acquisition/release, reference counting and consumer wakeups. Compare alternatives only with equivalent byte/count limits and failure semantics. Benchmark worker-local decode/books against an extra handoff when those components exist. Keep the simpler design absent a reproducible gain. |
| [ ] | CPU, allocations and memory | Measure CPU per stage, allocations/bytes per message, peak RSS and queue occupancy during sustained load and snapshot bursts. Include TLS/WebSocket buffers, backing payload allocations, retained state and consumer-held envelopes. Size budgets from the deployment memory limit and measured burst behavior. |
| [ ] | Timestamp fidelity | Separate exchange timestamp, complete-message application receive time, queue time and output visibility. Use monotonic clocks for local durations; state clock-sync uncertainty for cross-host measurements. Profile clock/conversion cost before replacing types. Never call these NIC timestamps. |
| [ ] | Assignment and registry cost | Measure count-balanced socket loads against bytes/s, event rate, depth and processing cost. Profile notification coalescing, full snapshot cloning and reconciliation during large sweeps. Consider incremental registry APIs or rebalancing only if necessary; any migration must preserve source validity boundaries. |
| [ ] | Rust/parser/book choices | Profile typed parsing, buffer reuse and safe baseline book structures on the same replay oracle. Compare `BTreeMap`, sorted vectors or other ladders using actual level/update distributions and tick changes. Test compiler/profile changes independently. No speculative `unsafe`, custom allocator, SIMD, faster hasher or CPU pinning without attributable benefit and correctness evidence. |

## Evidence required to close a row

Record the commit, toolchain, dependency lockfile, OS/CPU, build profile, runtime
configuration, fixture provenance/checksum and exact reproduction command.
Attach baseline and candidate results on identical inputs, counts/oracle checks,
resource measurements, uncertainty and the decision (keep/change/defer).
An unchecked item is unresolved; a deferred item needs a reason and revisit trigger.
No benchmark establishes exchange-wide losslessness without an exchange replay or
sequence guarantee. Keep accepted-input correctness, observed-stream capture and
unverifiable exchange history separate in the final assessment.

## Starting points and references to recheck

- [Challenge](https://voluntas.io/engineering-challenge), supplied `sbe_schemas/`,
  and [repository engineering rules](../AGENTS.md): scope and output contract.
- [Architecture](architecture.md), [protocol/evidence](protocol.md),
  [engineering reference](engineering-reference.md) and [research](research.md):
  existing decisions and hypotheses, not substitutes for new integrated evidence.
- [Polymarket market channel](https://docs.polymarket.com/api-reference/wss/market)
  and [discovery guide](https://docs.polymarket.com/market-data/discover-markets):
  recheck current wire behavior, lifecycle options and version coverage against
  dated real fixtures. Protocol limits must not come from unrelated APIs.
- [Tungstenite 0.28 WebSocket configuration](https://docs.rs/tungstenite/0.28.0/tungstenite/protocol/struct.WebSocketConfig.html)
  and [Tokio select](https://docs.rs/tokio/latest/tokio/macro.select.html): inspect
  the lockfile-matched implementations for buffering, cancellation and scheduling
  assumptions. Library defaults are not validated collector capacity settings.
- `tests/websocket.rs`, `tests/collector.rs`, `tests/ingestion_load.rs`:
  current transport regressions, executable integration and opt-in raw load test.
  Extend the evidence when downstream components are authorized; do not relabel
  these transport-only tests as end-to-end collector validation.
