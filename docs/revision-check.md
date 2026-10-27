# Full-project revision checklist

**Created:** 2026-10-27. **Status:** open; not full-collector sign-off.

Use this after the separately approved components form a working collector,
before claiming production readiness or low latency. It records deferred checks,
not permission to implement later components now. Component 1 owns discovery;
Component 2 owns assignments and raw transport. Components 3/4 now own exact
decoding, replica reconstruction and bounded recovery. SBE output remains C5 work.

Fix a demonstrated correctness failure when found; do not defer it just because
this checklist is scheduled for integration. Optimize only after the correctness
gates pass and the actual bottleneck is measured.

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
- [ ] Finish C5: namespaced exact external symbols, source-switch/periodic snapshots,
  timestamps, chunk framing, short writes, broken pipe, SIGINT drain and tail flush.
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
