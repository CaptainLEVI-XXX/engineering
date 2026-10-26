# Engineering reference — retained and corrected technical practices

This file preserves the useful detailed engineering practices from the user's **2026-10-25 harness**, relocated from the always-on agent instructions. It is reference material, **not a command to implement every technique**. Consult only the sections relevant to the measured problem and the current development stage. Newer official docs / code fixtures supersede stale observations.

## Repository map and baseline observations

The original harness reported (on 2026-10-25; **not revalidated by this document-generation process**): `src/main.rs` was a synthetic snapshot/update round trip; `src/side.rs` used `Side::Buy = 0` (bid) and `Side::Sell = 1` (ask); `src/util/dtos.rs` contained domain types and SBE encoding; `src/util/aligned_sbe_writer.rs` wrote complete messages in zero-padded fixed chunks; `src/util/aligned_sbe_reader.rs` and `orderbook_schema.rs` were the decoding oracle; `src/util/build.rs` generated YAML → XML → Rust via `ergo-sbe` into `OUT_DIR`; the SBE schemas lived in `sbe_schemas/engineering_challenge/challenge_orderbook.yaml` and `sbe_schemas/includes/common-types.xml`. The original author observed passing example/strict Clippy but **zero tests**, successful stable 1.90 release round trip, and Rust edition 2024. These are dated claims to check in the current checkout, not permanent truths.

**Do not edit generated codecs, schema files, or generators to make an incorrect application encoder appear correct.** Establish the exact output contract before choosing an adapter.

## Financial precision and exact semantics

- Template SBE prices/quantities: signed 64-bit, 1,000,000 fixed-point scale. Parse decimal text **without `f32`/`f64` intermediates**. Specify accepted grammar, sign/range, overflow, supported fractional precision; trailing zeros may be exactly representable even if longer than six characters. Reject excess nonzero precision; never round/truncate/saturate/wrap silently.
- Malformed or unrepresentable book-affecting messages are integrity failures. Do not skip one and claim an unaffected complete history. Consider fault isolation at the affected book/source versus process scope and make policy explicit.
- Introduce newtypes for price, quantity, exchange time and monotonic durations where they enforce a real invariant; avoid wrapping every primitive for aesthetics.
- Snapshot replacement, absolute quantity update, zero deletion, empty books, asymmetric bid/ask cases, multi-asset batches, and tick changes need reproducible fixtures.
- Distinguish observed **numerical accuracy**, **valid current reconstructed state**, **observed-stream capture**, and **exchange-wide historical completeness**. Current snapshot correctness does not imply gap-free history.

## Timestamp discipline

- Stamp immediately after a **complete** WebSocket message reaches the application and before parse/queue; this is an *application-receive* clock, not NIC/kernel hardware capture. Items in one frame/batch may legitimately share receive time.
- Preserve exchange event time separately. The author's original mapping used local receive time for update `backtest_timestamp` and received-snapshot `received_at`, while locally generated snapshots use coherent capture time for `received_at` and latest applied exchange event time for `exchange_timestamp`. **Verify exact field semantics with the current template schema before implementing.** Received vs generated snapshot origins can be indistinguishable in the required SBE format; document this and optionally maintain external provenance.
- `Instant` for latency/timeouts; wall clocks can adjust. Don't rewrite timestamps to manufacture monotonicity. Milliseconds → microseconds increases unit resolution but not source precision. Receive-minus-exchange isn't pure one-way network delay because of unsynchronized clocks and venue overhead.
- Separate receive→parse, parse→apply, queue wait, encode and OS-write stages; also measure **output flush/visibility delay**, not just producer return time. Histogram percentiles need counts and representative offered loads.

## Book ownership, recovery and ordering

- One owner per source/book. Model uninitialized/live/stale/resynchronizing/retired explicitly; never publish a partial startup book as a full snapshot.
- Same-connection snapshot then ordered deltas is baseline. A REST snapshot concurrent with WebSocket updates cannot be merged by comparing timestamps unless the exchange documents a correct boundary.
- Any replacement/resynchronization/failover must be replay-visible (usually a fresh SBE snapshot); periodic snapshots don't justify silent mid-minute replacements. Take snapshots coherently inside book ownership/order, not from a concurrently mutating book.
- Redundant feed sources retain **independent** replicas and one publishing authority; don't merge the fastest deltas. Failover must pick initialized healthy source, output its snapshot, then subsequent updates. Connection generation rejects late superseded work. Timestamp/hash equality is not a universal unique-event or sequence guarantee.
- Per-asset output replay order must be correct; **do not require global receive-timestamp ordering across sockets by default**. If an actual backtest contract needs it, measure and specify a synchronization/watermark policy.
- A quiet instrument may be healthy. Distinguish application-level `PONG`, individual subscription health and observed market inactivity before reconnecting repeatedly.

## Runtime, queues and overload

- Tokio networking and typed parsing are a safe baseline; separate blocking SBE output ownership is an initial candidate, **not established fastest architecture**. Benchmark socket-local book ownership against separate book shards. Avoid task-per-delta and excessive context switching.
- Size bounded queues from observed arrival/burst distribution, processing capacity and output bandwidth. Measure queue depth, oldest item age and high-water marks. Explicitly define saturation outcomes. No unbounded backlog, invisible drop/overwrite/coalesce or fabricated completeness.
- Backpressure may stop reads and cause upstream disconnects. It does not prove lossless capture. Once integrity is compromised, record uncertainty and trigger a defined fail/recovery policy rather than continuing as if no data were missed.
- `crossbeam-channel` fits synchronous workers; Tokio channels support async consumers; `rtrb` is an SPSC ring buffer requiring exactly one producer/consumer and deliberate wakeup policy. Lock-free does not mean contention-free, and a wait-free queue call does not make an entire application wait-free.
- In cross-thread handoffs, know whether you move owned decoded data, borrowed bytes or shared buffer handles. A borrowed JSON view cannot outlive or race reuse of its input storage; `RawValue` defers rather than eliminates parsing.

## SBE writer, stdout, framing and shutdown

- Use template codecs and reader as compatibility oracle. Existing `AlignedSbeWriter` (in the original repo observation) took a file path, while the challenge calls for stdout. Adapt the sink *without* silently changing record layout or assuming the example file-writing flow fulfills the requirement.
- Stdout contains **only binary SBE data**; logs and progress go to stderr or independent metrics/diagnostics. Remove example printlns from real collector. Keep independent provenance/integrity events out of the SBE stream if the required schema cannot express them.
- In the original template, SBE messages could not straddle aligned chunks, there was an eight-byte zero sentinel, and `flush` padded the complete 64 MiB chunk. Validate repeating-group/record size bounds and reject oversize records rather than truncate. The chunk size wasn't self-describing; changing it requires corresponding reader proof.
- A `write_msg` call may buffer without making bytes observable. Don't flush every delta or wait forever for a quiet chunk to fill; measure **padding bandwidth** and **flush visibility latency**. A 64 MiB aligned chunk flushed once a minute can produce enormous padding overhead if mostly empty.
- Distinguish encoding failure from partial OS writes: do not retry an entire partially written chunk blindly. Propagate `BrokenPipe`/I/O errors and fail in an observable manner.
- Explicitly flush before shutdown; dropping the observed writer did not automatically flush a tail. OS flush is not disk durability or consumer acknowledgment.
- SIGINT handler initiates controlled shutdown; stop control-plane production/retries, establish intake cutoff, close sockets, drain accepted ordered work where possible, flush and join tasks. Define handling for indefinitely blocked sink and signal abort paths. Avoid `process::exit` before cleanup. `panic=abort` skips unwinding on panic but isn't a durability strategy.

## Memory layout, allocation, collections and unsafe

- Prefer ownership transfer and borrowing over `Arc`/mutex on every update. `Arc::clone` doesn't clone payload but does reference-count work. Reuse `Vec`/`String` and parsing/encoding buffers; preallocate based on measured distributions and checked limits. Avoid retention growth in never-reset arenas.
- `BTreeMap` is an obvious ordered L2 book reference implementation. Compare sorted `Vec` for shallow books; price-indexed dense arrays or bitmap-backed storage only after proving valid tick/range semantics and total memory cost across all outcomes and replicas.
- `SmallVec` trades inline object size/branches against reduced allocation; do not use without actual size histograms. Avoid false sharing where profiling shows real contention; don't pad every struct. `repr(C)` doesn't guarantee network endianness, remove padding or permit arbitrary aligned casts; use codecs and explicit byte conversions.
- Generics/static dispatch can inline but increase code size. Trait objects on cold adapter/configuration boundaries are fine. Rust ownership checks don't erase runtime drops, dynamic dispatch, bounds checks, atomics, scheduling or memory allocation; measure concrete costs.
- Prefer safe slices, validated indexes and ordinary APIs before `get_unchecked`, `MaybeUninit`, manual SIMD or custom atomics. `Relaxed` atomics alone don't publish related payload safely; do not assume x86 behavior transfers to ARM. Use Loom for nontrivial concurrency models and Miri/fuzz/property tests where supported; their success is not universal proof.

## Compiler performance techniques (only after real profiling)

- Profile release builds with exact `rustc -vV`, target, CPU, compiler flags, lockfile, sample counts, workload and environment. Compare `opt-level` 2/3, Thin/fat LTO and codegen-units based on measured effect; aggressive inlining and fat LTO can worsen instruction-cache behavior or build size.
- `#[cold]`/selective inlining and assembly/LLVM IR inspection are for demonstrated problems, not automatic style rules. Financial validation must remain effective in release; `debug_assert!` and debug overflow are insufficient for untrusted input.
- Nightly/unstable flags are not default. `target-cpu=native` can emit incompatible binaries for other CPUs. The original harness referenced an Apple M2 development machine; benchmark findings there cannot be generalized to Linux/x86 deployments without testing.
- PGO only after reproducible, representative normal/burst/recovery fixtures. Train and evaluate on different workloads, compare to baseline, and document regressions.

## Tests, benchmarking, operational evidence

**Independent reference oracle:** Expected results from hand-checked arithmetic/simple reference model, not the optimized implementation itself. Test observable invariants after *intermediate meaningful transitions*, not only final checksum.

- Decimal boundaries: equivalent spellings, trailing zeros, minimum representable unit, overprecision, overflow, malformed sign/range and very large IDs.
- Replay: exact snapshot replacement, updates, zero deletion, empty and asymmetric books, multi-asset batches, equal timestamps, tick changes, invalid event atomicity and source generation boundaries.
- Discovery lifecycle: multipage, incomplete sweep, duplicate/conflicting IDs, new/retired, partial failures, subscription confirmation, delayed old-session messages, failover snapshot-before-delta (if extension built).
- Output: supplied-reader decode, side/symbol/quantity/price/clock, alignment/sentinel/chunk/tail flush, oversized and truncated output, injected short writes/errors.
- Operational: quiet markets, stalled outputs, queue saturation, reconnects, heartbeat failures, one-minute snapshots even for inactive books, SIGINT with pending accepted records.
- Compare microbenchmarks (parsing, book changes, snapshot generation, SBE encode) **and full pipeline**. Criterion's tight-loop numbers are not end-to-end p99. Use `black_box` where necessary to prevent benchmark elimination.
- Replay independently offered input including bursts/stalls; don't let a slow consumer suppress offered traffic (coordinated omission). Gather throughput, accepted/processed counts, bytes, allocations, resident memory, queue wait/depth, latency p50/p95/p99/p99.9/max, sample count and run duration. Histogram correction assumes a specified arrival model; don't invent one for irregular markets.
- Network tests keep TLS verification. Separate DNS/TCP/TLS/reconnect costs from steady-state delivery; local `TCP_NODELAY` affects local sends, not exchange-side buffering. Don't spam connections or evade rate limits; CDN/socket-affinity advantages require matched-event experiments. NIC timestamps are a separate Linux/platform feature, not what `Instant::now` after a WebSocket read provides.
- Report exactly what checks ran, what the evidence supports and what's unverified. "Builds", "zero tests passed", "matching latest book" and "no observed gaps" are **not** proof of a complete lossless historical collector. Do not claim fastest without controlled comparative data. Do not push, commit or deploy unless authorized.

## Original repository verification commands

Run from the repository root **when these targets exist**, and distinguish a clean compilation from passing behavioral tests:

```sh
rustc -vV
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked
cargo test --locked --release
cargo clippy --locked --all-targets -- -D warnings
```

After the collector replaces the synthetic example, `cargo run --locked` triggers live network capture rather than a harmless local round trip. Bound live tests by subscription scope, duration, output destination and safe termination. Add dedicated benchmark, Loom or Miri commands only after the corresponding targets/toolchains exist. Explain dependency/lockfile changes and supported minimum Rust version. A dependency's release announcement is not integration testing.
