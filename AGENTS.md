
# AGENTS.md — Voluntas market-data collector (Rust)

## Mission and scope

Build the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge) in Rust, using **Polymarket** as the initial exchange because the public market feed is accessible. This is a **full-universe L2 market-data collector for backtesting**, not a trading engine, execution bot, category screener, or general-purpose indexing platform. Keep the design capable of admitting a Kalshi exchange adapter later, but **do not implement Kalshi until requested**.

**Required behavior** (source of truth: live challenge and supplied repository): discover every eligible active instrument; distribute its subscriptions over multiple WebSockets; update an L2 book for each instrument; emit every accepted price-level update to binary **stdout** using the supplied SBE schema and helpers; emit a complete snapshot for every initialized live book every minute; discover additions/retirements continuously; and shut down gracefully on SIGINT. Treat redundant feeds, health monitoring, and advanced optimizations as **separate extensions**, not automatic core scope.

Priority: **numerical accuracy → reconstructed-book correctness → honest accounting of observed history → timestamp fidelity → sustainable throughput → latency/resource efficiency**. Do not sacrifice earlier properties for a benchmark. An exchange feed with no documented replay/sequence guarantee cannot prove exchange-wide losslessness. Distinguish *accepted-input correctness*, *observed-stream capture*, and *unverifiable/missing exchange history* in reports.

## Decision hierarchy

1. Explicit user scope and the published challenge; then the repository's SBE schema and reader/writer behavior.
2. Current official exchange docs **and reproducible real message fixtures**, noting version and verification date. Flag a conflict rather than quietly choosing a convenient interpretation.
3. `docs/architecture.md`, `docs/protocol.md`, `docs/engineering-reference.md`, and `docs/research.md` for tested decisions, operational rules, and optimization hypotheses.
4. Benchmark results on the actual workload; never assume an optimization wins because a blog calls it “HFT.”

Read only the relevant reference sections for the current task. Preserve existing user edits, do not add dependencies without a concrete reason, and do not create unrelated trading or metadata infrastructure. Do not use subagents unless the user explicitly changes that instruction.

## Current design decisions (not yet benchmark-proven)

- **Control plane:** a minimal, exchange-adapter-based **instrument discovery and desired-state registry**. Use paginated Gamma REST for Polymarket, plus lifecycle hints and periodic full reconciliation. Never derive new instruments by incrementing IDs; external IDs are opaque and version-scoped.
- **Adapter boundary:** `PolymarketV1` and `PolymarketV2` are separate adapters with shared Gamma transport, pagination, retries and common lifecycle parsing. Future `Kalshi` is a separate adapter with its own transport, not a Gamma version. The registry accepts normalized records keyed by adapter namespace plus exact native ID; keep wire fields and binary-outcome cardinality in the Polymarket adapters. Do not collapse these adapters merely because they share endpoints, or implement Kalshi prematurely.
- **No category/tags on the critical path.** Keep only identity, protocol, market/outcome relationship and collection eligibility. An optional timestamped metadata/strategy index can join on exact IDs for category-filtered backtests later.
- Use `HashMap` for registry lookup and temporary `HashSet` for scan reconciliation when useful. Do **not** maintain a redundant persistent `active_assets` set if the registry already expresses the desired set. Keep **desired**, **subscription-confirmed**, and **book-initialized** separate. Component 1 owns **desired state**; the WebSocket subscription manager owns **confirmed state** when implemented in stage 4 (it is not part of Component 1).
- **Data plane baseline candidate:** multiple Tokio WebSocket workers, each initially owning and updating its assigned books locally, feeding one ordered SBE output owner. Benchmark this against separately sharded book processors before adding an extra queue/handoff. One mutable owner per source/book; no global book mutex.
- **Exchange-neutral does not mean exchange-identical.** Polymarket subscribes to outcome asset IDs; Kalshi generally uses market tickers. Normalize identities/lifecycle at adapter boundaries without forcing a fake universal YES/NO book API.
- Discovery REST calls, metadata enrichment, monitoring, retry sleeps, and expensive logs must never block the market-update hot path.

## Non-negotiable data contracts

- A received `book` snapshot **replaces** the prior book; a `price_change` level gives an **absolute new quantity**; zero deletes. Validate an entire event before applying partial changes. A trade print is not itself a book delta.
- Preserve **per-instrument feed order**, including snapshots and update batches. Do **not** sort by exchange timestamps or assume equal timestamps imply duplicates. Global chronological ordering across sockets is **not promised** unless a downstream contract demands it and the cost is measured.
- Do not publish deltas against an uninitialized/known-stale book. Every authoritative replacement or source failover must create a visible replay boundary (normally a snapshot). A replacement snapshot restores *state*, not missing *history*.
- Financial amounts: exact checked decimal → signed `i64` at the repository's `1_000_000` scale; never route through floats or silently round. Preserve external IDs exactly or through a collision-free, persisted ID mapping.
- Capture **application-receive wall-clock time** immediately after a complete WebSocket message is delivered, before parsing or queuing. Retain exchange time separately; use monotonic `Instant` for duration measurement. Neither is automatically a NIC timestamp.
- Use the supplied SBE schema/helpers, respect chunk framing and reader compatibility. **Binary records only on stdout**; diagnostics to stderr or an independent sidecar. Resolve and test the supplied file-oriented writer versus stdout before claiming delivery complete. Explicitly specify whether output visibility is delayed by buffering and how flush occurs.
- A bounded queue must have an explicit full-queue and failure policy. Never silently drop, overwrite, coalesce, or relabel historical order-book updates. No finite queue can preserve an infinitely stalled consumer.
- Graceful SIGINT: stop discovery/reconnect scheduling, establish intake cutoff, close sockets, drain accepted ordered work if feasible, flush output, and join workers. Report unsuccessful drain/broken pipe; do not claim durability from flush alone.

## Implementation sequence and gates

**Currently focus on Component 1 only until asked to continue.** Every stage must be executable and independently validated before advancing:

1. **Instrument discovery:** use the documented, fixture-backed rules and explicit unresolved policies in `docs/protocol.md`; normalize opaque IDs and outcome mappings. Publish validated additions incrementally and atomically per market. Incomplete scans cannot remove subscriptions; known malformed/uncertain markets retain desired state and trigger targeted verification. Invalidate cached snapshots and notify on uncertainty/identity changes as well as additions/removals. Retirement and reactivation require fresh matching targeted evidence, not permanent tombstones. Tests: multipage, conflicting IDs, partial scans, unknown eligibility, missing fields, paused versus retired, snapshot recovery, and verified reactivation. Distinguish pagination completion from established coverage; an empty registry after failed startup is not an empty exchange. No live-WebSocket dependency for its tests. The future subscription consumer must reconcile on startup, notification and a finite periodic interval.
2. **Exact book engine + fixture replay:** prove decimal boundaries, snapshot replacement, absolute deltas, zero deletion, multi-asset batches, and correct order. Use an independent simple reference oracle.
3. **SBE output contract:** produce binary stdout and round-trip through the supplied reader; test 64 MiB framing, tail flush, short writes, large snapshots, side, IDs, units and timestamps. Resolve mapping before writing real data.
4. **One live socket → multiple sockets:** capture and timestamp real messages, initialize books, account for subscriptions, add socket assignment and lifecycle reconciliation.
5. **Recovery and optional redundancy:** explicit uninitialized/live/stale/resync/retired states; mark uncertainty intervals, reject superseded connection generations, emit snapshot on failover. Do not infer losslessness from a hash or timestamp.
6. **Performance:** record realistic traffic; benchmark full-pipeline load and p50/p95/p99/p99.9, counts, queue age, RSS and delivery delay. Optimize only measured bottlenecks, preserving the same replay oracle.

## Performance discipline

- **Fast code is not intentionally messy code.** Minimize work, allocation, copies, lock contention, context switches, and queue hops on the hot path while keeping invariants inspectable. No speculative `unsafe`, SIMD, lock-free map, kernel bypass, spin loop, task-per-delta, custom allocator, PGO, or compiler flag as an initial requirement.
- Start with typed parsing, `BTreeMap` books and safe Rust; compare sorted `Vec`, dense/bitmap ladders, or optimized parsers **only** on representative workload distributions. L2 aggregates price levels; do not copy L3 matching-engine queues/order-ID structures.
- A change is an optimization **only if** it improves a defined measurement with the same input, validation oracle, environment and correctness checks. Compare producer latency **and** sustained end-to-end consumer throughput / output visibility. Tail results require sufficient samples and realistic independent offered load.
- Keep official protocol rules and narrow engineering contracts separate from research links. Production writeups are evidence of tradeoffs, not proof that their implementation fits our workload.

## Agent working protocol

Justify an architectural change by a credible, explicitly described correctness failure it prevents, a concrete simplification, or a measured performance improvement. Correctness prevention does not require a prior production incident; performance claims do require evidence.

## Implementation gates — Stateful and network components

Apply these gates to consequential stateful, concurrent, networking, and performance-sensitive changes. Do not impose irrelevant requirements on trivial or isolated changes.

Before implementation, establish the applicable contracts:

1. **State and invariants**
   - Define valid state transitions and ownership.
   - Distinguish unknown, temporarily unavailable, invalid, and terminal states.
   - Identify what must never happen, including ordering and data-integrity violations.

2. **Work lifetime**
   - Define what starts, retries, cancels, and completes work.
   - Bound individual requests and repeated scheduling.
   - Specify whether duplicate observations reset retry budgets.
   - Ensure failed work cannot silently invalidate authoritative state.

3. **Resource and overload behavior**
   - Identify queues, buffers, tasks, and retained state that can grow.
   - Establish justified capacity, retention, and overload policies.
   - Distinguish recoverable control-plane work from irreplaceable market-data updates.
   - Never silently drop historical market-data updates or represent an invalid book as valid.

4. **Performance contract**
   - Identify the latency-critical path and necessary work.
   - Avoid unnecessary allocations, copies, synchronization, scheduling, and I/O.
   - Establish baseline throughput, tail latency, memory use, and queue behavior where relevant.
   - Test sustained load, bursts, and downstream stalls.
   - Accept optimization only when measured improvements preserve correctness.
   - Do not introduce low-level complexity without demonstrated benefit.

5. **Acceptance and evidence**
   - Separate connectivity, partial progress, completed traversal, validated coverage, and downstream readiness.
   - Mark protocol assumptions as documented, fixture-observed, or unresolved.
   - Test safety, progress, boundedness, and relevant performance behavior.
   - For confirmed bugs, first reproduce the failure in a regression test where practical. If not reproducible deterministically, document the evidence and add the strongest feasible test.
   - Map acceptance criteria to executed evidence.
   - Passing existing tests alone is not sign-off.

### Engineering decision rule

Prefer the simplest implementation that satisfies correctness, completeness, and measured performance requirements.

Do not introduce abstractions, synchronization, additional queues, dependencies, or low-level optimizations without a specific problem they solve.

Keep discovery and administrative work outside the market-data hot path.

Never trade silent data corruption or historical data loss for improved benchmark numbers.

For each task: **(a)** restate the component boundary and success contract in 3–6 lines; **(b)** inspect repository and relevant current docs/fixtures; **(c)** identify uncertainty and expected failure cases; **(d)** implement the smallest correct slice; **(e)** run format/check/test/replay and relevant benchmarks; **(f)** report exact files changed, evidence, remaining risks and the next test. If a gate fails, fix it before performance tuning.

Typical checks, when supported by the repository:

```sh
rustc -vV
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked
cargo test --locked --release
cargo clippy --locked --all-targets -- -D warnings
```

Do not assume `cargo run` is a harmless test after the live collector replaces the synthetic example. Specify test duration and binary stdout destination. Do not push/commit/deploy without authorization. Never claim tested, lossless or fastest without evidence.
