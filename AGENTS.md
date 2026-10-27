
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

## Component scope and authorization

The user develops this project **one component at a time**: investigate its requirements and contracts, review its implementation plan, then execute and validate the approved scope. **Currently authorized: Components 3 and 4 under `3&4.md`: exact decoding and source-specific L2 reconstruction, plus necessary C2 integration.** The complete challenge and all four extensions are the eventual target, not permission to implement their later delivery stages now.

- Use the user's component names and boundaries consistently. Do not substitute a different numbered implementation sequence or redefine Component 2 to mean the entire data plane.
- Requests such as “go ahead,” “execute it,” or “complete Component 2” authorize the agreed component plan, not downstream components or extensions. Finish that component's acceptance checks, report limitations, and stop before starting another component.
- Cross-component edits must be necessary for the current component's contract. State the dependency and why an existing interface or a small test consumer cannot satisfy it. Make only the smallest integration change; if it requires implementing another component, explain the blocker and obtain approval before expanding scope.
- A useful future integration, fewer eventual queues, or a desire to demonstrate the entire collector is **not** sufficient justification for implementing a downstream component early. Logical ownership decisions do not authorize its implementation.
- Validate the current boundary with deterministic fixtures and a minimal test consumer where appropriate. Do not build a production downstream subsystem merely to test the current component.
- Existing code and historical architecture documents do not expand authorization. Flag any existing implementation that crosses the approved boundary; do not silently delete existing work or use its presence as permission to extend it.

## Current design decisions (not yet benchmark-proven)

- **Control plane:** a minimal, exchange-adapter-based **instrument discovery and desired-state registry**. Use paginated Gamma REST for Polymarket, plus lifecycle hints and periodic full reconciliation. Never derive new instruments by incrementing IDs; external IDs are opaque and version-scoped.
- **Adapter boundary:** `PolymarketV1` and `PolymarketV2` are separate adapters with shared Gamma transport, pagination, retries and common lifecycle parsing. Future `Kalshi` is a separate adapter with its own transport, not a Gamma version. The registry accepts normalized records keyed by adapter namespace plus exact native ID; keep wire fields and binary-outcome cardinality in the Polymarket adapters. Do not collapse these adapters merely because they share endpoints, or implement Kalshi prematurely.
- **No category/tags on the critical path.** Keep only identity, protocol, market/outcome relationship and collection eligibility. An optional timestamped metadata/strategy index can join on exact IDs for category-filtered backtests later.
- Use `HashMap` for registry lookup and temporary `HashSet` for scan reconciliation when useful. Do **not** maintain a redundant persistent `active_assets` set if the registry already expresses the desired set. Component 1 owns **desired state**; Component 2 owns assignments and **successfully requested subscriptions for each connection session**. A successful send is not exchange confirmation. **Book initialization** belongs to the downstream book engine, not the discovery registry or transport's liveness state.
- **Future integrated data-plane candidate, not current implementation authorization:** multiple Tokio WebSocket workers may eventually own and update assigned books locally, feeding one ordered SBE output owner. Evaluate this against separately sharded book processors when those components are authorized. One mutable owner per source/book; no global book mutex. Component 2 must first be independently testable without production book reconstruction or SBE output.
- **Exchange-neutral does not mean exchange-identical.** Polymarket subscribes to outcome asset IDs; Kalshi generally uses market tickers. Normalize identities/lifecycle at adapter boundaries without forcing a fake universal YES/NO book API.
- Discovery REST calls, metadata enrichment, monitoring, retry sleeps, and expensive logs must never block the market-update hot path.

## Non-negotiable data contracts

These are system-wide contracts to apply when implementing the component that owns them, not permission to build that component early. Component 2 preserves raw message content and ordering for downstream validation; it does not need to reconstruct books or encode SBE to satisfy its transport contract.

- A received `book` snapshot **replaces** the prior book; a `price_change` level gives an **absolute new quantity**; zero deletes. Validate an entire event before applying partial changes. A trade print is not itself a book delta.
- Preserve **per-instrument feed order**, including snapshots and update batches. Do **not** sort by exchange timestamps or assume equal timestamps imply duplicates. Global chronological ordering across sockets is **not promised** unless a downstream contract demands it and the cost is measured.
- Do not publish deltas against an uninitialized/known-stale book. Every authoritative replacement or source failover must create a visible replay boundary (normally a snapshot). A replacement snapshot restores *state*, not missing *history*.
- Financial amounts: exact checked decimal → signed `i64` at the repository's `1_000_000` scale; never route through floats or silently round. Preserve external IDs exactly or through a collision-free, persisted ID mapping.
- Capture **application-receive wall-clock time** immediately after a complete WebSocket message is delivered, before parsing or queuing. Retain exchange time separately; use monotonic `Instant` for duration measurement. Neither is automatically a NIC timestamp.
- Use the supplied SBE schema/helpers, respect chunk framing and reader compatibility. **Binary records only on stdout**; diagnostics to stderr or an independent sidecar. Resolve and test the supplied file-oriented writer versus stdout before claiming delivery complete. Explicitly specify whether output visibility is delayed by buffering and how flush occurs.
- A bounded queue must have an explicit full-queue and failure policy. Never silently drop, overwrite, coalesce, or relabel historical order-book updates. No finite queue can preserve an infinitely stalled consumer.
- Graceful SIGINT: stop discovery/reconnect scheduling, establish intake cutoff, close sockets, drain accepted ordered work if feasible, flush output, and join workers. Report unsuccessful drain/broken pipe; do not claim durability from flush alone.

## Component boundaries and acceptance gates

### Component 1 — Instrument discovery and registry

Own eligibility, exact version-scoped identities and authoritative desired state. Preserve validated incremental additions, atomic market updates, uncertainty reporting and safe retirement/reactivation. Incomplete scans cannot erase prior desire. Keep its tests independent of WebSockets. During Component 2 work, retain this implementation except for required integration, such as a continuous lifecycle reusing its existing scheduler and snapshot/notification interface.

### Component 2 — Subscription management and WebSocket ingestion

- **Input:** Component 1's authoritative desired-state snapshots and change notifications.
- **Ownership:** one subscription manager owns stable market-to-worker assignments; one Tokio task owns each socket, its desired/requested subscriptions and connection generation.
- **Output boundary:** owned complete WebSocket messages, application-receive timestamps captured before parsing/queueing, per-session receive order, and ordered session-start/session-end boundaries. Keep enough assignment context for a future downstream consumer to identify the source. Do not sort by exchange timestamps or claim global chronological order.
- **Required behavior:** initial and incremental subscription commands, multiple sockets, heartbeat/control-frame handling, bounded reconnect/backoff, latest desired state after reconnect, explicit overload/failure handling and graceful shutdown/draining of accepted messages. Preserve adapter namespaces. Sending a subscription is not proof of acceptance or book initialization.
- **Allowed supporting work:** minimal discovery lifecycle integration, bounded transport interfaces, protocol-level control/lifecycle handling required by the approved plan, local HTTP/WebSocket test servers, and a small test consumer. Observing an initial `book` message in a fixture is not the same as reconstructing and validating a live book.
- **Acceptance:** validate stable assignments and capacity, dynamic changes, reconnect generations, ordered accepted-message delivery, missed-notification recovery, message/queue limits, full/closed consumers, stalled I/O and shutdown. Run bounded live probes when feasible and measure transport throughput, receive-to-handoff latency and queue behavior. Report missing evidence rather than building another component to fill it.
- **Not implicitly authorized:** production financial decoding, exact book reconstruction, periodic book snapshots, SBE serialization/stdout integration, downstream book recovery, redundant feeds, automatic rebalancing, Kalshi, or full-pipeline performance tuning. Existing implementations of these are not part of Component 2's acceptance merely because they currently run inside a socket worker.

### Components 3/4 — Decoding and source-specific books (current scope)

Use the approved `3&4.md` contracts: exact scaled values, bounded typed parsing,
whole-event prevalidation, independent source replicas, assignment revisions,
session validity, deterministic selection snapshots and bounded recovery.
Retain one synchronous processing owner behind existing C2 ingress. C2 changes
are limited to assignment context, an ordered retirement cutoff (including while
disconnected), and session-checked restart controls. Preserve transport-only tests.
Validate state and intermediate observations against an independent oracle;
separate genuine fixtures from synthetic cases and report missing live evidence.

### Later components — require their own reviewed plans and authorization

C5 owns supplied-reader-compatible binary stdout, minute snapshots, buffering and
tail flushing. Later extension work owns redundant socket provisioning, automatic
health-based failover, monitoring/rotation policies and connection experiments.
Do not implement those or Kalshi during C3/C4 work. Independent replicas and an
explicit selection API are current C4 contracts, not live redundancy delivery.

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
