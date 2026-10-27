# Full-project revision checklist

**Created:** 2026-10-27. **Status:** open; not full-collector sign-off.

Use this after the separately approved components form a working collector,
before claiming production readiness or low latency. It records deferred checks,
not permission to implement later components now. Component 1 owns discovery;
Component 2 owns assignments and raw transport. Book reconstruction, recovery
and SBE output remain separate work.

Fix a demonstrated correctness failure when found; do not defer it just because
this checklist is scheduled for integration. Optimize only after the correctness
gates pass and the actual bottleneck is measured.

## Current transport evidence and limits

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
- The current executable validates and discards raw messages. It does not persist
  history, reconstruct books, or emit SBE. See [protocol.md](protocol.md) for
  historical live-probe and load-test evidence and its limitations.

Local validation on 2026-10-27, macOS ARM64 / Rust 1.93.0-nightly: formatting,
locked all-target check, strict Clippy and all 35 default tests passed in debug
and release. The opt-in release raw-load test preserved 100,000 messages in order
and drained in 2.502728 seconds (39,956 messages/s). Queue-age p50/p95/p99/p99.9/max
were 6/206/960/8,294/9,985 µs; maximum generator lag was 13,707 µs. This is a single
loopback run, not proof of a performance improvement. Live universe/size coverage
was not rerun. Local log: `.amp/in/artifacts/component-2-revision-checks.log`.

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
