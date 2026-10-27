# Collector architecture and decision record

Status: Components 1–5 cover discovery, raw ingestion, exact decoding, source-specific L2 books and binary recording. Automatic redundancy/health policies and connection experiments remain later extension work. Full-universe acceptance and representative production latency remain open. Updated: 2026-10-27.

## Boundaries

```text
Gamma REST ---------------------------┐
      ↓                               │ control plane
Exchange discovery adapter            │
      ↓                               │
Desired instrument registry           │
      ↓ reconciled desired state       │
WebSocket subscription manager <------┘
      ↓ desired socket assignments (not exchange acknowledgements)
Multiple WebSocket workers (independent connection generations)
      ↓ owned raw message + receive timestamp + session/ordinal
Bounded ordered ingress channel (explicit overload policy)
      ↓ synchronous owner: decode → source-specific books → borrowed encoding
      ↓ fixed owned batch pool (one FIFO, no live-book borrows)
Dedicated output owner → supplied SBE framing → binary stdout
                       → ordered JSONL integrity sidecar
```

Discovery is not market data: Gamma tells us which instruments to listen to, not each live order-book update. Component 2 forwards complete text messages unchanged except application PONG, which belongs to connection health. It does not parse financial events, initialize books or produce periodic book snapshots.

## Component 5: Recording contract

`recording::Recorder` runs on the C4 owner, formats an exact namespaced symbol
once per BookId and directly encodes borrowed book iterators/levels with generated
codecs. The fixed pool transfers owned bytes, not books. A single output thread
routes SBE and integrity entries in order; no per-level channel item or symbol
allocation is needed. Pool exhaustion and encoder/I/O errors are terminal.

C4 keeps one selected-live deadline per book. Authoritative snapshots reset the
60-second cadence; deltas and standby snapshots do not. Captures run between
complete messages with book/level/time fairness limits. Actual lateness is counted;
missing a whole additional interval terminates recording. C5 records snapshot
origin, source cursor, input cutoff and the old selected binding on unavailability.
Neither snapshots nor a successful run footer assert missing exchange history was
recovered. Only selected-source invalidations create output gaps.

The supplied writer retains 64 MiB framing but no longer allocates a whole-chunk
buffer. Physical flush and logical chunk completion are separate. The supplied
reader still requires complete chunks and fixes file size at open. Stdout is an
owned descriptor clone under one explicit physical buffer; error/unwind teardown
disarms `BufWriter` rather than implicitly retrying a partially written prefix.

Startup requires a new integrity file and a successful run-start flush before
network intake. Success requires producer completion, supervisor approval of
intake/drain, stdout completion and sidecar flush. Files are not an atomic pair;
keep exit status and sidecar. Shutdown uses one overall deadline, then exits
unsuccessfully without waiting on blocked I/O. Flush does not mean durability.
See [the low-level plan](../5.md), [run instructions](../README.md), and
[remaining validation](revision-check.md).

## Component 1: Minimal instrument discovery and desired-state registry

### Identity

- FeedKey is an opaque adapter-native subscription key: adapter namespace plus exact instrument ID. `PolymarketV1` (CTF token IDs) and `PolymarketV2` (position IDs) are separate adapters/namespaces; future Kalshi uses its own namespace and native market ticker. Do not infer adjacent IDs, sequential creation, or tags from IDs.
- Instrument records only what's needed to manage capture and label output: FeedKey, parent market reference, outcome/side identity if documented, protocol/version, eligibility and any required SBE symbol mapping.
- Full external IDs may exceed 128-bit integers; preserve exact strings or validated 256-bit values. If the SBE helper uses narrower internal symbols, assign and persist a collision-free reversible mapping before encoding. Avoid reusing compact IDs ambiguously across sessions.
- Do not automatically store categories, liquidity, descriptions, titles or full events in the hot-path registry. For category-filtered backtests, an optional independent metadata store joins by the canonical identity and keeps point-in-time observations. "First observed" does not imply "first changed at exchange."
- Component 1 uses one authoritative `HashMap<MarketKey, MarketEntry>` where each entry owns its instrument identities. `MarketKey` contains `AdapterId` and exact native market ID (Gamma ID for Polymarket). A derived `(AdapterId, instrument ID)` ownership index detects reuse within an adapter; identical IDs across adapters are distinct. Temporary sets track scan absence and cursors. One mutable owner commits all of a market's instruments together, without a mutex shared by socket workers.

### Adapter boundary

`src/discovery/polymarket/adapters.rs` implements `PolymarketV1` and `PolymarketV2` separately. Each selects its request version and decodes its own ID field; neither falls back to the other format. They share Gamma envelope/lifecycle parsing and the HTTP/pagination/retry driver in `polymarket.rs`. The driver maintains independent scan progress and routes verification by adapter identity, while retaining one registry owner and one reusable HTTP client.

The shared registry consumes normalized `Page`/`MarketRecord`/`RecordError` values, not JSON, Gamma versions or status flags. Adapters validate native identities and classify eligibility. For malformed records they return any recoverable market/instrument IDs so the registry can preserve known desire and schedule verification. `MarketEntry.instruments` is a `Vec<Instrument>`; only the Polymarket adapters require two outcomes. Instrument count or mapping changes remain identity conflicts, including a matching prefix with added/removed instruments.

`GammaAdapter` is specifically the shared Polymarket transport contract, not a universal exchange API. Future Kalshi will provide its own discovery transport/normalizer feeding the same normalized registry boundary. No Kalshi implementation, fake two-outcome Kalshi representation, extra worker, dynamic plugin loader or universal HTTP abstraction is added here.

### Discovery policy — incremental publication, verified lifecycle changes

The implementation policy and fixture provenance are in [protocol.md](protocol.md#component-1-evidence-and-policy-2026-10-26). This supersedes the earlier complete-scan-only publication baseline. Missing or contradictory data is not a silent false; order-entry availability is not a proof of feed availability.

Polymarket adapter inclusion requires matching supported version, two distinct exact decimal-string outcome IDs, two labels mapped by index, `active=true`, `closed=false`, `archived=false`, and `enableOrderBook=true`. `acceptingOrders=false` alone does not exclude a book. This deliberately conservative listening policy is not proof that a paused market accepts WebSocket subscriptions; it is not a universal registry eligibility formula.

| Input case | Desired-state decision | Evidence / follow-up |
|---|---|---|
| New valid eligible market | Publish both outcomes immediately | Do not wait for other pages or versions |
| Malformed or uncertain record with identifiable market or known asset | Preserve prior desired state, mark known entries uncertain, queue targeted lookup | Unidentifiable records count as unresolved; never invent an identity |
| Identity collision or changed outcome mapping | Preserve existing identity/desire; mark affected known owners uncertain; verify all identifiable candidates | No half-market insertion or automatic identity remapping |
| Never-admitted inactive/book-disabled market with known lifecycle | Do not admit or poll; discard pending verification for it | A later eligible listing can admit it; missing IDs on a disabled book need no lookup |
| Known market inactive/book-disabled, or absent after completed pagination | Retain desire and queue verification | Neither is proof of retirement |
| Closed/archived record | Do not add a new market; existing market requires targeted verification before removal | Matching identities and no contradictory `acceptingOrders=true` |
| Retired market appears eligible again | Keep non-desired until fresh targeted evidence matches its original IDs and eligible status | Explicit reactivation; retirement is not a permanent tombstone |
| Failed page or repeated cursor | Preserve additions already published and all prior desire | Report traversal failure; no absence-based removals |

`pagination_complete` means that one version reached the end of its cursor traversal, not that all records were usable or that the exchange universe was captured atomically. Report unresolved records, pending verification, uncertain known markets and per-version errors separately. An empty registry after failed startup means unknown coverage, not an empty exchange.

Retired identities are retained for matching verified reactivation, not permanently barred. This initial in-memory registry has no persistence or eviction; memory grows with identities observed over its lifetime. Establish retention and operational staleness thresholds from long-running measurements before production use.

### Discovery and reconciliation protocol

1. Alternate V1 and V2 listing pages using `limit=100`, `closed=false`, explicit version and opaque cursor. One reusable HTTP client and one in-flight request avoid locks and unmeasured concurrency. A failed version does not discard the other version's progress.
2. Parse the page envelope, then each market independently. Validate both outcomes and identity ownership before an atomic market commit. Publish additions now; later contradictions mark uncertainty and request evidence rather than silently rewriting identities.
3. After each page, service one eligible targeted batch (up to 100 same-version market IDs with `closed=any`). Accumulate targeted pages before applying evidence. Missing/contradictory targeted results retain prior desire. Missing known markets are queued only after that version completes its listing traversal.
4. Targeted eligible evidence can add a new market, clear uncertainty, or reactivate a retired market only with matching identities. Targeted closed/archived evidence can set `desired=false`. Listing records alone cannot clear existing uncertainty, retire, or reactivate. Lifecycle hints only request this verification.
5. Every observable registry change, including uncertainty changes, invalidates the lazy cached `Arc<Snapshot>`. Notify once at each listing-page, targeted-batch, request-failure or completed-scan absence boundary, before awaiting more work. Market commits remain atomic; the final dirty batch must notify even when no next page arrives. Snapshot copies occur on consumer request after mutation, not for every addition. Old snapshots remain immutable. A full snapshot is authoritative desired state, not a historical change log.
6. The subscription manager reads full state at startup, at most once per second on notification, and every five seconds regardless of notifications. This repairs lost/coalesced notifications without a durable event log. Restarting the registry process requires rediscovery; manager failure terminates the baseline collector.
7. Requests have at most three HTTP attempts, a 15-second attempt timeout, jittered backoff and server `Retry-After` handling. A run deadline/Stop cancels HTTP or backoff. A targeted round accumulates at most 10 pages/1,000 records before applying evidence; oversize or incomplete results cannot retire anything. Sweeps resume 60 seconds after completion. Targeted work runs between pages and while waiting between sweeps. Snapshot/Hint/Stop commands remain serviceable during awaits.

Verification scheduling uses one identity-keyed map, replacing the old pending set plus separate retry-time map. Unknown, never-admitted identities get at most three rounds, with 30- then 60-second minimum start-to-start gaps, within a five-minute scheduling lifetime. Duplicate hints/listings do not refresh the budget. Exhausted jobs remain until expiry to suppress duplicates; at expiry they are removed and counted, not declared resolved. There is no automatic restart after expiry; a subsequent listing/hint may start a new bounded job. This deliberately permits later recovery without permanent suppression tombstones. An already-running HTTP round may finish after the scheduling lifetime, bounded by its transport budgets and run deadline.

At most 1,024 unknown jobs are retained, including exhausted jobs. New unknown work at capacity is rejected and counted; periodic discovery can recover it later. Known entries bypass that limit and continue verification with capped 30/60/120/240/300-second backoff. Neither failure nor job expiry removes desired state. Oldest-due selection prevents newer hints from continually overtaking older work. These are collector defaults, not exchange protocol limits; validate them under sustained churn before production use. Total verification state is bounded by retained known markets plus 1,024 unknown jobs, not by 1,024 alone. Retired registry history itself remains unbounded.

The command channel has capacity 64; senders await capacity rather than dropping commands. Watch notifications coalesce because consumers fetch current state. Timing/scanning counters describe the last sweep and current registry; expiry, overflow, identity-conflict and failed-verification counters accumulate over the owner lifetime. None count confirmed subscriptions. A single in-flight request may delay another version during backoff; evaluate parallelism only if measured discovery delay warrants it.

`listing_requests` and `verification_requests` count HTTP attempts, including retries and attempts cancelled at the deadline. The Gamma client uses reqwest's gzip support; the 8 MiB response limit applies to decompressed bytes. Keep the original bounded-job policy: a tested 30-minute cooldown was removed because no measured retry bottleneck justified its extra state and recovery delay. Expiry remains uncertainty, not resolution, and later observations may start a fresh bounded job.

Do not require category metadata, matching-engine structures, lock-free registries, advanced compiler flags or generic plugin frameworks for Component 1.

### Implemented interface

```rust
// Abbreviated; see src/discovery/{mod,polymarket}.rs and polymarket/adapters.rs.
enum AdapterId { PolymarketV1, PolymarketV2 }
struct MarketKey { adapter: AdapterId, id: String }
struct Instrument { id: String, label: String }
struct MarketEntry {
    instruments: Vec<Instrument>,
    desired: bool,
    uncertainty: Option<String>,
}
struct Snapshot { markets: HashMap<MarketKey, MarketEntry> }
enum Command { Snapshot(oneshot::Sender<Arc<Snapshot>>), Hint(MarketKey), Stop }
// Discovery::new(Gamma) -> (owner, bounded command sender, watch receiver)
// owner.run_for(duration, once) -> DiscoveryReport
// adapter.decode_page(bytes) -> Result<Page>
// registry.apply_page(adapter.id(), page) -> PageReport
// registry.verify_records(adapter.id(), requested_ids, accumulated_records)
```

Scan reports identify `adapter: "polymarket_v1"` or `"polymarket_v2"`, replacing the old bare `version` field. The Polymarket wire `Version` enum is confined to the adapter/transport module; registry reconciliation does not branch on it. `desired_assets` remains the report's count field for compatibility and now sums actual instrument counts rather than assuming two per market.

### Component 1 acceptance gate

Use deterministic fixture → expected desired-state tests. At minimum:

- Complete V1/V2 multipage discovery yields the exact eligible asset set; opaque IDs and market/outcome relationships are preserved, including non-YES/NO display labels where present.
- Each adapter selects the correct version/ID field and rejects responses for the other version. Overlapping native market/instrument IDs stay independent across adapters, including targeted verification. The normalized registry supports one-instrument records without pretending a second outcome exists.
- The fixture-backed eligibility matrix distinguishes feed eligibility from order acceptance, covers missing/null/conflicting fields and unavailable IDs, and explicitly marks unresolved cases unknown. Synthetic pauses/reactivations test collector policy, not exchange guarantees.
- Repeated identical scans are idempotent; new validated instruments are added exactly once, without relying on sequential IDs.
- A failed page, repeated cursor, malformed/unknown required fields, contradictory mapping or unsupported version cannot cause a mass unsubscribe or be silently counted as complete coverage.
- A malformed record cannot block unrelated valid additions. Prove a consumer sees page-one additions before the rest of pagination completes; later page/version failures cannot erase them.
- Identity conflicts and uncertainty changes invalidate cached snapshots and notify consumers while preserving previous immutable snapshots. Both outcomes commit or neither does.
- A known paused/unknown market retains desire and gets targeted verification. Closed targeted evidence retires it; only matching targeted eligible evidence reactivates it. Missing results and changed IDs cannot reactivate or retire.
- Disabled never-admitted markets create no targeted work. Unknown verification has bounded attempts, backoff, age and capacity; duplicate hints cannot reset these limits. Saturation/expiry must not suppress verification or remove desire for known markets.
- Strict observed-discovery acceptance requires both traversals to finish cleanly, nonempty desired state, and no unresolved records, pending/uncertain markets, conflicts, verification failures, expiry or overflow. It is intentionally conservative: successful later verification does not erase an earlier unresolved scan record. Even acceptance is not proof of an atomic exchange universe or downstream readiness.
- A consumer can recover current desired state by requesting a full snapshot without replaying notifications. When the actual subscription consumer is added, test periodic repair after a missed final notification with a controlled clock.
- Registry work stays in the control plane and does not require categories/tags or a live WebSocket to run tests.

## Historical book-placement candidates — benchmark before changing ownership

- The implemented baseline is one synchronous book owner behind C2 ingress. Socket-local books and separately sharded book owners remain alternatives for controlled end-to-end benchmarks. Avoid task-per-message and global mutable book maps.
- Timestamp after full WebSocket-message delivery, before parse, keep same timestamp for items in its batch. Route/commit per-asset changes in stream order; cross-socket global timestamp order is not guaranteed. If backtester requires global ordering, document and benchmark a specific policy, not an arbitrary sort.
- Explicit book state machine: Discovered → Subscribing → Initializing → Live → Stale/Resynchronizing → Live; retirement requires verified lifecycle evidence. Verified reactivation must initialize a fresh book, not resume stale state. Distinguish socket liveness from per-asset subscription health; inactivity may reflect a quiet market.
- Feed redundancy (extension): independent book replicas; one publishing authority; a controlled snapshot boundary on failover; never combine the "fastest" deltas from two connections into one state. Use connection generation to reject stale work.
- Failure policy: If integrity cannot be proven, mark stale and stop claiming a valid historical interval. Prefer same-connection snapshot + ordered subsequent updates. Do not blindly splice REST snapshot to concurrent WebSocket deltas via timestamps. No documented Polymarket sequence/replay means some gaps cannot be proven absent.

## Component 2 transport contract — 2026-10-27

Component 2 means subscription management and raw WebSocket ingestion. Its acceptance boundary is an ordered raw-message consumer, not reconstructed books or SBE records. Discovery coverage limitations remain explicit and are not erased by a successful socket probe.

**Ownership and integration**

- Keep one assignment manager and one Tokio task owning each socket. The worker moves an owned text payload to the bounded ingress channel without financial decoding. Downstream placement and possible removal of this handoff can be evaluated when those components are authorized.
- Reuse Component 1's watch notifications and Snapshot command. Reconcile on startup, notifications and a finite periodic interval. Consume the notification before fetching state so a change during the fetch remains pending. Do not require a new change-log API. Snapshots describe current desire, not every intervening lifecycle transition or proof of continuous feed history.
- The manager owns assignments; each worker retains its assigned set across connections and separately tracks successfully requested assets for the current session. Apply an assignment command locally before attempting its wire change. A failed Add therefore remains desired; a Remove during backoff is reflected before the next subscription. Clear requested state on reconnect. A dead command receiver is a worker failure, not successful delivery.
- Keep existing assignments stable; initially place all outcomes of a Polymarket market together. Choose a worker with enough remaining instrument capacity for the entire market. If configured socket capacity is exhausted, report unassigned desire and fail coverage rather than overfill or silently omit assets. Do not implement automatic migrations or manager hot-restart recovery; a manager failure terminates the run in this baseline.
- Preserve adapter identity in worker/session context; do not mix namespaces without an unambiguous wire-to-adapter mapping. Use existing exact IDs. Keep only indexes needed for reconciliation/routing, rather than automatically duplicating both market and asset ownership maps.
- Extend the current discovery loop to support running until Stop without a duration deadline; keep run_for as the bounded validation entry point. Reuse scheduling and cancellation rather than adding a second discovery loop or an independent shutdown protocol.

**Session boundaries**

- Identify each session by a worker identity and a generation that is never reused while its work remains outstanding. Preserve receive order within the session. Use complete-message terminology, not frame terminology: WebSocket fragmentation is handled below this boundary.
- A new connection clears requested state. Removing then re-adding an asset rotates the connection rather than assuming an exchange-side unsubscribe/resubscribe ordering guarantee. Whether any message initializes a usable book is exclusively C4's decision.
- Successful subscription requests, raw messages and session boundaries share one ordered channel. Previously accepted messages are not discarded merely because a later command removed their asset. Late exchange messages are still forwarded unchanged; request metadata is not an exchange acknowledgement or proof that all subsequent messages concern active assignments.

**Bounded work and failure**

- Timestamp immediately after complete-message delivery, before queueing. Exchange timestamps remain untouched inside the raw payload; use monotonic timing for durations. Moving an owned Utf8Bytes avoids an extra String conversion, not all allocations or copies in the network stack.
- Bound initial/dynamic subscription batches, handshake/write/close waits, message size, channel capacity and total sockets. Derive a total memory budget from queued payload bounds plus per-socket buffers and in-flight messages; channel item count alone is insufficient. Treat limits as collector choices until protocol evidence supports them.
- An awaited send inside a select handler suspends that task's receive polling. Keep batches bounded and make shutdown interrupt connect, backoff and pending sends. A timed-out send has uncertain delivery: terminate that session instead of blindly retrying on it. Drive control ping/pong and close handling according to the pinned library; unexpected data-bearing message types must not be silently ignored.
- Use process-level fail-closed handling for queue saturation, a lost consumer or an unrecoverable worker/manager failure initially. Distinguish a full channel from a closed one. Record the received-but-unaccepted message boundary, stop all intake/reconnect scheduling, drain accepted work within a deadline and join tasks; report unsuccessful drain and return failure. An independent shutdown signal must not cause the consumer to throw away the accepted prefix. No recovery service or unbounded emergency queue is required.
- Ordinary connection failures may reconnect with capped exponential delay and jitter, cancellable during shutdown. Each interruption is an explicit history gap; a new snapshot restores state only. Do not present a successful reconnect as gap-free capture.
- Separate tasks do not by themselves isolate CPU work: uncached registry snapshots synchronously clone the map. The combined validation runner uses four Tokio threads. Measure receive stalls during discovery/reconciliation before claiming isolation or adding dedicated threads.

**Implementation order and acceptance**

1. Run the bounded protocol probe in protocol.md. Pin a compatible WebSocket/TLS dependency version and verify its buffer, cancellation and control-frame behavior. Save observed wire fixtures separately from synthetic failure cases.
2. Keep the acceptance boundary raw: preserve text bytes, timestamps, session identity and order without parsing financial values or requiring an initial book. Use a small diagnostic consumer, not a downstream production subsystem.
3. Implement one socket with initial subscription, timestamping, heartbeat, session boundaries and bounded shutdown. Test fragmented/batched messages, unexpected payload types, oversize input, stalled writes and shutdown during connect/backoff/write.
4. Integrate continuous discovery and stable assignments, then multiple sockets. Test failed Add followed by reconnect, Remove during backoff, duplicate commands, same-socket re-admission, adapter namespace isolation, startup/periodic reconciliation, unchanged assignments and capacity exhaustion.
5. Test full/closed queues, worker failure and accepted-prefix draining at a session boundary. Replay recorded traffic with independently offered bursts and discovery churn; compare accepted/processed counts, sustained throughput, memory, queue age and latency distributions. A fast enqueue into a slow consumer is not a throughput result.

Implementation ownership stays small: discovery lifecycle changes in src/discovery/polymarket.rs; socket and assignment logic in src/websocket/worker.rs and manager.rs with shared public types in mod.rs; deterministic local-server tests under tests/. Add dependencies, files and instrumentation only as their executable stage needs them. No Kalshi implementation, redundancy, automatic rebalancing, custom queues or extra compiler tuning is included.

## Implemented raw ingress and transport-only mode

`src/websocket/ingress.rs` defines `Started`, `SubscriptionsRequested`, raw `Data` and `Ended` events. Data carries `Session { worker, generation, adapter }`, per-session ordinal, application receive time and owned `Utf8Bytes`. Financially malformed or unknown text is deliberately preserved for the downstream decoder; application PONG and WebSocket control frames are handled by the worker.

The existing bounded crossbeam handoff remains; no replacement queue implementation is introduced. Defaults are 8,192 queued envelopes and 64 MiB of accounted bytes. A semaphore permit stays with each envelope through consumption. Accounting includes envelope size, raw text length and control-event vector/string capacities. Utf8Bytes hides backing allocation capacity, so this is **not** an allocation/RSS bound. Registry retention, allocator overhead and per-socket buffers are additional.

`src/main.rs --transport-only true` retains the **transport-validation runner**: a small consumer checks session/ordinal order, counts and discards raw messages, and drains until senders close. Its stdout stays empty. Default mode instead uses C3/C4 and C5 recording as described above; the supplied schema/reader are unchanged and the writer is adapted for stdout. Successful transport-only validation is not financial correctness, subscription confirmation or book initialization.

Binary/oversized data and full/closed ingress fail the run through an independent stop signal. Accepted envelopes still drain; an unavailable final boundary remains an explicit failure, not a clean end. SIGINT stops discovery, socket intake and reconnect scheduling; shutdown joins workers and allows a shared 15-second drain deadline.

Connection failures reconnect with 250 ms–30 s exponential delay plus up to 99 ms jitter. Heartbeats are text PING every 10 seconds, with a 30-second PONG timeout. Each socket allows 1,000 assets initially, sends at most 100 IDs per batch, accepts at most 2 MiB per complete message/frame, and caps writes/handshakes at 10 seconds. Up to 1,024 sockets are configured; no market is split across a socket budget. These are provisional collector budgets, not documented exchange maxima. C4 owns initialization/recovery deadlines and C5 enables periodic capture; load-based migration and redundant feed provisioning remain unimplemented.

Per-session raw messages remain in receive order. Different workers may interleave; no global chronological ordering or exchange-wide completeness is promised.

## Future full-pipeline capacity and failure budgets (not current implementation)

Measure discovered eligible assets, confirmed active subscriptions, initialized books, per-socket messages/bytes/sec, per-worker CPU, full-snapshot size, memory per book, output bytes/sec, queue high-water and oldest age, burst ratio, time to reconnect and resynchronize. Scale sockets by measured work rather than blindly allocating equal symbol counts. Do not assume undocumented max assets-per-socket limits.

Unbounded queue growth and silent coalescing are not acceptable. Choose finite capacity and a documented overload response (e.g. stop/declare integrity loss/reinitialize); backpressure alone cannot guarantee network capture when sinks are stalled. Output failures and partial writes must be handled without blindly duplicating partially written chunks.

## Future full-pipeline performance experiments

Compare (A) socket-local ownership versus separate book shards, (B) BTreeMap versus sorted Vec on recorded L2 level-count distribution, (C) typed Serde versus alternate parser, and (D) output buffering schedules. For each: same representative fixture, independent correctness oracle, same machine/release profile, sustained load and bursts, p50/p95/p99/p99.9, throughput, RSS, allocations and queue/flush latency. Prefer removal of work/copies over exotic data structures. Document a rejected optimization as well as an accepted one.
