# Collector architecture and decision record

Status: Design hypothesis, not measured final architecture. Date: 2026-10-26. Scope: Voluntas challenge, Polymarket initially. The published challenge and actual template schema remain authoritative.

## Boundaries

```text
Gamma REST ---------------------------┐
      ↓                               │ control plane
Exchange discovery adapter            │
      ↓                               │
Desired instrument registry           │
      ↓ reconciled desired state       │
WebSocket subscription manager <------┘
      ↓ confirmed socket assignments
Multiple WebSocket workers (independent connection generations)
      ↓ receive timestamp → parse → validate → apply to owned L2 book
Bounded ordered output channel (explicit overload policy)
      ↓
Single SBE writer → binary stdout

Optional: external point-in-time metadata index → category-based backtesting joins.
```

Discovery is not market data: Gamma tells us which instruments to listen to, not each live order-book update. The WebSocket stream provides initial/refresh snapshots and price-level changes; the collector produces periodic local snapshots without repeatedly calling REST for them.

## Component 1: Minimal instrument discovery and desired-state registry

### Identity

- FeedKey is an opaque exchange-native subscription key with exchange and version/namespace: conceptual variants Polymarket { version, asset_id } and future Kalshi { market_ticker }. Treat Polymarket V1 CTF token IDs and V2 position IDs as distinct namespaces. Do not infer adjacent IDs, sequential creation, or tags from IDs.
- Instrument records only what's needed to manage capture and label output: FeedKey, parent market reference, outcome/side identity if documented, protocol/version, eligibility and any required SBE symbol mapping.
- Full external IDs may exceed 128-bit integers; preserve exact strings or validated 256-bit values. If the SBE helper uses narrower internal symbols, assign and persist a collision-free reversible mapping before encoding. Avoid reusing compact IDs ambiguously across sessions.
- Do not automatically store categories, liquidity, descriptions, titles or full events in the hot-path registry. For category-filtered backtests, an optional independent metadata store joins by the canonical identity and keeps point-in-time observations. "First observed" does not imply "first changed at exchange."
- Component 1 uses one authoritative `HashMap<MarketKey, MarketEntry>` where each entry owns its two outcome identities. `MarketKey` contains protocol version and exact Gamma ID. A derived asset-to-market index detects identity reuse; it is not a second desired/active set. Temporary sets track scan absence and cursors. One mutable owner commits both outcomes together, without a mutex shared by socket workers.

### Discovery policy — incremental publication, verified lifecycle changes

The implementation policy and fixture provenance are in [protocol.md](protocol.md#component-1-evidence-and-policy-2026-10-26). This supersedes the earlier complete-scan-only publication baseline. Missing or contradictory data is not a silent false; order-entry availability is not a proof of feed availability.

Initial inclusion requires matching supported version, two distinct exact decimal-string outcome IDs, two labels mapped by index, `active=true`, `closed=false`, `archived=false`, and `enableOrderBook=true`. `acceptingOrders=false` alone does not exclude a book. This deliberately conservative listening policy is not proof that a paused market accepts WebSocket subscriptions.

| Input case | Desired-state decision | Evidence / follow-up |
|---|---|---|
| New valid eligible market | Publish both outcomes immediately | Do not wait for other pages or versions |
| Malformed or uncertain record with identifiable market or known asset | Preserve prior desired state, mark known entries uncertain, queue targeted lookup | Unidentifiable records count as unresolved; never invent an identity |
| Identity collision or changed outcome mapping | Preserve existing identity/desire; mark affected known owners uncertain; verify all identifiable candidates | No half-market insertion or automatic identity remapping |
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
5. Every observable registry change, including uncertainty changes, invalidates the lazy cached `Arc<Snapshot>` and sends a watch notification. Snapshot copies occur on consumer request after mutation, not for every addition. Old snapshots remain immutable. A full snapshot is authoritative desired state, not a historical change log.
6. The future subscription consumer reads full state on startup/restart, change notification and a finite periodic interval independent of scan success. This repairs lost/coalesced notifications without a durable event log. The periodic consumer loop is not implemented here; restarting the registry process requires rediscovery.
7. Requests have at most three attempts, a 15-second attempt timeout, jittered backoff and server `Retry-After` handling. A run deadline/Stop cancels HTTP or backoff. Failed/unresolved targeted IDs retry no sooner than 30 seconds; sweeps resume 60 seconds after completion. Targeted work runs between pages and while waiting between sweeps. Snapshot/Hint/Stop commands remain serviceable during awaits.

The command channel has capacity 64; senders await capacity rather than dropping commands. Watch notifications coalesce by design because consumers fetch current state. Pending verification is deduplicated by market identity, but not size-capped; it and retained history need memory measurement on sustained churn. Timing/counter reports describe the last sweep and current registry, not confirmed subscriptions. A single in-flight request may delay another version during backoff; evaluate parallelism only if measured discovery delay warrants it.

Do not require category metadata, matching-engine structures, lock-free registries, advanced compiler flags or generic plugin frameworks for Component 1.

### Implemented interface

```rust
// Abbreviated; see src/discovery/{mod,polymarket}.rs.
struct MarketKey { version: Version, id: String }
struct MarketEntry {
    outcomes: [Outcome; 2],
    desired: bool,
    uncertainty: Option<String>,
}
struct Snapshot { markets: HashMap<MarketKey, MarketEntry> }
enum Command { Snapshot(oneshot::Sender<Arc<Snapshot>>), Hint(MarketKey), Stop }
// Discovery::new(Gamma) -> (owner, bounded command sender, watch receiver)
// owner.run_for(duration, once) -> DiscoveryReport
```

Don't start a generic plugin framework for a single adapter. Extract stable interfaces only when needed; Kalshi's subscription granularity is different, so don't fake an identical Polymarket-style outcome-book model.

### Component 1 acceptance gate

Use deterministic fixture → expected desired-state tests. At minimum:

- Complete V1/V2 multipage discovery yields the exact eligible asset set; opaque IDs and market/outcome relationships are preserved, including non-YES/NO display labels where present.
- The fixture-backed eligibility matrix distinguishes feed eligibility from order acceptance, covers missing/null/conflicting fields and unavailable IDs, and explicitly marks unresolved cases unknown. Synthetic pauses/reactivations test collector policy, not exchange guarantees.
- Repeated identical scans are idempotent; new validated instruments are added exactly once, without relying on sequential IDs.
- A failed page, repeated cursor, malformed/unknown required fields, contradictory mapping or unsupported version cannot cause a mass unsubscribe or be silently counted as complete coverage.
- A malformed record cannot block unrelated valid additions. Prove a consumer sees page-one additions before the rest of pagination completes; later page/version failures cannot erase them.
- Identity conflicts and uncertainty changes invalidate cached snapshots and notify consumers while preserving previous immutable snapshots. Both outcomes commit or neither does.
- A known paused/unknown market retains desire and gets targeted verification. Closed targeted evidence retires it; only matching targeted eligible evidence reactivates it. Missing results and changed IDs cannot reactivate or retire.
- A consumer can recover current desired state by requesting a full snapshot without replaying notifications. When the actual subscription consumer is added, test periodic repair after a missed final notification with a controlled clock.
- Registry work stays in the control plane and does not require categories/tags or a live WebSocket to run tests.

## Later stages: Ingestion baseline, not premature final design

- Start with multiple Tokio WebSocket workers, each owning its assigned instruments' L2 books, to minimize channel handoffs. Alternative: separate sharded book-owner workers. Select using controlled end-to-end benchmarks. Avoid task-per-message and global mutable book maps.
- Timestamp after full WebSocket-message delivery, before parse, keep same timestamp for items in its batch. Route/commit per-asset changes in stream order; cross-socket global timestamp order is not guaranteed. If backtester requires global ordering, document and benchmark a specific policy, not an arbitrary sort.
- Explicit book state machine: Discovered → Subscribing → Initializing → Live → Stale/Resynchronizing → Live; retirement requires verified lifecycle evidence. Verified reactivation must initialize a fresh book, not resume stale state. Distinguish socket liveness from per-asset subscription health; inactivity may reflect a quiet market.
- Feed redundancy (extension): independent book replicas; one publishing authority; a controlled snapshot boundary on failover; never combine the "fastest" deltas from two connections into one state. Use connection generation to reject stale work.
- Failure policy: If integrity cannot be proven, mark stale and stop claiming a valid historical interval. Prefer same-connection snapshot + ordered subsequent updates. Do not blindly splice REST snapshot to concurrent WebSocket deltas via timestamps. No documented Polymarket sequence/replay means some gaps cannot be proven absent.

## Output and ordering contract to resolve early

The provided example's AlignedSbeWriter accepts a file path and emits zero-padded fixed chunks, while the challenge asks for binary stdout. Confirm helper framing, record limits, sentinel, flush and reader compatibility. Use a minimal sink adapter preserving the provided codec; never invent a new SBE schema or claim a reader works without round-trip proof. Low-traffic streams must not buffer indefinitely. Benchmark batching vs observable delivery delay and padding bandwidth.

Per-asset records must replay in applied order; book replacement/failover must emit a new snapshot. Across assets, the writer may interleave source workers; unless the consumer formally requires a global clock order, do not pay for speculative global reordering. Expose diagnostics/uncertain intervals outside the required SBE data stream if schema can't express them; stdout stays binary only.

## Capacity and failure budgets (determine empirically)

Measure discovered eligible assets, confirmed active subscriptions, initialized books, per-socket messages/bytes/sec, per-worker CPU, full-snapshot size, memory per book, output bytes/sec, queue high-water and oldest age, burst ratio, time to reconnect and resynchronize. Scale sockets by measured work rather than blindly allocating equal symbol counts. Do not assume undocumented max assets-per-socket limits.

Unbounded queue growth and silent coalescing are not acceptable. Choose finite capacity and a documented overload response (e.g. stop/declare integrity loss/reinitialize); backpressure alone cannot guarantee network capture when sinks are stalled. Output failures and partial writes must be handled without blindly duplicating partially written chunks.

## Performance experiment decisions

Compare (A) socket-local ownership versus separate book shards, (B) BTreeMap versus sorted Vec on recorded L2 level-count distribution, (C) typed Serde versus alternate parser, and (D) output buffering schedules. For each: same representative fixture, independent correctness oracle, same machine/release profile, sustained load and bursts, p50/p95/p99/p99.9, throughput, RSS, allocations and queue/flush latency. Prefer removal of work/copies over exotic data structures. Document a rejected optimization as well as an accepted one.
