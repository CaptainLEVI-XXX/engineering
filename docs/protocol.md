# Polymarket protocol notes and verification checklist

**Research baseline:** Original harness (2026-10-25), checked against accessible official documentation on 2026-10-26. **Treat these as documented behavior requiring verification against actual versioned fixtures when implementing.** Source of truth: current Polymarket API reference. No market-feed sequence/replay contract establishing gap-free historical capture has been verified.

## Discovery and market identity

- Official discovery: https://docs.polymarket.com/market-data/discover-markets and https://docs.polymarket.com/concepts/markets-events
- An **event** groups one or more **markets**; a binary market has two outcome positions, whose display labels need not be YES/NO (they can be team names). Preserve the API's outcome-to-ID mapping rather than inferring identity from labels. An event with many outcomes can group several binary markets. Do not confuse event ID, Gamma market ID, condition ID, and per-outcome WebSocket asset ID.
- `GET /markets/keyset` supports cursor/keyset pagination; pass `next_cursor` as `after_cursor` while preserving *all* filters. Check page integrity, repeated cursors, errors, and version coverage. Explicitly request `version=v1` and `version=v2` if supported by the current API; omission may default to v1.
- V1 markets expose `clobTokenIds` as a JSON-encoded string array. V2 markets expose `positionIds` as decimal-string arrays. Parse according to explicit protocol version. Never assume `market_id + 1` or adjacent token IDs; preserve the exact 256-bit outcome identifiers.
- The [Protocol V2 migration guide](https://docs.polymarket.com/migrate/polymarket-v2/overview.md), rechecked on 2026-10-26, explicitly says to retain CTF support; existing CTF holdings are not converted. Keep `PolymarketV1` and `PolymarketV2` as separate adapters/namespaces sharing Gamma transport and pagination. No verified V1 retirement date justifies dropping V1 discovery here. Adapter separation does not imply separate exchanges or duplicated HTTP implementations.
- Verify book collection eligibility by documented market fields and market-specific status. `closed=false` alone is an initial filter, not proof that every child book is tradable. Distinguish transient pauses from permanent retirement. Do not exclude normal binary constituents just because the containing event uses a negative-risk structure. Products like combos need an explicit scope decision.
- The field-level evidence and conservative collector policy are recorded below. Order-entry availability is not automatically book-feed eligibility. Unknown values remain explicit uncertainty, not implicit exclusion.
- A full paginated sweep can be non-atomic as markets change during pagination. Treat absence as a retirement *candidate*; never delete based on an incomplete sweep or scheduled end date alone. Add lifecycle hints plus occasional full reconciliation.
- Categories, tags, and sport-specific filters belong in optional historical metadata / strategy services, not in the order-book ingestion hot path. If metadata is captured, persist timestamped observations and their ID joins, not just today's classifications.

## Component 1 evidence and policy (2026-10-26)

Sources: [market details](https://docs.polymarket.com/market-data/market-details.md), [discovery guide](https://docs.polymarket.com/market-data/discover-markets.md), [keyset API](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination.md), [book reads](https://docs.polymarket.com/market-data/prices-order-books). The discovery guide documents version selection; the keyset OpenAPI parameter list omits `version`. Actual V1/V2 requests returned the corresponding version field. Preserve this documentation discrepancy rather than assuming the omitted parameter is invalid.

Official meanings: `active` means deployed/not archived; `closed` means resolved or closed with no further trading; `archived` means read-only/no trading or resolution updates; `acceptingOrders` describes new order entry. The guide recommends `active && !closed && acceptingOrders` for **trading readiness**, not a complete book-subscription policy. We require `enableOrderBook=true` for new admission. No documented feed-availability guarantee was established for every lifecycle combination.

| Evidence/input | Collector decision |
|---|---|
| Matching V1/V2, exactly two distinct IDs and labels, active/open/not archived/book-enabled | Admit both outcomes atomically |
| Same status but acceptingOrders false or null | Admit/retain conservatively; does not prove that a live feed is available |
| Closed or archived, matching identity, acceptingOrders not true | Existing entry retires only after a targeted `closed=any` lookup; new entry is not admitted |
| Closed/archived but acceptingOrders true | Contradiction: retain desire and verify |
| Active/book-enabled false on an otherwise open market | Never-admitted: no admission or targeted polling, even with absent IDs. Previously admitted: retain desire and verify |
| Required lifecycle/identity fields null, absent, malformed, conflicting or wrong-version | Retain known desire; mark uncertainty and verify identifiable markets |
| Retired entry appears eligible in listing | Request verification; matching targeted eligible evidence can reactivate |
| Missing targeted result, HTTP error, partial scan, repeated absence, or past end date | No retirement/reactivation authority |

`tests/fixtures/gamma-markets.json` contains six reduced real responses (identity/status/outcomes only), fetched on 2026-10-26. Public query provenance, all under `https://gamma-api.polymarket.com/markets/keyset`:

- `?version=v1&closed=false&limit=100`: market `559651`; both ID fields populated with different values, proving selection must use version.
- `?version=v2&closed=false&limit=100`: market `5931604`; team-name outcome labels, not literal Yes/No.
- `?version=v1&closed=true&limit=5`: market `12`; active and closed both true, book/order-entry fields null.
- `?version=v2&closed=true&limit=5`: market `5662873`; active and closed both true, book enabled, order entry false.
- `?version=v1&closed=any&id=559651&id=12&limit=100` returned both requested open and closed identities. The API allows up to 100 identifiers per filter and limit 100; targeted reads preserve filters across cursors.
- `?version=v1&closed=any&id=964332&id=2290078&limit=100`: `964332` is active/open but book-disabled, with valid IDs; `2290078` is active/open/book-enabled but has null token/position IDs. These are distinct non-admission reasons, not evidence that either market has resolved. Tests also cover a synthetic disabled market with missing IDs.

Tests mutate those fixtures for pauses, reactivation, identity conflicts and malformed data. **Those mutations are synthetic, not observed lifecycle histories.** No representative Polymarket paused/reopened/archived transition was captured. [Kalshi lifecycle documentation](https://docs.kalshi.com/getting_started/market_lifecycle) explicitly allows closed→active reopening; that is a warning against generic permanent tombstones, not evidence that Polymarket behaves identically. No Kalshi adapter is implemented.

### Running Component 1

The separate discovery binary reads public Gamma data only; it does not start WebSockets, emit SBE, or claim subscribed coverage. Default duration is 120 seconds; `--once` stops after the initial sweep and currently due targeted work, or the deadline. Reports go to stderr; stdout remains empty. SIGINT cancels the active request/backoff and reports the retained state.

```sh
cargo test --locked --test discovery --test discovery_http
cargo test --locked --release --test discovery_http live_discovery_coverage -- --ignored --nocapture
cargo run --locked --release --bin discovery -- --seconds 120 --once --strict > /dev/null
cargo test --locked --release --test discovery large_universe_measurement -- --ignored --nocapture
```

The ignored live probe and `--strict` require clean completed V1/V2 traversals, nonempty desired state, zero unresolved scan records, pending/uncertain markets, conflicts, failed verification batches, expired unknown jobs and overflow. A timeout or Stop fails strict acceptance. Without `--strict`, the CLI remains a connectivity/progress probe, not an acceptance test. Historical unresolved scan records are not erased by a later successful targeted lookup. Reports distinguish last-sweep scan counts from owner-lifetime verification/error counters. There is no subscription manager or book initializer yet, so no confirmed-subscribed or initialized counts are claimed.

Unknown verification is bounded to three rounds within five minutes and 1,024 retained jobs. Repeated observations do not extend a job; after expiry a new observation may create a fresh bounded job. Known desired subscriptions never expire merely because verification fails. See [architecture.md](architecture.md#discovery-and-reconciliation-protocol) for capacity, backoff, publication and overload contracts.

Validation on 2026-10-26: a 120-second release-mode strict live test **failed acceptance**, honestly: V1 read 279 pages / 27,900 records without reaching the end; V2 completed 2 pages / 132 records. The registry desired 55,868 assets, with 44 unresolved records / pending identities, no known-market uncertainty, identity conflicts, overflow or failed verification batches. This is observed progress, not complete market coverage. The disabled-market regression fails on the original implementation; deterministic tests also cover retry/age/capacity boundaries, duplicate hints, known-state preservation and final notifications. Deliberately weakening attempt/capacity limits or suppressing notifications makes the regressions fail.

Control-plane measurement before the adapter-boundary refactor on this macOS ARM64 host, Rust `1.93.0-nightly (cc3eee7fb 2025-11-28)`, release profile: seven samples of 50,000 synthetic markets / 100,000 unique 77-digit asset IDs, pages of 100, same workload and expected count before/after the verification/publication fixes. Median total page ingestion: 124,133 → 125,174 µs; first full snapshot: 9,198 → 11,425 µs; cached snapshot requests: approximately 3 → 4 ns per call averaged over 10,000 requests. **No demonstrated throughput gain.** These runs do not establish a snapshot bottleneck under actual consumer demand. No persistent-map optimization is justified yet.

After adapter separation, all 20 deterministic tests passed in debug/release, including overlapping native IDs across adapters, V2 targeted routing, wrong-version rejection and single-instrument registry records. A 30-second live smoke run reached both adapter namespaces: V1 read 67 pages / 6,700 records without completing, V2 completed 2 pages / 132 records, and 44 records remained unresolved. Strict acceptance correctly failed; stdout stayed empty. The same seven-sample synthetic workload measured median ingestion 108,397 µs, first snapshot 14,460 µs and cached requests approximately 3 ns. These are local observations, not an end-to-end latency claim; snapshot creation was slower than in the preceding run. The 120-second acceptance test above predates this refactor and was not rerun as part of the adapter change.

First publication measures process-local discovery time, not exchange listing-to-subscription delay; page-apply maximum is one observation, not a latency percentile. The synthetic measurements exclude network, sustained churn, RSS and concurrent consumers. Full coverage and low latency remain unproven; next validate a sufficiently long complete traversal and resolve/classify remaining upstream missing-ID records without inventing subscription IDs.

## Market WebSocket semantics

Source: https://docs.polymarket.com/api-reference/wss/market and https://docs.polymarket.com/market-data/realtime-data

- Market channel: `wss://ws-subscriptions-clob.polymarket.com/ws/market` (check current docs). Market-data feed is public; unlike authenticated trading/order endpoints, subscription does not require trading credentials.
- Current documented initial subscription shape uses `type: "market"` and `assets_ids`; documented options include full L2 and initial book dump (`level: 2`, `initial_dump: true`). Dynamic subscription uses `operation: "subscribe"`/`"unsubscribe"` with asset IDs. Validate actual wire schema and server acknowledgement/rejection behavior before relying on it.
- Application-level text `PING`/`PONG` (historically 10-second cadence) is distinct from WebSocket control ping/pong frames. Check heartbeat requirements with the deployed endpoint and client library.
- A `book` event is a full **replacement** of that outcome's order book, not a merge. A `price_change` contains one or more absolute price-level quantities. `size=0` removes the level. The feed may batch updates for more than one asset. Validate the *entire relevant event* before commit to avoid a half-applied malformed batch.
- `BUY` refers to bid side, `SELL` to ask side. A `last_trade_price` notification must not be treated as a delta removing traded quantity. Handle `tick_size_change` without silently losing valid levels or assuming a permanent static price grid.
- Preserve receive/wire order *within a connection* and order within batches; don't sort by timestamps and don't deduplicate solely because two events share timestamp or summary hash. Test both actual top-level object and array shapes encountered in fixtures.
- Exchange times on relevant book/price events have been documented in Unix **milliseconds**; verify by event type. Repository SBE expects microseconds. Convert with checked arithmetic; multiplying by 1,000 does not improve measurement precision.
- Optional lifecycle notifications (previously enabled using `custom_feature_enabled`) can improve responsiveness, but they are not an authoritative complete universe-wide history. Reconcile with REST.
- No authoritative maximum `assets_ids` per CLOB socket was established in the original research. Probe controlled subscription batches under published service limits; do not import limits from Perps, RTDS, PolyBolt, or REST APIs.

## Snapshots, recovery and uncertainty

- Prefer the initial WebSocket `book` snapshot followed by ordered updates **on the same connection** as the baseline. A REST snapshot from a different timeline is **not** a safe merge boundary without a documented sequence protocol.
- A new valid snapshot can reestablish present state, but cannot reconstruct which historic deltas were missed. No assumed sequence number, exchange clock, timestamp, or book hash can by itself prove losslessness.
- On disconnect, malformed update, or detected inconsistency, mark the relevant book/source stale, surface an uncertainty interval, reinitialize or switch to a valid independent replica, and emit a new snapshot before downstream deltas.
- Some quiet markets have no changes; use socket health, positive protocol evidence and optional redundant comparison before calling a quiet subscription stale. Avoid futile resubscribe loops.

## Implementation checklist

1. Save actual sanitized samples for V1/V2 discovery records, initial subscription and confirmation, `book`, `price_change`, `last_trade_price`, `tick_size_change`, lifecycle and malformed payloads.
2. Confirm `assetId` normalization from each version and payload variant, including multi-market events and both outcome assets of binary markets with YES/NO and non-YES/NO display labels.
3. Verify every documented field against the deployed API and the exact SDK version used. Date-stamp findings and correct this document rather than hiding surprises in code.
4. Rate-limit queries and reconnects, jitter retries, report rejections, and preserve existing registry on discovery failure.
5. Separate protocol facts from performance hypotheses. No tag enrichment, scoring or strategy filtering is required to parse or apply order-book deltas.
