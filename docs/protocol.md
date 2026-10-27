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

The ignored live probe and `--strict` require clean completed V1/V2 traversals, nonempty desired state, zero unresolved scan records, pending/uncertain markets, conflicts, failed verification batches, expired unknown jobs and overflow. A timeout or Stop fails strict acceptance. Without `--strict`, the CLI remains a connectivity/progress probe, not an acceptance test. Historical unresolved scan records are not erased by a later successful targeted lookup. Reports distinguish last-sweep scan counts from owner-lifetime verification/error counters. This discovery-only executable does not start Component 2; no confirmed-subscribed or initialized counts are claimed.

Unknown verification is bounded to three rounds within five minutes and 1,024 retained jobs. Repeated observations do not extend a job; after expiry a new observation may create a fresh bounded job. Known desired subscriptions never expire merely because verification fails. See [architecture.md](architecture.md#discovery-and-reconciliation-protocol) for capacity, backoff, publication and overload contracts.

Validation on 2026-10-26: a 120-second release-mode strict live test **failed acceptance**, honestly: V1 read 279 pages / 27,900 records without reaching the end; V2 completed 2 pages / 132 records. The registry desired 55,868 assets, with 44 unresolved records / pending identities, no known-market uncertainty, identity conflicts, overflow or failed verification batches. This is observed progress, not complete market coverage. The disabled-market regression fails on the original implementation; deterministic tests also cover retry/age/capacity boundaries, duplicate hints, known-state preservation and final notifications. Deliberately weakening attempt/capacity limits or suppressing notifications makes the regressions fail.

Control-plane measurement before the adapter-boundary refactor on this macOS ARM64 host, Rust `1.93.0-nightly (cc3eee7fb 2025-11-28)`, release profile: seven samples of 50,000 synthetic markets / 100,000 unique 77-digit asset IDs, pages of 100, same workload and expected count before/after the verification/publication fixes. Median total page ingestion: 124,133 → 125,174 µs; first full snapshot: 9,198 → 11,425 µs; cached snapshot requests: approximately 3 → 4 ns per call averaged over 10,000 requests. **No demonstrated throughput gain.** These runs do not establish a snapshot bottleneck under actual consumer demand. No persistent-map optimization is justified yet.

After adapter separation, all 20 deterministic tests passed in debug/release, including overlapping native IDs across adapters, V2 targeted routing, wrong-version rejection and single-instrument registry records. A 30-second live smoke run reached both adapter namespaces: V1 read 67 pages / 6,700 records without completing, V2 completed 2 pages / 132 records, and 44 records remained unresolved. Strict acceptance correctly failed; stdout stayed empty. The same seven-sample synthetic workload measured median ingestion 108,397 µs, first snapshot 14,460 µs and cached requests approximately 3 ns. These are local observations, not an end-to-end latency claim; snapshot creation was slower than in the preceding run. The 120-second acceptance test above predates this refactor and was not rerun as part of the adapter change.

Later on 2026-10-26, a gzip-enabled release run completed both traversals in 694,719 ms: V1 2,424 pages / 242,317 records; V2 2 pages / 133 records; 484,704 desired instruments. There were 2,426 listing attempts and 6 verification attempts, no scan errors or identity conflicts, and 44 unresolved records. Strict acceptance failed; stdout stayed empty. Both keyset and legacy targeted lookups returned all 44 with missing outcome IDs and empty condition IDs. This run included an experimental cooldown, subsequently removed as unjustified; it is not post-revert live validation. Evidence: `.amp/in/artifacts/discovery-fixed-live.log`, `missing-ids-recheck.json`, and `missing-ids-legacy-check.json`.

Gzip reduced one sampled response from 549,216 to 46,907 transferred bytes (about 91.5%). Identity/lifecycle fields and cursor matched; volatile metadata differed, so this is not a controlled latency benchmark. The retained gzip fixtures wrap the first reduced market fixture, with the oversized variant padded to 8 MiB + 1 decompressed byte; both use mtime=0. Local HTTP tests verify admission and rejection against the decompressed size limit.

First publication measures process-local discovery time, not exchange listing-to-subscription delay; page-apply maximum is not a latency percentile. Completed pagination has been observed, but full coverage and low latency remain unproven. Preserve the original bounded retries and explicit upstream identity gaps; do not manufacture IDs or relax the strict coverage report.

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

### Ingestion preflight — executed with limitations (2026-10-27)

The official market-channel specification and realtime-data guide were rechecked on 2026-10-27. They document text PING every 10 seconds, initial_dump defaulting to true, level defaulting to 2, dynamic subscribe/unsubscribe, and custom_feature_enabled for lifecycle events. These are documentation checks, not new live fixtures. Neither a subscription acknowledgement contract nor an authoritative assets-per-CLOB-socket ceiling was established by this review.

Before committing the transport implementation to wire assumptions, run a read-only probe bounded to five minutes total, one connection at a time, and at most two binary markets per adapter (four outcome IDs). Resolve fresh IDs using a small bounded Gamma lookup for each version; do not blindly reuse old fixture IDs. If suitable IDs or connectivity are unavailable within the budget, report that adapter unverified rather than expanding the scan indefinitely. No credentials or trading requests are needed; keep TLS verification enabled, stdout empty and captures under .amp/in/artifacts/.

- Subscribe to one market with explicit level=2 and initial_dump=true. Record sent requests and complete received messages with application-receive timestamps. Observe text PONG separately from WebSocket control frames, initial books, actual object/array envelopes and any rejection messages.
- Add the second market without reconnecting. Then remove and re-add one market on the same socket. Record whether each operation supplies fresh snapshots, whether updates arrive around removal, and whether a safe reinitialization boundary can be established. A quiet interval is not proof of unsubscribe acceptance, and a successful send is not an acknowledgement.
- Reconnect once per adapter with the latest assigned set and confirm fresh initialization. Stop at the deadline even if a market is silent. Missing observations remain unresolved; a sample does not prove exchange-wide ordering, capacity or losslessness.
- Check the chosen Rust WebSocket/TLS version against the repository toolchain. Test fragmentation, oversize input, stalled sends, full/closed queues and shutdown deterministically on a local server; do not attempt to induce exchange overload. Document collector message/socket/batch budgets separately from exchange limits.

This preflight can settle representative wire behavior, not full-universe capacity or p99 latency. Those require the integrated replay/load tests in architecture.md. If same-socket re-subscription lacks sufficient boundary evidence, do not silently approve it; select and test a fresh-connection recovery path before enabling that lifecycle transition.

#### Observed evidence

A throwaway Rust probe used tokio-tungstenite/tungstenite 0.28.0, Tokio 1.52.3 and Rustls with WebPKI roots on the repository's Rust 1.93.0-nightly macOS ARM64 toolchain. No TLS verification bypass, credentials or trading calls were used. Its isolated manifest/lockfile did not change application dependencies. Two executions took approximately 62 and 48 seconds, one socket at a time, with four outcome IDs per adapter. Stdout stayed empty. The first V1 Gamma lookup failed with a connection reset; only V1 was retried after the V2 probe completed.

| Observation | V1 | V2 |
|---|---|---|
| Freshly selected Gamma market IDs | 561268, 559700 | 6245152, 6245150 |
| Initial subscription | Both requested books received | Both requested books received |
| Dynamic addition | Both added books received | Both added books received |
| Same-socket re-addition | Both re-added books received | Both re-added books received |
| Reconnect | Handshake failed with HTTP 525; initialization unverified | All four requested books received |
| Captured book events / text PONGs | 6 / 4 | 10 / 5 |
| Book contents | Nonempty, asymmetric sides (72 or 46 levels) | Empty bids and asks |
| Price-change events | None observed | None observed |

All seven data messages were top-level JSON arrays. Their observed maximum text sizes were 5,284 bytes (V1) and 1,394 bytes (V2), not safe production size ceilings. Re-addition snapshots repeated the initial exchange timestamps: a newly received initialization snapshot must not be discarded merely because its timestamp is old or unchanged. No explicit subscription/unsubscription acknowledgements were observed. The quiet sample cannot prove unsubscribe effectiveness or safe ordering between late deltas and reinitialization snapshots. Both V2 sessions and the first V1 session completed their close handshake; the failed V1 reconnect was not retried into a manufactured success.

Five deterministic in-memory client/server/channel checks passed in debug and release: fragmented UTF-8 with an interleaved control ping and automatic pong; cancellation of a partial receive; exact-size versus oversized reassembled messages; cancellation of a stalled send; and full-versus-closed channel behavior with ordered draining. The stalled-send check demonstrated that cancelling a pending send does not roll back buffered/partially written data: a later flush delivered the original message. Scratch formatting and strict Clippy passed. These established dependency behavior, not acceptance of a production worker at that stage. See the integrated evidence below for subsequent worker tests.

The probe capped incoming messages/frames at 2 MiB, the write buffer at 256 KiB, retained serialized capture at 32 MiB/20,000 entries, and individual HTTP/connect/send/close waits. These were small-probe safety limits, not measured full-universe budgets. Capture formatting and debug-build execution make its timings unsuitable for latency claims. Evidence and the scratch reproducer are under `.amp/in/artifacts/ws-preflight*`: raw messages, fresh Gamma pages, summary assertions, debug/release/Clippy logs and a source/lockfile archive. No permanent collector or benchmark framework was added.

**Scope decision:** implement raw WebSocket transport only. These observations do not authorize a book engine or SBE pipeline. V1 reconnect, per-asset acknowledgements, capacity and stressed ordering remain unverified. This probe does not resolve Component 1's 44 missing-ID records or establish low latency/losslessness.

### Component 2 boundary correction (2026-10-27)

An earlier local implementation exceeded Component 2 by adding financial decoding, book reconstruction, periodic snapshots and SBE output. Those implementations and their dedicated tests/fixtures have been removed. The original template schema/codecs/file helpers remain; the stdout-writer extension was reverted. Historical `book-*` and `ingestion-*` artifacts describe that removed implementation and are **not current acceptance evidence**.

The current worker timestamps and forwards complete text payloads byte-for-byte using owned Utf8Bytes, excluding application PONG. It carries adapter/session identity, per-session ordinals, successful subscription-request context and ordered lifecycle boundaries. It deliberately does not reject pre-snapshot deltas, unknown event types or financially invalid JSON; that belongs to a future downstream decoder. Binary and oversized messages still fail visibly. Requests are not exchange confirmations.

The transport-boundary regression first failed against the coupled worker because it attempted JSON/book decoding. Local-server tests now exercise raw text preservation, timestamp bounds, ordered generations and subscription context, dynamic additions, fresh-connection reactivation, heartbeat failure, bounded queues, accepted-prefix retention and missed-notification repair. The executable integration test uses real local Gamma/WebSocket servers, overlapping native IDs across adapters, deadline and SIGINT shutdown, exact drained-message counts and empty stdout. It does not require a book or SBE reader.

The executable is explicitly a diagnostic consumer: raw payloads are counted, checked for session order, then discarded. It is not a persistent capture tool. The opt-in `tests/ingestion_load.rs` now checks 100,000 raw messages in sequence using independently offered 40-message bursts every millisecond, one loopback worker and a small checking consumer. It measures monotonic queue age and total drain time, not financial correctness, exchange latency, maximum capacity or an SBE pipeline. Its workload is different from the removed SBE test; do not claim a speedup by comparing them.

Post-removal checks passed on the same macOS ARM64 / Rust 1.93.0-nightly host: formatting, locked all-target check, strict Clippy and all 33 default deterministic tests in debug and release. The explicit raw-load run preserved all 100,000 messages in order, draining in 2.504461 seconds (39,929 messages/s at a nominal 40,000/s offered rate). Queue-age p50/p95/p99/p99.9/max were 5/432/7,382/17,617/17,637 µs; maximum producer scheduling lag was 18,977 µs. This small loopback workload excludes TLS, representative exchange payload sizes and discovery churn. No low-latency or speedup claim follows from it. Logs: `.amp/in/artifacts/component-scope-checks.log`.

Live exchange validation and the large-registry measurement were not rerun for this scope correction. Earlier observations below do not replace those acceptance checks.

### Earlier live observations — not post-removal acceptance

- The explicit 120-second strict discovery test failed: 56,502 desired assets, V1 at 279 pages / 27,900 records without finishing, 44 unresolved records; V2 completed five pages / 449 records. No identity conflicts. Listing attempts: 286; verification attempts: six. These are discovery counts, not subscription counts.
- A bounded repeat of the wire probe lasted 125.7 seconds. V1 Gamma failed with a connection reset. V2 supplied initial/add/re-add/reconnect snapshots and text PONGs, but no deltas. It used the earlier isolated Tokio 1.52.3 probe harness, not the current raw worker.
- The now-removed integrated executable ran for 70 seconds on a restricted real V2 sample of four assets. Its book/SBE observations do not establish acceptance of the current raw transport. No live delta correctness or full-universe throughput was proven.

## Future book component: snapshots, recovery and uncertainty

- Prefer the initial WebSocket `book` snapshot followed by ordered updates **on the same connection** as the baseline. A REST snapshot from a different timeline is **not** a safe merge boundary without a documented sequence protocol.
- A new valid snapshot can reestablish present state, but cannot reconstruct which historic deltas were missed. No assumed sequence number, exchange clock, timestamp, or book hash can by itself prove losslessness.
- On disconnect, malformed update, or detected inconsistency, mark the relevant book/source stale, surface an uncertainty interval, reinitialize or switch to a valid independent replica, and emit a new snapshot before downstream deltas.
- Some quiet markets have no changes; use socket health, positive protocol evidence and optional redundant comparison before calling a quiet subscription stale. Avoid futile resubscribe loops.

## C3/C4 implemented contracts and fixture evidence (2026-10-27)

This section supersedes the historical transport-only executable description,
not its transport boundary. Workers still forward owned complete text without
financial parsing. The default consumer now decodes and reconstructs books;
`--transport-only true` preserves raw validation. Neither mode persists history.

### Wire interpretation and failure scope

- Decimal strings use unsigned digits, optional fractional digits, checked i64
  arithmetic and scale 1,000,000. Extra fractional zeros are exact; nonzero excess
  precision, signs, exponent notation and rounding are rejected. Escaped JSON
  strings are decoded before numeric interpretation. Polymarket prices are 0–1;
  generic storage constructors do not impose that probability ceiling.
- `book` requires asset ID, millisecond timestamp, bids and asks. Duplicate prices
  on one side are rejected, including duplicates involving zero quantities.
  Zero snapshot quantities are omitted. `price_change` requires a timestamp and
  bounded `price_changes`; each entry requires asset ID, BUY/SELL, price and size.
  Sizes are absolute, zero deletes, and repeated/no-op entries remain observations.
- Exchange milliseconds convert to checked microseconds **within the supplied
  Chrono-backed reader's range**, not merely within u64. Missing time is not
  replaced with receive time. Original C2 receive time is preserved separately.
- `tick_size_change` validates and retains the new tick; it does not rescale or
  rebuild levels. `last_trade_price`, `best_bid_ask`, `new_market` and
  `market_resolved` are recognized non-book traffic, not book liveness or deltas.
  Initial snapshot tick metadata, hashes and market IDs are not correctness
  checks; unused exchange metadata is ignored. This is financial-field validation,
  not strict validation of every field in the exchange schema.
- Full outer JSON syntax is validated first. Independent array elements then run
  in order; one malformed financial event rejects all its changes and invalidates
  its reliably identified source scope. A bounded cold-path identity scan handles
  failures before later assets were decoded. Ambiguous/unknown scope invalidates
  the source, never an independent backup. Valid inactive/uninitialized entries
  are accounted without applying them. Resource exhaustion fails the run.

### Assignment, selection and recovery

Successful requests carry slot/revision context. The manager also emits an
ordered `Retired` assignment cutoff **before dispatching removal**, independent
of WebSocket connectivity; it is not an unsubscribe acknowledgement. Without
this cutoff, retirement during failed I/O could leave a stale replica requiring
recovery forever. Later successful unsubscribe notifications are idempotent.
Older revisions cannot retire a newer cross-worker binding. C2 session/ordinal
violations remain terminal; obsolete assignment data on an active session is
inapplicable. Same-revision reconnection requires a fresh snapshot.

Books use separate BTreeMap sides per replica. One processor prepares changes,
preflights intermediate level counts, commits them, then synchronously exposes
ordered selected-source observations. Numeric preparation buffers are reused;
wire event arrays remain bounded temporary allocations. Backup books are updated
without duplicating selected output. Explicit selection always emits a snapshot
on a source change, including equal books, preserving state provenance and a
separate selection time. Every observation carries its processing-input index;
financial observations additionally carry the source/event/entry cursor.
Publication failure is terminal: already committed input must not be retried.

Initialization defaults to 30 seconds, total unavailable time to 120 seconds and
at most three recovery requests without a successful baseline. These are
provisional safety policies, not measured full-universe timing guarantees: the
small probe initialized in approximately 239 ms after subscription, while C2 I/O
timeouts are ten seconds. Repeated errors/duplicate requests do not reset the
deadline; reconnect retains the budget. Recovery restarts a complete current
session, checked again by the worker; stale-generation requests do nothing.
Shutdown cancels scheduling but drains accepted ingress. Full/closed controls,
exhausted budgets and missing end boundaries are explicit failures.

Default logical limits: 1,000,000 retained instrument identities, 2,000,000
replicas, 2,048 session owners (the executable uses its C2 max-socket setting),
65,535 levels per side, 10,000,000 aggregate retained/construction levels,
4,096 events/message, 65,535 entries/event, 256 bytes/identity and 2 MiB/message.
Snapshot preflight includes old plus replacement levels; incremental preflight
includes intermediate peaks. Scratch collections have at most entries/event
logical elements, with standard Vec/HashMap capacity rounding. Payloads, decoder
arrays, scratch and allocator overhead are additional to level counts. These
limits are not exact RSS accounting or verified exchange capacities.

### Retained genuine fixtures

Both files preserve complete raw text payloads, including array shape/order;
they are not reconstructed JSON examples. Endpoint:
`wss://ws-subscriptions-clob.polymarket.com/ws/market`. Requests used
`type=market`, `level=2`, `initial_dump=true`, with exact asset IDs selected from
version-scoped Gamma records. Capture used verified Rustls TLS, no credentials.

| Fixture | Provenance and observation | SHA-256 |
|---|---|---|
| `tests/fixtures/polymarket-v1-book.json` | Earlier same-day bounded preflight, retained from `ws-preflight-v1-retry.jsonl`; receive time 1793090285451078 µs. Gamma market IDs 561268/559700; initial request used the two IDs in this payload. Two books, 72 asks and 72 bids respectively, 5,284 bytes. | `503d785b529c273bdf39c4ec5a274f3318944e4e755d85c9331d506ad21296fe` |
| `tests/fixtures/polymarket-v2-book.json` | Current 70-second C3/C4 probe, received 2026-10-27T16:44:52.840238Z. Request contained all four IDs in this payload; four empty books, 1,394 bytes. | `698127d772dc572eb8e8db69d6f7a1749e7bb82634ea80f4d34050629600c0a9` |

Current V1 handshake failed with HTTP 525. V2 returned its four initial snapshots
and six text PONGs; **no live price changes were observed**. V1 replay evidence is
from the retained earlier capture, not a successful new V1 connection. Current
raw request/receive provenance is in `.amp/in/artifacts/c34-*-capture.jsonl`;
the older capture and source archive remain under `ws-preflight*`. All delta,
malformed-input, lifecycle and load traces in tests are explicitly synthetic.
Wire-format sign-off for genuine V1/V2 deltas remains open.

## Implementation checklist

1. Save actual sanitized samples for V1/V2 discovery records, initial subscription and confirmation, `book`, `price_change`, `last_trade_price`, `tick_size_change`, lifecycle and malformed payloads.
2. Confirm `assetId` normalization from each version and payload variant, including multi-market events and both outcome assets of binary markets with YES/NO and non-YES/NO display labels.
3. Verify every documented field against the deployed API and the exact SDK version used. Date-stamp findings and correct this document rather than hiding surprises in code.
4. Rate-limit queries and reconnects, jitter retries, report rejections, and preserve existing registry on discovery failure.
5. Separate protocol facts from performance hypotheses. No tag enrichment, scoring or strategy filtering is required to parse or apply order-book deltas.
