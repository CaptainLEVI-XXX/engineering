# Polymarket full-universe L2 market-data collector

A Rust collector for the [Voluntas engineering challenge](https://voluntas.io/engineering-challenge),
using **Polymarket** as the exchange. It discovers every eligible active
instrument, spreads subscriptions across many WebSockets, maintains an exact L2
book per instrument, and writes every accepted price-level update plus a full
snapshot of every live book each minute to **binary stdout**, using the supplied
SBE schema and helpers. It keeps discovering additions and retirements while it
runs, and shuts down gracefully on SIGINT.

Priorities, in order: numerical exactness → correct books → honest accounting of
what was observed → timestamp fidelity → throughput → latency.

## Quick start

```sh
cargo build --locked --release
target/release/engineering-challenge --seconds 600 \
  --integrity-path run.integrity.jsonl > run.sbe 2> collector.log
```

- **stdout** carries SBE records only; never point it at a terminal.
- **`--integrity-path`** names a new JSONL provenance file (an existing path is
  rejected, never overwritten).
- **stderr** carries diagnostics and the shutdown summary.
- Omit `--seconds` to run until SIGINT. Keep the exit status and both output
  files: a nonzero exit means the capture is incomplete.

## How each requirement is met

| Requirement | Implementation |
|---|---|
| Discover every eligible instrument | Paginated Gamma REST scans (keyset), exact opaque IDs per adapter namespace, periodic full reconciliation; an incomplete scan never erases prior desired state |
| Multiple WebSockets | Stable assignment of up to 1,000 assets per socket; one Tokio task owns each socket, its subscriptions and its connection generation |
| L2 book per instrument | One owner thread; `BTreeMap` price levels; a `book` message replaces the book, a `price_change` sets an absolute quantity, zero deletes; every event is validated whole before any change applies |
| Every update to binary stdout | Each accepted level change, including no-ops and deletes, is encoded with the supplied SBE codecs into 64 MiB reader-compatible chunks |
| Snapshot of every live book each minute | Each selected live book is due 60 s after its last authoritative snapshot; captured in bounded ≤1 ms slices between messages; lateness is reported |
| Additions and retirements | Continuous reconciliation sends ordered subscribe/unsubscribe fences per socket; retired books stop at an exact input cutoff |
| Graceful SIGINT | Stop discovery and reconnects, cut off intake, close sockets, drain accepted messages, flush output, join workers; a failed drain exits nonzero |

## Architecture

```
Gamma REST ──► C1 discovery/registry ──► C2 subscription manager
                                            │ 1 task per socket (×~527)
                                            ▼
                                  bounded ingress queue (fails closed when full)
                                            ▼
                     owner thread: C3 decode → C4 books → snapshot schedule
                                            ▼
                     C5 recorder: fixed batch pool → writer thread → stdout
                                                              └► integrity JSONL
```

| Component | Code |
|---|---|
| Entry point and wiring | `src/main.rs`, `src/collector/` (`options.rs` CLI, `config.rs` YAML, `owner.rs` the market-data owner thread) |
| C1 discovery | `src/discovery/` |
| C2 ingestion | `src/websocket/` (`manager.rs` assignments, `worker/` one socket per task, `ingress.rs` bounded handoff) |
| C3 decoding | `src/market_data/` (`decimal.rs` exact scaling, `polymarket.rs` bounded parsing) |
| C4 books | `src/orderbook/` (`book.rs` levels; `store/` the single-owner `Processor`: events, reconciliation, invalidation, selection, timers) |
| C5 recording | `src/recording/` (encoding, batching, writer, integrity sidecar) |
| Supplied SBE helpers | `src/util/`, `sbe_schemas/` (unchanged schema and reader) |

`src/bin/discovery.rs` is a small standalone discovery tool used by the
discovery tests.

## Correctness guarantees

- **Exact numbers.** Prices and sizes are parsed from decimal strings into
  checked `i64` at the schema's 1,000,000 scale. No floats, and no silent
  rounding: an inexact value invalidates the affected book.
- **Book semantics.** Snapshots replace; level updates are absolute; zero
  deletes. A whole event is validated before any part of it is applied.
- **Order.** Per-instrument feed order is preserved, including snapshots and
  batches. Records are never sorted by exchange time; replay in file order.
- **No silent loss.** Every queue is bounded with an explicit failure policy. A
  full ingress queue or exhausted output pool stops the run visibly; nothing is
  dropped, coalesced or relabelled.
- **Visible gaps.** Invalid input or a lost source marks the affected books
  unavailable in the integrity file and requests a session restart. A book is
  published again only after a fresh snapshot. A replacement snapshot restores
  state, not missed history.
- **Bounded memory.** `books.total_levels` caps book memory across all books. A
  book that exceeds a budget is quarantined and reported, never truncated.
- **Timestamps.** Application-receive time is captured as soon as a complete
  WebSocket message arrives, before parsing or queueing. Exchange time is kept
  separately. Internal delays never change recorded times.

## Output

- **SBE stdout:** `PriceLevelUpdate` and `Snapshot` records from the supplied
  schema, in 64 MiB chunks. Sparse chunks are completed after 60 s or at finish.
  The supplied reader reads completed files; it is not a live tailing reader.
- **Symbols:** `polymarket:v1:<exact-id>` / `polymarket:v2:<exact-id>`.
- **Timestamps:** microseconds. Generated snapshots carry the actual capture
  time and the last applied exchange time.
- **Integrity file (JSONL):** the effective configuration first, then snapshot
  provenance, subscription and reconciliation fences, unavailability, and a final
  `run_end` with written/confirmed record counts and `success`.

## Configuration

Precedence: built-in defaults < `--config FILE` < command-line flags. Every key
and its default is in [`collector.example.yaml`](collector.example.yaml) (a test
keeps them in sync); unknown keys fail at startup.

```sh
cp collector.example.yaml collector.yaml
target/release/engineering-challenge --config collector.yaml --seconds 600 \
  --integrity-path run.integrity.jsonl > run.sbe 2> collector.log
```

Flags: `--seconds`, `--integrity-path`, `--transport-only` (validate messages
with empty stdout), `--gamma-url`, `--ws-url`, `--assets-per-socket` (1000),
`--max-sockets` (1024), `--replicas-per-market` (1 or 2), `--monitoring`,
`--lifecycle-hints`, `--queue-items` (65536), `--queue-bytes` (67108864).

**Sizing a full-universe run.** About 517k books needed 527 sockets, peaked at
5.72M levels and 2.5 GB RSS, and wrote ~250 MB of SBE per minute plus ~200 MB of
integrity rows. Set `books.total_levels` comfortably above the `peak_levels`
reported in the shutdown summary (default 10M). Output is published and flushed
every 1 ms (`recording.publish_age_ms`, `flush_age_ms`; minimum 1).

**Two replicas** (`--replicas-per-market 2`) subscribe every market on two
independent sockets with sticky automatic source selection; a failover publishes
a replacement snapshot before the new source's deltas. At the current universe
size this needs more than the default 1,024 sockets.

## Performance

Measured on an 8-vCPU (Xeon 2.6 GHz) / 16 GB Linux VM, rustc 1.93.0, single
replica, default limits, live public Polymarket feed, 25–30 minute runs.

| Run | Records written = confirmed | Coverage | Result |
|---|---|---|---|
| 30 min | 40,301,598 | 516,858 desired, 514,075 live; 527 sockets | 7.58 GB SBE, 0 quarantines |
| 25 min (final code) | 34,768,552 | full universe | `success=true`, peak RSS 2.53 GB |

**Where a message's time goes** (steady state; the first two rows are from an
earlier instrumented run, the last two from the final code):

| Stage | p50 | p99 |
|---|---:|---:|
| Socket receive → ingress queue | 0.3 µs | 1.4 µs |
| Decode + book update + encode | ~7 µs | ~28 µs |
| Waiting for the owner thread | 26 µs | 2.0 ms |
| **Receive → flushed to stdout** | **1.6 ms** | **4.5 ms** |

Processing is single-digit microseconds; most end-to-end time is the 1 ms output
cadence. The owner thread averaged ~22% of one core and the writer ~7%, leaving
ample headroom.

**Tail stalls found by live attribution and removed:**

| Cause | Before | Fix |
|---|---|---|
| Per-minute depth statistic walking every book | 50–92 ms every minute | Sampled once, at stop |
| Asset-ID map rehashing as discovery added books | doubling to ~120 ms | Map reserved at the configured bound |
| Building integrity rows as `serde_json::Value` | ~2.1 µs per snapshot row | Typed rows, byte-identical output, ~3.7× cheaper |
| 5 ms publish/flush cadence | ~6 ms p50 | 1 ms cadence: 1.6 ms p50 |
| Resetting ~510k per-book deadlines at stop | ~240 ms (cold) | Clear the deadline set only |
| A clean worker exit noticed before the stop flag | false `success=false` | Read the stop flag directly |

An offline A/B with 550k books cut the worst queue wait from 110–126 ms to
10–11 ms, with every record matching an independent oracle.

## Testing

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test --locked --release
```

182 tests pass in debug and release (11 more are opt-in, below). They cover:

- exact decimal parsing and bounded, typed decoding against real Polymarket
  fixtures and generated adversarial cases;
- book replay and lifecycle against an independent reference implementation;
- local HTTP and WebSocket servers for discovery, subscriptions, reconnects,
  heartbeats, stalls, oversized messages and shutdown;
- recording round trips through the supplied SBE reader, including chunk
  framing and byte-identical integrity rows;
- the executable end to end, including config files and resource budgets.

Ignored by default (run explicitly, some use the live feed or several GiB):

```sh
# Full owner loop, 550k synthetic books, every record checked by an oracle
OWNER_BOOKS=550000 OWNER_SECONDS=70 cargo test --locked --release --lib production_owner_capacity -- --ignored --nocapture
cargo test --locked --release --test recording_load -- --ignored --nocapture
cargo test --locked --release --test ingestion_load -- --ignored --nocapture
cargo test --locked --test discovery_http live_discovery_coverage -- --ignored --nocapture
```

## Limitations

- Polymarket's feed has no replay or sequence guarantee, so exchange-wide
  losslessness cannot be proven; the collector proves only that every accepted
  message was recorded, and marks every known gap.
- Network latency from Polymarket is not measured; the public feed arrives over
  the internet with tens of milliseconds of delay.
- Opening ~527 sockets takes about 13 minutes at the deliberately polite
  connection pacing.
- Occasional owner-thread waits of 30–70 ms remain unattributed; initial
  subscription arrays cost 25–46 ms of real work during ramp.
- Two-replica full-universe operation has not been measured. Kalshi is not
  implemented; the adapter boundary admits it later.

## About the SBE schema

The schema is written in the template's YAML format
([`challenge_orderbook.yaml`](sbe_schemas/engineering_challenge/challenge_orderbook.yaml)),
converted to FIX SBE XML at build time, and used to generate the Rust codecs.
Timestamps use a custom field so the generated structs use `chrono::DateTime`;
on the wire they are `uint64` microseconds since the Unix epoch.
