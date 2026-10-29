# Read before getting started!!

## Running Components 1–5

The executable runs continuous discovery and stable per-adapter WebSocket
subscriptions. **The default mode records selected L2 history to binary stdout:**
one consumer validates exact Polymarket values and reconstructs source-specific
L2 books. Invalid input marks the affected replicas unavailable and requests a
bounded session restart. Source selection exposes snapshots before its deltas.
Every selected absolute level update (including no-ops and zero deletes),
authoritative snapshot, and periodic selected-live snapshot is encoded through
the supplied SBE codecs/helper. A separate, newly created JSONL integrity file
records snapshot provenance and selected-source unavailability. Diagnostics go
to stderr; never direct binary stdout to a terminal.
See [architecture](docs/architecture.md) and [protocol evidence](docs/protocol.md).

```sh
cargo build --locked --release
target/release/engineering-challenge --seconds 120 --integrity-path run.integrity.jsonl > run.sbe 2> collector.log
```

### Configuration

Limits and timings come from an optional YAML file; per-run settings stay on the
command line. Precedence is built-in defaults < `--config FILE` < override flags
(`--ws-url`, `--max-sockets`, `--monitoring`, …). [`collector.example.yaml`](collector.example.yaml)
lists every key at its default (a test keeps it in sync); unknown keys and
unrecordable values (for example `levels_per_side` above 65535) fail at startup.
The effective configuration, including values the collector derives (sessions,
selection, the 60 s snapshot cadence, decoder message bytes), is written as the
integrity file's first control row, so each capture records its own limits.

```sh
cp collector.example.yaml collector.yaml   # edit books.total_levels etc.
target/release/engineering-challenge --config collector.yaml --seconds 600 \
  --integrity-path run.integrity.jsonl > run.sbe 2> collector.log
```

**Sizing a full-book run.** `books.total_levels` bounds book memory across every
replica, including the old+new peak while a snapshot replaces a book (roughly
30–40 bytes per level). A book that does not fit is quarantined, visibly, rather
than truncated or crashing the process. Size it from a sampled run: the shutdown
summary and the integrity file's `owner_summary` report `peak_levels`,
`resource_quarantines` and a `depth` sample (live replicas, levels, p50/p95/p99/max
per replica, sampled once at stop: a per-minute walk of every replica stalled
ingress by 50–92 ms at full-universe size). Set
`total_levels` comfortably above the observed peak for the replicas you run, and
budget disk for minute snapshots (about 36 + 16 × levels + ~90 symbol bytes per
selected book per minute).

Omit `--seconds` to run until SIGINT. The library boundary supplies owned raw text,
application-receive timestamps, adapter/session identity, per-session ordinals and
ordered lifecycle/subscription-request events. Full/closed ingress fails visibly;
shutdown allows 15 seconds overall to drain accepted messages and finish output.
Retain the exit status and both output files: successful flush does not establish
durability, full-universe coverage or exchange-wide losslessness. Use
`--transport-only true` to retain opaque-message validation with empty stdout and
no required integrity file. An existing integrity path is rejected, never overwritten.

The output pool reserves 16 × 4 MiB plus descriptors (~65 MiB); book, ingress,
symbol-cache and physical-buffer memory is additional. Publication targets
256 KiB / 1 ms; physical flush targets 1 ms (configurable, minimum 1 ms). These are checked between work items,
not hard-real-time guarantees. With multiple batches, if all other batches are
outstanding, soft size/age publication retains the last batch and appends exact
records within its existing limits; age retries every publish interval. Hard byte/descriptor
exhaustion still fails closed when no reusable batch remains. Records never
cross 64 MiB logical chunks. Sparse
chunks are padded after 60 seconds or at finish, potentially adding ~64 MiB/minute.
The supplied reader opens completed files and ignores incomplete final chunks;
it is not a live pipe/tailing reader. Physical visibility is not reader compatibility.

Symbols are `polymarket:v1:<exact-id>` / `polymarket:v2:<exact-id>`. Timestamps use
microseconds; generated snapshots use actual capture/selection time and the last
applied exchange time. Replay **file order**, not sorted timestamps. A replacement
snapshot restores state, not missed history. Each selected-live book is due 60s
after its last authoritative snapshot; actual lateness is reported. Missing an
additional whole interval fails recording rather than backdating a snapshot.

The book consumer performs no diagnostic I/O. It returns exact invalidation
observation counts and the first four diagnostic samples (asset/reason previews
limited to 128 characters) after draining, or attaches them to a processing error.
Additional per-book details are omitted explicitly; this summary is not a history
archive or live monitoring feed. Main prints the summary after joining the consumer.
Other control-plane/transport stderr output is still synchronous: an indefinitely
blocked stderr can delay ordinary diagnostic delivery. A blocked output/drain
deadline exits nonzero without logging or joining stuck threads; it cannot cancel
an OS write or guarantee the tail. Keep stderr draining in operational runs.

Options (each requires a value): `--seconds`, `--gamma-url`, `--ws-url`,
`--assets-per-socket` (1000), `--max-sockets` (1024),
`--queue-items` (65536), `--queue-bytes` (67108864), `--transport-only` (false),
`--replicas-per-market` (1 or 2, default 1), `--monitoring` (false),
`--lifecycle-hints` (false),
`--integrity-path` (required for recording). Socket/message/queue settings are
collector safety budgets, **not verified exchange limits or a total RSS cap**.

The ingress item allowance includes headroom above a measured 23,404-message
live backlog that drained within its one-second observation window. The separate
64 MiB accounted-byte limit is unchanged; both bounds still fail explicitly.
This absorbs temporary bursts, not sustained overload, and does not make each
message faster. Queue-slot allocation adds memory outside the accounted-byte
limit. See the full-owner evidence in [EXPERIMENTS.md](EXPERIMENTS.md).

### Full-universe results and latency

Measured on an 8-vCPU (Xeon 2.6 GHz) / 16 GB Linux VM, rustc 1.93.0, single
replica, default limits, public read-only Polymarket feed. r1 ran `5269cf6`;
r2/r3 ran it plus opt-in stage/operation timers (`latency_histograms`).

| Run | Duration | Records written = confirmed | Output | Notes |
|---|---|---|---|---|
| r1 | 30 min | 40,301,598 | 7.58 GB SBE, 113 complete 64 MiB chunks | 516,858 desired, 514,075 selected live, 1,785 coverage-overdue at end; 527 sockets; peak 5.72M levels, 0 quarantines |
| r2 | 25 min | 35,141,365 | 6.58 GB SBE + 5.07 GB sidecar | peak RSS 2.50 GB; stage timings below |
| r3 | 25 min | 38,338,474 | — | owner-operation attribution; 164 network interruptions (r2: 4) |

Per-replica depth at r1's stop: p50 6, p95 40, p99 86, max 329 levels.

**Where a message's time goes** (r2 steady state, 11 one-minute windows):

| Stage | p50 | p99 | max |
|---|---:|---:|---:|
| Receive → ingress | 0.3 µs | 1.4 µs | 3.0 ms |
| Ingress wait for the owner | 26 µs | 2.4 ms | 130.5 ms |
| Decode + book + encode (owner) | 6.9 µs | 27.6 µs | 49.8 ms |
| Published → writer | 74 µs | 262 µs | 18.1 ms |
| Writer received → flushed | 3.3 ms | 5.8 ms | 22.3 ms |
| **Receive → flushed** | **6.0 ms** | **11.0 ms** | 58.8 ms |

Processing is microseconds; at the then-default 5 ms cadence ~95% of the
end-to-end p50 was deliberate batching. The owner averaged 21% of one core
(10 s peak 33%), each of four Tokio workers ~13%, the writer 2.6%; no dirty-page
throttling occurred. Receive timestamps are taken before parsing, so none of
these delays changes recorded times.

**Tail causes found and fixed** (branch `perf-local`; details in
[HYPOTHESES.md](HYPOTHESES.md) H20–H31):

| Cause (attributed in r3) | Stall | Fix | Evidence |
|---|---|---|---|
| Per-minute depth walk over every ~1.3 KB replica | 50–92 ms each minute | Sampled once at stop | Removed code path; stop-time walk 20–23 ms offline |
| Asset-ID `HashMap` rehash during ramp | doubling to 106–121 ms | Reserve `instruments` once | Offline A/B: running-window owner wait max 110–126 → 10–11 ms; 1,660,000 records matched the oracle in each of 4 runs |
| `json!` sidecar rows per snapshot / invalidation | ~2.1 µs per snapshot row (8× its SBE encoding) | Typed rows, byte-identical | 5,000 + 600 generated-case byte oracles; ~3.7× cheaper per snapshot row |
| 5 ms publish/flush cadence | ~5 ms of p50 | 1 ms default | Writer CPU ~2× offline (still ~8% of a core); live p50 not yet re-measured |
| Clean worker exit joined before the manager saw stop | false `success=false` (r3) | Read the stop flag; keep the report | Root cause of the ordering not proven |

Remaining, measured but accepted: initial 1,000-book subscription arrays take
~5–10 ms of real owner work (whole-event validation is not split); a session end
invalidating 1,000 books takes ~5–14 ms; the stop boundary has ~20–60 ms of
one-time work (depth sample, untimed snapshot-schedule teardown).

**Not established:** a live run of the fixed branch (expected: no per-minute
stall, ~1–2 ms end-to-end p50); exchange/network latency; exchange-wide
losslessness (the feed has no replay/sequence guarantee); two-replica
full-universe operation.

### Opt-in extensions and acceptance limits

Two-replica mode uses independent worker generations and automatic, sticky source
selection. A failover publishes a replacement snapshot before that source's
deltas. The sidecar preserves desired reconciliation cutoffs, successful requests,
standby initialization/loss, selected gaps and selection freshness evidence.
Neither a replacement nor a recently received message proves missing history was
recovered. The default remains one replica; monitoring and lifecycle hints are
separate opt-ins. Monitoring uses accepted L2 activity, not PONG or trade counts.
Guarded periodic renewal requires a usable peer and defers in single-source mode.
Missing instruments cannot trigger overlapping repairs that remove a neighboring
book's last usable redundant copy; fewer-attempted replicas get priority when
both repairs are due. This does not isolate failures in single-replica mode or
make an upstream-missing book available.

**Do not enable full-universe redundancy from these defaults alone.** R0 observed
556,974 desired assets, requiring 1,116 packed two-replica sockets, above the
configured 1,024 limit. Discovery also retained 44 unresolved V1 identities.
Capacity rejection names unadmitted identities rather than silently sampling.
Single-replica full-universe capacity, 30-minute live coverage and live stage
latency were measured; see [Full-universe results](#full-universe-results-and-latency).
Two-replica full-universe operation remains a separate gate.
See [the requirement/evidence ledger](docs/revision-check.md).

`latency_probe` is an isolated, bounded two-feed research executable, not an
automatic source-selection optimization. It requires explicit live approval and
a fixed sample. See [the research protocol](docs/latency-research.md); no live
performance improvement is claimed merely because the probe is implemented.
Local socket/control/failure tests and a 40,000-change loopback load passed.
`python3 -B tools/check_latency_probe.py <trial>/raw.jsonl` independently checks
financial match decisions against retained raw evidence, including timer-driven
window progress. It refuses incomplete captures and unsupported resource resets;
also check the probe's exit status, since a terminal row cannot prove final sync.

The opt-in production-owner capacity check runs the executable's real owner loop,
bounded ingress, minute snapshots and recorder against a file, then independently
checks every financial record. It defaults to 550,000 synthetic books, 550 sessions
and 70 seconds of 10,000 messages/second after paced initialization. Run it alone;
allow several GiB of RAM and temporary disk space. It excludes REST, WebSockets,
ongoing discovery churn and live coverage, and is not a latency percentile test.
`OWNER_REPLICAS=2` doubles stored replicas/sessions, enables automatic selection
and monitoring, and alternates equivalent deliveries across both sources. The
rate counts physical input messages, not unique selected updates. It uses the
production ingress item limit and unchanged 64 MiB accounted-byte limit.
`OWNER_CHECK_HEALTH=1` additionally requires maximum revisit ≤11s and lateness ≤1s;
these tolerances do not turn the 10s target into an exact real-time guarantee.

```sh
OWNER_BOOKS=550000 OWNER_RATE=100000 OWNER_SECONDS=70 cargo test --locked --release --lib production_owner_capacity -- --ignored --nocapture
# Small dual-feed smoke before the expensive full-population replay:
OWNER_BOOKS=1000 OWNER_REPLICAS=2 OWNER_RATE=10000 OWNER_SECONDS=3 OWNER_CHECK_HEALTH=1 cargo test --locked --release --lib production_owner_capacity -- --ignored --nocapture
```

`cargo test --locked --test dual_feed` exercises real localhost WebSocket workers,
ingress, book processing, recording and the supplied SBE reader under forced
disconnect, symbol silence, delayed batches, missing books and staggered renewal.
Its logical health clock accelerates policy thresholds; it does not measure
latency or exercise discovery/manager routing/shared connection admission.

For local optimization experiments, use the [bounded latency harness](EXPERIMENTS.md).
It compares explicit commits (or preserved source patches) with the existing replay
oracle in isolated worktrees, requires research evidence for candidates, and never
automatically promotes code. Shared test patches keep measurement changes identical
on both arms; the caller's uncommitted work is never silently included.
Ingress benchmarks use a dedicated sleeping producer with absolute deadlines;
socket benchmarks retain Tokio's millisecond-granularity timers. Both report
actual offered lag and interarrival gaps: equal average rates do not guarantee
equivalent arrival patterns. Changing the pacer establishes a new measurement
baseline, not a collector speedup.
The independent reference books and expected SBE stream are reconstructed after
output drain, from bounded input timestamps and periodic capture cutoffs. Exact
record-by-record checking remains mandatory. Timing and metadata instrumentation
still perturb the run; reference generation time is reported separately.
The [research ledger](RESEARCH.md) and [hypothesis backlog](HYPOTHESES.md) separate
observed costs from ideas that still need investigation.
Optimization decisions separate computational efficiency, bounded load response
and tail latency. The harness can reuse equivalent release builds across
`--rates 1000 3000 5000` and compare the same public decoder API with
`--decoder-profile`. Use `--baseline-patch` to compare a previous experimental
candidate against a combination without committing either. Optional
`--allocation-profile` captures a fixed small heaptrack replay after all timing;
instrumented timing is never included in latency comparisons. These are
experimental screens, not automatic promotion:
promising local savings retain any unresolved pipeline-regression warning until
the integrated collector passes qualification.
Shape-specific owner and recorder spans distinguish replacement arrays from
1/4/16-entry updates. Snapshot-neighborhood queue groups separately report the
first three affected and unrelated updates behind an array, based on offered
identities/ordinals. These diagnose head-of-line delay; they do not establish its
cause or maximum sustainable capacity. Empty groups have null quantiles.

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test --locked --release
cargo test --locked --release --test ingestion_load -- --ignored --nocapture
cargo test --locked --release --test recording_load -- --ignored --nocapture
# Read-only live coverage gate, bounded to 120 seconds; may fail honestly:
cargo test --locked --test discovery_http live_discovery_coverage -- --ignored --nocapture
```

The original schema, generated codecs and reader remain unchanged. The supplied
writer now separates physical flushing from chunk completion. See [the C5 plan](5.md),
[protocol contracts](docs/protocol.md) and [remaining checks](docs/revision-check.md).

### Where the code lives

| Component | Files |
|---|---|
| Entry point | `src/main.rs` (runtime + exit status only); `src/collector/` wires C1–C5 and owns ordered shutdown: `options.rs` (CLI), `owner.rs` (the market-data owner thread) |
| C1 discovery | `src/discovery/` |
| C2 ingestion | `src/websocket/` (`manager.rs` assignments, `worker.rs` one socket per task, `ingress.rs` bounded handoff) |
| C3 decoding | `src/market_data/` (`decimal.rs` exact scaling, `polymarket.rs` bounded parsing; `ResourceLimit` marks budget rejections) |
| C4 books | `src/orderbook/` — `book.rs` levels; `store/` the single-owner `Processor`: `apply.rs` events, `subscription.rs`, `desired.rs` reconciliation, `invalidation.rs`, `selection.rs`, `schedule.rs` timers, `slots.rs` replicas; `health.rs` policy |
| C5 recording | `src/recording/` |

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for its binary output. The supplied Rust helpers abstract encoding and decoding.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.
