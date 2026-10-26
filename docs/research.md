# Selected research references and how to use them

Treat links as research material, not implementation mandates. Check publication dates and current documentation when making exchange-specific changes. Production latency numbers are workload-specific.

## Specifications and protocols — read before implementing

- [Voluntas challenge](https://voluntas.io/engineering-challenge) — acceptance contract; Polymarket permitted as alternative to Kalshi; stdout + snapshots + multiple sockets.
- [Voluntas Rust template](https://github.com/voluntas-io/engineering-challenge-template) — actual codec, SBE writer/reader, schema and test oracle. Inspect checked-out version before changing outputs.
- [Polymarket discovery](https://docs.polymarket.com/market-data/discover-markets) — Gamma API pagination, V1/V2, eligible outcome IDs and optional categories.
- [Polymarket market WebSocket](https://docs.polymarket.com/api-reference/wss/market) and [realtime-data overview](https://docs.polymarket.com/market-data/realtime-data) — snapshot, absolute size updates, subscription control, wire shapes.
- [Polymarket markets and events](https://docs.polymarket.com/concepts/markets-events) — the event/market/outcome identity hierarchy.
- [Polymarket API limits](https://docs.polymarket.com/api-reference/rate-limits) and [negative risk](https://docs.polymarket.com/concepts/negative-risk) — respect service policies, don't misclassify constituent books.
- [Legacy Polymarket Python client's REST hash implementation](https://github.com/Polymarket/py-clob-client/blob/main/py_clob_client/utilities.py) — **not** a documented WebSocket replay sequence guarantee.

## Production engineering case studies

- [Databento: Why we didn't rewrite our feed handler in Rust](https://databento.com/blog/why-we-didnt-rewrite-our-feed-handler-in-rust) — important architecture mistakes: over-generic logic, excessive shared state, context switching/lock contention. It does **not** establish that C++ or Rust is universally faster.
- [Databento: Why we chose Rust for the real-time gateway](https://databento.com/blog/live-subscription-gateway-rust) — network-facing components can benefit from Rust/Tokio; evaluate component-by-component.
- [LMAX Disruptor technical paper](https://lmax-exchange.github.io/disruptor/disruptor.html) — preallocation, batching, single-writer ownership, cache and barriers. Historical microbenchmarks are not direct targets for a WebSocket collector.
- [Databento timestamping guide](https://databento.com/docs/architecture/timestamping-guide) — separate exchange, receive, and hardware capture semantics.
- [Aeron SBE design principles](https://github.com/aeron-io/simple-binary-encoding/wiki/Design-Principles) — binary encoding principles, not permission to bypass the supplied Voluntas framing.
- [Tokio cooperative scheduling](https://tokio.rs/blog/2020-04-preemption) — CPU-heavy continuous readiness can introduce starvation/tail latency in async tasks.

## Rust and low-level optimization reference

- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Tokio runtime docs](https://docs.rs/tokio/latest/tokio/runtime/index.html)
- [Rust Cargo profiles](https://doc.rust-lang.org/cargo/reference/profiles.html), [codegen options](https://doc.rust-lang.org/rustc/codegen-options/index.html), [PGO](https://doc.rust-lang.org/rustc/profile-guided-optimization.html)
- [Rust layout reference](https://doc.rust-lang.org/reference/type-layout.html), [MaybeUninit](https://doc.rust-lang.org/std/mem/union.MaybeUninit.html), [atomic Ordering](https://doc.rust-lang.org/std/sync/atomic/enum.Ordering.html)
- [Rust collections](https://doc.rust-lang.org/std/collections/index.html), [serde RawValue](https://docs.rs/serde_json/latest/serde_json/value/struct.RawValue.html), [simd-json](https://docs.rs/simd-json/latest/simd_json/), [rtrb SPSC](https://docs.rs/rtrb/latest/rtrb/)
- [Loom](https://docs.rs/loom/latest/loom/), [Miri](https://github.com/rust-lang/miri), [Criterion](https://bheisler.github.io/criterion.rs/book/), [HdrHistogram](https://docs.rs/hdrhistogram/latest/hdrhistogram/)
- [Linux TCP](https://man7.org/linux/man-pages/man7/tcp.7.html) and [kernel timestamping](https://docs.kernel.org/networking/timestamping.html) — platform-specific, separate from application read time.
- [Barter data adapters](https://docs.rs/barter-data/latest/barter_data/) — comparative interface inspiration, not an instruction to build a framework.

## Decision log: corrected guidance from original harness

| Previous ambiguity / risk | Revised principle |
|---|---|
| "Completeness" sounds absolutely provable | Guarantee exact processing of known accepted inputs; label unknown exchange gaps explicitly. |
| Full categories/tags baked into registry | Minimal IDs/eligibility only; independent point-in-time metadata join for category backtests. |
| Separate per-book shards assumed fastest | Socket-local ownership first; benchmark extra queue/parallelism before adding. |
| Generic market interface might flatten Kalshi/Polymarket differences | Adapter normalizes identity/lifecycle only; preserve exchange-specific book semantics. |
| Output must be fast, but writer is aligned/file-oriented | Resolve stdout framing/reader compatibility and observable flush delay as an early gate. |
| "Preserve ordering" was underspecified | Strict per-asset order; no unproven cross-asset global chronology requirement. |
| Full minute snapshots might spike workers | Schedule/coherently capture snapshots; benchmark one-minute output budget and pacing. |
| Every optimization trick appears equally urgent | Low-level tricks remain in reference; profiling determines if they're appropriate. |
| Success evaluated by code compiling | Component acceptance gates, independent replay oracle, measured capacity and failure tests. |

## Stage reminder

The current requested development focus is **Component 1: discovery and desired-state registry**. Read exchange discovery docs, confirm identifiers and eligibility, build deterministic fixtures, and verify reconciliation *before* implementing market-data WebSockets or optional metadata enrichment.
