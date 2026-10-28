# Latency research: evidence before experiments

Scope: the existing Polymarket collector, not a new trading engine. Priority is
exact numbers → correct books/history → recovery → bounded resources → throughput
→ tail latency. This ledger separates observations, mechanisms and speculation.
Source review: 2026-10-27, baseline `015703231785b0ae93fb1be0bb36ff4c14382a2c`,
Rust 1.93.0, serde_json 1.0.154, serde 1.0.229, Crossbeam channel 0.5.17,
Tokio 1.54.1. The Cargo.lock-matched dependencies were inspected locally.

## Adopted optimization workflow

The deliverable is a better, integrated collector, not a growing collection of
benchmark reports. Research and measurement serve implementation decisions.
Keep exact financial values, accepted history, ordering, recovery and bounded
resources as hard constraints. Measure three independent outcomes:

1. Computational efficiency: removed work, allocations and equivalent-build
   operation cost; hardware cycles/cache counters only when actually available.
2. Sustainable capacity: increasing offered load, queue-age trend, complete drain,
   memory and CPU. Whole-benchmark CPU includes the generator and post-drain oracle;
   it is not collector-only CPU. A finite load screen does not establish saturation.
3. Tail latency: same offered traffic, event mix and output path, with per-class
   attribution. Short p99.9 measurements are exploratory, not qualification.

Retain a correctness-verified mechanism experimentally when its computational
benefit is credible, even if low-load p99 cannot resolve the saving. Carry an
independent unresolved-regression flag; never erase failed pipeline evidence by
calling it noise. Accept only with repeatable benefit and convincing system-level
non-regression. Reject attributable material regressions or incorrect behavior;
mark unresolved evidence inconclusive. No automatic promotion from a runner label.

Each bounded campaign has a predeclared hypothesis, fixed workload/build settings,
budget and falsifier. Use targeted tests, isolated measurement, short replay and
then broader qualification. Research one bottleneck against relevant sources;
do not expand into a survey of unrelated HFT systems. Run CPU-heavy work serially.
Do not rerun unchanged campaigns until they pass. Only combine two or three
independently measured, reversible changes; preserve isolated results and bisect
regressing combinations. No unsafe Rust, architecture rewrite, live campaign or
external delivery is implied by experimental retention.

## Critical path and measurement boundaries

| Owner / code | Work | Available evidence / gap |
|---|---|---|
| `src/discovery/` | Desired identities, reconciliation, verification | Control plane; full-universe capacity unresolved, not a per-delta optimization target |
| `src/websocket/worker.rs` | WebSocket receive, timestamp, controls | Local socket receive→enqueue p99 0.84µs; excludes unread kernel/TLS bytes |
| `src/websocket/ingress.rs` | Byte permit, bounded Crossbeam handoff | Queue p99 136µs in one local trial; not isolated enqueue cost |
| `src/market_data/polymarket.rs` | Envelope validation, typed decoding, exact decimals | E01 isolated timings, E06 CPU samples, E07 filtered allocation counts; borrowing reduced allocations but failed latency screens |
| `src/orderbook/{store,book,health}.rs` | Provisional event validation, mutation, selection, health | Combined decode/book/recorder p99 43µs in one local trial; health measured separately, not in that pipeline run |
| `src/recording/` | Borrowed SBE encoding, bounded batches, one output owner | Callback/write-call timings available; not consumer-visible per-record delivery |

Previous local trials: 100 books, 1,000 messages/s, 120s. Three direct-ingress
processing p99s were 37–38µs; one socket trial was 43µs. These are observations,
not budgets, improvements or exchange latency. See `docs/revision-check.md`.
Through E13 the oracle ran before the timed owner span but delayed the next
dequeue. E14 moves reference reconstruction after output drain; bounded metadata
capture and timing instrumentation remain. Do not compare across those measurement
baselines as a collector optimization, attribute all queue time to the queue
implementation, or add stage percentiles.

## Reviewed mechanisms and applicability

### JSON traversal and allocation — mechanism confirmed, benefit not measured

- [serde_json 1.0.154 `de.rs`](https://docs.rs/serde_json/1.0.154/src/serde_json/de.rs.html):
  `deserialize_raw_value` invokes `ignore_value` between begin/end buffering.
  Borrowing RawValue avoids a JSON DOM allocation, not validation/traversal.
- Our `envelope → decode → list` revisits nested text and constructs bounded
  vectors. Removing envelope validation could expose earlier events before a
  malformed array tail is discovered. That is a correctness regression.
- [Serde 1.0.229 deserialization implementation](https://docs.rs/serde_core/1.0.229/src/serde_core/de/impls.rs.html)
  implements `VecInPlaceVisitor` with reservation and `InPlaceSeed`. This is real
  reuse, but not a drop-in replacement for our bounded visitor: our entries may
  borrow from different message lifetimes, and bounds/error atomicity must remain.
- E01 measured retained two-entry envelope+decode median 1.588µs, versus
  138.639µs for the retained four-snapshot array. See EXPERIMENTS.md for boundaries,
  controls and synthetic depths. E07 subsequently measured allocation sites. Do not replace
  the parser or add a dependency based on these isolated timings alone.

### Scheduling versus queue mechanics — attribution required

- [Tokio cooperative scheduling](https://tokio.rs/blog/2020-04-preemption)
  explains why always-ready async loops may delay other tasks and how operation
  budgets help. Its historical speedup is not a prediction for this collector.
- [Crossbeam channel 0.5.17 bounded array implementation](https://docs.rs/crossbeam-channel/0.5.17/src/crossbeam_channel/flavors/array.rs.html):
  `try_send` reserves a slot and writes; blocking paths additionally manage waits.
  Our ingress uses `try_send`, not a blocking async send. The book owner is a
  dedicated synchronous thread, so changing Tokio task budgets cannot directly
  preempt its parsing or its oracle work.
- An SPSC replacement does not fit the current multiple socket producers.
  Adding per-worker queues changes arbitration, ordering and failure semantics.
  No evidence currently justifies that architectural change or a spin loop.

### Bulk tree construction — useful technique with a contract mismatch

- [Rust 1.93 BTreeMap source](https://doc.rust-lang.org/1.93.0/src/alloc/collections/btree/map.rs.html):
  `FromIterator` collects a vector, stable-sorts by key and bulk-builds through
  `DedupSortedIter`; duplicate keys are silently collapsed.
- `Book::replacement` currently rejects duplicates, including a zero level
  followed by a nonzero level at the same price. Blind `collect` is rejected.
  A researched candidate would first validate duplicates, retain the original
  failure scope, and account for sorting/allocation plus peak replacement memory.
- E02 isolated current-orb construction and drop at 8/64/256/2048 levels. Validated
  bulk construction improved both descending and permuted cases. The candidate
  stays within `Book::replacement`; decoder, processor budget preflight and commit
  point are unchanged. Burst results improved, but the mixed control failed its
  queue guard. E02 is parked; see the complete decision in EXPERIMENTS.md.
- `Processor::event` checks per-side and old+new level budgets before construction,
  then replaces the book only after successful construction of both sides.
  Sorting owns the existing vectors and uses allocation-free unstable sort, but
  standard bulk collection may require intermediate vector/sort storage. RSS
  comparison is a process-level guard, **not** allocator-peak proof. No claim of
  reduced memory or changed capacity follows from faster construction.

## Research queue, not implementation commitments

| Direction | Evidence to obtain before a hypothesis is executable |
|---|---|
| Allocation/cache locality | Current allocation counts and retained capacities, lifetimes and realistic depth distribution |
| Replacement construction | Depth-stratified cost and source-backed construction alternative preserving duplicate rejection |
| Queue/wakeup ownership | Oracle-isolated attribution, runnable/sleeping state and producer lag; compare same semantics |
| Output batching/visibility | Independent consumer timestamps and stall matrix, not just write return times |
| Compiler/layout tuning | Profiles and generated code; identical flags for comparisons unless that is the declared experiment |
| Discovery/health scaling | Measured population, sweep lateness, deadline backlog and retention, not extrapolation from 100 books |

Kalshi remains a compatibility-research topic: the linked API-key generator is
credential management, not market data. No keys, authenticated feeds or trading
calls are part of this local campaign. Synthetic alternate-protocol conformance
does not establish live Kalshi support. Broader source exploration continues only
inside an agreed budget; a list of famous systems is not evidence of suitability.

## Code-level ownership audit (2026-10-28)

The audit follows actual call sites, not the number of `.clone()` strings.

| Flow | Ownership/cost found | Decision |
|---|---|---|
| Worker `stream → emit → ingress::Sender::send` | Complete `Utf8Bytes` moves into the envelope; no payload clone. Receive wall/monotonic clocks precede classification/queuing | Preserve payload ownership and timestamp boundary |
| Ingress byte lease | One `Arc<Semaphore>` clone per message supports an owned permit that outlives the socket task and releases only after processing | Atomic reference counting is real work, but removing it requires an equivalent cross-thread lifetime/budget contract; no measured case for replacement |
| Session decoder | `Limits::clone` copies four machine integers. RawValue and direct annotated Cow fields borrow; E07 found nested optional Cow fields allocate | Do not confuse scalar Clone with heap copies; measure optional-text borrowing separately |
| JSON envelope and nested lists | Repeated validation/traversal, fresh bounded vectors; E01 quantifies total decode cost | Do not remove full-envelope validation or emit partial financial events; reuse needs a lifetime-safe design and allocation evidence |
| Source resolution | Each accepted asset currently hashes its text in both `by_adapter` and `source.requested`; the returned Binding already contains stable BookId | E03 isolated gain did not pass paired pipeline screens; candidate parked |
| Delta preparation | `quantities.get` then `quantities.insert` repeats a numeric-key lookup; counts already uses Entry, vectors/maps retain bounded capacity | E04 isolated Entry gain did not pass paired pipeline screens; candidate parked |
| Recorder | Cached symbol and borrowed book view; delta records do not clone symbols/books or allocate JSON. Bounded batch bytes own the asynchronous output | Preserve the required copy into owned output storage; avoid mistaking borrowing for an asynchronous lifetime guarantee |
| Reconnect/subscription maintenance | String clones are membership/control state or owned control messages, not per-price-update payload copies | Outside current small-workload hot-path optimization |
| Health monitoring | Clones fixed-size windows, not books; happens in policy evaluation | No evidence yet that avoiding these copies pays for more complicated aliasing/ownership |
| Invalid input | Scope recovery owns IDs and invalidation collects/sorts bindings for stable output order | Error-path work, not evidence of valid-stream allocation pressure; preserve source-scope honesty |

### Existing handles can avoid a second identity lookup

The [lasso 0.7.3 resolver implementation](https://docs.rs/lasso/0.7.3/src/lasso/resolver.rs.html)
stores key-indexed strings and resolves a previously obtained key by indexing,
without another text-key hash lookup. Its key-origin and range restrictions are
important; copying its unchecked access would not be appropriate here.
Our collector already has `Binding { book, revision, session }`: no interner,
dependency or new cache is needed. `instrument_id` only appends BookIds and never
reuses/removes their vector slots. `subscription` constructs requested bindings
from that same table. Retirement removes matching requests; supersession can leave
an old request, so the **existing binding equality and inactive-state checks must
remain**. Unknown assets must still invalidate the same source scope, while known
but unrequested assets remain ignored. This is a source-backed hypothesis, not
a demonstrated pipeline improvement: E03's isolated savings did not pass the
native-ID or mixed paired screens, and the production candidate was removed.

For numeric preparation, Rust 1.93's
[Entry API](https://doc.rust-lang.org/1.93.0/std/collections/hash_map/enum.Entry.html)
provides a single occupied/vacant view (`insert_entry` stable since 1.83). It can
avoid get-then-insert, but provisional quantities are not committed book values:
repeated keys, zero deletion followed by an add, overflow and event rejection
must keep their current semantics. No hasher replacement or unsafe access proposed.

Inspected installed [hashbrown 0.17.1 map source](https://docs.rs/hashbrown/0.17.1/src/hashbrown/map.rs.html):
`entry` computes a hash, retaining the found bucket or vacant hash/key; separate
`insert` computes a hash again. This is implementation evidence for the mechanism,
not a claim that this installed crate is precisely Rust 1.93's vendored version.
The collector already uses Entry for provisional per-side counts. H06 uses that
same pattern for quantities, retaining the existing insertion point after
arithmetic/budget validation, including intermediate zeros. An isolated synthetic
map-fragment experiment found a 16-distinct-entry median of 725 → 442ns and
16 entries repeating two keys of 767 → 423ns (medians of three paired medians).
These omit actual book fallback, parser, budgets and output; E04 did not establish
a complete-pipeline improvement. No dependency or persistent state added.

Recorder identity checks are not dead code: `observe` accepts externally supplied
observations, so removing the cached-symbol/BookId consistency check would weaken
its public contract. Snapshot metadata creates JSON, but accepted level updates
do not. Measuring snapshot metadata cost may be useful; deleting it would remove
the provenance needed to interpret replacement/failover boundaries.

### Decimal digit validation and accumulation

`parse_scaled` checks every whole/fraction byte with `is_ascii_digit` and then
walks both slices again to accumulate checked fixed-point values. Inspected the
locked [serde_json 1.0.154 integer parser](https://docs.rs/serde_json/1.0.154/src/serde_json/de.rs.html):
it classifies each digit while accumulating, rather than requiring a separate
ASCII-validation pass. Only that traversal mechanism is applicable. Its leading
zero restrictions, exponent support and floating-point overflow fallback are
**not** this collector's contract and must not be copied.

H07 first measures a temporary fused-byte-check implementation against the real
public `parse_scaled`, with identical checked multiply/add and excess-precision
rules. Invalid input remains invalid, but multiply-invalid strings can change
which error is encountered first; any production proposal must disclose that
diagnostic difference. Boundaries, every Latin-1 character in whole/fraction
positions, and independently calculated valid values preceded timing. The measured
saving was only about 1–10ns/value; defer production changes and a pipeline campaign.

### Distinguish benchmark allocation from collector allocation

E06 sampled user-space CPU-clock stacks at 499Hz without changing compiler flags.
In the first five-second burst run, 384/636 sampled stacks contained the test
`payload` generator, versus 121 containing `Processor::process`. In mixed traffic
the counts were 90/214 and 31. These are small whole-run profiles, including warmup
and completion, with potentially truncated stacks—not precise CPU percentages or
per-message latency. Visible generator stacks included JSON Value/BTreeMap
construction, string formatting, malloc and destruction. They are not evidence of
per-delta allocations inside the collector.

The harness generator now writes its fixed synthetic ASCII/numeric JSON directly
to a String with Rust's existing `fmt::Write` API, avoiding the temporary DOM.
This is not a production JSON serializer or a changed financial oracle. An
executed before/after byte comparison covered 4,016 messages across initialization,
single/batched updates, zero sizes, replacement boundaries, 78-character identities
and burst depths: all 13,550,839 framed bytes matched exactly. Repeated short
profiles showed 57/285 burst stacks containing the generator and 121 containing
Processor; mixed had 24/156 and 38. Sampling is too small for precise attribution,
but supports reducing generator interference before pursuing larger changes.

This change establishes a **new measurement baseline**, not a collector speedup.
Do not compare its latency percentiles directly to earlier E02–E05 campaigns or
erase their failed gates. A fresh identical-code A/A calibration is required.

### Optional Cow fields do not inherit direct-Cow borrowing

E07 corrected the earlier source-only assumption about all annotated Cow fields.
The [locked serde_derive implementation](https://docs.rs/serde_derive/1.0.229/src/serde_derive/internals/attr.rs.html)
specializes direct Cow fields with `borrow_cow_str`; its type matching does not
recurse through Option. The established
[serde_with BorrowCow adapter](https://docs.rs/serde_with/latest/serde_with/struct.BorrowCow.html)
documents this nested-type gap. Our optional Wire timestamp and asset fields
therefore allocate even for ordinary unescaped JSON strings.

Following the [Rust Performance Book's allocation guidance](https://nnethercote.github.io/perf-book/heap-allocations.html),
used [heaptrack's documented Rust workflow](https://github.com/KDE/heaptrack)
without adding a Rust dependency or allocator hook. Scope is a one-second measured
trace plus two-second warmup, 100 books/1,000 msg/s, under explicit instrumented
process deadlines. Counts filtered by stacks containing `Processor::process`
separate collector work from generator/oracle allocations; they include callbacks
and initialization and are not per-delta counts or latency measurements.

H08 uses one local transparent Cow wrapper inside optional-text deserialization,
with explicit default handling for missing fields. This retains escaped-string
ownership instead of incorrectly replacing Cow with `&str`. The public borrowing
regression failed on baseline, then passed along with escaped/missing/null/type
cases and 98 focused release tests. Mixed Processor-stack allocation calls fell
26,570 → 23,355; burst calls fell 142,948 → 137,498. String-deserialization sites
fell 3,215 → 0 and 5,450 → 0 respectively. Both subsequent paired pipeline screens
failed, so the candidate is parked, not promoted as a latency improvement. RawValue
validation still allocates scratch state; this finding does not justify removing
validation.

### Proposed directions: applicability and ownership before implementation

The retained V1 trace contains 31 two-entry change batches with no consecutive
repeated asset IDs. A last-asset cache would have no hits there. This small trace
does not characterize all exchange traffic; do not claim that repeated-ID batches
are impossible. Removing the second lookup per entry is a different mechanism,
already measured in E03 and parked after failed pipeline screening.

`prepared`, `quantities`, and `counts` already retain allocation capacity across
events. Decoder `List` constructs fresh vectors; `Event` transfers those owned
vectors to its consumer, and `Book::replacement` consumes snapshot vectors.
Therefore safe decoder reuse needs an explicit return/borrow lifetime contract,
not a persistent vector holding references to a previous WebSocket message.
Serde's existing stateful visitor/in-place mechanisms are relevant precedents,
but do not solve the ownership, bounds or malformed-tail atomicity requirements.
E09 subsequently isolated list storage reuse: two-entry changes saved 21–52ns,
while larger cases were mixed or regressed. That did not justify changing the
public ownership contract. It is not evidence about allocator contention tails.

The health due set already removes retired books after their pending check.
E08 exercised public APIs with 100 books/two replicas: 6,000, 3,000 and zero checks
over equal logical intervals as the desired population fell from 100 to 50 to zero.
Quiet desired replicas remained valid, with no repairs. The old recording timing
workload does not invoke health tick/maintenance; E08 is a separate hot-cache
diagnostic, not a silent new baseline or evidence of full-universe capacity.
No sharding or replacement parser is justified by these observations.

## Snapshot replacement: horizontal source audit, 2026-10-28

Research first, one boundary: validated snapshot levels → replacement construction
→ atomic publication by the book owner. No new candidate or campaign follows merely
from finding another implementation. The earlier planned bulk-construction retest
is paused while this review establishes applicability and missing measurements.

### Fork discovery includes non-default branches

GitHub's forks endpoint returned 18 visible forks (the repository metadata reported
19). All returned branch lists were inspected, not just default-branch language or
README. The inventory is preserved with exact commit IDs under
`.amp/in/artifacts/harness/fork-research/branches.json`. Important implementation
branches include `xuan0x0/kalshi-collector` and `redundant-sockets` (same head),
`g4titanx/solution`, `princedotrs/orderbook-collector`,
`haneulhanna/collector-implementation`, `mubarizkyc/baraz`, and
`kj111422/polymarket-continuity-failure`.

The following are **source observations about the named helpers**, not whole-project
correctness scores. Their tests/benchmarks were not executed. Upstream adapter
validation may impose additional rules; a helper mismatch alone does not establish
an end-to-end bug in another collector.

| Pinned source | Construction mechanism | Compatibility with our contract |
|---|---|---|
| [Eternal1506, `Book::replace`](https://github.com/Eternal1506/engineering-challenge-template/blob/5941fa630c5cc103bdd0905423c7e1b29b1d4b51/src/core/book.rs#L80-L97) and [`positive_levels`](https://github.com/Eternal1506/engineering-challenge-template/blob/5941fa630c5cc103bdd0905423c7e1b29b1d4b51/src/core/book.rs#L197-L203) | Bulk collect, then remove nonpositive levels | Explicitly last-duplicate-wins; exchange time uses max. Construction technique is relevant, duplicate/timestamp policies are not ours |
| [JimmyPotekev, `from_levels`](https://github.com/JimmyPotekev/engineering-challenge-template/blob/4e56a9abfa60ec9fd8e5a173789433e5fc710387/src/book.rs#L24-L31) | Filter positive quantities, then collect | Does not reject duplicate prices in this helper; filtering first erases zero-duplicate evidence |
| [xuan0x0, `from_levels`](https://github.com/xuan0x0/engineering-challenge-template/blob/e5c71ed060d21fdbf647e7460fb8fca2ee2b2d12/src/orderbook.rs#L29-L42) | Filter/map/collect into BTreeMaps | Same duplicate/zero caveat; do not infer parity from the same container type |
| [princedotrs, `from_levels`](https://github.com/princedotrs/engineering-challenge-template/blob/5be5d6c498e779a52405e129475ed26160e522de/src/book.rs#L41-L55) | Bulk collect from borrowed levels | Same duplicate/zero caveat; our vectors are already owned, so its copies are not automatically needed |
| [g4titanx, `from_snapshot`](https://github.com/g4titanx/engineering-challenge-template/blob/c34989e87d89cfd03724b330fe2122f6d25ffef2/src/book.rs#L61-L85) | Parse and insert into a new book; reject duplicates before returning | Similar fresh-book transaction boundary, but rejects zero snapshot quantities rather than omitting them |
| [kj111422, `replace`](https://github.com/kj111422/engineering-challenge-template/blob/9ab6cd5a584fea58c0086efa50d55c796e9aac4e/src/orderbook.rs#L77-L96) and [`insert_snapshot_level`](https://github.com/kj111422/engineering-challenge-template/blob/9ab6cd5a584fea58c0086efa50d55c796e9aac4e/src/orderbook.rs#L179-L208) | Build `next`, then assign once | Good ownership precedent. Helper skips zero before duplicate checking, unlike our rejection of all repeated prices. Book file is identical in its inspected continuity branch |
| [haneulhanna, `from_snapshot`](https://github.com/haneulhanna/engineering-challenge-template/blob/e720e287d0fdb7964e8ac910dd54f16c9fcb2799/src/orderbook.rs#L89-L99) | Parse into a fresh YES/NO BTreeMap book through `set` | Exchange-specific normalization; repeated levels use set/delete semantics, not our duplicate rejection |
| [sunguru98, `handle_book`](https://github.com/sunguru98/engineering-challenge-template/blob/7b8678b4124c27b7d0a428b00ccfef909b425dc2/src/handlers.rs#L80-L113) | Construct both temporary maps, then assign | Useful build-before-publish structure; insertion return values do not reject duplicates here |
| [swar09, orderbook](https://github.com/swar09/engineering-challenge-solution/blob/89f08d60c618f439d35ad9dd46486932feedd2e0/src/orderbook/mod.rs#L120-L165) | Clear/reinsert; buffered deltas replay by exchange timestamp | Not a template for our wire-order contract. Its fixed-point helper also routes strings through f64, which we cannot adopt |
| [mubarizkyc, Polymarket adapter](https://github.com/mubarizkyc/engineering-challenge-template/blob/b02edd2e7f62ac9e5ef8b81e82e755bdd5bc189a/src/polymarket.rs#L182-L234) | Delegates book update to `polyfill_rs`, then checks loaded timestamp/crossing | Wrapper is not evidence about internal construction cost or container choice; dependency internals not audited here |

The most important counterexample is not a speed number: at the same price,
quantities `[7, 0]` produce an empty level with collect-then-filter, retain 7 with
filter-then-collect, and are rejected by our contract. These are different semantics,
not interchangeable optimized versions. Preserve the same rejection oracle.

### Established implementations explain tradeoffs, not universal HFT rules

- [Rust 1.93 BTreeMap](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/map.rs#L150-L180)
  already packs multiple entries per node; it is not a one-allocation-per-level
  binary search tree. Its node has capacity 11. Avoid generic claims that replacing
  it with an array automatically removes one pointer chase/allocation per level.
  [`FromIterator`](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/map.rs#L2379-L2395)
  collects a vector, stable-sorts, then bulk-builds with deduplication. The internal
  sorted builder is private. Our previous candidate's explicit sort/duplicate pass
  followed by collect therefore still enters the standard sort path; it is not a
  public zero-sort bulk-load API. Extra allocation/peak-storage claims need profiles.
- [NautilusTrader ladder](https://github.com/nautechsystems/nautilus_trader/blob/202808567f6233d538b6d8c9915b071f3a4fdc7f/crates/model/src/orderbook/ladder.rs#L118-L139)
  also uses BTreeMap price levels, plus an order-ID cache for its broader L1/L2/L3
  responsibilities. Its [depth replacement](https://github.com/nautechsystems/nautilus_trader/blob/202808567f6233d538b6d8c9915b071f3a4fdc7f/crates/model/src/orderbook/book.rs#L558-L625)
  clears existing ladders and applies normalized orders. That API's preconditions
  differ from our fallible network-input validation; do not copy destructive clearing
  or L3 order caches into an aggregate L2 collector.
- [hftbacktest HashMapMarketDepth](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/hashmapmarketdepth.rs#L313-L363)
  reuses hash-map capacity by clearing, then inserts price-tick quantities; sorted
  output requires collection/sorting. Float rounding, best-price repair heuristics
  and in-place replacement are different contracts. A hash map cannot recover
  missing exchange history merely by changing the best-price heuristic.
- [hftbacktest ROI vector](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/roivectormarketdepth.rs#L464-L500)
  clears dense arrays and skips prices outside its configured region. That is an
  explicit strategy-region tradeoff, incompatible with silently discarding full-depth
  collector data. At our admitted millionth-price granularity over [0,1], two i64
  arrays alone need 16,000,016 bytes per replica book (~1.49 GiB for 100 books).
  Coarser indexing requires a proven tick contract and transition handling.
- [Boost flat containers](https://www.boost.org/doc/libs/latest/doc/html/container/non_standard_containers.html)
  document the sorted-vector alternative: contiguous storage and ordered iteration,
  logarithmic lookup, but linear insertion/deletion and reference invalidation.
  Their old illustrative speed ratios are not predictions for our workload. A safe
  Rust Vec ladder would need churn/depth tests and the same absolute/relative update
  oracle; it is a larger representation change, not a free snapshot optimization.

### Our snapshot path: why each block exists and what it costs

| Local block | Contract / ownership | Work to attribute before changing it |
|---|---|---|
| `polymarket.rs:146–174` | Entire bounded snapshot parses before an Event exists; exact scalar types, both sides, identity and timestamp required | Envelope/nested scans and Vec growth, not book construction; do not remove validation to improve constructor results |
| `store.rs:1010–1020` | Resolve requested session binding; unknown/unrequested/stale rules remain distinct | String lookups; E03 already isolated them, no proven pipeline gain |
| `store.rs:1021–1029` | Preflight side limits and old-plus-new level peak | Checked accounting; this is a level bound, not a byte-accurate allocator bound |
| `book.rs:45–47` | Own decoded vectors; create a separate, unpublished book | Moving vectors is not cloning levels; old book remains owned by replica |
| `book.rs:48–54` | Insert each level and detect repeated scaled prices | Tree searches/splits/node allocation; partial new book is discarded on error |
| `book.rs:56` | Remove zero levels only after duplicate validation | Whole-side traversal; skipping it unconditionally changes behavior |
| `store.rs:1030–1042` | Construction failure invalidates the affected replica without publishing a partial replacement | Preserve old financial levels as stale evidence, not as a valid book |
| `store.rs:1043–1063` | Commit replacement, metadata and accepted activity under one owner | Assigning the new book synchronously destroys the old tree; construction-only timing misses that reclamation cost |
| `store.rs:1064–1096` | Selected source emits full snapshot; standby emits provenance only | Borrowed ordered iteration and recorder cost must remain separate from construction |

The retained V1 fixture has four initial snapshots with bid/ask depths 149/41,
41/149, 48/143 and 143/48; no zero snapshot quantities; bids arrive ascending.
This is observed input, not a guaranteed exchange sort order or depth ceiling.
The next evidence should cover those actual ladders plus shuffled and adversarial
zero/duplicate cases, and separately attribute build, old-book destruction and
ordered encoding. A faster constructor alone cannot establish a faster complete
replacement. No fork benchmark or source observation changes the failed E02 gate.

### Retained-snapshot attribution (unchanged constructor)

Executed a temporary release-only diagnostic over all four retained initial
snapshots, in wire, reversed and deterministically permuted level order. Three
passes, 200 warmup replacements and 2,000 measured replacements per case. The
supplied SnapshotDomain reader checked levels/order, exact symbol and both times
against the independent fixture expectations; every timed encoding then matched
those checked bytes. All 79,200 encodings (including warmup) matched. Input clones,
parsing, output allocation and comparisons were outside the timed spans.

Median of the 12 per-case run medians in microseconds (not pooled percentiles):

| Input ordering | Build new book | Commit + old-tree drop | Borrowed SBE encode |
|---|---:|---:|---:|
| Captured wire | 4.769 | 0.622 | 0.461 |
| Reversed | 4.717 | 0.607 | 0.468 |
| Permuted | 4.865 | 0.544 | 0.456 |

This is a warm, isolated snapshot-stage diagnostic, not full Processor latency,
allocation-stall tails or consumer-visible latency. Clock overhead is included;
do not sum stage percentiles into an end-to-end percentile. The initial diagnostic
had two compile errors (different generated bid/ask types and an overlong closure
borrow); both were corrected before the successful 0.50s execution. Source patch
and successful log are in `fork-research/snapshot-stages.{patch,log}`; temporary
source instrumentation was removed and `src/` again matched HEAD.

Decision: construction is the largest of these measured stages, so retest the
existing H02 validated bulk-construction patch once against E10's improved shared
measurement baseline. Do not add deferred reclamation, a dense ladder, a custom
allocator or new representation. Rust's second sort and temporary storage still
count against the candidate. Use the same primary and regression gates; snapshot
bursts are the primary workload and mixed deltas the control. E02's failed queue
guard remains in the record. Passing this small screen alone is not promotion.

### Construction changes subsequent update work

Rust 1.93 [`bulk_push`](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/append.rs#L36-L95)
fills leaf nodes before extending the right border, whereas ordinary
[`splitpoint`](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/node.rs#L909-L924)
redistributes entries around a middle key. Its
[`find_key_index`](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/search.rs#L216-L233)
searches a node linearly. Thus identical sorted rows do not imply identical future
comparison costs. The relevant local consumers are `Book::quantity` during
whole-event preparation and `Book::set` during commit, not just replacement.

After E11 failed its mixed control, a separate deterministic counted-key probe
replayed both workloads' bid mutations through insert-built and bulk-built maps.
An independent Vec reference checked old quantities and every resulting book.
The counted-key wrapper used the same i64 ordering; atomic instrumentation makes
this a comparison-count diagnostic, not a latency benchmark. Construction counts
and 2,000 warmup messages were excluded; 20,000 later message shapes were included.
Mixed get/mutation comparisons increased 0.66%/1.61%; burst comparisons increased
14.81%/15.84%. Bulk construction can therefore save replacement time while doing
more work on subsequent updates. That mechanism alone does not explain E11's
mixed 10–15% p99 regression: source layout, caches and scheduling are still
unattributed. Do not introduce a depth threshold fitted to these two workloads.
Full E11 results and rejection are recorded in EXPERIMENTS.md.

## E12 — mixed-update ownership and type audit

New unchanged-code diagnostic: ten seconds at 5,000 messages/s, 100 books, two
seconds warmup, local ingress/file, perf cpu-clock:u at 1,999Hz with DWARF stacks.
Output replay passed. Of 2,836 samples (zero reported lost), 951 had a Processor
frame: 418 involved serde_json, 145 envelope, 303 decode, 79 resolve, 58 Recorder,
and 11 Health. These ancestor counts overlap, include setup/warmup and have
unresolved frames; they are not disjoint CPU shares or uninstrumented latency.
They justify investigating repeated JSON traversal before changing queues or
health storage. Logs and flat profile are under `e12/`.

| Hot-path representation | Necessity / current ownership | Decision for this experiment |
|---|---|---|
| `Utf8Bytes(Bytes)` payload | Owned handoff; existing reference-counted backing, moved into ingress | No additional Arc or copy; borrowed decoding stays within envelope lifetime |
| `Arc<Semaphore>` | Shares byte credits across socket producers and outstanding envelopes | Preserve real shared ownership and accounting; no unsafe borrowed permits |
| `Price(i64)`, `Quantity(i64)` and relative i64 | Exact scale, overflow and supplied SBE range | No floats or narrower integer assumption |
| `BookId(u32)`, Copy `Binding`/`Session` | Small stable indices plus generation/revision fences | Copying these is not heap cloning; Arc would add allocation/atomic traffic |
| `Vec<Instrument>` | Stable-index owner storage; direct BookId access | Already avoids a second ownership indirection; retain |
| Owned registry/requested String keys | Outlive frames and encode different identity/membership contracts | Do not merge maps or intern IDs without preserving namespace and retirement rules |
| Borrowed `Cow<str>` event IDs | Frame-local values; escapes require owned decoding | Existing E07 borrowing candidate failed; don't stack it into this change |
| Reused prepared Vec and numeric HashMaps | Preserve intermediate updates and whole-event preflight | Already retain capacity; don't remove provisional state or use weak hashing blindly |
| BTreeMap replica/source and price maps | Source isolation, ordered iteration, sparse prices | Candidate representations need separate measured evidence; E11 rejects bulk building |
| BTreeSet deadline indexes | Expiring work without scanning all books per event | No replacement with per-event full scans |
| Fixed Health buckets | Bounded receipt-time history without per-entry allocation | Only 11 sampled Processor stacks; not the first target |
| Cached symbols and borrowed SBE views | Level output does not allocate another symbol/JSON object | No shared-pointer conversion needed; output batches already move ownership |
| `Vec<&RawValue>` envelope | Complete array validation before exposing any event | Necessary for current array atomicity; avoid it only when one fully validated object can be decoded directly |

Horizontal references: the [Rust Performance Book](https://nnethercote.github.io/perf-book/heap-allocations.html)
recommends profiling cloning sites, borrowing where appropriate and reusing
workhorse collections; it explicitly warns that rarely shared Rc/Arc can add
allocations. [Arc documentation](https://doc.rust-lang.org/std/sync/struct.Arc.html)
identifies atomic reference-count overhead. These are tradeoffs, not permission
to share the mutable book owner or a promise that a container change will help.

Vertical source evidence: locked serde_json 1.0.154 `de.rs:1285–1293` scans an
entire RawValue with `ignore_value`; `from_trait` at 2507–2519 calls `de.end()` after
typed deserialization, checking the complete input. Therefore a successful typed
decode of a **single object** already establishes full syntactic consumption.
The current envelope pass scans it once before typed parsing scans it again.
This is unnecessary work on valid single-object messages, not missing validation.

H12 candidate: attempt the existing exact/bounded decoder directly only for an
admitted single object. On success emit once. On any decode failure, use the
unchanged envelope-then-decode path so malformed syntax, semantic failure scope,
resource errors and diagnostics retain their existing classification. Arrays keep
full-envelope validation. Preserve sink failures without fallback/retry. No custom
parser, unsafe RawValue construction, type-width change or additional dependency.
Invalid objects may do extra bounded parsing; measure that diagnostic separately.
Reject on any contract mismatch or failed mixed/native-ID pipeline screen; don't
combine this with previous parked candidates.

E12 short-screen outcome: all six processing-p99 comparisons improved. Mixed
passed all guards; native-ID pair 1 failed queue p99 (122,635 → 130,577ns, +6.48%),
while its processing p99/p99.9 improved. Do not declare this a general optimization
or silently waive the queue guard. Known A/A variability and oracle-on-owner
interference make a cause attribution unsupported by aggregate quantiles.

Bounded follow-up fixed before execution: one native-ID qualification campaign,
three alternating 120s pairs at the same 100 books/1,000 messages/s, original
thresholds and exact candidate/measurement patches. This also exercises minute
snapshots and full debug/release/check/Clippy gates in both arms. This is a longer
held-out validation, not a retry-until-pass loop; preserve the failed 20s screen.
If queue non-regression remains unresolved, keep the candidate experimental or
remove it, rather than claiming an overall win. No live/universe expansion.

That qualification failed one pair; the source patch was removed. Full results
and the measured invalid-input fallback cost remain in EXPERIMENTS.md.

## E13 — actual arrival pacing is part of the workload

Vertical audit of `tests/recording_load.rs` found `tokio::time::sleep_until` in the
independent ingress producer. Locked Tokio 1.54.1
[`sleep.rs`](https://docs.rs/tokio/1.54.1/src/tokio/time/sleep.rs.html) explicitly
documents millisecond timer granularity. The benchmark asks for intervals of
1,000µs or even 200µs, so timer rounding and catch-up can create unintended batches.
This is distinct from intended exchange bursts, and average-rate validation alone
does not detect it. A small alternating timer-only diagnostic reproduced short
gaps in 17–19% / ~83% of Tokio arrivals at 1k/s / 5k/s; a blocking-thread standard
sleep producer had no short gaps in those same bounded trials. OS oversleep and
host jitter remain possible; neither API promises hard real-time delivery.

The ingress harness now uses one blocking producer with absolute deadlines and
records actual offered lag and interarrival distributions in both modes. Socket
mode remains explicitly Tokio-paced because its async control handling is a
different concern; do not silently treat it as the corrected ingress pacer. This
changes the shared measurement baseline, not production transport. Keep the
oracle, message shapes, exact outputs, warmup and acceptance thresholds unchanged.
Run identical-binary calibration before another candidate. It remains unresolved
how much of E12's observed regression came from pacing, scheduling or source code.

## E14 — separate verification work from collector service time

The E13 identical-binary control still varied 31.580–51.566µs at processing p99.
The [Rust Performance Book's benchmarking guidance](https://nnethercote.github.io/perf-book/benchmarking.html)
distinguishes wall-time from other metrics and warns that it can have high
variance, including ephemeral memory-layout effects. This supports measuring
controls, not selecting the fastest repeated trial or attributing every tail to
one source change. No observed evidence identifies host scheduling, hash seeding
or layout as the cause of this particular variation.

Direct inspection identifies a removable measurement contaminant: the independent
reference engine executes on the same thread immediately before Processor,
allocating, mutating a second book and writing expected SBE records. Its measured
p99 is 26–35µs in E13. It is not part of the production collector. E14 records only
input identity/receipt timestamps and capture cutoffs during intake, then replays
the same independent actions after output drain. Periodic snapshots must use
state at their original cutoff, not the final book; the deterministic test checks
this across updates, deletion and replacement. Correctness checking remains
complete, and no accepted record is omitted to improve timing.

This changes measurement fidelity, not production speed. One fresh A/A calibration
is required before any candidate comparison. It can falsify the claim that
removing reference interference suffices to stabilize the benchmark. Residual
clock/sample-vector overhead and virtualized scheduling remain limitations;
neither a shared pointer nor a new parser resolves those measurement questions.

Outcome: that control remained noisy (32.870–56.064µs processing p99), despite
exact replay in every run. A separate same-clock perf switch trace overlapped only
2/10,000 processing spans with guest descheduling; subtracting those intervals
left p99 unchanged. Thus ordinary guest task switches do not explain most tails
in that diagnostic. IRQ/hypervisor time, cache/frequency and layout remain
unattributed; hardware cycles/instruction counters are unsupported in this orb.
Do not replace "unexplained" with "host noise" or blame a specific Rust type.
See E14 in EXPERIMENTS.md for counts, limitations and preserved evidence.

## E15 — adapt direct object decoding without field-order assumptions

The user requested a parser comparison followed by applicable implementation.
Reference: NautilusTrader revision `202808567f6233d538b6d8c9915b071f3a4fdc7f`,
[`MarketWsMessage::parse` and visitor](https://github.com/nautechsystems/nautilus_trader/blob/202808567f6233d538b6d8c9915b071f3a4fdc7f/crates/adapters/polymarket/src/websocket/messages.rs#L395-L490).
It dispatches directly when `event_type` is first, calls `Deserializer::end()`
before success, and falls back for reordered input. Its market batch handler
first parses `Vec<&RawValue>`, then parses elements; it is not universally a
single-pass implementation. This is source comparison, not a performance claim
about their complete system. No upstream source was copied into production.

Applicable mechanism: combine complete object validation with typed parsing and
check the input end before exposing the result. Our existing Wire supports every
field order, so retain it rather than importing a first-key assumption, another
parser or a generic dispatch framework. Arrays keep complete prevalidation.
Nested level arrays still use bounded RawValue-based parsing: this removes one
outer scan, not every internal JSON traversal.

The earlier H12 candidate retried the entire pipeline after any semantic failure.
E15 instead separates Wire parsing from financial normalization. Successful Wire
parsing plus `end()` proves complete syntax; semantic failures use the same Wire
for affected-ID extraction. Snapshot/tick IDs move into Events only after all
validation succeeds, so a late timestamp error retains its scope without an
extra clone. Multi-asset failures still scan bounded identities to include entries
after the failing quantity. Typed-field failures validate raw syntax before
reporting an event error; malformed syntax remains a source-level envelope error.
Only whitespace-prefixed typed failures reparse to retain the original relative
line/column diagnostics. Trailing-input errors after successful Wire parsing
return directly from `end()`; they need no second classification scan.

Acceptance separates three claims: preserved behavior (differential tests and
independent SBE replay), reduced decoder work (source mechanism and isolated
measurement), and overall latency (paired pipeline results, still affected by
E14 control variability). A local decoder improvement does not establish a
pipeline win. No change to numeric scale, book state, output schema, dependency,
queue or ownership topology is bundled into this slice.

### E17/E18 — combine measured mechanisms, then profile the remaining work

E17 restored optional Cow borrowing under the adopted workflow. It reproduced
3,215 fewer Processor-stack allocation calls both alone (26,570 → 23,355) and
alongside H15 (20,572 → 17,357). The decoder and pipeline timing outcomes were
mixed; exact replay passed. Retain the allocation mechanism experimentally with
its tail-regression warning, not as a claim that allocation count predicts time.
The earlier E07 statement that H08 was parked describes that earlier decision.

After H15+H08, E18 sampled a bounded ten-second mixed replay at 5k/s using
`perf record -e cpu-clock -F 997 --call-graph dwarf,16384`, separately from latency
comparisons. Of 1,547 software CPU samples, 420 contained Processor, 65 contained
resolve and 48 contained Recorder. Nested categories overlap; samples include
warmup/initialization and instrumentation, not just measured updates. These are
not hardware cycles or evidence that resolve caused a particular wall-time tail.
Generator stacks appeared in 391 samples, so whole-program percentages must not
be presented as collector-only costs. Crossbeam receive also sampled heavily;
its locked bounded-channel implementation spins/backoffs before parking. That
does not establish that its queue design is wrong or justify spinning more.

The new owner profile supports revisiting H05 on the changed implementation.
The applicable mechanism remains the reviewed Lasso stable-key resolver, not a
new shared pointer/cache. Current source inspection confirms append-only BookIds,
namespace-specific subscription construction, active-session checks before decode,
and retained replica binding/state validation. Reuse Binding.book only on request
hits; retain registry validation on misses. E03's isolated savings and failed
pipeline screens remain recorded. Compare this third independently researched
change against H15+H08; do not combine the parked tree/preparation-map changes.

E18 completed that comparison: post-change resolve appeared in 30/390 Processor
samples versus 65/420 before, but short-ID/low-rate latency screens failed. The
native-width 5k/s smoke passed all three pairs and guards; longer twenty-second
confirmation failed one pair with a 77.56% processing-p99 regression. This does
not establish overall speedup or explain the regression. Keep the computational
candidate experimental with its exact baseline preserved; do not promote from
the favorable median or dismiss the failed pair as environmental noise.

### E19 — separate harness allocation from collector work

The bounded stage trace found a specific measurement defect before another
collector hypothesis was warranted: `encoding.push()` grew a latency-sample Vec
inside the measured Processor callback. The Rust Vec growth mechanism here is
directly observable, not a recommendation to replace every Vec or use shared
pointers. A same-executable setup toggle preallocated and touched its pages
before intake. Repeated capacity-crossing publication spikes fell from 78–123µs
to 3–15µs at the same input ordinals. Linux minor-fault IPs and disassembly locate
first-touch faults at the sample store, outside Recorder itself. Reserving capacity
alone would not guarantee touching the pages; filling with nonzero values before
clearing avoids that omission without unsafe code or a new allocator.

This moves harness setup work, not collector work. Most slow batches remained
unexplained: even with prepared storage, 327 batches exceeded 40µs; none had a
minor fault and only two overlapped guest descheduling. Sampled free output
capacity stayed high. These observations do not prove host scheduling, cache
misses, parser allocations or writer contention caused the residual tail.
Per-fault tracing also perturbs faulting runs. The corrected harness is a new
measurement baseline, not grounds to revise E18 into a pass. Preserve both
acceptance failures and mechanism evidence; detailed results are in E19.

## E20 — specialization audit across C1–C5

Scope: inspect existing ownership, state, lifetime and resource contracts; rank
avoidable work, then test one candidate in a disposable checkout. No production
architecture change, unsafe Rust, dependency, live campaign, commit or push.
Earlier failures remain evidence, not permission to promote a stacked change.

### What the Lemire article does and does not establish

Read the [article](https://x.com/lemire/status/2115046718039306377) and pinned
`ConstexprCore/perfect_hash` revision `4fdf1b7fff40838c8a43080bac066429c9452327`.
Its principle is to pay for invariant decisions once, then carry their verified
result in the representation. That can mean generated dispatch, an indexed
binding, typed input, an initialized reusable buffer or a precomputed selection;
it does not mean replacing every HashMap with a perfect hash.

- [Generator](https://github.com/ConstexprCore/perfect_hash/blob/4fdf1b7fff40838c8a43080bac066429c9452327/include/ConstexprCore/detail/gperf_generator.h#L1076-L1119)
  selects a scheme for a fixed key set. [Lookup](https://github.com/ConstexprCore/perfect_hash/blob/4fdf1b7fff40838c8a43080bac066429c9452327/include/ConstexprCore/perfect_hash.h#L610-L622)
  still checks exact candidate length/content. Collision-free placement of known
  keys does not prove that an arbitrary incoming key belongs to the set.
- [Rust PHF](https://github.com/rust-phf/rust-phf/blob/79bf7d18be8496764befcb2260b47bba8fcd01ac/phf/src/map.rs#L168-L207)
  likewise verifies equality; its [default shared hash](https://github.com/rust-phf/rust-phf/blob/79bf7d18be8496764befcb2260b47bba8fcd01ac/phf_shared/src/lib.rs#L34-L65)
  still uses SipHash-1-3. Precomputed placement is not zero runtime hashing.
- [Serde derive](https://github.com/serde-rs/serde/blob/0db9b3b504bcb0a7fa9cf001429869519afd3e9a/serde_derive/src/de/identifier.rs#L448-L473)
  emits literal matches, not a HashMap for field names. The installed derive
  source has the same mechanism. The C++ comparison to chained unordered_map
  is not our baseline. The upstream [benchmark](https://github.com/ConstexprCore/perfect_hash/blob/4fdf1b7fff40838c8a43080bac066429c9452327/benchmarks/bench_protocol.cpp#L113-L208)
  takes the fastest of 300 repetitions; we did not reproduce its headline rate
  and do not equate it with p99 latency or sustainable collector capacity.
- The C++ [SSE2](https://github.com/ConstexprCore/perfect_hash/blob/4fdf1b7fff40838c8a43080bac066429c9452327/include/ConstexprCore/detail/sse2_compare.h#L56-L96)
  and [NEON](https://github.com/ConstexprCore/perfect_hash/blob/4fdf1b7fff40838c8a43080bac066429c9452327/include/ConstexprCore/detail/neon_compare.h#L60-L85)
  paths intentionally read beyond short logical strings under page guards.
  Mapped memory is not initialized, in-allocation Rust slice memory. Do not copy
  these overreads. Bounded comparisons can already receive compiler vectorization;
  padded copies add work and need their own break-even evidence.
- [DuckDB dictionary vectors](https://github.com/duckdb/duckdb/blob/93ed7eae2f4d196a03c2981c6bcebf71f3bbff4b/src/common/vector/dictionary_vector.cpp#L67-L78)
  reuse payload through selection indices and carry validity with it.
  [Selection composition](https://github.com/duckdb/duckdb/blob/93ed7eae2f4d196a03c2981c6bcebf71f3bbff4b/src/common/types/selection_vector.cpp#L188-L198)
  preserves the supplied order and duplicates; it does not authorize sorting our
  feed. Its 256-mask selection table illustrates a genuinely small finite domain.
- [NautilusTrader](https://github.com/nautechsystems/nautilus_trader/blob/202808567f6233d538b6d8c9915b071f3a4fdc7f/crates/adapters/polymarket/src/websocket/messages.rs#L395-L490)
  specializes known message types with fallback and checks end-of-input. Its
  event_type-first fast path would miss all 31 retained delta objects here,
  which begin with `market`. Borrow the mechanism, not the field-order assumption.
- [hftbacktest ROI depth](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/roivectormarketdepth.rs#L10-L108)
  indexes tick offsets into contiguous vectors, but intentionally excludes prices
  outside its region, uses floats and unsafe access. Those choices do not satisfy
  our full-book exact-decimal contract. At our full millionth resolution, two
  dense i64 sides alone need 16,000,016 bytes per replica (before other state).
  A 100,000-instrument, two-replica universe would need about 3.2 TB just for those
  quantities. Narrower tick grids require proven lattice invariants across tick
  changes, replacement and replay; observed current tick size is insufficient.

### Ranked opportunities and rejection conditions

Ranking uses frequency × plausibly removable work × demonstrated relevance,
then discounts correctness risk, memory and implementation complexity. It is
ordinal, not invented CPU percentages or promised speedups. Frequency is per
accepted path; no full-universe population/traffic distribution is assumed.

| Rank / owner | Known information and time | Repeated work / possible specialization | Already optimized? | Invariants and cost / decision |
|---|---|---|---|---|
| 1 — C3 nested decoding | Field vocabulary at compile time; entry bound at startup; event shape after decoding | Raw `price_changes` scan followed by typed scan per object; directly construct bounded typed entries | Outer object scan removed by H15; optional strings borrow; nested list still rescanned | End-of-input before emission, arbitrary field order, exact error scope/precedence, strict caps. No extra persistent state; error fallback may add CPU. E20 investigates this first because E19 decode was the largest normal 16-entry phase. |
| 2 — C4 preparation | Binding at subscription; one session/slot and event at processing | Numeric tuple hashing for quantities/counts per entry; test small event-local indexed side-count state with bounded fallback | Scratch Vec and maps already reused; H05 already reuses Binding.book; H06 Entry-only experiment failed | Repeated prices, both sides, relative deltas, intermediate budgets and whole-event prevalidation cannot be skipped. Extra epoch arrays cost memory per instrument; small linear structures can become quadratic. Profile distinct-key distribution before choosing representation. |
| 3 — C4 health observations | Same receive time throughout an event; accepted binding runs after preparation | Repeated bucket/clock checks per entry could become one count update per consecutive binding run | Fixed seven-bucket/five-minute arrays, no allocation; counts API already accepts multiple entries | Never merge financial updates or change output order. Counter overflow/invalid receive times must fail consistently; all entries, including zero/no-op sets, count. Retained two-asset deltas have no adjacent repeated IDs, so synthetic 16-same-asset results cannot establish typical savings. |
| 4 — C4 replica access/layout | Binding/generation at subscription; manager provisions one or two slots | Repeated small replica BTreeMap lookups in resolve/prepare/apply/publish; compact stable replica handle or small sorted container | Books already Vec-indexed by u32 BookId; source protocols selected per session | Processor accepts u16 slots and global replica limits, not a proven two-slot-only public contract. Preserve stale-binding, retirement and deterministic selection guards. Flat handles need lifetime rules; inline Replica arrays duplicate large health/book state. Benchmark cardinality and bytes before changing layout. |
| 5 — C5 encoding/buffers | Schema compile time; exact symbol on first observation; timestamps per event | Repeated timestamp conversion, identity verification, zero-fill then overwrite; investigate generated wire setters or initialized fixed buffers | Generated static codecs, borrowed observations, cached symbols, fixed reusable batch pool; level path allocates no JSON/symbol | Preserve timestamp reader range, missing-time rejection, exact identity, schema padding, partial-write poison and pool bounds. Compiler may fold constant stores/checks; moving checks needs a trusted type, not a bypass. Buffer cursors complicate failed encoding rollback. Profile before code. |
| 6 — C4 scheduling | Periods at startup; current deadlines after source/selection transition | Ordered deadline tree updates could use buckets/wheel at large populations | Only due/dirty books are examined; retired checks expire; bounded time slices already implemented | Quiet live books still need monitoring; preserve 60-second snapshot cadence, source suspension and overdue accounting. Quantization can change semantics; no observed scheduling bottleneck at current small load. |
| 7 — C2 ingress | Budget/config at startup; payload length at receive | Per-message atomic byte-permit ownership; potential credit batching only if measured dominant | Payload Utf8Bytes moves, no JSON parse/deep clone here; static protocol dispatch | Credits must remain held until accepted envelope processing completes. Batching hides real outstanding bytes or changes overload/failure behavior unless carefully designed. A new Arc is not automatically less work; current Arc keeps the budget alive. High correctness complexity, no rewrite justified. |
| 8 — C2 WebSockets/assignments | Adapter at worker creation; desired set/revision at reconciliation | Rebuild/sort subscription JSON on reconnect; precompute immutable request chunks per revision if reconnect cost dominates | Worker<P> is statically dispatched; protocol/config clones happen at session boundaries, not each level; heartbeat is a literal comparison | Incremental removals, byte/count limits, generation ordering and readiness ambiguity invalidate cached requests. Control-plane frequency is low; avoid duplicating desired state for steady-state latency. |
| 9 — C1 discovery | Adapter/HTTP client at startup; exact IDs and eligibility per observation | Snapshot cloning/scan normalization, potentially reused storage or compact internal IDs | Cached Arc<Snapshot> already clones the registry only after mutations; clients reused; namespaces explicit | Membership is dynamic: compile-time perfect hashing is inapplicable. Preserve leading zeros, version scope, incomplete-scan safety, verification TTLs and retirement. REST/control-plane cost is not the market hot path; no change proposed without discovery profiling. |
| 10 — C3 fixed names/decimals/SIMD | Small keyword set and 1e6 scale at compile time | PHF dispatch, vector digit checks, fused decimal parsing | Serde/literal matches already specialize; constant arithmetic is compiler-visible; H07 measured only ~1–10ns/value | Unknown/escaped strings, exact grammar/overflow and no rounding remain mandatory. Branch-free is not inherently cheaper than predictable branches. Long-key/dense numeric tricks do not automatically fit short decimals; no unsafe SIMD or new parser dependency. |

The highest-value mapping improvement already implemented locally is H05's
subscription-time Binding.book reuse, not a new global interner. Dynamic unknown
IDs still require exact membership checks. C1's Arc snapshots and C2's owned
Utf8Bytes are appropriate sharing boundaries; replacing every String or Book
with Arc would add reference-count operations and would not remove wire hashing.

### First specialization experiment

E20 uses a generic Wire representation instantiated with either RawValue or a
bounded typed Change list. Only default-limit single objects attempt the typed
path. Successful snapshots reuse their raw book fields; top-level arrays and
non-default limits keep the old path. All fast-path failures fall back before
emission, preserving the existing decoder's error identity and precedence.
This deliberate restriction isolates the mechanism without a new parser,
configuration system, public type contract or production rollout. The cost is
extra code and reparsing of invalid inputs; record that cost alongside successful
decode savings. Full campaign outcomes and decision are in EXPERIMENTS.md E20.

Compiler control: Rust 1.93.0 / LLVM 21.1.8, x86-64 `rustc -O` lowered an
equivalent seven-event literal match to length dispatch, integer comparisons and
SSE2 equality for the two 16-byte names. Those wide reads occur only after length
dispatch. No hash table, allocation or custom unsafe code was needed. This is a
small equivalent-expression control, not a claim about all instructions in the
linked decoder. Exact-membership tests include prefixes, same-length misses,
case changes and embedded NUL. Source/assembly are retained with E20 artifacts.

Outcome: 51,040 differential cases and all 1,765,230 independent SBE records
matched. Equivalent-build 16-entry decoder loop time fell 22.70–29.75% in three
pairs; 64-entry time fell 27.02–33.69%. Retained small deltas improved in two pairs
but regressed 10.01% in one total-loop measurement, despite lower median call
time. Snapshot-array loop times regressed 0.77–14.30%. Pipeline guards failed at
both tested rates. Invalid-input fallback also added real work. Keep the
prototype archived, not integrated; the ranking identifies where to investigate,
not a blanket recommendation to specialize more code. No production source
changed in this audit. Retain batch-width benchmark coverage for future tests.

## E21 — C4 preparation and snapshot attribution

Scope: C4 only; C2 queue delay and C5 callback cost are controls. C1 remains
unchanged and new C3 optimizations remain paused. Baseline includes H15/H08/H05,
not bare HEAD. No production architecture, unsafe, dependency or live-feed change.

### Existing representations and what they actually cost

| Structure / operation | Invariant known before the hot path | Repeated work / conclusion |
|---|---|---|
| `books: Vec<Instrument>`, `BookId(u32)` | Exact external identity is bound at subscription time | Direct numeric indexing already exists; H05 removed duplicate global identity hashing. Incoming opaque IDs still require exact session-scoped resolution. A perfect hash cannot assume a fixed universe. |
| `Source.requested: HashMap<String, Binding>` | Session, revision and slot established by controls | Each entry resolves identity and validates its binding. Replacing Strings with shared pointers would not remove string-content hashing and adds ownership machinery. No change without lookup attribution. |
| `quantities: HashMap<(BookId,u16,Side,Price),Quantity>` | Numeric key after resolution; no fixed price grid | Provisional values are necessary for repeated-price `Set`/signed `Add`. Storage already retains capacity between events. H06's get+insert→Entry change failed earlier pipeline screens; it is not silently bundled here. |
| `counts: HashMap<(BookId,u16,Side),usize>` | Config and committed occupancy are valid | Entry lookup runs even when occupancy is unchanged. This is the smallest removable work; intermediate transitions still need immediate checks. |
| `prepared: Vec<(Binding,Side,Level,usize)>` | At most bounded decoded entries | Retains capacity and exact original entry indices. Separate prevalidation/mutation/publication preserves atomicity and intermediate history; do not coalesce duplicates or remove this vector merely to save an allocation. |
| Per-instrument `BTreeMap<u16,Replica>` | Manager usually provisions one/two slots | Public Processor permits more slots. A two-element replacement would narrow a real contract. Dynamic lifecycle/selection invariants must be carried by any compact representation. |
| Two `BTreeMap<Price,Quantity>` book sides | Exact integer prices, arbitrary legal ordering | `len()` is a stored-length load. Replacement inserts and validates duplicates, then traverses for zeros. Tree construction allocates; assignment synchronously frees the old tree. `clear()` does not retain a reusable node arena. |
| Health / dirty / snapshot due state | Source ownership and logical clocks | Already bounded/reused in several places. Current replay excludes maintenance ticks, so its results cannot justify changing health scheduling. |

Peak validation currently counts old plus incoming levels, including zero input
levels before filtering. This is a logical level budget, not a byte-accurate RSS
bound. Skipping its checks, clearing the old book before validation, or deferring
old-tree destruction would change correctness/resource contracts.

### Source-backed hypotheses and priorities

1. **H17: count occupancy only on an occupancy transition.** Highest frequency,
   no new retained state. Current `previous != 0` and `quantity != 0` already
   supply the predicate. Initialize side count lazily on its first transition;
   earlier nontransitions leave its committed side length valid. Reject if
   intermediate limits/history differ or transition-heavy controls regress.
2. **H18: skip snapshot zero-filter traversal when no zero was inserted.**
   Accumulate a flag during the existing insertion loop; keep duplicate detection
   and insertion order, then retain only when needed. Unlike H02 bulk construction,
   this does not intentionally change tree shape. Duplicate positive/zero pairs
   must still reject before publication. Reject if flag work or zero-heavy inputs
   erase the saving; its ceiling is only part of C4's construction share.
3. **H19: small bounded event-local side-count records, existing map fallback.**
   Only worth measuring after H17 establishes remaining work. Two records are
   an experimental cap, not an exchange guarantee. Transfer provisional counts
   correctly on the third binding, preserve both sides and every intermediate
   global limit. Reject if linear comparisons/fallback cost erase any gain.

Pinned implementations inspected (existing snapshots, not claimed latest):

- hftbacktest `5f3ec40b2afb764e0fea112f941ed85523ef4e88`,
  [occupied/vacant level update](https://github.com/nkaz001/hftbacktest/blob/5f3ec40b2afb764e0fea112f941ed85523ef4e88/hftbacktest/src/depth/hashmapmarketdepth.rs#L96-L110).
- Nautilus `202808567f6233d538b6d8c9915b071f3a4fdc7f`,
  [positive update versus zero removal](https://github.com/nautechsystems/nautilus_trader/blob/202808567f6233d538b6d8c9915b071f3a4fdc7f/crates/model/src/orderbook/ladder.rs#L344-L382)
  and [snapshot replacement](https://github.com/nautechsystems/nautilus_trader/blob/202808567f6233d538b6d8c9915b071f3a4fdc7f/crates/model/src/orderbook/book.rs#L582-L612).
- Rust 1.93 [retain](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/map.rs#L1170-L1178),
  [bulk FromIterator](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/map.rs#L2379-L2394)
  and [clear](https://github.com/rust-lang/rust/blob/1.93.0/library/alloc/src/collections/btree/map.rs#L669-L677).

These implementations support separating replacement/insertion/removal work,
not our transactional budget semantics. No code is copied from their float,
unsafe, region-filtered or clear-before-validation paths. Source inspection
does not show that LLVM removes the map lookup; measured executable comparisons
are required. H17 adds no table or memory layout; HashMap random seeds and code
layout can still affect results. No instruction/cache counter claim is made.

### The deep-snapshot tail is not solely C4 reconstruction

Disposable source instrumentation measures matched per-message intervals: decode
outside event callbacks; snapshot resolve/budget; construction including fused
duplicate/zero validation; installation plus old-tree drop; state/health; change
preparation; mutation/health; publication/scheduling. Recorder callback is nested
inside publication, so it must not be added a second time. No per-level clocks.
Rows are retained, not inferred by subtracting independent percentiles.

Two three-second 5k/s burst runs, 750 eight-snapshot arrays each, produced:

| Mean microseconds per array | Run 1 | Run 2 |
|---|---:|---:|
| Decode and wire validation outside event handling | 406.265 | 361.162 |
| Resolve and snapshot resource preflight | 1.515 | 1.213 |
| Tree construction + duplicate/zero validation | 91.694 | 77.035 |
| Install + old-tree destruction | 20.592 | 15.531 |
| State/health bookkeeping | 1.271 | 1.028 |
| Publication/scheduling, including callback | 54.992 | 43.980 |
| Recorder callback alone (nested) | 51.654 | 41.292 |
| Total process span, including unassigned overhead | 577.911 | 501.186 |

Decode occupies 70–72%, construction 15–16%, installation/drop 3–4%, recorder
8–9%. In each run's slowest 1% of arrays, decode still averaged 749.661/465.778µs
and construction 186.179/118.929µs. Multiple stages slowed together; these clocks
do not distinguish CPU execution from descheduling/cache effects. Instrumented
times are attribution, not a new latency baseline. C3 remains paused despite
its large existing share; this evidence limits what a C4-only change can achieve.

The first three subsequent updates to books NOT replaced by the array had queue
p99 1064.834/694.146µs versus 323.048/85.695µs for later updates. This establishes
an offered-position association consistent with head-of-line blocking, not proof
that all delay came from tree construction. The improved harness retains these
groups and shape-specific owner/recorder distributions for uninstrumented runs.
