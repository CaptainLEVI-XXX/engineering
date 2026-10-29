# Research-gated hypothesis backlog

**Frozen at the user's request after E21.** Further optimization and experiment
execution are paused. Preserve the current tested H15+H08+H05 implementation and
all experiment evidence; H17 remains archived, not integrated. This freezes a
correctness-tested baseline, not a qualified latency improvement. Historical
"next" investigations below are backlog only until the user resumes research.

H02 and H06 are parked; H07 is deferred after isolated cost measurement.
E18 reopens H05 on top of H15+H08 after a fresh profile found resolution in
65/420 Processor-containing samples. Post-change resolve samples fell to 30/390,
but the favorable native-width 5k/s smoke failed longer confirmation. Retain the
implementation experimentally with unresolved regressions; no broader promotion
or fourth stacked change is justified by these results.
E06 reduced generator interference but A/A noise remains. H08 was parked after
both pipeline screens failed despite confirmed optional-string allocation reduction.
Under the adopted three-dimension workflow, E17 restored it experimentally:
3,215 removed allocations reproduced alone and alongside H15, but isolated
decoder time and pipeline tails did not consistently improve. The combined
H15+H08 campaign matched 8,318,106 records; no promotion on allocation counts.
E11 reopened H02 after horizontal research and stage attribution; burst gains
repeated, but mixed p99 regressed in every pair, so it remains parked.
E13 corrected pacing and E14 moved reference reconstruction after drain; neither
stabilized identical-binary wall-time tails. E14's switch trace does not support
blaming guest task descheduling for most processing tails. Hardware instruction
counters are unavailable here. Resolve measurement sensitivity before promotion.
H15 is now implemented locally at the user's request: direct object decoding and
reuse of parsed error context. E16 confirmed lower decoder cost through equivalent
release builds and passed its 1k/s tail screen, but 3k/s and 5k/s regression guards
still failed. Retained as a promising computational improvement with unresolved
tail regressions, not an accepted overall latency improvement.
A mechanism in another implementation is necessary evidence, not sufficient
proof that our workload benefits. Unknown costs stay unknown; don't manufacture
an expected speedup.

Before marking `ready-for-experiment`, record: the current measured cost and
source path; the external implementation/mechanism; why its assumptions fit;
the smallest proposed change; failure risks; the falsifier; exact correctness
and performance checks. The agent/reviewer judges substance. The runner checks
that this record exists, not that the research is intellectually sound.

## Ranked investigations

| Priority / ID | Question | Expected benefit if confirmed | Risk / complexity | Current decision |
|---|---|---|---|---|
| 0 / H00 | Can unchanged code generate an apparent latency win? | Prevent accepting noise | No production risk; low | Confirmed repeatedly: E14 identical-binary p99 ranged 32.870–56.064µs after pacing/oracle corrections; no improvement accepted |
| 1 / H01 | Are repeated JSON traversal and bounded-vector allocations material in current decode time? | Less per-message parsing/allocation work | High malformed-input/lifetime risk; medium-high | E01: retained delta decode+envelope median 1.588µs; four-snapshot array 138.639µs. Parser rewrite deferred; allocations still unattributed |
| 2 / H02 | Does validated bulk snapshot construction outperform repeated tree insertion at observed depths? | Reduce snapshot burst interference | Duplicate validation and replacement peak; medium | Parked: E11 researched retest improved burst p99 19–23% but regressed mixed p99 10–15%, with all mixed guards failing. Node-comparison counts confirm an update-cost tradeoff but do not establish the timing regression's cause |
| 3 / H03 | Is the queue tail mostly scheduling/oracle interference rather than queue operations? | Avoid unnecessary queue rewrite; possibly reduce wakeup delays | Ordering/arbitration risk if architecture changes; high | Research separates Tokio and Crossbeam mechanisms; attribution missing |
| 4 / H04 | Does batch policy delay consumer-visible records under realistic output stalls? | Improve visibility without losing accepted history | Output failure/framing risk; high | Existing write-call measurements cannot answer; need consumer measurement |
| 5 / H05 | Can existing Binding.book avoid hashing each accepted asset twice? | Remove redundant string lookup without new state | Identity/namespace and stale binding semantics; low-medium | Parked: isolated lookup gain, but both native-ID and mixed paired pipeline screens failed; candidate removed |
| 6 / H06 | Does Entry reduce numeric preparation get-then-insert cost? | Hash/probe once per provisional quantity | Repeated-key, zero, relative arithmetic and event atomicity; low-medium | Parked: isolated fragment gains, but both E04 paired pipeline screens failed; candidate removed |
| 7 / H07 | Does fused digit validation avoid a redundant decimal pass? | Remove a short byte traversal | Exact arithmetic/grammar and diagnostic precedence; medium | Deferred: isolated saving about 1–10ns/value is too small for current pipeline sensitivity; no production change |
| 8 / H08 | Are optional annotated Cow strings actually borrowed? | Avoid unnecessary ownership allocation while preserving escapes | Lifetimes, absent/null fields and decode failure scope; low-medium | Parked: eliminated 3,215 mixed/5,450 burst string allocations, but both paired latency screens failed. Candidate and its borrowing-only assertion removed; evidence retained |
| 9 / H09 | Would consecutive-entry identity caching help real batches? | Avoid repeating resolution within one event | Cache scope, stale bindings, preserved feed order; low-medium | Not justified by the retained trace: 31 two-asset batches, 62 entries, zero adjacent repeated IDs. Do not generalize this small trace to all exchange traffic |
| 10 / H10 | Does retired-book health work remain on the critical path? | Reduce unnecessary background checks | Quiet live books must still be monitored; medium | E08 confirmed 6,000/3,000/0 checks for 100/50/0 desired books over equal logical intervals. Retirement work already expires; quiet live peers stayed valid. No scheduler rewrite justified |
| 11 / H11 | Is decoder-list reuse worth changing Event ownership? | Remove vector growth/drop allocations | Borrowed frame lifetimes, bounded parsing and whole-event rejection; medium-high | E09 saved 21–52ns for two-entry changes; larger cases were mixed/regressed. Failed the 10% + 0.5µs applicability screen; no ownership rewrite |
| 12 / H12 | Can successful single-object decoding avoid a separate whole-envelope scan? | Remove measured repeated parsing and a one-element Vec | Malformed-tail atomicity, error scope and cold-path fallback cost; medium | Parked: processing p99 improved in 8/9 pairs, but short and long native-ID campaigns each failed a guard. Full correctness passed; production patch removed. E13 addresses a demonstrated producer-pacing flaw before any retest |
| 13 / H15 | Can direct object parsing and reused error context remove traversal without full-error retries? | Less valid-object and semantic-error decoding work | Envelope/event error distinction, retained identities and malformed tails; medium | Implemented locally, not qualified: 43,827 differential cases and full suites passed; isolated valid-object cost fell 30.91%, but mixed p99 worsened in 3/3 pairs and native-ID results failed guards in 2/3. No overall speedup claim or promotion |

## First three experiments

1. **E00 — unchanged-code A/A calibration.** Same commit, toolchain, fixture and
   copied binary; three alternating five-second pairs. Require exact replay and
   complete accounting. Always report `control_only`, even if one label appears
   faster. Record per-pair variation to inform—not retroactively tune—later gates.
2. **E01 — attribute decoding before changing it.** Inspect the locked serde
   implementation, then time 10k–50k decode operations across the retained V1
   trace and asymmetric shallow/deep inputs. Keep malformed-tail, overflow and
   event-atomicity checks. Use independent expected values and separately measure
   instrumentation overhead. A parser/allocation change remains blocked if its
   cost is small or observations cannot distinguish it from other owner work.
3. **E02 — isolate replacement construction.** Measure replacement-only cost by
   depth; compare standard bulk construction only after explicit duplicate checks
   are proven, including duplicate-zero cases and budgets. Retain BTreeMap if
   sorting/validation/allocation eliminates the benefit. Any new benchmark must
   be established identically on baseline and candidate before comparison.

E01/E02 are diagnostic experiment specifications, not authorized performance
claims or permission to skip research gates. Architecture/concurrency/recovery
changes require integration/fault qualification before latency screening can
support promotion. No guessed dead-code removal: prove unused paths and preserve
public behavior; a compiler warning alone is not a performance bottleneck.

## Record passed to the runner

Use `--hypothesis path.json` for different source trees. Fill this template from
actual evidence, then deliberately change status only when the gate is satisfied:

```json
{
  "id": "H01",
  "status": "research-needed",
  "observed_cost": "",
  "implementation_evidence": "",
  "references": [],
  "applicability": "",
  "change": "",
  "correctness_risks": "",
  "falsifier": "",
  "verification": ""
}
```

No runner output automatically authorizes a commit, push or merge. Promotion
requires repeated evidence on representative data, not only three favorable
small samples. Failed ideas stay in `EXPERIMENTS.md`; reopen them only with new
evidence. Use the same threshold and primary metric declared before the run.

## Reclassification under the adopted three-dimension workflow

The historical table above records decisions made under the original p99 screen.
This table separates mechanism evidence from the still-binding regression flags;
it does not retroactively change a failed result or restore deleted candidates.

| Candidate | Computational evidence | Capacity / tail evidence | Current treatment |
|---|---|---|---|
| H15 direct object parsing | E16 equivalent-build retained-update median cost fell 26.75–35.25% in all three pairs | Exact output at 1k/3k/5k; no capacity limit established; 1k tail screen passed, 3k and 5k guards failed | Promising computational improvement, unresolved tail regression; retained locally, not promoted |
| H08 optional Cow borrowing | E17 reproduced 3,215 fewer mixed allocations alone and combined; decoder time mixed | Allocation-only and combined tail screens failed | Retained experimentally alongside H15; allocation benefit, unresolved latency warning, not production accepted |
| H05 repeated identity lookup | E03 isolated savings; E18 resolve samples fell 65/420 → 30/390 | E18 mixed/low-rate screens failed; native-width 5k smoke passed but longer confirmation failed | Retained experimentally with unresolved regressions; no new cache/hasher or promotion |
| H06 repeated numeric lookup | Isolated preparation fragments improved | Both E04 workload screens failed | Archived; no new evidence supporting a combination yet |
| H02 bulk snapshot construction | Replacement throughput improved; tree shape changes later update work | Burst benefit with repeated mixed regression | Regression warning; hold out of combinations pending attribution |
| H07 decimal pass fusion / H11 scratch reuse | Tiny or inconsistent isolated savings | No convincing applicability evidence | Defer, not rejected merely for noisy p99; lower value than current candidates |
| H09 consecutive-ID cache / H10 retired health pruning | Retained data has no adjacent repeated IDs; retired work already expires | Proposed redundancy not established | Do not implement without new workload evidence |

## Next five ranked hypotheses

1. **H15 / equivalent-build parser efficiency and load response.** The scan saving
   should persist when both arms call the same public API from equivalent release
   builds. Test 1k/3k/5k messages/s, per-shape service tails and chronological queue
   age. Falsifier: no stable decoder saving, or an attributable material regression.
   **Executed E16:** decoder saving confirmed; all 8,318,106 output records matched.
   Tail qualification remains open at 3k/s and 5k/s; do not repeat unchanged trials.
2. **H08 / borrow optional wire strings alongside the parser candidate.** Avoid
   already-counted allocations without changing lifetimes beyond a message. First
   recover and measure the allocation candidate alone under equivalent builds;
   only then test the pair. Keep escaped strings and error identities exact.
   **E17 allocation-only completed:** 12.10% fewer Processor-stack allocations,
   all 1,765,230 replay records matched, mixed decoder-time/tail results. Combined
   H15 → H15+H08 removed 15.63% of these allocations and matched 8,318,106 records,
   but failed queue/p99.9 guards. E18 profiles the remainder and isolates H05's
   existing-ID lookup on top of that pair. Its longer confirmation also failed;
   preserve computational evidence and regression warnings separately. Do not
   repeat unchanged trials or stack a fourth change.
3. **Snapshot interference and update layout.** Measure whether replacement bursts
   or subsequent tree updates account for service-time spikes. Separate those
   paths before changing BTreeMap construction/layout; fail on duplicate-level,
   replacement, memory-budget or mixed-update regressions.
4. **Ingress wakeup versus owner saturation.** A bounded load curve may distinguish
   scheduling overhead at low load from sustained queue growth at high load.
   Research the measured regime, not a preferred queue implementation. Sharding
   remains blocked until single-owner saturation and identity-order preservation
   are demonstrated; the first 5k/s screen may not reach saturation at all.
5. **SBE batch visibility versus throughput.** Measure a real downstream reader's
   first decodable record and complete accepted-prefix drain under healthy and
   stalled sinks. Writer-call latency alone cannot justify batch-policy changes.

These are investigations, not five promised rewrites. Each needs its own research
and correctness evidence before implementation; maintain a fixed time/resource
budget and deliver qualified collector changes, not speculative infrastructure.

### E19 diagnostic prerequisite

The requested bounded replay proved that sample-vector growth and first-touch
faults contaminate timed publication. Same-binary off/on/on/off controls removed
the repeated capacity-crossing spikes, but did not explain the remaining tail or
turn E18 into a pass. Retain the harness-only preallocation/page-touch correction;
production candidates and their unresolved regression status are unchanged.

Before another optimization or confirmation, distinguish actual owner CPU work
from wall-time inflation in the remaining decode-heavy outliers using the repaired
measurement source. Control diagnostic clock overhead and preserve unchanged-code
controls. Guest switch records alone were insufficient: 325 of 327 slow prepared
samples had neither a minor fault nor guest descheduling. Do not infer a queue,
parser, allocator or cache redesign from that absence of attribution.

### H16 / E20 — specialize the nested change list

The user's later specialization directive authorizes a horizontal audit and a
bounded mechanism experiment, not promotion of another production change.
RESEARCH.md E20 ranks C1–C5 opportunities by frequency, removable work, actual
workload fit, risk and memory. The highest-ranked new experiment is typed
`price_changes` during Wire parsing, eliminating its RawValue rescan. Static
keyword hashing ranks low: Serde already emits fixed matches, and an equivalent
release match already compiled to length dispatch and safe SIMD comparisons.

**Executed, archived, not integrated.** 51,040 differential cases and 1,765,230
SBE oracle records matched. Equivalent-build 16-entry decoder loop elapsed time
fell 22.70–29.75% and 64-entry time 27.02–33.69% across three pairs. Smaller
retained deltas and snapshot controls were mixed/adverse. All-error fallback
raised malformed-tail cost about 2.4×. Pipeline regression guards failed at both
1k/s and 5k/s, including a final 5k/s processing-p99 regression of 18.60%.

No promotion or unchanged rerun follows. To revisit this candidate, first show
bounded runtime-limit parsing and error-scope preservation without speculative
full failure reparsing, then discriminate remaining pipeline regressions using
the repaired harness. The independent alternative is to profile C4 event-local
numeric preparation before choosing a small-index representation; do not stack
that change on H16. Keep the new opt-in batch-width cases in the harness.

## H17–H19 — C4 occupancy work and snapshot traversal (E21)

**H17 tested in isolation, retained as an experiment, not integrated:** skip
the side-count lookup and capacity check only when old/new occupancy agree.
Every transition still checks intermediate side/global limits before mutation.
Existing provisional quantity semantics, all observations, source validity and
snapshot construction peaks remain unchanged. Hypothesis/patches and build
identities are archived in `e21/`; production source hash remains E20's baseline.

The independent array oracle checked 9,604 four-entry, two-asset/both-side traces
across four budget configurations. Existing signed-relative/negative/overflow
conformance and exact SBE output comparisons passed. For predecoded 16-entry
nontransition batches, total measured event time improved 5.8–11.9% for Set and
7.6–11.6% for Add. However, all-transition 16-entry controls regressed in every
pair (Set 7.7–54.3%, Add 7.4–36.5%). Do not label this uniformly cheaper.

Pipeline screens: mixed failed at both 1k and 5k/s; snapshot-burst failed at 1k/s
and passed the short screen at 5k/s only. Mixed 5k pair 2 processing p99 rose
20.666→28.498µs, with slower owner and recorder tails; no unique machine-level
cause established. Allocations were unchanged. **Do not promote from the one
favorable workload/rate or retry these runs until they pass.** A controlled
hash-seed/code-layout and CPU-time diagnostic would discriminate remaining
causes better than adding another optimization on top.

**H18, next independent C4 candidate:** remember whether snapshot insertion saw
any zero, skipping the final tree-wide retain only for all-positive sides.
Keep duplicate checks, insertion order, old-book lifetime and peak limits.
Current deep-snapshot attribution gives construction only 15–16% of total array
time; the removable retain pass is a subset of that, not a 70% opportunity.
Validate empty/zero-heavy/duplicate snapshots and later mixed updates before
claiming any win. Not implemented in E21.

**H19, deferred:** bounded event-local side counts with existing HashMap fallback.
Do not add persistent epochs or assume a two-asset feed. First establish the
remaining count cost and binding distribution; fallback must migrate provisional
counts and preserve intermediate peaks. No implementation or speed claim.

## H20–H26 — full-universe run evidence and horizontal build/output research (2026-10-29)

Inputs: the 30-minute, 527-socket orb run (40,301,598 records, 7.58 GB, 516,858
desired books, `max_batch_age_us` 104,036, `max_snapshot_lateness_us` 87,946),
the existing E20 analysis of the same Lemire article, pinned external sources
(reth `966010c9`, NautilusTrader `49ad9ca7`, hftbacktest `5f3ec40b`, dbn
`a7769303`, Rust Performance Book, kernel vm/pipe docs, LWN dirty-throttling
series) and code reading. No production source changed; nothing below is a
measured win.

Load context: ~22k records/s and ~4.2 MB/s on average. Byte budget from the
schema (snapshot 36 + 16/level + symbol, level 45 + symbol, ~91-byte symbol,
~11 levels/book) estimates ~7.4 GB, consistent with the measured 7.58 GB:
periodic snapshots are ~43% of bytes, level updates ~52%. Per-message CPU is
not the live bottleneck; the observed tail is output-side.

**Tail mechanism (code-confirmed, cause of stall unmeasured).** `publish_due`
runs on every owner-loop pass (`collector/owner.rs:302`) and snapshot capture
is sliced to ≤1 ms, so the owner cannot hold a batch for 100 ms by itself.
When the free pool is empty, `publish_due` deliberately retains and keeps
filling the current batch (`recording/mod.rs:203`); its age then grows until
the writer returns a batch. A ~100 ms batch age therefore implies the writer
was stalled ~100 ms. Linux dirty-page throttling pauses writers up to 100 ms by
design (LWN 419638/415838); a slow stdout pipe consumer is the other candidate.

| Id | Hypothesis | Expected effect | Confidence | Confirm / refute |
|---|---|---|---|---|
| H20 | Stall tolerance is ~80 ms + one batch fill, not 64 MiB. 5 ms aging publishes ~20 KB batches, so 16 slots empty after ~80 ms of writer stall; the retained batch then fills 4 MiB (~1 s) and the next acquire fails closed ("output pool exhausted"). More, smaller batches (config only, e.g. 64 × 1 MiB) raise tolerance with the same 64 MiB. | Fewer fatal stops under writer stalls; no CPU change | High (mechanism), unmeasured magnitude | Throttled-destination replay: stall duration that kills the run, 16×4 MiB vs 64×1 MiB; record min free-pool per minute |
| H21 | Writer stalls come from page-cache writeback throttling (file) or the 64 KiB pipe (pipe). | Explains 104 ms tail | Medium | Per-`write_all`/flush duration histogram; `writeback:balance_dirty_pages` trace; rerun to file vs `/dev/null` vs pipe; `dirty_background_bytes=32M` operator test |
| H22 | Done (uncommitted): `[profile.profiling]` = release + `debug = "line-tables-only"`. Release is deliberately unchanged: line tables changed generated code (`__text` 3,474,604 → 3,483,436 bytes, +0.25%, aarch64-apple-darwin, Rust toolchain of this repo), so a profile of the profiling build is attribution, not release timing. | Readable perf/samply profiles | High | Confirm hot spots against plain release before acting on them |
| H23 | `maxperf` profile (`lto = "fat"`, `codegen-units = 1`), also try thin LTO. reth ships this as opt-in; rustc measured cgu=1 at −1.9% cycles; fat LTO can regress (rust#103453). | Low–medium CPU, no tail effect | Medium | Harness A/A then A/B on identical oracle; build time recorded |
| H24 | Allocator (jemalloc/mimalloc) for BTreeMap node churn on snapshot replacement. Needs a dependency → only if a Linux profile shows malloc/free ≳10% of owner self-time. Nautilus measured large wins on Windows/glibc; reth measured mimalloc v3 worse and +0.8–2.5 GiB RSS. | Unknown | Low until profiled | H22 profile first; then A/B with RSS |
| H25 | Done (uncommitted): replace the 60 s depth sort with a counting histogram; 500-case sorted nearest-rank oracle test. Sort of 515k values measured 1.3–3.5 ms (M2 standalone) plus the instrument walk. | Removes a few ms/min of owner work | High (small) | Same summary values from both methods on random inputs |
| H26 | SIMD JSON (sonic-rs/simd-json). No published small-message (100 B–10 KB) evidence; unsafe-heavy; no exactness advantage over borrowed `&str`. New parser dependency needs separate approval. | Unknown CPU, no tail effect | Low | Criterion on retained Polymarket fixtures vs current decoder |


**H27 — per-snapshot integrity rows (found while reviewing the r1 sidecar).**
Every snapshot record (r1: ~11.3M, mostly periodic) also builds a
`serde_json::Value` row on the owner thread (`recording/mod.rs:280`, nested
`binding()` with `format!`), zero-fills an 8 KiB slot, serializes and
truncates. This explains the 5.8 GB r1 sidecar (~483 B/row) next to 7.58 GB of
SBE, both written synchronously by the one writer thread. Standalone M2
replica of that code, 1M rows × 3 rounds: ~2.1 µs/row, of which Value
construction+drop ~1.47 µs, serialization ~0.52 µs, 8 KiB memset ~0.14 µs —
about 8× the ~250 ns SBE encoding of an 11-level snapshot. ~24 s owner CPU per
30 min (~1.3% of a core): not a tail cause by itself, but ~43% of disk bytes
feed the H21 writeback pressure. Lemire's "pay once" fits here: symbol,
binding and origin are fixed per binding, so a direct writer could emit
precomputed static bytes plus integers. The output must stay byte-identical to
today's serde_json (BTreeMap key order); oracle = byte comparison against the
current `json!` row on generated cases. Reducing what periodic rows contain is
a C5 contract change and needs user approval; a faster identical writer does not.

*Implemented (uncommitted, local, base 5269cf6):* snapshot rows are a typed
`SnapshotFields` serialized by a hand-ordered `Serialize` impl (alphabetical,
matching serde_json's BTreeMap), with a non-allocating binding object; other
row kinds keep `json!`. Oracle: 5,000 generated cases (escaped/control/unicode
symbols, integer boundaries, None/Some, selection objects, both adapters) are
byte-identical to the former construction kept verbatim in the test; oversized
rows fail in both. One intended difference: the former `json!` panicked for
`due_us`/`receive_age_us` above u64::MAX (unreachable: ~584,000 years of
microseconds); the typed row writes the number. Standalone M2 replica,
1M rows × 3 rounds: 2,107–2,168 → 572–574 ns/row (~3.7×), the 8 KiB zero-fill
(~140 ns) now a quarter of what remains. Same bytes on disk, so no change to
H21 writeback pressure. Debug and release: 209 passed, 0 failed, 12 ignored;
fmt and strict clippy clean. Pipeline A/B on the orb harness still pending.

**Correction after the r2 live latency run (orb, 25 min, 2026-10-29).** The
code-reading inference above — "a ~100 ms batch age implies a writer stall" —
was wrong. r2 measured S5 p99 0.23–0.29 ms and S6 at the 5 ms flush cadence in
every window, `nr_throttled_written` 0, dirty pages far below threshold. The
tail is the owner: S2 queue wait max 24–40 ms in every minute window and
130–141 ms twice; S3 single-message max 49.8 ms. H21 is refuted for this host;
H20 remains an unobserved fail-closed risk, not the tail cause. Missed: an
open batch also ages while the *owner* is busy outside message processing.

**H28 — the once-a-minute depth walk.** `Replica` is 600 bytes and each
instrument holds two inline, so `DepthSummary::of` strides ~517k × 1.3 KB
(~640 MB) to read two fields per replica, on the owner thread. Simulated on the
M2: 7.7 ms per walk once streaming, 198 ms on the first cold walk; a VM Xeon
with 4 KiB pages will be slower. Plausible for the 24–40 ms per-minute floor;
H25 removed only the sort. Unproven until timed in place. Candidate fix: count
depth during the existing periodic snapshot pass (no extra walk, but samples
spread over the minute — a documented semantic change), or slice the walk.

**Rejected after analysis:**

- *Cached encoded snapshots of unchanged books* (Lemire "pay once"). E21 measured
  encoding at ~20–25 ns/level; 5.7M snapshot levels/min ≈ 125 ms CPU/min
  (~0.2% of a core). The cache would hold ~150 MB and add invalidation risk on
  every mutation path. No precedent found beyond CME MDP3's re-emitted
  (not byte-cached) snapshot loop.
- *`Vec::swap_remove`*: no hot-path `Vec` removal exists; remaining removals are
  control-plane and order-sensitive.
- *`panic = "abort"`*: removes the panicked consumer/writer reporting via
  `join().is_err()` (`collector/mod.rs:211`, `:237`) and skips destructor
  flushing of stdout and the integrity sidecar.
- *`target-cpu=native` as default*: host-specific; at most a local experiment,
  with a portable level (e.g. x86-64-v3) if ever shipped.

Order: H21 measurement (in progress on the orb) and H20 tolerance test first,
because they address the only observed tail and a fail-closed risk; then H22 →
H23; H24/H26 only with profile evidence and approval; H25 any time.

## H28/H29 fixes (local, uncommitted, base 5269cf6)

**H28 fixed by removal.** r3 attributed the steady ~50 ms per-minute S2 floor
to `DepthSummary::of` (matched S2 max within ~1 ms in 6 of 11 windows; 92 ms
max live, ~50 ms offline). The periodic sample is removed; one sample is taken
at stop (on the first envelope after stop, or on an idle timeout after stop).
`peak_levels` remains exact for the whole run. `deeper()` is deleted with its
only caller.

**H29 confirmed and fixed.** The doubling `DesiredChunk` stalls (5→117 ms) are
`HashMap<String, BookId>` resizes rehashing every ~77-byte asset ID. Standalone
M2 replica, 550k inserts: stalls >1 ms at 14,336 / 28,672 / 57,344 / 114,688 /
229,376 / 458,752 entries = 1.3 / 1.9 / 4.1 / 11.9 / 22.9 / 53.6 ms; none with
`with_capacity(1_000_000)`. The adapter map now reserves `config.instruments`
when the adapter first appears (~2M buckets, mostly untouched virtual memory).
Rejected alternative: `books: Vec<Instrument>` growth. On the M2, 550k pushes
of 1,304-byte elements had a worst push of 0.46–1.8 ms with or without
reservation, because large reallocations remap pages (glibc `mremap` on Linux
similarly), so it is left to grow. Regression test: the map's capacity is
unchanged from the first to the bound-th identity, and the bound still rejects.

Gates: fmt, strict clippy, debug and release 210 passed / 0 failed / 12
ignored. Not yet re-measured: the offline owner-capacity test with the orb's
instrumentation (`engineering/test`, bcadca6) is the A/B.

## H30 — 1 ms publish/flush cadence (default changed, not yet measured)

r2 steady state: ~6 ms p50 end to end, of which S6 (writer received → flushed)
was 3.3 ms p50 and the rest mostly batch aging; S3 processing 6.9 µs. Defaults
`publish_age` and `flush_age` are now 1 ms (minimum the config admits). Expected
p50 ~1–2 ms; cost ~5× more publish/flush calls. Receive timestamps unchanged.

Pool left at 16 × 4 MiB. Correction to H20's earlier "64 × 1 MiB" suggestion:
`recording::channel` requires a batch to hold the largest admitted snapshot
plus its 8 KiB sidecar slot (~2.1 MB at 65,535 levels/side), so 1 MiB batches
fail at startup. Also, a shorter age shortens only the first part of the stall
tolerance (slots × age: 80 → 16 ms); the retained last batch still fills its
4 MiB before the fail-closed acquire, so total tolerance stays near one batch
fill (~1 s at r2's ~4.4 MB/s), not 16 ms.

A/B caution: arms that differ in these defaults are not comparable on batch
age, S4–S7 or recorder metrics. Compare the H28/H29 stall fixes on S2 and
owner-operation timings, or pin both arms to the same cadence.

## H31 — typed invalidation rows (done)

A session end invalidates ~1,000 books at once, each writing an
`unavailable`/`replica_unavailable` sidecar row built with `json!` plus an
owned `chars().take(128)` reason copy. These rows are now a typed
`InvalidationFields` serialized in serde_json's alphabetical key order, with
the reason truncated by borrowing (`first_chars`). Oracle: 600 generated cases
(every cause, both kinds, >128-character multi-byte/escaped reasons, integer
extremes, empty/control symbols) byte-identical to the former construction.
Not separately timed; same mechanism as H27 (~2 µs → ~0.5 µs per row
expected), i.e. ~1.5 ms off a 1,000-book `ended`. Gates: 211 passed / 0 failed
/ 12 ignored in debug and release; fmt and strict clippy clean.

Offline A/B of perf-local vs bcadca6 (550k books, 70 s, A/B/A/B, orb): running
window S2 max 126.2/109.7 → 11.1/10.3 ms; desired_chunk max 120.9/104.3 →
5.6/4.9 ms; 1,660,000 records matched per run; slow S3 messages were on-CPU work
(perf sched run-delay overlap ≤0.06 ms), so "descheduled" was refuted.
