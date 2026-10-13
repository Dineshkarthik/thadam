# Advanced processing: event-time windows, WASM operators, anomaly rules, inference

- **Status:** Draft. A design note, not an ADR. Nothing here is accepted; section 5 lists what the owner must decide.
- **Date:** 2026-10-13
- **Base:** `main` at `b6f8b27` (working tree clean when checked).
- **Scope:** product.md Phase 6 (lines 1512-1518) and the P1/P2 transform rows (lines 559-576). Design only; no code was changed.

Style: the smallest design that does the core job, with what is deferred stated explicitly. YAML below is illustrative, not a settled syntax.

## 0. Recommendation in brief

1. Build **tumbling event-time windows first**, as one new `window` step. It also covers anomaly rules (window, then the existing `filter`/`validate`) and feature extraction (window aggregates), so those need no operators of their own.
2. Make window state safe with **cursor hold-back**, not a new checkpoint file: the sink cursor never passes the oldest WAL frame an open window still needs, and a restart re-reads from there. The WAL stays the only durable state.
3. Do **WASM after windows**, and only after ADR-0017 (plugin mechanism) exists and a concrete need appears. Runtime: `wasmi`. Stateless pure functions only.
4. Do **inference last**, only with a user who has a model. Runtime: `tract`. `ort` is out under ADR-0020.

## 1. Constraints that shape everything

### 1.1 Fixed by existing decisions

- **Capture-first.** The WAL stores raw messages; each sink has its own chain that runs after the commit (ADR-0003). A chain is synchronous, pure, `!Sync`, and owned by one task (`crates/thadam-transform/src/chain.rs:87-92`). A stateful operator therefore lives inside one sink's chain, and its state is never shared between sinks.
- **Replay is a cursor rewind**, and chains use the *current* configuration (ADR-0003; ARCHITECTURE.md section 7, lines 299-306). The determinism property is **row-set equivalence**: same WAL range, same config hash, same enrichment-file hash gives the same `_event_id`, values, and flags (ARCHITECTURE.md lines 342-344).
- **At-least-once delivery**, deduplicated downstream on deterministic event IDs (ARCHITECTURE.md section 12). No design below claims exactly-once.
- **Budgets.** 1-4 cores, 256 MB-2 GB RAM, no unbounded queues (product.md lines 1237-1256). State is capped by `ChainLimits` (`limits.rs`); overflow is a counted eviction or reject, never growth.
- **Not yet built, on purpose.** Windowing, aggregation, and a user step trait are "deliberately not exposed" (`thadam-transform/src/lib.rs:68-72`; ARCHITECTURE.md line 348). ARCHITECTURE.md line 61 lists windowing, watermarks, WASM operators, and ML inference as MVP non-goals. `owner-decisions.md:104` and `product-amendments.md` A.5 moved windowed aggregation to P1 because it "needs watermark and late-data semantics". product.md section 33 says not to build a general stream-processing engine or custom ML infrastructure; everything below stays a few operators inside the chain.

### 1.2 How existing stateful steps behave on restart and replay

| Step | State | Time basis | Persisted? |
|---|---|---|---|
| dataset `dedup` | `DedupWindow`: map plus queue of event IDs, capped by `max_keys` and `max_dedup_keys` (1,000,000) (`dedup.rs:15-22`, `limits.rs:19`) | WAL ingest time (`dedup.rs:9`) | No |
| `on_change` | per-asset last-kept values and `last_kept_ns` (`steps/on_change.rs:25-36`) | arrival order; heartbeat in ingest time (`:185-213`) | No |
| `sample` | per-asset `seen` counter and `last_kept_ns` (`steps/sample.rs:16-27`) | arrival count and ingest time (`:69-78`) | No |
| `validate`, `filter`, `enrich`, `rename`, `project`, `cast`, `pose`, `joint_state` | configuration or a static table only | none | n/a |

`units` and `explode` are dataset-level options, not steps, and are stateless. Per-asset state sits in `AssetMap`, which evicts the oldest asset first-in-first-out at `max_state_keys` (default 100,000), an order that depends only on input order (`steps/state.rs:8-18`, `limits.rs:137`).

**Recovery mechanism.** No step persists state. On start, panic, or rewind, the runtime rebuilds the chain and `warm` re-feeds up to `warm_start_frames` frames before the cursor, with output discarded (`thadam-runtime/src/chain.rs:140-184, 229-260, 402, 429-436`; ARCHITECTURE.md line 572). The default is 10,000 frames (`options.rs:134`). The budget is a **frame count**, not a time span or a step's reach, and only dedup, `on_change`, and `sample` trigger it (`plan.rs:197-209`).

**Consequence (from reading the code; not tested).** Equivalence after a restart is conditional. `Chain::warm` says so itself: it holds "whenever the warm records cover each state's reach" (`chain.rs:443-445`). `sample every_n` keeps an event when `seen % n == 0`, with `seen` counted from the asset's first event, so a restart after more than 10,000 frames of history can start a different phase. `on_change` can keep one extra event. The tests warm from the full prefix only (`thadam-transform/tests/properties.rs:438-461`; `thadam-runtime/tests/warm_replay.rs`).

The one durable chain state is the Sparkplug birth store, written during processing and deliberately not on warm-up (`chain.rs:329-331`; `lib.rs:74-78`). State kept outside the WAL needs special cases for replay.

### 1.3 Requirements for any new stateful operator

- **R1** A pure function of the ordered WAL records, the config, and the hashes of any file it loads. No wall clock, no RNG, no hash-map iteration order, no threads.
- **R2** Independent of batching. The chain handles records one at a time (`thadam-transform/src/chain.rs:343-436`), but frames are grouped by clock state and read chunk (`thadam-runtime/src/chain.rs:50-52, 463-498`), so no result may depend on group boundaries.
- **R3** Bounded state, with counted eviction.
- **R4** Deterministic output IDs.
- **R5** A restart gives the same row set, with duplicates at worst. No silently partial output.

Frame-count warm-up cannot meet R5 for windows, because the reach (window plus lateness) is a time span. That drives the state design in 2.3.

## 2. Event-time windows and watermarks

### 2.1 Smallest useful operator set

One step, `kind: window`, **tumbling** only, keyed by the event's asset identity (the unit `on_change` and `sample` already key on). Aggregates are chosen to be mergeable: `count`, `sum`, `min`, `max`, `mean`, `first`, `last` first, then `stddev`, `range`, `delta`, `rms`. The time basis is explicit, with no default, as with `time.domain` (`spec.rs:294-296`):

- `time: event` uses `Times::event_time()`, which is the corrected time, or the source time only when its domain is `utc` (`thadam-core/src/time.rs:237-242`). Events with no event time are excluded and counted.
- `time: ingest` uses the WAL ingest time. Most ROS 2 sim-time or monotonic sources have no UTC event time, so this basis is needed on day one.

```yaml
transforms:
  - kind: window
    dataset: telemetry
    sinks: [clickhouse]        # chains are per sink; the existing sink selector applies
    size: 1m
    time: event                # or ingest
    allowed_lateness: 5s       # event time only
    aggregate:
      - { field: motor_temp_c, ops: [count, min, max, mean, first, last] }
  - kind: filter               # anomaly rule on the aggregate: see 4.1
    dataset: telemetry
    sinks: [clickhouse]
    where: { field: motor_temp_c_max, gt: 90.0 }
```

A record is offered to every dataset whose matcher accepts it (`chain.rs:355-359`), so a raw dataset and a windowed dataset can both read one topic by using two dataset IDs. I did not check the config layer for overlap rules.

### 2.2 Watermark and lateness policy

- **Per-asset watermark** = maximum time seen for that asset minus `allowed_lateness`. A window `[s, s+size)` emits when the watermark reaches `s+size`, that is, when a record of that asset arrives with time at or past `s+size+lateness`.
- **Per asset, not global**, because one robot with a fast clock would otherwise make every other robot's events late; the state is already per asset; and one record closes windows of one asset only, which makes emission order deterministic with no map iteration.
- **Late events** (for a window already emitted) are excluded from the aggregate and counted: a new `late` value on the `outcome` label that `count_output` already reports (`thadam-runtime/src/chain.rs:357-363`). The raw record stays in the WAL and in any raw dataset. Emitting corrections is deferred because it needs upsert-capable sinks.
- **No timers.** Windows close only on a later record of the same asset. The last window of a robot that stops stays open until it speaks again. Two options are rejected: flush on shutdown, because a restart would later re-emit the same window ID with different values; and any wall-clock idle timeout, because it breaks R1. A deterministic idle close, driven by the ingest time of other WAL records, is deferred (step S5) and is open question 3.

### 2.3 State, and checkpointing tied to WAL positions

| Option | What it is | Verdict |
|---|---|---|
| A. Warm by frame count | Reuse today's `warm` | Rejected. A frame count is not a time span, so windows can be silently partial (R5). |
| B. Checkpoint file | Save state plus a WAL position beside the cursor | Deferred. A new durable format needs epoch and config-hash binding, versioning, corruption handling, and invalidation on rewind (the birth store shows the replay special cases). |
| C. Cursor hold-back | The cursor never passes the oldest frame an open window needs; a restart re-reads from there | **Chosen for v1.** No new durable state. |

How C fits the code:

- Each open pane tracks `first_pos`, the WAL position of its first contributing record. After each group the chain reports `hold = min(first_pos)`. A new `hold` field on `ChainBatch` carries it to the driver.
- `SegmentDriver::maybe_commit` commits `min(watermark, seen_through)` today (`thadam-runtime/src/segment.rs:311-317`); it would also cap at `hold - 1`. `BatchDriver::store_cursor` commits through the same cursor API (`batch.rs:244-262`); I did not read the rest of its recovery.
- Today startup recovery moves the cursor up to the sink's recovered position `m` when `m > c0` (`segment.rs:238-246`). For a window-bearing sink it must instead take the existing `skip_through = m` branch, which is already used when quarantine lags (`:241-245`).
- After a restart the chain starts at `cursor + 1`, rebuilds open windows by normal processing, and re-emits. `trim_through` (`:75-80`) and the `skip` logic in `on_batch` (`:388-398`) drop output at or below `m`. This works if **the aggregate's `wal_pos` is the trigger record's frame**, which lies inside the batch origin that produced it. The aggregate is then trimmed exactly when it was already durable. `rewind_after` (`:350-371`) already uses this re-read-and-trim path.
- Duplicates are possible for batch sinks, whose batch IDs come from the range (ClickHouse notes in CHANGELOG.md). That is within at-least-once; deterministic IDs make them removable downstream.
- **Cost.** WAL retention grows by `(size + lateness) x ingest byte rate`, and a restart reprocesses the same span. Arithmetic, not measured: 6 minutes at 100 KB/s is 36 MB; at the 10 MB/s example in ARCHITECTURE.md section 12 it is 3.6 GB. `thadam validate` must add this term to its estimate (ARCHITECTURE.md lines 519-521). If it proves too costly, option B is the fallback.
- **Gaps.** A `Gap` (evicted, corrupt, missing) while windows are open: affected windows still emit, with `quality.status = uncertain` and a reason such as `window_gap`. This uses existing quality fields and adds no flag bit, since bit positions are permanent (`thadam-core/src/quality.rs:43`).
- **Replay of a range** (`replay.rs:1-10`) starts cold plus warm-up, not from a hold-back floor. The first window per asset at the range start is flagged `uncertain` (`window_range_start`). Equivalence is claimed only for windows entirely inside the range. Normal resume is complete because of hold-back, so the chain needs one boolean, `complete_history`, from the runtime.
- **Bounded state.** A new `max_open_windows` limit; at the cap the oldest asset's windows are evicted, counted in `state_evictions` (`dataset.rs:700-711`), and no longer hold the cursor back. Rough size, arithmetic only: about 7 words (56 B) per field per pane, so 10,000 assets x 4 fields is about 2.2 MB per pane. Nothing here is measured; a bench beside `crates/thadam-transform/benches/chain.rs` must come before any CPU or memory claim (ADR-0003 says chain CPU is unmeasured).

### 2.4 Output shape

One event per (asset, window): `window_start`, `window_end`, `count`, and `<field>_<op>` columns. The shape is interned at compile time, as `rename` and `project` already do (`steps/mod.rs:94-110`).

- **ID:** `xxh3_128` over a new tag, dataset, asset, window start, and size, using the existing `IdBuf` (`eventid.rs`).
- **Times:** ingest is the trigger's ingest time; the source time is `window_start` in the dataset's domain (open question 4).
- **Sums** accumulate in WAL arrival order in f64. Rust does not fuse multiply-add on its own, so I expect identical results on x86 and ARM; not tested.
- **Interface change.** `Step::apply` is one event in, at most one out (`steps/mod.rs:33-40`), and `finish_event` runs steps in a loop (`dataset.rs:1187-1203`). A window step needs to emit zero or more events and let each continue through the later steps. That change is the main code cost of S1.

### 2.5 Sliding windows

Deferred to S4. They reuse tumbling state as panes of width `slide`, merged across `size/slide` panes. This works only because the chosen aggregates are mergeable; median and percentiles are not, so they stay deferred.

## 3. WASM operators

### 3.1 Gate

ADR-0017 (plugin mechanism) is reserved and has not been written; `docs/design/adr/` has no 0017. The step trait is `pub(crate)` with "no user-defined step trait" (`steps/mod.rs:1-2`; ADR-0018 requirements), and product.md lines 945-947 say to investigate WASM "only after the operator/plugin abstraction is stable". WASM also does not solve windows: a guest that keeps memory between calls breaks R1 and R5, because a restart loses that memory. State therefore stays in Thadam; guests are pure.

### 3.2 Runtime choice

Facts from crates.io and upstream docs read on 2026-10-13. No Thadam-side measurements exist.

| | `wasmi` 2.0.0 | `wasmtime` 49.0.2 (36.0.17 older line) |
|---|---|---|
| License | MIT/Apache-2.0, on the allow-list (`deny.toml:42-51`) | `Apache-2.0 WITH LLVM-exception`. Not on the allow-list; today only `target-lexicon` has that exception (`deny.toml:56-58`). Needs an owner policy change. |
| MSRV | 1.86, below the declared floor 1.89 (`Cargo.toml:67`) | 1.96 for 49.x, which would raise the floor (ADR-0020 rule); 1.86 for 36.0.17 |
| Execution | Pure-Rust interpreter, `no_std` capable | Cranelift JIT, plus the Pulley interpreter (Tier 2) |
| Determinism | README: "deterministic execution", built-in fuel | Deterministic only when configured: fuel not epochs, NaN canonicalization, relaxed-SIMD off, memory and table growth bounded, no non-deterministic imports (docs: Deterministic Execution) |
| Audits | Two third-party audits, of 0.31 and 0.36-0.38 per README; 2.0.0 not covered | Documented security model; continuous fuzzing for Tier 1 |
| Linux ARM64 | Pure Rust, no JIT; should behave the same (untested) | `aarch64-unknown-linux-gnu` is Tier 2 (missing continuous fuzzing); x86_64 Linux is Tier 1 |
| Component model | Not in the README feature table | Supported |
| Size, speed | Not measured | Not measured |

**Choice: `wasmi`**, because its license and MSRV fit the existing policy, ARM64 is a first-class Thadam target (product.md lines 1243-1245), and per-record rules at edge rates do not obviously need a JIT. Revisit `wasmtime` only if guests need the component model or Thadam's own bench shows interpretation is too slow.

### 3.3 ABI

A **core-module byte ABI**, versioned, with **no imports at all** (no WASI, no clock, no random): exports `memory`, an allocator, an ABI version number, and one function taking a batch and returning a batch. Records use a small length-prefixed encoding of Thadam `Value`s against input and output shapes declared in YAML, as datasets declare fields. JSON is avoided (product.md line 372; the `f32` widening hazard in ADR-0018). The exact encoding belongs to ADR-0017. WIT and the component model are deferred: they need `wasmtime`, and the version export lets a later ABI add them without speculating now.

### 3.4 Limits and failure isolation

- Fuel per call (deterministic, so replays reject the same records), a memory cap with minimum equal to maximum and no `memory.grow`, a module size cap like `max_enrichment_bytes` (`limits.rs:37-38`), and an output record cap.
- A trap, fuel exhaustion, or ABI violation becomes a per-event reject with the raw bytes kept, through the existing `StepResult::Reject` path (`dataset.rs:1194-1198`). Host panics are caught per chain and the poison frame is quarantined after `poison_retries` (`thadam-runtime/src/chain.rs:96-116, 266-317`). A native abort inside the embed library still kills the process (ADR-0003, "single process shares fate").

### 3.5 Supply-chain posture

The module is a local file, never fetched, and its hash is recorded in lineage like enrichment files (`EnrichmentInfo`, `chain.rs:38`), so the determinism contract gains "module hash". The crate is pinned exactly, as `rskafka` is (`Cargo.toml:153`), behind an off-by-default cargo feature like `opcua`, `ros2-live`, and `iceberg` (`apps/thadam-cli/Cargo.toml:13-31`), so the default binary does not change. `thadam validate` instantiates the module and checks exports, imports, and limits. Module signing and a fuzz target for module loading are not proposed.

### 3.6 Before windows?

No: it needs an ADR that does not exist, it is P2 in product.md, it cannot hold state, and windows serve the P1 needs directly. Do it after S3 and only against a concrete need (for example vendor-specific record maths that structured config cannot express).

## 4. Anomaly rules, inference, feature extraction

### 4.1 What rules and windows already cover

| Need | Expressible with | Note |
|---|---|---|
| Fixed threshold | `validate` or `filter` today | Exists. |
| Threshold on a trend (hot for a minute) | `window` then `filter` on `<field>_max` or `_mean` | Needs only S1/S2. |
| Flat-lined or noisy sensor | `window` then `filter` on `_range` or `_stddev` | Needs S3. |
| Rate of change over a window | `delta` aggregate (`last - first`) | Needs S3. Between consecutive samples is not covered. |
| z-score of the latest value | Not expressible | The predicate cannot compare fields or do arithmetic (ADR-0018 gaps, line 150). Emit window `mean`/`stddev` and compute downstream, for example in ClickHouse. |
| Silence or missing heartbeat | Not expressible deterministically | Needs timers (2.2); S5 covers only silence observed through other records. |

So anomaly detection needs no operator beyond windows: `validate` and `filter` after a window already flag or drop aggregate rows. The one prerequisite is the emit-many step change in 2.4.

### 4.2 Feature extraction

Window aggregates are the features: `count`, `min`, `max`, `mean`, `stddev`, `range`, `delta`, `rms`, output as a dataset for ML pipelines. Spectral features (FFT) are "advanced signal processing", P2 in product.md, and are deferred; a WASM guest is the likely home.

### 4.3 Inference runtime

| | `tract-onnx` 0.23.8 | `ort` 2.0.0-rc.14 |
|---|---|---|
| License | MIT OR Apache-2.0 | MIT OR Apache-2.0 |
| MSRV | 1.91 (raises the 1.89 floor) | 1.92 |
| Native code | Pure Rust | Wraps ONNX Runtime (C++). It ships its own prebuilt ONNX Runtime by default, or links a custom build via `ORT_LIB_PATH`, or loads a `libonnxruntime.so` at runtime (`load-dynamic`). |
| Maturity | 0.x; README says depend on the `tract` crate, not the internal crates | Release candidates only (no stable on crates.io) |
| Fit with ADR-0020 | Fits: one static binary, `--locked`, no cmake or clang | Does not fit: ADR-0020 rejected `aws-lc` for the same C-toolchain reason on ARM; prebuilt native blobs are invisible to `cargo deny`; `load-dynamic` breaks the single binary |

**Choice: `tract`, if and when needed.** Open items I could not close: ONNX operator coverage for real models, binary size, and the license of the ONNX and TensorFlow `.proto` files tract copies (its README says they are outside its own license statement; `cargo deny` reads crate metadata only).

Design limits if built:

- A step that runs a user-supplied model on window-feature records and adds a score field.
- Reject models without fixed input shapes at `thadam validate`; tract has no fuel, so cost is bounded by the fixed graph, a model size cap, and a per-call budget measured on ARM.
- Model hash in lineage.
- **Determinism:** tract's SIMD kernels differ per architecture, so float outputs may differ between x86 and ARM. Row-set equivalence across architectures is **not** claimed; same-binary, same-architecture equivalence is expected but untested.
- No training, model registry, or update channel (product.md section 33, "custom ML infrastructure").
- Gate: a user with a model, and S1-S3 shipped.

## 5. Order, steps, deferrals, open questions

### 5.1 Step plan

Each step ships alone and is useful alone.

| # | Step | Useful because | Test gate |
|---|---|---|---|
| S0 | Owner answers 5.3; write a short ADR for stateful operators (hold-back, per-asset watermark) | Fixes semantics before code | Review |
| S1 | Emit-many `Step`; `hold` through `ChainBatch` and both drivers; startup-recovery branch; tumbling `window` on **ingest time** with `count/sum/min/max/mean/first/last`; deterministic IDs | Basic aggregation and downsampling (product.md P0 list; A.5), with `filter` after it | Same frames twice; restart at every WAL position equals the continuous run after trimming (extend `crash_matrix.rs` and `crash_kill9.rs`); `rewind_after`; a bench |
| S2 | `time: event`; per-asset watermark; `allowed_lateness`; `late` counter; gap and range-start flags | Out-of-order and skewed fleets | Property test with shuffled arrival within and beyond lateness |
| S3 | `stddev`, `range`, `delta`, `rms`; docs for anomaly rules and feature extraction | Covers section 4.1 and 4.2 with no new operator | Aggregate unit tests; cross-architecture f64 check |
| S4 | Sliding windows via panes | Smoother signals | Pane-merge equals brute force |
| S5 | Optional: deterministic idle close from WAL ingest time | Closes a robot's last window | Same row set across restarts |
| S6 | Gated: ADR-0017, then `kind: wasm` on `wasmi`, stateless, feature-gated | Custom logic without a fork | Trap and fuel rejects replay identically |
| S7 | Gated: `kind: infer` on `tract`, feature-gated | Local scoring | Cost bound; cross-architecture caveat documented |

Stop after S3 unless a concrete user need justifies S6 or S7.

**Status, 2026-10-13: S1, S2, S3, S4 and S5 are built (experimental); S6 and S7 are not started.** Owner decisions of 2026-10-12 are implemented as stated (cursor hold-back, per-asset window and per-asset watermark, last window open until the asset speaks or `idle_close` elapses on another asset's ingest time, aggregate event time = window start, late events dropped and counted as `outcome="late"`, WAL-gap and range-replay first-window `uncertain` quality flags). S1, S2, S3, S4 and S5 as built, and where they depart from the sections above:

- **`Step` change.** `Step::apply` is unchanged. `StepResult` gains `Emit(Vec<Event>)` (zero, one or many events, each continued through the later steps) and `Late` (increments `DatasetStats::late`, exported as `thadam_chain_events_total{outcome="late"}`), and `Step` gains `hold() -> Option<WalPos>`, `windowed() -> bool`, `on_gap()`, and `mark_range_start()`. All other steps are untouched, so their behaviour is identical.
- **The cap uses the hold at the watermark, not the latest hold (corrects 2.3).** Capping the cursor at `hold - 1` of the newest batch is not enough: a window can close in a batch the sink has not made durable yet, and a cursor past its first frame would let a restart rebuild it partially. `SegmentDriver` keeps `(last frame, hold)` for each batch the watermark has not passed, and the cursor stops at the hold of the last batch the watermark covers, minus one (or at that batch's last frame when no window was open then). Covered by a unit test of the queue and by `window_holdback.rs`.
- **Warm-up skips windows.** `Chain::warm` stops at the first `window` step; steps after it start empty. A rebuilt chain mid-run (panic) instead replays from the pre-panic hold with windows fed (`Chain::warm_open_windows`).
- **Startup recovery** takes the `skip_through = m` branch for a chain with a `window`, as 2.3 says.
- **Batch sinks refuse `window`** at pipeline build instead of getting a hold-back: `BatchDriver` re-sends by range, so a held-back cursor would re-insert every raw event in the held span. This is the one place S1 did less than the plan ("both drivers").
- **A full window state rejects, it does not evict.** At `max_state_keys` assets with open windows, events of a new asset become counted rejects with their raw bytes in quarantine, because evicting an open window would drop its aggregate or emit it twice under one id. Idle-closed asset entries with no open windows are reclaimed at the cap.
- **Event time, watermarks, and uncertainty flags (S2).** `time: ingest | event` (default `ingest`, omitted when default so config hashes do not move) and `allowed_lateness` (valid only with `time: event`, default `0s`). Under `time: event`, each asset tracks its own `max_event_time` and monotonic `watermark = max_event_time - allowed_lateness` (also advanced to `window_end` when `idle_close` closes a pane), keeps open panes in a `BTreeMap<i64, Pane>` so out-of-order events within lateness can span multiple open panes per asset, emits panes with `window_end <= watermark` in ascending order, drops events with `window_end <= watermark` as `late`, and filters events without a UTC event time. `Chain::on_gap()` marks open panes `uncertain` (`reason = "window_gap"`), and `Chain::mark_range_start()` (called by `replay` when `from > 1`) marks the first window opened per asset `uncertain` (`reason = "window_range_start"`).
- **Mergeable statistical and boundary aggregates, memory/hold-back estimate, and anomaly/feature docs (S3).** `window` gains `first`, `last`, `stddev`, `range`, `delta`, and `rms` (`f64`, null when the window saw no value, keeping the input field's unit). `first` and `last` order samples within a pane by `(point_ns, wal_pos, sample_idx)` so out-of-order arrivals under `time: event` within `allowed_lateness` still produce deterministic earliest/latest values and `delta = last - first`; `stddev` uses Welford's online update (`m2 / n`) for numerical stability on large-offset floats. `PipelineSpec::memory_estimate()` adds a `window state` component (`100,000 assets x (128 B + aggs x 64 B)` per window step, noting the `size + allowed_lateness` cursor hold-back span).
- **Sliding windows via mergeable panes (S4).** Optional `slide: <duration>` (`0 < slide <= size`, `size % slide == 0`, `size / slide <= 1024`; tumbling windows are `slide = size`) accumulates incoming events into panes of width `slide` and emits each non-empty sliding window `[w * slide, w * slide + size)` by merging its `k = size / slide` panes (`Acc::merge`, with Chan-Welford parallel merge for `mean`/`m2`) once the window end `(w + k) * slide` is reached (or on `idle_close`). A pane `p` stays in `state.panes` and holds its `first_pos` in `self.open` until all sliding windows `w <= p` that could include it have closed, and `PipelineSpec::memory_estimate()` scales the per-asset window state by `size / slide`.
- **Operational hazard and `idle_close` (S5).** Without `idle_close`, an asset that stops speaking keeps its last window open, and with it the cursor of the whole sink chain, until it speaks again; WAL retention then grows without bound, or blocks capture under `on_full: block`. S5 adds optional `idle_close: <duration>`: each open pane tracks `last_ingest_ns`, and on any incoming event whose ingest time is at least `idle_close` past another asset's `last_ingest_ns`, that silent asset's pane closes in deterministic `(first_pos, asset)` order, emits with the trigger event's WAL position and ingest time, and releases its hold on the cursor. If every asset in the dataset goes silent at once, open windows still wait for the next event (no wall-clock timers, R1).
- **The `sample every_n` phase gap is not fixed by hold-back.** Hold-back rebuilds the windows exactly, but the events a window sees after a restart are what the upstream step lets through, and `sample`, `on_change`, and dedup are still rebuilt from `warm_start_frames`. A pinned test (`thadam-transform/tests/window.rs`, `hold_back_does_not_fix_the_sample_phase_gap`) shows a window behind `sample every_n: 3` differing from the continuous run when warm-up is shorter than the history. `sample` itself was not changed.
- **Sinks write only declared columns.** The Parquet sink writes the columns a dataset declares, matched by name, so a window's output columns reach Parquet only when the dataset declares fields with those names (`window_parquet.rs`). This holds for `enrich` as well and is a sink limit, not a window one.

### 5.2 Deferred, explicitly

Checkpoint files (option B); session windows; percentiles and any non-mergeable aggregate; late-event corrections and retraction; extra key fields beyond the asset; global watermark; timers and flush-on-shutdown; cross-field and arithmetic predicates (CEL, ADR-0018); WIT and the component model; `wasmtime`; `ort`; module signing; FFT and spectral features; model training, registry, or update; stateful guests; joins and complex event processing; sharing window state between sinks; public-site claims (every feature stays "Planned" in docs until shipped, per `AGENTS.md`).

### 5.3 Open questions for the owner

1. Accept cursor hold-back for v1, including WAL retention growing by window plus lateness, with checkpoints only if measured restart cost is too high?
2. Per-asset watermark (proposed) or a global one?
3. Is a robot's last window staying open until it speaks again acceptable, or should S5 (idle close) move into S2?
4. Aggregate time convention: `window_start` as event time (proposed) or `window_end`? Is flagging first windows `uncertain` on range replay acceptable?
5. Late events: drop and count (proposed), or emit corrections, which would need upsert sinks?
6. Is a windowed dataset next to a raw dataset on the same topic (two dataset IDs) the intended way to keep both?
7. Policy: allow `Apache-2.0 WITH LLVM-exception` for `wasmtime`, or settle on `wasmi`? Accept a declared MSRV of at least 1.91 if `tract` lands?
8. Is there a concrete user, with a workload and a model, for WASM or inference? If not, stop after S3.

## 6. Not verified

- **Performance:** no benchmark was run. Chain CPU, window memory, restart cost, wasmi versus wasmtime speed, and binary-size deltas are all unmeasured; every number in this note is arithmetic or a quoted upstream fact.
- **Not run:** no build, no tests, no cargo-deny, nothing on ARM hardware.
- **Not read:** the rest of `BatchDriver` recovery (`batch.rs`), the config-layer overlap rules for datasets, the WAL retention code paths under `drop_oldest`, and ADR-0011's claims register wording.
- **Upstream claims:** versions, licenses, and MSRVs come from the crates.io API; runtime properties come from README and docs pages. I did not read the crates' source, build them, or check the dependency-tree size, license of every transitive crate, or advisories.
- **Reading, not testing:** the `sample every_n` phase shift after a long-history restart is inferred from the code and was not reproduced.
