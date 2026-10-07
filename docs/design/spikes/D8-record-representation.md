# Spike report: D8 record representation and D9 value types

- **Status:** spike report. Evidence for owner decision B6 and ADR-0001 (D8, D9). It is not an ADR and changes no decision by itself.
- **Date:** 2026-10-07
- **Code:** `spikes/d8-record/` (standalone Cargo project, own empty `[workspace]`)
- **Raw outputs:** `spikes/d8-record/results/`

## 1. Summary

**Recommendation.**

- **D8: choose B**, `Record { shape: Arc<Shape>, values: Vec<Value> }`, as already drafted in ADR-0001. Reject A (keyed). Do not pick C (shape by index).
- **D9: choose the extended value set.** The measured payoff is in the typed primitive arrays (`F64s`, and by extension `F32s`, `I32s`, `I64s`). Narrow scalars and `F32` showed no memory or CPU effect here; their value is physical-type fidelity, which this benchmark does not measure.
- **Keep `Record` opaque**, as ADR-0001 already requires. Expose a `ShapeKey` plus a positional hot path so that B can be swapped for C without touching callers. Section 8 has the details.

**Headline numbers** (Apple Silicon (arm64), single thread unless noted; medians; run-to-run noise is about 4% median and up to 17%, see section 7):

- **Memory per event (inline record plus live heap, malloc-rounded).**
  - W1 telemetry, 7 fields: A-fresh 712 B, A-interned 488 B, **B 368 B**, C 368 B.
  - W2 joint state, 3 arrays of 7 f64: minimal values 1120 B (B), **extended values 400 B (B)**, a 64% reduction.
- **Allocations per event at construction.** W1: A-fresh 9, A-interned 2, B 2, C 2. W2: 7, 4, 4, 4.
- **Transform pass** (rename, unit conversion, filter), ns per event on W1: A-fresh 126, A-interned 44, **B 11 to 13**, C 12 to 13. On W2 with extended values: 93, 38, **21**, 21.
- **Field lookup:** 2.6 to 5.9 ns by name against 0.3 to 0.6 ns by cached index, for 3 to 7 fields.
- **Arrow RecordBatch conversion** (10,000 events), ns per event on W1: A 40 to 47, **B and C 15 to 18**. About 4% of the derived parse-to-Arrow path (well under the 25% ADR-0001 revisit trigger).
- **B against C:** indistinguishable single-threaded (ratios 0.8 to 1.15 across all operations and both runs, inside the noise).
- **Cross-thread (8 threads).**
  - With private per-chain shapes, which is the planned design, B matches C within noise on 3 of 4 workload and value-set combinations (about 22 M events/s each on W1).
  - With one shape shared by all threads, B reaches about 54 to 63% of C's throughput.
  - Cloning the same events from many threads is B's weak spot: on W1 and W2 / ext its throughput stays flat at about 20 M clone+drop/s regardless of thread count, against 120 to 150 M/s for C.
  - A with interned names (one refcount per field name) is the worst representation under sharing: 3.4 to 4.1 M events/s in the shared ring at 8 threads, below its own single-thread rate (6.2 to 8.7), against 11 to 13 for B and 18 to 23 for C.

## 2. Environment

| Item | Value |
|---|---|
| Host | Apple Silicon (arm64), 18 cores (6 "Super" + 12 "Performance"), 48 GB RAM |
| OS | macOS, arm64 |
| Toolchain | rustc 1.99.0 (b940084d7 2026-09-28), LLVM 23.1.1, `aarch64-apple-darwin`; picked up from the repository's root `rust-toolchain.toml` |
| Profile | `release`: `lto = "thin"`, `codegen-units = 1`, `debug = "line-tables-only"` (mirrors the plan's release profile) |
| Crates | `criterion` 0.8.2, `arrow-array` / `arrow-schema` 60.0.0, `bytes` 1.12.1, `serde_json` 1.0.151 |
| Allocator | macOS system `malloc` for every run. The counting allocator wraps it and is installed only in `alloc_report`. |
| Machine load | Not idle: other agents and background processes were running. 1-minute load average around each run: alloc_report about 9.5 (read at 13:05, just after it ran), criterion run 1 6.5 at start and 4.4 at end, xthread 2.6 at start and 8.1 at end, criterion run 2 8.5 at start and 5.9 at end. `criterion_raw*.log` and `xthread.md` carry the before/after `uptime` lines. |

The host is a development laptop, not an edge target. **Relative numbers are what matter.** Absolute nanoseconds, atomic-contention behaviour, and `malloc` behaviour will differ on Linux x86_64 and on Cortex-A class ARM (section 7).

## 3. What was compared

### Representations (decision D8)

| Name | Type | Notes |
|---|---|---|
| **A-fresh** | `Vec<(Arc<str>, Value)>` | One `Arc<str>` allocation per field per event (no interning). Original design as written in the architecture review. |
| **A-interned** | `Vec<(Arc<str>, Value)>` | Field names are pre-interned `Arc<str>`s from the dataset declaration and cloned per event: no allocation, one atomic increment per field. This is **A in its best case**. A hash-based interner fed by untrusted keys would cost more and was not measured. |
| **B** | `Record { shape: Arc<Shape>, values: Vec<Value> }` | The shape holds names and declared types once. Transforms cache resolved indexes keyed by `Arc::ptr_eq` on the shape. |
| **C** | `Record { shape: ShapeId(u32), values: Vec<Value> }` | A per-chain `Vec<Shape>` registry. Transforms cache resolved indexes in a `Vec` indexed by `ShapeId`. No atomics in the record. |

All four decode the **same declared fields** with the **same** `Val::from_json`, so the only difference between them is how field names are carried. `tests/equivalence.rs` asserts that all representations produce identical transformed events and identical Arrow batches for both workloads (the filter drops about 30% of events, so the test is not vacuous).

### Value sets (decision D9)

| Name | Variants used | Size |
|---|---|---|
| **min** | `Null`, `Bool`, `I64`, `F64`, `Str(Utf8Bytes)`, `List(Vec<Value>)`. Narrow ints and f32 widen to `I64` / `F64`; arrays become `List` of `F64`. | 40 B |
| **ext** | adds `U8`, `U32`, `F32`, `F64s(Arc<[f64]>)`; declared types pick the variant. | 40 B |

`Str` is `Utf8Bytes(Bytes)` exactly as in the implementation plan, so both sets have the same string cost. The extended set here omits variants no workload constructs (`I8`, `I16`, `U16`, `U64`, `Bytes`, `Ts`, `F32s`, `I32s`, `I64s`, nested `Record`). None is wider than the existing 32-byte payloads, so `size_of::<Value>()` would not change.

### Workloads

Each workload is 1024 distinct, deterministic events (fixed-seed xorshift) cycled as needed.

| Workload | Fields (declared type) | Transform pass |
|---|---|---|
| **W1 telemetry** | `ts` i64, `seq` u32, `temp_f` f32, `volt_mv` f32, `curr_ma` f32, `soc` u8 (six scalars), `robot` string. 7 fields. | rename 4 fields; convert degF to Cel, mV to V, mA to A; drop events with temperature above 33 Cel (about 27% dropped) |
| **W2 joint state** | `position`, `velocity`, `effort`: each an array of 7 f64. 3 fields. | rename 3 fields; convert rad to deg on 2 arrays; drop events where any effort element exceeds 36 (about 29% dropped) |

## 4. Method

- **Construction input.** Already-parsed `serde_json::Value` objects. Parsing is excluded, and shown separately as a reference row. Strings are copied (`Bytes::copy_from_slice`) in every variant; real decode would slice the WAL chunk without copying.
- **Single-thread timing** (`benches/d8.rs`, criterion 0.8.2, system allocator, no counting). 300 ms warm-up, 1.5 s measurement, 30 samples. Each iteration processes 1024 events (10,000 for Arrow). Tables report the **median ns per event** with the half-width of the median's 95% confidence interval.
  - `construct`: build 1024 records; the output is dropped outside the timer.
  - `lookup`: one field (the last numeric field), and every field, **by name** against **by cached index**. The cached path includes the shape-identity check a real step would do (`Arc::ptr_eq` for B, integer compare for C).
  - `transform`: setup (fresh, uniquely owned events) is untimed; only the transform is timed.
  - `clone`: clone of 1024 records (includes the allocation); the drop is outside the timer.
  - `drop`: drop of 1024 uniquely owned records; setup is untimed.
  - `arrow`: convert 10,000 records to a `RecordBatch` with `arrow-array` 60 builders (pre-sized to the row count) against the declared schema.
- **Allocation and memory** (`alloc_report`): a counting global allocator over the system allocator, single-threaded, N = 10,000 events. It counts allocation calls and requested bytes. On macOS it also records `malloc_size` of each live block, which includes size-class rounding. "Total B/event" is `size_of::<Record>()` plus live usable heap bytes per event.
- **Cross-thread** (`xthread`, plain system allocator, `std::thread` plus `std::sync::mpsc`). 7 repetitions after one warm-up; cells show the median.
  - **Ring:** T threads. Every round, a thread builds a batch of 256 events, sends it to the next thread, receives a batch from the previous thread, runs the transform pass on it, and drops it. So every event is created on one thread and transformed and freed on another. Two context modes:
    - `shared`: every thread uses a clone of one context, so shape or name `Arc`s are shared by all threads (worst case for refcount contention).
    - `private`: every thread has its own context (a per-chain registry, as planned).
  - **Storm:** T threads repeatedly clone and drop records from one shared, read-only set of 1024 records. This models fanning the same events out to several consumers.
  - T is 1, 2, 4, 8. Throughput is the total across threads. Efficiency is `throughput(T) / (T x throughput(1))`.
- **Derived sums.** The tables label any sum of independent medians as derived; it is not a separately measured pipeline.

## 5. Results

### 5.1 Allocations and bytes per event

#### Value sizes (bytes, inline)

| type | size_of |
|---|---|
| bytes::Bytes | 32 |
| Arc<str> | 16 |
| Arc<[f64]> | 16 |
| Vec<_> | 24 |
| min::Value (Str = Utf8Bytes(Bytes), as in plan) | 40 |
| ext::Value (Str = Utf8Bytes(Bytes), as in plan) | 40 |
| compact alt (Str = Arc<str>, List = Box<[_]>), size only | 24 |
| Record A (Vec<(Arc<str>, ext::Value)>) | 24 |
| Record B (Arc<Shape> + Vec) | 32 |
| Record C (u32 + Vec) | 32 |

#### Allocation and memory per event (N = 10000, single thread)

Columns: `live B req` = requested bytes still live after construction; `live B usable` = malloc_size of those blocks (includes size-class rounding); `total B/ev` = sizeof(Rec) + live B usable; `xform allocs` = uniquely owned events, `xform(shared)` = events whose arrays are shared with a clone.

| wl | values | repr | sizeof(Rec) | build allocs/ev | build B req/ev | live B req/ev | live B usable/ev | total B/ev | clone allocs/ev | xform allocs/ev | xform(shared) allocs/ev | kept | arrow allocs/row | arrow B req/row |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| w1 | min | A-fresh | 24 | 9.00 | 568 | 568 | 688 | 712 | 1.00 | 4.00 | 4.00 | 73.0% | 0.01 | 45 |
| w1 | min | A-interned | 24 | 2.00 | 400 | 400 | 464 | 488 | 1.00 | 0.00 | 0.00 | 73.0% | 0.01 | 45 |
| w1 | min | B-shape-arc | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.0% | 0.01 | 45 |
| w1 | min | C-shape-id | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.0% | 0.01 | 45 |
| w1 | ext | A-fresh | 24 | 9.00 | 568 | 568 | 688 | 712 | 1.00 | 4.00 | 4.00 | 73.4% | 0.01 | 45 |
| w1 | ext | A-interned | 24 | 2.00 | 400 | 400 | 464 | 488 | 1.00 | 0.00 | 0.00 | 73.4% | 0.01 | 45 |
| w1 | ext | B-shape-arc | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.4% | 0.01 | 45 |
| w1 | ext | C-shape-id | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.4% | 0.01 | 45 |
| w2 | min | A-fresh | 24 | 7.00 | 1080 | 1080 | 1248 | 1272 | 4.00 | 3.00 | 3.00 | 71.0% | 0.00 | 180 |
| w2 | min | A-interned | 24 | 4.00 | 1008 | 1008 | 1152 | 1176 | 4.00 | 0.00 | 0.00 | 71.0% | 0.00 | 180 |
| w2 | min | B-shape-arc | 32 | 4.00 | 960 | 960 | 1088 | 1120 | 4.00 | 0.00 | 0.00 | 71.0% | 0.00 | 180 |
| w2 | min | C-shape-id | 32 | 4.00 | 960 | 960 | 1088 | 1120 | 4.00 | 0.00 | 0.00 | 71.0% | 0.00 | 180 |
| w2 | ext | A-fresh | 24 | 7.00 | 456 | 456 | 528 | 552 | 1.00 | 3.00 | 5.00 | 71.0% | 0.00 | 180 |
| w2 | ext | A-interned | 24 | 4.00 | 384 | 384 | 432 | 456 | 1.00 | 0.00 | 2.00 | 71.0% | 0.00 | 180 |
| w2 | ext | B-shape-arc | 32 | 4.00 | 336 | 336 | 368 | 400 | 1.00 | 0.00 | 2.00 | 71.0% | 0.00 | 180 |
| w2 | ext | C-shape-id | 32 | 4.00 | 336 | 336 | 368 | 400 | 1.00 | 0.00 | 2.00 | 71.0% | 0.00 | 180 |

Reading the table:

- **W1.** B and C hold 7 values at 40 B plus the 8-byte string: 288 B requested, 336 B after malloc rounding, 368 B with the inline 32-byte record. A-interned adds 16 B per field name (the `Arc<str>` in each tuple). A-fresh adds an allocation per field, which also costs the rounding overhead.
- **W1 min against ext.** Identical (288 B requested, 2 allocations): a narrow scalar still occupies a 40-byte `Value` slot.
- **W2.** The minimal set stores 21 `F64` values as 21 `Value`s of 40 B. The extended set stores three `Arc<[f64]>`. B drops from 1120 B to 400 B per event, and clone drops from 4 allocations to 1.
- **`xform(shared)`.** When the arrays are shared with another live copy (refcount above 1), `Arc<[f64]>` cannot convert in place and the transform copies each converted array (2 extra allocations per W2 event). The normal pipeline case moves events and keeps the in-place path (`xform allocs` column).
- **Arrow.** The conversion path allocates almost nothing per row once the builders are pre-sized, identically for all representations.

### 5.2 Single-thread timing (criterion, run 1)

Median ns per element, with the half-width of the median's 95% CI after the plus-minus sign. Elements are events, except `lookup_all` where they are individual lookups.

#### Reference: serde_json::from_slice -> Value (parse only, no record construction)

| wl | ns/event |
|---|---|
| w1 | 250.3 ±2.3 |
| w2 | 723.9 ±26.3 |

#### Construction from parsed serde_json::Value (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 161.6 ±1.9 | 100.6 ±2.1 | 96.9 ±1.6 | 101.0 ±2.5 |
| w1 | ext | 161.2 ±2.1 | 102.3 ±2.0 | 109.1 ±2.3 | 104.7 ±2.5 |
| w2 | min | 90.7 ±0.5 | 62.6 ±1.1 | 72.4 ±4.2 | 71.0 ±2.2 |
| w2 | ext | 88.1 ±1.3 | 57.5 ±0.4 | 57.8 ±0.5 | 57.2 ±1.9 |

#### Field lookup: one field (last numeric field) (ns/lookup)

| wl | values | A-fresh by-name | A-interned by-name | B by-name | B cached-idx | C by-name | C cached-idx |
|---|---|---|---|---|---|---|---|
| w1 | min | 5.5 ±0.1 | 5.7 ±0.1 | 5.8 ±0.1 | 0.5 ±0.0 | 5.7 ±0.1 | 0.5 ±0.0 |
| w1 | ext | 5.9 ±0.1 | 5.6 ±0.1 | 5.8 ±0.1 | 0.6 ±0.0 | 5.9 ±0.1 | 0.5 ±0.0 |
| w2 | min | 2.8 ±0.0 | 2.8 ±0.0 | 2.6 ±0.0 | 0.6 ±0.0 | 2.7 ±0.0 | 0.5 ±0.0 |
| w2 | ext | 2.8 ±0.1 | 2.8 ±0.0 | 2.6 ±0.0 | 0.5 ±0.0 | 2.7 ±0.0 | 0.5 ±0.0 |

#### Field lookup: every field of the record (ns/lookup, W1: 7 fields, W2: 3 fields)

| wl | values | A-fresh by-name | A-interned by-name | B by-name | B cached-idx | C by-name | C cached-idx |
|---|---|---|---|---|---|---|---|
| w1 | min | 4.3 ±0.1 | 4.3 ±0.1 | 3.7 ±0.0 | 0.3 ±0.0 | 4.2 ±0.1 | 0.3 ±0.0 |
| w1 | ext | 4.1 ±0.1 | 4.1 ±0.1 | 4.2 ±0.1 | 0.3 ±0.0 | 4.7 ±0.1 | 0.3 ±0.0 |
| w2 | min | 2.9 ±0.1 | 2.8 ±0.0 | 2.9 ±0.0 | 0.4 ±0.0 | 2.7 ±0.0 | 0.3 ±0.0 |
| w2 | ext | 2.9 ±0.0 | 2.9 ±0.0 | 2.9 ±0.1 | 0.3 ±0.0 | 2.8 ±0.0 | 0.3 ±0.0 |

#### Transform pass: rename + unit conversion + filter (ns/event, uniquely owned input)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 126.1 ±1.9 | 43.8 ±0.7 | 11.2 ±0.0 | 11.6 ±0.1 |
| w1 | ext | 125.9 ±2.1 | 45.3 ±0.7 | 13.0 ±0.3 | 12.9 ±0.1 |
| w2 | min | 115.6 ±3.8 | 55.9 ±0.3 | 41.1 ±0.7 | 40.6 ±0.4 |
| w2 | ext | 93.2 ±0.8 | 38.0 ±0.4 | 21.0 ±0.2 | 20.5 ±0.1 |

#### Clone (ns/event; includes allocation, drop is outside the timer)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 19.7 ±0.2 | 19.5 ±0.2 | 14.6 ±0.1 | 14.8 ±0.1 |
| w1 | ext | 19.9 ±0.2 | 19.6 ±0.3 | 15.8 ±0.2 | 15.4 ±0.3 |
| w2 | min | 60.3 ±1.2 | 60.6 ±0.8 | 63.7 ±1.4 | 69.5 ±2.3 |
| w2 | ext | 12.3 ±0.1 | 12.1 ±0.1 | 11.2 ±0.1 | 10.9 ±0.1 |

#### Drop of uniquely owned events (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 77.3 ±1.3 | 20.1 ±0.1 | 18.6 ±0.1 | 18.1 ±0.4 |
| w1 | ext | 76.8 ±0.5 | 20.4 ±0.1 | 19.1 ±0.4 | 17.8 ±0.2 |
| w2 | min | 76.2 ±0.8 | 58.6 ±3.7 | 54.3 ±1.8 | 49.1 ±0.8 |
| w2 | ext | 54.5 ±0.7 | 32.1 ±0.1 | 30.1 ±0.1 | 30.4 ±0.2 |

#### Conversion of 10,000 events to an Arrow RecordBatch (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 39.8 ±0.4 | 40.7 ±0.6 | 16.3 ±0.2 | 15.2 ±0.0 |
| w1 | ext | 40.4 ±0.6 | 46.7 ±0.9 | 17.4 ±0.5 | 17.5 ±0.4 |
| w2 | min | 31.5 ±1.1 | 38.2 ±1.1 | 27.0 ±0.6 | 33.3 ±1.2 |
| w2 | ext | 26.1 ±0.3 | 26.2 ±0.3 | 18.8 ±0.2 | 18.6 ±0.4 |

#### Derived: construct + transform + arrow + drop (ns/event)

Sum of the independent medians above. It is an indicative per-event total for the decode-to-Arrow path, not a separately measured pipeline: it ignores that the filter removes ~30% of events before Arrow conversion and that real decode starts from WAL bytes rather than a parsed serde_json::Value.

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 405 | 205 | 143 | 146 |
| w1 | ext | 404 | 215 | 159 | 153 |
| w2 | min | 314 | 215 | 195 | 194 |
| w2 | ext | 262 | 154 | 128 | 127 |

Run 2 reproduced these tables within the noise described in section 7 (`results/criterion_summary_run2.md`).

Ratios used in the text, as run 1 / run 2 (from the same data):

| Comparison | W1 | W2 |
|---|---|---|
| Transform, A-interned / B | 3.5 to 3.9 / 3.6 to 3.7 | 1.4 to 1.8 / 1.4 to 1.8 |
| Transform, A-fresh / B | 9.7 to 11.3 / 10.3 to 10.6 | 2.8 to 4.4 / 2.8 to 4.2 |
| Arrow conversion, A / B | 2.3 to 2.7 / 2.4 to 2.6 | 1.2 to 1.4 / 1.2 to 1.4 |
| Construction, A-fresh / B | 1.5 to 1.7 / 1.6 | 1.3 to 1.5 / 1.3 to 1.4 |
| Construction, A-interned / B | 0.94 to 1.04 / 1.0 | 0.86 to 0.99 / 0.94 to 1.05 |
| Drop, A-fresh / B | 4.0 to 4.2 / 4.1 to 4.3 | 1.4 to 1.8 / 1.6 to 1.7 |
| B / C (all operations) | 0.96 to 1.07 / 0.97 to 1.06 | 0.81 to 1.11 / 0.94 to 1.15 |
| W2 min / ext, B: clone | n/a | 5.7 / 4.7 |
| W2 min / ext, B: transform | n/a | 2.0 / 1.9 |
| W2 min / ext, B: drop | n/a | 1.8 / 1.5 |
| W2 min / ext, B: Arrow | n/a | 1.4 / 1.4 |

### 5.3 Cross-thread behaviour

#### Scenario ring: build on thread i, transform + drop on thread i+1 (M events/s total)

| wl | values | repr | ctx | T=1 | T=2 | T=4 | T=8 |
|---|---|---|---|---|---|---|---|
| w1 | min | A-fresh | shared | 2.94 (100%) | 2.77 (47%) | 4.50 (38%) | 6.41 (27%) |
| w1 | min | A-fresh | private | 2.99 (100%) | 4.59 (77%) | 4.76 (40%) | 6.54 (27%) |
| w1 | min | A-interned | shared | 6.52 (100%) | 3.06 (23%) | 3.55 (14%) | 3.43 (7%) |
| w1 | min | A-interned | private | 6.41 (100%) | 5.22 (41%) | 10.04 (39%) | 13.23 (26%) |
| w1 | min | B-shape-arc | shared | 8.32 (100%) | 7.88 (47%) | 11.90 (36%) | 13.19 (20%) |
| w1 | min | B-shape-arc | private | 8.18 (100%) | 9.77 (60%) | 16.48 (50%) | 22.71 (35%) |
| w1 | min | C-shape-id | shared | 8.38 (100%) | 10.64 (63%) | 19.12 (57%) | 22.55 (34%) |
| w1 | min | C-shape-id | private | 8.23 (100%) | 10.41 (63%) | 18.59 (56%) | 20.12 (31%) |
| w1 | ext | A-fresh | shared | 2.74 (100%) | 2.25 (41%) | 3.82 (35%) | 9.85 (45%) |
| w1 | ext | A-fresh | private | 2.80 (100%) | 3.97 (71%) | 7.01 (63%) | 9.87 (44%) |
| w1 | ext | A-interned | shared | 6.24 (100%) | 3.09 (25%) | 3.52 (14%) | 3.49 (7%) |
| w1 | ext | A-interned | private | 6.26 (100%) | 5.16 (41%) | 10.50 (42%) | 15.13 (30%) |
| w1 | ext | B-shape-arc | shared | 7.87 (100%) | 7.63 (48%) | 7.60 (24%) | 11.62 (18%) |
| w1 | ext | B-shape-arc | private | 8.12 (100%) | 10.46 (64%) | 17.08 (53%) | 21.69 (33%) |
| w1 | ext | C-shape-id | shared | 7.99 (100%) | 10.88 (68%) | 18.67 (58%) | 21.37 (33%) |
| w1 | ext | C-shape-id | private | 7.99 (100%) | 11.08 (69%) | 18.84 (59%) | 22.00 (34%) |
| w2 | min | A-fresh | shared | 3.94 (100%) | 4.67 (59%) | 7.83 (50%) | 10.75 (34%) |
| w2 | min | A-fresh | private | 3.91 (100%) | 5.55 (71%) | 8.47 (54%) | 9.89 (32%) |
| w2 | min | A-interned | shared | 6.58 (100%) | 3.92 (30%) | 4.01 (15%) | 4.05 (8%) |
| w2 | min | A-interned | private | 6.49 (100%) | 9.33 (72%) | 10.96 (42%) | 14.47 (28%) |
| w2 | min | B-shape-arc | shared | 7.28 (100%) | 7.37 (51%) | 11.38 (39%) | 12.24 (21%) |
| w2 | min | B-shape-arc | private | 7.31 (100%) | 10.71 (73%) | 17.96 (61%) | 18.42 (31%) |
| w2 | min | C-shape-id | shared | 7.34 (100%) | 7.10 (48%) | 17.98 (61%) | 19.33 (33%) |
| w2 | min | C-shape-id | private | 7.25 (100%) | 11.03 (76%) | 18.66 (64%) | 22.69 (39%) |
| w2 | ext | A-fresh | shared | 4.61 (100%) | 6.49 (70%) | 7.72 (42%) | 11.07 (30%) |
| w2 | ext | A-fresh | private | 4.66 (100%) | 5.32 (57%) | 7.89 (42%) | 11.30 (30%) |
| w2 | ext | A-interned | shared | 8.66 (100%) | 3.86 (22%) | 3.89 (11%) | 3.67 (5%) |
| w2 | ext | A-interned | private | 8.67 (100%) | 8.24 (48%) | 8.79 (25%) | 11.32 (16%) |
| w2 | ext | B-shape-arc | shared | 9.83 (100%) | 7.56 (38%) | 9.15 (23%) | 11.00 (14%) |
| w2 | ext | B-shape-arc | private | 10.38 (100%) | 8.37 (40%) | 11.46 (28%) | 16.79 (20%) |
| w2 | ext | C-shape-id | shared | 10.29 (100%) | 8.40 (41%) | 11.61 (28%) | 17.77 (22%) |
| w2 | ext | C-shape-id | private | 10.19 (100%) | 8.58 (42%) | 11.70 (29%) | 16.76 (21%) |

#### Scenario storm: clone + drop from a shared record set (M clone+drop/s total)

| wl | values | repr | T=1 | T=2 | T=4 | T=8 |
|---|---|---|---|---|---|---|
| w1 | min | A-fresh | 26.76 (100%) | 45.15 (84%) | 79.61 (74%) | 89.61 (42%) |
| w1 | min | A-interned | 26.73 (100%) | 7.04 (13%) | 10.47 (10%) | 10.43 (5%) |
| w1 | min | B-shape-arc | 35.28 (100%) | 21.26 (30%) | 20.83 (15%) | 20.80 (7%) |
| w1 | min | C-shape-id | 36.52 (100%) | 68.46 (94%) | 105.07 (72%) | 145.80 (50%) |
| w1 | ext | A-fresh | 27.85 (100%) | 45.39 (81%) | 74.32 (67%) | 85.76 (38%) |
| w1 | ext | A-interned | 27.49 (100%) | 6.87 (12%) | 10.48 (10%) | 10.49 (5%) |
| w1 | ext | B-shape-arc | 34.89 (100%) | 21.41 (31%) | 20.62 (15%) | 21.68 (8%) |
| w1 | ext | C-shape-id | 37.49 (100%) | 67.07 (89%) | 116.45 (78%) | 151.19 (50%) |
| w2 | min | A-fresh | 9.37 (100%) | 17.90 (95%) | 32.35 (86%) | 46.07 (61%) |
| w2 | min | A-interned | 9.33 (100%) | 9.04 (48%) | 7.51 (20%) | 7.55 (10%) |
| w2 | min | B-shape-arc | 9.13 (100%) | 7.38 (40%) | 19.01 (52%) | 16.36 (22%) |
| w2 | min | C-shape-id | 9.30 (100%) | 18.05 (97%) | 30.83 (83%) | 46.62 (63%) |
| w2 | ext | A-fresh | 39.37 (100%) | 54.94 (70%) | 84.27 (54%) | 93.45 (30%) |
| w2 | ext | A-interned | 36.19 (100%) | 11.41 (16%) | 10.20 (7%) | 10.30 (4%) |
| w2 | ext | B-shape-arc | 41.56 (100%) | 19.53 (24%) | 20.46 (12%) | 20.18 (6%) |
| w2 | ext | C-shape-id | 40.92 (100%) | 64.99 (79%) | 134.56 (82%) | 122.72 (37%) |

Cells are the median over 7 repetitions in M events/s (ring) or M clone+drop/s (storm), total across all threads, with the parallel efficiency in parentheses. Min-max ranges are in `results/xthread.md`.

Reading the tables:

- **Scaling is limited for every representation**, including C (21 to 39% efficiency at 8 threads in the ring). The system allocator, the memory system, and background load cap scaling. Read the **C rows as the ceiling** for this harness and judge the others against them, not against ideal scaling.
- **Noise at 8 threads is large.** C's `shared` and `private` ring rows run identical code, yet differ by up to 17% (W2 / min: 19.3 against 22.7 M/s).
- **Ring, private contexts (the planned design).** B is within the noise of C in 3 of 4 workload and value-set combinations at 8 threads. W2 / min is the exception: B 18.4 against C 22.7 M/s, a 19% gap, which is larger than the 17% spread between C's own two identical rows. A-interned reaches 11 to 15 M/s.
- **Ring, shared context.** B falls to 11.0 to 13.2 M/s (0.54 to 0.63 of C). A-interned collapses to 3.4 to 4.1 M/s, which is **below its own single-thread rate** (6.2 to 8.7 M/s): it contends on one refcount per field name. C is unaffected, as expected, because it has no atomics.
- **Storm.** B's total throughput stays near 19 to 22 M/s from 2 threads upward on W1 and W2 / ext, because every clone and drop touches the same shape refcount. At 8 threads B reaches 0.49 to 0.62 of its own single-thread throughput on those three combinations (W1 / min, W1 / ext, W2 / ext). C reaches 122 to 151 M/s (about 6 to 7 times B) on W1 and on W2 / ext. A-interned is flat at about 10 M/s (7.6 on W2 / min), limited by its hot name refcounts. A-fresh scales (86 to 93 M/s on W1 and W2 / ext) because its name `Arc`s are distinct per event. W2 / min is allocation-bound for all representations (each clone makes 4 allocations), so refcounts are less visible there (C 46.6, B 16.4, A-fresh 46.1 M/s).

## 6. Interpretation

1. **A fails on its own terms, even in its best case.**
   - Memory: +33% per W1 event for A-interned (488 against 368 B), and 1.9 times for A-fresh (712 B, with 4.5 times the allocations).
   - Transform: A-interned is 3.5 to 3.9 times slower than B on W1 (1.4 to 1.8 times on W2), because each rename is a name scan plus two atomic operations, and each unit conversion and filter is another string-compare scan. A-fresh is about 10 times slower on W1 (it also allocates per rename).
   - Arrow conversion: 2.3 to 2.7 times slower on W1, because it does a by-name scan per field per row.
   - Sharing: the refcounts inside every record turn a read-only fan-out into a contention point (7 hot refcounts for W1).
   - The ADR-0001 revisit condition ("shape-shared records are not materially better than keyed records") is **not met**: B is materially better than the keyed design, mostly on transforms and Arrow, not on construction.
2. **Construction is not where B wins.** A-interned and B construct in the same time, because both are dominated by `serde_json::Map` lookups and value materialisation. A-fresh is 1.3 to 1.7 times slower than B because of the per-field name allocation.
3. **B against C is a wash single-threaded.** Every B/C ratio sits between 0.81 and 1.15 over both runs, inside the run-to-run noise (median 3.9%, p90 12.2%, max 17.5%). C wins only when many threads clone or hold records that share one shape.
   - The planned design avoids that case: events are moved rather than cloned (ownership rules in the implementation plan, section 3), each chain owns its own shape registry (so shape `Arc`s are touched by at most the producer thread and its immediate consumer), and `Chain` is `!Sync`.
   - Under exactly those conditions the **ring private** rows show B at parity with C on 3 of 4 combinations (W2 / min is 19% lower; see section 5.3).
4. **C costs API ergonomics that the benchmark cannot show.** A `ShapeId` record is meaningless without its registry. Every accessor that needs a name (`get`, `iter`, `Debug`, JSON or NDJSON output, the Arrow encoder in `thadam-arrow`, test assertions) needs a registry parameter, and handing a record to the wrong chain's registry gives silently wrong field names. In the ring `private` rows that mistake is invisible, because shape ids happen to coincide across contexts. B is self-describing: `rec.get("temperature")` works with `&self` alone.
5. **D9: the typed arrays carry the payoff.**
   - W2, B, minimal against extended: 1120 B against 400 B per event; clone 4.7 to 5.7 times faster; transform about 2 times faster; drop 1.5 to 1.8 times faster; Arrow conversion 1.4 times faster.
   - W1, with narrow scalars and f32: no difference in memory or time (identical bytes, timings within noise).
   - Narrow scalars still earn their place for fidelity at the typed-source boundary (CDR, Parquet physical types, `u64` above `i64::MAX`, exact `f32`), but that is a correctness argument, not a performance one. With declared schemas, the min and ext sets produced identical Arrow batches in `tests/equivalence.rs`.
6. **`Value` is 40 bytes, and that is now the next-largest lever on W1.** `Utf8Bytes(Bytes)` makes `Bytes` (32 B) the widest payload. A variant with `Str` held behind a thin pointer, and `List` as `Box<[Value]>`, is 24 B by `size_of` (a layout check only, not benchmarked). On W1 that would save about 112 B per event (7 fields x 16 B), roughly the same as the whole B-against-A-interned saving (120 B). The trade-off is the zero-copy WAL slice that motivates `Utf8Bytes`. This belongs in ADR-0001 as a follow-up measurement, not a blocker.
7. **The record representation is not the dominant CPU cost of decode.** JSON parsing alone is 250 ns (W1) and 724 ns (W2) per event here, against a derived construct + transform + Arrow + drop subtotal of 159 ns (W1, B, ext) and 128 ns (W2). Choosing B over best-case A (A-interned) lowers that subtotal by 9 to 30% depending on workload and value set (W2 / min is the 9% end), and over A-fresh by 1.6 to 2.8 times. The real decode path reads WAL bytes directly into declared shapes and skips the intermediate `serde_json::Value`, so these shares will move; re-measure with the real decoder (PR8).
8. **Arrow conversion share (ADR-0001 revisit trigger).** 17.4 ns per event on W1 / ext / B is about 4% of parse + construct + transform + Arrow + drop (408 ns, derived). On W2 it is 18.8 of about 850 ns (2%). Both are far under the 25% trigger, which supports keeping Arrow at the sink boundary. This run did not benchmark Arrow-throughout, only the conversion cost; ADR-0001 lists it as a candidate and it remains unmeasured.

## 7. Limitations

- **Platform.** Apple Silicon (arm64) on macOS with the system allocator. Not Linux, not x86_64, not a Cortex-A edge CPU. Atomic-contention cost, `malloc` scalability, and cache behaviour differ there. Contended atomics are likely **more** expensive on small ARM cores, which would widen the gaps in the shared and storm scenarios. `malloc_size` accounting is macOS-specific. Re-run `xthread` and `alloc_report` on the reference ARM and x86 hardware before any claim is published.
- **Noise.** The machine was shared and busy (load average 2.6 to 9.5 on 18 cores during runs), and thread placement across the two core classes was not controlled. Run-to-run difference between the two full criterion runs (`results/run_to_run.txt`): median 3.9%, p90 12.2%, max 17.5% over 130 benchmarks. **Treat differences below about 15% as noise.** The conclusions above rest on larger effects. Cross-thread numbers come from a single 7-repetition run per cell; at 8 threads, rows with identical code differ by up to 17%.
- **Decode shape.** All representations decode declared fields in declared order from a parsed `serde_json::Value`. There is no schema inference, no per-event shape detection, and no multi-shape stream. B's and C's caches are monomorphic (one shape). Behaviour with many shapes (10k distinct shapes, eviction, the ADR-0001 `max_shapes` limits) is not measured. Real decode skips the intermediate `serde_json::Value`.
- **A is modelled generously.** Names come from the declaration, so there is no hash interner and no per-event key comparison during decode. Real keyed decode from untrusted JSON would cost more.
- **Strings are copied** into `Bytes` in every variant (real decode slices the WAL chunk, so string cost is lower and equal across D8 options). The first clone of a copied `Bytes` promotes it to shared storage (one allocation); `alloc_report` excludes that with a warm-up pass. Criterion's clone timing is steady state.
- **Records are small.** 3 to 7 fields. By-name lookup is a linear scan; wide records (50 or more fields) would need a hash index in `Shape`. The cached-index path stays O(1) regardless. The `Event` envelope (times, quality, identity, attributes) is not modelled; its per-event cost dilutes D8's share of total event cost.
- **Cached-index lookup times (0.3 to 0.6 ns)** come from `black_box` on the returned reference; they omit the cost of loading and using the value.
- **Transform steps** are rename, unit conversion, and filter only. Dedup, explode, sampling, enrichment, and the CEL or structured-predicate filter (ADR-0018) are not covered.
- **Arrow.** Conversion only (builders, no Parquet encode). Builders are pre-sized to the row count, which flatters allocation counts. Arrow-throughout is not benchmarked.
- **Single toolchain** (rustc 1.99.0) and one build profile.
- **Cross-thread scaling** is bounded by the allocator and machine for every representation (C reaches 21 to 39% efficiency at 8 threads in the ring), so absolute scaling numbers say nothing about production.

## 8. Recommendation

### D8: shape-shared records (B), with `Record` opaque

- **Adopt B** as drafted in ADR-0001. Confidence: **medium-high**, up from medium. A is materially worse on transform, Arrow, memory, and contention, and the A-versus-B gap is not an artefact of A's weakest variant. B and C are tied single-threaded; B costs nothing in API cleanliness.
- **Keep the three guard rails** that make B behave like C in the measurements:
  - events are moved, not cloned, across threads on the hot path;
  - shape registries are per chain, never process-global;
  - `Chain` stays `!Sync` (all already in the implementation plan).
- **Revisit B to C** only if a real pipeline benchmark shows refcount contention on shapes (for example, cloning events to several sinks from one thread pool).

### Should `Record` be opaque? Yes

ADR-0001 already requires it. This spike supports that: the cost is nil (accessors inline), and the B-to-C question is not fully closed until the contended case is seen on target hardware. Design the opaque API so that only name-based, slow-path accessors would change under C:

```rust
// thadam-core, illustrative
pub struct Record { /* private */ }
impl Record {
    pub fn len(&self) -> usize;
    pub fn shape_key(&self) -> ShapeKey;                 // Copy + Eq + Hash. B: pointer identity. C: u32 id.
    pub fn get_at(&self, i: FieldIdx) -> &Value;         // hot path, identical under B and C
    pub fn set_at(&mut self, i: FieldIdx, v: Value);
    pub fn get(&self, name: &str) -> Option<&Value>;     // slow path; would need a registry under C
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)>; // same caveat
}
// Steps cache `ShapeKey -> ResolvedPlan` (field indexes, new shape) and use get_at on the hot path.
```

- Do **not** expose `shape: Arc<Shape>` or `values: Vec<Value>` as public fields, and do not let steps compare `Arc` pointers themselves; go through `ShapeKey`.
- Document that `Record::clone` is a refcount bump plus a `Vec` clone and is not intended for cross-thread fan-out.

### D9: extended value set

- **Adopt the extended set.** Confidence stays **high**. Typed primitive arrays (`F64s`, `F32s`, `I32s`, `I64s`) are the measured win (W2: -64% bytes per event, clone 4.7 to 5.7 times faster, transform about 2 times faster).
- **Narrow scalars and `F32`:** keep them for physical-type fidelity (they cost nothing measurable), but do not justify them on performance grounds. With declared schemas the shape's declared type already drives the Arrow column type, so typed-source fidelity is where they matter.
- **Follow-ups (not blockers for PR3):**
  - Measure a 24-byte `Value` (thin `Str`, boxed `List`) against `Utf8Bytes(Bytes)` zero-copy on the real decoder.
  - State the ownership rule for `Arc<[T]>` arrays: in-place conversion needs a unique `Arc`. Shared arrays are copied on write (2 extra allocations per W2 event in `xform(shared)`).
  - Re-run `alloc_report` and `xthread` on the reference ARM and x86 hardware.

### What this does not decide

D10 (event granularity), ADR-0018 (filter language), Parquet layout, and Arrow-throughout are outside this spike.

## 9. Reproduction

From the repository root. The project is standalone (`spikes/d8-record/Cargo.toml` has its own empty `[workspace]`), and `target/` is git-ignored by `spikes/d8-record/.gitignore`. If your environment sets `CARGO_TARGET_DIR`, unset it so criterion output lands in `target/criterion`.

```sh
cd spikes/d8-record

# 0. correctness guard: all representations produce identical events and Arrow batches
cargo test --release

# 1. allocations, bytes per event, value sizes (counting allocator, single thread)
cargo run --release --bin alloc_report | tee results/alloc_report.md

# 2. single-thread timings (about 5 minutes; 130 benchmarks)
cargo bench --bench d8 -- --noplot
python3 scripts/collect.py target/criterion > results/criterion_summary.md

# 3. cross-thread (about 3 minutes): REPS RING_EVENTS_PER_THREAD STORM_OPS_PER_THREAD
cargo run --release --bin xthread -- 7 200000 1000000 | tee results/xthread.md

# 4. run-to-run variance between two criterion runs
cp -r target/criterion /tmp/run1   # after the first run; delete target/criterion and re-run step 2
python3 scripts/compare.py /tmp/run1 target/criterion

# 5. rebuild this report from the result files
python3 scripts/build_report.py
```

Result files (`spikes/d8-record/results/`): `alloc_report.md`, `criterion_summary.md` (run 1), `criterion_summary_run2.md`, `criterion_raw.log`, `criterion_raw_run2.log`, `run_to_run.txt`, `xthread.md`.

Sources: `src/value.rs` (value sets), `src/repr.rs` (A, B, C), `src/workload.rs`, `src/arrow_conv.rs`, `src/alloc.rs`, `src/bin/alloc_report.rs`, `src/bin/xthread.rs`, `benches/d8.rs`, `tests/equivalence.rs`.
