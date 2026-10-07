# ADR-0001: Canonical event model

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (owner decisions B6 recorded; D8 spike evidence folded in; accepted)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"). The owner signed D8, D9, and D10 on 2026-10-07 (B6), and the record-representation microbenchmark has reported (`docs/design/spikes/D8-record-representation.md`). The unticked items in "Evidence and validation required" are implementation-time verification in PR3 and PR8, not blockers. Two tracked items do not change any decision recorded here: the 24-byte `Value` layout measurement (PR3) and the placement of processing time (open item O10, additive when decided; to be decided before PR13).

## Context

Thadam needs one in-memory representation of captured data that every transform, chain, and sink agrees on. product.md section 8 lists the fields the product wants (event ID, source, device, asset, stream, schema, event time, ingest time, sequence, quality, frame metadata, protocol metadata, payload, attributes) and says never to make JSON the internal model and never to silently overwrite device time. The original MVP plan turned that list into a row-oriented `Event` with a keyed record (`Vec<(Arc<str>, Value)>`), a per-event `SchemaRef`, `frame_id`, three timestamps plus a derivation tag, an optional `ClockInfo`, and both `device_id` and `asset_path`.

The architecture review found the following problems with that design:

- **Cost.** Per-event field names mean one reference-count operation per field per event, contended across worker threads, plus a `Vec` allocation per event. The minimal value set (`i64`, `u64`, `f64`, string, list) has no `f32`, narrow integers, or typed arrays, so robotics payloads such as covariance arrays and joint vectors grow roughly 3 to 4 times in memory and lose their Parquet physical type.
- **Undefined granularity.** It was not stated whether a `JointState` message with 7 joints is one event or seven. That choice affects deduplication keys, quality semantics, and file layout.
- **Redundancy and leakage.** `SchemaRef` per event is redundant with the dataset. `frame_id` is robotics-specific and partial (a TF message carries many frames). `device_id` and `asset_path` express the same identity two ways.
- **Time.** Sources may report monotonic time since boot, ROS simulation time, or TAI (International Atomic Time, 37 seconds ahead of UTC under PTP, the Precision Time Protocol). Edge devices without a real-time clock boot with wrong wall time, and NTP can step the clock backwards. `ClockInfo` has no estimation method behind it.
- **Event IDs.** A deterministic ID only enables deduplication if the payload carries source identity. A hash that excludes any per-ingest component merges legitimate identical samples. A hash that includes one never matches a redelivered copy. MQTT supplies no source identity (packet IDs are per session and reused).

Decision D1 (ADR-0003) moves the write-ahead log (WAL) in front of all decoding. The WAL therefore stores raw source messages, and the canonical model becomes an in-memory-only concern. That removes the on-disk migration cost from model changes and lowers the reversibility cost of the choices below. The model remains hard to change once transforms, sinks, and user configuration depend on it.

## Decision

### Scope and representation (folds reserved ADR-0005)

- The canonical model lives in `thadam-core` and exists only in memory. It is never written to the WAL (ADR-0003, ADR-0004). The WAL envelope is the only place protobuf (via `prost`) is used.
- Events are row-oriented in memory. Arrow appears only at the sink boundary, in `thadam-arrow`, `thadam-segments`, and (at M3) `thadam-clickhouse`. Arrow types never appear in the public APIs of `thadam-core`, `thadam-transform`, or `thadam-runtime`.
- `Event` has private fields. Accessors and a builder enforce the invariants (an event cannot be built without an ingest time; `source_time` cannot be overwritten; a time correction always sets a quality flag). `Record` is likewise opaque (accessors only), so D8 can change after the microbenchmark without touching callers. PR3 asserts an `Event` size budget of 128 bytes excluding heap allocations.

### Records and values (D8, D9)

Decided by the owner on 2026-10-07 (B6, D-B6-3 and D-B6-4).

- **D8: shape-shared positional records (representation B), behind an opaque `Record`.** Internally `Record { shape: Arc<Shape>, values: Vec<Value> }`. A `Shape` holds field names, types, and units once, and carries a fingerprint. Transforms resolve field indexes once per shape and cache them. Each chain owns its shape registry, so there is no cross-thread contention. A shape converts to an Arrow schema directly.
  - **The `Record` API is opaque so that a later move to representation C (a per-chain `ShapeId(u32)` registry) changes only the name-based accessors.** Neither `shape: Arc<Shape>` nor `values: Vec<Value>` is a public field, and steps never compare `Arc` pointers themselves. The illustrative surface, taken from the D8 report (section 8), is:

    ```rust
    pub struct Record { /* private */ }
    impl Record {
        pub fn len(&self) -> usize;
        pub fn shape_key(&self) -> ShapeKey;                        // Copy + Eq + Hash. B: pointer identity. C: u32 id.
        pub fn get_at(&self, i: FieldIdx) -> &Value;                // hot path, identical under B and C
        pub fn set_at(&mut self, i: FieldIdx, v: Value);
        pub fn get(&self, name: &str) -> Option<&Value>;            // slow path; would need a registry under C
        pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)>; // same caveat
    }
    ```

    Steps cache `ShapeKey -> ResolvedPlan` (field indexes and the output shape) and use `get_at` on the hot path. Under C only `get`, `iter`, and `Debug` would need a registry parameter, and they are the slow-path accessors.
  - **Three guard rails keep B equivalent to C in the measurements:** events are moved, not cloned, across threads on the hot path; shape registries are per chain and never process-global; and `Chain` stays `!Sync`. `Record::clone` is documented as a refcount bump plus a `Vec` clone and is not intended for cross-thread fan-out.
  - Wide records (50 or more fields) need a hash index in `Shape` for by-name lookup; the benchmark covered only 3 to 7 fields. The cached-index path is O(1) regardless.
- **D9: extended value types.** `Null`, `Bool`, `I8` to `I64`, `U8` to `U64`, `F32`, `F64`, `Str` (a validated `Bytes` slice, `Utf8Bytes`), `Bytes`, `Ts`, typed primitive arrays (`F32s`, `F64s`, `I32s`, `I64s`, as `Arc<[T]>`), `List`, and nested `Record`.
  - The measured payoff is the typed arrays. Narrow scalars and `F32` showed no memory or CPU effect in the spike (a narrow scalar still occupies a full `Value` slot) and are kept for physical-type fidelity at typed-source boundaries (CDR, Parquet physical types, `u64` above `i64::MAX`, exact `f32`).
  - **Ownership rule for typed arrays.** In-place conversion (for example unit conversion) needs a uniquely owned `Arc<[T]>`. A shared array is copied on write, which cost 2 extra allocations per W2 event in the benchmark's `xform(shared)` case. The normal pipeline moves events and keeps the in-place path.
  - **Follow-up for PR3 (from the D8 report, not a blocker):** `size_of::<Value>()` is 40 bytes because `Utf8Bytes(Bytes)` makes `Bytes` (32 bytes) the widest payload. A 24-byte layout (the `Str` payload held behind a thin pointer, and `List` as `Box<[Value]>`) is 24 bytes by `size_of` alone, a layout check that was not benchmarked. On W1 it would save about 112 bytes per event (7 fields times 16 bytes), roughly the same as the whole saving of B over the best-case keyed design (120 bytes), at the cost of the zero-copy WAL slice that motivates `Utf8Bytes`. PR3 evaluates the 24-byte layout against `Utf8Bytes(Bytes)` on the real decoder (PR8 provides the real decode path) and records the result here before the `Value` layout is treated as settled. Because `Record` is opaque, the layout is an internal choice and the decision above does not change.
- **Payload kinds.** `Record`, `Raw { content_type, bytes }`, and `ObjectRef`. Large media travels as an `ObjectRef`. There are no `Pose` or `Ros2Msg` variants. Robotics types are `Record`s that follow field conventions, and `frame_id` is a payload field, not an envelope field.

### Granularity (D10)

Decided by the owner on 2026-10-07 (B6, D-B6-5): one event per source message by default. An `explode` transform turns a message that contains an array of samples into one event per sample and records a `sample_idx`. Quality is event-level only for the MVP; the owner decided this on 2026-10-07 (B3, D-B3-3). Field-level quality is deferred. product.md section 22's per-measurement example (`temperature = 91.2, quality = OUT_OF_RANGE`) fits only single-measurement messages or exploded samples.

### Time model (D15, folds the semantics part of reserved ADR-0014)

- `source_time` is what the device reported. It is set once at decode and never overwritten.
- `ingest_time` is the edge receipt time, stamped by the source. It is never null and never changed.
- `corrected_time` is optional. A correction must set a quality flag so device time is never silently replaced.
- `time_domain` is one of `Utc`, `Tai`, `Monotonic`, `Sim`, `Unknown`, configured per dataset or set by the decoder. A non-UTC source time never becomes `event_time` without an explicit mapping.
- `clock_synced` is recorded per batch from the kernel's NTP state **[V: `adjtimex` status]**.
- Sinks materialize `event_time = corrected.or(source if domain is Utc).or(null)`.
- Timestamps are `i64` nanoseconds since the Unix epoch, UTC. Parsing requires explicit units and rejects strings without a timezone unless one is configured. Float epoch seconds are accepted with the known precision limit (an `f64` resolves about 0.24 microseconds at current epoch values) [V in PR8].
- Wall time is never used to measure durations. Retention, rotation, and timeouts use monotonic time and WAL positions.
- A `ClockInfo` slot is reserved but not implemented. Add it when an offset-estimation method exists.
- The Parquet timestamp unit (microsecond default, nanosecond opt-in, D14) and the partition time basis (D16) belong to ADR-0013.

### Quality

A three-level status (`Good`, `Uncertain`, `Bad`), a `u32` flag bitset, and an optional reason, all stored as columns. Flags in use across the plans: `OUT_OF_RANGE`, `CLOCK_SKEW`, `MISSING_TIME`, `OUT_OF_ORDER`, `DUPLICATE`, `STALE`, `SEQUENCE_GAP`, `DECODE_ERROR`. The set is documented in `docs/concepts/quality.md` when that document exists.

### Identity

An ordered hierarchy of (level, value) pairs (`AssetPath`). `device_id` is just a level. Values come from topic templates such as `{topic.3}` or from payload fields. No level is mandatory, which matches product.md section 8.

### Event ID

- **Default:** `xxh3_128(epoch, frame_seq, record_index)`. It is deterministic per WAL record and stable across replay, but a broker redelivery receives a different ID because it becomes a new WAL record.
- **Content-derived:** a dataset may configure `event_id.key` (for example `[site, robot, seq]`). For exploded datasets the ID also incorporates `sample_idx`, as in the demo's `xxh3_128(dataset, site, robot, seq, sample_idx)` **[A]**.
- **Dedup rule:** deduplication across broker redelivery requires a content-derived ID. A dataset that enables `dedup` without `event_id.key` fails validation. The deterministic default is only meaningful when the payload carries a source sequence or a high-resolution source timestamp. The deduplication behavior itself is in ADR-0011.
- The lineage columns carry the frame position but not the record index, so a row cannot yet be located inside a frame (open item O4, closed in PR13 as not written: `thadam-arrow` reserves field ID 13 for a `_wal_record_idx` column; ADR-0013).

### Envelope columns, interning, and limits

- Every dataset carries `_event_id`, `_ingest_time`, `_source_time`, `_event_time`, `_time_domain`, `_quality_status`, `_quality_flags`, `_quality_reason`, `_clock_synced`, `_wal_epoch`, `_wal_pos`, identity level columns, and `sample_idx` for exploded datasets. Column types and Parquet details are ADR-0013's.
- Interning tables and shape caches are size-capped through `ChainLimits` (`max_shapes`, `max_dedup_keys`, maximum JSON depth), because keys can come from untrusted payloads. Overflow becomes a counted reject, not unbounded growth.

### What this ADR supersedes

From ARCHITECTURE.md section 19, "In the original MVP plan":

- The keyed `Record = Vec<(Arc<str>, Value)>` and the minimal `Value` set.
- Per-event `SchemaRef`, `frame_id`, `ClockInfo`, and `device_id` alongside `asset_path`.
- `Timestamp` as three times plus `TimeDerivation`, and the event-level `event` time field.
- Event ID as a `uuid_v7` or `hash(fields)` configuration choice with in-memory dedup.
- WAL frames as prost-encoded `EventBatch` (the model is no longer serialized).

It also records the former reserved ADR-0005 (internal representation) and the semantics half of ADR-0014 (time and clock). See "Revisit when" for the ADR-0005 trigger.

## Alternatives considered

- **Keyed dynamic records (original design, representation A).** Field names and `Arc` refcounts per event, one allocation per record. Not chosen, and the D8 report measured why. Against B on W1 telemetry (7 fields): 712 bytes per event for A with a fresh name allocation per field and 488 bytes for A with pre-interned names, against 368 for B; a transform pass of 126 ns (fresh) and 44 ns (interned) against 11 to 13 ns; Arrow conversion 2.3 to 2.7 times slower. With one shape or name set shared across 8 threads the interned variant collapsed to 3.4 to 4.1 million events per second, below its own single-thread rate. The interned variant is the generous case; real keyed decode from untrusted JSON costs more.
- **Shape by index (representation C, `ShapeId(u32)` plus a per-chain registry).** Indistinguishable from B single-threaded (ratios 0.81 to 1.15 over both runs, inside the 15% noise floor) and much better when many threads clone records that share one shape (122 to 151 million clone+drop per second against about 20 for B). Not chosen now: a `ShapeId` record is meaningless without its registry, every name-based accessor needs a registry parameter, and passing a record to the wrong chain's registry yields silently wrong field names. The planned ownership rules avoid the case where C wins. The opaque `Record` keeps C available as a later change (see "Revisit when").
- **Arrow `RecordBatch` throughout.** Zero conversion at the sinks and vectorized transforms. Not chosen: it needs a schema per batch while MQTT and ROS streams are heterogeneous and often schemaless; per-event transforms (dedup, filter, unit conversion) are awkward on columns; builders suit small low-latency batches poorly; and Arrow's frequent breaking releases would ripple into the core. The D8 spike did not benchmark Arrow-throughout, only the cost of converting records to Arrow, which was 17 to 19 ns per event, about 2 to 4% of the derived decode-to-Arrow path and far under the 25% revisit trigger. The conversion cost does not justify the design cost, so it is not chosen.
- **Protobuf `DynamicMessage` (`prost-reflect`).** Couples the core model to protobuf descriptors. Not chosen.
- **Custom binary envelope (`postcard`, `rkyv`, hand-rolled).** Fast and possibly zero-copy, but Thadam would own schema evolution permanently and it is not language-neutral. Moot for the in-memory model once the WAL stores raw bytes; not chosen.
- **Minimal value set (`i64`, `f64`, string, list).** Loses physical types and costs memory on arrays. Not chosen (D9).
- **One event per measurement.** Matches product.md's per-measurement quality example but multiplies event counts and dedup keys for array messages. Not chosen; `explode` provides it per dataset when wanted.
- **Field-level quality now.** Larger model and Parquet layout, with no user evidence. Deferred by owner decision B3 (2026-10-07): event-level quality only for the MVP.
- **Three times plus derivation plus `ClockInfo`.** `ClockInfo` has no estimation method and the derivation tag is subsumed by the quality flag on correction. Not chosen.
- **Per-event `SchemaRef`, `frame_id` in the envelope, and separate `device_id`.** Redundant or protocol-specific, as described in Context. Not chosen.
- **`uuid_v7` event IDs.** Cheap but makes cross-redelivery deduplication impossible. Not chosen as a default. A hash that includes ingest position has the same defect.
- **Making `Record` public (non-opaque).** Simpler now but freezes D8. Not chosen.

## Consequences

### Easier

- Model changes are no longer disk migrations, because the WAL holds raw messages.
- Memory per event and CPU per transform drop against the keyed design. The D8 spike measured this on one Apple Silicon (arm64) laptop: 368 against 488 to 712 bytes per W1 event, a transform pass roughly 3.5 to 10 times faster, and, with typed arrays on W2 joint state, 400 against 1120 bytes per event. Absolute numbers are machine-specific and need a re-run on the reference ARM and x86 hardware before any published claim (see Evidence).
- Arrow conversion is close to a shape-to-schema mapping.
- Identity has a single path, so sinks flatten one structure into columns.

### Harder

- Shapes add a cache to bound and invalidate. Interning and shape growth need caps, metrics, and tests.
- Content-derived IDs need per-dataset configuration, and users must know their payloads carry a source sequence.
- Event-level quality cannot express a bad field inside an otherwise good message.
- The product.md section 8 field list is no longer followed literally: `schema_id`, `frame_id`, and `device_id` are gone from the envelope, and no per-event `process_time` is carried. product.md section 8 requires distinguishing `process_time`, so this is a divergence to catalog in `docs/design/product-amendments.md` **[OPEN]**. Whether to record processing time per file or batch is undecided.
- A per-event ingest sequence is replaced by WAL position (`_wal_epoch`, `_wal_pos`). A source sequence is a declared payload field, not an envelope field **[A]**.

### Fixed once released

- The envelope column names and meanings, the `TimeDomain` set, the flag meanings, and the event-ID derivation. Datasets written to users' lakes depend on them.

### Failure modes to design against

- JSON numeric ambiguity (`1` versus `1.5`) changes value types and therefore shapes, causing downstream file churn. Mitigation: declared schemas, widening-only casts, lock-after-first-file in dev mode (ADR-0013).
- Interning growth from untrusted payload keys. Mitigation: size caps and counted rejects.
- `u64` values above `i64::MAX` reaching sinks without unsigned types. Mitigation: a per-sink unsigned policy (ADR-0013).
- Large payloads ingested as a `Record` instead of an `ObjectRef`.

## Evidence and validation required

- [x] **Record-representation microbenchmark**, report at `docs/design/spikes/D8-record-representation.md` (done 2026-10-07). It compared keyed (A, fresh and interned), shape-shared (B), and shape-by-index (C) on allocations and bytes per event for W1 (small telemetry) and W2 (joint-state arrays), transform throughput by cached index versus by name, Event-to-Arrow conversion cost, and cross-thread behavior. Outcome: B recommended with medium-high confidence, extended values recommended with high confidence, `Record` stays opaque. Arrow-throughout was not benchmarked (not a candidate any more; see Alternatives).
- [x] Owner sign-off on decisions D8, D9, and D10 (owner decision B6 in `docs/design/owner-decisions.md`), signed 2026-10-07, before PR3 and PR4.
- [ ] **D8 re-measurement on reference hardware (ARM and x86) and with the real decoder (PR8).** The spike used an Apple Silicon (arm64) with the system allocator and a parsed `serde_json::Value`; contended atomics are likely more expensive on small ARM cores, and real decode skips the intermediate `Value`. Re-run `alloc_report` and `xthread`. Not a PR3 blocker.
- [ ] **24-byte `Value` layout** evaluated in PR3 against `Utf8Bytes(Bytes)` zero-copy, on the real decoder, and the result recorded here (see Decision, D9).
- [ ] **Opaque `Record` API test:** a test that fails if a public field or an `Arc<Shape>` pointer comparison leaks outside `thadam-core`, so the B-to-C move stays confined to accessors.
- [ ] Builder invariant tests: `source_time` is immutable, `correct()` always sets a flag, no event without ingest time. The `Event` size budget test.
- [ ] Property tests: value round trip through Arrow preserving types; widening never narrows; `explode` correctness; unsigned-to-signed mapping per sink.
- [ ] Shape cache behavior at 10,000 distinct shapes, including eviction and counted rejects at the cap.
- [ ] Time-domain mapping review against sample data from ROS simulation time, PTP/TAI, and monotonic-clock sources. A clock-step simulation with a fake clock jumping backwards. An unsynced-boot test.
- [ ] Timestamp parsing tests for every supported format, including precision properties.
- [ ] Quality-semantics review against user payload samples, to check that event-level granularity (decided by the owner, B3, 2026-10-07) holds for real messages. This review informs a later revisit; it no longer gates the ADR.
- [ ] Event-ID tests: default ID stable across replay; content-derived ID stable across redelivery; validation failure for `dedup` without `event_id.key`.

## Revisit when

- Benchmark B3 shows Event-to-Arrow conversion above about 25% of pipeline CPU. Then prototype Arrow batches between the WAL and the sinks (the former ADR-0005 trigger). Benchmark B3 here is the plan's latency and conversion-cost benchmark, not owner decision B3.
- A real pipeline benchmark shows refcount contention on shapes (for example, cloning events to several sinks from one thread pool). Then move from representation B to C behind the opaque `Record`; only the name-based accessors change. The D8 report found that B reaches only about 54 to 63% of C's throughput with one shape shared by all threads, and about 20 million clone+drop per second against 120 to 150 million for C.
- The D8 re-measurement on reference hardware or with the real decoder shows shape-shared records are not materially better than keyed records. (The first D8 run did not meet this condition: B was materially better than the keyed design, mostly on transforms and Arrow conversion, not on construction.)
- Users need field-level quality (the owner's B3 decision limits the MVP to event-level).
- A clock-offset estimation method exists. Then add `ClockInfo` in the reserved slot.
- User payloads show that one event per message is the wrong default granularity.

## References

- ARCHITECTURE.md section 3 (D8, D9, D10, D15), section 4 (canonical model), section 7 (processing order, determinism), section 19 (superseded decisions), section 22 (open items O1, O4, O8).
- ADR-0003 (execution and process model), ADR-0004 (durable buffer, WAL envelope), ADR-0011 (deduplication semantics, claims register), ADR-0013 (Parquet layout, timestamp unit, partitioning), reserved ADR-0005 and ADR-0014.
- product.md sections 8 (canonical event model), 22 (data quality), 23 (time synchronization).
- Plans: `thadam_architecture_review` sections 1 and 8 and decision matrix D8, D9, D10, D15; `thadam_rust_implementation_plan` section 5.1; `thadam_mvp_architecture_plan` section C and the ADR-005 trigger.
- Spike report: `docs/design/spikes/D8-record-representation.md` (2026-10-07; sections 5 and 6 for the measurements, section 8 for the recommendation and the opaque API).
- Owner decisions: `docs/design/owner-decisions.md`, D-B6-3 (D8), D-B6-4 (D9), D-B6-5 (D10), and D-B3-3 (event-level quality).
