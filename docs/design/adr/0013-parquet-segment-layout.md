# ADR-0013: Parquet segment layout

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (PR13 implementation decisions recorded in `thadam-arrow`: field-ID policy (O5 closed), `_wal_record_idx` (O4 closed), unsigned default, list element name, `sample_idx` width, schema fingerprint)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"). The unticked items in "Evidence and validation required" (benchmarks W4 and B6 with their `[OPEN]` criteria, the reader compatibility matrix, O4 and O5 and the unsigned-integer default) are implementation-time verification in PR13 and PR14, not blockers. PR13 (`thadam-arrow`, commit `4fa3802`) has since closed O4, O5, and the unsigned-integer default; see "PR13 implementation decisions".

## Context

Thadam's first sink writes finalized, immutable Parquet files locally (called segments), and the shipper uploads them to S3-compatible storage under deterministic keys. Everything about the file layout becomes visible to users: the directory and object-key structure, the column names and types, the timestamp unit, the footer metadata. Changing any of them after release means a new dataset version or a rewrite, so they need to be decided before PR13 (`thadam-arrow`) and PR14 (`thadam-segments`).

Forces:

- **Fleets.** The original design partitioned files and keys per stream. With `robots/+/telemetry` from 500 robots that gives 500 open writers and a small-file explosion, and at 1000 streams per-stream files cannot fit in 256 MB of memory at all (architecture review, section 10).
- **Edge memory.** The default Parquet `max_row_group_size` is about 1 million rows, and compression contexts such as zstd take several MB each. Memory for open writers must be bounded by configuration (D21).
- **Unreliable clocks.** Devices without a real-time clock boot with the wrong time until NTP syncs, and device clocks can be skewed. Partitioning by event time puts files in 1970; partitioning by ingest time with no fallback is wrong before NTP sync.
- **Reader diversity.** Output must be readable by DuckDB, pyarrow, Spark, Trino, and Athena. Nanosecond timestamps are poorly supported by some readers **[V]**.
- **Schema churn.** JSON `1` versus `1.5` flips an inferred type and produces a new file per flip.
- **A later table format.** Apache Iceberg's append-only `fast_append` (iceberg-rust 0.10.1, MSRV 1.94, as checked 2026-10-07 in the ecosystem maturity review) matches immutable segments with deterministic names, provided every column has a stable Parquet field ID. The `deltalake` crate reaching 1.0 in September 2026 (same review) is the comparison point for the later P2 table-format ADR.

Related decisions: WAL placement and chains are in ADR-0003; commit-then-ship (D18) and the delivery claims are in ADR-0011; the object-store shipper detail is covered in ARCHITECTURE.md section 8 because ADR-0009 is reserved. This ADR folds in the Parquet timestamp and partition parts of the reserved ADR-0014; the time model itself (D15) is in ADR-0001.

## Decision

### Datasets are the output unit (D11)

A dataset is an output table: a topic or source match, a decode format, an identity template, an optional `event_id.key`, optional `dedup` and `explode`, a time rule, and a declared field list. Device identity is a column and never a partition or file unit. Open writers are capped by `max_open_files` (4 on the edge profile); when the cap is reached, the least recently written file is finalized early. User-defined partition templates are not available in the MVP.

### Wide layout with declared schemas (D12)

One row per event, one column per declared field, with a declared schema per dataset. Decided by the owner on 2026-10-07 (B3, D-B3-2), together with event-level quality only for the MVP (D-B3-3). A narrow layout (`stream, ts, field, value`) is considered only after evidence from target users.

### Schemas (D13)

- **Production:** declared schemas are required. A field has a name, type, source path, and optionally a unit, a `convert_to`, and `required: true`.
- **Development:** inference is allowed with widening only (I32, then I64, then F64; never narrowing) and a lock after the first file is written.
- **Unknown fields:** `strict` quarantines the message, `lenient` drops and counts, and an optional `_unmapped` column captures them.
- **Evolution rules (Iceberg-compatible):** add optional columns; widen `int` to `long` and `float` to `double`; never rename or narrow. Anything else is a new dataset version, `dataset@v2`, which maps to a new topic version and a new key prefix.
- **Unsigned integers** above `i64::MAX` follow a per-sink unsigned policy (`ArrowOptions.unsigned`). The default is **`Native`** (decided in PR13): write the unsigned Arrow types (`UInt8`, `UInt16`, `UInt32`, `UInt64`), which Parquet stores as unsigned logical types. `Signed` is an option: it maps to the next wider signed type (`Int16`, `Int32`, `Int64`), with `u64` staying `Int64` so a value above `i64::MAX` is rejected with `schema.narrowing`. The policy applies to every unsigned column, envelope columns included, so `Signed` yields a schema with no unsigned type in it. Reader support for unsigned logical types is **[V]** and still unverified in this repository (Iceberg has no unsigned types; Athena and some Trino versions are the open items); see "PR13 implementation decisions".

### Stable Parquet field IDs from the first release

Every column, including envelope and lineage columns, is written with a stable `PARQUET:field_id` from the first release. The cost now is almost nothing, and retrofitting is hard (maturity review section 15, item 3). **The assignment policy was open item O5 and is closed by PR13; the decision is recorded under "PR13 implementation decisions" below.** The options considered were:

1. **Explicit IDs in configuration.** Stable and reviewable, and identical across nodes that share a configuration. Costs: user burden, validation for duplicates and reuse, and a risk of mistakes when editing.
2. **Auto-assignment in declaration order with a persisted lock.** No user burden. Costs: the lock must be stored somewhere durable (per node, so two nodes with different declaration order could assign different IDs to the same logical column), and removing a field must never free its ID.
3. **Reserved ranges for envelope columns with sequential allocation for declared fields.** Envelope IDs are fixed by the Thadam format version and identical on every node. Declared-field IDs still need a persisted allocation so additions and removals never reuse IDs.

Evidence that decides it: a field-ID round-trip test through the writer and a reader; a check that a downstream Iceberg reader or writer accepts the IDs **[V]**; a two-node scenario with differing declaration order; and the owner's and the PR13 author's review. Any option must satisfy: IDs unique within a dataset, never reused after a field is removed, stable across restarts and Thadam upgrades, and identical for the same logical column across nodes that share a configuration.

### Time columns and units (D14, D16)

- **Unit:** `Timestamp(Microsecond, "UTC")` by default; nanoseconds are opt-in per dataset (`timestamp_unit`). Microseconds are enough at 1000 Hz. Reader compatibility for both units across DuckDB, pyarrow, Spark, Trino, and Athena is tested in CI **[V]**.
- **Partition basis:** ingest date and hour, from `_ingest_time`. When `clock_synced` was false for the data, the file is placed under `unsynced/` instead. Footers carry min and max `event_time` statistics.
- **Event time** is materialized as `corrected` or `source` (when the domain is UTC) or null (ADR-0001). Non-UTC source times never become `event_time` without an explicit mapping.
- **File partition key [A, OPEN for PR13 and PR14]:** a file never spans two partition keys. The sink finalizes a file when the next frame's key (ingest hour of its first record, and the synced flag) differs. Rows in a frame that straddles an hour boundary keep their exact `_ingest_time`; readers must not assume the path hour equals every row's hour.

### Paths, names, and keys

- **Ready path:** `ready/<dataset>/date=YYYY-MM-DD/hour=HH/{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet`; or `ready/<dataset>/unsynced/...`. Files are written under `staging/` as `*.inprogress` and renamed into `ready/`.
- `first` and `last` are the WAL positions the file covers. Inclusivity of `last` is to be fixed in PR14 **[A]**.
- **Object key:** `{prefix}/thadam/v1/{pipeline}/{dataset}/date=.../hour=.../{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet`. Hive-style, so DuckDB, Trino, Athena, and Spark read it directly.
- **Epoch and machine-id binding** prevent key collisions when a data directory is wiped and WAL positions restart, or when a directory is copied to another node. A collision with different content is the Fatal `OBJECT_KEY_COLLISION`.
- Replays write to a separate directory or an isolated prefix, never the production prefix.

### Footer key-value metadata

`thadam.version`, `thadam.dataset`, `thadam.schema_fp`, `thadam.wal_range`, `thadam.config_hash`, `thadam.enrichment_hash`, `thadam.clock_synced`. Dataset-level metadata such as joint names also goes in key-value metadata, not in per-row columns. `created_by` is `thadam <version>`.

### Envelope and lineage columns

Every dataset carries `_event_id` (FixedSizeBinary(16)), `_ingest_time`, `_source_time`, `_event_time`, `_time_domain`, `_quality_status`, `_quality_flags` (UInt32 bitset), `_quality_reason`, `_clock_synced`, `_wal_epoch`, `_wal_pos`, identity level columns (dictionary strings), and `sample_idx` for exploded datasets. This is the demo plan's superset. Columns prefixed with `_` are reserved for Thadam **[A]**.

As implemented in `thadam-arrow` (PR13), in column order. Unsigned types are shown under the default `Native` policy; `Signed` widens them. `_event_id`, `_wal_epoch`, and `_wal_pos` are the lineage columns and are written only while `ArrowOptions.lineage_columns` is on (the default).

1. `_event_id`, `FixedSizeBinary(16)` (lineage)
2. `_ingest_time`, never null
3. `_source_time`, nullable
4. `_event_time`, nullable
5. `_time_domain`, `Dictionary(Int8, Utf8)`
6. `_quality_status`, `Dictionary(Int8, Utf8)`
7. `_quality_flags`, `UInt32`
8. `_quality_reason`, nullable `Utf8`
9. `_clock_synced`, `Boolean`
10. `_wal_epoch`, `UInt64` (lineage)
11. `_wal_pos`, `UInt64` (lineage)
12. one nullable `Dictionary(Int32, Utf8)` column per identity level
13. `sample_idx`, nullable **`UInt16`** (exploded datasets only)
14. the declared fields, in declaration order

All timestamps are UTC, in the unit given by `ArrowOptions.ts_unit` (microseconds by default, nanoseconds opt-in). `sample_idx` is `UInt16`, so a sample index above 65,535 is out of range for the column.

**Open item O4 (`_wal_record_idx`): closed in PR13, not written.** The default event ID is `xxh3_128(epoch, frame_seq, record_index)`, but the lineage columns carry only the frame position, so a row cannot be mapped back to its record within a frame and the default ID cannot be recomputed from a row. This ADR proposed a `_wal_record_idx` column, UInt32 **[A, OPEN: width]** (a frame is capped at 4 MiB, so a 16-bit index may be too small for tiny records). `thadam-arrow` does **not** write the column; field ID 13 is reserved for it, so adding it later is an additive change that cannot collide with another column. Its width and storage cost (B6) are therefore not decided; they are decided if and when the column is added.

### Writer settings

- **Row groups sized by bytes:** 8 MiB on the edge profile, flushed on `ArrowWriter::in_progress_size()`. The default row-count limit is too large.
- Dictionary encoding on low-cardinality strings; statistics on time columns; bloom filters off.
- **Compression:** `snappy` by default on aarch64 and `zstd(1)` on x86_64, pending benchmark B6. Both are configurable.
- **Memory:** `max_open_files x 2 x row_group_bytes` is part of the Thadam-owned memory estimate printed by `thadam validate`.

### Rotation

A file is finalized on whichever of `max_bytes`, `max_rows`, or `max_age` comes first, or at shutdown (`finalize_on_shutdown`). Production default: `max_age: 5m`, `max_bytes: 128MiB`. The demo uses 60 s and 64 MiB so output appears quickly. Short rotation means small files; the README explains the trade-off.

### Finalize sequence and catalog

The finalize sequence (close and SHA-256, `fdatasync`, rename, directory fsync, `Finalized` catalog record, new watermark, cursor commit, `Eligible` record, then upload) is specified in ARCHITECTURE.md section 8; commit-then-ship (D18) is owned by ADR-0011. This ADR relies on one consequence: each WAL range of a dataset lands in exactly one finalized file.

### Quarantine segment

A fixed schema: `_wal_epoch`, `_wal_pos`, `_ingest_time`, `source_id`, `topic`, `error_domain`, `error_code`, `reason`, and `raw` (binary, truncated at 64 KiB, with a `raw_truncated` flag). It has its own disk budget (256 MiB in the demo) so a schema change cannot starve normal data. One chain is designated primary for quarantine to avoid double counting (O3).

### PR13 implementation decisions (`thadam-arrow`, commit `4fa3802`)

These close the items this ADR left open for PR13. The facts come from the `thadam-arrow` crate documentation (`columns` module, `options`, `fingerprint`).

**Field-ID policy (O5, closed).** The policy is a mix of the three options above: IDs are partitioned by origin, envelope IDs are fixed by the format, and every other top-level ID is explicit.

| Range | Origin | Assigned by |
|---|---|---|
| `1..=99` | envelope and lineage columns | fixed by `thadam-arrow` (format v1) |
| `100..=199` | identity level columns | explicit, from configuration |
| `1000..=4_194_303` | declared top-level fields | explicit, from configuration |
| `8_388_608..` | list elements and record children | derived from the parent ID and tree position |

- **Envelope IDs (format v1):** `_event_id` 1, `_ingest_time` 2, `_source_time` 3, `_event_time` 4, `_time_domain` 5, `_quality_status` 6, `_quality_flags` 7, `_quality_reason` 8, `_clock_synced` 9, `_wal_epoch` 10, `_wal_pos` 11, `sample_idx` 12. **ID 13 is reserved** for `_wal_record_idx` (O4); the column is not written.
- **Declared top-level IDs are explicit and required.** A declared field must carry an ID in `1000..=4_194_303`; the ID does not depend on declaration order, field removal, restarts, or upgrades. `thadam-arrow` rejects duplicates and out-of-range IDs. **IDs are never reused after a field is removed**, but `thadam-arrow` cannot know history, so keeping that rule is the job of whoever owns the configuration (`thadam-config` and the runtime).
- **Nested IDs are derived.** A nested field's ID is derived from its top-level field's ID and its position in that field's type tree (depth first, pre-order): `8_388_608 + (id - 1000) * 256 + slot`. A top-level field may have at most 256 nested fields. Nested IDs are unique in the dataset and stable while the field's type is unchanged, including under widening (`list<i32>` to `list<i64>` keeps its shape). Changing a nested type's shape is a new dataset version (evolution rules above), so IDs written on nested field definitions are ignored.
- **List element name is `element`** in every list type.
- **Two-node requirement.** Because IDs are explicit in configuration, two nodes that share a configuration produce the same ID for the same logical column. This is what the ADR required of any option.
- **Not yet wired.** `thadam-config` (PR5) has no key for a field ID or an identity-level ID today (`FieldSpec` carries `name`, `type`, `from`, `required`, `unit`, `convert_to`), and `thadam-arrow` requires them. How the pipeline configuration supplies the IDs, and how the "never reuse" rule is enforced, must be settled before the runtime builds a `DatasetSchema` from a `PipelineSpec` (wave 3).
- **Evidence so far.** `thadam-arrow` tests cover that field IDs survive an Arrow IPC round trip. The Parquet writer and reader round trip, and the Iceberg compatibility check **[V]**, belong to PR14.

**Unsigned integers (closed).** The default policy is `Native` (`UInt8`, `UInt16`, `UInt32`, `UInt64`); `Signed` is an option (see "Schemas"). Reasons for the default: it matches the envelope types this ADR and the demo plan name (`_quality_flags` is `UInt32`, `_wal_pos` is `UInt64`), it is lossless and has no reject path, and it suits sinks with native unsigned types (ClickHouse, DuckDB, pyarrow). `Signed` is for lakes read by engines or table formats without unsigned types. Reader behavior for unsigned Parquet logical types is unverified in this repository.

**`_wal_record_idx` (O4, closed).** Not written. ID 13 is reserved (see "Envelope and lineage columns").

**Other encoding details.** `sample_idx` is `UInt16`. The list element field is named `element`. `ArrowOptions` also carries `ts_unit` (microseconds by default, nanoseconds opt-in) and `lineage_columns` (on by default).

**Schema fingerprint.** `thadam.schema_fp` carries a 16-byte fingerprint, written in hex in the schema metadata (`thadam.dataset` carries the dataset name). It is SHA-256 over a canonical, versioned encoding of the output columns, truncated to 128 bits. The encoding is defined in `thadam-arrow` and does not use Arrow's `Debug` or `Display`, so an Arrow upgrade cannot change a fingerprint that is already in files. It is domain-separated (`thadam-arrow/schema/v1`) and covers each column's name, nullability, data type tag, and sorted field metadata (which includes the field ID and unit), recursively for lists and structs, so a change to any of these changes the fingerprint; the unsigned policy and timestamp unit are part of it through the column types. Type tags are fixed once released.

### Superseded by this ADR (ARCHITECTURE.md section 19)

- Partitioning Parquet output and object keys per `stream_id`, and the `<stream_id>` path and key.
- The Parquet commit point being the rename, with the cursor recovered from footers (replaced by cursor-then-ship, D18; footers are still read to rebuild the catalog).
- A configurable partition time basis between `ingest_time` and `event_time` with no fallback.
- "Byte-identical Parquet on replay" (replaced by row-set equivalence).
- `epoch8` in names (now `{epoch:016x}`).
- The reserved ADR-0014's Parquet timestamp and partition content.

## Alternatives considered

- **Per-stream or per-device partitioning (the original design).** Rejected: 500 robots mean 500 open writers, a small-file explosion, and a memory footprint that cannot fit 256 MB at 1000 streams.
- **Narrow long layout (`stream, ts, field, value`).** Not chosen for the MVP: compression and query ergonomics depend on target-user query patterns that are unknown. Revisited after user evidence; the owner chose wide for the MVP on 2026-10-07 (B3).
- **Both layouts.** Rejected: two writers, two test matrices, and double storage.
- **Partition by `event_time`.** Rejected: skewed device clocks put files in 1970 or the far future.
- **Partition by `ingest_time` with no fallback.** Rejected: wrong before NTP sync on devices without a real-time clock. The `unsynced/` partition is the fallback.
- **Nanosecond timestamps as the default.** Rejected: reader support is poor in some Spark and Hive-era engines **[V]**; opt-in per dataset.
- **Always infer schemas.** Rejected: type flapping and file-count explosion. Inference remains a development-mode tool with widening and lock-after-first-file.
- **Byte-identical file contents on replay.** Rejected: rotation uses wall-clock `max_age`, `created_by` includes the version, and the enrichment file may change. The defined property is row-set equivalence for the same WAL range, configuration hash, and enrichment-file hash.
- **Adding field IDs later.** Rejected: retrofitting needs a rewrite, while writing them now costs almost nothing.
- **User-defined partition templates.** Deferred past the MVP; fixed layout keeps keys stable.

## Consequences

**Easier**

- Open-writer count is bounded by `max_open_files`, so memory is predictable and files are large enough to query.
- Readers see Hive-style partitions and additive schema changes (DuckDB `union_by_name`, Spark `mergeSchema`).
- A later Iceberg `fast_append` stage can reference existing files without rewriting them.
- Lineage columns let any row be traced to its capture record without logging payloads.

**Harder**

- Declared schemas are required for production, which is friction for new users; development-mode inference softens it.
- Users querying one device scan files that contain many devices; per-device pruning relies on statistics and dictionary encoding, not on paths.
- Field-ID assignment needs a durable policy. O5 is closed (explicit IDs in configuration, never reused), which puts the burden of unique, never-reused IDs on the configuration owner; the configuration key that carries them is not yet defined.
- Small files appear at short rotation intervals; the production defaults trade visibility latency for file size.

**Fixed once released**

- The ready path and object-key layout, the partition basis (ingest date and hour plus `unsynced/`), file naming, footer key names, envelope column names, and field IDs.
- Changing a column's type incompatibly, renaming it, or narrowing it means a new dataset version.

## Evidence and validation required

This ADR can be accepted once its blocking items pass. Items marked **[V]** may remain open with an explicit owner note.

- [ ] **W4 high-cardinality benchmark:** memory and file count with many devices and streams, showing dataset partitioning stays within the memory target. Criterion: [OPEN: criterion to be set before the benchmark runs].
- [ ] **Reader compatibility matrix in CI:** DuckDB, pyarrow, Spark, Trino, and Athena reading files with microsecond and nanosecond timestamps **[V]**.
- [ ] **Benchmark B6:** write MB/s by rotation size (16, 64, 256 MiB), codec (snappy, zstd(1), zstd(3)), and row-group size; p99 latency spikes during rotation; memory as open partitions grow. This settles the compression defaults. Criterion: [OPEN].
- [ ] **Type-flapping benchmark:** file count with and without lock-after-first-file.
- [ ] **Schema fingerprint cost** per shape. The fingerprint itself is implemented and snapshot-tested for stability in PR13 (see "PR13 implementation decisions"); its cost was not recorded here.
- [ ] **Clock tests:** clock-step and RTC-less boot (an `unsynced/` partition, then recovery), and the file partition key rule for hour-straddling frames.
- [ ] **Finalize failpoint matrix** (`segments::after_rename_before_catalog`, `segments::before_cursor_commit`): exactly one file per WAL range, none missing.
- [ ] **Field-ID round-trip test** through the writer and a reader, and a check that a downstream Iceberg reader or writer accepts the IDs **[V]**. **O5 closed** with a recorded policy (PR13, "PR13 implementation decisions"). The Arrow IPC round trip is tested in `thadam-arrow`; the Parquet writer and reader round trip and the Iceberg check remain for PR14.
- [ ] **Multi-file reads** across additive schema changes in DuckDB and pyarrow.
- [ ] **Unsigned-integer behavior** per sink, including reader support **[V]**. The default is decided (`Native`, with a `Signed` option); reader support is still unverified.
- [x] **O4 decision** on `_wal_record_idx`: not written; field ID 13 reserved (PR13). Its storage cost (B6) is only needed if the column is added.
- [x] **Owner decision B3** on wide versus narrow layout (wide) and quality granularity (event-level): recorded 2026-10-07 in `docs/design/owner-decisions.md`.

## Revisit when

- User query patterns favor a narrow layout.
- The reader matrix shows problems with microsecond timestamps.
- Iceberg or Delta work (P2) starts, which would reopen the field-ID policy and the key layout.
- The WAL record index needs to change (ADR-0004), or a reader needs a row mapped back to its record inside a frame (the `_wal_record_idx` column, field ID 13, is reserved but not written).
- A reader needs per-device partition pruning that statistics cannot provide.

## References

- ARCHITECTURE.md sections 3 (D11 to D14, D16, D18), 4, 7, 8, 19, 20, 21 (PR13, PR14), 22 (O3, O4, O5).
- ADR-0001 (canonical model and time), ADR-0003 (chains), ADR-0004 (WAL positions and epoch), ADR-0011 (commit-then-ship, claims), ADR-0012 (configuration), ADR-0020 (license and dependency policy).
- `product.md` sections 5, 13.
- Design history: architecture review sections 8, 9, 10, 14 (D11 to D16); Rust implementation plan sections 5.6 and 5.7; edge-capture demo plan sections 5, 7, 8; ecosystem maturity review sections 7, 8, 15 (item 3), as checked 2026-10-07.
- Apache Iceberg and Parquet specifications for field IDs and schema evolution rules.
