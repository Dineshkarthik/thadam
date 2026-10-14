# Parquet segments sink

**Status: working.** The `parquet` kind builds, runs, and is covered by the segment-sink conformance suite (monotonic watermark, crash recovery at every finalize failpoint, dataset isolation). Delivery to object storage rides on these segments and is still in progress ([object storage sink](object-store.md)).

## What it does

A segment sink has no network dependency: it turns each dataset's events into compressed columnar files on local disk, finalizes them safely, and records them in a crash-recoverable catalog. Object storage delivery then ships those files.

## Where the schema comes from

Datasets — fields with their Parquet field IDs, identity levels with theirs, and `explode` — are declared **once** in the pipeline-level [`datasets:`](../configuration/datasets.md) section. The runtime hands the declarations to the sink through its build context, restricted to the ids in the sink's `config.datasets` list, and the sink turns them into its per-dataset schemas. There is no schema declaration inside the sink's `config:` block: an in-sink `schemas:` key from an older draft is an error that points back to `datasets:`. Because the runtime reads the same `datasets:` selector to route frames, the sink's schemas can never disagree with its chain.

```yaml
datasets:
  - id: telemetry
    match: { source: fleet-mqtt, topic: "robots/+/telemetry" }
    decode: json
    identity: { robot: { id: 100, template: "{topic.1}" } }
    fields:
      - { id: 1000, name: seq,     type: u64, from: /seq, required: true }
      - { id: 1001, name: temp_c,  type: f32, from: /temp_c, unit: Cel }

sinks:
  - id: local
    kind: parquet
    config:
      datasets: [telemetry]              # selector; empty or absent consumes every declared dataset
      compression: auto                  # zstd(1) on x86_64, snappy on aarch64 (pending benchmark B6)
      row_group_bytes: 8388608           # 8 MiB (edge profile; integer bytes)
      rotation: { max_bytes: 134217728, max_age: 300 }   # 128 MiB or 300 s (production default; demo uses 60)
      max_open_files: 4                  # LRU-finalize the least recently written file beyond this
      max_disk: 4294967296               # 4 GiB per-sink disk budget for local segments (integer bytes)
      timestamp_unit: micros             # micros default; nanos opt-in per sink
      unsigned: native                   # or signed: widen to the next signed type
      finalize_on_shutdown: true
```

## Datasets and layout

- **One table per dataset.** Device identity is a column, never a partition — 500 robots must not become 500 open writers (D11). Open writers are capped by `max_open_files`; the least-recently-written file is finalized early when the cap is hit.
- **Ready path:** `ready/<dataset>/date=YYYY-MM-DD/hour=HH/{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet`, with date/hour from `_ingest_time`. `{node_id}` is the pipeline's [`node_id`](../configuration/pipeline.md) unless the sink's `config.node_id` overrides it, so files from nodes with different `node_id`s never share a name; `first` and `last` are the inclusive WAL positions covered. Files written while `clock_synced` was false land under `ready/<dataset>/unsynced/` instead (D16). Open segments live in `staging/<dataset>/<seq>.inprogress` and are renamed into `ready/`.
- **Footer metadata per file:** `thadam.version`, `thadam.dataset`, `thadam.schema_fp`, `thadam.wal_range`, `thadam.config_hash`, `thadam.enrichment_hash`, `thadam.clock_synced`, plus min/max `event_time` statistics — the lineage inputs for replay equivalence. `thadam.config_hash` is the pipeline's [config hash](../configuration/index.md#config-hash-and-identity), the value `thadam validate` prints; the sink's `config:` block cannot set it. `thadam.enrichment_hash` is the SHA-256 over the sorted `dataset sha256` lines of the enrichment tables the sink's chain loaded, where each `sha256` is the digest of that table's file; it is empty when the chain loads none. Quarantine files carry the same two values as the sink they belong to.
- **Timestamps:** microsecond by default (D14), nanoseconds opt-in per sink. Reader compatibility across DuckDB, pyarrow, Spark, Trino, and Athena is a validation item.
- **Writer settings:** row groups sized by bytes (8 MiB edge profile), dictionary encoding on low-cardinality strings, statistics on time columns, bloom filters off, `created_by = "thadam <version>"`.
- **Rotation:** first of `max_bytes`, `max_rows`, `max_age`, or shutdown.

## Commit-then-ship (the correctness invariant)

The finalize sequence, D18 in ADR-0011/0013, fixed a duplicate-object defect in the original design:

1. Close the Parquet writer (computing SHA-256), `fdatasync` the file.
2. Rename from `staging/` to `ready/`, fsync the directory, append a `Finalized` record to the catalog.
3. The runtime commits the WAL cursor to the new watermark **durably**.
4. Only then is the file marked `Eligible` — the shipper never uploads an ineligible file.
5. Local deletion requires both an `Uploaded` record **and** a cursor at or beyond the file's end.

Net effect, matched to the [delivery guarantees](../concepts/delivery-guarantees.md): each WAL range lands in exactly one finalized file, and recovery always knows which files may be shipped. A dataset whose segment closes with zero rows writes no file — a *coverage marker* goes into the catalog instead, so the watermark keeps moving and recovery can tell "empty" from "lost".

## Segment catalog

An append-only, CRC-checked `catalog.log` with `Finalized`, `Eligible`, `Uploaded`, and `Deleted` records — the pending-upload queue lives on disk, not in memory. At startup, staging files are removed, the log is replayed, and ready files the log lost track of are read for their footer `thadam.wal_range` and appended back; everything beyond the watermark is dropped, so rows never appear twice after a crash.

## Schema evolution and field IDs

Every column carries a stable Parquet field ID from the first release ([field ID policy](../configuration/datasets.md#field-ids)). Evolution is Iceberg-compatible: add optional columns, widen `int`→`long` and `float`→`double`, never rename or narrow; anything else is a new dataset version. Unknown input fields are quarantined (strict), dropped-and-counted (lenient), or captured into an optional `_unmapped` column. `schema.narrowing` is the error for an unrepresentable conversion.

## Unsigned integers

Unsigned values follow a per-sink unsigned policy: `Native` (default — `UInt8`…`UInt64`) or `Signed` (next wider signed type; `u64` stays `Int64` and values above `i64::MAX` are rejected). Reader support for unsigned logical types is still a validation item (`[V]` in ADR-0013).

## Quarantine format

The same segment machinery writes [quarantine](../concepts/per-sink-chains.md#quarantine) files with a fixed schema and their own disk budget (see [`quarantine`](../configuration/wal.md#quarantine)).

A quarantine segment does not hold back the sink that owns it. Rejects are fsynced into a staging file first, which is what releases the sink's cursor; the quarantine segment then rotates on `quarantine.rotation` (default 5 minutes or 128 MiB) and ships on its own, and a reject staged but not yet in a durable segment at crash time is recovered into the next quarantine segment exactly once. A quarantine segment becomes eligible for shipping once both the sink's cursor and the quarantine's durable position have passed it.
