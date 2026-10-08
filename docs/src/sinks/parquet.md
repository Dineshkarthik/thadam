# Parquet segments sink

**Status: In progress.** The `parquet` kind is registered and configurations validate, but the sink factory is still a placeholder — `thadam run` cannot build a pipeline containing it yet, and the options below are the **planned** surface, from ADR-0013, ARCHITECTURE.md section 8, and the [demo pipeline](https://github.com/Dineshkarthik/thadam/blob/main/examples/edge-capture/thadam/pipeline.yaml) sketch. Everything on this page describes intended behavior, clearly marked, not code you can run today. This is the M1 P0 sink: it proves Thadam preserves data through outages before any database is involved.

## What it will do (planned)

A segment sink has no network dependency: it turns each dataset's events into compressed columnar files on local disk, finalizes them safely, and records them in a crash-recoverable catalog. Object storage delivery then ships those files ([object storage sink](object-store.md)).

Planned configuration surface (demo sketch; keys may settle as the implementation lands):

```yaml
sinks:
  - id: local
    kind: parquet
    config:
      datasets: [telemetry, joints, imu, pose, state, events]
      compression: auto                    # zstd(1) on x86_64, snappy on aarch64 (pending benchmark B6)
      row_group_bytes: 8MiB                # edge profile
      rotation: { max_bytes: 128MiB, max_age: 5m }   # production default; demo uses 60s
      max_open_files: 4                    # LRU-finalize the least recently written file beyond this
      max_disk: 4GiB                       # per-sink disk budget for local segments
      timestamp_unit: us                   # us default; ns opt-in per dataset
      finalize_on_shutdown: true
```

## Datasets and layout (planned)

- **One table per dataset.** Device identity is a column, never a partition — 500 robots must not become 500 open writers (D11). Open writers are capped by `max_open_files`; the least-recently-written file is finalized early when the cap is hit.
- **Ready path:** `ready/<dataset>/date=YYYY-MM-DD/hour=HH/{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet`, with date/hour from `_ingest_time`. Files written while `clock_synced` was false land under `ready/<dataset>/unsynced/` instead (D16).
- **Footer metadata per file:** `thadam.version`, `thadam.dataset`, `thadam.schema_fp`, `thadam.wal_range`, `thadam.config_hash`, `thadam.enrichment_hash`, `thadam.clock_synced`, plus min/max `event_time` statistics — the lineage inputs for replay equivalence.
- **Timestamps:** microsecond by default (D14), nanoseconds opt-in per dataset. Reader compatibility across DuckDB, pyarrow, Spark, Trino, and Athena is a validation item.
- **Writer settings (planned):** row groups sized by bytes (8 MiB edge profile), dictionary encoding on low-cardinality strings, statistics on time columns, bloom filters off, `created_by = "thadam <version>"`.
- **Rotation:** first of `max_bytes`, `max_rows`, `max_age`, or shutdown.

## Commit-then-ship (the correctness invariant)

The finalize sequence, D18 in ADR-0011/0013, fixed a duplicate-object defect in the original design:

1. Close the Parquet writer (computing SHA-256), `fdatasync` the file.
2. Rename from `staging/` to `ready/`, fsync the directory, append a `Finalized` record to the catalog.
3. The runtime commits the WAL cursor to the new watermark **durably**.
4. Only then is the file marked `Eligible` — the shipper never uploads an ineligible file.
5. Local deletion requires both an `Uploaded` record **and** a cursor at or beyond the file's end.

Net effect, matched to the [delivery guarantees](../concepts/delivery-guarantees.md): each WAL range lands in exactly one finalized file, and recovery always knows which files may be shipped.

## Segment catalog (planned)

An append-only, CRC-checked `catalog.log` with `Finalized`, `Eligible`, `Uploaded`, and `Deleted` records — the pending-upload queue lives on disk, not in memory. At startup, files in `ready/` missing from the log are read for their footer `thadam.wal_range` and appended back.

## Schema evolution and field IDs

Every column carries a stable Parquet field ID from the first release ([field ID policy](../configuration/datasets.md#field-ids)). Evolution is Iceberg-compatible: add optional columns, widen `int`→`long` and `float`→`double`, never rename or narrow; anything else is a new dataset version. Unknown input fields are quarantined (strict), dropped-and-counted (lenient), or captured into an optional `_unmapped` column. `schema.narrowing` is the error for an unrepresentable conversion.

## Unsigned integers

Unsigned values follow a per-sink unsigned policy: `Native` (default — `UInt8`…`UInt64`) or `Signed` (next wider signed type; `u64` stays `Int64` and values above `i64::MAX` are rejected). Reader support for unsigned logical types is still a validation item (`[V]` in ADR-0013).

## Quarantine format

The same segment machinery writes [quarantine](../concepts/per-sink-chains.md#quarantine) files with a fixed schema and their own disk budget (see [`quarantine`](../configuration/wal.md#quarantine)).
