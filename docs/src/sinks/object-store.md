# Object storage sink

**Status: Implemented (PR16).** The `object_store` kind ships finalized Parquet segments to S3-compatible object storage. It is configured as a `sinks:` entry and runs as a service (the shipper), consuming the segment catalog of a [`parquet`](parquet.md) sink. Targets: Amazon S3, MinIO, and S3-compatible private object stores over one generic transport (the cloud-provider principle: one transport, many stores — not a thin sink per vendor).

## How it works

A file reaches the store only through the commit-then-ship chain (the correctness invariant behind ["exactly one finalized file per WAL range"](../concepts/delivery-guarantees.md)):

1. The `parquet` sink finalizes a file (`close + SHA-256`, `fdatasync`, rename into `ready/`, directory fsync, `Finalized` catalog record), the runtime durably commits the WAL cursor past it, and only then the sink appends an `Eligible` record (D18).
2. The shipper's startup **capability probe** writes a probe object: does the store enforce `If-None-Match: *`, does it return user metadata, and how far is the store clock from the local clock. Results are logged; a skew over five minutes warns and names `thadam_clock_synced`.
3. Eligible files upload **oldest first** with `upload_concurrency` in-flight.
4. After each upload the catalog gets an `Uploaded` record, then **local retention** deletes local files per `local_retention` (never before upload).

## Configuration

```yaml
sinks:
  - id: local
    kind: parquet
    config: { datasets: [telemetry], ... }
  - id: s3
    kind: object_store
    config:
      from: local                          # the segment-sink id whose catalog to ship
      pipeline: edge-capture               # written into every object key
      url: s3://thadam-demo/fleet           # bucket + prefix
      endpoint: http://minio:9000          # S3-compatible stores; omit for AWS S3
      region: us-east-1
      allow_http: true                     # plain HTTP endpoint (local demos only; required for http:// endpoints)
      access_key_id: "${file:/run/secrets/key_id}"
      secret_access_key: "${file:/run/secrets/key_secret}"
      upload_concurrency: 2                # default
      multipart_threshold: 32MiB           # default; single conditional puts at or below
      part_size: 8MiB                      # default; 5MiB..5GiB (S3 multipart part limits)
      conditional_put: auto                # auto | create | overwrite (default auto)
      retry: { initial: 1s, max: 60s, jitter: true }   # defaults; attempts are unbounded
      local_retention: { mode: delete_after_upload, keep_for: 30m }
```

Notes:

- `url` carries bucket and key prefix (`s3://bucket/prefix`); keys land under `{prefix}/thadam/v1/{pipeline}/...`.
- Credentials are plain strings **already resolved** by `${env:}`/`${file:}` interpolation ([secrets](../configuration/index.md#secrets)); the shipper wraps them in a redacting type and never logs them.
- `retry` is an outer per-file backoff on top of the transport's own inner retries, and **never gives up** — uploads stay visible through the metrics/log fields below.
- `local_retention.keep_for` is required with `mode: keep_for` and accepted (ignorable) elsewhere.

## Keys, uploads, and conflicts

- **Object key:** `{prefix}/thadam/v1/{pipeline}/{dataset}/date=YYYY-MM-DD/hour=HH/{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet` (or `unsynced/` when `clock_synced` was false for the rows). Hive-style, so DuckDB, Trino, Athena, and Spark query the prefix directly. The random **epoch** (created when the data directory is initialized) prevents collisions after a data-dir wipe.
- **Object metadata:** every put carries `thadam-sha256`, `thadam-wal-range`, `thadam-dataset`, and `thadam-node` user metadata. Objects appear atomically, so readers never see partial objects; orphaned multipart uploads rely on the bucket lifecycle rule (Thadam never lists them).
- **Upload:** at or below `multipart_threshold` a single put; above it, multipart with `part_size` chunks, at most `upload_concurrency x part_size` in memory, aborted cleanly on failure.
- **Create-if-absent:** `auto` probes and falls back to overwrite where conditional put is not enforced (safe only because finalized files are immutable and keys deterministic); `create` refuses to start when unsupported; `overwrite` skips verification deliberately. Stores without conditional writes rely on deterministic immutable keys plus HEAD size + `thadam-sha256` verification; a collision there cannot be refused, it can only be detected after the fact — SeaweedFS is one such store: its 3.81 S3 gateway answers a create-mode PutObject over an existing key with `200 OK` (silently replacing the object) where real S3 answers `412 Precondition Failed`, so `auto` runs overwrite puts against it.
- **Conflict:** an AlreadyExists response (or a pre-existing object found before a multipart upload) triggers verification by size and `thadam-sha256`. Equal is success — the expected path after a crash between upload and catalog update. Different is the **Fatal** [`sink.object_key_collision`](../operations/error-codes.md): epoch or node-id misconfiguration; restart after fixing the data directory or prefix.
- **Clock skew:** a `RequestTimeTooSkewed` response is transient with a warning; devices without an RTC fail signed requests until the clock syncs.

## Integration tests

Docker-gated tests run the shipper against a real S3 API (SeaweedFS; MinIO stopped publishing community Docker Hub images in late 2025):

```text
cargo nextest run -p thadam-object-store --run-ignored only -E 'binary(docker_s3)'
```

The image is a public Docker Hub one pinned by tag, `chrislusf/seaweedfs:3.81` (never a mirror); re-pin with `THADAM_TEST_S3_IMAGE`. Per above, SeaweedFS does not enforce `If-None-Match`, so the collision test asserts the probed behavior per capability: the Fatal-`sink.object_key_collision` path when the store enforces conditional writes, the documented overwrite fallback (and that the final object is exactly the segment payload) when it does not. The always-fatal guarantee on stores with conditional writes is pinned by the crate's in-memory tests regardless. Set `RUST_LOG=debug` for shipper logs during the run.

## Local retention modes

| Mode | Meaning |
|---|---|
| `delete_after_upload` | Delete the local file once uploaded (and the WAL cursor has passed it, which eligibility already implies). |
| `keep_for` | Keep uploaded files for `keep_for` from their upload time. |
| `keep_until_pressure` | Keep until the segment disk budget is tight; eviction order: uploaded and expired, then uploaded. |

Files that were never uploaded are never deleted by these passes.

## Replay isolation

Replays must use an isolated prefix (`.../replays/{replay_id}/`) or a separate bucket — never the production prefix — so a replay experiment cannot contaminate the real dataset.

## Metrics and status

Progress shows up in logs as structured fields named after the planned Prometheus metrics (`thadam_ship_uploads_total`, `thadam_ship_bytes_total`, `thadam_ship_retry_age_seconds`, `thadam_ship_upload_seconds`), the service component state in `/status`, and a `backlog` of eligible segments. The typed runtime metric channel for services lands separately; [metrics.md](../operations/metrics.md#planned) keeps `thadam_ship_*` in the planned list until then.

## Delivery class

Per the [claims register](../concepts/delivery-guarantees.md): Parquet to object store is at-least-once *attempts*. Each object is **effectively written once**: deterministic keys and immutable files, with create-if-absent where the store supports it (the probe reports support; the overwrite fallback is safe only for immutable files). A key with different content is the Fatal `sink.object_key_collision`, never a silent overwrite.
