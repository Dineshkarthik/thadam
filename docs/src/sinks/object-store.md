# Object storage sink

**Status: In progress.** The `object_store` kind is registered and configurations validate, but — like [`parquet`](parquet.md) — the factory is a placeholder today. This page describes the **planned** shipper behavior (ARCHITECTURE.md section 8, [demo pipeline](https://github.com/Dineshkarthik/thadam/blob/main/examples/edge-capture/thadam/pipeline.yaml) sketch), not runnable code. Targets: Amazon S3, MinIO, and S3-compatible private object stores over one generic `ObjectStore` abstraction (the cloud-provider principle: one transport, many stores — not a thin sink per vendor).

## What it will do (planned)

The shipper is a *service*, not a sink in the delivery path: it consumes finalized, cursor-committed segment files from the [segment catalog](parquet.md#segment-catalog-planned) and uploads them. Nothing reaches the store that is not eligible (commit-then-ship), which is what makes each object effectively written once.

Planned configuration surface (demo sketch):

```yaml
sinks:
  - id: s3
    kind: object_store
    config:
      from: local                          # the parquet sink whose segments to ship
      url: s3://thadam-demo/fleet           # bucket + prefix
      endpoint: http://minio:9000          # for S3-compatible stores
      region: us-east-1
      allow_http: false                    # true only in local demos
      access_key_id: "${file:/run/secrets/key_id}"
      secret_access_key: "${file:/run/secrets/key_secret}"
      upload_concurrency: 2
      multipart_threshold: 32MiB
      part_size: 8MiB
      conditional_put: auto                # probe at startup; fall back to overwrite (safe: immutable files)
      retry: { initial: 1s, max: 60s, jitter: true }
      local_retention: { mode: delete_after_upload, keep_for: 30m }
```

## Keys, uploads, and conflicts (planned)

- **Object key:** `{prefix}/thadam/v1/{pipeline}/{dataset}/date=…/hour=…/{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet`. Hive-style, so DuckDB, Trino, Athena, and Spark can query the prefix directly. The random **epoch** (created when the data directory is initialized) prevents key collisions if a data directory is wiped and WAL positions restart.
- **Upload:** oldest first with `upload_concurrency`. Files at or below `multipart_threshold` use a single conditional put; larger files use multipart with `part_size` chunks. Objects appear atomically; orphaned multipart uploads are handled by a documented bucket lifecycle rule.
- **Conditional writes:** `create-if-absent` where supported (an `ETagMatch`-style conditional put), probed at startup because support varies by store. On `AlreadyExists`, the shipper HEADs the object and compares size and SHA-256: equal is success (the expected path after a crash between upload and catalog update); different is a fatal `OBJECT_KEY_COLLISION`. Without conditional support it falls back to overwrite — safe only because finalized files are immutable and keys deterministic.
- **Retry:** unbounded per-file backoff with jitter (1 s initial, 60 s cap). Edge uploads never give up and stay visible through metrics; a `RequestTimeTooSkewed` response is transient with a long backoff and a status warning (devices without an RTC fail signed requests until synced).
- **Credentials:** supplied externally (`${env:}` / `${file:}`), wrapped in `Secret<T>`, never logged. The demo bucket policy is least-privilege (`PutObject`, `GetObject`, `AbortMultipartUpload` only).
- **Clock skew:** correlated with `thadam_clock_synced`.

## Local retention modes (planned)

| Mode | Meaning |
|---|---|
| `delete_after_upload` | Delete the local file once uploaded (and the cursor has passed it). |
| `keep_for` | Keep uploaded files for a duration (e.g. `30m`) before deleting. |
| `keep_until_disk_pressure` | Keep until the local budget is tight; eviction order: uploaded-and-expired, then uploaded, then policy order. |

## Replay isolation (planned)

Replayed data is shipped with an isolated prefix (`.../replays/{replay_id}/`) or to a separate directory — never the production prefix, so a replay experiment cannot contaminate the real dataset.

## Delivery class

Per the [claims register](../concepts/delivery-guarantees.md): at-least-once attempts; deterministic keys, immutable files, and create-if-absent make each object effectively written once, with the conditional-put probe result reported rather than assumed.
