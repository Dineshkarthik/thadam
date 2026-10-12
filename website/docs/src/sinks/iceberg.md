# Apache Iceberg registrar

**Status: In progress (step 1; experimental, P2).** The `iceberg` kind registers already-shipped Parquet segments in Apache Iceberg tables, using the REST catalog. It is configured as a `sinks:` entry and runs as a service (the registrar), consuming the segment catalog of a [`parquet`](parquet.md) sink — normally the same sink an [`object_store`](object-store.md) shipper ships. It is built only with `thadam --features iceberg` (off by default: it adds a second Arrow/Parquet dependency tree).

## How it works

A segment reaches a table only through the same commit-then-ship chain as [object storage](object-store.md#how-it-works):

1. The `parquet` sink finalizes a segment and the WAL cursor commits (D18).
2. The `object_store` shipper uploads the file and records a durable `Uploaded` catalog entry with the object key.
3. The registrar sees the `Uploaded` entry and **registers that existing file** with its dataset's table using `fast_append`. The Parquet files already carry stable field IDs (see [Parquet segments](parquet.md)), so no data file is rewritten — the append is metadata-only (manifests plus table metadata JSON).

### Tables

One table per dataset, named `{namespace}.{dataset}`, unpartitioned, format version 2. The table location is `{url}/thadam/v1/{pipeline}/{dataset}` — the same directory the shipper owns. With `create_table: false` (the default) the table must exist: on first contact the registrar reads a segment footer and checks that every file column's field ID is present in the table schema with an equal or Iceberg-promotable type (`int`→`long`, `float`→`double`); a mismatch is the fatal `sink.iceberg_schema_mismatch`. With `create_table: true` an absent table (and namespace) is created from the footer schema.

### Duplicates, crashes, and retries

Registration is idempotent across service restarts and commit retries. Every registrar commit writes the segment's object key into the Iceberg snapshot summary, and the committed-key set is rebuilt from the table's snapshot summaries at startup and after every commit failure: a key already present is skipped. A crash between the catalog's `Uploaded` record and the commit, or a commit whose response was lost, therefore converges on one data file — the Docker test restarts the registrar after a commit and asserts the table still has exactly one snapshot and one data file. Concurrent writers are handled optimistically: a commit conflict makes the registrar reload the table, re-check the committed set, and rebuild the append before retrying.

## Configuration

```yaml
sinks:
  - id: local
    kind: parquet
    config: { datasets: [telemetry], ... }
  - id: s3
    kind: object_store
    config: { from: local, pipeline: edge-capture, url: s3://warehouse/lake, ... }
  - id: iceberg
    kind: iceberg
    config:
      from: local                            # the segment sink id (same as the shipper's `from`)
      pipeline: edge-capture                 # must match the shipper's `pipeline`
      catalog_uri: http://iceberg-rest:8181  # Iceberg REST catalog
      url: s3://warehouse/lake               # bucket + prefix (same semantics as the shipper's)
      namespace: robotics                    # dotted names allowed (a.b -> two levels)
      create_table: false                    # default: tables must exist and match
      store:                                 # S3 access for metadata, manifests, footer reads
        endpoint: http://seaweedfs:9000
        region: us-east-1
        allow_http: true
        access_key_id: "${file:/run/secrets/key_id}"
        secret_access_key: "${file:/run/secrets/key_secret}"
      retry: { initial: 1s, max: 60s, jitter: true }
```

Notes:

- `catalog_uri` is the Iceberg REST catalog endpoint; HTTP in demos, HTTPS elsewhere (rustls + `ring`, the process-wide provider, like every other thadam sink).
- `url` gives the bucket and key prefix. Tables are created at `{url}/thadam/v1/{pipeline}/{dataset}`; data-file URIs come from the catalog's recorded object keys, never from string templates.
- Credentials in `store` arrive already resolved by `${env:}`/`${file:}` ([secrets](../configuration/index.md#secrets)) and are never logged.
- Created tables carry the segment footers' own field IDs. With `create_table: true` thadam authors the initial `v1.metadata.json` itself and registers it, because Iceberg reassigns IDs at REST `create_table` (verified against `apache/iceberg-rest-fixture:1.10.1`).

### The unsigned policy matters

Iceberg has no unsigned types. The default `unsigned: native` Arrow policy writes `UInt64` lineage columns (`_wal_epoch`, `_wal_pos`), which the Iceberg schema conversion rejects — the dataset then fails with the fatal `sink.iceberg_unsupported_type`, naming the fix: run that dataset's Parquet sink with the `unsigned: signed` policy (see [Parquet segments](parquet.md)). With `signed`, no unsigned column exists and every dataset is registrable.

## Integration tests

Docker-gated tests run a real pipeline: a real Parquet segment is shipped to SeaweedFS (S3) and registered in the `apache/iceberg-rest-fixture` REST catalog, then scanned back through the Iceberg Rust client:

```text
cargo nextest run -p thadam-iceberg --run-ignored only -E 'binary(docker_iceberg)'
```

Images are public Docker Hub images pinned by tag (`chrislusf/seaweedfs:3.81`, `apache/iceberg-rest-fixture:1.10.1`; re-pin with `THADAM_TEST_S3_IMAGE` / `THADAM_TEST_ICEBERG_REST_IMAGE`). They assert row counts, field IDs (the table keeps the file's own IDs), a read-back of column values, that a restarted registrar appends nothing twice, and that a pre-existing mismatched table fails with `sink.iceberg_schema_mismatch`. Two fixture notes: SeaweedFS runs with S3 auth enabled (its 3.81 gateway rejects classic aws-chunked writes with auth off and cannot unwrap SDK v2 trailing-checksum writes), and the fixture runs with `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` for that reason.

## Limits (step 1)

- **Schema evolution is step 2.** The schema is checked once per dataset per process; widening a dataset's declared fields requires evolving the table out-of-band (or a fresh table) until step 2 lands.
- Tables are unpartitioned (partition transforms come later; file skipping uses manifest record counts).
- Column metrics on data files are not populated (row counts only).
- Datasets must use the `unsigned: signed` Arrow policy (see above).
- One table-write path per table (one registrar service; multiple nodes appending to the same table is untested).
- Data files and table metadata must live in the configured bucket.
- Table maintenance (compaction, snapshot expiry, data deletion) is out of scope.

## Delivery class

Object-store shipping is unchanged (see [object storage](object-store.md#delivery-class)). On top of it, the registrar adds no duplicate data files across crashes, restarts, and commit retries (see above), and no data-file deletion: every uploaded segment file stays referenced by a snapshot once committed.
