# Apache Iceberg registrar

**Status: In progress (steps 1 to 3; experimental, P2).** The `iceberg` kind registers already-shipped Parquet segments in Apache Iceberg tables, using the REST catalog. It is configured as a `sinks:` entry and runs as a service (the registrar), consuming the segment catalog of a [`parquet`](parquet.md) sink — normally the same sink an [`object_store`](object-store.md) shipper ships. It is built only with `cargo build -p thadam-cli --features iceberg` (off by default: it adds a second Arrow/Parquet dependency tree).

## How it works

A segment reaches a table only through the same commit-then-ship chain as [object storage](object-store.md#how-it-works):

1. The `parquet` sink finalizes a segment and the WAL cursor commits (D18).
2. The `object_store` shipper uploads the file and records a durable `Uploaded` catalog entry with the object key.
3. The registrar sees the `Uploaded` entry and **registers that existing file** with its dataset's table using `fast_append`. The Parquet files already carry stable field IDs (see [Parquet segments](parquet.md)), so no data file is rewritten — the append is metadata-only (manifests plus table metadata JSON).

### Tables

One table per dataset, named `{namespace}.{dataset}`, unpartitioned, format version 2. The table location is `{url}/thadam/v1/{pipeline}/{dataset}` — the same directory the shipper owns. With `create_table: false` (the default) the table must exist. With `create_table: true` an absent table (and namespace) is created from the footer schema. Either way the table's schema is brought up to each segment before that segment is appended (next section).

### Schema evolution

Before appending a segment, the registrar reads that segment's Parquet footer and merges its schema into the table's **by field ID** (the stable IDs written into every file, see [Parquet segments](parquet.md)):

| What the segment has | What happens |
|---|---|
| A field ID the table does not have | The column is **added** as optional, keeping the file's own ID (a required column also lands optional, because older files have no value for it) |
| A known field ID under a new name | The column is **renamed** to the new name (the ID is the identity) |
| A wider type than the table's: `long` over `int`, `double` over `float`, or `decimal(P',S)` over `decimal(P,S)` with `P' > P` | The table column is **promoted** (Iceberg's widening promotions; existing files are read through the wider type) |
| A column the dataset no longer has | The table column **stays** (it must be optional) |
| A narrower type than the table's (`long`→`int`, `double`→`float`, smaller decimal precision) | **Fatal** |
| Any other type change on a field ID, including a primitive turning into a record or list, or a decimal scale change | **Fatal** |
| Optional where the table says required, or required where the table says optional, or no value for a required table column | **Fatal** |
| A new field ID that reuses the name of another column | **Fatal** |

Fatal means the service stops with `sink.iceberg_schema_mismatch`, and the message lists every violation with the column path, field ID, and both types. The table is left untouched and the segment stays in the segment catalog (nothing is dropped); fix the dataset or the table and restart. Evolution is two commits per changed segment, schema first and then the append, ordered so that a crash between them is harmless: on restart the same change is re-derived as "nothing to do" and only the append runs (see the [design note](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/iceberg-append.md)).

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
      catalog_uri: https://iceberg-rest:8181 # Iceberg REST catalog
      catalog_ca_file: /etc/thadam/catalog-ca.pem  # optional: CA behind an https:// catalog
      catalog_token: "${file:/run/secrets/catalog_token}"  # optional bearer token (or catalog_credential)
      catalog_client_cert: /etc/thadam/catalog-client.pem   # optional mutual TLS, with
      catalog_client_key: /etc/thadam/catalog-client-key.pem  # catalog_client_key
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

- `catalog_uri` is the Iceberg REST catalog endpoint; HTTP in demos, HTTPS elsewhere (rustls + `ring`, the process-wide provider, like every other thadam sink). Without `catalog_ca_file` an HTTPS catalog is verified against the platform trust roots. With `catalog_ca_file` (a PEM bundle, `https://` only) the file **replaces** the platform roots for the catalog connection, so only that CA is trusted; the S3 `store` is unaffected. A certificate the registrar does not trust (unknown issuer, expired, wrong name) is a fatal `config.invalid` error that names the TLS cause, not an endless retry; an unreadable or certificate-less CA file fails the same way at startup.
- **Catalog authentication** is one of two options, never both (a config with both is refused). Both apply to every catalog request, including the direct schema commit:
  - `catalog_token`: a static bearer token, sent as `Authorization: Bearer <token>`. Supply it through `${file:}` or `${env:}`; it is never logged. It is not refreshed: when it expires the catalog answers 401 and the registrar stops (below).
  - `catalog_credential`: OAuth2 client credentials, exchanged at the token endpoint with `grant_type=client_credentials`. Keys: `client_id` (no `:`), `client_secret` (use `${file:}` or `${env:}`), optional `scope` (the catalog's default, `catalog`, when absent), and optional `oauth2_server_uri` (the token endpoint; default `{catalog_uri}/v1/oauth/tokens`). The exchange is done by the `iceberg-catalog-rest` client and for the direct schema commits; when an exchanged access token expires and the catalog answers 401, the registrar re-exchanges `catalog_credential` once (rebuilding the catalog client and committer, or re-exchanging directly on a schema commit) and retries immediately before treating a 401/403 from the token exchange or from the retry as fatal `auth.failed`.
  ```yaml
  catalog_credential:
    client_id: thadam
    client_secret: "${file:/run/secrets/catalog_secret}"
    scope: catalog                                  # optional
    oauth2_server_uri: https://auth.example/token   # optional
  ```
  A token or credential the catalog or token endpoint rejects (401 or 403) is a fatal `auth.failed` error that stops the service; it is not retried forever. Unreachable endpoints and 5xx stay transient. Configuring `catalog_token` or `catalog_credential` over a non-loopback `http://` `catalog_uri` (or `oauth2_server_uri`) logs a warning because credentials travel in cleartext.
- **Mutual TLS:** `catalog_client_cert` (a PEM certificate chain) and `catalog_client_key` (its PEM private key) must be set together, need an `https://` `catalog_uri`, and must be existing files (checked at config time). Every catalog connection, including the schema commit, presents that certificate. It combines with `catalog_ca_file` and with either auth option. A catalog that refuses the certificate (none presented, or one from a CA it does not trust) answers with a TLS alert, which is a fatal `auth.failed` error. The key is read at startup and never logged.
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

Images are public Docker Hub images pinned by tag (`chrislusf/seaweedfs:3.81`, `apache/iceberg-rest-fixture:1.10.1`, `caddy:2.10.2`; re-pin with `THADAM_TEST_S3_IMAGE` / `THADAM_TEST_ICEBERG_REST_IMAGE`). They assert row counts, field IDs (the table keeps the file's own IDs), a read-back of column values, that a restarted registrar appends nothing twice, and that a pre-existing mismatched table fails with `sink.iceberg_schema_mismatch`. Schema evolution is tested end to end: one dataset goes from `int` to `long` with a new column and then a rename, the table follows (new column keeps its configured ID, rows older than it read as null, values read back through the promoted type), and a later narrowing to `int` stops the service with the fatal error and leaves the table unchanged. TLS is tested with `caddy:2.10.2` (re-pin with `THADAM_TEST_PROXY_IMAGE`) terminating TLS with a throwaway test CA in front of the REST fixture: the registrar registers segments and commits a schema change over HTTPS with the CA configured, and fails fast and fatally both with a different CA and with no CA. The same proxy tests the authentication options, each with a schema-evolution commit so the direct REST call is covered: a front that requires `Authorization: Bearer <token>` (works with the token; a wrong token is fatal `auth.failed` and registers nothing), a front that requires OAuth2 client credentials (the fixture's own `/v1/oauth/tokens` issues the token; works with the credential, and a token endpoint answering 401 is fatal `auth.failed`), and a front with `client_auth { mode require_and_verify }` (works with a client certificate from the trusted test CA; no certificate and a certificate from another CA are both fatal `auth.failed`). A conflicting second writer is tested in-process and deterministically (a catalog wrapper commits a competing writer's segment just before the registrar's commit): the registrar reloads, skips a segment the other writer already registered, and retries a different one on the fresh table, with no lost and no duplicated data file. Two fixture notes: SeaweedFS runs with S3 auth enabled (its 3.81 gateway rejects classic aws-chunked writes with auth off and cannot unwrap SDK v2 trailing-checksum writes), and the fixture runs with `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` for that reason.

## Limits

- A schema change and the segment's append are two commits (schema first), because the Iceberg Rust client in use (0.10.1) cannot put both in one; the order makes a crash between them safe. The schema commit goes straight to the REST update endpoint, with the same authentication and client certificate as the catalog client.
- Static `catalog_token` is not refreshed: when it expires the catalog answers 401 and the registrar stops with `auth.failed`. With `catalog_credential`, an HTTP 401 triggers one OAuth2 re-exchange and immediate retry before failing with `auth.failed`. Not tested: a real OAuth2 provider (the test token endpoint is the REST fixture's or an in-process mock), token-endpoint client authentication other than `client_id`/`client_secret` in the form body, and managed or cloud catalogs.
- The footer of every not-yet-registered segment is read (two small ranged reads) to derive the schema change.
- Tables are unpartitioned (partition transforms come later; file skipping uses manifest record counts).
- Column metrics on data files are not populated (row counts only).
- Datasets must use the `unsigned: signed` Arrow policy (see above).
- One table-write path per table (one registrar service; multiple nodes appending to the same table is untested).
- Data files and table metadata must live in the configured bucket.
- Table maintenance (compaction, snapshot expiry, data deletion) is out of scope.

## Delivery class

Object-store shipping is unchanged (see [object storage](object-store.md#delivery-class)). On top of it, the registrar adds no duplicate data files across crashes, restarts, and commit retries (see above), and no data-file deletion: every uploaded segment file stays referenced by a snapshot once committed.
