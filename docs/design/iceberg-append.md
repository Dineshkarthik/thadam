# Iceberg append, step 1: register shipped segments with `fast_append`

- **Status:** Implementation note (not an ADR; step 1 of the P2 table-format work planned in `product.md` and anticipated by ADR-0013). Step 2 will own schema evolution.
- **Date:** 2026-10-12
- **Crate:** `crates/thadam-iceberg` (new), exposed by `thadam-cli` as `kind: iceberg` behind the default-off `iceberg` feature.

## Where the append happens

Immediately *after* a segment is durably uploaded, and never before. The durable
order is owned by the existing machinery, not by this crate:

1. The `parquet` sink finalizes a segment, the WAL cursor commits, the segment
   becomes `Eligible` (D18).
2. The `object_store` shipper uploads the file and appends an fsynced
   `Uploaded` record (object key, etag) to the segment catalog.
3. **This crate**: the Iceberg registrar, a `kind: iceberg` service configured
   under `sinks:` with `from:` naming the same segment sink, watches the same
   `SegmentCatalog` and registers every `Uploaded` segment's file with the
   table using the `iceberg` crate's `fast_append` — Parquet field IDs are
   already on disk (ADR-0013), so there is **no rewrite of Parquet data**.

A service-over-catalog (not a hook inside the shipper) is the smallest correct
placement: crash recovery comes for free, because the replay target is "every
`Uploaded` segment not yet committed" — readable from the catalog at startup.
A hook would need an equivalent replay path of its own. The shipper keeps no
knowledge of Iceberg and stays in the default binary; the registrar rides only
the `iceberg` feature.

Supporting change in `thadam-segments`: the in-memory catalog state retains the
uploaded object key for `Uploaded` segments and for `Deleted { uploaded: true }`
ones (local retention may delete the local copy before the registrar runs; the
S3 object and the key are still valid), and `SegmentCatalog::uploaded()`
returns `(SegmentRecord, key)` pairs, oldest WAL range first. No on-disk change:
the key was already in the `Uploaded` log record.

## Table identity and configuration

`kind: iceberg` (a `ServiceFactory`, configured under `sinks:` like
`object_store`):

```yaml
- id: iceberg
  kind: iceberg
  config:
    from: local                 # segment sink id (same as the shipper's)
    pipeline: edge-capture      # written into the table location; must match the shipper's
    catalog_uri: http://iceberg-rest:8181
    url: s3://warehouse/lake    # bucket + prefix; same semantics as the shipper's `url`
    namespace: robotics         # dotted names allowed (a.b -> two levels)
    create_table: false         # default: tables must exist and match
    store:                      # S3 access for metadata/manifests and data-file reads
      endpoint: http://seaweedfs:9000
      region: us-east-1
      allow_http: true
      access_key_id: ${file:/run/secrets/key_id}
      secret_access_key: ${file:/run/secrets/key_secret}
    retry: { initial: 1s, max: 60s, jitter: true }
```

- One table per dataset: table name = dataset name (validated as an Iceberg
  identifier), inside `namespace`. A multi-dataset segment sink produces one
  table per dataset.
- `url` (`s3://bucket/prefix`) gives the store bucket plus key prefix, exactly
  like the shipper. The REST catalog is configured with `warehouse` =
  `s3://bucket`; every table is created with an explicit
  `location = {url}/thadam/v1/{pipeline}/{dataset}`, the same directory the
  shipper owns. Data-file URIs are `s3://{bucket}/{uploaded-key}` and come from
  the catalog record, never from string templates, so a config drift cannot
  fabricate a path.
- `create_table: false` (default): the table must exist; on first contact the
  registrar reads one segment's Parquet footer (ranged tail read through the
  table's `FileIO`), converts its Arrow schema (field IDs included) to an
  Iceberg schema, and checks assignability against the table's current schema:
  every file field ID must exist with a type equal to or promotable from the
  file type (`int`→`long`, `float`→`double`; names may differ — IDs win; a
  nullable file field against a required table field is a mismatch). Failure is
  Fatal `sink.iceberg_schema_mismatch`. `create_table: true`: if the table is
  absent the schema is derived the same way, the namespace is created if
  needed, and the table is created (format v2, unpartitioned) — by **client-authored
  initial metadata plus `register_table`**, not the REST `create_table`
  endpoint. Reason: Iceberg assigns fresh sequential field IDs at create time
  (verified against `apache/iceberg-rest-fixture:1.10.1`, which turned
  `[2, 1000]` into `[1, 2]`; iceberg-rust's own `TableMetadataBuilder` does
  the same), which would orphan the stable Parquet field IDs ADR-0013
  guarantees and leave data files unreadable-by-id. So thadam authors the v1
  metadata JSON with the file's own IDs, writes it through its FileIO, and
  asks the catalog to `register_table` it. The schema check runs once per
  dataset per process, not per file.

## Schema mapping

The Parquet file footer is the single source of truth (it carries the Arrow
schema with `PARQUET:field_id` metadata, put there by `thadam-arrow`). The
registrar reads the footer with the `parquet` 58 crate inside the `iceberg`
dependency tree and converts with `iceberg::arrow::arrow_schema_to_schema`,
which honors those field IDs. No hand-maintained copy of the envelope/declared
mapping lives in this crate, and no Arrow types cross the workspace boundary:
the workspace builds Arrow 60 while `iceberg` builds Arrow 58, so feet-on-the-
ground the conversion can only live inside the 58 tree.

Unsigned types (ADR-0013 default `Native`): Iceberg has none. The conversion
maps `UInt8/16`→`int` and `UInt32`→`long` (zero-extension on read; values fit)
and **rejects `UInt64`** — which the default envelope has (`_wal_epoch`,
`_wal_pos`). Consequence for step 1: datasets registered with Iceberg must run
the `unsigned: signed` Arrow policy (then no unsigned column exists and the
map is total); an `unsigned: native` dataset fails schema conversion with a
Fatal error naming the column and the remedy. This matches ADR-0013: `Signed`
"is for lakes read by engines or table formats without unsigned types".

## Exactly-once across crashes and retries

Iceberg commits are single-writer/optimistic; the registrar is the only writer
it controls and runs sequentially (oldest first, one segment per commit). The
idempotence key is the deterministic object key.

- Every registrar commit sets snapshot summary properties
  `thadam.segment = <object key>`, `thadam.dataset`, `thadam.wal_range`.
- Before committing (at startup and after each table reload) the registrar
  scans the table's snapshot summaries and collects the committed key set; a
  key already present is skipped. The crash windows — after catalog `Uploaded`
  but before commit, or after a commit that succeeded but whose response was
  lost — converge to "the summary either has the key or it doesn't".
- The `fast_append` commit carries Iceberg's optimistic requirements
  (`assert current snapshot id`), so a conflict means another writer committed
  first. The registrar then **reloads the table**, rebuilds the committed set,
  skips if its key is now present, otherwise rebuilds the append on the fresh
  base and retries (bounded, jittered; still conflicting => Transient error and
  the next catalog tick retries). This is the race with concurrent writers:
  conflicts never duplicate a file because the check runs on the fresh table
  state inside each attempt.
- REST fixture / real catalogs: HTTP 409 maps to `CatalogCommitConflicts` in
  iceberg-rust 0.10.1.

## Partitioning

Unpartitioned (spec id 0), the smallest option. Thadam's Hive-style
`date=/hour=` directories are not an Iceberg partition spec (identity on
`_ingest_time` has the wrong granularity and the unsynced-copy case does not
fit one column). File skipping still uses the manifest-level record counts;
column min/max metrics are not populated in step 1 (see limits).

## Dependency choice (audit summary)

- `iceberg = =0.10.1` and `iceberg-catalog-rest = =0.10.1` (both Apache-2.0;
  rust-version 1.94 — the crate declares `rust-version = "1.94"`; the pinned
  1.99 toolchain builds it, and the default-off feature keeps it out of plain
  binaries). Both crates ship no TLS default (reqwest 0.12,
  `default-features = false`); `thadam-iceberg` adds reqwest 0.12 with
  `rustls-tls-native-roots-no-provider` so the crypto provider is the process
  default `ring` that `thadam-cli` installs (ADR-0020; mirrors the reqwest 0.13
  `rustls-no-provider` pattern of `thadam-object-store`).
- `iceberg-storage-opendal` is **rejected**: through opendal 0.57 it resolves
  reqwest 0.13's default rustls stacks (`hyper-rustls`,
  `rustls-platform-verifier`) and pulls `aws-lc-sys`, which `deny.toml` bans,
  plus a second HTTP stack. Instead `thadam-iceberg` implements iceberg's
  `io::Storage`/`StorageFactory` (eleven small async methods) over the
  workspace's audited `object_store` 0.14 S3 stack; the REST catalog receives
  it through `RestCatalogBuilder::with_storage_factory`.
- Cost, measured: **95 net-new crates ≈ 52 MiB of crate sources**, of which
  11 are the duplicate Arrow-58 family (`arrow-arith/array/buffer/cast/data/
  ipc/ord/schema/select/string` v58.4.0 + `parquet` v58.4.0; the workspace
  builds 60) plus reqwest 0.12.28 next to 0.13. The `iceberg` feature is off
  by default in `thadam-cli` for exactly this reason.
- `iceberg` 0.10.x has no S3-ready storage out of the box: the core ships
  memory/fs `FileIO`, the S3 story is `iceberg-storage-opendal` (rejected
  above). The crate was pinned by deleting-features of everything else: the
  remaining tree is unavoidable.

## Error classification

- **Transient (retry forever, visible):** catalog unreachable / timeout /
  5xx / 429, S3 IO failures, commit conflicts past the bounded retry.
- **Fatal:** auth (401/403, store `Unauthenticated`/`PermissionDenied`), the
  named table missing with `create_table: false`, schema mismatch, unsupported
  footer type (e.g. `UInt64` under `unsigned: native`), configuration errors.

## Test-harness discoveries (SeaweedFS + REST fixture)

These cost half a day to find; they are written down so the next person does
not pay it again:

- **AWS SDK v2 trailing checksums vs SeaweedFS 3.81.** The Java REST fixture
  writes `v1.metadata.json` server-side. With the SDK's default
  (`when_supported`) request-checksum mode the writes arrive as
  `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` aws-chunked, which SeaweedFS
  3.81 does not unwrap: it stores the chunk-framing envelope (`28d;chunk-signature=…`)
  as the object, and the fixture then 500s parsing its own metadata. Retry
  rule: `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` (and the matching
  response env) on the fixture makes it write plain or classic-chunked PUTs.
- **SeaweedFS 3.81 needs S3 auth *enabled* for classic aws-chunked.** With
  auth off it rejects `authTypeStreamingSigned` PUTs outright
  (`ErrAuthNotSetup`); with a `-s3.config` static identity it verifies then
  unwraps them. The harness therefore mounts a small identity file and the
  bucket-creation SigV4 must be a *valid* signature (a broken date helper —
  missing day-of-year math — produced month 30/day 82 and was chewed on for
  a while; an auth-off store never notices). The same latent bug lives in
  `thadam-object-store`'s `docker_s3` copy of `amz_times` and surfaces the
  day anyone flips auth on there.

## Deliberately out of scope (step 2 and later)

- Schema evolution (evolve the table schema when a dataset gains fields —
  step 2; today additive fields surface only via `create_table` mismatch on a
  fresh table, and existing tables must be evolved out-of-band).
- Compaction / snapshot expiry / data deletion; replay isolation for Iceberg
  (replays must still not point at a production table).
- Populating column metrics on data files (`shortcut:` bounds checks could
  come from the Parquet footer stats the writer already emits).
- Delete vectors, merge-on-read, distributed/cluster commits (one registrar
  per table-write path), and Iceberg-side handling of datasets with
  `unsigned: native`.

### `shortcut:` limits carried in code

- One catalog schema check per dataset per process; a dataset whose Parquet
  layout changes mid-run is only re-validated on restart (step 2 re-derives
  per file).
- The committed-key set is rebuilt by scanning snapshot summaries at load;
  unbounded snapshot history makes that scan grow (persist a local committed
  cursor when it hurts).
- FileIO supports exactly one bucket (metadata and data in the `url` bucket;
  a per-bucket store map is the upgrade).
- `create_table` authors metadata with the file's own field IDs rather than
  going through REST `create_table`; catalogs that cannot `register_table`
  cannot be used with `create_table: true` (hand-create the table instead,
  matching the footer field IDs, and keep `create_table: false`).
- Column metrics on data files are empty maps (populate from footer stats).
