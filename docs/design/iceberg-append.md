# Iceberg append: register shipped segments with `fast_append` (steps 1 and 2)

- **Status:** Implementation note (not an ADR; the P2 table-format work planned in `product.md` and anticipated by ADR-0013). Step 1 registers segments; step 2 adds schema evolution, a concurrent-writer conflict test, and an HTTPS catalog test.
- **Date:** 2026-10-12 (step 1); 2026-10-13 (step 2)
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
    catalog_ca_file: null       # PEM of the CA behind an https:// catalog (replaces platform roots)
    catalog_token: null         # static bearer token, or catalog_credential (OAuth2 client_credentials)
    catalog_client_cert: null   # PEM client certificate for mTLS (https:// only, with catalog_client_key)
    catalog_client_key: null    # PEM private key for catalog_client_cert
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
- `create_table: false` (default): the table must exist, otherwise Fatal
  `config.iceberg_table_missing`. `create_table: true`: if the table is absent
  the schema is derived from the first pending segment's footer (see "Schema
  mapping"), the namespace is created if needed, and the table is created
  (format v2, unpartitioned) — by **client-authored initial metadata plus
  `register_table`**, not the REST `create_table` endpoint. Reason: Iceberg
  assigns fresh sequential field IDs at create time (verified against
  `apache/iceberg-rest-fixture:1.10.1`, which turned `[2, 1000]` into
  `[1, 2]`; iceberg-rust's own `TableMetadataBuilder` does the same), which
  would orphan the stable Parquet field IDs ADR-0013 guarantees and leave data
  files unreadable-by-id. So thadam authors the v1 metadata JSON with the
  file's own IDs, writes it through its FileIO, and asks the catalog to
  `register_table` it.
- Whether the table matches is decided per segment, ahead of its append, by
  schema evolution (next section); there is no separate first-contact check.
- `catalog_ca_file`: with an `https://` `catalog_uri`, the PEM bundle that
  signed the catalog's certificate. It **replaces** the platform trust roots
  for the catalog connection (and for nothing else: the S3 store keeps its
  own trust). Absent, the platform roots apply. See "TLS to the catalog".

## Schema evolution (step 2)

Before each segment is appended, the registrar reads that segment's footer
schema (two ranged GETs; the footer is the single source of truth, see "Schema
mapping") and merges it into the table's current schema **by field ID**
(`schema::schema_evolution`, a pure function):

| Same field ID, file vs table | Outcome |
|---|---|
| ID not in the table | Added as an **optional** column with the file's own ID (a required file column lands optional: older files have no value for it) |
| Same type, new name | **Rename** (the file's spelling wins; IDs are the identity, ADR-0013) |
| Table `int`, file `long`; table `float`, file `double`; table `decimal(P,S)`, file `decimal(P',S)` with `P' > P` | **Promotion**: the table column widens |
| Equal | Nothing |
| Table column the file lacks (the dataset dropped the field) | The column **stays** (it must be optional, see below) |
| File type narrower than the table's (`long`→`int`, `double`→`float`, smaller decimal precision) | **Fatal** (narrowing) |
| Any other type change (`int`→`string`, primitive→struct, decimal scale change, ...), i.e. ID reuse with a different type | **Fatal** |
| Required-ness differs in either direction (optional→required, required→optional), or a **required** table column is missing from the file | **Fatal** |
| List element / map key / map value IDs differ | **Fatal**; element/key/value types otherwise follow the rules above, struct children merge recursively by ID |
| A new ID whose name another column already has | **Fatal** (the merged schema fails Iceberg's own validation) |

Every violation in a segment is reported together (`sink.iceberg_schema_mismatch`,
Fatal, naming the column path, field ID, and both types); the table is not
touched. The merge is **idempotent**: deriving it again from the merged table
schema returns "no change", which is what makes the crash ordering below safe.

### How the schema is committed

Not through the `iceberg` crate's `update_schema` action: in 0.10.1
`UpdateSchemaAction` assigns fresh sequential IDs to added columns (the same
reason step 1 avoids REST `create_table`: configured ID 1001 would become
`last-column-id + 1` and the Parquet files would be unreadable-by-id) and has
no rename or promotion at all. The escape hatch is sealed: `TableCommit`'s
builder is `pub(crate)` and `TransactionAction` is a crate-private trait, so no
caller can hand a catalog a custom update. The registrar therefore posts the
commit to the REST update endpoint itself (`rest::RestCommitter`), with the
crate's own public wire types (`CommitTableRequest`, `CommitTableResponse`):
updates `add-schema` (the merged schema, files' own IDs) and
`set-current-schema -1`, requirements `assert-table-uuid`,
`assert-current-schema-id`, and `assert-last-assigned-field-id`. The catalog
applies both updates atomically. A 409 (another writer changed the schema)
reloads the table and re-derives the merge, bounded like append conflicts; a
merge that a concurrent writer already made derives "no change". Verified
against `apache/iceberg-rest-fixture:1.10.1`: it honors client-supplied field
IDs in `add-schema`, and the renamed/promoted/added schema reads back
correctly through the Iceberg Rust scan (old `int` files through the promoted
`long`, rows older than a new column read it as null).

### Crash ordering: schema first, then append

Atomically committing the schema change *with* the append would need a custom
`TableUpdate` list on the snapshot commit, which 0.10.1 does not allow (above:
the append uses `Transaction::fast_append`, the schema change the REST
endpoint). So they are two commits, **schema first**:

1. **Crash before the schema commit**: nothing changed; on restart the same
   merge is derived from the same footer.
2. **Crash after the schema commit, before the append** (or the schema
   commit's response was lost): the table holds the merged schema and the
   segment is not in the committed set. On restart the merge from the
   (already merged) current schema is "no change", and the append runs. If the
   response of the schema commit was lost and the commit did apply, the retry
   fails its `assert-current-schema-id`, reloads, and derives "no change"; if it
   did not apply, the retry applies it. Either way: one schema change.
3. **Crash inside the append**: the step 1 argument (committed key in the
   snapshot summary, or not).

The intermediate state of case 2 is safe for readers: an evolved schema only
adds optional columns (null for every existing file), widens types with
Iceberg's lossless promotions (readers promote existing files), and renames by
ID (no data touches). No file is reinterpreted, nothing is lost. The reverse
order would not be safe: a file referencing ID 1001 appended to a table whose
schema lacks it is invisible to readers for that column, and a crash after
the append marks the segment "committed" so nothing would ever add the column.

The table's schema history therefore gains at most one entry per segment that
changes the schema, and none when it does not.

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
  state inside each attempt. Two layers cover a writer whose view is stale:
  `Transaction::commit` re-bases on the newest table before every commit, and
  `FastAppendAction` (iceberg 0.10.1 `check_duplicate`, on by default) rejects
  there a data file path the table already references (`DataInvalid`); that
  failure lands in the reload-and-recheck branch, finds the key committed, and
  reports "already registered". Tested deterministically (see below).
- REST fixture / real catalogs: HTTP 409 maps to `CatalogCommitConflicts` in
  iceberg-rust 0.10.1.

## Partitioning

Unpartitioned (spec id 0), the smallest option. Thadam's Hive-style
`date=/hour=` directories are not an Iceberg partition spec (identity on
`_ingest_time` has the wrong granularity and the unsynced-copy case does not
fit one column). File skipping still uses the manifest-level record counts;
column min/max metrics are not populated in step 1 (see limits).

## TLS to the catalog

`iceberg-catalog-rest` 0.10.1 has no TLS or CA property of its own, but
`RestCatalogBuilder::with_client(reqwest::Client)` takes a ready-made client.
The registrar builds one reqwest 0.12 client (rustls + the process `ring`
provider, ADR-0020) — with `catalog_ca_file` its trust roots are replaced by
the PEM bundle (`tls_built_in_root_certs(false)` plus the bundle's
certificates) — and hands the same client to the catalog and to the schema
committer. An unreadable or certificate-less CA file is Fatal
`config.invalid` at startup. A rejected server certificate (untrusted issuer,
expired, wrong name: rustls `invalid peer certificate: ...`) is classified
Fatal `config.invalid` rather than retried forever; reqwest's `Display` hides
that cause, so the classifier and the error message walk the `source` chain.
With `catalog_client_cert` and `catalog_client_key` (PEM files, set together,
`https://` only), the same client loads the client identity
(`reqwest::Identity::from_pem`) and presents it on every catalog and
schema-commit connection; a server TLS alert refusing the client certificate
(`CertificateRequired`, `BadCertificate`, `UnknownCA`) is classified Fatal
`auth.failed`. Catalog authentication (`catalog_token` or `catalog_credential`
with `client_id`, `client_secret`, optional `scope` and `oauth2_server_uri`,
mutually exclusive) configures the REST catalog client's `token` or
`credential`/`oauth2-server-uri`/`scope` properties and sends the same bearer
token on `RestCommitter`'s `/v1/config` and schema-commit requests (exchanging
client credentials at connect and refreshing once on HTTP 401: `RestCommitter`
re-exchanges `catalog_credential` and retries its schema commit once on 401,
and `Registrar` rebuilds `(RestCatalog, RestCommitter)` once and retries the
pass on a catalog 401, failing with fatal `auth.failed` only if the token
exchange itself or the immediate retry still answers 401/403).

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
  named table missing with `create_table: false`, schema mismatch (every
  evolution rule violation above), unsupported footer type (e.g. `UInt64`
  under `unsigned: native`), a rejected catalog TLS certificate, configuration
  errors (including an unusable `catalog_ca_file`, `catalog_client_cert` or
  `catalog_client_key`), a rejected catalog token or OAuth2 credential, and a
  catalog that refuses our client certificate (TLS alert).

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

## Tests (step 2)

- `schema::tests`: every row of the evolution table, including nested
  struct/list/map and "all reasons listed".
- `table::tests` (in-process `MemoryCatalog`, deterministic): a stale second
  writer neither duplicates nor loses; a `Racing` catalog wrapper makes writer
  A commit just before B's commit reaches the catalog, so B gets a real commit
  conflict — B reloads and skips the segment A committed, or (different
  segment) retries on the fresh base; snapshot and committed-key counts are
  asserted.
- Docker (`apache/iceberg-rest-fixture:1.10.1` + SeaweedFS):
  `schema_evolves_before_each_append_and_incompatible_changes_are_fatal`
  (int→long + new column, then rename; values read back; narrowing fatal and
  the table unchanged) and
  `https_catalog_works_with_its_ca_and_fails_fatally_without_trust`
  (`caddy:2.10.2` terminating TLS with an `rcgen` CA in front of the fixture:
  works with the CA — including the schema commit over TLS — and fails fast
  and fatally with another CA and with no CA).

## Tests (catalog authentication)

- Config and refresh unit tests: `catalog_token` and `catalog_credential` are
  exclusive; empty secrets and a `:` in `client_id` are refused; certificate and
  key come together, must exist, and need `https://`; the classifier maps a
  401/403 and a client-certificate TLS alert to fatal `auth.failed` (and does
  not read a port like `:40123` as a status); `rest::tests` and
  `registrar::tests` run an in-process mock HTTP catalog + OAuth2 token server
  that rejects the first bearer token with 401, issues a fresh token on
  re-exchange, and accepts the retry (and verifies that a 401 from the token
  endpoint or a persistent 401 on the immediate retry fails fatally with
  `auth.failed`).
- Docker, one Caddy front per case, each with a schema-evolution commit so the
  direct REST call is covered: `Authorization: Bearer` required (right token
  works; wrong token fatal), OAuth2 client credentials (the fixture's own
  `/v1/oauth/tokens` issues the token; a token endpoint answering 401 via
  `oauth2_server_uri` is fatal), and `client_auth { mode require_and_verify }`
  (client certificate works; none, or one from another CA, fatal).

## Deliberately out of scope (later)

- Compaction / snapshot expiry / data deletion; replay isolation for Iceberg
  (replays must still not point at a production table).
- Populating column metrics on data files (`shortcut:` bounds checks could
  come from the Parquet footer stats the writer already emits).
- Delete vectors, merge-on-read, distributed/cluster commits (one registrar
  per table-write path), and Iceberg-side handling of datasets with
  `unsigned: native`.

### `shortcut:` limits carried in code

- The schema is re-derived from every not-yet-committed segment's footer (two
  small ranged GETs each); a per-dataset cache keyed by schema fingerprint is
  the upgrade if that read cost shows up.
- Schema commits and the append are two commits (crash-safe by ordering, see
  above), because 0.10.1 offers no custom commit; revisit if a later iceberg
  release exposes `TableCommit` construction or custom transaction actions.
- The schema commit bypasses the crate's REST client for the update POST and
  discovers the catalog `prefix` itself from `/v1/config`. It shares the
  catalog client's reqwest client (CA, client certificate) and sends the same
  bearer token; for `catalog_credential` it runs its own client-credentials
  exchange at connect (and refreshes once on 401), because the crate keeps its
  token private.
- Static `catalog_token` is never refreshed (an expired static token is a 401,
  fatal `auth.failed`). With `catalog_credential`, `iceberg-catalog-rest`
  0.10.1 does not refresh its cached token automatically, so an HTTP 401
  rebuilds `(RestCatalog, RestCommitter)` once in `Registrar` (and re-exchanges
  once in `RestCommitter::commit`) before classifying a 401/403 on the
  re-exchange or immediate retry as fatal `auth.failed`.
- 401/403 and TLS client-certificate alerts are classified by message scan
  (the crate folds the status into error text); upgrade when it exposes the
  status structurally.
- Fast-append duplicate-file validation reads every manifest of the current
  snapshot per commit; fine until manifests accumulate (no compaction yet).
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
