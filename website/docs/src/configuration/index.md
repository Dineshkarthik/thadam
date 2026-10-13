# The configuration file

One file defines one pipeline; one process runs one pipeline. The format is **config version 1**: the first meaningful key must be `version: 1`, and anything else is refused (`config.incompatible`, exit 2). Generate the JSON Schema for editors with `thadam schema`; check a file with `thadam validate`.

## Top-level shape

```yaml
version: 1                     # required, must be 1
pipeline: { ... }              # required: identity and data directory
sources: [ ... ]               # required, at least one: where data comes from
wal: { ... }                   # required: the capture WAL
quarantine: { ... }            # optional: where rejected events go
datasets: [ ... ]              # required, at least one field each: the output tables
transforms: [ ... ]            # optional: steps applied to datasets, in order
sinks: [ ... ]                 # required, at least one: where data goes
runtime: { ... }               # optional: process-level settings
observability: { ... }         # optional: logs and the admin API
```

Every struct rejects unknown keys, so adding to this format is a minor release and misspellings are errors. `quarantine`, `transforms`, `runtime`, and `observability` have defaults when absent; the rest are required.

| Key | Type | Required | Page |
|---|---|---|---|
| `version` | integer, must be `1` | yes | this page |
| `pipeline` | mapping | yes | [`pipeline`](pipeline.md) |
| `sources` | list of connectors | yes (≥ 1) | [`sources` and `sinks`](sources-sinks.md) |
| `wal` | mapping | yes (`on_full` explicitly) | [`wal` and `quarantine`](wal.md) |
| `quarantine` | mapping | no | [`wal` and `quarantine`](wal.md#quarantine) |
| `datasets` | list of mappings | yes (≥ 1 field each) | [`datasets`](datasets.md) |
| `transforms` | list of steps | no | [`transforms` and predicates](transforms.md) |
| `sinks` | list of connectors | yes (≥ 1) | [`sources` and `sinks`](sources-sinks.md) |
| `runtime` | mapping | no | [`runtime` and `observability`](runtime-observability.md) |
| `observability` | mapping | no | [`runtime` and `observability`](runtime-observability.md) |

## Loading lifecycle

1. Read the file (≤ 1 MiB), index it, and check syntax: duplicate keys, anchors/aliases, merge keys, custom tags, and extra documents are errors at their position.
2. Parse to JSON values (`serde-saphyr`, strict booleans, ≤ 50,000 nodes, depth ≤ 32).
3. **Interpolate secrets**: `${env:NAME}` and `${file:/absolute/path}` on string scalars only.
4. Check `version`, then deserialize every section with `deny_unknown_fields`.
5. Run cross-field checks (unique ids, references, JSON Pointers, predicates, limits).
6. Estimate memory and warn above `runtime.memory_target`.

`thadam validate` stops after step 6. `thadam run` continues: connector-specific validation through the registry, then build and start.

## Scalar types {#scalars}

| Type | Accepted forms | Canonicalized to | Examples |
|---|---|---|---|
| Duration | text with a unit, or — in the MQTT `config` block only — a whole number of seconds | `humantime` text | `50ms`, `5s`, `10m`, `1h`, `1d` |
| Size | `KiB`/`MiB`/`GiB`/`TiB` or `KB`/`MB`/`GB`/`TB` text, or a plain byte count | byte integer | `16MiB`, `2GiB`, `1048576` |
| Count | integer, or text with `k`/`m` suffix (transform durations also accept `g` inside `thadam-transform`) | integer | `4`, `500k`, `2m` |
| Field type | `bool`, `i8`…`i64`, `u8`…`u64`, `f32`, `f64` (`str`/`string`, `bytes`, `ts`/`timestamp`), `list<T>` (nesting ≤ 3) | canonical spelling | `f32`, `u64`, `list<f64>` |
| Time domain | `utc`, `tai`, `monotonic`, `sim`, `unknown` | as written | `utc` |

Bare numbers are rejected for durations; units are mandatory. Equal values hash identically regardless of spelling (`1h` ≡ `60m`).

## YAML dialect rules

- Only `true`/`false` are booleans; `yes`/`no` are strings. Quote identifiers like `client_id: "014"`.
- No anchors, aliases, merge keys, custom tags, or multiple documents. Duplicate keys are errors.
- `on_full` has no default; filters are structured predicates; `expr:` is reserved.

## Secrets {#secrets}

Values are interpolated on parsed string scalars (never on raw text or keys), so a substitution cannot change the document structure:

```yaml
sinks:
  - id: s3
    kind: object_store
    config:
      access_key_id: "${env:THADAM_DEMO_KEY}"     # from the environment
      secret_access_key: "${file:/run/secrets/s3_secret}"
```

- `${env:NAME}`: environment variable; names are `[A-Za-z_][A-Za-z0-9_]*`.
- `${file:/absolute/path}`: UTF-8 file, absolute path, at most 64 KiB (`MAX_SECRET_FILE_BYTES`); one trailing newline is removed. Intended for keys and tokens.
- Substituted values are always strings (`sizes`/durations parse afterwards), are never expanded again, and nesting (`${env:${env:X}}`) and defaults are not supported. `$${` writes a literal `${`. An interpolated result may be at most 1 MiB.
- Unresolved references are errors by default and warnings with `thadam validate --allow-unresolved`.
- Every interpolated value is wrapped in `Secret<T>`: redacted (`<redacted>`) in `Debug`, logs, and serialization, and hashed *as written*, so rotating a secret does not move the config hash. Payloads and secrets are never logged.

## Config hash and identity

The config hash ( algorithm version 1, reported by `thadam version`) is taken after typed normalization and before interpolation, in canonical form. Keys at their defaults are omitted, so defaults added later do not move the hash. The hash is:

- recorded in `data_dir/thadam.json` (with the previous hash when it changed) — a changed configuration is accepted and recorded; a different pipeline name or WAL epoch is refused (exit 3, see [crash recovery](../operations/crash-recovery.md));
- written into every Parquet footer as `thadam.config_hash`, together with `thadam.enrichment_hash` (the lineage hash of the enrichment tables the sink's chain joined) — the two inputs for replay row-set equivalence.

## Where to go next

- [`pipeline`](pipeline.md) · [`sources` and `sinks`](sources-sinks.md) · [`wal` and `quarantine`](wal.md) · [`datasets`](datasets.md) · [`transforms` and predicates](transforms.md) · [`runtime` and `observability`](runtime-observability.md)
- Connectors: [MQTT source](../sources/mqtt.md), [Parquet](../sinks/parquet.md) and [object storage](../sinks/object-store.md) sinks.
- [Validating configurations](../getting-started/validating-configs.md) for diagnostics and `--allow-unresolved`.
