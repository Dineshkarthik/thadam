# Validating configurations

Thadam configuration is load-bearing: every byte a pipeline captures depends on it, and a config change alters what happens to data that was captured hours ago. `thadam validate` is the off-line gate that a configuration must pass before it is deployed. By default it checks the file; with `--against-data-dir` it also runs the checks `thadam run` makes before it starts.

## `thadam validate`

```bash
thadam validate <path> [--allow-unresolved] [--against-data-dir]
```

On success it prints the number of sources, datasets, transforms, and sinks, the config hash, and the memory estimate; it exits `0`. On failure it prints every error and warning and exits `2` (invalid configuration).

### Multi-error diagnostics

Validation is a multi-error pass: one mistake does not hide the rest. Each diagnostic carries a stable error code, a YAML path, a line/column, and usually a hint:

```text
error[config.invalid]: unknown transform kind `teleport`, expected one of `validate`, `on_change`, `filter`, `sample`, `enrich`, `rename`, `project`, `cast`
  --> pipeline.yaml:23:11 (transforms[0].kind)
error[config.invalid]: `max_disk` is smaller than one segment
  --> pipeline.yaml:33:5 (wal.max_disk)
  = hint: raise `max_disk` or lower `segment_bytes`
```

Every struct rejects unknown keys (`deny_unknown_fields`), so typos in keys are caught rather than ignored. Error codes are stable strings such as `config.invalid`; the full list is in the [error code reference](../operations/error-codes.md).

### The memory estimate

`thadam validate` computes an upper bound of the buffers Thadam owns and warns when it exceeds `runtime.memory_target`:

```text
estimated Thadam-owned memory: 234.6 MiB (runtime.memory_target: 256.0 MiB)
  source `fleet-mqtt` broker window    20.0 MiB  window 20 x message 1.0 MiB
  wal.max_pending_bytes                 8.0 MiB
  sink `local` read and batch           3.0 MiB
  sink `local` open files              96.0 MiB  max_open_files 6 x 2 x row group 8.0 MiB
  sink `s3` uploads                    16.0 MiB  upload_concurrency 2 x part 8.0 MiB
  dedup windows                        91.6 MiB  2000000 keys x 48 B (upper bound)
```

This is the "bounded by configuration" half of the memory model: Thadam-owned buffers are bounded by configuration, while total RSS (which includes `rumqttc`, `hyper`, compression, and allocator fragmentation) is measured per workload and published with benchmarks. The largest lever is usually `wal.max_pending_bytes`, `max_open_files` times `row_group_bytes`, and the dedup window (`max_keys`).

### Secrets and `--allow-unresolved`

A configuration interpolates `${env:NAME}` and `${file:/absolute/path}` at load time ([the configuration file](../configuration/index.md#secrets)). Without the flag, an unset variable or an unreadable file is an error; with it, validation substitutes a placeholder, downgrades the error to a warning, and still checks everything else:

```text
$ thadam validate --allow-unresolved examples/edge-capture/thadam/pipeline.yaml
warning[config.missing]: cannot read `/run/secrets/minio_uploader_key`: No such file or directory (os error 2)
  --> examples/edge-capture/thadam/pipeline.yaml:167:22 (sinks[1].config.access_key_id)
  = hint: check that the file exists and thadam can read it
0 errors, 2 warnings
```

This lets CI validate a config on a machine that has no secrets. Interpolated values are hashed *as written* (the template, not the secret), so rotating a secret never changes the config hash.

### Against the data directory

Plain `thadam validate` reads the file and does not start connectors. A setting that only a connector checks passes it: a misspelled key in a sink's `config:`, or an MQTT source without its `client_id`, is found by `thadam run`, not by plain validate. Add `--against-data-dir` to run the checks `thadam run` makes before it starts, against the data directory the file names in `pipeline.data_dir`:

```text
$ thadam validate --against-data-dir pipeline.yaml
pipeline.yaml: configuration is valid
  pipeline `edge-demo` (node `edge-01`): 1 source, 1 dataset, 0 transforms, 1 sink
  config hash: 4b56621cd831e857830a21e5d2be2ca8 (hash version 1)
estimated Thadam-owned memory: 75.0 MiB (no runtime.memory_target set)
  wal.max_pending_bytes           8.0 MiB  8.0 MiB
  sink `local` read and batch     3.0 MiB  read chunk 1.0 MiB (default) + 2 x batch 1.0 MiB (default)
  sink `local` open files        64.0 MiB  max_open_files 4 (default) x 2 x row group 8.0 MiB (default)
data directory: /var/lib/thadam/edge-demo
  no thadam.json: a new or uninitialized data directory; a run creates it
```

The extra checks only read. They never create a directory, take the data directory's `LOCK`, or write to the WAL:

- **Connector and chain checks.** Every source and sink validates its own `config:`, and every sink's chain compiles. A chain's enrichment file is read at this point. A relative path resolves against the working directory, as it does under `thadam run`, so run the check from the directory `thadam run` will use. No connector is built, so no source or sink is opened.
- **Refusals (exit `3`).** The directory would be refused at start: `thadam.json` names another pipeline, the WAL is bound to another `node_id`, the WAL is missing or has another epoch than the one the recorded sink state was written against, or a format version is one this binary does not read.
- **Notes.** Printed on success, describing what a restart would do: the config hash change (`current -> candidate (changed)`), each sink that has no cursor yet (it starts from its `start_from`), and each directory under `sinks/` or `sources/` the file no longer lists (its state stays on disk).

A connector or chain error exits `2`. The message is the one the connector gives, which `thadam run` also reports when it fails to start:

```text
$ thadam validate --against-data-dir typo.yaml
error: config/config.invalid: unknown field `compresion`, expected one of `datasets`, `compression`, `row_group_bytes`, `rotation`, `max_open_files`, `max_disk`, `timestamp_unit`, `unsigned`, `finalize_on_shutdown`, `node_id` (path=config, component=sinks[0])
typo.yaml: configuration cannot start against this data directory
```

Its limits, which only a start finds:

- A running pipeline is not detected. The check takes no lock, so a directory that another `thadam run` holds is read as it is.
- Reachability, valid credentials, and the state of an existing target (for example, whether a ClickHouse table exists) are not checked.
- The WAL is inspected, not recovered. A torn tail, which `thadam run` truncates at start, is not reported.
- `machine_id` is not checked. `thadam run` does not set one either.
- It needs the real secrets, so it cannot be combined with `--allow-unresolved` (exit `2`).

## `thadam schema`

```bash
thadam schema > thadam-config.schema.json
```

Prints the JSON Schema (draft 2020-12) of the whole configuration file for editor integration. Point a YAML-language-server at it for completion and inline validation:

```yaml
# yaml-language-server: $schema=./thadam-config.schema.json
version: 1
```

## YAML syntax rules

Config `version: 1` is deliberately a restricted YAML dialect. The rules, all enforced by `thadam validate` (from [ADR-0012](../design/index.md)):

- **File limits.** At most 1 MiB, 50,000 YAML nodes, and 32 levels of nesting.
- **Strict booleans.** Only `true` and `false` are booleans; `yes`/`no`/`on`/`off` are plain strings.
- **No anchors, aliases, merge keys, or custom tags.** Repeat the value instead; a diagnostic points at the position.
- **One document per file.** A second `---` is an error.
- **Duplicate keys are errors.**
- **Quote identifiers.** YAML reads `014` as a number; write `client_id: "014"`.
- **Durations need units** (`50ms`, `5s`, `10m`, `1h`, `1d`); sizes use binary or decimal suffixes (`16MiB`, `2GiB`, or a plain byte count); counts may use `k`/`m` suffixes (`500` or `500k`).

## Config hash and versioning

- `version: 1` is mandatory as the first meaningful key. A newer or unknown version is refused with `config.incompatible` (exit 2); the loader never guesses.
- The **config hash** is computed after typed normalization but before interpolation, in canonical form — `1h` and `60m` hash the same, and so do `1MiB` and `1048576`. Keys at their defaults are omitted from the hash, so defaults introduced later do not move it.
- The hash (plus its algorithm version) is recorded in `data_dir/thadam.json` and on every Parquet footer (`thadam.config_hash`), so any row can be traced back to the exact configuration that produced it. See [crash recovery](../operations/crash-recovery.md#restart-with-a-changed-configuration) for what a changed hash on restart means.

## Next

[The configuration file](../configuration/index.md) — every section and key.
