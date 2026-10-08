# Validating configurations

Thadam configuration is load-bearing: every byte a pipeline captures depends on it, and a config change alters what happens to data that was captured hours ago. `thadam validate` is the off-line gate that a configuration must pass before it is deployed, and it runs exactly the same checks `thadam run` runs at startup.

## `thadam validate`

```bash
thadam validate <path> [--allow-unresolved]
```

On success it prints the number of sources, datasets, transforms, and sinks, the config hash, and the memory estimate; it exits `0`. On failure it prints every error and warning and exits `2` (invalid configuration).

### Multi-error diagnostics

Validation is a multi-error pass: one mistake does not hide the rest. Each diagnostic carries a stable error code, a YAML path, a line/column, and usually a hint:

```text
error[config.invalid]: unknown transform kind `teleport`, expected one of `validate`, `on_change`, `filter`
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
