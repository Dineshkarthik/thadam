# `runtime` and `observability`

Process-level settings and the operational surface. Both sections are optional; defaults shown.

## `runtime` {#runtime}

```yaml
runtime:
  worker_threads: 2
  memory_target: 256MiB
```

| Key | Type | Default | Description |
|---|---|---|---|
| `worker_threads` | count | `min(cores, 4)` | Tokio worker threads. The blocking pool is fixed at 8 threads. |
| `memory_target` | size | none | `thadam validate` warns when the estimate of Thadam-owned memory exceeds this. See [the memory estimate](../getting-started/validating-configs.md#the-memory-estimate). |

The deeper runtime tuning — shutdown timeout (30 s), drain timeout (10 s), cursor-commit interval (200 ms), batching thresholds, and backoff policies — is fixed in code today, not configuration keys (see `RuntimeOptions` in `thadam-runtime`; values are listed in [crash recovery](../operations/crash-recovery.md#graceful-shutdown)).

## `observability` {#observability}

### `observability.logs` {#logs}

```yaml
observability:
  logs:
    format: json        # json (default) | pretty
    level: info         # trace | debug | info (default) | warn | error
```

| Key | Type | Default | Description |
|---|---|---|---|
| `format` | `json` or `pretty` | `json` | Structured JSON lines (one object per line) or human-readable output. `text` is a deprecated alias for `pretty` and warns at validation. Structured JSON is the default. |
| `level` | level name | `info` | The minimum log level. |

Logs go to stderr through a bounded, **lossy** writer: when the buffer fills, lines are dropped and the drop is counted (`thadam_log_dropped_total`, once wired) rather than blocking the pipeline. Payloads and secrets are never logged. `thadam run --log-format` overrides `observability.logs.format` for a single run; without the flag, the config value is used (`json` when the section is absent). `--log-level` sets the minimum level for a run and accepts env-filter strings such as `thadam_runtime=debug,warn`.

### `observability.admin` {#admin}

```yaml
observability:
  admin:
    bind: 127.0.0.1:9464
    token_file: /run/secrets/admin_token
```

| Key | Type | Default | Description |
|---|---|---|---|
| `bind` | `host:port` | `127.0.0.1:9464` | The admin API listen address. A **non-loopback bind requires `token_file`** (configuration error `config.incompatible` otherwise — the endpoints expose operational state). |
| `token_file` | path | none | A file holding the bearer token used to authorize `/readyz`, `/metrics`, and `/status`. `/healthz` is always unauthenticated. |

Endpoints and auth details: [admin API](../operations/admin-api.md). `thadam run --admin-bind` and `--admin-token-file` override this section for a single run.
