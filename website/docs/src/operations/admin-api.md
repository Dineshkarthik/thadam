# Admin API

The admin HTTP API is the operational front door: health, readiness, metrics, and a full pipeline status snapshot. It binds to loopback by default (`127.0.0.1:9464`, configured in [`observability.admin`](../configuration/runtime-observability.md#admin) or overridden with `thadam run --admin-bind`).

## Endpoints

| Method & path | Auth | Response |
|---|---|---|
| `GET /healthz` | never | `200 ok` (text) while the process is alive. The liveness probe. |
| `GET /readyz` | bearer if configured | `200 ok` when the pipeline phase is `running` and no component is `starting` or `failed`; `503` otherwise. Flips to 503 at first shutdown signal. |
| `GET /metrics` | bearer if configured | All Prometheus metrics in OpenMetrics text format (`application/openmetrics-text; version=1.0.0`). See the [metrics reference](metrics.md). |
| `GET /status` | bearer if configured | The full pipeline status snapshot as pretty JSON (below). |

## Authentication

- With no token configured, all endpoints are open (acceptable only on loopback).
- With `observability.admin.token_file` (or `--admin-token-file`): `/readyz`, `/metrics`, and `/status` require `Authorization: Bearer <token>`; a missing or wrong token gets `401` (compared in constant time over SHA-256 digests without leaking token length). `/healthz` is always unauthenticated.
- **A non-loopback bind without a token is a configuration error** (`config.incompatible`), not a warning — the runtime refuses rather than exposing operational state unauthenticated.
- Concurrent admin connections are capped at 1024; HTTP/1 request headers time out after 10 s, idle keep-alive connections after 60 s, and request body reads after 10 s (`408 Request Timeout`; bodies over 64 KiB get `413`).

## `/status` {#status}

The snapshot (`thadam status` pretty-prints exactly this):

```jsonc
{
  "pipeline": "edge-capture",
  "node_id": "edge-01",           // pipeline.node_id, as resolved from the configuration
  "thadam_version": "0.1.0",       // the binary that runs this pipeline
  "config_hash": "f6410eaa…",     // 32 hex digits; the value `thadam validate` prints
  "config_hash_version": 1,       // the version of the hash algorithm
  "previous_config_hash": null,   // the hash in force before the last change (thadam.json); null until one happens
  "started_at": 1760350000000,    // Unix milliseconds when `run` began; null before that
  "phase": "running",            // built | starting | running | draining | stopped
  "ready": true,
  "components": [
    {
      "id": "source:fleet-mqtt",  // source:<id>, chain:<sink>, sink:<id>, service:<id>, maintenance
      "kind": "source",          // source | chain | segment_sink | batch_sink | service | maintenance
      "state": { "state": "running" },
      // degraded:   { "state": "degraded", "since_unix_ms": 1, "last_error": {domain, code, message} }
      // failed:     { "state": "failed", "error": {domain, code, message} }
      "counters": {
        "messages": 0, "bytes": 0, "batches": 0, "events": 0, "rejects": 0,
        "unmatched": 0, "quarantined": 0, "retries": 0, "restarts": 0, "gaps": 0, "panics": 0,
        "bisects": 0, "queued_bytes": 0, "uncommitted_batches": 0
      },
      "delivery": null,            // batch sinks: the probed delivery class, when known
      "oldest_unacked_secs": null  // sources: the lost-token watchdog input
    }
  ],
  "cursors": [
    {
      "sink": "local",
      "cursor": "sink.local",
      "committed": 0,              // every frame at or below this is delivered
      "inflight": null,            // batch sinks: [first, last] with outcome unknown
      "lag_frames": 0,             // durable frames not yet committed
      "required": true,
      "max_lag": null,             // { "bytes": n } or { "age_secs": n }
      "start_from": "earliest"
    }
  ],
  "wal": {
    "epoch": "2b1f04ae4c3d8e01",  // hex
    "first_seq": 1,
    "durable_end": 0,              // last durable frame (0: none)
    "segments": 0,
    "disk_bytes": 0,
    "evicted_bytes": 0,
    "writer": "running"            // writer thread state
  },
  "budgets": [
    { "category": "wal", "used": 0, "limit": 2147483648 },
    { "category": "segments", "used": 0, "limit": null },
    { "category": "quarantine", "used": 0, "limit": 268435456 }
  ],
  "clock_synced": false
}
```

Component states are the error-model states: `starting`, `running`, `degraded` (retrying after transient errors), `draining`, `stopped`, `failed` (terminal for that component; readiness false, capture continues within policy).

The identity fields (`node_id`, `thadam_version`, `config_hash`, `config_hash_version`, `previous_config_hash`, `started_at`) match the startup log record and the [`thadam_build_info`](metrics.md) gauge. The `thadam.config_hash` footer of each Parquet file the run writes is the same `config_hash`.

## CLI equivalents

| Command | Behavior |
|---|---|
| [`thadam status`](../cli/index.md#thadam-status) | `GET <admin-url>/status`, pretty-printed; `--admin-url` (default `http://127.0.0.1:9464`), `--token-file`. Exit 0 on HTTP 200, 1 otherwise. |
| `curl -s http://127.0.0.1:9464/readyz` | readiness probe for orchestrators |
| `curl -s http://127.0.0.1:9464/metrics` | OpenMetrics scrape |

## Notes

- The admin server starts before sources do, so `starting` and recovery state is observable.
- There is no write API: replay and inspection controls come through the CLI, not HTTP.
