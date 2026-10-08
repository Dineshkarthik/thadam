# Logging

Structured logs are the runtime's narrative channel; metrics tell you what moved, logs tell you why a component changed state.

## Format and destination

- **Sink:** stderr, via a bounded, **lossy** non-blocking writer. When the buffer fills, lines are dropped — and the drop counted — rather than back-pressuring the pipeline. Logging is never a queue that can stall capture.
- **Formats:** `json` (one JSON object per line; the config default) or human-readable `pretty`. The config also accepts `text` as a deprecated alias for `pretty` (validation warns). Structured JSON is the production choice.
- **Level:** `trace` < `debug` < `info` (default) < `warn` < `error`; also accepts env-filter strings (`thadam_runtime=debug,warn`).

Precedence for a single run: `thadam run --log-format` overrides [`observability.logs.format`](../configuration/runtime-observability.md#logs); without the flag, the config value applies (`json` by default). `--log-level` sets the minimum level for the run.

## What is and is not logged

- **Never logged:** payloads and secrets. Secrets travel inside `Secret<T>`, whose debug/display output is `<redacted>`; credential-bearing connector state redacts interpolation sites. Lineage (config hash, enrichment hash, WAL positions) is logged instead of payload content.
- **Component lifecycle:** state transitions, retries with the last error, failures.
- **Signals:** first signal begins graceful drain; the second forces exit 130.
- **Spans (planned):** one span per batch carrying `wal_range` and sink; no per-event spans. OpenTelemetry export is an optional `otlp` cargo feature whose exporter is Beta upstream.

## Reading logs operationally

- A `degraded` component in `/status` with a rising `retries` counter has its last error in the logs and in the component state.
- `SESSION_LOST` messages mean the broker forgot a session that was expected; check broker persistence settings ([required broker settings](../sources/mqtt.md#required-broker-settings)).
- The writer is lossy: if you depend on complete logs, drain stderr promptly (a collector sidecar) rather than filling the bounded buffer.
