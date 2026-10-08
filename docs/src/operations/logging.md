# Logging

Structured logs are the runtime's narrative channel; metrics tell you what moved, logs tell you why a component changed state.

## Format and destination

- **Sink:** stderr, via a bounded, **lossy** non-blocking writer. When the buffer fills, lines are dropped — and the drop counted — rather than back-pressuring the pipeline. Logging is never a queue that can stall capture.
- **Formats:** `json` (one JSON object per line; the default in config) or human-readable `pretty`/`text`. Structured JSON is the production choice.
- **Level:** `trace` < `debug` < `info` (default) < `warn` < `error`; also accepts env-filter strings (`thadam_runtime=debug,warn`).

Precedence for a single run: `thadam run --log-format` / `--log-level` override [`observability.logs`](../configuration/runtime-observability.md#logs).

## What is and is not logged

- **Never logged:** payloads and secrets. Secrets travel inside `Secret<T>`, whose debug/display output is `<redacted>`; credential-bearing connector state redacts interpolation sites. Lineage (config hash, enrichment hash, WAL positions) is logged instead of payload content.
- **Component lifecycle:** state transitions, retries with the last error, failures.
- **Signals:** first signal begins graceful drain; the second forces exit 130.
- **Spans (planned):** one span per batch carrying `wal_range` and sink; no per-event spans. OpenTelemetry export is an optional `otlp` cargo feature whose exporter is Beta upstream.

## Reading logs operationally

- A `degraded` component in `/status` with a rising `retries` counter has its last error in the logs and in the component state.
- `SESSION_LOST` messages mean the broker forgot a session that was expected; check broker persistence settings ([required broker settings](../sources/mqtt.md#required-broker-settings)).
- The writer is lossy: if you depend on complete logs, drain stderr promptly (a collector sidecar) rather than filling the bounded buffer.

## Known limitation

At this stage of the build-out, `thadam run` initializes its logging guard in a scope that drops it again immediately, so application log lines after startup can be lost silently. This is a known pre-alpha defect (tracked by the ongoing runtime PRs), not intended behavior; it is listed here so operators are not surprised by quiet stderr. Metrics and `/status` are unaffected.
