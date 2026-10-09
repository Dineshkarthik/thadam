# Metrics reference

Thadam exports Prometheus metrics in OpenMetrics text format on [`GET /metrics`](admin-api.md). Naming conventions: `thadam_` prefix; counters end in `_total`; durations end in `_seconds`. Label cardinality is deliberately bounded: **labels are only source ids, sink ids, dataset ids, and bounded enums** — never topic, device, or payload values. (High-cardinality context such as connection state is available in [`/status`](admin-api.md) instead.)

## Implemented metrics

These are registered by the runtime today (`thadam-runtime`'s Prometheus sink). Exported counter names always end in exactly one `_total`.

### Intake

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_source_messages_total` | counter | `source` | Messages received from a source. |
| `thadam_source_bytes_total` | counter | `source` | Payload bytes received from a source. |
| `thadam_source_dropped_total` | counter | `source`, `reason` | Messages dropped by a source (e.g. QoS 0 overflow). |
| `thadam_source_restart_total` | counter | `source` | Source reopens after transient errors. |
| `thadam_source_ack_latency_seconds` | histogram | `source` | Submission → acknowledgement latency. Buckets: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10. |

### Processing (per chain)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_chain_events_total` | counter | `sink`, `dataset`, `outcome` | Events produced by chains, per chain batch from the chain's own stats. `outcome` is one of `written`, `filtered`, `deduped`, `rejected`, `exploded`. The same rejects also appear in `thadam_rejects_total{domain}`, sliced by error domain instead of dataset; poison-frame rejects (whole frames rejected after panic retries) bypass the chain stats and appear only in `thadam_rejects_total`, not in the `rejected` outcome. |
| `thadam_rejects_total` | counter | `sink`, `domain` | Records/events rejected, by error domain (`decode`, `schema`, `transform`, `quality`, …). |
| `thadam_wal_gap_total` | counter | `sink`, `kind` | WAL gap items delivered to readers; `kind` is `corrupt`, `evicted`, or `missing`. |
| `thadam_chain_panic_total` | counter | `sink` | Chain panics caught at the task boundary (each is followed by a rebuild). |
| `thadam_quarantined_total` | counter | `sink` | Records/events written to quarantine. |
| `thadam_rejects_dropped_total` | counter | `sink` | Rejects dropped because no quarantine sink is registered, or because a non-primary chain saw them (the primary-chain rule). |

### Delivery

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_cursor_committed_total` | counter | `sink` | Durable cursor commits for a sink. |
| `thadam_batch_written_total` | counter | `sink` | Successful batch-sink writes. |
| `thadam_batch_written_events_total` | counter | `sink` | Events delivered in those batches. |
| `thadam_batch_write_seconds` | histogram | `sink` | Time inside batch-sink `write`, retries excluded. Same buckets as the ack histogram. |
| `thadam_batch_bisect_total` | counter | `sink` | Batches bisected to isolate poison events. |
| `thadam_retry_total` | counter | `component` | Retry attempts across components. |

### Segments

Segment lifecycle gauges come from the Parquet segment sink and the object-store shipper. Reporters publish **absolute totals** on every catalog transition, so last-writer-wins keeps the series exact across restarts. `state` is one of `staging` (open files; only the segment sink reports these, and their bytes include the in-progress row group's estimate), `finalized`, `eligible`, `uploaded`, `deleted` (local file removed after upload; the count covers tombstones and bytes is the file's recorded size at finalization, not disk usage).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_segment_files` | gauge | `sink`, `state` | Segment records with files per catalog state. |
| `thadam_segment_bytes` | gauge | `sink`, `state` | Recorded bytes for those records. |
| `thadam_segment_finalize_seconds` | histogram | `sink` | Time to finalize one segment file (close, checksum, rename, catalog append). Coverage markers are not files and observe nothing. Buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60. |

### Shipping

Reported by the `object_store` shipper service, labeled with its own sink id.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_ship_bytes_total` | counter | `sink` | Segment bytes settled in the store (`ok` and `conflict_equal`; `fatal` adds nothing). |
| `thadam_ship_uploads_total` | counter | `sink`, `outcome` | Upload settlements. `outcome` is `ok`, `conflict_equal` (a create-if-absent conflict verified as identical content — the crash-recovery path), `retry` (a transient attempt failed; shipper retries are unbounded), or `fatal` (an unresolvable object-key collision; the pipeline fails). |
| `thadam_ship_upload_seconds` | histogram | `sink` | End-to-end time of one settled upload, verification included (`fatal` observes nothing). Same buckets as `thadam_segment_finalize_seconds`. |
| `thadam_ship_retry_age_seconds` | histogram | `sink` | Age of an upload's first failure at each retried attempt. Buckets: 1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600. |

### Durability and health

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_wal_maintained_deleted_bytes_total` | counter | — | Bytes deleted by retention passes. |
| `thadam_wal_evicted_bytes_total` | counter | — | Bytes evicted under `on_full: drop_oldest`. |
| `thadam_component_state` | gauge | `component`, `state` | 1 for the current state of each component (`starting`, `running`, `degraded`, `draining`, `stopped`, `failed`). |
| `thadam_clock_synced` | gauge | — | 1 when the host clock is synchronized (from the clock-sync probe), else 0. Correlates with intended `unsynced/` partitioning and upload signature failures. |

## Additional connector detail

Per-source connection detail (MQTT epoch, ack pipeline depth, queue sizes, unacked maximum equalling the broker window) is exposed through the components in [`/status`](admin-api.md#status) rather than as unbounded-label metrics.

## Planned metric names {#planned}

ARCHITECTURE.md section 14 lists the consolidated metric set, some of which are **planned — not yet emitted** by this build. Names reserved there for later PRs include `thadam_wal_commit_seconds`, `thadam_wal_fsync_seconds`, `thadam_wal_disk_bytes`, `thadam_wal_budget_bytes`, `thadam_recovery_seconds`, `thadam_recovery_truncated_bytes`, `thadam_source_connected`, `thadam_source_backpressure_seconds_total`, `thadam_ack_oldest_age_seconds`, `thadam_source_unacked_max`, `thadam_source_stale_acks_total`, `thadam_quality_flags_total{dataset,flag}`, `thadam_sink_lag_bytes`, `thadam_sink_lag_seconds`, `thadam_log_dropped_total`, and `thadam_sink_delivery_class{class}`. Every metric name is pinned by an exact-list test, so names do not drift silently.
