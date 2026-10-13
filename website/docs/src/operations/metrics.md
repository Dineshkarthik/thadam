# Metrics reference

Thadam exports Prometheus metrics in OpenMetrics text format on [`GET /metrics`](admin-api.md). Naming conventions: `thadam_` prefix; counters end in `_total`; durations end in `_seconds`. Label cardinality is deliberately bounded: **labels are only source ids, sink ids, dataset ids, and bounded enums** — never topic, device, or payload values — with one deliberate exception, `thadam_build_info` below. (High-cardinality context such as connection state is available in [`/status`](admin-api.md) instead.)

## Implemented metrics

These are registered by the runtime today (`thadam-runtime`'s Prometheus sink). Exported counter names always end in exactly one `_total`.

### Intake

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_source_messages_total` | counter | `source` | Messages received from a source. |
| `thadam_source_bytes_total` | counter | `source` | Payload bytes received from a source. |
| `thadam_source_dropped_total` | counter | `source`, `reason` | Messages dropped by a source (e.g. QoS 0 overflow). |
| `thadam_source_restart_total` | counter | `source` | Source reopens after transient errors. |
| `thadam_source_connected` | gauge | `source` | 1 while the source session is open, 0 when disconnected, backing off, or stopped. |
| `thadam_source_backpressure_seconds_total` | counter | `source` | Seconds a source spends unable to pull because its uncommitted-batch window is full or the WAL is waiting for space. |
| `thadam_ack_oldest_age_seconds` | gauge | `source` | Age in seconds of the oldest unacknowledged batch (0 when no batch is pending). |
| `thadam_source_unacked_max` | gauge | `source` | High-water mark of unacknowledged batches observed for a source. |
| `thadam_source_stale_acks_total` | counter | `source` | Acknowledgements dropped because they belonged to an older connection epoch. |
| `thadam_source_ack_latency_seconds` | histogram | `source` | Submission → acknowledgement latency. Buckets: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10. |

### Processing (per chain)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_chain_events_total` | counter | `sink`, `dataset`, `outcome` | Events produced by chains, per chain batch from the chain's own stats. `outcome` is one of `written`, `filtered`, `late`, `deduped`, `rejected`, `exploded` (`late` counts events dropped because their `time: event` window already closed). The same rejects also appear in `thadam_rejects_total{domain}`, sliced by error domain instead of dataset; poison-frame rejects (whole frames rejected after panic retries) bypass the chain stats and appear only in `thadam_rejects_total`, not in the `rejected` outcome. |
| `thadam_rejects_total` | counter | `sink`, `domain` | Records/events rejected, by error domain (`decode`, `schema`, `transform`, `quality`, …). |
| `thadam_quality_flags_total` | counter | `dataset`, `flag` | Quality flags raised on emitted events (`out_of_range`, `clock_skew`, `missing_time`, `out_of_order`, `duplicate`, `stale`, `sequence_gap`, `decode_error`, `invalid`). |
| `thadam_wal_gap_total` | counter | `sink`, `kind` | WAL gap items delivered to readers; `kind` is `corrupt`, `evicted`, or `missing`. |
| `thadam_chain_panic_total` | counter | `sink` | Chain panics caught at the task boundary (each is followed by a rebuild). |
| `thadam_chain_unmatched_total` | counter | `sink` | Records no dataset of the chain matched, for example frames from a source removed from the configuration that are still in the WAL. They are not rejects and not in `thadam_rejects_total`. Counts records this process's chain processes; warm-up frames read before the cursor are not counted. A chain rewind re-reads frames and counts their unmatched records again, and a chain panic can under-count the batch it interrupts. The same count is `counters.unmatched` on the `chain:<sink>` component in [`/status`](admin-api.md#status). |
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
| `thadam_sink_delivery_class` | gauge | `sink`, `class` | 1 for the sink's effective delivery class (`idempotent_batch`, `at_least_once`, or `at_most_once`), recorded when the sink opens. |
| `thadam_retry_total` | counter | `component` | Retry attempts across components. |

### Segments

Segment lifecycle gauges come from the Parquet segment sink and the object-store shipper. Reporters publish **absolute totals** on every catalog transition, so last-writer-wins keeps the series exact across restarts. `state` is one of `staging` (open files; only the segment sink reports these, and their bytes include the in-progress row group's estimate), `finalized`, `eligible`, `uploaded`, `deleted` (local file removed after upload; the count covers tombstones and bytes is the file's recorded size at finalization, not disk usage).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_segment_files` | gauge | `sink`, `state` | Segment records with files per catalog state. |
| `thadam_segment_bytes` | gauge | `sink`, `state` | Recorded bytes for those records. |
| `thadam_segment_finalize_seconds` | histogram | `sink` | Time to finalize one segment file (close, checksum, rename, catalog append). Coverage markers are not files and observe nothing. Buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60. |
| `thadam_sink_lag_bytes` | gauge | `sink` | Unconsumed WAL segment bytes after a sink's committed cursor. |
| `thadam_sink_lag_seconds` | gauge | `sink` | Monotonic age in seconds of the oldest unconsumed WAL segment after a sink's committed cursor. |

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
| `thadam_wal_commit_seconds` | histogram | — | Duration of each WAL group-commit write. Buckets: 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1. |
| `thadam_wal_fsync_seconds` | histogram | — | Duration of each WAL segment `fsync`. Same buckets as `thadam_wal_commit_seconds`. |
| `thadam_wal_disk_bytes` | gauge | — | Current bytes held by retained WAL segments. |
| `thadam_wal_budget_bytes` | gauge | — | Configured WAL disk budget (`wal.max_disk`) in bytes. |
| `thadam_wal_maintained_deleted_bytes_total` | counter | — | Bytes deleted by retention passes. |
| `thadam_wal_evicted_bytes_total` | counter | — | Bytes evicted under `on_full: drop_oldest`. |
| `thadam_recovery_seconds` | gauge | — | Duration of startup WAL recovery in seconds. |
| `thadam_recovery_truncated_bytes` | gauge | — | Bytes truncated from the last WAL segment during startup recovery. |
| `thadam_component_state` | gauge | `component`, `state` | 1 for the current state of each component (`starting`, `running`, `degraded`, `draining`, `stopped`, `failed`). |
| `thadam_clock_synced` | gauge | — | 1 when the host clock is synchronized (from the clock-sync probe), else 0. Correlates with intended `unsynced/` partitioning and upload signature failures. |
| `thadam_log_dropped_total` | counter | — | Log lines dropped by the lossy logging writer. |

### Build and configuration identity

`thadam_build_info` is recorded once, when `run` starts, and is the one metric whose labels identify the process. Its labels carry the config hash and node id, which no other metric may carry, so one series per process is the only cardinality it adds. Its value is always 1. To attach the hash to another series, join on the process (for example with `group_left`). The same values appear in [`/status`](admin-api.md#status) and in the startup log record.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `thadam_build_info` | gauge | `version`, `config_hash`, `node_id`, `pipeline` | Always 1. `version` is the thadam version, `config_hash` the configuration's hash (the value `thadam validate` prints), `node_id` the pipeline's node id. |

## Additional connector detail

Per-source connection detail (MQTT epoch, ack pipeline depth, queue sizes, unacked maximum equalling the broker window) is exposed through the components in [`/status`](admin-api.md#status) rather than as unbounded-label metrics.

## Planned metric names {#planned}

All consolidated metrics from ARCHITECTURE.md section 14 are now implemented and listed in the tables above. Every exported metric name is pinned by an exact-list test (`crates/thadam-runtime/tests/prometheus_metrics.rs`), so names do not drift silently.
