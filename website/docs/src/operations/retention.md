# Retention and disk budgets

Thadam manages three independent disk budgets, tracked with per-category atomics and enforceable from [`/status`](admin-api.md#status) and the maintenance cycle. The conceptual model — why loss can only happen at `on_full` — is in [backpressure and disk budgets](../concepts/backpressure-and-disk-budgets.md); this page is the operational view.

## The three budgets

| Category | Configured by | Holds |
|---|---|---|
| WAL | [`wal.max_disk`](../configuration/wal.md#wal) | Raw captured messages (segments + frames, retained window) |
| Segments (per sink) | sink config `max_disk` | Finalized local Parquet awaiting upload, plus `keep_for` retention |
| Quarantine | [`quarantine.max_disk`](../configuration/wal.md#quarantine) (default 256 MiB) | Rejected events, so failures cannot starve normal data |

Usage (`used` and the `limit`) is reported per category in [`/status`](admin-api.md). Enforcement stays well below filesystem capacity: open and recovery never allocate space, so consumers can still drain and reclaim on a full disk.

## WAL retention mechanics

- Retention deletes **whole segments, oldest first, and never the newest one** (the newest carries the sequence high-water mark across restarts).
- A segment is deleted once **every required sink's cursor has passed it**, subject to [`wal.retain_for`](../configuration/wal.md#wal): consumed data also stays for the replay window, bounded by the budget.
- A `required: false` sink never holds retention; it sees `Gap::Evicted` if it falls behind.
- A required sink that exceeds its [`max_lag`](../configuration/sources-sinks.md#sink-policies) (`bytes` or `age`) stops holding retention — an explicit, counted loss for that sink, visible as gaps in its reader.
- All time-based decisions use monotonic durations and WAL offsets, never wall time.
- Deleted bytes count into `thadam_wal_maintained_deleted_bytes_total`; evicted bytes into `thadam_wal_evicted_bytes_total` ([metrics](metrics.md)).

## When the WAL budget fills: `on_full`

The explicit, required choice, once more in operational terms:

| Operationally | `block` | `drop_oldest` |
|---|---|---|
| Symptom | Sources stall (acks withheld); MQTT keepalive stays up | Old history evaporates from the WAL |
| Loss location | Broker queues (invisible to producers) | Inside Thadam, counted (`thadam_wal_evicted_bytes_total`, `Gap::Evicted`) |
| Pick when | The broker's queue is sized for the outage and you watch it (see [required broker settings](../sources/mqtt.md#required-broker-settings)) | You prefer bounded, visible loss over broker dependency |

## Sizing sketch

Rough envelope for the WAL:

```text
max_disk ≥ ingest_rate × longest_outage_you_want_to_absorb × (1 + framing overhead)
```

The WAL framing overhead is about 2% for 200 B records (WAL spike, 24-byte frame headers plus per-segment header). The endurance math matters too: 10 MB/s of payload is about 860 GB/day of WAL writes before Parquet writes, which is why industrial SSD or eMMC is the supported baseline and SD cards are not ([delivery guarantees](../concepts/delivery-guarantees.md#memory-and-storage-discipline)).

## Watching it

- `/status`: per-category `used`/`limit`, WAL `disk_bytes`, per-sink cursor lag (`lag_frames`), evicted bytes.
- Metrics: `thadam_wal_gap_total{kind}`, `thadam_wal_evicted_bytes_total`, `thadam_component_state`.
- Alert shape: lag frames growing while a required sink is `degraded`, budgets within ~15% of limits, any `Gap::Evicted` on a pipeline that did not expect WAL eviction.
