# `wal` and `quarantine`

The `wal` section configures the capture write-ahead log: its disk budget, durability policy, retention, and what happens when the budget runs out. Concepts: [capture-first WAL](../concepts/wal-and-replay.md); operational details: [retention and disk budgets](../operations/retention.md).

## `wal` {#wal}

```yaml
wal:
  max_disk: 2GiB
  segment_bytes: 16MiB
  fsync: { interval: 10ms }
  on_full: block
  retain_for: 1h
  max_pending_bytes: 8MiB
```

| Key | Type | Default | Description |
|---|---|---|---|
| `max_disk` | size | required | The WAL disk budget. Must be at least one segment; a budget under four segments warns (retention and `on_full` then act on very coarse units). |
| `segment_bytes` | size | `16MiB` | The size at which a segment file is sealed. Minimum 1 MiB. |
| `fsync` | `always`, `never`, or `{ interval: <duration> }` | `{ interval: 10ms }` (provisional) | The fsync policy; see below. `interval: 0` is invalid. |
| `on_full` | `block` or `drop_oldest` | **none — required** | What happens at `max_disk`. Data loss must be an explicit choice (ADR-0011, D4). See [`on_full`](#on_full). |
| `retain_for` | duration | none | How long *consumed* data stays available for replay, bounded by `max_disk`. Zero is invalid; omit to keep nothing beyond consumption. |
| `max_pending_bytes` | size | `8MiB` | Bytes a source may have submitted to the WAL writer without a completed commit. Zero is invalid. A batch larger than the budget takes the whole budget, which avoids deadlock. |

### `fsync` {#fsync}

| Value | Meaning | Durability |
|---|---|---|
| `always` | fsync after every commit | Process crash and, on flush-honoring storage, power loss. Unbatched small messages amplify writes (≈12.8× payload for 200 B messages at 5,000/s on Linux, spike S1). |
| `{ interval: 10ms }` | Group commit: one `write` + one `fdatasync` per group, at most once per interval | Same guarantee class; commits within an unflushed interval can be lost to power loss — the reason no power-loss claim is published without hardware evidence. The default is provisional pending the S1 re-run on reference hardware. |
| `never` | Never fsync; durability depends on the OS | Survives a process crash only (the OS flushes asynchronously). |

The fsync interval lands inside commit latency, so it directly sets the per-connection [throughput ceiling](../concepts/delivery-guarantees.md#throughput-ceiling) of `ack_mode: on_commit`.

### `on_full` {#on_full-policy}

There is no default: `thadam validate` fails without it.

- **`block`** — withhold acknowledgements. Loss moves to the broker's bounded session queue, where it can happen invisibly once queue limits are exceeded.
- **`drop_oldest`** — evict the oldest unconsumed segments. Loss is visible in Thadam: readers get `Gap::Evicted` items and `thadam_wal_evicted_bytes_total` increments.

Full trade-offs: [backpressure and disk budgets](../concepts/backpressure-and-disk-budgets.md#on_full).

## `quarantine` {#quarantine}

Where rejected events go, with their own disk budget so rejects cannot starve normal data. Optional; defaults as shown.

```yaml
quarantine:
  format: parquet      # parquet (default); ndjson is reserved and rejected
  max_disk: 256MiB
```

| Key | Type | Default | Description |
|---|---|---|---|
| `format` | `parquet` | `parquet` | The segment format for quarantine files. `ndjson` is reserved: `thadam validate` rejects it. |
| `max_disk` | size | `256MiB` | The quarantine disk budget. Must be greater than zero. It covers both the quarantine segments and the small staging file described below. |
| `rotation` | `{ max_bytes, max_rows, max_age }` | `max_bytes: 128 MiB`, `max_age: 300` (seconds) | When the open quarantine segment is closed. This is the quarantine's own cadence: it does not delay the owning sink (see below). |

Quarantine rows use a fixed schema (WAL position, source attributes, error domain/code/reason, and the raw payload truncated at 64 KiB) — see [quarantine](../concepts/per-sink-chains.md#quarantine).

A reject releases its sink's WAL cursor as soon as it is fsynced into the quarantine instance's **staging file** (`quarantine/<sink>/rejects.staging`), not when the quarantine segment is written. The sink's cursor, and so its uploads, no longer wait for `rotation`; the quarantine segment is built from staging on its own cadence. A reject's frame can therefore leave the WAL (retention follows the cursor) while the reject is still only in staging: staging holds the full quarantine row, including the truncated raw bytes, so recovery never needs the WAL for it. The staging file is small (the open segment is closed early once about 8 MiB is staged or the quarantine budget is full) and is truncated once the quarantine segment covering its rows is durable.
