# Backpressure and disk budgets

Thadam is built to degrade predictably under load rather than grow unbounded queues. There are two distinct regimes — flow-controlled sources and best-effort sources — and one explicit decision every pipeline must make at the disk budget: `on_full`.

## Every queue is bounded or accounted

The complete queue inventory (ARCHITECTURE.md section 10):

1. **MQTT incoming queue** — in MQTT 3.1.1 the receive window is the *broker's* setting, not the client's, so the queue is unbounded in count but byte-accounted; the broker's inflight window bounds it. A hard guard disconnects (never blocks) if unacknowledged messages exceed `expected_broker_window × unacked_guard_multiple`. QoS 0 messages use a byte-bounded queue (`qos0_queue_bytes`, default 8 MiB) where the newest message is dropped and counted when full.
2. Source → WAL: a channel bounded to 256 batches plus a byte semaphore (`wal.max_pending_bytes`, default 8 MiB).
3. Uncommitted commit-tickets per source: 64.
4. Chain → sink driver: 2 batches, byte-capped at twice the chain read size.
5. Parquet encoding and shipper upload pools, bounding open files (`max_open_files`, default 4) and upload memory (`upload_concurrency × part_size`).
6. Logs: a bounded, lossy writer that counts drops.

`thadam validate` turns the user-tunable subset into the [memory estimate](../getting-started/validating-configs.md#the-memory-estimate) and warns above `runtime.memory_target`.

## Flow-controlled sources: withhold acks, keep polling

For MQTT QoS 1 and 2, backpressure works by **withholding acknowledgements while the client keeps polling** its network loop, so the keepalive ping continues and the broker stops at exactly its inflight window. The MQTT spike verified this end to end (acks withheld for 3× the keepalive: 0 reconnects, 0 loss, broker stopped at its window). Stopping the poll loop instead causes a broker disconnect after roughly 1.5–2× the keepalive and a redelivery storm — which is why the connector's poll task is forbidden from awaiting anything but the library's `poll()`.

Source-side memory is therefore bounded by `broker window × max_message_bytes`, plus the QoS 0 queue budget.

## Best-effort sources: counted drops

Sources without flow control (MQTT QoS 0, best-effort ROS 2 topics) cannot apply backpressure at all. The only honest options are a **counted drop** or a visible overflow. Thadam drops the newest message when the QoS 0 byte budget fills, counts it (`thadam_source_dropped_total{reason=...}` and, for MQTT, the `mqtt.gap` / `mqtt.gap_dropped` message attributes), and never pretends the drop did not happen.

## Disk budgets and `on_full` {#on_full}

Disk is the second resource that can fill. Budgets are per category — WAL (`wal.max_disk`), segments (per sink), and quarantine (`quarantine.max_disk`) — tracked with atomics so any one category can be inspected and capped separately.

When the **WAL** budget is reached, `wal.on_full` decides, and there is deliberately **no default**: configuration validation fails without it.

| Policy | What happens | Where the loss goes |
|---|---|---|
| `block` | Thadam withholds acknowledgements; sources stall | The broker's persistent-session queue is bounded (Mosquitto `max_queued_messages` default 1000; EMQX `mqtt.max_mqueue_len` default 1000). Once it overflows, the broker drops — newest message on Mosquitto, oldest on EMQX — **silently from the publisher's point of view**. `block` does not prevent loss; it moves it to the broker. |
| `drop_oldest` | The oldest unconsumed WAL segments are evicted | Inside Thadam, where it is visible: readers receive `Gap::Evicted` items and `thadam_wal_evicted_bytes_total` counts the bytes. |

`on_full` is the single place inside Thadam where acknowledged data can be lost, and the choice is recorded in every config hash and Parquet footer. See [wal](../configuration/wal.md) for the keys.

## Per-sink lag policy

Because retention waits for *required* sinks, one stalled sink would otherwise pin the WAL forever. The escape valves (in [`sources` and `sinks`](../configuration/sources-sinks.md#sink-policies)):

- `required: false` — the sink never holds retention back; if it falls behind eviction it sees a `Gap::Evicted`.
- `max_lag: { bytes: … }` or `{ age: … }` — a required sink that falls more than this behind is evicted from retention: an explicit, counted loss for that sink only.
- `start_from: latest` — a sink with no cursor starts at the current durable end instead of the oldest retained frame.

## Backpressure end to end

The wave travels outward only as far as it must: a stalled sink fills its chain channel → its cursor stops → the WAL retains more → at `max_disk`, `on_full` triggers → sources see backpressure or eviction. At every step the telemetry tells you where the wave is: `thadam_component_state`, cursors and lag in [`/status`](../operations/admin-api.md), `thadam_wal_gap_total`, `thadam_retry_total`.
