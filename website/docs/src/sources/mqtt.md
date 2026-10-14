# MQTT source

The `mqtt` source subscribes to a broker, captures messages into the WAL with persistent sessions, and acknowledges after the WAL commit — the at-least-once path. Design: [ADR-0006](../design/index.md) with evidence from spike S2. Built on `rumqttc` 0.25.1 behind `thadam-mqtt`; library types never cross the connector boundary.

```yaml
sources:
  - id: fleet-mqtt
    kind: mqtt
    config:
      url: mqtts://broker.example:8883
      client_id: thadam-edge-01
      clean_start: false
      session_expiry: 1d
      keep_alive: 30s
      topics:
        - { filter: "robots/+/telemetry", qos: 1 }
      ack_mode: on_commit
      max_message_bytes: 1048576
      expected_broker_window: 20
      tls: { ca: /etc/thadam/ca.pem, cert: /etc/thadam/client.pem, key: /etc/thadam/client.key }
```

## All options

Required: `url`, `client_id`, `topics`. Unknown keys are rejected. Durations are whole seconds (`30`) or a unit string (`30s`, `5m`, `1h`; `keep_alive`, `connect_timeout`, and `open_timeout` require whole seconds). Limits below are enforced by `thadam validate`.

| Key | Type | Default | Description |
|---|---|---|---|
| `url` | string | — | `mqtt://host[:1883]` or `mqtts://host[:8883]`; IPv6 in brackets. No credentials, path, or query — use `username`/`password_file`. |
| `client_id` | string | — | Stable, non-empty, ≤ 256 bytes, no control characters. **Names the persistent session** — keep it stable across restarts or redelivery will not work. |
| `topics` | list | — | At least one `{ filter, qos }`. `qos` is 0, 1, or 2 (default 1). Filters follow MQTT rules (`+`, trailing `#`); shared subscriptions (`$share/…`) and duplicate filters are rejected. |
| `clean_start` | boolean | `false` | Start a clean session on every connect. **`true` is rejected with `ack_mode: on_commit`**: a clean session redelivers nothing after a crash, so at-least-once is impossible. |
| `session_expiry` | duration | none | MQTT 5 session expiry interval. Requires `protocol: v5` (≤ 4,294,967,295 s). |
| `keep_alive` | duration | `30s` | Whole seconds: 1–65535 for `v311`, ≥ 5 for `v5`; 30–60 s recommended. Keepalive is what keeps the connection up while acknowledgements are withheld. |
| `connect_timeout` | duration | `10s` | Whole seconds, ≥ 1. Per connect attempt; also bounds each network flush. |
| `open_timeout` | duration | `30s` | Whole seconds, ≥ 1. How long startup waits for the first connection and subscription. |
| `receive_maximum` | integer | none | MQTT 5 Receive Maximum (1–65535): the window the client asks for. Requires `protocol: v5`; can only *lower* the broker's window. |
| `tls` | mapping | none | `{ ca, cert, key }` PEM file paths. Only valid with `mqtts://`; `cert` and `key` must be given together; absent `ca` means platform trust roots. See [TLS and mTLS](#tls-and-mtls). |
| `username` | string | none | Username. |
| `password_file` | path | none | File holding the password, keeping the secret out of the config. Requires `username`. (For `${file:}`/`${env:}` interpolation see [secrets](../configuration/index.md#secrets).) |
| `ack_mode` | `on_commit` or `on_receive` | `on_commit` | See [ack modes](#ack-modes). |
| `expected_broker_window` | integer | `100` | 1–65535. The broker's inflight window, used only for the `thadam validate` memory estimate and the unacknowledged-message guard (disconnects above window × `unacked_guard_multiple`). In MQTT 3.1.1 the window is the broker's setting, not configured here. |
| `unacked_guard_multiple` | integer | `4` | ≥ 2. Multiplier for the guard above. The guard disconnects rather than ever blocking the poll loop. |
| `qos0_queue_bytes` | integer | `8388608` | Byte budget for queued QoS 0 messages (8 MiB); beyond it the **newest message is dropped and counted**. Must be ≥ `max_message_bytes`. |
| `max_message_bytes` | integer | `1048576` | Largest accepted payload in bytes (1 MiB). The client's incoming packet limit (`max_incoming_packet_size`) is derived from it; align the broker's own limit with it (see [required broker settings](#required-broker-settings)). |
| `protocol` | `v311` or `v5` | `v311` | `v311` is the supported protocol; `v5` is **experimental** and needs the `v5` cargo feature of `thadam-mqtt` (rejected in builds without it). |
| `request_channel_capacity` | integer | `1024` | Client request-channel capacity, ≥ 16. |

## Ack modes {#ack-modes}

| `ack_mode` | Guarantee | Behavior |
|---|---|---|
| `on_commit` (default) | **At-least-once** for QoS 1 and 2, in receive order | The broker is acknowledged (PUBACK) only after the WAL commit resolves. Backpressure is applied by withholding acks while the client keeps polling, so keepalive survives and the broker stops at exactly its inflight window (verified in S2: 3× keepalive stall, 0 reconnects, 0 loss). Throughput is bounded by the broker window ÷ commit latency — see [throughput ceiling](../concepts/delivery-guarantees.md#throughput-ceiling). |
| `on_receive` | **At-most-once across a crash** | The message is acknowledged as the runtime pulls it. A crash between receive and WAL commit loses it. Opt-in throughput escape hatch only. |

QoS 0 is at-most-once in both modes. QoS 1 is claimed; **QoS 2 is experimental** (happy path tested only).

On every reconnect the connector bumps a connection epoch; acknowledgement handles from an older epoch are dropped and counted (`connector.stale_ack`), which prevents acknowledging the wrong message after session loss. When the broker reports `session_present = false` for a session that was expected, the connector emits `SESSION_LOST` (`connector.session_lost`), resubscribes, and marks the first message after the gap (see below).

## Message attributes

Every captured message carries attributes, available for dataset `match` and lineage:

| Attribute | Value |
|---|---|
| `topic` | The topic (interned string). The normalized attribute `match.topic` filters and `{topic.N}` identity templates read. |
| `mqtt.topic` | The topic (interned string), also under the connector namespace. |
| `mqtt.qos` | `0`, `1`, or `2`. |
| `mqtt.retain` | `true` / `false`. |
| `mqtt.dup` | `true` / `false` — broker redelivery marker. |
| `mqtt.gap` | Present on the first message after a gap: `session_lost`, `queue_full`, `oversize` (comma-joined). |
| `mqtt.gap_dropped` | Messages dropped for that gap, when non-zero. |

## TLS and mTLS {#tls-and-mtls}

`mqtts://` is terminated **inside `thadam-mqtt`**, not by the client library. Because every `rumqttc` TLS feature pins a `rustls-webpki` version with open RustSec advisories, `rumqttc` is built with no features at all and speaks plain MQTT to an in-process relay over a private Unix socket (`0700` directory); the relay dials the broker with `tokio-rustls` and the `ring` provider (ADR-0006 amendment, 2026-10-07). Consequences:

- CA, mutual-TLS, and hostname verification apply as usual through the `tls` block; a TLS failure surfaces as the real TLS or network error.
- Connection-epoch semantics are unchanged — the relay is transport-only.
- The relay stays until `rumqttc` releases a build on a patched `rustls-webpki` (the documented revisit trigger).

## Required broker settings {#required-broker-settings}

Thadam's guarantees depend on settings it does not control. The deployment runbook table (ADR-0006, verified in S2 against Mosquitto 2.1.2 and EMQX 5.8.6):

| Setting | Mosquitto 2.x | EMQX 5.8.x |
|---|---|---|
| Inflight window | `max_inflight_messages` (default 20). Size to at least rate × commit latency × 1.2 — e.g. 1000 for 5k msg/s at 100 ms commit | `mqtt.max_inflight` (default 32). **If ≥ ~1000, also raise `force_shutdown.max_mailbox_size`** (default 1000 killed sessions in S2) |
| Queue for an offline/slow client | `max_queued_messages` (default 1000), `max_queued_bytes`; size for outage × rate. **Drops the newest, silently** | `mqtt.max_mqueue_len` (default 1000). **Drops the oldest.** The queue lives in process memory; `force_shutdown.max_heap_size` (default 32 MB) can kill the session |
| Session survival across broker restart | `persistence true` (verified) | **Default does not survive a restart** (sessions in memory); `mqtt.session_expiry_interval` defaults to 2 h |
| Message size | `message_size_limit` at or below Thadam's `max_message_bytes` (unlimited by default) | `mqtt.max_packet_size` (default 1 MB) — align with `max_message_bytes` |
| Keepalive grace | ≈ 1.5× (7.3 s observed at 5 s) | 2.0× observed at 5 s (multiplier setting 1.5) |
| Watch the logs for | `Outgoing messages are being dropped` | `dropped_msg_due_to_mqueue_is_full`, `check_oom_shutdown`, `maximum heap size reached` |

Two structural caveats:

- **An oversized message cannot be skipped.** The client codec fails the connection instead (and with a persistent session the broker redelivers it every reconnect — an infinite loop). Prevention is the only remedy: keep the broker limit aligned with `max_message_bytes`.
- **Broker-side overflow is invisible to producers.** Under `ack_mode: on_commit` and `on_full: block`, loss moves silently into these queues. Monitor the broker, the `mqtt.gap*` attributes, and Thadam's metrics.

**Version pin.** Tests, demos, and documentation pin EMQX to the last open-source line, 5.8.x (5.8.6 in S2), by exact tag. Newer Enterprise-licensed images are not used, by owner decision (2026-10-07).

## Throughput ceiling

With `ack_mode: on_commit`, one connection is bounded by roughly `window ÷ (commit latency + RTT)`, a model S2 reproduced within 1–6%: about 200–320 msg/s per connection at a 100 ms commit with default windows (20 Mosquitto, 32 EMQX), about 1,700 msg/s at the provisional 10 ms WAL group commit with window 20. Mitigations — producer batching, raised broker limits, MQTT 5 Receive Maximum, `ack_mode: on_receive` — in [delivery guarantees](../concepts/delivery-guarantees.md#throughput-ceiling).

## Metrics and status

Source-side counters feed `thadam_source_messages_total`, `thadam_source_bytes_total`, `thadam_source_dropped_total{reason}`, `thadam_source_restart_total`, and the acknowledgement latency histogram `thadam_source_ack_latency_seconds` — see the [metrics reference](../operations/metrics.md). Connector detail (epoch, session state, ack pipeline, queue sizes) is in the [`/status`](../operations/admin-api.md#status) component snapshot.

## Operating notes

- Keep `client_id` stable; a new id is a new session, and the broker forgets the old one's queue when its expiry passes.
- A clean shutdown sends DISCONNECT so the persistent session keeps unacknowledged messages; a crash relies on broker redelivery, which is why redelivery duplicates are expected and counted.
- One connection per source in the MVP (D23). Shared subscriptions are a documented future option with an ordering caveat.
