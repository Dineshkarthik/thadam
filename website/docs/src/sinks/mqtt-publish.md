# MQTT publish sink

**Status: Implemented.** The `mqtt_publish` kind is a batch sink that publishes one dataset's events to an MQTT broker, one message per event, as JSON. Use it to fan captured events back out to MQTT subscribers (dashboards, alerting, other edge services) from the replay edge.

## How it works

1. The runtime forms batches on WAL frame boundaries and hands each batch to the sink with a stable batch id.
2. The sink renders each event's topic from the `topic` template, encodes the event as one JSON document, and publishes it. At most `max_in_flight` publishes are unacknowledged at any moment. For QoS 1, `write` returns when every event is acknowledged (PUBACK) or individually rejected. For QoS 0, `write` returns once every message has been written to the connection.
3. A connection lost mid-batch fails the write as transient. The runtime retries the same batch, so the broker may see some events more than once.

## Configuration

```yaml
sinks:
  - id: mqtt-out
    kind: mqtt_publish
    config:
      datasets: [imu]                        # exactly one dataset per sink entry
      url: mqtts://broker.internal:8883      # mqtt://host[:1883] or mqtts://host[:8883]
      client_id: thadam-edge-01               # stable identifier of this publisher (1-256 chars)
      topic: thadam/{dataset}/{robot}         # template; {dataset} + identity level names
      qos: 1                                 # 0 or 1 (default 1)
      retain: false                          # default
      payload: json                          # the only payload
      tls:                                   # optional; only with mqtts://
        ca: /etc/thadam/ca.pem                #   trust root for the broker
        cert: /etc/thadam/client.pem          #   client certificate (requires key)
        key: /etc/thadam/client.key
      username: thadam                        # optional; requires password_file
      password_file: /run/secrets/mqtt_pass  # read at open, never logged
      max_in_flight: 100                     # unacknowledged publishes (1-65535, default 100)
      request_timeout: 10s                   # per-publish acknowledgement timeout (whole seconds, default 10s)
      max_message_bytes: 1048576             # larger encoded events are rejected individually (default 1 MiB)
      keep_alive: 30s                        # whole seconds (default 30s)
      connect_timeout: 10s                   # whole seconds (default 10s)
```

| Key | Type | Default | Notes |
|---|---|---|---|
| `datasets` | list of one id | every dataset | the one dataset this sink publishes; required when the pipeline declares two or more datasets |
| `url` | string | required | `mqtt://` or `mqtts://` only; no path, userinfo, query, or fragment |
| `client_id` | string | required | 1-256 characters; one publisher per id on a broker |
| `topic` | string | required | see [topic templates](#topic-templates) |
| `qos` | integer | `1` | `0` at most once, `1` at least once |
| `retain` | boolean | `false` | sets the retain flag on every publish |
| `payload` | `json` | `json` | the only payload format |
| `tls` | object | none | `ca`, `cert`, `key`; requires `mqtts://`; `cert` and `key` go together |
| `username` | string | none | requires `password_file` |
| `password_file` | string | none | a file holding the password; resolved at open |
| `max_in_flight` | integer | `100` | bounds the payloads held at once |
| `request_timeout` | duration | `10s` | how long a publish may wait for its PUBACK |
| `max_message_bytes` | integer | `1048576` | an encoded event over this size is a per-event rejection |
| `keep_alive` | duration | `30s` | whole seconds |
| `connect_timeout` | duration | `10s` | whole seconds |

Durations are whole seconds (`30`) or a string such as `500ms`, `30s`, `5m`, `1h`. `keep_alive` and `connect_timeout` take whole seconds only. The schema is closed: an unknown key fails validation. Run `thadam validate` to check a configuration without connecting.

Notes:

- Secrets are plain strings already resolved by `${env:}`/`${file:}` interpolation ([secrets](../configuration/index.md#secrets)); the sink never logs them.
- A `kind: mqtt_publish` sink publishes exactly one dataset; add one sink entry per dataset. Every identity level named in `topic` must belong to that dataset, or the sink fails at `validate`/`open` rather than per event.

## Topic templates

`topic` is a template. `{dataset}` renders the dataset id; any other placeholder renders the identity level of that name.

```yaml
topic: thadam/{dataset}/{robot}    # thadam/imu/r7
```

- The literal text of the template may not contain the wildcards `+` or `#`, a NUL character, an empty level (`a//b`), or a leading `$` (reserved by brokers). These fail at validation.
- Each rendered value must be non-empty and must not contain `/`, `+`, `#`, or NUL. A value that breaks these rules, or an event that lacks a level the template names, is rejected individually to quarantine; the rest of the batch proceeds.

## The JSON envelope

Each event publishes as one JSON object merging three regions. Later regions never overwrite an earlier key, so `_event_id` is always the event id even when a record field has the same name:

```json
{
  "_event_id": "0123456789abcdef0011223344556677",
  "_ingest_time": "2026-10-10T06:00:00.000000001Z",
  "_event_time": "2026-10-10T05:59:59.500000000Z",
  "_quality_status": "good",
  "robot": "r7",
  "temp": 43.25
}
```

- `_event_id` — the event id, lowercase hex. Use it to deduplicate.
- `_ingest_time` — edge receipt time, RFC 3339 with nanosecond precision, always present. Times are text because integer nanoseconds would lose precision through a JSON number beyond 2^53.
- `_event_time` — the event time when the event carries one, else `null`.
- `_quality_status` — `good` | `uncertain` | `bad`.
- Identity levels appear as `"level": "value"` members; the dataset's record fields follow. Integers stay numbers, floats are numbers (`null` when NaN or infinite, since JSON has neither), strings stay strings, `bytes` values are lowercase hex, timestamps are RFC 3339 text, and lists and records nest.

`payload: json` expects decoded record events. A raw-payload or object-reference event is rejected individually.

## Delivery guarantees

| `qos` | Reported class | Behaviour |
|---|---|---|
| `1` | `at_least_once` | A batch that fails in flight is retried whole, so the subscriber may see an event more than once. Deduplicate by `_event_id`. |
| `0` | `at_most_once` | A write that failed in flight may be lost. It is never applied twice. |

- **Retries.** Network loss, a dropped connection, and an acknowledgement timeout are transient: the runtime retries the same batch with the same batch id. A publish is only counted as delivered on a PUBACK (QoS 1) or a write to the connection (QoS 0) attributed to it; the sink never reports a false acknowledgement.
- **Per-event rejections.** A topic that cannot be rendered, or an encoded message over `max_message_bytes`, is a permanent rejection of that event alone. The event goes to quarantine and the batch proceeds.
- **Fatal errors.** Configuration and credential failures stop the sink.
- **Restarts.** The sink reconnects on the next write after a broker restart. Events published while the broker was down fail transiently and are retried.
- **Packet-id collisions.** When a connection drops while a packet-id collision is outstanding, the MQTT client may later write the colliding publish under its old id. The sink detects that the attribution can no longer be trusted, drops the client and connection, and connects again before its next publish, so the new publish starts a fresh session. This happens inside the sink and needs no configuration. The outcome is still at-least-once for QoS 1.

## TLS

- `mqtts://` with no `tls` block uses the platform trust roots.
- `tls.ca` replaces the platform roots with the given CA file. With `tls.cert` and `tls.key` the sink presents a client certificate (mutual TLS).
- Setting `tls` on a `mqtt://` URL is a configuration error.

## Integration tests

Docker-gated tests run the sink against real brokers, Mosquitto 2 and EMQX 5.8.6, both pinned by tag and drawn from public Docker Hub:

```text
cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(docker_publish)'
```

They cover a QoS 1 batch and a QoS 0 batch received by a test subscriber with the expected topics and envelopes, a broker restart mid-batch (the write fails transiently and a retry delivers every event at least once), TLS and mutual TLS through the relay, and the batch-sink conformance suite. Each test starts its own container, labelled `thadam-mqtt-test`, and removes it when the test ends, including on panic. To clean up after a killed run: `docker rm -f $(docker ps -aq --filter label=thadam-mqtt-test)`.
