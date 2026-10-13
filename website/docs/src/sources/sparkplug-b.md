# Sparkplug B decoding

**Status: Available (pre-alpha), with documented limits** ([below](#limits)). `decode: sparkplug_b` is a dataset decoder, not a source: an [MQTT source](mqtt.md) captures the raw `spBv1.0` protobuf messages into the WAL, and the chain decodes them. Design record: the Sparkplug spike in `docs/design/spikes/sparkplug-b-and-opcua-pubsub.md`; the wire schema was written for Thadam from the Sparkplug 3.0.0 specification (chapter 6), not copied from another implementation.

Thadam is a passive consumer: it subscribes and records, and **never publishes** to a Sparkplug namespace (no STATE, no rebirth requests, no commands).

## Configuration

Point a dataset at the Sparkplug topics and set `decode: sparkplug_b`. This is the configuration the Docker end-to-end test runs (Mosquitto 2, a Rust publisher, local Parquet):

```yaml
sources:
  - id: plant
    kind: mqtt
    config:
      url: "mqtt://127.0.0.1:1883"
      client_id: thadam-sparkplug-e2e
      clean_start: false
      topics: [{ filter: "spBv1.0/#", qos: 1 }]
      ack_mode: on_commit

datasets:
  - id: sp
    match: { source: plant, topic: "spBv1.0/#" }
    decode: sparkplug_b
    identity:
      group: { id: 100, template: "{topic.1}" }
      node: { id: 101, template: "{topic.3}" }
    time: { field: /timestamp, unit: ms, domain: utc }
    fields:
      - { id: 1000, name: kind, type: str, from: /kind }
      - { id: 1001, name: device, type: str, from: /device }
      - { id: 1002, name: seq, type: u64, from: /seq }
      - { id: 1003, name: bd_seq, type: u64, from: /bd_seq }
      - { id: 1004, name: temp, type: f32, from: /metrics/temp }
      - { id: 1005, name: speed, type: f64, from: "/metrics/motor.speed" }
```

**The dataset must match the BIRTH and DEATH topics, not only DATA.** The decoder learns metric names and aliases from NBIRTH and DBIRTH messages, and a message is decoded only when a `decode: sparkplug_b` dataset in the chain matches its topic. A dataset that matches only `spBv1.0/+/NDATA/#` never sees a BIRTH and cannot resolve aliases. Use `spBv1.0/#`, or `spBv1.0/<group>/#`.

Topic positions for `{topic.N}` are `0` = `spBv1.0`, `1` = group, `2` = message kind, `3` = edge node, `4` = device (device topics only). Use `device` from the payload for the device id, since node-level topics have no position 4.

## Output shape: one event per message

Each message becomes one JSON document that the dataset's fields read with JSON Pointers, exactly like `decode: json`:

| Key | Value |
|---|---|
| `group`, `node` | From the topic. |
| `device` | The device id, or `null` for node-level messages. |
| `kind` | `NBIRTH`, `NDATA`, `NDEATH`, `DBIRTH`, `DDATA`, or `DDEATH`. |
| `seq` | The payload `seq` (0 to 255), or `null`. |
| `bd_seq` | The node's current session counter (`bdSeq`), when known. |
| `timestamp` | The payload timestamp in milliseconds since the epoch; use it as `time` with `unit: ms`. |
| `metrics` | Metric name to value, with every alias already resolved to its name. |
| `samples` | Present only when a Sparkplug dataset uses `explode`: a list of `{name, datatype, value, is_null, timestamp, alias}`. |

A metric named `Line/Mode` is read with `from: /metrics/Line~1Mode` (JSON Pointer `~1` for `/`). Integers keep their width and sign (Int8 to Int64 are signed, UInt8 to UInt64 unsigned), floats are floats, and an explicit Sparkplug null is a JSON null.

**Per-metric rows.** The default is one event per message, which keeps the metrics of one scan together and keeps the row rate at the message rate. For one row per metric use the existing [`explode`](../configuration/datasets.md#explode) over `/samples` (`explode: { path: /samples, index_field: n }`, fields with `from: /name`, `/value`, and `^/kind` for the parent). A declared field has one type, so `explode` fits metric sets of one type (or unknown width); mixed-type sets should use the `metrics` object, because a row whose value does not fit the declared type is rejected.

**What is emitted.**

- **BIRTH** messages are events (`kind` says which) with their initial values: a free state snapshot.
- **DEATH** messages are events with no metrics (`metrics` is empty). Add a [`filter`](../configuration/transforms.md) on `kind` if you do not want them.
- **STATE** (host application JSON) and **NCMD/DCMD** messages are not telemetry and are dropped, counted as `filtered`. The raw bytes stay in the WAL.
- A message that is not valid Sparkplug protobuf, or a topic outside `spBv1.0/<group>/<kind>/<node>[/<device>]`, is a reject to quarantine (`decode.invalid_payload`) with the raw bytes kept. Error text never includes payload bytes.

## Quality mapping

The decoder's observations become the event's quality flags and the `_quality_reason` text:

| Observation | Flag | Status | Reason text |
|---|---|---|---|
| `seq` is not the successor of the previous one (0 to 255, wrapping) | `SEQUENCE_GAP` | `good` | `sparkplug.seq expected N got M` |
| DATA for a node or device past its DEATH | `STALE` | `uncertain` | `sparkplug.past-ndeath` |
| A metric alias missing from the BIRTH table (named `alias:N`), an unsupported value, a missing `seq`, a non-finite float, a wrong topic shape | `DECODE_ERROR` | `good` | e.g. `sparkplug alias N` |

Values stay trustworthy across a gap, so a gap does not lower the status. A reset on NBIRTH is not a gap. An NDEATH that carries a `bdSeq` other than the current session's is a late will message and is ignored. The flags combine with the dataset's own (for example `CLOCK_SKEW`), and the reason lists at most three notes.

## Birth store and replay

Sparkplug DATA carries aliases, and the table that maps them to names was published once, in a BIRTH that WAL retention may have trimmed by the time the DATA is read. Each chain therefore keeps the latest BIRTH of every edge node and device on disk:

```text
<data_dir>/sparkplug/births/<sink id>/<sha-256 of group/node/device>.birth
```

- Records hold the raw BIRTH bytes, the session `bdSeq`, the BIRTH's `seq`, and a dead flag set by an NDEATH. Writes use the [schema store](../operations/data-directory.md#the-schema-store-schemas) discipline: write a temporary file, fsync, rename, fsync the directory. Reads verify a content digest, so a damaged file fails loudly.
- BIRTH and DEATH state is written and fsynced **before** the events of that message are handed to the sink. If the write fails, the message is rejected with the store's `buffer.io` error (raw bytes in quarantine) rather than being decoded against state that would not survive a restart.
- At start the chain loads the store and seeds its decoder, so DATA after a restart resolves without a new BIRTH. A damaged store fails the chain build (the pipeline does not start) and is never skipped silently.
- There is one store per sink chain, because chains run at different WAL positions and a shared store would let a lagging chain put an older BIRTH over a newer one.
- **Replay** (`thadam replay`) seeds from the sink's production store and **never writes it**: replaying history must not overwrite the seeds production restarts from. Warm-up after a chain rebuild also does not write.

## Limits {#limits}

- **Template and DataSet metrics are not decoded.** They yield a null value, `DECODE_ERROR`, and the reason `sparkplug unsupported value for <name>`. Packed array datatypes arrive as raw bytes (a JSON list of byte values); the metric's datatype tells how to unpack them. Property sets, metric metadata, `is_historical`, and `is_transient` are ignored.
- **A `seq` gap that spans a restart is not detected.** The `seq` cursor is not persisted (persisting every `seq` would cost an fsync per message): the first message after a restart anchors the cursor, so a gap inside the downtime is invisible. Within a run, gaps are flagged.
- **A message is decoded only if a dataset in the chain matches it** (see [Configuration](#configuration)).
- **The birth store is a cache, not the source of truth.** The WAL holds everything it holds. The store can lag the newest committed BIRTH by one crash window; the next BIRTH resyncs it.
- **No cap on tracked nodes and devices.** Memory and store size grow with the number of distinct group, node, and device ids that send a BIRTH.
- Sparkplug telemetry is QoS 0 by specification. Thadam has no way to prevent loss at the MQTT level; gaps surface as `SEQUENCE_GAP`.
- The decoder is covered by unit tests, a `cargo fuzz` target (`thadam-sparkplug`, `decoder_handle`), and an ignored Docker test. There are no throughput benchmarks and no tests against real Sparkplug edge devices.
