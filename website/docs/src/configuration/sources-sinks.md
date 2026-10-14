# `sources` and `sinks`

Sources and sinks are *connectors*: an `id`, a registered `kind`, and an opaque connector-specific `config` block. Both lists require at least one entry.

```yaml
sources:
  - id: fleet-mqtt          # unique id, used in metric labels and file names
    kind: mqtt              # a source kind registered with the binary
    config:                 # connector-specific; validated by the connector
      url: mqtt://broker:1883
      ...

sinks:
  - id: local
    kind: parquet           # a sink kind registered with the binary
    config: { ... }
    required: true          # WAL consumer policy (below)
    start_from: earliest
```

## Connector shell

| Key | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Unique within its section. 1–64 characters: letters, digits, `_`, `-`, starting with a letter or digit. Used in metric labels, cursor names, and file names. |
| `kind` | string | yes | Lowercase letters, digits, `_`. Must name a connector registered with the binary, or validation fails with `config.unknown_kind`. |
| `config` | mapping | no | Connector-specific configuration (`deny_unknown_fields` inside each connector). Kept as untyped JSON at this layer and validated by the connector factory, so connector options evolve independently of the config format. |

### Registered kinds

| Kind | Role | Feature flag | Status |
|---|---|---|---|
| `mqtt` | source | built-in | Available ([MQTT source](../sources/mqtt.md)) |
| `http` | source | built-in | Available ([HTTP source](../sources/http.md)) |
| `grpc` | source | built-in | Available ([gRPC source](../sources/grpc.md)) |
| `mcap` | source | built-in | Available ([ROS 2](../sources/ros2.md)); replays MCAP and rosbag2-MCAP recordings |
| `modbus` | source | built-in | Available ([Modbus source](../sources/modbus.md)); Modbus TCP polling |
| `can` | source | built-in | Available ([CAN source](../sources/can.md)); Linux SocketCAN (`raw` and DBC `decoded`) |
| `opcua` | source | `opcua` (off by default) | Available ([OPC UA source](../sources/opcua.md)) |
| `ros2_live` | source | `ros2-live` (off by default) | Available ([ROS 2 live source](../sources/ros2-live.md)); `rmw_zenoh` transport, experimental |
| `parquet` | segment sink | built-in | Available ([Parquet segments sink](../sinks/parquet.md)) |
| `object_store` | shipper service | built-in | Available ([object storage sink](../sinks/object-store.md)); configured under `sinks:` and runs as a service shipping the named segment sink |
| `iceberg` | registrar service | `iceberg` (off by default) | Available ([Apache Iceberg registrar](../sinks/iceberg.md)); configured under `sinks:`, experimental (P2) |
| `nats` | batch sink | built-in | Available ([NATS JetStream sink](../sinks/nats.md)); one message per event, deduplicated by `Nats-Msg-Id` |
| `kafka` | batch sink | built-in | Available ([Apache Kafka sink](../sinks/kafka.md)); one record per event, `acks=all` |
| `mqtt_publish` | batch sink | built-in | Available ([MQTT publish sink](../sinks/mqtt-publish.md)); one message per event, QoS 0 or 1 |
| `clickhouse` | batch sink | `clickhouse` (on by default) | Available ([ClickHouse sink](../sinks/clickhouse.md)); inserts into an existing table, never creates or alters tables |

## Sink WAL-consumer policies {#sink-policies}

Each sink entry takes three optional keys next to `id`, `kind`, and `config`. They control how the sink participates in WAL retention — who decides when old segments may be deleted. Keys at their defaults are omitted from serialization and from the config hash.

| Key | Type | Default | Description |
|---|---|---|---|
| `required` | boolean | `true` | Whether the WAL keeps data until this sink has consumed it. `false` makes the sink an optional consumer: it never holds retention back, and if it falls behind eviction it sees a `Gap::Evicted` in its reader. |
| `max_lag` | `{ bytes: <size> }` or `{ age: <duration> }` | none | How far a **required** sink may fall behind before it stops holding WAL data back — an explicit, counted loss for that sink. One key only, and a zero value is invalid. Refused on a `required: false` sink (an optional sink never holds retention, so there is nothing to limit). |
| `start_from` | `earliest` or `latest` | `earliest` | Where a sink with **no committed cursor** starts. `latest` commits the cursor at the current durable end during startup, so a restart cannot skip data. A sink that already has a cursor always resumes from it, ignoring this key. |

Examples:

```yaml
# An analytics sink that may not starve the WAL of the capture path:
- id: scratch-analytics
  kind: parquet
  required: false
  config: { ... }

# A required sink that must not pin more than 2 GiB of WAL:
- id: local
  kind: parquet
  max_lag: { bytes: 2GiB }
  config: { ... }
```

## Secrets in connector config

Interpolated values inside `config:` blocks are tracked per connector, wrapped in `Secret<T>`, and redacted from logs and diagnostics. See [secrets](index.md#secrets).
