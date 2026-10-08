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

| Kind | Role | Status |
|---|---|---|
| `mqtt` | source | Implemented ([MQTT source](../sources/mqtt.md)) |
| `parquet` | segment sink | **In progress**: registered, but the factory is a placeholder; `thadam run` fails to build a pipeline that uses it ([planned behavior](../sinks/parquet.md)) |
| `object_store` | batch sink / shipper | **In progress**, same caveat ([planned behavior](../sinks/object-store.md)) |
| ClickHouse | batch sink | **Planned (P1)**; not registered with this binary ([design](../sinks/clickhouse.md)) |

Because the sink factories are placeholders today, any configuration with a sink validates but the pipeline build fails at `thadam run` — the error names the kind. This is the expected mid-build-out state ([project status](../introduction.md#project-status)).

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
