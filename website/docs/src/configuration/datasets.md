# `datasets`

Datasets are the output tables. Each one selects messages, decodes their payloads, derives identity and time, and declares the columns every row will have. Declared schemas are required — this is what makes Parquet output deterministic and schema evolution explicit.

```yaml
datasets:
  - id: telemetry
    match: { source: fleet-mqtt, topic: "thadam-demo/v1/+/+/telemetry" }
    decode: json
    identity:
      site:  { id: 100, template: "{topic.2}" }
      robot: { id: 101, template: "{topic.3}" }
    event_id: { key: [site, robot, seq] }
    dedup: { window: 10m, max_keys: 500k }
    explode: { path: /samples, index_field: sample_idx }
    time: { field: /ts_us, unit: us, domain: utc, max_skew: 5s }
    fields:
      - { id: 1000, name: seq,        type: u64,        from: /seq, required: true }
      - { id: 1002, name: voltage_v,  type: f32,        from: /voltage_mv, unit: mV, convert_to: V }
      - { id: 1005, name: temps_c,    type: "list<f32>", from: /temps_dc,   unit: dCel, convert_to: Cel }
```

## Dataset keys

| Key | Type | Required | Description |
|---|---|---|---|
| `id` | string | yes | Unique. Used in paths, object keys, and metric labels. Same character rules as connector ids. |
| `match` | mapping | yes | Which messages feed the dataset. |
| `decode` | `json`, `ros2`, or `sparkplug_b` | yes | The payload decoder. `ros2` decodes ROS 2 CDR payloads from an [MCAP source](../sources/ros2.md) through the schema store; its flattened dotted field names become the payload keys. `sparkplug_b` decodes Sparkplug B protobuf from an [MQTT source](../sources/mqtt.md), resolving aliases through BIRTH messages and the birth store; see [Sparkplug B decoding](../sources/sparkplug-b.md). |
| `identity` | mapping | no | The asset-path levels, **in the order written**. Order matters: it is the hierarchy and part of the config hash. |
| `event_id` | mapping | no | Content-derived event IDs; see [below](#event_id). |
| `dedup` | mapping | no | Message-level deduplication; see [below](#dedup). |
| `explode` | mapping | no | Split an array-valued message into one event per element; see [below](#explode). |
| `time` | mapping | no | Source-timestamp extraction; see [below](#time). |
| `fields` | list of mappings | yes (≥ 1) | The declared schema; at most 4,096 fields per dataset (`MAX_SHAPE_FIELDS`). |

### `match` {#match}

| Key | Type | Required | Description |
|---|---|---|---|
| `source` | string | yes | The `id` of a source. Unknown ids are errors. |
| `topic` | string | no | A topic filter matched against the message's topic attribute. For MQTT sources the MQTT rules apply: `+` matches one level, `#` matches the remainder and must be last. Omit to match every message from the source. |

### `identity` {#identity}

Each level is `name: { id, template }`:

| Key | Type | Description |
|---|---|---|
| `id` | integer 100–199 | The stable Parquet field ID of the level's column, unique in the dataset. The older bare-string form (`site: "{topic.2}"`) is refused with a hint. |
| `template` | string | The value of the level. `{topic.N}` is the Nth topic level (0-based); everything else is literal text. Placeholders are validated against the `match.topic` filter's level count when it has no `#`. |

- Level names start with a letter and use letters, digits, `_` (a leading `_` is reserved; envelope columns start with `_`).
- At most 32 levels per dataset (`MAX_ASSET_LEVELS`).
- Levels become columns in every row of the dataset.
- Because `identity` is a hierarchy, it must keep the written order all the way to the transform chain; it is hashed and passed as an ordered list, never an unordered map.

### `event_id` {#event_id}

```yaml
event_id: { key: [site, robot, seq] }
```

| Key | Type | Description |
|---|---|---|
| `key` | list of names | Identity levels and/or declared fields whose values form the event ID. At least one name; each must exist in this dataset. |

Without `event_id`, IDs are derived from the WAL position (`xxh3_128` over epoch, frame sequence, record index): deterministic across replay, but different for a broker redelivery. With a content-derived key, a redelivered message hashes to the same ID as the original — which is exactly what deduplication needs.

### `dedup` {#dedup}

```yaml
dedup: { window: 10m, max_keys: 500k }
```

| Key | Type | Description |
|---|---|---|
| `window` | duration | How long a key is remembered, in event time order. Must be > 0. |
| `max_keys` | count | The most keys remembered; the oldest is evicted past this. Must be > 0. |

- Runs at **message level, before `explode`**, so one large message costs one key, not one key per element.
- Warm-started from the WAL tail `[cursor − window, cursor)` at startup, so dedup survives restarts deterministically.
- Best-effort by design (ADR-0011): a duplicate arriving after the window, or after `max_keys` eviction, is not detected. Authoritative deduplication belongs downstream (`ReplacingMergeTree`, `QUALIFY`, or `DISTINCT ON`).
- **`dedup` requires `event_id.key`.** Validation fails with `config.invalid` without it: the default WAL-position ID changes when a message is redelivered, so duplicates would never be recognized (ADR-0011).
- Memory shows up in the `thadam validate` estimate as `max_keys × 48 B` (upper bound).

### `explode` {#explode}

```yaml
explode: { path: /samples, index_field: sample_idx }
```

| Key | Type | Description |
|---|---|---|
| `path` | JSON Pointer | The array to split. Each element becomes one event. |
| `index_field` | name | Optional column receiving each element's index (0-based). It is carried as the event's sample index column, not as a payload field. Must be a valid field name and must not collide with a declared field or identity level. |

Inside an exploded dataset, a field's `from` may use `^/` to read from the **parent message** (for example a sequence number that applies to every sample): `from: "^/seq"`. `^/` is rejected in datasets without `explode`.

### `time` {#time}

```yaml
time: { field: /ts_us, unit: us, domain: utc, max_skew: 5s }
```

Exactly one of `field` (read the payload) or `attr` (read a message attribute) is required; giving both or neither is a config error.

| Key | Type | Required | Description |
|---|---|---|---|
| `field` | JSON Pointer | one of `field`/`attr` | Where the payload holds the timestamp. `^/` reads it from the parent message of an exploded sample. A dotted ROS 2 name is one token: `/header.stamp` (a flattened key). A nested JSON object uses separate tokens: `/header/stamp`. |
| `attr` | attribute name | one of `field`/`attr` | A message attribute holding the timestamp as decimal text, such as `mcap.publish_time`. The text is parsed in `unit`. `^` does not apply. |
| `unit` | `s`, `ms`, `us`, `ns`, `rfc3339` | yes | `s`, `ms`, `us`, `ns` are the unit of a numeric epoch timestamp, in the payload or in the attribute text. `rfc3339` means RFC 3339 text rather than an epoch number. A ROS 2 `Ts` value is unix nanoseconds, so use `unit: ns` with no conversion. |
| `default_offset` | UTC offset | no | The offset assumed for RFC 3339 text that carries none. Forms: `+HH:MM`, `-HH:MM`, `+HHMM`, `+HH`, and `Z`/`UTC`; hours ≤ 23, minutes ≤ 59. Only valid with `unit: rfc3339` — a config error otherwise. |
| `domain` | time domain | yes | `utc`, `tai`, `monotonic`, `sim`, or `unknown`. Only `utc` becomes the materialized `event_time`; explicitness is the point ([event model](../concepts/event-model.md#time-three-clocks-never-silently-merged)). |
| `max_skew` | duration | no | When a `utc` source time differs from ingest time by more than this, the event is flagged `CLOCK_SKEW` (never corrected). Must be > 0. |

```yaml
time: { field: /ts, unit: rfc3339, default_offset: "+02:00", domain: utc }
```

Without `default_offset`, RFC 3339 text that carries no offset is a runtime reject (the message is quarantined).

Examples for the attribute and ROS 2 forms:

```yaml
# ROS 2 builtin_interfaces/Time (header.stamp), already unix nanoseconds
time: { field: /header.stamp, unit: ns, domain: utc, max_skew: 5s }

# MCAP publisher clock, attached by the source
time: { attr: mcap.publish_time, unit: ns, domain: utc, max_skew: 5s }

# MCAP recorder clock
time: { attr: mcap.log_time, unit: ns, domain: utc }
```

`mcap.log_time` is recorder time: when the recorder wrote the message. `mcap.publish_time` and `header.stamp` are publisher time: the clock of the node that produced the message, which can drift from the recorder or the host. Neither is corrected; `max_skew` flags a gap against ingest time.

For `attr`, a missing attribute is the same as an absent payload field: the event is flagged `MISSING_TIME`. Attribute text that does not parse in `unit` is a reject (`decode.invalid_time`), the same as an unparseable payload timestamp.

### `fields` {#fields}

Each entry declares one column:

| Key | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | The column name: starts with a letter, then letters/digits/`_`; ≤ 63 characters. **Single dots are allowed between segments** — `pose.position.x` is *one literal flat column*, never nesting: each segment follows the same rule, so leading/trailing dots, empty segments (`a..b`), and digit-first segments are refused. Dotted names exist for the ROS 2 decoder's flattened shape, for example `header.frame_id`. Must not collide with an identity level or the `explode.index_field`. |
| `id` | integer 1000–4,194,303 | yes | The stable Parquet field ID. Unique within the dataset. **Never reuse the ID of a removed field** — IDs are part of the config hash and of long-term schema identity ([field IDs](#field-ids)). |
| `type` | field type | yes | `bool`, `i8`…`i64`, `u8`…`u64`, `f32`, `f64`, `str`, `bytes`, `ts`, or `list<T>` (nesting ≤ 3). `record` is not supported in config version 1. |
| `from` | JSON Pointer | no (default `/<name>`) | Where to read the value in the decoded payload; `^/` reads the parent message of an exploded sample. When omitted, the pointer is `/<name>` as **one pointer token**, so a dotted name reads the decoder's flattened key: a field named `pose.position.x` reads the payload key `pose.position.x` (not a nested `pose` object), consistent with the `ros2` bridge. |
| `required` | boolean | no (default `false`) | Reject the message when the field is missing or null. |
| `unit` | UCUM code | no | The unit of the **source** value (for example `mV`). |
| `convert_to` | UCUM code | no | Convert to this unit; the stored column carries the converted value and unit. Requires `unit`, and a numeric type (scalars and typed lists; conversion is element-wise on lists). Unknown or dimensionally incompatible units are rejects. |

## Parquet field IDs {#field-ids}

Every column carries a stable Parquet `field_id` from the first release, allocated from disjoint ranges (ADR-0013):

| Range | Owner |
|---|---|
| 1–99 | Thadam envelope columns, fixed by `thadam-arrow` (13 reserved) |
| 100–199 | Identity levels — explicit in config |
| 1000–4,194,303 | Declared top-level fields — explicit in config |
| ≥ 8,388,608 | Nested (list elements, record children) — derived from the parent ID, never assigned |

Rules: IDs are unique per dataset, ranges are enforced per kind, and **an ID is never reused** for a different column — even after the column is removed. Thadam cannot see history; retiring IDs is the configuration owner's job. This is what keeps a later Iceberg `fast_append` path cheap (no rewrite) and makes schema evolution (add optional columns, widen `int`→`long`, `float`→`double`; never rename or narrow) safe.

## Complete example

The six-dataset demo with dedup, explode, units, and quality transforms is [`examples/edge-capture/thadam/pipeline.yaml`](https://github.com/Dineshkarthik/thadam/blob/main/examples/edge-capture/thadam/pipeline.yaml).
