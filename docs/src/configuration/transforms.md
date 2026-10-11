# `transforms` and predicates

The `transforms:` list applies quality, projection, and filtering steps to datasets, in listed order, per sink chain. Every step takes a `dataset` selector and an optional `sinks` restriction.

**Config version 1 accepts all ten kinds** the transform engine implements: `validate`, `on_change`, `filter`, `sample`, `enrich`, `rename`, `project`, `cast`, `pose`, and `joint_state`. See also the [transform reference](../transforms/index.md).

Each step is checked against the dataset's fields **as left by the earlier steps** for that dataset (and for the sinks the step applies to). After `rename`, `project`, `cast`, or `enrich`, later steps use the new names and types.

## Step shell

```yaml
transforms:
  - kind: validate | on_change | filter | sample | enrich | rename | project | cast | pose | joint_state
    dataset: telemetry            # the dataset the step applies to (required, single id)
    sinks: [local]                # optional: restrict the step to these sink ids
    ...                           # kind-specific keys
```

| Key | Type | Required | Description |
|---|---|---|---|
| `kind` | `validate`, `on_change`, `filter`, `sample`, `enrich`, `rename`, `project`, `cast`, `pose`, `joint_state` | yes | The step type. Unknown kinds are configuration errors. |
| `dataset` | string | yes | The dataset id. Unknown ids are errors. |
| `sinks` | list of sink ids | no | Restricts the step to these sinks; absent means **every sink**. An empty list is an error; unknown sink ids are errors. |

## `kind: validate` {#validate}

Assigns quality flags and a status to events whose fields break a range rule. Events are **flagged, not dropped** — quality is data.

```yaml
- kind: validate
  dataset: telemetry
  rules:
    - { field: motor_temp_c, min: -40, max: 150, flag: OUT_OF_RANGE, status: bad }
    - { field: temps_c, min: -40, max: 150, flag: OUT_OF_RANGE, status: uncertain }
```

| Key | Type | Required | Description |
|---|---|---|---|
| `rules` | list of rules (≥ 1) | yes | Each rule checks one field. |

Each rule:

| Key | Type | Required | Description |
|---|---|---|---|
| `field` | string | yes | A numeric (or `list<numeric>`) field of the dataset. Unknown names are errors. On a list, every element must be in range. |
| `min` | number | no | Inclusive lower bound. |
| `max` | number | no | Inclusive upper bound. |
| `flag` | quality flag | yes | The flag raised on violation, from the known set: `OUT_OF_RANGE`, `CLOCK_SKEW`, `MISSING_TIME`, `OUT_OF_ORDER`, `DUPLICATE`, `STALE`, `SEQUENCE_GAP`, `DECODE_ERROR`, `INVALID`. Unknown flags are errors. |
| `status` | `good`, `uncertain`, `bad` | yes | The quality status assigned on violation. |

A null field is not checked. Status and flag land in the `_quality_status`, `_quality_flags`, and `_quality_reason` columns.

## `kind: on_change` {#on_change}

Passes an event only when a watched field changed value, or when the heartbeat elapses — deadband-style reduction for slow-changing state. It is **lossy by design** and it is not deduplication (product.md UC-05 conflates the two; ADR-0011 D17 keeps them separate).

```yaml
- kind: on_change
  dataset: state
  fields: [mode, cycle_id, program]
  heartbeat: 5m
```

| Key | Type | Required | Description |
|---|---|---|---|
| `fields` | list (≥ 1) of field names or `{ field, deadband }` entries | yes | Declared fields compared with the previous kept event. Unknown names are errors; listing the same field twice is an error. |
| `heartbeat` | duration | no | Pass an event at least this often even when nothing changed (measured in ingest time). Must be > 0 when present. |

An entry is either a bare name or a mapping:

```yaml
fields:
  - mode
  - { field: soc_pct, deadband: 0.5 }
```

| Key | Type | Required | Description |
|---|---|---|---|
| `field` | string | yes | A declared field. |
| `deadband` | number | no (default `0`) | Absolute deadband: a change must **exceed** it to count. Must be finite and ≥ 0. A positive deadband is valid only on numeric fields or lists of numbers; on any other field it is an error. |

`deadband: 0` is the same as the bare name and hashes identically.

## `kind: filter` {#filter}

Drops events that do not match a structured predicate; dropped events are counted as `filtered`, and they are recoverable by replaying with a corrected predicate.

```yaml
- kind: filter
  dataset: events
  where: { field: level, ne: debug }
```

| Key | Type | Required | Description |
|---|---|---|---|
| `where` | predicate | yes | Events for which it is false are dropped and counted. |
| ~~`expr`~~ | — | — | **Reserved** for a future expression language (ADR-0018); setting it is an error. |

## Predicate grammar {#predicates}

Structured YAML predicates (ADR-0018) — composable, type-checked at config time, deterministic in replay.

```yaml
# One condition
where: { field: level, ne: debug }

# Combinators
where:
  all:
    - { field: level, ne: debug }
    - any:
        - { field: site, eq: plant-2 }
        - { field: soc_pct, min: 10 }
  # or:  not: { field: code, in: [E100, E200] }
```

**Combinators:** `all` (every child true), `any` (any child true), `not` (child false). A condition names a `field` and exactly one test:

| Test | Form | Meaning |
|---|---|---|
| `eq` | `{ field: f, eq: v }` | equal to |
| `ne` | `{ field: f, ne: v }` | not equal to |
| `lt`, `le`, `gt`, `ge` | `{ field: f, lt: v }` | ordered comparisons (numbers) |
| `in` | `{ field: f, in: [v, ...] }` | membership in a literal set |
| `min` / `max` | `{ field: f, min: a, max: b }` | inclusive range, either or both; on a list field, every element |
| `null` | `{ field: f, null: true }` | `true` when the value is null or missing; `null: false` is `true` when present and non-null. Takes a YAML boolean only. |

Rules (checked by `thadam validate`):

- **Fields** resolve against the dataset: declared fields, identity levels, the `explode` index field, and the quality names `quality.status`, `quality.flags`, `quality.reason`. Typo'd names are errors with suggestions.
- **Literal typing**: integer fields take integer literals that fit (`u64` values above `i64::MAX` stay exact, never widened to float); float fields compare in their own domain.
- **Null semantics**: a null or missing field makes `eq`, the ordered comparisons, `in`, `min`, and `max` false, and `ne` true. `NaN` is unordered. To test for null, use the `null:` test; `eq: null` is rejected.
- **`null` test**: allowed on any resolvable name — fields, identity levels, the `explode` index field, and `quality.*`. A condition still takes exactly one test. `null:` needs no quoting in YAML.
- **Cost limits**: predicate nodes, depth, and `in`-set size are bounded by chain limits; overflow is a configuration error, not a runtime surprise.
- An evaluation that still cannot complete at runtime becomes a `transform.eval_failed` reject with the raw bytes kept.

## `kind: sample` {#sample}

Thins events per asset, in ingest order. Lossy by design; dropped events are not recoverable except by replay.

```yaml
- kind: sample
  dataset: imu
  every_n: 10
  min_interval: 100ms
```

| Key | Type | Required | Description |
|---|---|---|---|
| `every_n` | count ≥ 1 | one of the two | An integer, or text with a `k`/`m` suffix. An asset's first event and every Nth after it pass the count rule; every event counts, whether or not it is kept. |
| `min_interval` | duration > 0 | one of the two | An event is kept only if at least this much **ingest time** passed since the asset's last kept event. |

At least one of `every_n` and `min_interval` is required (`config.invalid` otherwise). With both, an event must pass the count rule first, then the interval rule.

## `kind: enrich` {#enrich}

Adds fields from a static CSV or YAML lookup table.

```yaml
- kind: enrich
  dataset: telemetry
  file: robots.yaml
  key: { field: robot, column: robot_id }
  add:
    - { name: payload_kg, type: f32, unit: kg }
    - { name: cell, type: str, column: cell_name }
  on_miss: null
```

| Key | Type | Required | Description |
|---|---|---|---|
| `file` | path | yes | The table file; must not be empty. A relative path resolves against the config file's directory. |
| `format` | `csv`, `yaml` | no | Inferred from the extension (`.csv`, `.yaml`, `.yml`). Required when the extension is anything else. |
| `key` | mapping | yes | How events find their row; see below. |
| `add` | list of mappings (≥ 1) | yes | The fields to add; see below. |
| `on_miss` | `null`, `reject`, `drop` | no (default `null`) | When the key has no row: `null` sets the added fields to null; `reject` rejects the event; `drop` drops it. Omitted from the canonical (hashed) form when `null`. An unquoted `on_miss: null` works; no quoting is needed. |

`key`:

| Key | Type | Required | Description |
|---|---|---|---|
| `field` | string | yes | A declared string or integer field of the dataset (as left by earlier steps) or an identity level. |
| `column` | string | yes | The table column holding the key. |

Each `add` entry:

| Key | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | The new field's name: a valid field name, unique within `add`, and not colliding with an existing field or identity level. |
| `type` | field type | yes | Any field type except `bytes`, nested lists, and `list<ts>`. |
| `column` | string | no (default: `name`) | The table column to read. Must not be empty if given. |
| `unit` | `s`, `ms`, `us`, `ns`, `rfc3339` | for `ts` | The timestamp encoding of a `ts` field. Must not be empty if given. |

Table layout:

- **CSV** has a header row. CSV cannot hold `list<...>` columns; use YAML for those.
- **YAML** is either a list of mappings (each holding the key column) or a mapping from key to a mapping of columns.

The file is read when the transform chain is compiled at startup, **not** when `thadam validate` checks the config, so a missing or malformed file is not a config-validation error. Its SHA-256 is recorded for lineage, as the enrichment hash that replay row-set equivalence depends on. Added fields have no Parquet field ID declared in config.

## `kind: rename` {#rename}

Renames fields. Values are untouched.

```yaml
- kind: rename
  dataset: telemetry
  fields: { temp_c: temperature_c, volts: voltage_v }
```

| Key | Type | Required | Description |
|---|---|---|---|
| `fields` | mapping old name → new name (≥ 1) | yes | Every old name must be a field of the dataset as left by earlier steps; identity levels cannot be renamed. New names must be valid field names, unique, and must not collide with a field that remains or with an identity level. |

Renames apply simultaneously, so swapping two names is allowed. Renaming a field to its own name is a no-op.

## `kind: project` {#project}

Keeps only the listed fields.

```yaml
- kind: project
  dataset: telemetry
  fields: [seq, soc_pct, voltage_v]
```

| Key | Type | Required | Description |
|---|---|---|---|
| `fields` | list of field names (≥ 1) | yes | The fields to keep, in the listed order. Each must be a field of the dataset as left by earlier steps; duplicates are errors. Unlisted fields are dropped. |

## `kind: cast` {#cast}

Widens field types. Narrowing is an error.

```yaml
- kind: cast
  dataset: telemetry
  fields: { seq: i64, voltage_v: f64 }
```

| Key | Type | Required | Description |
|---|---|---|---|
| `fields` | mapping field name → type (≥ 1) | yes | Each name must be a field of the dataset as left by earlier steps. |

Allowed casts:

- Signed integers widen: `i8` → `i16` → `i32` → `i64`. Unsigned integers widen likewise: `u8` → `u16` → `u32` → `u64`.
- Any unsigned integer to a larger signed integer (for example `u32` → `i64`).
- `f32` → `f64`, `list<f32>` → `list<f64>`, and `list<i32>` → `list<i64>`.
- A cast to the field's current type is allowed and does nothing.

Anything else is a `config.invalid` error: narrowing (`f64` → `f32`, `i64` → `i32`), integer to float, and anything involving `str`, `bool`, `ts`, or `bytes`. The engine reports the same condition at chain-compile time as `schema.narrowing`.

## `kind: pose` {#pose}

Flags malformed flattened poses — the dotted fields the ROS 2 decoder writes for `geometry_msgs/PoseStamped` and cousins (see [robotics conventions](../concepts/robotics-conventions.md)). Events are **flagged, not dropped** — quality is data.

```yaml
- kind: pose
  dataset: poses
  # prefix: pose                 # the default
  # frame_field: header.frame_id # the default; an explicit null skips the frame check
  # quaternion_tolerance: 0.001  # the default
```

With `prefix: pose` the step reads `pose.position.{x,y,z}` and `pose.orientation.{x,y,z,w}`, each an `f32` or `f64` field.

| Key | Type | Required | Description |
|---|---|---|---|
| `prefix` | string | no (default `pose`) | The flattened-pose prefix. Must not be empty. |
| `frame_field` | string or `null` | no (default `header.frame_id`) | The string field holding the frame id. An explicit `null` skips the frame check. Must not be empty when set. |
| `quaternion_tolerance` | number | no (default `0.001`) | How far the quaternion norm may drift from 1; greater than 0 and less than 1. |

| Signal | Flag and status | Reason |
|---|---|---|
| A component is null or not a number | `INVALID`, bad | `pose_missing_field` |
| A component is NaN or infinite | `INVALID`, bad | `pose_not_finite` |
| \|\|q\| − 1\| exceeds the tolerance | `OUT_OF_RANGE`, bad | `quaternion_not_unit` |
| The frame id is empty or missing | `INVALID`, bad | `missing_frame_id` |

The norm is only computed when all four quaternion components are finite numbers. An event violating several rules raises every flag; the reason is the first violation (an upstream reason is kept), and everything lands in `_quality_status`, `_quality_flags`, and `_quality_reason` as usual.

The flattened fields usually come from the ROS 2 decoder's shape rather than declared dataset fields, so — like `enrich`'s table file — they are resolved when the transform chain compiles at pipeline startup, not by `thadam validate`. Validation does check the scalar options: `prefix` non-empty, the tolerance range, a non-empty `frame_field`.

## `kind: joint_state` {#joint_state}

Flags malformed joint-state arrays, following `sensor_msgs/JointState` semantics (see [robotics conventions](../concepts/robotics-conventions.md)). Events are **flagged, not dropped** — quality is data.

```yaml
- kind: joint_state
  dataset: joints
  # name_field: name         # the default
  # position_field: position # the default
  # velocity_field: velocity # the default
  # effort_field: effort     # the default
```

| Key | Type | Required | Description |
|---|---|---|---|
| `name_field` | `list<str>` field | no (default `name`) | The joint names. Must not be empty. |
| `position_field` | float-array field | no (default `position`) | Joint positions. Must not be empty. |
| `velocity_field` | float-array field | no (default `velocity`) | Joint velocities. Must not be empty. |
| `effort_field` | float-array field | no (default `effort`) | Joint efforts. Must not be empty. |

Array fields accept `list<f32>` and `list<f64>` (generic lists of floats decode as either); a null array reads as an empty one. `thadam validate` resolves all four fields against the dataset as left by earlier steps — unknown names and wrong element types are configuration errors — and the engine re-checks against the actual record shape when the chain compiles at startup.

| Signal | Flag and status | Reason |
|---|---|---|
| A non-empty array's length differs from the name count | `INVALID`, bad | `joint_state_length_mismatch` |
| A joint name repeats | `INVALID`, bad | `duplicate_joint_name` |
| A value is NaN, infinite, or a null element in a generic list | `INVALID`, bad | `joint_state_not_finite` |

Several violations at once settle on the first reason in this order; flags accumulate.

## Execution position

Transforms run **after** decode, event-ID derivation, dedup, `explode`, field extraction, unit conversion, and time handling — see [per-sink chains](../concepts/per-sink-chains.md) for the full order and for how panics and poison records are handled.
