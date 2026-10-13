# `transforms` and predicates

The `transforms:` list applies quality, projection, and filtering steps to datasets, in listed order, per sink chain. Every step takes a `dataset` selector and an optional `sinks` restriction.

**Config version 1 accepts all eleven kinds** the transform engine implements: `validate`, `on_change`, `filter`, `sample`, `enrich`, `rename`, `project`, `cast`, `pose`, `joint_state`, and `window` (experimental). See also the [transform reference](../transforms/index.md).

Each step is checked against the dataset's fields **as left by the earlier steps** for that dataset (and for the sinks the step applies to). After `rename`, `project`, `cast`, or `enrich`, later steps use the new names and types.

## Step shell

```yaml
transforms:
  - kind: validate | on_change | filter | sample | enrich | rename | project | cast | pose | joint_state | window
    dataset: telemetry            # the dataset the step applies to (required, single id)
    sinks: [local]                # optional: restrict the step to these sink ids
    ...                           # kind-specific keys
```

| Key | Type | Required | Description |
|---|---|---|---|
| `kind` | `validate`, `on_change`, `filter`, `sample`, `enrich`, `rename`, `project`, `cast`, `pose`, `joint_state`, `window` | yes | The step type. Unknown kinds are configuration errors. |
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

The flattened fields are declared as ordinary (dotted) dataset fields, so `thadam validate` checks them here: every `<prefix>.position.{x,y,z}` and `<prefix>.orientation.{x,y,z,w}` must exist as an `f32` or `f64` field, and the frame field — `header.frame_id` by default — as a `str` field. Each missing or mistyped one is its own error at `transforms[…].prefix` or `transforms[…].frame_field`, and the scalar options are checked too (`prefix` non-empty, the tolerance range, a non-empty `frame_field`). The engine re-checks against the actual record shape when the transform chain compiles at pipeline startup.

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

## `kind: window` {#window}

**Status: In progress (experimental).** Tumbling windows on **ingest time** (`time: ingest`, the default) or **event time** (`time: event`, with per-asset watermarks and `allowed_lateness`), one aggregate event per asset and window. Sliding windows and `first`/`last`/`stddev`/`range`/`delta`/`rms` are not built yet; the design is in `docs/design/advanced-processing.md`.

```yaml
datasets:
  - id: telemetry
    # ...
    fields:
      - { id: 1000, name: motor_temp_c, type: f32 }
      # The aggregates the window writes. The Parquet sink writes the columns
      # a dataset declares, so declare one per output column (see below).
      - { id: 1010, name: temp_mean, type: f64 }
      - { id: 1011, name: temp_max, type: f64 }
      - { id: 1012, name: samples, type: u64 }
      - { id: 1013, name: window_start, type: ts, unit: ns }
transforms:
  - kind: window
    dataset: telemetry
    size: 1m
    time: event                # or ingest (the default)
    allowed_lateness: 5s       # event time only (0s when absent)
    aggregates:
      - { field: motor_temp_c, fn: mean, as: temp_mean }
      - { field: motor_temp_c, fn: max, as: temp_max }
      - { field: motor_temp_c, fn: count, as: samples }
```

| Key | Type | Required | Description |
|---|---|---|---|
| `size` | duration > 0 | yes | The window length. Windows are aligned to the epoch: `[k·size, (k+1)·size)` of the chosen timestamp. |
| `time` | `ingest`, `event` | no (default `ingest`) | Which timestamp assigns an event to a window and advances the watermark. Omitted from the canonical (hashed) form when `ingest`. |
| `allowed_lateness` | duration ≥ 0 | no (default `0s`) | Valid only with `time: event` (`config.invalid` on `time: ingest`). How far behind an asset's maximum seen event time a window of that asset stays open for out-of-order events. |
| `idle_close` | duration > 0 | no | Closes a silent asset's open windows once another asset in the dataset sends an event at least `idle_close` in ingest time after that asset's last event, releasing the cursor hold-back. |
| `aggregates` | list (≥ 1) of `{ field, fn, as }` | yes | The output columns; see below. |

Each aggregate:

| Key | Type | Required | Description |
|---|---|---|---|
| `field` | string | yes | A field of the dataset as left by earlier steps. |
| `fn` | `count`, `sum`, `min`, `max`, `mean` | yes | `count` counts non-null values and accepts any field. The others need `i8`, `i16`, `i32`, `u8`, `u16`, `u32`, `f32`, or `f64`; `i64` and `u64` are refused because the `f64` result cannot hold them exactly. |
| `as` | string | yes | The output column. Unique, not empty, and not `window_start` or `window_end`. |

**What a window emits.** With `time: ingest`, an asset's window closes when the asset sends an event whose ingest time falls in a later window. With `time: event`, each asset tracks its own watermark (`max_event_time − allowed_lateness`); an out-of-order event within `allowed_lateness` joins its window (so an asset can hold more than one open pane at once), and open windows with `window_end ≤ watermark` close and emit in ascending `window_start` order. The emitted event has `window_start` and `window_end` (`ts`), then one column per aggregate: `count` is a `u64`, the rest are `f64` (null when the window saw no value). A null or `NaN` value is skipped. The event replaces the dataset's fields, so later steps see only these columns, and the asset identity stays. Its event time is the window start, its ingest time is the closing event's, and its WAL position is the closing event's frame. Its id is derived from the dataset, asset, window start, size, and column names, so a re-read after a restart gives the same id. The events that fed the window are not emitted.

**Counters and quality.** Events absorbed into an open window, and events with no UTC event time under `time: event` (for example a `monotonic` or `sim` time domain), count as `filtered`. Under `time: event`, an event whose window has already closed (`window_end ≤ watermark`) is dropped without re-emitting and counted as `late` (`thadam_chain_events_total{outcome="late"}`). A window that was open across a WAL gap emits with `quality.status = uncertain` and `quality.reason = "window_gap"`; when `thadam replay` starts after frame 1 (`--from` > 1), the first window opened for each asset emits with `quality.status = uncertain` and `quality.reason = "window_range_start"`.

**Columns reach Parquet only when declared.** The Parquet sink writes the columns the dataset declares, matched by name. A window's output columns are written only if the dataset declares fields with the same names (as in the example above, where the declared fields only exist to name and type the output columns; they read null from the payload). The same holds for columns added by `enrich`.

**Open windows and the cursor.**

- There are no wall-clock timers. Without `idle_close`, an asset's open windows stay open until that asset sends an event that advances past them; nothing is flushed on shutdown. With `idle_close`, another asset's event whose ingest time is at least `idle_close` past the silent asset's last event closes all of that asset's open windows (ordered by `(first_pos, asset)` across assets and ascending `window_start` within an asset), advances its watermark to the last closed `window_end`, and releases its hold on the cursor.
- Open windows are kept in memory only. To survive a restart without a new state file, the sink's WAL cursor is **held back**: it never moves to or past the first frame of a window whose aggregate the sink does not hold yet, and a restart re-reads from the cursor to rebuild the windows. Aggregates the sink already holds are skipped, so a restart neither drops an aggregate nor writes one twice.
- The WAL therefore keeps data for at least the window length (plus `allowed_lateness` under `time: event`), plus the time any asset stays silent (bounded by `idle_close` when other assets in the dataset keep speaking). **Without `idle_close`, a silent asset's open window holds the cursor for every dataset in that sink's chain until the asset speaks again**, so the WAL grows (or, with `on_full: block`, blocks) in the meantime. State is bounded by the chain's per-step state limit (`max_state_keys`, 100,000 assets by default); beyond it, events of further assets are rejected to quarantine with a `transform.failed` reason rather than dropped.
- Under `time: ingest`, if ingest time steps backwards, the event joins its asset's open window.
- Restarts are exact for the `window` step itself. A step before it that depends on history (`sample every_n`, `on_change`, dedup) is rebuilt from `warm_start_frames` of history, so a window behind such a step can differ from an uninterrupted run when the history is longer than that. Steps after a `window` start empty after a restart.

**Limits in this release.**

- Only segment sinks (such as `parquet`) support `window`. Building a pipeline that routes a windowed dataset to a batch sink is a configuration error.
- Late events under `time: event` are dropped and counted rather than emitting retractions or corrections.
- Tumbling windows only; `first`, `last`, `stddev`, `range`, `delta`, and `rms` are not implemented.

`thadam validate` checks `size`, `time`, `allowed_lateness`, `idle_close`, the aggregates, and the field types, and the later steps are checked against the window's columns.

## Execution position

Transforms run **after** decode, event-ID derivation, dedup, `explode`, field extraction, unit conversion, and time handling — see [per-sink chains](../concepts/per-sink-chains.md) for the full order and for how panics and poison records are handled.
