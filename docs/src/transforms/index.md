# Transform reference

Transforms are the synchronous, pure steps a [per-sink chain](../concepts/per-sink-chains.md) runs between decode and delivery. This page covers **every step kind implemented in `thadam-transform`**, with its options and examples. All ten kinds are accepted in the top-level `transforms:` list of config version 1.

See also the [configuration reference](../configuration/transforms.md), which has the same information organized by config key.

## Execution model recap

- Steps run per chain, per dataset, **after** decode → event-ID/dedup → `explode` → field extraction/unit conversion → time handling, and each step's `dataset:` (and optional `sinks:`) selector is applied.
- Steps are deterministic: no wall clock, no randomness. Sampling uses arrival order and ingest time.
- Cost is bounded by `ChainLimits` (shape registry, dedup keys, JSON depth, predicate nodes/depth/set size).

## `validate` — range rules to quality flags

**Config: accepted.** Assigns a quality flag and status when a numeric field (or every element of a numeric list) falls outside `min`/`max`; events are flagged, not dropped. Full key table: [`kind: validate`](../configuration/transforms.md#validate).

```yaml
- kind: validate
  dataset: telemetry
  rules:
    - { field: motor_temp_c, min: -40, max: 150, flag: OUT_OF_RANGE, status: bad }
```

## `on_change` — deadband with heartbeat

**Config: accepted.** Keeps an event only when a watched field changed, or when `heartbeat` of ingest time passed. Lossy by design; not deduplication (ADR-0011 D17). Full key table: [`kind: on_change`](../configuration/transforms.md#on_change).

```yaml
- kind: on_change
  dataset: state
  fields: [mode, cycle_id, program]
  heartbeat: 5m
```

Watched fields may be bare names or `{ field: <name>, deadband: <number> }` entries with an absolute numeric deadband (default 0); both forms are accepted in config.

## `filter` — structured predicates

**Config: accepted.** Drops events failing a structured predicate (ADR-0018). Full grammar: [predicates](../configuration/transforms.md#predicates).

```yaml
- kind: filter
  dataset: events
  where:
    all:
      - { field: level, ne: debug }
      - any:
          - { field: site, eq: plant-2 }
          - { field: soc_pct, min: 10 }
```

Tests: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `min`, `max`, and `null` (`null: true` tests for null-or-missing); config version 1 accepts all of them. Combinators: `all`, `any`, `not`. `expr:` is reserved and refused. Names resolve against dataset fields, identity levels, the explode index field, `quality.status`, `quality.flags`, and `quality.reason`; missing/null fields make ordered tests false and `ne` true.

## `sample` — every-N and minimum-interval

**Config: accepted.** Keeps the first event and every Nth after it, and/or events at least an interval apart (ingest time), per asset. Full key table: [`kind: sample`](../configuration/transforms.md#sample).

```yaml
- kind: sample
  dataset: imu
  every_n: 10
  min_interval: 100ms
```

| Key | Type | Description |
|---|---|---|
| `every_n` | integer | Keep the first event and every Nth after it. |
| `min_interval` | duration | Keep an event only when this much ingest time passed since the last kept one. |

## `enrich` — static table lookup

**Config: accepted.** Adds fields from a static CSV or YAML table, read once at chain-compile time; the file's SHA-256 is recorded as the enrichment hash in lineage (one of the three inputs of replay row-set equivalence). Relative paths resolve against the config file's directory. Full key table: [`kind: enrich`](../configuration/transforms.md#enrich).

```yaml
- kind: enrich
  dataset: telemetry
  file: robots.yaml
  key: { field: robot, column: robot_id }
  add:
    - { name: payload_kg, type: f32, unit: kg }
    - { name: cell, type: str, column: cell_name }
  on_miss: null        # null (default) | reject | drop
```

## `rename` — field rename

**Config: accepted.** Renames fields without touching values. Full key table: [`kind: rename`](../configuration/transforms.md#rename).

```yaml
- kind: rename
  dataset: telemetry
  fields: { temp_c: temperature_c, volts: voltage_v }
```

## `project` — keep a subset

**Config: accepted.** Keeps only the listed fields, in the listed order. Full key table: [`kind: project`](../configuration/transforms.md#project).

```yaml
- kind: project
  dataset: telemetry
  fields: [seq, soc_pct, voltage_v]
```

## `cast` — widening only

**Config: accepted.** Widens field types (`i32` → `i64`, `f32` → `f64`, and so on). Narrowing is rejected: `config.invalid` at validation and a *compile error* (`schema.narrowing`) in the engine, matching the schema-evolution rules for datasets. Full key table: [`kind: cast`](../configuration/transforms.md#cast).

```yaml
- kind: cast
  dataset: telemetry
  fields: { soc_pct: f64 }
```

## `pose` — flattened-pose checks

**Config: accepted.** Flags a flattened pose (default prefix `pose`, the ROS `geometry_msgs/PoseStamped` layout) whose components are missing (`pose_missing_field`) or non-finite (`pose_not_finite`), whose quaternion norm drifts past `quaternion_tolerance` (`quaternion_not_unit`, `OUT_OF_RANGE`), or whose frame id (`frame_field`, default `header.frame_id`, explicit `null` skips) is empty (`missing_frame_id`). Flagged, not dropped; see [robotics conventions](../concepts/robotics-conventions.md). Full key table: [`kind: pose`](../configuration/transforms.md#pose).

```yaml
- kind: pose
  dataset: poses
  quaternion_tolerance: 0.01
```

## `joint_state` — joint-array checks

**Config: accepted.** Flags a joint-state record (ROS `sensor_msgs/JointState` semantics) whose `position`/`velocity`/`effort` arrays are neither empty nor as long as `name` (`joint_state_length_mismatch`), whose names repeat (`duplicate_joint_name`), or with non-finite values (`joint_state_not_finite`). Field names are configurable (`name_field`, `position_field`, `velocity_field`, `effort_field`). Full key table: [`kind: joint_state`](../configuration/transforms.md#joint_state).

```yaml
- kind: joint_state
  dataset: joints
```

## Fixed-position behaviors (not `transforms:` steps)

These dataset-shaping behaviors are configured on the dataset, not in `transforms:`, and run in a fixed order before your steps:

| Behavior | Config | Runs |
|---|---|---|
| Router / match | [`match`](../configuration/datasets.md#match) | first |
| Decode (JSON) | [`decode`](../configuration/datasets.md) | message level |
| Event IDs | [`event_id.key`](../configuration/datasets.md#event_id) | message level |
| Deduplication | [`dedup`](../configuration/datasets.md#dedup) | message level, before explode |
| Array splitting | [`explode`](../configuration/datasets.md#explode) | then |
| Field extraction & units | [`fields` with `unit`/`convert_to`](../configuration/datasets.md#fields) | per event; element-wise on `list<T>`, UCUM-coded |
| Time extraction | [`time`](../configuration/datasets.md#time) | with `max_skew` flagging |
