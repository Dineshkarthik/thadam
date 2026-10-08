# Transform reference

Transforms are the synchronous, pure steps a [per-sink chain](../concepts/per-sink-chains.md) runs between decode and delivery. This page covers **every step kind implemented in `thadam-transform`**, with its options and examples.

> **Config reachability.** Config version 1 accepts only `validate`, `on_change`, and `filter` in the top-level `transforms:` list — the five others (`sample`, `enrich`, `rename`, `project`, `cast`) exist in the engine and are tested, but this binary's configuration parser rejects them (`unknown transform kind`), so **they cannot be used from a pipeline file today**. They are documented here for completeness and for when the config surface widens. Each section states its reachability.

For the three config-reachable kinds, see also the [configuration reference](../configuration/transforms.md), which has the same information organized by config key.

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

**Config: accepted (field-name form).** Keeps an event only when a watched field changed, or when `heartbeat` of ingest time passed. Lossy by design; not deduplication (ADR-0011 D17). Full key table: [`kind: on_change`](../configuration/transforms.md#on_change).

```yaml
- kind: on_change
  dataset: state
  fields: [mode, cycle_id, program]
  heartbeat: 5m
```

Engine form (not accepted in config version 1): watched fields may also be `{ field: <name>, deadband: <number> }` with an absolute numeric deadband (default 0).

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

Tests in the engine: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `min`, `max`, and `null` (`null: true` tests for null-or-missing; config version 1 accepts all but `null`). Combinators: `all`, `any`, `not`. `expr:` is reserved and refused. Names resolve against dataset fields, identity levels, the explode index field, `quality.status`, `quality.flags`, and `quality.reason`; missing/null fields make ordered tests false and `ne` true.

## `sample` — every-N and minimum-interval

**Engine only — not accepted in config version 1.** Keeps the first event and every Nth after it, and/or events at least an interval apart (ingest time), per asset.

```yaml
# Rejected by thadam validate today; documented from thadam-transform.
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

**Engine only — not accepted in config version 1.** Adds fields from a static CSV or YAML table, read once at chain-compile time; the file's SHA-256 is recorded as the enrichment hash in lineage (one of the three inputs of replay row-set equivalence). Relative paths resolve against the config file's directory.

```yaml
# Not accepted in config version 1 today.
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

**Engine only — not accepted in config version 1.** Renames fields without touching values.

```yaml
- kind: rename
  dataset: telemetry
  fields: { temp_c: temperature_c, volts: voltage_v }
```

## `project` — keep a subset

**Engine only — not accepted in config version 1.** Keeps only the listed fields, in the listed order.

```yaml
- kind: project
  dataset: telemetry
  fields: [seq, soc_pct, voltage_v]
```

## `cast` — widening only

**Engine only — not accepted in config version 1.** Widens field types (`i32` → `i64` → `f64` direction). Narrowing is a *compile error* (`schema.narrowing`), matching the schema-evolution rules for datasets.

```yaml
- kind: cast
  dataset: telemetry
  fields: { soc_pct: f64 }
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
