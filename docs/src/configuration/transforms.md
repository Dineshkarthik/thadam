# `transforms` and predicates

The `transforms:` list applies quality, projection, and filtering steps to datasets, in listed order, per sink chain. Every step takes a `dataset` selector and an optional `sinks` restriction.

**Config version 1 accepts three kinds** — `validate`, `on_change`, and `filter`. The transform engine implements more (`sample`, `enrich`, `rename`, `project`, `cast`); those are documented in the [transform reference](../transforms/index.md), and this binary's configuration rejects them with `unknown transform kind`, so they cannot be used from a pipeline file today.

## Step shell

```yaml
transforms:
  - kind: validate | on_change | filter
    dataset: telemetry            # the dataset the step applies to (required, single id)
    sinks: [local]                # optional: restrict the step to these sink ids
    ...                           # kind-specific keys
```

| Key | Type | Required | Description |
|---|---|---|---|
| `kind` | `validate`, `on_change`, `filter` | yes | The step type. Unknown kinds are configuration errors. |
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
| `flag` | quality flag | yes | The flag raised on violation, from the known set: `OUT_OF_RANGE`, `CLOCK_SKEW`, `MISSING_TIME`, `OUT_OF_ORDER`, `DUPLICATE`, `STALE`, `SEQUENCE_GAP`, `DECODE_ERROR`. Unknown flags are errors. |
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
| `fields` | list of field names (≥ 1) | yes | Declared fields compared with the previous kept event. Unknown names are errors. |
| `heartbeat` | duration | no | Pass an event at least this often even when nothing changed (measured in ingest time). Must be > 0 when present. |

(The engine also supports per-field `{ field, deadband }` entries with an absolute numeric deadband; that spelling is accepted by `thadam-transform` but not by config version 1's `on_change`, which takes plain field names.)

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

Rules (checked by `thadam validate`):

- **Fields** resolve against the dataset: declared fields, identity levels, the `explode` index field, and the quality names `quality.status`, `quality.flags`, `quality.reason`. Typo'd names are errors with suggestions.
- **Literal typing**: integer fields take integer literals that fit (`u64` values above `i64::MAX` stay exact, never widened to float); float fields compare in their own domain.
- **Null semantics**: a null or missing field makes `eq`, the ordered comparisons, `in`, `min`, and `max` false, and `ne` true. `NaN` is unordered. A `null` test exists in the engine but is not part of config version 1.
- **Cost limits**: predicate nodes, depth, and `in`-set size are bounded by chain limits; overflow is a configuration error, not a runtime surprise.
- An evaluation that still cannot complete at runtime becomes a `transform.eval_failed` reject with the raw bytes kept.

## Execution position

Transforms run **after** decode, event-ID derivation, dedup, `explode`, field extraction, unit conversion, and time handling — see [per-sink chains](../concepts/per-sink-chains.md) for the full order and for how panics and poison records are handled.
