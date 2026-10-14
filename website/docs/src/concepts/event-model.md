# The canonical event model

Thadam's internal representation (defined in `thadam-core`, ADR-0001) is a typed, binary-friendly event — never a JSON blob. It lives in memory only: the WAL stores raw source bytes and never depends on the model, which keeps the durable format independent of model evolution.

## Events, batches, and messages

- Sources emit **raw messages**: `ingest_time`, attributes, and opaque payload bytes. One WAL frame carries one source batch, uninterpreted.
- Chains decode messages into **events**: one event per source message by default, or one per element when a dataset uses [`explode`](../configuration/datasets.md#explode) to split an array-valued message into samples.
- Every dataset carries **envelope columns** alongside its declared fields, so any row can be traced back to its capture record without logging payloads: `_event_id`, `_ingest_time`, `_source_time`, `_event_time`, `_time_domain`, `_quality_status`, `_quality_flags`, `_quality_reason`, `_clock_synced`, `_wal_epoch`, `_wal_pos`, the identity-level columns, and `sample_idx` for exploded datasets.

## Time: three clocks, never silently merged

Robotic data lives on several clocks, and Thadam keeps them distinct (ADR-0001):

| Field | Meaning |
|---|---|
| `source_time` | The time the device reported. Set once at decode; never overwritten. |
| `ingest_time` | The time the Thadam edge received it. Always present; never changed. |
| `corrected_time` | Optional. Any correction **must** set a quality flag, so device time is never silently replaced. |
| `time_domain` | The clock the source time is on: `utc`, `tai`, `monotonic`, `sim`, or `unknown` (the default, so mapping to UTC is always explicit). |
| `clock_synced` | Per-batch flag from the host's clock-sync state; drives the `unsynced/` Parquet partition when the clock was not synced. |

Sinks materialize `event_time` as `corrected_time` if present, else `source_time` when the domain is `utc`, else null. Non-UTC source times (monotonic-since-boot, simulation time, TAI) never become `event_time` without an explicit mapping, and a UTC source time farther than [`max_skew`](../configuration/datasets.md#time) from ingest time raises the `CLOCK_SKEW` quality flag rather than being "fixed". Wall time is never used to measure durations inside Thadam; retention and rotation use monotonic durations and WAL positions.

## Quality: data, not logs

Every event carries event-level quality: a status (`good` = 0, `uncertain` = 1, `bad` = 2), a flag bitset, and an optional short reason — all stored as columns, so bad data is queryable rather than buried in logs. The flags in use:

| Flag | Raised when |
|---|---|
| `OUT_OF_RANGE` | A value violated its declared range (a `validate` rule). |
| `CLOCK_SKEW` | A UTC source time disagrees with ingest time by more than `max_skew`. |
| `MISSING_TIME` | The message carried no usable source time. |
| `OUT_OF_ORDER` | The event arrived out of order. |
| `DUPLICATE` | The event duplicates an earlier one. |
| `STALE` | The value is older than its freshness bound. |
| `SEQUENCE_GAP` | A gap in the source sequence precedes this event. |
| `DECODE_ERROR` | The payload was only partly decoded. |
| `INVALID` | A validation step found the data itself unusable: a value missing, of the wrong kind, not finite, or inconsistent with its siblings (raised by `pose`, `joint_state`, and `validate` rules). |

Events that cannot be processed (decode, schema, transform, quality failures) become rejects routed to [quarantine](per-sink-chains.md#quarantine) with their raw bytes retained. The pipeline never aborts on a bad event.

## Identity: a hierarchy, as columns

Device identity is an ordered list of `(level, value)` pairs — `site` → `robot` → … → `sensor` — extracted at decode time from templates such as `{topic.2}` or from payload fields. No level is mandatory; `device_id` is just a level. Identity levels become **columns** in every dataset, never partitions or file units: partitioning 500 robots into 500 directory trees would explode open-writer counts and small files, which is exactly the failure mode datasets avoid ([ADR-0013](../design/index.md)).

## Datasets: the output unit

A dataset is an output table: a `match` (source and topic), a decode format, an identity template, optional `event_id.key` and `dedup`, optional `explode`, a `time` rule, and a **declared field list** (`name`, type, `from` JSON Pointer, optional `unit`/`convert_to`, `required`, and a stable Parquet field `id`). Declared schemas are required; schema inference is a development-only convenience. Fields evolve additively or by widening (`int` → `long`, `float` → `double`); anything else is a new dataset version. See the [`datasets` reference](../configuration/datasets.md).

## Event IDs and deduplication

By default the event ID is deterministic per WAL record (`xxh3_128` over the epoch, frame sequence, and record index), so it is stable across replay but **different for a broker redelivery** — which is exactly when duplicates appear. A dataset may instead derive IDs from content with [`event_id: { key: [...] }`](../configuration/datasets.md#event_id) (for example `[site, robot, seq]`); only a content-derived ID survives redelivery, so `dedup` without it is a validation error (ADR-0011). ADR-0011 fixes the responsibility split: dedup inside Thadam is best-effort over a bounded, warm-started window; authoritative deduplication happens downstream (`ReplacingMergeTree`, `QUALIFY`, `DISTINCT ON`).

## Payloads and media

Sources hand chains raw bytes; decoding (JSON today) produces typed records that conform to the declared dataset shape. Large media does not travel in the event path: frames, bags, and videos are referenced by object URI (an `ObjectRef` payload kind) rather than embedded. There are no `Pose`- or `Ros2Msg`-typed variants in the core model — robotics types are records following field conventions (for example `frame_id` is a payload field, not an envelope field).
