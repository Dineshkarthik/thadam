# Proposed amendments to product.md

Note: the owner approved Part A (A.1 to A.11) on 2026-10-07 and must now apply the approved amendments to `product.md` (this document does not edit it). A.11 (structured-predicate filter syntax) was added the same day after ADR-0018 was accepted.

Status: Part A approved by the owner (2026-10-07), including A.11. Parts B and C are still awaiting owner approval. This document proposes text; it does not change `product.md`, which stays read-only to the coding agents and the source of truth until the owner applies these edits.

Date: 2026-10-07. Prepared for blocker B2 (scope freeze) in `docs/design/owner-decisions.md`, with a second, lower-priority part that aligns `product.md` with the architecture decisions in `ARCHITECTURE.md`.

## How to use this document

1. Each amendment names a `product.md` section, quotes what it says now, gives replacement text, and states why.
2. Part A is required to freeze the MVP scope (A.11 records the filter-syntax decision). Part A depended on whether ROS 2 is in MVP v0.1 or in M2. The owner decided on 2026-10-07 that ROS 2 is M2, which is what the proposed text assumes. Part A.10 is kept only as a record of the alternatives that were not chosen.
3. Part B aligns the document with accepted architecture. It is not needed to start work, but leaving it unapplied means `product.md` describes designs that have been superseded (for example the position of the buffer in the pipeline).
4. Part C is editorial.
5. The owner may approve amendments individually. Mark each one Approved, Rejected, or Modified in the "Owner decision" line, then apply the approved text to `product.md` in one documentation pull request.

## Milestone vocabulary used by the proposed text

The proposed text introduces three milestone names. They come from the implementation planning documents and appear in `ARCHITECTURE.md` section 1.

- **M1 (MVP v0.1):** MQTT to a durable capture log to Parquet segments to S3-compatible storage, with replay and chaos verification.
- **M2:** ROS 2, MCAP and rosbag2 offline ingestion first, then a live transport.
- **M3:** ClickHouse, starting only after the M1 gate passes.

---

# Part A: Scope freeze (required for B2)

## A.1 Section 31, "MVP definition": objective, sources, transforms, reliability, sinks, examples

**What it says now.**

> The MVP is deliberately narrow.
> **MVP objective:** Demonstrate that one Rust edge runtime can reliably ingest two important robotic/IoT protocols, transform the data, survive outages, and deliver to two modern data targets.
> **MVP sources:** 1. MQTT 2. ROS 2 / DDS-compatible source
> **MVP transforms:** 1. decode 2. normalize 3. filter 4. field mapping 5. unit conversion 6. deduplication 7. basic enrichment 8. batching
> **MVP reliability:** 1. durable local buffer 2. retry 3. backpressure 4. restart recovery 5. replay
> **MVP sinks:** 1. Local durable capture to Parquet segments (P0) 2. S3-compatible object storage for Parquet/raw archives (P0) 3. ClickHouse (P1 early integration, conditional on MVP capacity and target-user validation)
> **MVP examples:** MQTT -> durable local Parquet -> S3-compatible object storage; ROS 2 -> durable local capture -> replay after reconnect; MQTT -> ClickHouse (P1 analytics example, if included in MVP scope); offline edge -> reconnect -> replay without data loss within the configured retention budget

**Proposed text.**

> The MVP is deliberately narrow. It is delivered in milestones. MVP v0.1 is milestone M1.
>
> **MVP objective (M1):** Demonstrate that one Rust edge runtime can capture MQTT data durably, survive network, destination, and process failures without silent loss within documented limits, transform the data into declared analytical datasets, and deliver it as Parquet files to local disk and to S3-compatible object storage.
>
> **Milestones**
>
> - **M1 (MVP v0.1):** MQTT source, durable capture log, local Parquet segments, S3-compatible shipping, replay, and chaos verification.
> - **M2:** ROS 2 source. MCAP and rosbag2 offline ingestion first, then a live transport chosen by an ADR-backed spike.
> - **M3 (P1):** ClickHouse sink, started only after the M1 gate passes.
>
> **MVP sources (M1):** 1. MQTT 3.1.1 (MQTT 5 if ADR-0006 confirms client support). ROS 2 / DDS-compatible ingestion is a P0 product capability delivered in M2.
>
> **MVP transforms:** 1. decode 2. normalize to declared dataset schemas 3. filter 4. field mapping (projection, rename, extraction, type widening) 5. unit conversion 6. best-effort deduplication, and change-based filtering (deadband) as a separate transform 7. static-file enrichment 8. timestamp extraction with explicit correction flags 9. sampling and downsampling by decimation 10. routing to datasets and to quarantine 11. batching
>
> **MVP reliability:** 1. durable local capture log, written before the source is acknowledged 2. retry 3. backpressure (acknowledgement withholding for flow-controlled sources, counted drops for sources without flow control) 4. restart recovery 5. replay from the capture log retention window
>
> **MVP sinks (M1):** 1. Local durable capture to Parquet segments (P0) 2. S3-compatible object storage for Parquet segments (P0). ClickHouse is a P1 sink delivered in M3 after the M1 gate; it is not part of v0.1.
>
> **MVP examples:**
>
> - MQTT -> durable local Parquet -> S3-compatible object storage (M1)
> - offline edge -> reconnect -> replay without data loss within the configured retention budget and the broker's queue limits (M1)
> - ROS 2 -> durable local capture -> Parquet (M2)
> - MQTT -> ClickHouse (P1 analytics example, M3)

**Rationale.**

- The product document already says P0 is capture plus Parquet and object storage, and says to defer ClickHouse first if capacity is constrained. The unamended list in this section still describes "two protocols" and "two modern data targets", which contradicts that decision.
- The MVP transform list omits sampling, timestamp correction, and routing, which section 10 lists as P0. The architecture needs routing (datasets and quarantine) and timestamp extraction anyway.
- "Deduplication" is split. The architecture review found that in-process deduplication loses its state exactly when redelivery happens and cannot be authoritative, and that "remove duplicate values" in use case UC-05 is deadband filtering, a different operation.
- "Without data loss" is qualified to the broker's queue limits because `block` does not prevent loss when the broker's persistent-session queue is bounded (Mosquitto defaults to 1000 queued messages [V]).

**Depends on:** the ROS 2 decision (Part A.10), decided: ROS 2 is M2. **Owner decision:** Approved (2026-10-07), with D-B2-1 and D-B2-2.

## A.2 Section 31, "Sink-priority decision (MVP)": acceptance criteria

**What it says now.**

> **Acceptance criteria:** the team can demonstrate a disconnected edge capture, restart recovery, replay/upload, bounded disk behavior, and independently configurable ClickHouse delivery; benchmark claims must be measured rather than assumed.

**Proposed text.**

> **Acceptance criteria (M1 gate):** the team can demonstrate disconnected edge capture, restart recovery, replay and upload, bounded disk behavior with every loss attributed, and quarantine of malformed input, each verified against the producer's own record of what it sent; benchmark claims must be measured rather than assumed.
>
> **Acceptance criteria (P1 gate, after M1):** independently configurable ClickHouse delivery that does not change the M1 results. The Parquet and object-storage verification must still pass while ClickHouse is unavailable.

**Rationale.** The M1 criteria that matter most (zero missing acknowledged samples, zero duplicate rows, bounded disk, attributed loss) are defined by the demo plan's scenarios F1 to F8. A criterion that requires ClickHouse cannot be part of an M1 gate if ClickHouse is not in M1.

**Owner decision:** Approved (2026-10-07), with D-B2-3.

## A.3 Section 46, "Definition of MVP done"

**What it says now.**

> **Demo A:** MQTT -> normalize -> filter -> buffer -> ClickHouse
> **Demo B:** MQTT -> normalize -> Parquet -> S3
> **Demo C:** ROS2 -> canonical event -> ClickHouse
> **Failure demo:** source -> edge buffer -> network outage -> recovery -> sink
> **Replay demo:** record -> replay -> pipeline -> destination
> **Developer experience:** A new developer can run the full demo locally with documented commands.

**Proposed text.**

> The MVP (v0.1, milestone M1) is done only when all of the following work:
>
> **Demo A:** MQTT -> capture log -> normalize -> filter -> local Parquet segments
>
> **Demo B:** MQTT -> capture log -> normalize -> Parquet -> S3-compatible object storage
>
> **Failure demo:** source -> capture log -> network and destination outage, broker partition, process kill, disk pressure, and malformed input -> recovery -> verified delivery with zero missing acknowledged samples and zero duplicate rows
>
> **Replay demo:** a deliberately misconfigured filter loses nothing; the retained capture log is replayed through the corrected configuration into a separate output
>
> **Developer experience:** A new developer can run the full demo locally with documented commands, from a clean clone to a first query result in under 10 minutes using prebuilt images.
>
> **Later gates:**
>
> - **M2 gate:** ROS 2 -> canonical event -> Parquet (MCAP file input first)
> - **P1 gate (M3):** MQTT -> ClickHouse, and later ROS 2 -> ClickHouse, once the M1 gate has passed

**Rationale.** Demos A and C currently end in ClickHouse, which contradicts section 13, section 31's sink-priority decision, and the agreed P1 status. The proposed Demo A keeps the original "filter" step and the original Demo B is retained. The demo plan already defines these demos as levels 1 to 4 of one runnable example, `examples/edge-capture`.

**Depends on:** the ROS 2 decision, decided: ROS 2 is M2, so Demo C moves to the M2 gate as proposed. **Owner decision:** Approved (2026-10-07), as the rewrite of the section 46 demos to the P0 path that follows from D-B2-1 to D-B2-3.

## A.4 Section 7, "Protocol strategy": Tier 1 table

**What it says now.** The heading is "Tier 1 - MVP" and the table lists MQTT 3.1.1 / 5 (P0), ROS 2 / DDS-compatible ingestion (P0), HTTP/HTTPS (P0), gRPC (P0), OPC UA (P1), and CAN / CAN-FD (P1).

**Proposed text.** Retitle the heading "Tier 1 - Core protocols" and replace the table:

> | Protocol | Role | Priority | Milestone |
> |---|---|---|---|
> | MQTT 3.1.1 / 5 | IoT telemetry/pub-sub | P0 | M1 (3.1.1 first; version 5 per ADR-0006) |
> | ROS 2 / DDS-compatible ingestion | Robotics middleware | P0 | M2 |
> | HTTP/HTTPS | Generic device/vendor APIs | P1 | after the MVP |
> | gRPC | Modern application/device APIs | P1 | after the MVP |
> | OPC UA | Industrial/plant-floor data | P1 | after M2 |
> | CAN / CAN-FD | Vehicle/robot embedded bus | P1 | after M2 |

**Rationale.** Section 31 lists only MQTT and ROS 2 as MVP sources and section 32 places HTTP in Phase 1 and ROS 2 in Phase 2. HTTP and gRPC as P0 sources are not in any milestone plan; keeping them P0 contradicts the narrow MVP. HTTP as an admin API surface (health, metrics, status) is unaffected, because it is not a data source.

**Owner decision:** Approved (2026-10-07), with D-B2-4.

## A.5 Section 10, "Processing model": required transformation types

**What it says now.** P0 lists projection, field rename, field extraction, type conversion, unit conversion, filtering, routing, timestamp correction, metadata enrichment, deduplication, basic aggregation, sampling, downsampling. P1 lists windowing, joins against local/reference metadata, sequence validation, anomaly rules, and others.

**Proposed text.**

> ### P0
>
> - projection
> - field rename
> - field extraction
> - type conversion (widening only)
> - unit conversion
> - filtering
> - routing (to datasets and to quarantine)
> - timestamp extraction and correction, with explicit quality flags
> - metadata enrichment from a static file
> - deduplication (best-effort; authoritative deduplication happens downstream)
> - change-based filtering (deadband, on-change)
> - sampling and downsampling by decimation (every Nth sample, or a minimum interval)
>
> ### P1
>
> - windowing and basic aggregation
> - joins against local/reference metadata
> - sequence validation
> - (unchanged remainder)

**Rationale.** Basic aggregation needs windowing semantics (watermarks, late data, out-of-order input) that the plans explicitly defer; section 10 already puts windowing in P1. Downsampling by decimation does not need windows. Deduplication and change-based filtering are separate operations with different semantics (architecture review section 7).

**Owner decision:** Approved (2026-10-07), with D-B2-4.

## A.6 Section 14, "Pipeline configuration": example

**What it says now.** The YAML example has `sources` with `type: mqtt`, a `transforms` list with `decode`, `normalize_units`, `enrich`, and `filter` steps, a `buffer` block of `type: disk` and `max_size: 20GiB`, and a `clickhouse` sink with `endpoint` and `table`. It ends: "The configuration format must remain stable and versionable."

**Proposed text.** Replace the example (the filter step uses the structured-predicate form decided in ADR-0018, see A.11; the exact key spellings are frozen in PR5 and PR9):

```yaml
version: 1

pipeline:
  name: robot-telemetry
  node_id: edge-01
  data_dir: /var/lib/thadam/robot-telemetry

sources:
  - id: robot-mqtt
    kind: mqtt
    config:
      url: mqtt://edge-broker:1883
      client_id: thadam-edge-01
      topics:
        - { filter: "robots/+/telemetry", qos: 1 }
      ack_mode: on_commit

wal:
  max_disk: 20GiB
  fsync: { interval: 10ms }
  on_full: block

datasets:
  - id: telemetry
    match: { source: robot-mqtt, topic: "robots/+/telemetry" }
    decode: json
    identity: { robot: "{topic.1}" }
    time: { field: /ts, unit: ms, domain: utc }
    fields:
      - { name: temperature, type: f32, from: /temp, unit: degC }

transforms:
  - kind: filter
    dataset: telemetry
    where: { field: quality.status, eq: good }

sinks:
  - id: local
    kind: parquet
    config:
      datasets: [telemetry]
      rotation: { max_bytes: 128MiB, max_age: 5m }
  - id: s3
    kind: object_store
    config:
      from: local
      url: s3://robot-data/thadam
```

Followed by: "The configuration format must remain stable and versionable. The current schema is `version: 1`."

**Rationale.**

- The example's only sink is ClickHouse, which is P1.
- `buffer` becomes `wal` because the architecture stores raw messages before any processing, and `on_full` has no default (the operator must choose).
- `decode` is a dataset property and datasets are the unit of output (ADR-0013). The unit symbol `degC` in the sketch must be checked against the unit vocabulary in the transform design (UCUM codes such as `Cel`) [V].

**Owner decision:** Approved (2026-10-07), with D-B2-3. ADR-0012 and ADR-0018 were accepted on 2026-10-07; the filter example now uses the structured form (A.11). The remaining key spellings (`kind`, `where`, `wal`) are frozen in PR5 and PR9, so the syntax shown is the working form until the first fixture set is published.

## A.7 Section 40, "Developer experience"

**What it says now.**

> ```bash
> git clone ...
> docker compose up -d
> robotic-data run examples/mqtt-to-clickhouse/pipeline.yaml
> ```
> Then: `SELECT * FROM robot_telemetry ORDER BY event_time DESC;`
> The project should provide the demo broker, sample producer, and destination locally.

**Proposed text.**

> ```bash
> git clone ...
> cd thadam/examples/edge-capture
> make up
> make run
> make query
> ```
> Then, in DuckDB:
> ```sql
> SELECT * FROM 'data/**/ready/telemetry/**/*.parquet' ORDER BY _event_time DESC LIMIT 20;
> ```
> The project should provide the demo broker, sample producer, and a local destination (a local Parquet directory, with an S3-compatible store as the second step).

**Rationale.** The target workflow ends in ClickHouse, which is P1. The M1 equivalent needs no database: DuckDB reads the Parquet files directly. The exact file glob and command names are to be confirmed when the demo is built [V]. A ClickHouse variant moves to the P1 demo (`examples/edge-analytics`).

**Owner decision:** Approved (2026-10-07), with D-B2-3.

## A.8 Section 32, "Phase roadmap": Phases 1 and 2

**What it says now.** Phase 1 (MVP runtime): MQTT, HTTP, object store, ClickHouse, disk buffer, basic transforms, replay, metrics. Phase 2 (Robotics): ROS2, DDS/Zenoh evaluation, trajectory/pose structures, frame metadata, better timestamps.

**Proposed text.**

> ## Phase 1 - MVP runtime (M1)
>
> - MQTT
> - capture log (durable local buffer)
> - Parquet segments
> - object store
> - basic transforms
> - replay
> - metrics
>
> ## Phase 2 - Robotics (M2)
>
> - ROS 2: MCAP and rosbag2 offline ingestion, then a live transport
> - DDS/Zenoh evaluation
> - trajectory/pose structures
> - frame metadata
> - better timestamps
>
> ## Phase 2b - Real-time analytics (M3, after the M1 gate)
>
> - ClickHouse
> - HTTP and gRPC sources, as demand dictates

**Rationale.** Aligns the roadmap with sections 13 and 31 and with the milestone plan. HTTP moves out of Phase 1 for the reason in A.4.

**Owner decision:** Approved (2026-10-07), with D-B2-1 to D-B2-4.

## A.9 Section 5, use case UC-05: "remove duplicate values" and "window"

**What it says now.**

> 1000 Hz sensor -> quality validation -> remove duplicate values -> window -> downsample to 100 Hz -> transmit reduced stream

**Proposed text.**

> 1000 Hz sensor -> quality validation -> remove unchanged values (deadband) -> downsample to 100 Hz by decimation -> transmit reduced stream. Windowed aggregation (P1) can be added after the validation step.

**Rationale.** "Remove duplicate values" describes value-based, lossy, per-stream change filtering, not message deduplication. "Window" implies windowed aggregation, which is P1 (A.5).

**Owner decision:** Approved (2026-10-07), with D-B2-4.

## A.10 The ROS 2 decision, and the alternatives that were not chosen

**Decided by the owner (2026-10-07):** ROS 2 is a P0 product capability delivered in M2, not in MVP v0.1 (decision D-B2-2 in `docs/design/owner-decisions.md`). Text in A.1, A.3, A.4, and A.8 above reflects this. The rest of this section records what would have changed under the other options and is not to be applied.

Had the owner chosen to include MCAP offline ingestion in v0.1:

- A.1: MVP sources add "2. ROS 2 via MCAP and rosbag2 file input (offline). Live ROS 2 transport is delivered in M2." The M1 example list adds "ROS 2 MCAP file -> Parquet".
- A.3: the M1 definition adds "Demo C: ROS 2 MCAP file -> canonical event -> Parquet". Live transport stays an M2 gate.
- A.4: the ROS 2 row reads "M1 (MCAP file input); M2 (live transport)".
- A.8: Phase 1 adds "ROS 2 offline file ingestion".
- Cost, from the plans: a schema-driven CDR decoder and message-definition parser (`thadam-ros2-msg`) plus an MCAP source, a few reviewable pull requests, and an `thadam export --mcap` command. This delays the M1 gate and adds the `mcap` dependency to the MVP.

Had the owner chosen live ROS 2 in v0.1, the plans would not have supported it: the live-transport spike has not run, the leading Rust options are rated experimental or low-medium maturity in the ecosystem review, and the preferred route depends on ROS distributions that are not yet tiered. This option would also invalidate the readiness gate's schedule estimate.

## A.11 Section 14, "Pipeline configuration": filter syntax (structured predicates)

Added 2026-10-07 after the owner accepted ADR-0018.

**What it says now.** The `transforms` list in the section 14 example has a filter step written as an expression string:

> ```yaml
> - type: filter
>   expression: "quality == 'good'"
> ```

**Proposed text.** Replace the filter step with the structured-predicate form (this supersedes the `expr:` line shown in the A.6 text if the owner applied A.6 before this entry):

> ```yaml
> transforms:
>   - kind: filter
>     dataset: telemetry
>     where: { field: quality.status, eq: good }
> ```
>
> Conditions are structured predicates: a `field`, an operator (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `min`/`max`), and a value, combined with `all`, `any`, and `not`. Thadam does not evaluate free-form expression strings in v0.1. The `expr:` key is reserved for a future expression language.

If section 14 keeps its original `type` and `buffer` key names, the owner may apply only the filter step above and leave the key renaming to A.6.

**Rationale.** ADR-0018 (Accepted 2026-10-07): structured predicates are schema-checked at `thadam validate`, allocation-free, bounded in cost, and keep `f32`, `u64`, and null semantics in Thadam's hands. `quality` is a struct, so the condition names the status as `quality.status`. CEL is reserved for a future ADR, and each structured construct has an exact CEL equivalent so a later addition is additive.

**Owner decision:** Approved (2026-10-07), with the ADR-0018 decision (B7).

---

# Part B: Alignment with accepted architecture (recommended, not blocking)

These edits make `product.md` consistent with `ARCHITECTURE.md`. Apply them when the corresponding ADRs are accepted; if an ADR is rejected, the matching amendment is dropped or revised.

## B.1 Section 8, "Canonical event model"

**What it says now.** A `DataEnvelope` tree with `event_id`, `source_id`, `device_id`, `asset_id`, `stream_id`, `schema_id`, `event_time`, `ingest_time`, `sequence`, `quality`, `coordinate/frame metadata`, `protocol metadata`, `content_type`, `payload`, `attributes`; required temporal concepts `event_time`, `ingest_time`, `process_time`; hierarchical identity.

**Proposed text.** Replace the tree with a conceptual list and keep the time rule:

> ```text
> Event
> ├── event_id
> ├── source_id
> ├── dataset            (the output table the event belongs to)
> ├── identity           (hierarchical path; the device is one level)
> ├── times              (source_time, ingest_time, corrected_time, time_domain)
> ├── source_sequence
> ├── quality            (status, flags, reason)
> ├── payload            (decoded record, raw bytes, or object reference)
> └── attributes         (namespaced protocol metadata)
> ```
> `schema_id` belongs to the dataset or record shape, not each event. Coordinate frame information is a payload field following a well-known schema convention. Processing time is not carried per event. Where it is recorded (per batch in lineage metadata, or per file in footer metadata) is not yet specified. The original plan stored it per batch and the implementation plan dropped it **[OPEN, ADR-0001]**. The `clock_synced` state is recorded per batch. Never silently overwrite device time with edge time, and never treat a non-UTC device clock (monotonic since boot, simulation time, TAI) as UTC without an explicit mapping.

**Rationale.** Decisions D8 to D10, D15 (ADR-0001). Per-event schema references and frame metadata were removed to cut memory and avoid protocol leakage; device and asset identity were merged into one hierarchical path.

**Owner decision:** ______________

## B.2 Section 10, "Processing model": position of the buffer

**What it says now.** The flow is Source, Decode, Normalize, Validate, then Transform or Dead-letter, Enrich, Filter / Sample, Buffer, Sink.

**Proposed text.**

> ```text
> Source
>   |
>   v
> Capture log (raw, durable; the source is acknowledged after this step)
>   |
>   v   (one chain per sink)
> Decode -> Normalize -> Validate -> (Quarantine)
>   |
>   v
> Transform -> Enrich -> Filter / Sample
>   |
>   v
> Sink
> ```
> Each sink has its own chain, so different sinks can receive different transformations of the same captured data.

**Rationale.** Decision D1 (ADR-0003): capture before processing means a misconfigured filter loses nothing, replay is a cursor rewind, and the on-disk format does not depend on the in-memory model. The "dead-letter" stage becomes the quarantine writer.

**Owner decision:** ______________

## B.3 Section 12, "Buffering architecture"

**What it says now.** Requirements include "FIFO or priority policies" and "backpressure". Candidate storage approaches: embedded append-only log; RocksDB/redb-style store; Parquet segment files.

**Proposed text.**

- Replace the first requirement list item "FIFO or priority policies" with "FIFO ordering; priority policies are post-MVP".
- Add: "The buffer stores raw source messages before any decoding. A full disk is handled by an explicit, operator-chosen policy (`block` or `drop_oldest`); there is no default, and under either policy loss is counted and visible."
- Candidate approaches: "1. custom segmented append-only log (provisional); 2. SQLite in WAL mode (fallback); 3. redb or an LSM store. Parquet segment files are the P0 sink output, not the buffer, because the file footer is written only at close."

**Rationale.** D1, D2, D4 (ADR-0004). Parquet cannot be appended durably at low latency. SQLite was added to the candidate list by the architecture review, and the ecosystem review found no maintained segmented-log crate. Spike S1 has reported and recommends the custom segmented log with SQLite as the fallback, but ADR-0004 stays Draft until the S1 re-run on reference hardware, so this amendment still does not name a final engine. The example configuration above uses `interval: 10ms`, the provisional fsync default recommended by S1 (the earlier 50 ms default capped per-source throughput); it is not a frozen default.

**Owner decision:** ______________

## B.4 Section 19, "Connector contract"

**What it says now.** A `SourceConnector` trait with `start(context, output: EventSink)` and a `SinkConnector` trait with `write(batch) -> SinkAck`. "Avoid locking the exact trait design until benchmarks and concurrency requirements are understood."

**Proposed text.** Keep the closing sentence and replace the two sketches with:

> Sources are pulled by the runtime (`next_batch`), must keep protocol liveness independent of backpressure, and must be cancel-safe. Sinks come in two kinds: batch sinks (transactional targets such as ClickHouse, retried with the same batch identifier) and segment sinks (accumulating file targets such as Parquet, which report a durable watermark). The exact shapes are recorded in ADR-0002 and remain unstable (0.x) until the first release.

**Rationale.** D5 and D6 (ADR-0002). A push contract invites blocking the protocol loop, which breaks MQTT keepalive; one sink trait hides two incompatible acknowledgement models.

**Owner decision:** ______________

## B.5 Section 22, "Data quality"

**What it says now.** "Every event/batch should be able to carry quality metadata", with an example of a single-measurement message (`temperature = 91.2`, `quality = OUT_OF_RANGE`), and bad records routable to a DLQ, quarantine storage, metrics, and alerting.

**Proposed text.**

> Every event carries event-level quality (status, flags, reason). Batch-level state is recorded as lineage. Field-level quality is not supported in the MVP; a message with several measurements can be expanded to one event per measurement, where each event has its own quality. Bad records are routed to quarantine storage and counted in metrics; alerting is delegated to the metrics system.

**Rationale.** D10 and the open question on quality granularity (decision D-B3-3). The per-measurement example in the product document only fits single-measurement messages or exploded events. The owner decided on 2026-10-07 (D-B3-3) that quality is event-level only for the MVP, which is what this amendment states.

**Owner decision:** ______________

## B.6 Section 23, "Time synchronization"

**What it says now.** The product should expose: device timestamp, collector timestamp, clock offset estimate, sequence number, optional clock quality, out-of-order handling.

**Proposed text.** Replace the list:

> - source (device) timestamp and its time domain (UTC, TAI, monotonic since boot, simulation time, unknown)
> - ingest timestamp, with a per-batch flag for whether the edge clock was synchronized
> - optional corrected timestamp, always accompanied by a quality flag
> - sequence number
> - out-of-order flagging
>
> Clock offset estimation and clock quality are reserved and deferred until an estimation method is chosen.

**Rationale.** D15 (ADR-0001). No estimation method exists, and edge devices without a real-time clock boot with wrong wall time, so synchronization state must be recorded rather than assumed.

**Owner decision:** ______________

## B.7 Section 26, "Resource constraints"

**What it says now.** Initial target includes "local SSD". Baseline: "predictable memory", "no unbounded queues", "controlled disk usage".

**Proposed text.**

> - "local SSD" becomes "local SSD or eMMC that honors flush; consumer SD cards are not a supported target for durability claims" (owner decision D-B3-5, 2026-10-07: industrial SSD or eMMC that honors flush is the supported baseline).
> - Add: "Thadam-owned buffers are bounded by configuration. Total memory is measured and published per workload under container memory limits."

**Rationale.** D21 and the claims register in ADR-0011: Thadam cannot bound memory used inside third-party libraries and the allocator. Flash endurance depends on storage media; the owner set the supported baseline on 2026-10-07 (D-B3-5).

**Owner decision:** ______________

## B.8 Section 27, "Reliability requirements"

**What it says now.** "Must" includes survive network outage, maintain bounded memory, maintain bounded disk. "Avoid claiming universally": exactly-once, zero-loss, deterministic end-to-end latency.

**Proposed text.**

- Qualify "survive network outage": add "for sources with broker-side buffering, within the broker's queue limits and session expiry".
- Qualify "maintain bounded memory" as in B.7.
- Add to "Avoid claiming universally": power-loss survival (claimed only with a flush-honoring device and hardware evidence), ordering beyond receive order per connection, byte-identical replay, and authoritative deduplication inside the runtime.
- Add: "Every public reliability claim must appear in the claims register (ADR-0011) with its conditions."

**Rationale.** The architecture review's claims register (section 11), now ADR-0011.

**Owner decision:** ______________

## B.9 Section 25, "Security"

**What it says now.** MVP security includes "encryption-at-rest strategy for sensitive local buffers" and "API authentication".

**Proposed text.**

> - Encryption at rest: documented use of filesystem or block-level encryption (for example LUKS) for the MVP. Thadam does not implement application-level encryption of its capture log or segments. CRC32C checksums detect corruption but are not tamper protection.
> - Admin API: binds to loopback by default. A non-loopback bind requires a bearer token read from a file.
> - TLS uses rustls with the `ring` provider. The dependency policy is in ADR-0020.

**Rationale.** The MVP non-goals in ARCHITECTURE.md section 1 and ADR-0020.

**Owner decision:** ______________

## B.10 Sections 15, 29, and 18: CLI name and repository structure

- **Sections 15 and 29** use the command name `robotic-data`. Propose `thadam` (provisional until ADR-0016 clears the name).
- **Section 18** shows a repository named `robotic-data-plane` with a long list of per-protocol and per-sink concept crates. Replace the tree with a pointer: "The accepted repository and crate structure is in `ARCHITECTURE.md` section 9" and keep only the top-level layout (`docs/`, `crates/`, `apps/`, `examples/`, `benchmarks/`). The sentence "The actual structure may change after the coding-agent architecture review" is replaced by that pointer.

**Rationale.** The architecture review replaced 17 concept crates with a smaller set split by dependency weight (D20), and the implementation plan settled on ten library crates plus the CLI.

**Owner decision:** ______________

## B.11 Section 44, "Initial ADR candidates"

**What it says now.** ADR-001 to ADR-010, as listed in product.md.

**Proposed text.** Replace the list with the current index (full detail in `ARCHITECTURE.md` section 17):

> - ADR-0000 ADR process
> - ADR-0001 Canonical event model (includes internal representation and time semantics)
> - ADR-0002 Connector contract
> - ADR-0003 Execution and process model
> - ADR-0004 Durable buffer
> - ADR-0006 MQTT client
> - ADR-0007 ROS 2 strategy
> - ADR-0008 ClickHouse sink
> - ADR-0011 Delivery guarantees and claims register
> - ADR-0012 Config format and YAML crate
> - ADR-0013 Parquet segment layout
> - ADR-0018 Filter and expression language
> - ADR-0020 Dependency security and toolchain policy
> - Reserved, not yet written: 0009 object-store architecture, 0010 Python SDK boundary, 0015 recording and replay format, 0016 naming and publishing, 0017 plugin mechanism, 0019 transform semantics. Numbers 0005 and 0014 were folded into ADR-0001 and ADR-0013.

**Rationale.** The product document's numbering (ADR-005 Arrow vs protobuf, ADR-009 object store) no longer matches the decisions: the representation question was resolved inside the event-model ADR, and the delivery, layout, config, and toolchain decisions need their own records.

**Owner decision:** ______________

---

# Part C: Editorial

## C.1 Duplicate use-case number (section 5)

Section 5 has two items headed "UC-04" (Factory machine -> analytics, and Offline edge buffering). The planning documents refer to UC-05 (data reduction) and UC-06 (replay), so renumbering later items would break those references. Proposed: rename the first "UC-04A: Factory machine -> analytics" and the second "UC-04B: Offline edge buffering".

## C.2 Kafka listed under P1 and P2 (section 13)

Section 13 lists Kafka as a P1 sink ("Useful as a streaming/interoperability sink...") and again under P2. Proposed: keep the P1 entry and delete the P2 entry, or move both to a single position the owner prefers.

## C.3 Example directory names

Earlier plans named examples `mqtt-to-parquet`, `mqtt-to-s3`, `offline-replay`, and `mqtt-to-clickhouse`. The demo plan consolidates M1 into one example, `examples/edge-capture`, with levels 1 to 4, and names the P1 follow-on `examples/edge-analytics`. Product section 18 (examples) and section 40 should use those names.

---

# What this document does not change

The product thesis, the target users, the boundaries of scope, the data classes, the principle that Thadam is Rust-first and open source, the design principles in section 36, the security stance of building no identity provider, and the long-term vision are unchanged. The ClickHouse sink stays a first-class P1 integration, as the product document already says in sections 13 and 31; these amendments only remove the places where ClickHouse was still required for the MVP to be "done".
