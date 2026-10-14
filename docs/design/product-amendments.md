# Amendments to product.md

Date: 2026-10-07 (Part A applied 2026-10-13).

`product.md` is the source of truth for product direction. This document records the scope amendments (Part A) approved by the maintainer on 2026-10-07 and applied to `product.md` on 2026-10-13 (A.1 to A.9 and A.11 verbatim; A.10 records the unchosen ROS 2 alternatives and is not applied), followed by architecture-alignment amendments (Part B, where the underlying technical decisions are recorded in the accepted ADRs of 2026-10-07 while applying the replacement prose to `product.md` remains Open) and editorial items (Part C, Open).

## Milestone vocabulary

The amendments use three milestone names from `ARCHITECTURE.md` section 1:

- **M1 (MVP v0.1):** MQTT to a durable capture log to Parquet segments to S3-compatible storage, with replay and chaos verification.
- **M2:** ROS 2, MCAP and rosbag2 offline ingestion first, then a live transport.
- **M3:** ClickHouse, outside the M1 reliability gate.

---

# Part A: Scope freeze (applied 2026-10-13)

## A.1 Section 31, "MVP definition": objective, sources, transforms, reliability, sinks, examples

**Original text.**

> The MVP is deliberately narrow.
> **MVP objective:** Demonstrate that one Rust edge runtime can reliably ingest two important robotic/IoT protocols, transform the data, survive outages, and deliver to two modern data targets.
> **MVP sources:** 1. MQTT 2. ROS 2 / DDS-compatible source
> **MVP transforms:** 1. decode 2. normalize 3. filter 4. field mapping 5. unit conversion 6. deduplication 7. basic enrichment 8. batching
> **MVP reliability:** 1. durable local buffer 2. retry 3. backpressure 4. restart recovery 5. replay
> **MVP sinks:** 1. Local durable capture to Parquet segments (P0) 2. S3-compatible object storage for Parquet/raw archives (P0) 3. ClickHouse (P1 early integration, conditional on MVP capacity and target-user validation)
> **MVP examples:** MQTT -> durable local Parquet -> S3-compatible object storage; ROS 2 -> durable local capture -> replay after reconnect; MQTT -> ClickHouse (P1 analytics example, if included in MVP scope); offline edge -> reconnect -> replay without data loss within the configured retention budget

**Applied text.**

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

- The product document already says P0 is capture plus Parquet and object storage, and says to defer ClickHouse first if capacity is constrained. The unamended list in this section still described "two protocols" and "two modern data targets", which contradicted that decision.
- The MVP transform list omitted sampling, timestamp correction, and routing, which section 10 lists as P0. The architecture needs routing (datasets and quarantine) and timestamp extraction.
- "Deduplication" is split: in-process deduplication loses its state when redelivery happens across a long gap and cannot be authoritative, while "remove duplicate values" in use case UC-05 is deadband filtering, a different operation.
- "Without data loss" is qualified to the broker's queue limits because `block` does not prevent loss when the broker's persistent-session queue is bounded (Mosquitto defaults to 1000 queued messages [V]).

**Decision:** Approved by the maintainer (2026-10-07, D-B2-1 and D-B2-2); applied to `product.md` on 2026-10-13.

## A.2 Section 31, "Sink-priority decision (MVP)": acceptance criteria

**Original text.**

> **Acceptance criteria:** the team can demonstrate a disconnected edge capture, restart recovery, replay/upload, bounded disk behavior, and independently configurable ClickHouse delivery; benchmark claims must be measured rather than assumed.

**Applied text.**

> **Acceptance criteria (M1 gate):** the team can demonstrate disconnected edge capture, restart recovery, replay and upload, bounded disk behavior with every loss attributed, and quarantine of malformed input, each verified against the producer's own record of what it sent; benchmark claims must be measured rather than assumed.
>
> **Acceptance criteria (P1 gate, after M1):** independently configurable ClickHouse delivery that does not change the M1 results. The Parquet and object-storage verification must still pass while ClickHouse is unavailable.

**Rationale.** The M1 criteria that matter most (zero missing acknowledged samples, zero duplicate rows, bounded disk, attributed loss) are defined by scenarios F1 to F8. A criterion that requires ClickHouse cannot be part of an M1 gate if ClickHouse is not in M1.

**Decision:** Approved by the maintainer (2026-10-07, D-B2-3); applied to `product.md` on 2026-10-13.

## A.3 Section 46, "Definition of MVP done"

**Original text.**

> **Demo A:** MQTT -> normalize -> filter -> buffer -> ClickHouse
> **Demo B:** MQTT -> normalize -> Parquet -> S3
> **Demo C:** ROS2 -> canonical event -> ClickHouse
> **Failure demo:** source -> edge buffer -> network outage -> recovery -> sink
> **Replay demo:** record -> replay -> pipeline -> destination
> **Developer experience:** A new developer can run the full demo locally with documented commands.

**Applied text.**

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

**Rationale.** Demos A and C originally ended in ClickHouse, which contradicted section 13, section 31's sink-priority decision, and the P1 status. These demos correspond to levels 1 to 4 of `examples/edge-capture`.

**Decision:** Approved by the maintainer (2026-10-07, D-B2-1 to D-B2-3); applied to `product.md` on 2026-10-13.

## A.4 Section 7, "Protocol strategy": Tier 1 table

**Original text.** The heading was "Tier 1 - MVP" and the table listed MQTT 3.1.1 / 5 (P0), ROS 2 / DDS-compatible ingestion (P0), HTTP/HTTPS (P0), gRPC (P0), OPC UA (P1), and CAN / CAN-FD (P1).

**Applied text.** Retitle the heading "Tier 1 - Core protocols" and replace the table:

> | Protocol | Role | Priority | Milestone |
> |---|---|---|---|
> | MQTT 3.1.1 / 5 | IoT telemetry/pub-sub | P0 | M1 (3.1.1 first; version 5 per ADR-0006) |
> | ROS 2 / DDS-compatible ingestion | Robotics middleware | P0 | M2 |
> | HTTP/HTTPS | Generic device/vendor APIs | P1 | after the MVP |
> | gRPC | Modern application/device APIs | P1 | after the MVP |
> | OPC UA | Industrial/plant-floor data | P1 | after M2 |
> | CAN / CAN-FD | Vehicle/robot embedded bus | P1 | after M2 |

**Rationale.** Section 31 lists only MQTT and ROS 2 as MVP sources and section 32 places HTTP in Phase 1 and ROS 2 in Phase 2. HTTP as an admin API surface (health, metrics, status) is unaffected because it is not a data source.

**Decision:** Approved by the maintainer (2026-10-07, D-B2-4); applied to `product.md` on 2026-10-13.

## A.5 Section 10, "Processing model": required transformation types

**Original text.** P0 listed projection, field rename, field extraction, type conversion, unit conversion, filtering, routing, timestamp correction, metadata enrichment, deduplication, basic aggregation, sampling, downsampling. P1 listed windowing, joins against local/reference metadata, sequence validation, anomaly rules, and others.

**Applied text.**

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

**Rationale.** Basic aggregation requires windowing semantics (watermarks, late data, out-of-order input), which section 10 places in P1. Downsampling by decimation does not need windows. Deduplication and change-based filtering are separate operations with different semantics.

**Decision:** Approved by the maintainer (2026-10-07, D-B2-4); applied to `product.md` on 2026-10-13.

## A.6 Section 14, "Pipeline configuration": example

**Original text.** The YAML example had `sources` with `type: mqtt`, a `transforms` list with `decode`, `normalize_units`, `enrich`, and `filter` steps, a `buffer` block of `type: disk` and `max_size: 20GiB`, and a `clickhouse` sink with `endpoint` and `table`. It ended: "The configuration format must remain stable and versionable."

**Applied text.** Replace the example (the filter step uses the structured-predicate form decided in ADR-0018, see A.11):

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

- The original example's only sink was ClickHouse, which is P1.
- `buffer` becomes `wal` because the architecture stores raw messages before any processing, and `on_full` has no default.
- `decode` is a dataset property and datasets are the unit of output (ADR-0013).

**Decision:** Approved by the maintainer (2026-10-07, D-B2-3, ADR-0012, ADR-0018); applied to `product.md` on 2026-10-13.

## A.7 Section 40, "Developer experience"

**Original text.**

> ```bash
> git clone ...
> docker compose up -d
> robotic-data run examples/mqtt-to-clickhouse/pipeline.yaml
> ```
> Then: `SELECT * FROM robot_telemetry ORDER BY event_time DESC;`
> The project should provide the demo broker, sample producer, and destination locally.

**Applied text.**

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

**Rationale.** The original workflow ended in ClickHouse, which is P1. The M1 workflow reads the Parquet files directly with DuckDB.

**Decision:** Approved by the maintainer (2026-10-07, D-B2-3); applied to `product.md` on 2026-10-13.

## A.8 Section 32, "Phase roadmap": Phases 1 and 2

**Original text.** Phase 1 (MVP runtime): MQTT, HTTP, object store, ClickHouse, disk buffer, basic transforms, replay, metrics. Phase 2 (Robotics): ROS2, DDS/Zenoh evaluation, trajectory/pose structures, frame metadata, better timestamps.

**Applied text.**

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

**Rationale.** Aligns the roadmap with sections 13 and 31 and with the milestone plan.

**Decision:** Approved by the maintainer (2026-10-07, D-B2-1 to D-B2-4); applied to `product.md` on 2026-10-13.

## A.9 Section 5, use case UC-05: "remove duplicate values" and "window"

**Original text.**

> 1000 Hz sensor -> quality validation -> remove duplicate values -> window -> downsample to 100 Hz -> transmit reduced stream

**Applied text.**

> 1000 Hz sensor -> quality validation -> remove unchanged values (deadband) -> downsample to 100 Hz by decimation -> transmit reduced stream. Windowed aggregation (P1) can be added after the validation step.

**Rationale.** "Remove duplicate values" describes value-based, lossy, per-stream change filtering, not message deduplication. "Window" implies windowed aggregation, which is P1 (A.5).

**Decision:** Approved by the maintainer (2026-10-07, D-B2-4); applied to `product.md` on 2026-10-13.

## A.10 The ROS 2 decision, and the alternatives that were not chosen

**Decision (2026-10-07):** ROS 2 is a P0 product capability delivered in M2, not in MVP v0.1 (D-B2-2 in `docs/design/owner-decisions.md`). Text in A.1, A.3, A.4, and A.8 reflects this. The rest of this section records what would have changed under the other options and is not applied to `product.md`:

- Under option B (MCAP offline ingestion in v0.1): A.1 would add "2. ROS 2 via MCAP and rosbag2 file input (offline)" to MVP sources; A.3 would add "Demo C: ROS 2 MCAP file -> canonical event -> Parquet" to M1; A.4 would list "M1 (MCAP file input); M2 (live transport)"; and A.8 would add "ROS 2 offline file ingestion" to Phase 1.
- Under option C (live ROS 2 in v0.1): rejected because live transport required spike S3 and experimental middleware bindings.

## A.11 Section 14, "Pipeline configuration": filter syntax (structured predicates)

Added 2026-10-07 after ADR-0018 was accepted.

**Original text.** The `transforms` list in the section 14 example had a filter step written as an expression string:

> ```yaml
> - type: filter
>   expression: "quality == 'good'"
> ```

**Applied text.** Replace the filter step with the structured-predicate form:

> ```yaml
> transforms:
>   - kind: filter
>     dataset: telemetry
>     where: { field: quality.status, eq: good }
> ```
>
> Conditions are structured predicates: a `field`, an operator (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `min`/`max`), and a value, combined with `all`, `any`, and `not`. Thadam does not evaluate free-form expression strings in v0.1. The `expr:` key is reserved for a future expression language.

**Rationale.** ADR-0018 (Accepted 2026-10-07): structured predicates are schema-checked at `thadam validate`, allocation-free, bounded in cost, and keep `f32`, `u64`, and null semantics explicit. CEL is reserved for a future ADR.

**Decision:** Approved by the maintainer (2026-10-07, D-B7-2); applied to `product.md` on 2026-10-13.

---

# Part B: Alignment with accepted architecture

These amendments align `product.md` with the decisions in `ARCHITECTURE.md` and the accepted ADRs. The underlying technical decisions were made on 2026-10-07 (and 2026-10-14 for [ADR-0016](adr/0016-naming-and-publishing.md)); applying the replacement text to `product.md` remains **Open**.

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
> `schema_id` belongs to the dataset or record shape, not each event. Coordinate frame information is a payload field following a well-known schema convention. Processing time is not carried per event; where it is recorded (per batch in lineage metadata, or per file in footer metadata) remains open (`ARCHITECTURE.md` O10, ADR-0001). The `clock_synced` state is recorded per batch. Never silently overwrite device time with edge time, and never treat a non-UTC device clock (monotonic since boot, simulation time, TAI) as UTC without an explicit mapping.

**Rationale.** Decisions D8 to D10 and D15 (ADR-0001). Per-event schema references and frame metadata were removed to cut memory and avoid protocol leakage; device and asset identity were merged into one hierarchical path.

**Decision:** Underlying model decided in ADR-0001 (Accepted 2026-10-07; D-B6-3, D-B6-4, D-B6-5). `process_time` placement (O10) and applying this amendment text to `product.md`: Open.

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

**Decision:** Capture-first WAL placement decided in ADR-0003 (Accepted 2026-10-07, D1). Applying this amendment text to `product.md`: Open.

## B.3 Section 12, "Buffering architecture"

**What it says now.** Requirements include "FIFO or priority policies" and "backpressure". Candidate storage approaches: embedded append-only log; RocksDB/redb-style store; Parquet segment files.

**Proposed text.**

- Replace the first requirement list item "FIFO or priority policies" with "FIFO ordering; priority policies are post-MVP".
- Add: "The buffer stores raw source messages before any decoding. A full disk is handled by an explicit, operator-chosen policy (`block` or `drop_oldest`); there is no default, and under either policy loss is counted and visible."
- Candidate approaches: "1. custom segmented append-only log (provisional); 2. SQLite in WAL mode (fallback); 3. redb or an LSM store. Parquet segment files are the P0 sink output, not the buffer, because the file footer is written only at close."

**Rationale.** D1, D2, D4 (ADR-0004, ADR-0011). Parquet cannot be appended durably at low latency. Spike S1 (`docs/design/spikes/S1-wal.md`) confirmed the custom segmented log with SQLite as the fallback, while ADR-0004 stays Draft until the S1 re-run on reference hardware.

**Decision:** Explicit `on_full` policy decided 2026-10-07 (D-B3-1, ADR-0011 Accepted); custom segmented WAL implemented (`thadam-wal`), with ADR-0004 remaining Draft pending the reference-hardware re-run. Applying this amendment text to `product.md`: Open.

## B.4 Section 19, "Connector contract"

**What it says now.** A `SourceConnector` trait with `start(context, output: EventSink)` and a `SinkConnector` trait with `write(batch) -> SinkAck`. "Avoid locking the exact trait design until benchmarks and concurrency requirements are understood."

**Proposed text.** Keep the closing sentence and replace the two sketches with:

> Sources are pulled by the runtime (`next_batch`), must keep protocol liveness independent of backpressure, and must be cancel-safe. Sinks come in two kinds: batch sinks (transactional targets such as ClickHouse, retried with the same batch identifier) and segment sinks (accumulating file targets such as Parquet, which report a durable watermark). The exact shapes are recorded in ADR-0002 and remain unstable (0.x) until the first release.

**Rationale.** D5 and D6 (ADR-0002). A push contract invites blocking the protocol loop, which breaks MQTT keepalive; one sink trait hides two incompatible acknowledgement models.

**Decision:** Pull `Source` and split `BatchSink`/`SegmentSink` contracts decided in ADR-0002 (Accepted 2026-10-07; D-B6-1, D-B6-2). Applying this amendment text to `product.md`: Open.

## B.5 Section 22, "Data quality"

**What it says now.** "Every event/batch should be able to carry quality metadata", with an example of a single-measurement message (`temperature = 91.2`, `quality = OUT_OF_RANGE`), and bad records routable to a DLQ, quarantine storage, metrics, and alerting.

**Proposed text.**

> Every event carries event-level quality (status, flags, reason). Batch-level state is recorded as lineage. Field-level quality is not supported in the MVP; a message with several measurements can be expanded to one event per measurement, where each event has its own quality. Bad records are routed to quarantine storage and counted in metrics; alerting is delegated to the metrics system.

**Rationale.** D10 and D-B3-3 (ADR-0001). The per-measurement example in the product document fits single-measurement messages or exploded events.

**Decision:** Event-level quality for the MVP decided 2026-10-07 (D-B3-3, ADR-0001 Accepted). Applying this amendment text to `product.md`: Open.

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

**Rationale.** D15 (ADR-0001). Edge devices without a real-time clock boot with wrong wall time, so synchronization state must be recorded rather than assumed.

**Decision:** Time model decided in ADR-0001 (Accepted 2026-10-07, D15). Applying this amendment text to `product.md`: Open.

## B.7 Section 26, "Resource constraints"

**What it says now.** Initial target includes "local SSD". Baseline: "predictable memory", "no unbounded queues", "controlled disk usage".

**Proposed text.**

> - "local SSD" becomes "local SSD or eMMC that honors flush; consumer SD cards are not a supported target for durability claims" (D-B3-5, 2026-10-07).
> - Add: "Thadam-owned buffers are bounded by configuration. Total memory is measured and published per workload under container memory limits."

**Rationale.** D21 and the claims register in ADR-0011: Thadam cannot bound memory used inside third-party libraries and the allocator, and flash endurance depends on storage media (D-B3-5).

**Decision:** Storage baseline (D-B3-5) and memory claim wording (D21, ADR-0011 Accepted) decided 2026-10-07. Applying this amendment text to `product.md`: Open.

## B.8 Section 27, "Reliability requirements"

**What it says now.** "Must" includes survive network outage, maintain bounded memory, maintain bounded disk. "Avoid claiming universally": exactly-once, zero-loss, deterministic end-to-end latency.

**Proposed text.**

- Qualify "survive network outage": add "for sources with broker-side buffering, within the broker's queue limits and session expiry".
- Qualify "maintain bounded memory" as in B.7.
- Add to "Avoid claiming universally": power-loss survival (claimed only with a flush-honoring device and hardware evidence), ordering beyond receive order per connection, byte-identical replay, and authoritative deduplication inside the runtime.
- Add: "Every public reliability claim must appear in the claims register (ADR-0011) with its conditions."

**Rationale.** The claims register in ADR-0011.

**Decision:** Claims register and conditions decided in ADR-0011 (Accepted 2026-10-07). Applying this amendment text to `product.md`: Open.

## B.9 Section 25, "Security"

**What it says now.** MVP security includes "encryption-at-rest strategy for sensitive local buffers" and "API authentication".

**Proposed text.**

> - Encryption at rest: documented use of filesystem or block-level encryption (for example LUKS) for the MVP. Thadam does not implement application-level encryption of its capture log or segments. CRC32C checksums detect corruption but are not tamper protection.
> - Admin API: binds to loopback by default. A non-loopback bind requires a bearer token read from a file.
> - TLS uses rustls with the `ring` provider. The dependency policy is in ADR-0020.

**Rationale.** `ARCHITECTURE.md` sections 1 and 16, and ADR-0020.

**Decision:** Security and toolchain policy decided in ADR-0020 (Accepted 2026-10-07). Applying this amendment text to `product.md`: Open.

## B.10 Sections 15, 29, and 18: CLI name and repository structure

- **Sections 15 and 29** use the command name `robotic-data`. Propose `thadam` (provisional until [ADR-0016](adr/0016-naming-and-publishing.md), Draft, clears the name).
- **Section 18** shows a repository named `robotic-data-plane` with a long list of per-protocol and per-sink concept crates. Replace the tree with a pointer: "The accepted repository and crate structure is in `ARCHITECTURE.md` section 9" and keep only the top-level layout (`docs/`, `crates/`, `apps/`, `examples/`, `tools/`, `sdk/`, `website/`).

**Rationale.** ADR-0003 (D20) replaced the 17 concept crates with a workspace split by dependency weight, and [ADR-0016](adr/0016-naming-and-publishing.md) (Draft) records the provisional `thadam` name.

**Decision:** Crate layout decided in ADR-0003 (Accepted 2026-10-07, D20) and provisional name recorded in [ADR-0016](adr/0016-naming-and-publishing.md) (Draft 2026-10-14). Applying this amendment text to `product.md`: Open.

## B.11 Section 44, "Initial ADR candidates"

**What it says now.** ADR-001 to ADR-010, as listed in `product.md`.

**Proposed text.** Replace the list with the current index (full detail in `ARCHITECTURE.md` section 17):

> - ADR-0000 ADR process (Proposed)
> - ADR-0001 Canonical event model (Accepted)
> - ADR-0002 Connector contract (Accepted)
> - ADR-0003 Execution and process model (Accepted)
> - ADR-0004 Durable buffer (Draft)
> - ADR-0006 MQTT client (Accepted)
> - ADR-0007 ROS 2 strategy (Draft)
> - ADR-0008 ClickHouse sink (Draft)
> - ADR-0011 Delivery guarantees and claims register (Accepted)
> - ADR-0012 Config format and YAML crate (Accepted)
> - ADR-0013 Parquet segment layout (Accepted)
> - ADR-0015 Recording and replay format (Draft)
> - [ADR-0016 Naming and publishing](adr/0016-naming-and-publishing.md) (Draft)
> - ADR-0018 Filter and expression language (Accepted)
> - ADR-0020 Dependency security and toolchain policy (Accepted)
> - Reserved, not yet written: 0009 object-store architecture, 0010 Python SDK boundary, 0017 plugin mechanism, 0019 transform semantics. Numbers 0005 and 0014 were folded into ADR-0001 and ADR-0013.

**Rationale.** The product document's initial ADR numbering no longer matches the ADR directory.

**Decision:** ADR index reflects the accepted and draft ADRs in `docs/design/adr/`. Applying this amendment text to `product.md`: Open.

---

# Part C: Editorial

## C.1 Duplicate use-case number (section 5)

Section 5 has two items headed "UC-04" (Factory machine -> analytics, and Offline edge buffering). The design documents refer to UC-05 (data reduction) and UC-06 (replay), so renumbering later items would break those references. Proposed: rename the first "UC-04A: Factory machine -> analytics" and the second "UC-04B: Offline edge buffering".

**Decision:** Open.

## C.2 Kafka listed under P1 and P2 (section 13)

Section 13 lists Kafka as a P1 sink ("Useful as a streaming/interoperability sink...") and again under P2. Proposed: keep the P1 entry and delete the P2 entry.

**Decision:** Open.

## C.3 Example directory names

Earlier drafts named examples `mqtt-to-parquet`, `mqtt-to-s3`, `offline-replay`, and `mqtt-to-clickhouse`. The repository consolidates M1 into `examples/edge-capture` (with levels 1 to 4) and `examples/ros2-mcap-to-parquet` for M2, and names the P1 ClickHouse follow-on `examples/edge-analytics`. `product.md` section 18 (examples) and section 40 (already updated by A.7) should use those names.

**Decision:** Open.

---

# What this document does not change

The product thesis, the target users, the boundaries of scope, the data classes, the principle that Thadam is Rust-first and open source, the design principles in section 36, the security stance of building no identity provider, and the long-term vision are unchanged. The ClickHouse sink stays a first-class P1 integration, as `product.md` states in sections 13 and 31.
