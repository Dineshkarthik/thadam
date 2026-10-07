# Thadam — Product Requirements & Scope

**Status:** Draft v0.1  
**Date:** 2026-10-06  
**Document type:** Product + architecture source of truth  
**Implementation direction:** Rust-first, open source, edge-native  
**Project name:** Thadam  
**Descriptor:** Open-source robotics data infrastructure  
**Tagline:** The open data plane for robotics.

**Naming note:** Thadam is the selected project name. Treat it as the product name in docs, examples, CLI/package naming proposals, and public communications. Before public launch, validate repository, crate, domain, and trademark availability; the name is selected but not legally cleared.

---

## 1. Product thesis

Modern robots and industrial systems generate valuable data, but the path from physical device to an analytics/AI platform is fragmented.

A typical environment contains multiple protocols, vendors, payload formats, clocks, sensors, robot middleware, edge computers, brokers, and cloud/data platforms. Teams often solve this with custom bridges, scripts, Kafka Connect, ROS bags, vendor gateways, ETL jobs, and cloud-specific services.

The product should provide a **robotics-aware data plane** that sits between physical/robotic systems and modern data platforms.

### Core proposition

> **Connect any important robotic/industrial data source, understand and normalize the data at the edge, enrich/filter it safely, buffer it during failures, and deliver it to the data platform of choice.**

The product is not intended to become a robot operating system, a full SCADA system, a general-purpose workflow platform, or another generic enterprise ETL product.

The differentiation is **robotics and industrial data semantics + edge reliability + modern analytical sinks + Rust performance/safety**.

---

# 2. Problem

## 2.1 Current problems

Robotics and industrial teams commonly face:

- protocol fragmentation
- vendor-specific payloads
- telemetry schemas that are poorly documented or inconsistent
- different sampling rates
- clock and timestamp inconsistencies
- intermittent connectivity
- expensive transmission of redundant sensor data
- edge compute constraints
- lack of reliable local buffering
- difficulty applying filtering and enrichment before cloud transmission
- weak lineage from physical device to analytical record
- difficult replay/debugging
- different downstream targets across organizations
- separate tooling for robotics engineers and data engineers
- custom one-off integrations that become operationally expensive

## 2.2 Existing alternatives

The ecosystem contains strong components:

- MQTT brokers
- OPC UA gateways
- ROS/ROS 2 tooling
- DDS implementations
- Kafka / Kafka Connect
- Apache NiFi
- Apache Flink / Spark
- cloud IoT services
- vendor robotics platforms
- cloud data ingestion products
- generic observability/event pipelines

The gap is not simply "there is no connector."

The opportunity is a **cohesive, robotics-aware edge-to-data platform** that can combine:

`connect -> decode -> normalize -> validate -> enrich -> filter -> buffer -> batch -> deliver`

without forcing robotics teams to adopt a heavyweight distributed data platform.

---

# 3. Product vision

Build an open-source runtime that can run on:

- industrial PCs
- robot edge computers
- factory servers
- NVIDIA/ARM edge devices
- developer laptops
- Kubernetes
- cloud VMs
- managed/container environments

The same pipeline definition should be portable across environments.

### Vision

```text
Robots / Sensors / PLCs / Machines
            |
            v
+---------------------------+
| Thadam                     |
|                           |
| connectors                |
| decode + normalize        |
| validate + schema         |
| transform + enrich        |
| filter + sample           |
| local buffer / replay     |
| delivery + retry          |
+---------------------------+
            |
     +------+------+------------------+
     |      |      |        |         |
     v      v      v        v         v
 ClickHouse Iceberg S3    Kafka    Snowflake
                         /NATS
            |
            v
     Analytics / ML / AI
```

---

# 4. Target users

## Primary personas

### Robotics / controls engineer

Needs:

- simple access to robot telemetry
- protocol interoperability
- low-latency local processing
- deterministic behavior
- reliable delivery
- minimal operational burden

### Robotics platform engineer

Needs:

- fleet-wide configuration
- reusable pipelines
- schema/version management
- observability
- edge deployment
- replay/debugging

### Data engineer

Needs:

- analytical sinks
- structured schemas
- data quality
- partitions
- batch/stream delivery
- lakehouse integration
- lineage and metadata

### ML / AI engineer

Needs:

- synchronized sensor data
- trajectories
- events
- dataset generation
- replay
- labels/enrichment
- access to historical and real-time data

### Platform / infrastructure engineer

Needs:

- Linux/container/Kubernetes deployment
- security
- metrics/logging/tracing
- fleet management
- resource controls
- upgrades

---

# 5. Ideal use cases

## P0 use cases

### UC-01: Reliable robot data capture -> Parquet/object storage

```text
ROS2 / MQTT
    -> edge agent
    -> normalize
    -> filter
    -> durable local buffer
    -> Parquet segments
    -> S3-compatible object storage
```

This is the core reliability and portability path: capture data even when downstream services or network connectivity are unavailable, then replay or upload it when possible.

### UC-02: Robot telemetry -> S3/Parquet (Iceberg follow-on)

```text
ROS2 / MQTT
    -> normalize
    -> durable local Parquet segments
    -> S3-compatible object storage
```

This is a P0 portable-data path. Add Iceberg catalog, transaction, and snapshot semantics as a later P2 table-format integration once the underlying file/object-store path is reliable.

### UC-03: Robot telemetry -> ClickHouse (P1 real-time analytics)

```text
ROS 2 / MQTT
    -> edge agent
    -> normalize / filter
    -> durable buffer
    -> batch and deliver
    -> ClickHouse
```

Use this path to validate low-latency operational analytics, dashboarding, and time-window aggregations. It is a first-class early integration, but it is not a prerequisite for using Thadam or the platform’s defining storage path.

### UC-04: Factory machine -> analytics

```text
OPC UA
    -> edge collector
    -> unit normalization
    -> metadata enrichment
    -> ClickHouse
```

### UC-04: Offline edge buffering

When the network disappears:

```text
device -> edge buffer -> local durable storage

network returns
        |
        v
replay -> sink
```

### UC-05: Data reduction at edge

Example:

```text
1000 Hz sensor
      |
      +--> quality validation
      +--> remove duplicate values
      +--> window
      +--> downsample to 100 Hz
      |
      +--> transmit reduced stream
```

### UC-06: Robotics data replay

Record an incoming stream locally and replay it through the same transformations to debug a production issue or generate a training dataset.

---

# 6. Product boundaries

## In scope

- edge data collection
- robotic/industrial protocols
- telemetry/event ingestion
- binary and structured payload decoding
- schema normalization
- data quality
- enrichment
- filtering/sampling
- local durable buffering
- retry/replay
- sink delivery
- pipeline configuration
- observability
- security
- SDK/API
- fleet-friendly deployment primitives

## Explicitly out of scope initially

- robot motion planning
- robot control loops
- safety-certified control software
- full PLC/SCADA replacement
- HMI
- asset maintenance management
- general BPM/workflow orchestration
- data warehouse
- general BI
- large distributed stream-processing cluster
- video analytics platform
- full ML platform
- proprietary vendor lock-in

The platform may transport and reference video, but should not become a video storage/processing system in the MVP.

---

# 7. Protocol strategy

Protocol support must be modular. A protocol implementation must not leak into the core processing model.

## Tier 1 — MVP

| Protocol | Role | Priority |
|---|---|---|
| MQTT 3.1.1 / 5 | IoT telemetry/pub-sub | P0 |
| ROS 2 / DDS-compatible ingestion | Robotics middleware | P0 |
| HTTP/HTTPS | Generic device/vendor APIs | P0 |
| gRPC | Modern application/device APIs | P0 |
| OPC UA | Industrial/plant-floor data | P1 |
| CAN / CAN-FD | Vehicle/robot embedded bus | P1 |

## Tier 2

| Protocol | Role | Priority |
|---|---|---|
| Modbus TCP/RTU | Industrial equipment | P1 |
| WebSocket | Streaming device APIs | P1 |
| AMQP | Enterprise messaging | P2 |
| NATS | Cloud/edge messaging | P2 |
| CoAP | Constrained devices | P2 |
| DDS directly | Native DDS environments outside ROS | P2 |
| vendor-specific adapters | High-value integrations | P2 |

## Future / investigate

- EtherCAT
- PROFINET
- EtherNet/IP
- TSN-related industrial networking
- LoRaWAN
- BLE
- OPC UA PubSub
- Sparkplug B

### Important architectural rule

Do **not** build every protocol into the main binary at compile time forever.

The connector boundary must support:

- compiled-in connectors for critical protocols
- optional connectors
- plugin connectors
- protocol-specific configuration
- versioned connector contracts

---

# 8. Canonical event model

The canonical internal representation is one of the most important product decisions.

Do not make JSON the internal model.

The internal event should be typed and binary-friendly.

Conceptually:

```text
DataEnvelope
├── event_id
├── source_id
├── device_id
├── asset_id
├── stream_id
├── schema_id
├── event_time
├── ingest_time
├── sequence
├── quality
├── coordinate/frame metadata
├── protocol metadata
├── content_type
├── payload
└── attributes
```

## Required temporal concepts

At minimum distinguish:

- `event_time`: time the measurement/event happened
- `ingest_time`: time the data plane received it
- `process_time`: time processing occurred

Never silently overwrite device time with edge time.

## Identity

Support hierarchical identity:

```text
organization
  -> site
    -> line
      -> cell
        -> robot
          -> controller
            -> sensor
              -> stream
```

The product should not require all levels, but the model must permit them.

---

# 9. Data classes

The system should explicitly recognize different classes of robotic data.

### Telemetry

Examples:

- temperature
- pressure
- speed
- torque
- battery state

### State

Examples:

- robot mode
- controller state
- machine state
- safety state

### Events

Examples:

- cycle_start
- cycle_end
- fault
- alarm
- maintenance_event

### Commands

The MVP should support **transporting command/event-like messages only where explicitly configured**.

It must not present itself as a safety-critical robot control platform.

### Trajectory / pose data

Examples:

- XYZ
- quaternion
- joint positions
- velocity
- acceleration

### Sensor frames

Examples:

- lidar metadata
- camera frame references
- point-cloud metadata

### Binary payloads

Examples:

- vendor binary protocols
- packed CAN frames

### Media references

Large objects should generally be referenced, not placed into the telemetry event path.

Example:

```json
{
  "type": "camera_frame",
  "object_uri": "s3://bucket/path/frame.parquet",
  "capture_time": "...",
  "sensor_id": "camera-01"
}
```

---

# 10. Processing model

The platform should provide a lightweight streaming pipeline model.

```text
Source
  |
  v
Decode
  |
  v
Normalize
  |
  v
Validate
  |
  +----------------+
  |                |
  v                v
Transform       Dead-letter
  |
  v
Enrich
  |
  v
Filter / Sample
  |
  v
Buffer
  |
  v
Sink
```

## Required transformation types

### P0

- projection
- field rename
- field extraction
- type conversion
- unit conversion
- filtering
- routing
- timestamp correction
- metadata enrichment
- deduplication
- basic aggregation
- sampling
- downsampling

### P1

- windowing
- joins against local/reference metadata
- sequence validation
- anomaly rules
- geospatial operations
- coordinate/frame transformation
- stateful operators
- watermarking

### P2

- embedded ML inference
- feature extraction
- complex event processing
- advanced signal processing
- user-defined WASM operators

---

# 11. Edge-first execution model

The edge is a first-class environment, not merely a smaller cloud server.

The runtime must work under:

- unreliable network
- limited CPU
- limited memory
- intermittent power
- high local throughput
- long periods of disconnection

## Delivery semantics

Default:

- at-least-once
- durable local queue
- idempotency metadata
- retries
- exponential backoff

Exactly-once should be treated as a **sink-specific capability**, not as a universal product promise.

---

# 12. Buffering architecture

The MVP should include a local durable buffer.

Requirements:

- bounded disk usage
- configurable retention
- FIFO or priority policies
- checksummed records
- recovery after restart
- replay
- corruption detection
- backpressure
- sink-specific acknowledgements

Candidate storage approaches:

1. embedded append-only log
2. RocksDB/redb-style embedded store
3. Parquet segment files

Do not choose the implementation purely because it benchmarks well in isolation.

Benchmark:

- sustained writes
- crash recovery
- fsync behavior
- disk utilization
- replay speed
- memory footprint
- ARM performance

---

# 13. Sink strategy

Sink priorities are based on MVP user value and architectural validation, not on database popularity or familiarity. Thadam must remain sink-agnostic and useful at the edge without requiring a database service.

## P0 sinks — reliable capture and portability

### Local durable capture with Parquet segments

The first sink path should prove that Thadam can preserve valuable robot data through network outages and downstream downtime.

Required capabilities:

- configurable segment/rotation thresholds
- safe file finalization and crash recovery
- bounded disk usage and clear disk-full behavior
- schema and metadata alongside each segment
- replay/upload after reconnect
- integration tests for interrupted writes and restart recovery

### S3-compatible object storage

First-class generic object-store abstraction, initially focused on Parquet segments and optional raw-payload archives.

Targets include:

- Amazon S3
- MinIO
- compatible private object stores

Required capabilities:

- multipart upload where supported and appropriate
- retryable uploads and idempotent object naming
- configurable endpoint, bucket, prefix, and credentials
- clear behavior for partial uploads and interrupted connectivity
- Parquet plus optional JSON/NDJSON for debugging or interoperability

The MVP should prioritize a dependable local-file-to-object-store path. It should not require Iceberg catalog/transaction semantics to deliver portable data files.

## P1 sinks — real-time analytics and streaming interoperability

### ClickHouse

First-class early integration for users who need low-latency operational analytics, dashboards, time-window aggregations, and queryable telemetry. ClickHouse is valuable because it validates high-throughput batching, schema mapping, retry handling, and analytical delivery, but it is not a mandatory dependency and should not define Thadam's core architecture.

Required capabilities:

- batching and configurable flush thresholds
- schema/type mapping with explicit handling of incompatible fields
- retries, backpressure, and clear delivery acknowledgements
- documented deduplication/idempotency behavior
- compression and asynchronous ingestion
- benchmarked HTTP/native protocol choice; do not assume native is always best
- integration tests covering server downtime, retries, and duplicate delivery

### Kafka

Useful as a streaming/interoperability sink when users need to route normalized events to an existing event backbone.

### MQTT publishing

Prioritize if early users require forwarding normalized events to brokers or downstream device applications; keep it distinct from MQTT source support.

## P2 sinks — table formats and managed analytics destinations

### Apache Iceberg

Treat this as a table-format integration, not just an "Iceberg database connector". Start after the local Parquet/object-store path is reliable and the target users demonstrate a need for managed table semantics.

Support should cover:

- catalog configuration
- table creation or explicit table expectation
- schema mapping
- schema evolution where safe
- partitioning
- commit/retry behavior
- snapshot semantics

Use S3/object-store abstractions underneath where appropriate.

### Kafka

Useful as an interoperability sink.

### NATS

Useful for lightweight edge/application integrations.

## P2 sinks

- Snowflake
- GCP/GCS/BigQuery
- Databricks/Delta Lake
- AWS-native streaming services
- PostgreSQL
- Timescale-style time-series targets
- OpenSearch

### Cloud-provider principle

Prefer a generic storage/transport abstraction over creating dozens of thin cloud-specific sinks.

For example:

```text
ObjectStore
  -> S3
  -> GCS
  -> Azure Blob
  -> MinIO
```

Then separately:

```text
Lakehouse
  -> Iceberg
  -> Delta
```

---

# 14. Pipeline configuration

The user should define pipelines declaratively.

Example:

```yaml
pipeline:
  name: robot-telemetry

sources:
  - id: robot-mqtt
    type: mqtt
    config:
      broker: mqtt://edge-broker:1883
      topic: robots/+/telemetry
      qos: 1

transforms:
  - type: decode
    format: json

  - type: normalize_units
    temperature: celsius

  - type: enrich
    metadata:
      source: local_asset_registry

  - type: filter
    expression: "quality == 'good'"

buffer:
  type: disk
  max_size: 20GiB

sinks:
  - id: clickhouse
    type: clickhouse
    config:
      endpoint: http://clickhouse:8123
      table: robot_telemetry
```

The configuration format must remain stable and versionable.

---

# 15. CLI

Initial CLI:

```text
robotic-data
  init
  validate
  run
  status
  inspect
  replay
  benchmark
  connectors
  schemas
  pipelines
  logs
```

Examples:

```bash
robotic-data validate pipeline.yaml
robotic-data run pipeline.yaml
robotic-data status
robotic-data replay ./recordings/session-001
robotic-data benchmark --source mqtt --sink clickhouse
```

---

# 16. API surface

The system should expose:

### Local/admin API

- health
- readiness
- pipeline status
- connector status
- metrics
- configuration
- replay controls

### Data API

The MVP should avoid exposing all internals through REST.

Prefer:

- typed internal Rust API
- gRPC control/data API where network interaction is needed
- language-neutral schema

### SDK

Rust is the primary SDK.

Python should be first-class because robotics/data science teams commonly use Python.

Later:

- C/C++
- Go
- Java/Kotlin

---

# 17. Rust architecture

## Principles

- async-first
- zero-copy where practical
- explicit ownership/lifetimes only within internal implementation; public interfaces should be ergonomic
- avoid unnecessary serialization/deserialization cycles
- modular crates
- stable core traits
- no protocol-specific types leaking through the core event model

## Candidate stack

### Runtime

- Tokio

### Serialization

- serde
- serde_json

### Binary/structured formats

- prost / protobuf
- Apache Arrow Rust implementation
- Parquet Rust implementation

### HTTP

- hyper
- reqwest

### gRPC

- tonic

### Bytes / buffers

- bytes

### MQTT

- rumqttc or equivalent mature client

### OPC UA

- evaluate mature Rust OPC UA implementations carefully for protocol/security coverage

### CAN

- Linux SocketCAN ecosystem

### Object stores

- Apache Arrow `object_store` ecosystem or equivalent abstraction

### Python

- PyO3 + maturin

### WASM

Investigate a WASM runtime only after the operator/plugin abstraction is stable.

---

# 18. Crate/repository structure

Start as a Cargo workspace.

Proposed structure:

```text
robotic-data-plane/
├── Cargo.toml
├── README.md
├── LICENSE
├── product.md
├── ARCHITECTURE.md
├── CONTRIBUTING.md
├── SECURITY.md
├── docs/
│   ├── getting-started/
│   ├── concepts/
│   ├── protocols/
│   ├── sinks/
│   ├── operations/
│   └── design/
│
├── crates/
│   ├── core/
│   ├── model/
│   ├── pipeline/
│   ├── runtime/
│   ├── config/
│   ├── buffer/
│   ├── schema/
│   ├── observability/
│   ├── connector-api/
│   ├── source-mqtt/
│   ├── source-ros2/
│   ├── source-http/
│   ├── source-grpc/
│   ├── source-opcua/
│   ├── source-can/
│   ├── sink-clickhouse/
│   ├── sink-object-store/
│   ├── sink-kafka/
│   └── sink-iceberg/
│
├── apps/
│   └── robotic-data/
│
├── sdk/
│   └── python/
│
├── examples/
│   ├── mqtt-to-clickhouse/
│   ├── ros2-to-s3/
│   └── opcua-to-clickhouse/
│
└── benchmarks/
```

The actual structure may change after the coding-agent architecture review.

---

# 19. Connector contract

Every source connector should conceptually implement:

```rust
trait SourceConnector {
    fn metadata(&self) -> ConnectorMetadata;

    async fn start(
        &self,
        context: SourceContext,
        output: EventSink,
    ) -> Result<()>;
}
```

Every sink connector should conceptually implement:

```rust
trait SinkConnector {
    fn metadata(&self) -> ConnectorMetadata;

    async fn write(
        &self,
        batch: EventBatch,
    ) -> Result<SinkAck>;
}
```

Avoid locking the exact trait design until benchmarks and concurrency requirements are understood.

---

# 20. Schema strategy

The product needs a schema layer, but should not attempt to recreate a complete enterprise schema registry in the MVP.

Required capabilities:

- schema IDs
- versioning
- compatibility checks
- validation
- protocol-to-canonical mapping
- optional user-defined schemas
- metadata attachment

Potential formats:

- Protobuf
- Arrow Schema
- JSON Schema
- Avro where ecosystem interoperability demands it

The internal canonical representation should not require JSON.

---

# 21. Robotic semantics

This is a major differentiation area.

Support first-class metadata for:

### Coordinate frames

```text
map
  -> odom
    -> base_link
      -> sensor
```

### Poses

```text
position: x/y/z
orientation: quaternion
frame_id
```

### Joint state

```text
joint_name
position
velocity
effort
timestamp
```

### Sensor calibration

```text
sensor_id
calibration_version
intrinsics
extrinsics
valid_from
```

The product does not need to become a full robotics geometry framework.

It does need enough context so downstream systems can know **what a measurement means**.

---

# 22. Data quality

Every event/batch should be able to carry quality metadata.

Examples:

- valid
- invalid
- stale
- duplicated
- out_of_range
- clock_skew
- sequence_gap
- decode_error

Quality should be data, not only logs.

Example:

```text
temperature = 91.2
quality = OUT_OF_RANGE
quality_reason = sensor_limit_exceeded
```

Bad records should be routable to:

- DLQ
- quarantine storage
- metrics
- alerting

---

# 23. Time synchronization

Robotic data requires explicit treatment of time.

The product should expose:

- device timestamp
- collector timestamp
- clock offset estimate
- sequence number
- optional clock quality
- out-of-order handling

Future integrations may include:

- PTP metadata
- NTP-derived offset
- hardware timestamps

The system must never hide timestamp uncertainty.

---

# 24. Observability

The runtime must expose:

### Metrics

- events received
- events processed
- events dropped
- bytes received
- bytes transmitted
- processing latency
- queue depth
- retry count
- sink latency
- buffer utilization
- decode failures
- schema failures
- out-of-order records
- clock-skew measurements

### Logs

Structured JSON logs by default.

### Tracing

OpenTelemetry support where practical.

Every record should be traceable through:

```text
source -> pipeline -> transform -> buffer -> sink
```

without logging payloads by default.

---

# 25. Security

MVP security:

- TLS
- MQTT TLS
- OPC UA security profiles as supported by the selected implementation
- API authentication
- secrets supplied externally
- filesystem permission controls
- encryption-at-rest strategy for sensitive local buffers
- configurable certificate rotation
- least-privilege runtime
- container hardening

Do not build a custom identity provider.

Support integration with existing mechanisms.

---

# 26. Resource constraints

The runtime should be viable on modest edge hardware.

Initial target:

- Linux x86_64
- Linux ARM64
- 1–4 CPU cores
- 256 MB–2 GB RAM depending on workload
- local SSD

Target baseline for a simple telemetry pipeline:

- low idle CPU
- predictable memory
- no unbounded queues
- controlled disk usage

Every performance statement must be supported by reproducible benchmarks.

---

# 27. Reliability requirements

### Must

- recover after process crash
- recover after machine restart
- survive sink downtime
- survive network outage
- maintain bounded memory
- maintain bounded disk
- detect corrupt local buffers
- retry safely
- expose delivery state

### Avoid claiming universally

- exactly-once
- zero-loss
- deterministic end-to-end latency

Those depend on protocol, source, storage, network and sink behavior.

---

# 28. Error handling

Define error domains:

```text
CONNECTOR_ERROR
DECODE_ERROR
SCHEMA_ERROR
TRANSFORM_ERROR
QUALITY_ERROR
BUFFER_ERROR
SINK_ERROR
AUTH_ERROR
CONFIG_ERROR
```

Errors need:

- machine-readable code
- human-readable reason
- retryability
- severity
- source/pipeline context

---

# 29. Replay

Replay is an important differentiator.

Minimum model:

```bash
robotic-data replay session-001 \
  --pipeline pipeline.yaml \
  --speed 1.0
```

Support:

- original timing
- accelerated playback
- paused playback
- filtering by stream
- deterministic reprocessing

Use cases:

- debugging
- regression testing
- demos
- ML dataset generation
- incident analysis

---

# 30. Testing strategy

Every connector requires:

### Unit tests

Protocol parsing, validation, serialization.

### Integration tests

Real device/broker/database containers where practical.

### Failure tests

- connection loss
- reconnect
- malformed payload
- sink unavailable
- disk full
- process crash
- restart
- duplicate input
- out-of-order input

### Performance tests

- events/sec
- bytes/sec
- p50/p95/p99 latency
- CPU
- memory
- disk
- replay throughput

### Compatibility matrix

Track tested versions of:

- Linux
- ARM64/x86_64
- MQTT broker implementations
- ROS 2 distributions
- local Parquet capture
- S3-compatible stores
- ClickHouse as an early analytics sink

---

# 31. MVP definition

The MVP is deliberately narrow.

## MVP objective

Demonstrate that one Rust edge runtime can reliably ingest two important robotic/IoT protocols, transform the data, survive outages, and deliver to two modern data targets.

### MVP sources

1. MQTT
2. ROS 2 / DDS-compatible source

### MVP transforms

1. decode
2. normalize
3. filter
4. field mapping
5. unit conversion
6. deduplication
7. basic enrichment
8. batching

### MVP reliability

1. durable local buffer
2. retry
3. backpressure
4. restart recovery
5. replay

### MVP sinks

1. Local durable capture to Parquet segments (P0)
2. S3-compatible object storage for Parquet/raw archives (P0)
3. ClickHouse (P1 early integration, conditional on MVP capacity and target-user validation)

If delivery capacity is constrained, ship local Parquet + object storage first and defer ClickHouse rather than compromising durability, recovery, or replay. The architecture must support ClickHouse as an independent sink without making it a core runtime dependency.

### MVP interface

1. CLI
2. YAML configuration
3. Prometheus/OpenTelemetry-compatible observability
4. Rust API

### MVP examples

- MQTT -> durable local Parquet -> S3-compatible object storage
- ROS 2 -> durable local capture -> replay after reconnect
- MQTT -> ClickHouse (P1 analytics example, if included in MVP scope)
- offline edge -> reconnect -> replay without data loss within the configured retention budget

---

## Sink-priority decision (MVP)

**Decision:** P0 is durable local capture plus Parquet/object-store delivery. ClickHouse is P1: an important early analytics integration, but not a hard dependency or the default MVP sink if delivery capacity is limited.

**Rationale:** Robotics data may be expensive or impossible to reproduce, edge connectivity is intermittent, and payloads can include large binary sensor data. Durable capture, bounded buffering, recovery, replay, and portable files validate Thadam's edge-first promise before a database is available. ClickHouse validates the separate real-time analytics use case.

**Revisit when:** interviews or pilots show that queryable live telemetry is the dominant initial workflow, and a reliable local capture/object-store path can still be delivered without compromising it.

**Acceptance criteria:** the team can demonstrate a disconnected edge capture, restart recovery, replay/upload, bounded disk behavior, and independently configurable ClickHouse delivery; benchmark claims must be measured rather than assumed.

---

# 32. Phase roadmap

## Phase 0 — Foundations

- repository
- coding conventions
- canonical event model
- connector traits
- pipeline model
- CLI skeleton
- observability
- benchmark harness
- documentation
- CI

## Phase 1 — MVP runtime

- MQTT
- HTTP
- object store
- ClickHouse
- disk buffer
- basic transforms
- replay
- metrics

## Phase 2 — Robotics

- ROS2
- DDS/Zenoh evaluation
- trajectory/pose structures
- frame metadata
- better timestamps

## Phase 3 — Industrial

- OPC UA
- CAN
- Modbus
- Sparkplug B / OPC UA PubSub evaluation

## Phase 4 — Lakehouse

- Iceberg
- Parquet optimization
- schema evolution
- catalog integration

## Phase 5 — Fleet / platform

- remote configuration
- fleet management
- pipeline versioning
- deployment tooling
- centralized observability

## Phase 6 — Advanced processing

- stateful windows
- WASM operators
- anomaly detection
- lightweight model inference
- feature extraction

---

# 33. What NOT to build initially

Do not build:

- a GUI
- a managed cloud platform
- a full schema registry product
- a full Kafka replacement
- a general stream-processing engine
- a workflow scheduler
- proprietary robotics dashboards
- dozens of connectors
- custom ML infrastructure
- a video database
- Kubernetes operators before the single-binary runtime is proven

The first proof must be:

> "This little Rust binary reliably moves real robotic data into modern analytics systems."

---

# 34. Competitive positioning

The product must be compared against categories, not only named competitors.

| Category | Strength | Gap to exploit |
|---|---|---|
| MQTT brokers | Excellent transport | Not a robotics data plane |
| OPC UA gateways | Strong industrial interoperability | Limited modern analytical pipeline ergonomics |
| ROS tooling | Excellent robotics integration | Not designed as a general data-to-lakehouse platform |
| Kafka Connect | Huge connector ecosystem | Heavy for some edge environments; weak robotics semantics |
| NiFi | Broad connectivity and flow management | General purpose, not robotics-native |
| Flink/Spark | Powerful processing | Too heavy for many edge deployments |
| Cloud IoT platforms | Managed fleet/cloud integration | Cloud/vendor coupling |
| Vendor robotics platforms | Deep vendor integration | Vendor lock-in |
| Observability pipelines | Excellent telemetry routing | Focused on observability, not robot semantics |

The project should **complement** Kafka/Flink rather than claim to replace them.

Example architecture:

```text
Robot
  -> Thadam
  -> Kafka
  -> Flink
  -> Iceberg

or

Robot
  -> Thadam
  -> ClickHouse
```

---

# 35. Differentiation

## Strong differentiation

### 1. Robotics-aware canonical data model

The system understands:

- device identity
- event time
- frame
- pose
- sensor
- sequence
- quality
- trajectory

### 2. Edge-native reliability

- offline-first
- local durable buffering
- deterministic resource limits
- replay

### 3. Protocol + analytical stack bridge

Robotics teams get:

```text
ROS / MQTT / OPC UA / CAN
```

while data teams get:

```text
ClickHouse / Iceberg / S3 / Kafka / Snowflake
```

### 4. Rust core

Strong performance, small deployment footprint, memory safety, embeddability.

### 5. Open source

The connector ecosystem becomes a community asset.

---

# 36. Design principles

1. **Edge first**
2. **Robotics aware**
3. **Protocol agnostic**
4. **Data-platform neutral**
5. **Durable by default**
6. **Observable by default**
7. **Typed over stringly-typed**
8. **Configuration over custom code**
9. **Open standards over proprietary APIs**
10. **Benchmark before claiming performance**
11. **Do not reinvent mature infrastructure**
12. **Prefer composability to feature breadth**

---

# 37. Architectural decisions requiring validation

These must be researched/benchmarked before final implementation:

- ROS 2 native Rust integration vs bridge approach
- DDS implementation choice
- Zenoh position in future architecture
- embedded WAL implementation
- Arrow as internal batch representation
- protobuf vs Arrow vs custom envelope
- native Iceberg Rust capabilities and maturity
- ClickHouse native vs HTTP ingestion approach and whether its P1 scope is justified by target-user validation
- WASM operator model
- schema registry integration
- local metadata store
- exact connector plugin mechanism
- cloud credential handling
- encryption at rest
- multi-process vs single-process runtime

The coding agent must not silently turn these assumptions into hard architectural commitments.

---

# 38. Engineering quality bar

A feature is not considered complete because it "works on the happy path."

A connector or subsystem is complete only when it has:

- documentation
- tests
- failure behavior
- observability
- configuration validation
- security review
- benchmark numbers where performance matters
- example configuration
- upgrade/versioning considerations

---

# 39. Open-source strategy

The open-source repository should optimize for technical credibility.

### First public release

Focus on:

- excellent README
- architecture diagram
- runnable local demo
- Docker Compose demo
- benchmark results
- connector matrix
- clear roadmap
- clean Rust code
- good contribution guide

### Demonstration dataset

Create a synthetic robotic environment producing:

- joint states
- IMU
- temperature
- pose
- robot state
- fault events

This gives developers a zero-hardware path to try the system.

---

# 40. Developer experience

A developer should be able to go from zero to first pipeline in less than 10 minutes.

Target:

```bash
git clone ...
docker compose up -d
robotic-data run examples/mqtt-to-clickhouse/pipeline.yaml
```

Then:

```sql
SELECT *
FROM robot_telemetry
ORDER BY event_time DESC;
```

The project should provide the demo broker, sample producer, and destination locally.

---

# 41. Blog / thought-leadership strategy

The project and technical writing should reinforce each other.

The public narrative should be:

> "I am exploring what a modern data infrastructure stack for robotics should look like."

Not:

> "I built the definitive robotics data platform."

This makes the work credible even while the project is evolving.

## Suggested article sequence

1. **Robots Are Becoming Data Generators. The Data Stack Hasn't Caught Up.**
2. **What Happens Between a Robot and a Lakehouse?**
3. **MQTT vs ROS 2 vs OPC UA vs CAN: A Practical Robotics Data Protocol Guide**
4. **I Built a Rust Data Collector for Robot Telemetry**
5. **Why Robotic Data Is Different From Ordinary IoT Data**
6. **Designing a Canonical Event Model for Robotics**
7. **How to Buffer Robot Data When the Network Goes Down**
8. **ROS 2 to ClickHouse: Building a Real-Time Robotics Analytics Pipeline**
9. **Robot Data at 1,000 Hz: What Should You Process at the Edge?**
10. **Why Timestamp Semantics Matter in Robotics Data**
11. **Rust vs Go vs Python for Edge Data Infrastructure**
12. **From Robot Telemetry to Apache Iceberg**
13. **Designing Replayable Robotics Data Pipelines**
14. **Building a Robotics Data Plane With Open Standards**
15. **What the Modern Robotics Data Stack Should Look Like**

Each post should ideally contain:

- architecture diagram
- real benchmark
- runnable GitHub example
- lessons learned
- explicit tradeoffs
- links to standards and upstream projects

---

# 42. Success metrics

## Technical

- events/sec
- MB/sec
- p95 processing latency
- memory footprint
- CPU usage
- recovery time
- replay throughput
- buffer durability
- connector reliability

## Product

- time to first pipeline
- number of working integrations
- GitHub stars
- contributors
- issues/PRs
- repeat users
- community examples

## Thought leadership

- technical article readership
- GitHub referral traffic
- engineering discussions
- conference/community invitations
- references by practitioners
- inbound opportunities related to robotics/data infrastructure

The project should optimize for **technical respect**, not vanity metrics.

---

# 43. Governance

The project should use:

- semantic versioning
- ADRs for major architecture decisions
- public roadmap
- issue templates
- pull request template
- security policy
- release notes
- changelog
- compatibility matrix

Major architectural decisions should be recorded in:

```text
docs/design/adr/
```

---

# 44. Initial ADR candidates

Create these before or during MVP implementation:

- ADR-001: Canonical event model
- ADR-002: Connector architecture
- ADR-003: Pipeline execution model
- ADR-004: Local durable buffer
- ADR-005: Arrow vs protobuf internal representation
- ADR-006: MQTT implementation
- ADR-007: ROS2 strategy
- ADR-008: ClickHouse sink architecture
- ADR-009: Object-store architecture
- ADR-010: Python SDK boundary

---

# 45. Research and source policy

Technical and architectural claims should prefer:

1. protocol standards
2. official project documentation
3. upstream GitHub repositories
4. vendor technical documentation
5. academic/industry papers
6. reputable engineering publications

Avoid using marketing claims as architectural facts without validation.

For each major connector, maintain a source file:

```text
docs/protocols/<protocol>.md
```

containing:

- protocol overview
- operational characteristics
- Rust implementation options
- security
- throughput/latency considerations
- compatibility
- known limitations

---

# 46. Definition of MVP done

The MVP is done only when all of the following work:

### Demo A

```text
MQTT -> normalize -> filter -> buffer -> ClickHouse
```

### Demo B

```text
MQTT -> normalize -> Parquet -> S3
```

### Demo C

```text
ROS2 -> canonical event -> ClickHouse
```

### Failure demo

```text
source -> edge buffer -> network outage
                      |
                      X
                      |
                  recovery
                      |
                      v
                    sink
```

### Replay demo

```text
record -> replay -> pipeline -> destination
```

### Developer experience

A new developer can run the full demo locally with documented commands.

---

# 47. Long-term vision

The long-term platform can evolve into a **robotics data infrastructure layer** connecting physical systems to data, analytics, AI, digital twins, simulation, and training workflows.

Potential future capabilities:

```text
                 +--------------------+
                 | Robotics Data Plane|
                 +--------------------+
                 /    |       |       \
                /     |       |        \
          Analytics  AI/ML  Digital   Simulation
                           Twin
```

The long-term moat should not be "we support more protocols."

The stronger moat is:

> **a standardized, observable, replayable, robotics-aware data layer that makes physical-system data usable by modern data and AI infrastructure.**

---

# 48. Immediate next actions for the coding agent

The coding agent should:

1. Read this document completely.
2. Treat it as the product source of truth.
3. Identify assumptions requiring validation.
4. Research current Rust/robotics/data infrastructure options where the document marks a decision as uncertain.
5. Create an implementation plan before coding.
6. Create ADRs for decisions that materially affect the architecture.
7. Define the smallest vertical slice that demonstrates real value.
8. Build benchmarks into the plan from the beginning.
9. Avoid premature support for dozens of connectors.
10. Keep the core protocol-agnostic and sink-agnostic.

**Critical:** Do not begin implementation by generating thousands of lines of code. First establish architecture, module boundaries, event model, connector contract, buffer semantics, test strategy, and the first demonstrable vertical slice.

---

# 49. Product north star

### One sentence

> **An open-source, Rust-first data plane that reliably turns heterogeneous robotic and industrial data into clean, contextual, replayable data for modern analytics and AI systems.**

### First principle

> **Make the edge boring, reliable and observable — so the rest of the data stack can stay simple.**
