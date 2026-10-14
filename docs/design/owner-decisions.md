# Decision register

Date: 2026-10-07.

This document records the scope, product-default, core-API, configuration, and ADR acceptance decisions made by the maintainer on 2026-10-07. The record-representation microbenchmark (`docs/design/spikes/D8-record-representation.md`), the WAL spike (`docs/design/spikes/S1-wal.md`), the MQTT spike (`docs/design/spikes/S2-mqtt.md`), and the YAML and filter spikes (`docs/design/spikes/ADR-012-yaml.md`, `docs/design/spikes/ADR-018-filter.md`) provide the supporting evidence. Implementation and verification status is tracked in `docs/design/readiness.md`.

## Summary

| ID | Topic | Recommendation | Decision |
|---|---|---|---|
| D-B2-1 | MVP v0.1 definition | v0.1 equals milestone M1 | Decided 2026-10-07: M1 |
| D-B2-2 | ROS 2 in v0.1 or M2 | M2 (MCAP file input first) | Decided 2026-10-07: M2, not in v0.1 |
| D-B2-3 | ClickHouse position | P1, starts after the M1 gate | Decided 2026-10-07: P1 after the M1 gate |
| D-B2-4 | HTTP, gRPC, aggregation | After the MVP | Decided 2026-10-07: after the MVP |
| D-B3-1 | `on_full` default | No default; explicit choice | Decided 2026-10-07: no default |
| D-B3-2 | Parquet layout | Wide, declared schemas | Decided 2026-10-07: wide |
| D-B3-3 | Quality granularity | Event-level for the MVP | Decided 2026-10-07: event-level |
| D-B3-4 | Producer batching assumption | Assume batching; document the ceiling | Decided 2026-10-07: batching NOT assumed; `on_commit` default; ceiling documented |
| D-B3-5 | Target storage media | SSD, NVMe, or industrial eMMC | Decided 2026-10-07: industrial SSD or eMMC; SD cards unsupported for durability claims |
| D-B6-1 | D5: pull source contract | Pull | Decided 2026-10-07: pull, cancel-safe `next_batch`, liveness clause |
| D-B6-2 | D6: split sink contracts | Split into BatchSink and SegmentSink | Decided 2026-10-07: split |
| D-B6-3 | D8: record representation | Shape-shared, with an opaque `Record` API | Decided 2026-10-07: representation B behind an opaque `Record` |
| D-B6-4 | D9: extended value types | Extended | Decided 2026-10-07: extended |
| D-B6-5 | D10: event granularity | One event per message plus `explode` | Decided 2026-10-07: one event per source message plus explicit explode |
| D-B7-1 | ADR-0012: YAML crate | `serde-saphyr`, `serde_norway` fallback | Decided 2026-10-07: `serde-saphyr` with `strict_booleans: true`; MSRV 1.89; `serde_norway` documented fallback |
| D-B7-2 | ADR-0018: filter language | Structured predicates for the MVP | Decided 2026-10-07: structured YAML predicates; CEL reserved for a future ADR |
| D-ADR | ADR acceptances | Accept 0001, 0002, 0003, 0006, 0011, 0013, 0020 | Decided 2026-10-07: accepted; 0004 stays Draft; 0007 and 0008 unchanged |
| D-EMQX | EMQX version for CI, tests, docs | Pin the open-source line | Decided 2026-10-07: 5.8.x (last open-source build); newer Enterprise-licensed images not used |

---

# B2: Scope freeze

Background: before Part A of `docs/design/product-amendments.md` was applied to `product.md`, section 46 required ClickHouse demos A and C, sections 14 and 40 used ClickHouse in examples, section 7 listed HTTP and gRPC as P0 sources, section 31 listed ROS 2 as an MVP source, and section 10 listed aggregation as P0. The agreed sink priority (section 13 and the sink-priority decision in section 31) and the milestone plan contradicted each of these. The amended wording for every affected section is in `docs/design/product-amendments.md`, Part A.

## D-B2-1: What is MVP v0.1?

- **Question:** Is the first release defined as milestone M1: MQTT to a durable capture log to Parquet to S3-compatible storage, with replay and chaos verification?
- **Options:**
  - (a) v0.1 equals M1. ROS 2 and ClickHouse come later.
  - (b) v0.1 equals M1 plus ROS 2 offline file ingestion (see D-B2-2).
  - (c) Keep the original `product.md` definition (two protocols, two data targets, ClickHouse demos A and C).
- **Recommendation:** (a), combined with the answer to D-B2-2.
- **Consequence:**
  - (a): the smallest scope that proves the product's defining promise (durable capture through outages) and keeps the M1 gate from being cut. The "two protocols" language in the product document is reworded, and the first public milestone is MQTT-first.
  - (b): adds ROS 2 message decoding and MCAP source work before the M1 gate.
  - (c): requires ClickHouse and live ROS 2 before the reliability gate, which the plans rank as the first things to defer.
- **Evidence and references:** `ARCHITECTURE.md` section 1; `product-amendments.md` A.1 to A.3.
- **Decision (2026-10-07):** (a). MVP v0.1 = MQTT -> capture WAL -> Parquet -> S3, with replay and chaos verification (milestone M1).

## D-B2-2: Is ROS 2 in v0.1 or in M2?

- **Question:** `product.md` section 31 originally listed ROS 2 as an MVP source while the architecture plans placed it in M2. Which is correct, and if ROS 2 is in v0.1, does that mean offline MCAP file ingestion only, or live ROS 2 as well?
- **Options:**
  - (A) ROS 2 is a P0 product capability delivered in M2. v0.1 has no ROS 2 source.
  - (B) MCAP and rosbag2 offline file ingestion is in v0.1; live transport is M2.
  - (C) Live ROS 2 transport is in v0.1.
- **Recommendation:** (A). If ROS 2 must be in the v0.1 scope, choose (B). Do not choose (C).
- **Consequence:**
  - (A): v0.1 does not require a robotics-specific source for the M1 reliability gate; ROS 2 lands in M2.
  - (B): requires a schema-driven CDR decoder with a message-definition parser (`thadam-ros2-msg`) and an MCAP source with `thadam export --mcap`, and adds the `mcap` dependency to v0.1. It proves ROS 2 to canonical event to Parquet with no live middleware risk.
  - (C): not supported for v0.1; live transport requires spike S3 (`docs/design/spikes/S3-ros2-live.md`) and depends on experimental Rust middleware bindings.
- **Evidence and references:** ADR-0007 (Draft); `product-amendments.md` A.10.
- **Decision (2026-10-07):** (A). ROS 2 is not in v0.1. It is M2, with MCAP and rosbag2 offline ingestion first.

## D-B2-3: ClickHouse position

- **Question:** Is ClickHouse a P1 sink that starts after the M1 gate, with `product.md` demos A and C re-targeted to Parquet, as the sink-priority decision states?
- **Options:**
  - (a) P1, outside the M1 reliability gate. Demos A and C re-targeted; the ClickHouse demos move to a P1 gate (M3).
  - (b) P0 in M1, alongside Parquet.
  - (c) Removed from the roadmap until target-user validation shows the need.
- **Recommendation:** (a).
- **Consequence:**
  - (a): users who need queryable local telemetry in M1 use Parquet plus DuckDB. The architecture keeps ClickHouse as an independent batch sink, so nothing in M1 depends on it. The sink-priority revisit trigger (pilots showing live queryable telemetry is the dominant workflow) stays.
  - (b): adds the ClickHouse sink and its effective-delivery-class probe to the M1 reliability gate.
  - (c): the architecture still supports adding it later; this only changes the roadmap.
- **Evidence and references:** `product.md` sections 13 and 31; ADR-0008 (Draft); `product-amendments.md` A.2, A.3, A.6, A.7.
- **Decision (2026-10-07):** (a). ClickHouse is P1 and sits outside the M1 reliability gate.

## D-B2-4: HTTP, gRPC, and aggregation

- **Question:** Are HTTP and gRPC sources (`product.md` section 7) and basic aggregation (section 10) placed after the MVP scope?
- **Options:**
  - (a) After the MVP: HTTP and gRPC become P1 sources, aggregation joins windowing in P1.
  - (b) Keep them P0 in the MVP reliability gate.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the M1 reliability gate stays narrow. HTTP remains in the core runtime as the admin API surface. Downsampling by decimation stays in P0; windowed aggregation needs watermark and late-data semantics that are outside M1.
  - (b): contradicts section 31's narrow MVP and adds protocol and windowing work to the M1 gate.
- **Evidence and references:** `product.md` sections 7, 10, 31, 32; `product-amendments.md` A.4, A.5, A.8.
- **Decision (2026-10-07):** (a). HTTP, gRPC, and aggregation are outside the M1 MVP scope, and the `product.md` section 46 demos are rewritten to the P0 path.

---

# B3: Product decisions that set defaults or user-facing behavior

Each of these decisions is recorded in the corresponding accepted ADR.

## D-B3-1: Default for `on_full`

- **Question:** When the local disk budget is exhausted, what does Thadam do by default: stop accepting data (`block`) or delete the oldest data (`drop_oldest`)?
- **Options:**
  - (a) No default. The operator must choose explicitly; `thadam init` writes `block` with an explanatory comment, and configuration validation fails if the key is missing.
  - (b) `block` is the default.
  - (c) `drop_oldest` is the default.
- **Recommendation:** (a).
- **Consequence:**
  - `block` does not prevent loss; it moves loss to the broker. The broker's persistent-session queue is bounded (Mosquitto defaults to 1000 queued messages [V]), so under `block` data can be lost in the broker without Thadam seeing it. Under `drop_oldest`, loss happens in Thadam's custody but is counted and shown as a gap. Neither is right for every deployment.
  - (a) requires an explicit operator choice; `thadam init` provides a commented `block` starter value.
  - Broker settings (`max_queued_messages`, `max_inflight_messages`, session expiry) are documented as part of deploymenteither way.
- **Evidence and references:** D4; demo scenario F7; ADR-0004, ADR-0011.
- **Decision (2026-10-07):** (a). No default. The configuration must choose `on_full` explicitly, and `thadam init` writes `block` with guidance.

## D-B3-2: Parquet layout

- **Question:** Is the first Parquet output wide (one column per declared field, declared schema per dataset) or narrow (rows of stream, timestamp, field, value), or both?
- **Options:**
  - (a) Wide, with declared schemas required for production datasets (inference only in development mode).
  - (b) Narrow (long) layout.
  - (c) Both layouts.
- **Recommendation:** (a). Reconsider narrow after target-user query-pattern evidence.
- **Consequence:**
  - (a): best compression and query ergonomics for analytics, and a clean mapping to Arrow, ClickHouse, and Iceberg. Users declare schemas up front. Schema changes beyond additive ones become a new dataset version.
  - (b): schemaless and simple to write, but poor query ergonomics and larger files.
  - (c): doubles the writer, test, and documentation surface.
- **Evidence and references:** D11 to D13; ADR-0013.
- **Decision (2026-10-07):** (a). Wide tables with a declared schema per dataset.

## D-B3-3: Quality granularity

- **Question:** Is event-level quality (status, flags, and reason per event) enough for the MVP, or is field-level quality required?
- **Options:**
  - (a) Event-level only. A message with several measurements can be expanded to one event per measurement with the `explode` step.
  - (b) Field-level quality in the MVP.
  - (c) Batch-level only.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the product document's per-measurement example (`temperature = 91.2`, `quality = OUT_OF_RANGE`) works for single-measurement messages and exploded events. A multi-measurement message has one quality status, so one bad field marks the whole event.
  - (b): needs a validity structure beside every record and extra columns or nested structures in sinks.
  - (c): too coarse for the quarantine and flagging behavior in `product.md` section 22.
- **Evidence and references:** D10; ADR-0001; `product-amendments.md` B.5.
- **Decision (2026-10-07):** (a). Event-level quality only for the MVP.

## D-B3-4: Producer batching assumption

- **Question:** Do the target users' producers send many samples per MQTT message, or one sample per message?
- **Options:**
  - (a) Assume realistic producers batch several samples per message, keep `ack_mode: on_commit` as the default, and document the throughput ceiling for unbatched producers.
  - (b) Assume one sample per message; plan for multiple connections (shared subscriptions) or an `on_receive` default.
  - (c) Gather payload samples from target users before deciding.
- **Recommendation:** (a), and do (c) in parallel.
- **Consequence:**
  - With `on_commit`, one connection is limited to roughly the broker's inflight window divided by commit latency. With the Mosquitto default of 20 inflight messages [V] and a 100 ms commit, that is about 200 messages per second; with EMQX's default of 32 [V], about 320. At that rate, unbatched high-frequency sensors cannot be captured on one connection with default broker limits.
  - Under (a), users are told the ceiling and can batch at the producer, raise broker limits, or opt into `on_receive` (at-most-once across a Thadam crash).
  - Under (b), the default and the MQTT design change: shared subscriptions lose cross-connection ordering, and `on_receive` weakens the delivery guarantee.
- **Evidence and references:** D7 and D23; ADR-0006; ADR-0011; spike S2 (`docs/design/spikes/S2-mqtt.md`).
- **Decision (2026-10-07):** Producer batching is not assumed (neither (a) nor (b) selected as an assumption). `ack_mode` defaults to `on_commit`, and the throughput ceiling (broker inflight window divided by commit latency) is documented as a limit rather than hidden. Collecting payload samples from target users (option (c)) remains open for future input.

## D-B3-5: Target storage media

- **Question:** Which storage media must the durability claims cover?
- **Options:**
  - (a) Linux on SSD, NVMe, or industrial eMMC that honors flush. Consumer SD cards are unsupported for durability claims and the documentation says why.
  - (b) Also support consumer SD cards, with warnings and likely `fsync: always`.
  - (c) NVMe and SSD only.
- **Recommendation:** (a).
- **Consequence:**
  - The `fsync` default and the flash-endurance statement depend on this. At 50 sensors at 1000 Hz with 200-byte samples (about 10 MB/s, or roughly 860 GB per day of WAL writes before Parquet writes), consumer SD cards wear out quickly.
  - Spike S1 (`docs/design/spikes/S1-wal.md`) recommends a provisional default of `interval(10ms)` instead of `interval(50ms)`, because 50 ms caps per-source throughput at the inflight window divided by about 50 ms. The default is re-measured on reference eMMC before it is frozen (ADR-0004).
  - (b) would require `fsync: always`, hurting throughput, and still cannot be verified without a hardware power-cut rig (500 cycles).
- **Evidence and references:** `product.md` section 26; ADR-0004, ADR-0011; spike S1.
- **Decision (2026-10-07):** (a). Industrial SSD or eMMC that honors flush is the supported baseline. SD cards are not a supported target for durability claims.

---

# B6: Decisions that shape the `thadam-core` public API

These five decisions shape the public API of `thadam-core`. They are recorded in ADR-0001 and ADR-0002 (both Accepted 2026-10-07). The record-representation microbenchmark (`docs/design/spikes/D8-record-representation.md`) backs D8 and D9, and `Record` is opaque (accessors only) so its internal layout can change later.

## D-B6-1: D5, a pull source contract

- **Question:** Does the runtime pull batches from sources (`next_batch`), or do sources push into an emitter?
- **Options:** (a) pull, with a liveness clause and an ordered ack tracker; (b) push emitter.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the runtime controls batching, fairness, metrics, and cancellation. Connectors run their own liveness task, and `next_batch` must be cancel-safe.
  - (b): suits callback libraries but encourages blocking the protocol loop, which breaks MQTT keepalive under backpressure.
- **Evidence and references:** D5; ADR-0002; spike S2 (`docs/design/spikes/S2-mqtt.md`). S2 confirmed the liveness clause empirically (withholding acks while polling kept the connection up for 3 times the keepalive with zero loss; stopping or blocking the poll loop caused a broker disconnect) and confirmed that `EventLoop::poll` is not cancel-safe, which is why the connector owns the poll loop and `next_batch` only receives from an internal queue.
- **Decision (2026-10-07):** (a). Pull-based sources. `next_batch` must be cancel-safe, and every source must satisfy the liveness clause (protocol keepalive is maintained regardless of how slowly the runtime pulls). Acknowledgement is ordered through a runtime-owned tracker.

## D-B6-2: D6, split sink contracts

- **Question:** One `Sink` trait, or separate `BatchSink` and `SegmentSink`?
- **Options:** (a) split; (b) single trait.
- **Recommendation:** (a).
- **Consequence:**
  - (a): each trait is unambiguous. Batch sinks (ClickHouse, Kafka, NATS, MQTT publish) retry the same batch identifier; segment sinks (Parquet) own open state and report a durable watermark. The shipper is a service over finalized segments, not a sink.
  - (b): "retry the same batch ID" is meaningless for Parquet, and watermarks are unnecessary for batch sinks.
- **Evidence and references:** D6; ADR-0002.
- **Decision (2026-10-07):** (a). Split sinks: `BatchSink` for transactional batch targets and `SegmentSink` for accumulating file targets. The shipper is a service, not a sink.

## D-B6-3: D8, record representation

- **Question:** How are decoded records represented in memory?
- **Options:**
  - (a) Shape-shared positional records: field names stored once per shape, values in a vector.
  - (b) Keyed dynamic records: field names per event (the original design).
  - (c) Arrow record batches throughout.
  - (d) Make `Record` opaque so the representation can change, and decide with the microbenchmark.
- **Recommendation:** (a), implemented behind an opaque `Record` API as in (d), confirmed by the microbenchmark.
- **Consequence:**
  - (a): much less memory and CPU per event than (b) and a direct path to Arrow because the shape maps to the schema. Adds a per-chain shape cache.
  - (b): at 50k events per second with 10 fields, 500k atomic reference-count operations per second, plus an allocation per event.
  - (c): requires a schema up front before row-wise transforms and couples core APIs to Arrow releases.
- **Evidence and references:** D8; ADR-0001; `docs/design/spikes/D8-record-representation.md` (11 to 13 ns per event for the transform pass against 44 to 126 ns for keyed records on W1 telemetry, 368 B against 488 to 712 B per event, and indistinguishable from shape-by-index single-threaded).
- **Decision (2026-10-07):** Representation B (shape-shared records, `Record { shape: Arc<Shape>, values: Vec<Value> }`) behind an opaque `Record` API, so that a later move to representation C (shape by index) changes only the name-based accessors and not callers. `shape: Arc<Shape>` and `values` are never public; steps use a `ShapeKey` and positional accessors on the hot path.

## D-B6-4: D9, extended value types

- **Question:** Does the value type set include narrow integers, `f32`, and typed primitive arrays, or only `i64`, `f64`, strings, and lists?
- **Options:** (a) extended; (b) minimal.
- **Recommendation:** (a).
- **Consequence:**
  - (a): robotics payloads (covariance matrices, joint vectors, `f32` sensor values) keep their size and their Parquet physical types.
  - (b): a generic list costs 24 to 32 bytes per element instead of 8 and loses the physical type.
- **Evidence and references:** D9; ADR-0001; `docs/design/spikes/D8-record-representation.md` section 6 (on the W2 joint-state workload the extended set cut memory from 1120 B to 400 B per event, clone was 4.7 to 5.7 times faster, and the transform pass about 2 times faster).
- **Decision (2026-10-07):** (a). The extended value set: narrow integers, `f32`, typed primitive arrays, and the other variants listed in ADR-0001.

## D-B6-5: D10, event granularity

- **Question:** Is one source message one event (with an explicit `explode` step to split arrays), or is every measurement its own event?
- **Options:** (a) one event per message plus `explode`; (b) one event per measurement.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the unit of acknowledgement, deduplication, and quality matches the unit the source delivered. A joint-state message with seven joints is one event unless the dataset explodes it.
  - (b): forces splitting before the pipeline knows the dataset's intent, and changes deduplication keys and Parquet layout.
- **Evidence and references:** D10; ADR-0001.
- **Decision (2026-10-07):** (a). One event per source message, with an explicit `explode` step to split array messages into per-sample events, consistent with D-B3-3 (event-level quality).

---

# B7: ADR-0012 (YAML crate) and ADR-0018 (filter language)

## D-B7-1: YAML crate and Rust floor (ADR-0012)

- **Question:** Which YAML crate does `thadam-config` use, and does the workspace accept the Rust 1.89 floor that the leading candidate requires?
- **Options:**
  - (a) `serde-saphyr` 1.x with `strict_booleans: true`, with `serde_norway` as the documented fallback. Needs Rust 1.89.
  - (b) `serde_norway` (or `serde_yaml_ng`), staying on Rust 1.88, with a path-to-span index, an explicit duplicate-key check, and a rule against merge keys.
- **Recommendation:** (a) (`docs/design/spikes/ADR-012-yaml.md`, section 7).
- **Consequence:**
  - (a): exact diagnostics, duplicate keys rejected everywhere, bounded parse cost, `#![forbid(unsafe_code)]`, with a workspace MSRV of Rust 1.89 and `serde_norway` as the fallback if needed.
  - (b): stays on 1.88 and uses a dormant fork.
- **Evidence and references:** ADR-0012; `docs/design/spikes/ADR-012-yaml.md`; ADR-0020.
- **Decision (2026-10-07):** (a). `serde-saphyr` with `strict_booleans: true`; the workspace MSRV is Rust 1.89 (`Cargo.toml`, ADR-0020, `ARCHITECTURE.md`, `version-inventory.md`); `serde_norway` is the documented fallback. ADR-0012 is Accepted.

## D-B7-2: MVP filter language (ADR-0018)

- **Question:** Do users write filter conditions as CEL expression strings or as structured YAML predicates?
- **Options:** (a) structured YAML predicates (`field`, operator, value, with `all`, `any`, `not`) for the MVP, each with an exact CEL equivalent, and CEL reserved for a future ADR; (b) CEL through the `cel` crate in the MVP.
- **Recommendation:** (a) (`docs/design/spikes/ADR-018-filter.md`, section 7).
- **Consequence:**
  - (a): schema-checked at `thadam validate`, allocation-free, bounded cost, no expression parser dependency; arithmetic, cross-field comparison, and string functions are post-MVP. Replaces the `expression: "quality == 'good'"` sketch in `product.md` section 14 via `product-amendments.md` A.11.
  - (b): an open language, but no static identifier checks, an `f32` widening hazard, no cost budget, two process-abort or panic modes found by the spike, and a 42-crate dependency tree.
- **Evidence and references:** ADR-0018; `docs/design/spikes/ADR-018-filter.md`; `product-amendments.md` A.11.
- **Decision (2026-10-07):** (a). Structured YAML predicates for the MVP; CEL reserved for a future ADR. ADR-0018 is Accepted, and `product-amendments.md` A.11 is approved.

---

# ADR acceptances (2026-10-07)

Per ADR-0000 only the maintainer accepts an ADR. On 2026-10-07 the maintainer recorded these ADR states:

| ADR | Decision | Result |
|---|---|---|
| ADR-0001 Canonical event model | Accept | **Accepted** |
| ADR-0002 Connector contract | Accept | **Accepted** |
| ADR-0003 Execution and process model (capture-first WAL, D1) | Accept | **Accepted** |
| ADR-0006 MQTT client (`rumqttc` pinned, wrapper rules, EMQX 5.8.x) | Accept | **Accepted** |
| ADR-0011 Delivery guarantees and claims register | Accept | **Accepted** |
| ADR-0012 Config format and YAML crate | Accept with `serde-saphyr` (D-B7-1) | **Accepted** |
| ADR-0013 Parquet segment layout | Accept | **Accepted** |
| ADR-0018 Filter and expression language | Accept with structured predicates (D-B7-2) | **Accepted** |
| ADR-0020 Dependency security and toolchain policy | Accept, with the MSRV floor raised to 1.89 | **Accepted** |
| ADR-0004 Durable buffer | Stays Draft: the WAL re-run on x86 NVMe and ARM eMMC is still required before the on-disk format is frozen | Draft |
| ADR-0007 ROS 2 strategy | Unchanged (M2) | Draft |
| ADR-0008 ClickHouse sink | Unchanged (P1) | Draft |

- Unticked evidence items in the accepted ADRs remain listed as implementation-time verification.
- ADR-0000 (the process itself) stays Proposed; the lifecycle it defines is followed.
- ADR-0015 (recording and replay format) and [ADR-0016](adr/0016-naming-and-publishing.md) (naming and publishing) were later written as Draft (2026-10-10 and 2026-10-14).

# EMQX version for CI, tests, and documentation (2026-10-07)

- **Question (ADR-0006, `ARCHITECTURE.md` O13):** which EMQX builds may Thadam test against in CI and recommend in documentation? S2 ran `emqx/emqx:5.8.6` (an Apache-2.0 build) and observed that `emqx/emqx:5.10.5` identifies itself as EMQX Enterprise with a non-commercial-use licence banner.
- **Decision (2026-10-07):** pin the last open-source build, the **5.8.x** line, for CI, tests, demos, and documentation. Newer Enterprise-licensed images are not used. Images are referenced by an exact 5.8.x tag (currently 5.8.6, the tested build). Recorded in ADR-0006 ("Broker versions for CI, tests, and documentation") and in `ARCHITECTURE.md` (sections 10, 21, and open item O13).
