# Owner decision register

Status: B2, B3, B6, and B7 are all decided by the owner (2026-10-07), and the owner accepted nine ADRs the same day (see "ADR acceptances" below). The D8 microbenchmark has reported (`docs/design/spikes/D8-record-representation.md`) and supports the D8 decision. The project is CONDITIONALLY READY: PR3, PR4, and PR5 can start once the docs baseline (PR2) and PR1 are committed. The remaining items are the S1 re-run on x86 NVMe and ARM eMMC (which gates only the WAL work, PR6 and PR7, through ADR-0004), and the owner applying the product-amendments Part A to `product.md`. See "What flips the project to READY" and `docs/design/readiness.md`.

Date: 2026-10-07. Source: the MVP readiness gate, section 2, blockers B2, B3, B6, and B7, and the owner's decisions on ADR acceptance.

## How to use this register

- Each entry has a question, the options, a recommendation, the consequences, and an `Owner decision:` line. All B2, B3, B6, and B7 entries are filled in (decided 2026-10-07), and the ADR acceptances and the EMQX pin are recorded in their own sections at the end of this register.
- To decide: write one of the options (or your own) on the `Owner decision:` line, with the date, and change the entry's status to "Decided by owner (date)". Coding agents and reviewers may recommend; they may not fill in the line.
- Decisions that change an ADR are applied by moving that ADR from Proposed or Draft to Accepted. Per ADR-0000, only the owner accepts an ADR.
- Decisions that change scope or product wording are applied to `product.md` through `docs/design/product-amendments.md`.

## Summary

| ID | Decision | Recommendation | Blocks |
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

The last column shows the decision status. Blocks for each entry are listed inside the entry.

---

# B2: Scope freeze

Background: product.md section 46 requires ClickHouse demos A and C, sections 14 and 40 use ClickHouse in examples, section 7 lists HTTP and gRPC as P0 sources, section 31 lists ROS 2 as an MVP source, and section 10 lists aggregation as P0. The agreed sink priority (section 13 and the sink-priority decision in section 31) and the milestone plan contradict each of these. The proposed wording for every affected section is in `docs/design/product-amendments.md`, Part A.

## D-B2-1: What is MVP v0.1?

- **Question:** Is the first release defined as milestone M1: MQTT to a durable capture log to Parquet to S3-compatible storage, with replay and chaos verification?
- **Options:**
  - (a) v0.1 equals M1. ROS 2 and ClickHouse come later.
  - (b) v0.1 equals M1 plus ROS 2 offline file ingestion (see D-B2-2).
  - (c) Keep the current product.md definition (two protocols, two data targets, ClickHouse demos A and C).
- **Recommendation:** (a), combined with the answer to D-B2-2.
- **Consequence:**
  - (a): the smallest scope that proves the product's defining promise (durable capture through outages) and keeps the M1 gate from being cut. The "two protocols" language in the product document must be reworded, and the first public message is MQTT-first.
  - (b): adds ROS 2 message decoding and an MCAP source work before the M1 gate.
  - (c): the plans do not support it. It requires ClickHouse and live ROS 2 before the reliability gate, which the plans rank as the first things to cut.
- **Evidence and references:** readiness gate sections 2 and 3; `ARCHITECTURE.md` section 1; `product-amendments.md` A.1 to A.3.
- **Blocks:** scope freeze, PR scopes, README claims, the flip to READY.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). MVP v0.1 = MQTT -> capture WAL -> Parquet -> S3, with replay and chaos verification (milestone M1). Decided 2026-10-07.

## D-B2-2: Is ROS 2 in v0.1 or in M2?

- **Question:** Product.md section 31 lists ROS 2 as an MVP source; the plans place it in M2. Which is correct, and if ROS 2 is in v0.1, does that mean offline MCAP file ingestion only, or live ROS 2 as well?
- **Options:**
  - (A) ROS 2 is a P0 product capability delivered in M2. v0.1 has no ROS 2 source.
  - (B) MCAP and rosbag2 offline file ingestion is in v0.1; live transport is M2.
  - (C) Live ROS 2 transport is in v0.1.
- **Recommendation:** (A). If the owner needs ROS 2 in the MVP claim, choose (B). Do not choose (C).
- **Consequence:**
  - (A): v0.1 does not demonstrate a robotics-specific source, so v0.1 messaging says "MQTT first". The M1 gate is unaffected.
  - (B): needs a schema-driven CDR decoder with a message-definition parser (M2-A in `ARCHITECTURE.md` section 21) and an MCAP source with `thadam export --mcap` (M2-B), and adds the `mcap` dependency to the MVP. The plans do not estimate the size. It proves ROS 2 to canonical event to Parquet with no live middleware risk.
  - (C): not supported. The live-transport spike (S3) has not run, the leading Rust options are experimental or low-to-medium maturity in the ecosystem review, and the chosen route depends on ROS distributions whose middleware tiers are not yet published for Lyrical [V]. It would invalidate the readiness estimate.
- **Evidence and references:** ecosystem maturity review sections 1 to 3 and 15; ADR-0007 (Draft); `product-amendments.md` A.10.
- **Blocks:** scope freeze, product.md Demo C, M2 planning.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (A). ROS 2 is NOT in v0.1. It is M2, with MCAP and rosbag2 offline ingestion first. Decided 2026-10-07.

## D-B2-3: ClickHouse position

- **Question:** Is ClickHouse a P1 sink that starts after the M1 gate, with product.md demos A and C re-targeted to Parquet, as the sink-priority decision already says?
- **Options:**
  - (a) P1 after the M1 gate (PR19 to PR21). Demos A and C re-targeted; the ClickHouse demos move to a P1 gate.
  - (b) P0 in M1, alongside Parquet.
  - (c) Removed from the MVP and the roadmap until target-user validation shows the need.
- **Recommendation:** (a).
- **Consequence:**
  - (a): users who need live queryable telemetry use Parquet plus DuckDB until M3. The architecture keeps ClickHouse as an independent sink, so nothing in M1 depends on it. The sink-priority revisit trigger (pilots show live queryable telemetry is the dominant workflow) stays.
  - (b): roughly three more pull requests before the M1 gate, and the effective-delivery-class probe adds validation work.
  - (c): the architecture still supports adding it later; this only changes the roadmap.
- **Evidence and references:** product.md sections 13 and 31; ADR-0008 (Draft; updated lean to the official `clickhouse` crate); demo plan section 15; `product-amendments.md` A.2, A.3, A.6, A.7.
- **Blocks:** PR19 to PR21, product.md sections 14, 40, and 46.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). ClickHouse is P1 and starts after the M1 reliability gate. Decided 2026-10-07.

## D-B2-4: HTTP, gRPC, and aggregation

- **Question:** Are HTTP and gRPC sources (product.md section 7, P0) and basic aggregation (section 10, P0) moved after the MVP?
- **Options:**
  - (a) After the MVP: HTTP and gRPC become P1 sources, aggregation joins windowing in P1.
  - (b) Keep them P0 as written.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the MVP stays narrow. HTTP remains in the product as an admin API surface, which is unaffected. Downsampling by decimation stays in the MVP; windowed aggregation needs watermark and late-data semantics that are deferred.
  - (b): contradicts section 31's narrow MVP and adds protocol and windowing work to the critical path with no milestone plan.
- **Evidence and references:** product.md sections 7, 10, 31, 32; `product-amendments.md` A.4, A.5, A.8.
- **Blocks:** product.md amendment.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). HTTP, gRPC, and aggregation are after the MVP. Approved together with the scope decision (D-B2-1), as it follows from it; this includes rewriting the product.md section 46 demos to the P0 path. Decided 2026-10-07.

---

# B3: Product decisions that set defaults or user-facing behavior

Each of these is recorded in the ADR named, which moves to Accepted when the owner signs.

## D-B3-1: Default for `on_full`

- **Question:** When the local disk budget is exhausted, what does Thadam do by default: stop accepting data (`block`) or delete the oldest data (`drop_oldest`)?
- **Options:**
  - (a) No default. The operator must choose explicitly; `thadam init` writes `block` with an explanatory comment, and configuration validation fails if the key is missing.
  - (b) `block` is the default.
  - (c) `drop_oldest` is the default.
- **Recommendation:** (a).
- **Consequence:**
  - `block` does not prevent loss; it moves loss to the broker. The broker's persistent-session queue is bounded (Mosquitto defaults to 1000 queued messages [V]), so under `block` data can be lost in the broker without Thadam seeing it. Under `drop_oldest`, loss happens in Thadam's custody but is counted and shown as a gap. Neither is right for every deployment.
  - (a) forces the operator to know this; the cost is a slightly less automatic first run, softened by `thadam init`.
  - Broker settings (`max_queued_messages`, `max_inflight_messages`, session expiry) become part of the deployment documentation either way.
- **Evidence and references:** architecture review D4 and section 4; demo scenario F7; ADR-0004, ADR-0011. User interviews and broker queue-behavior tests would strengthen the choice.
- **Blocks:** PR5 (validation rule), PR7 (`on_full` implementation).
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). No default. The configuration must choose `on_full` explicitly, and `thadam init` writes `block` with guidance. Decided 2026-10-07.

## D-B3-2: Parquet layout

- **Question:** Is the first Parquet output wide (one column per declared field, declared schema per dataset) or narrow (rows of stream, timestamp, field, value), or both?
- **Options:**
  - (a) Wide, with declared schemas required for production datasets (inference only in development mode).
  - (b) Narrow (long) layout.
  - (c) Both layouts.
- **Recommendation:** (a). Reconsider narrow after target-user query-pattern evidence.
- **Consequence:**
  - (a): the best compression and query ergonomics for analytics, and a clean mapping to Arrow, ClickHouse, and later Iceberg. Users must declare schemas up front (`thadam inspect sample --suggest-schema` is planned to ease this). Schema changes beyond additive ones become a new dataset version.
  - (b): schemaless and simple to write, but poor query ergonomics and larger files.
  - (c): doubles the writer, test, and documentation surface.
  - Reversibility is medium (a new dataset version) but the object-key layout becomes hard to change after release.
- **Evidence and references:** architecture review D11 to D13; ADR-0013. Evidence still wanted: target-user query patterns and a compression benchmark (B6).
- **Blocks:** PR13, PR14, and the demo schemas.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). Wide tables with a declared schema per dataset. Decided 2026-10-07.

## D-B3-3: Quality granularity

- **Question:** Is event-level quality (status, flags, and reason per event) enough for the MVP, or is field-level quality required?
- **Options:**
  - (a) Event-level only. A message with several measurements can be expanded to one event per measurement with the `explode` step.
  - (b) Field-level quality in the MVP.
  - (c) Batch-level only.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the product document's per-measurement example (`temperature = 91.2`, `quality = OUT_OF_RANGE`) works for single-measurement messages and exploded events. A multi-measurement message has one quality status, so one bad field marks the whole event.
  - (b): needs a validity structure beside every record and extra columns or nested structures in sinks. It touches the model, the encoder, and every sink.
  - (c): too coarse for the quarantine and flagging behavior in section 22.
  - The product amendment in `product-amendments.md` B.5 depends on this answer.
- **Evidence and references:** architecture review D10 and the open question on quality; ADR-0001.
- **Blocks:** PR3, PR13, B.5 amendment.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). Event-level quality only for the MVP. Decided 2026-10-07.

## D-B3-4: Producer batching assumption

- **Question:** Do the target users' producers send many samples per MQTT message, or one sample per message?
- **Options:**
  - (a) Assume realistic producers batch several samples per message, keep `ack_mode: on_commit` as the default, and document the throughput ceiling for unbatched producers.
  - (b) Assume one sample per message; plan for multiple connections (shared subscriptions) or an `on_receive` default.
  - (c) Gather payload samples from target users before deciding.
- **Recommendation:** (a), and do (c) in parallel.
- **Consequence:**
  - With `on_commit`, one connection is limited to roughly the broker's inflight window divided by commit latency. With the Mosquitto default of 20 inflight messages [V] and a 100 ms commit, that is about 200 messages per second; with EMQX's default of 32 [V], about 320. At that rate, unbatched high-frequency sensors cannot be captured on one connection.
  - Under (a), those users are told the ceiling and must batch at the producer, raise broker limits, or accept `on_receive` (at-most-once across a Thadam crash).
  - Under (b), the default and the MQTT design change: shared subscriptions lose cross-connection ordering, and `on_receive` weakens the delivery guarantee.
  - This sets the default `ack_mode` and every throughput claim.
- **Evidence and references:** architecture review D7 and D23, sections 4 and 6; ADR-0006 and spike S2 (ack-window curve); demo scenario M2 with `unbatched: true`.
- **Blocks:** ADR-0006, throughput wording in ADR-0011.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** The owner did not select a batching assumption (neither (a) nor (b)). Recorded outcome: producer batching is NOT assumed. `ack_mode` defaults to `on_commit`, and the throughput ceiling (broker inflight window divided by commit latency) is documented as a limit, not hidden. Collecting payload samples from target users (option (c)) remains useful but is not part of this decision. Decided 2026-10-07.

## D-B3-5: Target storage media

- **Question:** Which storage media must the durability claims cover?
- **Options:**
  - (a) Linux on SSD, NVMe, or industrial eMMC that honors flush. Consumer SD cards are unsupported for durability claims and the documentation says why.
  - (b) Also support consumer SD cards, with warnings and likely `fsync: always`.
  - (c) NVMe and SSD only.
- **Recommendation:** (a).
- **Consequence:**
  - The `fsync` default (interval 50 ms with acknowledgement after fsync) and the flash-endurance statement depend on this. The review's example, 50 sensors at 1000 Hz with 200-byte samples, is about 10 MB/s or roughly 860 GB per day of WAL writes before Parquet writes. Consumer SD cards wear out quickly at that rate.
  - Update after spike S1 (2026-10-07): S1 recommends a provisional default of `interval(10ms)` instead of `interval(50ms)`, because 50 ms caps per-source throughput at the inflight window divided by about 50 ms. The default is re-measured on reference eMMC before it is frozen (ADR-0004). The media decision itself is unchanged.
  - (b) would likely require `fsync: always`, hurting throughput, and still cannot be verified without a hardware power-cut rig (a relay-switched board and 500 cycles is the plan's proposal).
  - Reference ARM hardware (for example a Raspberry Pi 5 with NVMe, or a Jetson) is assumed available for the spikes and benchmarks **[A]**.
- **Evidence and references:** architecture review section 5 and open questions; product.md section 26 ("local SSD"); ADR-0004, ADR-0011; spike S1.
- **Blocks:** fsync default, ADR-0004 acceptance, power-loss claim wording.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). Industrial SSD or eMMC that honors flush is the supported baseline. SD cards are NOT a supported target for durability claims. Decided 2026-10-07.

---

# B6: Sign-off on decisions that shape the `thadam-core` public API

These five decisions shape the public API of `thadam-core`, which every other crate depends on. They blocked PR3 (model) and PR4 (traits and test kit) until signed. The owner signed all five on 2026-10-07. They are recorded in ADR-0001 and ADR-0002, which the owner accepted on 2026-10-07 (see "ADR acceptances"). The record-representation microbenchmark (`docs/design/spikes/D8-record-representation.md`) backs D8 and D9, and `Record` is opaque (accessors only) so D8 can change later.

## D-B6-1: D5, a pull source contract

- **Question:** Does the runtime pull batches from sources (`next_batch`), or do sources push into an emitter?
- **Options:** (a) pull, with a liveness clause and an ordered ack tracker; (b) push emitter.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the runtime controls batching, fairness, metrics, and cancellation. Connectors need slightly more boilerplate and must run their own liveness task. `next_batch` must be cancel-safe. The three candidate client libraries (`rumqttc`, Zenoh, `r2r`) already expose pull or stream APIs.
  - (b): suits callback libraries but encourages blocking the protocol loop, which breaks MQTT keepalive under backpressure.
  - Reversibility is medium (an internal, unstable API before 1.0).
- **Evidence and references:** architecture review D5 and section 2; ADR-0002; spike S2 (`docs/design/spikes/S2-mqtt.md`). S2 confirmed the liveness clause empirically (withholding acks while polling kept the connection up for 3 times the keepalive with zero loss; stopping or blocking the poll loop caused a broker disconnect) and that `EventLoop::poll` is not cancel-safe, which is why the connector, not the runtime, owns the poll loop and `next_batch` only receives from an internal queue. Prototype ergonomics with Zenoh are still to be checked.
- **Blocks:** PR3, PR4, PR10, PR12 (unblocked by this decision).
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). Pull-based sources. `next_batch` must be cancel-safe, and every source must satisfy the liveness clause (protocol keepalive is maintained regardless of how slowly the runtime pulls). Acknowledgement is ordered through a runtime-owned tracker. Decided 2026-10-07.

## D-B6-2: D6, split sink contracts

- **Question:** One `Sink` trait, or separate `BatchSink` and `SegmentSink`?
- **Options:** (a) split; (b) single trait.
- **Recommendation:** (a).
- **Consequence:**
  - (a): each trait is unambiguous. Batch sinks (ClickHouse) retry the same batch identifier; segment sinks (Parquet) own open state and report a durable watermark. The shipper is a service over finalized segments, not a sink. The cost is more API surface.
  - (b): "retry the same batch ID" is meaningless for Parquet, and watermarks are unnecessary for ClickHouse.
  - Confidence is medium-high; reversibility medium.
- **Evidence and references:** architecture review D6; ADR-0002. Parquet and ClickHouse prototypes against both shapes remain to be done and are tracked as evidence items in ADR-0002.
- **Blocks:** PR3, PR4, PR10 (unblocked by this decision).
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). Split sinks: `BatchSink` for transactional batch targets and `SegmentSink` for accumulating file targets. The shipper is a service, not a sink. Decided 2026-10-07.

## D-B6-3: D8, record representation

- **Question:** How are decoded records represented in memory?
- **Options:**
  - (a) Shape-shared positional records: field names stored once per shape, values in a vector.
  - (b) Keyed dynamic records: field names per event (the original design).
  - (c) Arrow record batches throughout.
  - (d) Hedge: make `Record` opaque so the representation can change, and decide with the microbenchmark.
- **Recommendation:** (a), implemented behind an opaque `Record` API as in (d), confirmed by the microbenchmark before PR3 merges.
- **Consequence:**
  - (a): much less memory and CPU per event than (b) and a trivial path to Arrow, because the shape becomes the schema. Adds a shape cache to manage.
  - (b): at 50k events per second with 10 fields, 500k atomic reference-count operations per second, plus an allocation per event.
  - (c): requires a schema up front, but MQTT and ROS streams are heterogeneous. Row-wise transforms become awkward and Arrow's frequent breaking releases would spread into the core.
  - Under the capture-first design the representation is in memory only, so reversibility is medium even after release.
- **Evidence and references:** architecture review D8 and section 1; ADR-0001; microbenchmark report `docs/design/spikes/D8-record-representation.md` (recommends B, confidence medium-high, with an opaque `Record`). In that report B is 11 to 13 ns per event for the transform pass against 44 to 126 ns for keyed records on W1 telemetry, 368 B against 488 to 712 B per event, and indistinguishable from option C single-threaded. The microbenchmark ran on one Apple Silicon (arm64) laptop and needs a re-run on reference ARM and x86 hardware before any published claim; that does not block the decision.
- **Blocks:** PR3, PR4 (unblocked by this decision).
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** Representation B (shape-shared records, `Record { shape: Arc<Shape>, values: Vec<Value> }`) behind an opaque `Record` API, so that a later move to representation C (shape by index) changes only the name-based accessors and not callers. `shape: Arc<Shape>` and `values` are never public; steps use a `ShapeKey` and positional accessors on the hot path. Decided 2026-10-07.

## D-B6-4: D9, extended value types

- **Question:** Does the value type set include narrow integers, `f32`, and typed primitive arrays, or only `i64`, `f64`, strings, and lists?
- **Options:** (a) extended; (b) minimal.
- **Recommendation:** (a).
- **Consequence:**
  - (a): robotics payloads (covariance matrices, joint vectors, `f32` sensor values) keep their size and their Parquet physical types. Slightly more variants to support in every sink.
  - (b): a generic list costs 24 to 32 bytes per element instead of 8, losing the physical type; the review estimates 3 to 4 times growth in memory for such payloads.
  - Confidence high; reversibility medium.
- **Evidence and references:** architecture review D9; ADR-0001; `docs/design/spikes/D8-record-representation.md` section 6. On the W2 joint-state workload the extended set cut memory from 1120 B to 400 B per event (64%), clone was 4.7 to 5.7 times faster, and the transform pass about 2 times faster. Narrow scalars and `f32` showed no memory or CPU effect and are justified by physical-type fidelity.
- **Blocks:** PR3, PR13 (unblocked by this decision).
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). The extended value set: narrow integers, `f32`, typed primitive arrays, and the other variants listed in ADR-0001. Decided 2026-10-07. The report's follow-up (evaluating a 24-byte `Value` layout in PR3) is an implementation measurement tracked in ADR-0001, not a change to this decision.

## D-B6-5: D10, event granularity

- **Question:** Is one source message one event (with an explicit `explode` step to split arrays), or is every measurement its own event?
- **Options:** (a) one event per message plus `explode`; (b) one event per measurement.
- **Recommendation:** (a).
- **Consequence:**
  - (a): the unit of acknowledgement, deduplication, and quality matches the unit the source delivered. A joint-state message with seven joints is one event unless the user explodes it. Users who want per-sample rows write an `explode` step.
  - (b): forces a decision about splitting before the pipeline knows the user's intent, and changes deduplication keys and Parquet layout.
  - This choice interacts with D-B3-3 (quality granularity).
- **Evidence and references:** architecture review D10; ADR-0001. User payload samples would validate it; that review informs a later revisit and no longer gates the decision.
- **Blocks:** PR3, PR9 (unblocked by this decision).
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). One event per source message, with an explicit `explode` step to split array messages into per-sample events. Decided 2026-10-07. This is consistent with D-B3-3 (event-level quality).

---

# B7: ADR-0012 (YAML crate) and ADR-0018 (filter language)

## D-B7-1: YAML crate and Rust floor (ADR-0012)

- **Question:** Which YAML crate does `thadam-config` use, and does the workspace accept the Rust 1.89 floor that the leading candidate requires?
- **Options:**
  - (a) `serde-saphyr` 1.x with `strict_booleans: true`, with `serde_norway` as the documented fallback. Needs Rust 1.89.
  - (b) `serde_norway` (or `serde_yaml_ng`), staying on Rust 1.88, with a path-to-span index, an explicit duplicate-key check, and a rule against merge keys.
- **Recommendation:** (a), confidence medium (`docs/design/spikes/ADR-012-yaml.md`, section 7).
- **Consequence:**
  - (a): exact diagnostics, duplicate keys rejected everywhere, bounded parse cost, `#![forbid(unsafe_code)]`. The cost is a 1.89 floor now rather than when the P1 crates land, 10 more transitive crates, and a young, essentially single-maintainer crate; `serde_norway` is the fallback if it stalls.
  - (b): stays on 1.88 and uses a more settled but dormant fork.
- **Evidence and references:** ADR-0012; `docs/design/spikes/ADR-012-yaml.md`; ADR-0020.
- **Blocks:** PR5.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). `serde-saphyr` with `strict_booleans: true`; the workspace MSRV is raised to Rust 1.89 (root `Cargo.toml`, ADR-0020, `ARCHITECTURE.md`, `version-inventory.md`); `serde_norway` is the documented fallback. ADR-0012 is Accepted. The detail items the spike left open (key spelling, hash scope, interpolation edge cases, parse budget numbers, anchors and merge keys, path-to-span index, transform-to-sink binding) are PR5 implementation details listed in ADR-0012. Decided 2026-10-07.

## D-B7-2: MVP filter language (ADR-0018)

- **Question:** Do users write filter conditions as CEL expression strings or as structured YAML predicates?
- **Options:** (a) structured YAML predicates (`field`, operator, value, with `all`, `any`, `not`) for the MVP, each with an exact CEL equivalent, and CEL reserved for a future ADR; (b) CEL through the `cel` crate in the MVP.
- **Recommendation:** (a), confidence medium (`docs/design/spikes/ADR-018-filter.md`, section 7).
- **Consequence:**
  - (a): schema-checked at `thadam validate`, allocation-free, bounded cost, no new dependency and no expression parser to fuzz; arithmetic, cross-field comparison, string functions, and window rules are post-MVP. Diverges from product.md section 14 (`expression: "quality == 'good'"`), which is replaced through `product-amendments.md` A.11.
  - (b): an open language, but no static identifier checks, an `f32` widening hazard, no cost budget, two process-abort or panic modes found by the spike, and a fast-moving 42-crate dependency tree.
- **Evidence and references:** ADR-0018; `docs/design/spikes/ADR-018-filter.md`; `product-amendments.md` A.11.
- **Blocks:** PR9 and any published demo YAML.
- **Status:** Decided by owner (2026-10-07)
- **Owner decision:** (a). Structured YAML predicates for the MVP; CEL reserved for a future ADR. ADR-0018 is Accepted. The residual items (filter key spelling, namespace, nested access, literal typing, null semantics, cost limits, ergonomics review) are PR9 implementation details listed in ADR-0018. `product-amendments.md` A.11 replaces the `expression: "quality == 'good'"` example in product.md section 14 and is Approved 2026-10-07. Decided 2026-10-07.

---

# ADR acceptances (2026-10-07)

Per ADR-0000 only the project owner accepts an ADR. On 2026-10-07 the owner made these decisions, and each ADR file records "Accepted by project owner on 2026-10-07".

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

- Unticked evidence items in the accepted ADRs remain listed as implementation-time verification. They are not blockers and do not change an accepted decision.
- ADR-0000 (the process itself) is not in the owner's list and stays Proposed. The lifecycle it defines is already followed.
- ADR-0004 can leave Draft only after the S1 re-run and the other items in its "Remaining before this ADR can leave Draft". Until then PR6 and PR7 stay gated.

# EMQX version for CI, tests, and documentation (2026-10-07)

- **Question (ADR-0006, ARCHITECTURE.md O13):** which EMQX builds may Thadam test against in CI and recommend in documentation? S2 ran `emqx/emqx:5.8.6` (an Apache-2.0 build) and observed that `emqx/emqx:5.10.5` identifies itself as EMQX Enterprise with a non-commercial-use licence banner.
- **Owner decision:** pin the last open-source build, the **5.8.x** line, for CI, tests, demos, and documentation. Newer Enterprise-licensed images are not used. Images are referenced by an exact 5.8.x tag (currently 5.8.6, the tested build). Recorded in ADR-0006 ("Broker versions for CI, tests, and documentation") and in `ARCHITECTURE.md` (sections 10, 21, and open item O13). Decided 2026-10-07.
- **Note:** the repository has no demo, example, or CI job that references EMQX yet; PR12 and PR15 apply the pin when they add one.

---

# Related decisions tracked elsewhere

These are not owner sign-offs in this register but must be done before READY.

- **B1: docs baseline.** The ADRs in `docs/design/adr/` are accepted as listed in "ADR acceptances" above, except ADR-0004, which stays Draft until the S1 re-run on reference hardware (eMMC and x86 NVMe) because it freezes the on-disk format. What remains is committing the docs baseline (PR2) so the acceptances are recorded in the repository.
- **B4: Spike S1 (WAL)** and **B5: Spike S2 (MQTT).** Both reports are in `docs/design/spikes/S1-wal.md` and `S2-mqtt.md`. S1 ran without reference hardware (see ADR-0004). S2 recommends a short follow-up, S2b, against the `rumqttc-next` fork, which is implementation-time verification and not a blocker.
- **B7: ADR-0012 (YAML crate) before PR5, ADR-0018 (filter language) before PR9** and before any demo configuration is published. Both decided and Accepted 2026-10-07 (D-B7-1, D-B7-2); their residual details are PR5 and PR9 implementation details.
- **B8: plan synchronization.** Done in `ARCHITECTURE.md` (sections 19 to 21).

# What flips the project to READY

- The docs baseline (PR2) and PR1 are committed to the repository, with ADRs 0001, 0002, 0003, 0006, 0011, 0012, 0013, 0018, and 0020 accepted (done 2026-10-07; the commit is pending). This unblocks PR3, PR4, and PR5.
- B2, B3, B6, and B7 are signed in this register (all done 2026-10-07).
- The S1 and S2 reports are in (done), with ADR-0006 accepted (done). ADR-0004 stays Draft until the S1 re-run on reference hardware; this gates only the WAL work (PR6 and PR7).
- B8 is folded in (this is done in `ARCHITECTURE.md`).
- The owner applies the approved Part A amendments (A.1 to A.11) to `product.md`.

The current verdict and the exact remaining blockers are in `docs/design/readiness.md`.

Estimated elapsed time from the readiness gate: 1.5 to 2 weeks, with the spikes running in parallel with the documentation work.
