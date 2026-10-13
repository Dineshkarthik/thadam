# ADR-0008: ClickHouse sink

- **Status:** Draft
- **Date:** 2026-10-07
- **Deciders:** Project owner

## Context

ClickHouse is a column-oriented database used for low-latency analytics. It is a P1 sink for Thadam: product.md section 13 calls it a first-class early integration for users who need queryable live telemetry, and also says it is not a mandatory dependency and must not define Thadam's core architecture. Section 31 makes it "conditional on MVP capacity and target-user validation".

**This ADR is deferred until the M1 gate.** ARCHITECTURE.md section 21 places the work in M3 (PR19 benchmark B8, PR20 `thadam-clickhouse`, PR21 example), and PR19 is the benchmark that decides this ADR. If capacity tightens, the whole block moves beyond the MVP and nothing in M1 depends on it. Nothing here may be built before the M1 gate passes.

**Product conflict, resolved by the owner (B2, D-B2-3, 2026-10-07).** product.md sections 14, 40, and 46 originally used ClickHouse in Demo A (MQTT to ClickHouse) and Demo C (ROS 2 to ClickHouse), and section 46 defined MVP done to include them. The owner decided that ClickHouse is P1 and starts after the M1 reliability gate, and approved the amendments that rewrite those sections to the P0 path (`docs/design/product-amendments.md`, Part A; applied to product.md on 2026-10-13).

### Scope

This ADR covers the wire format and protocol choice, the effective delivery class (found by a startup probe), deduplication tokens, schema mapping, the no-DDL rule, error classification, and poison-row bisection.

### Fixed by other ADRs and ARCHITECTURE.md

- The sink is a `BatchSink` (ADR-0002, D6) with its **own chain and its own WAL cursor** (ADR-0003). It does not share a cursor with the Parquet path, so an outage at ClickHouse never blocks Parquet or S3 (sink isolation).
- `thadam-clickhouse` is a library crate that joins the workspace at M3 and is built only behind the `clickhouse` feature of `apps/thadam-cli`. `thadam-core`, `thadam-runtime`, and the default build never depend on it. Arrow may appear in it, only as sub-crates (ARCHITECTURE.md section 9).
- Dependency rule: `thadam-clickhouse` depends on `thadam-core` and `thadam-arrow` only among workspace crates.
- The runtime retries the same `BatchId` on every attempt. A dropped HTTP request has an **unknown outcome** and is retried with the same batch ID (ARCHITECTURE.md section 10).
- `max_inflight_batches` is 1 in the MVP (ARCHITECTURE.md section 10, bounded queues).
- Batch sinks report an effective delivery class (`AtLeastOnce` or `IdempotentBatch`) from `open()` (ARCHITECTURE.md section 5).

## Decision

Per ADR-0000, this ADR is Draft: no recommendation below is binding. The text states the **current lean, which is provisional**.

### Client and format: updated lean

Use the **official `clickhouse` crate** (ClickHouse Inc.), 0.15.2 (2026-08), MIT or Apache-2.0, MSRV 1.89, about 5M recent downloads. The preferred ingestion paths are `Client::insert_native()` (`FORMAT Native` blocks, from the crate's changelog) and the new `clickhouse-ext-arrow` crate (0.1.0, 2026-06, low maturity; type coverage is **[V]**).

This **updates** the earlier lean of raw HTTP through `reqwest` with `FORMAT ArrowStream` (original plan, section H; implementation plan, section 5.10). The reason is the maturity review (section 15, item 2): the official crate now appears to cover Thadam's dynamic-schema need without hand-written HTTP code, and it is vendor-maintained.

**Parity rule.** Benchmark B8 compares both paths. At performance parity, prefer the vendor-maintained crate (product.md principle 11, "do not reinvent mature infrastructure"). Raw HTTP with `ArrowStream` is chosen only if it wins materially. [OPEN: criterion for "parity" and for "wins materially" to be set before B8 runs.] product.md section 13 separately requires that HTTP and native be benchmarked and that native not be assumed best.

Candidates dropped from primary consideration: `klickhouse` 0.15.3 (native TCP) and `clickhouse-arrow` 0.2.1 (native protocol with Arrow; last release December 2025, 48 stars), both essentially single-maintainer. `clickhouse-rs` (suharev7) is rejected (stale alpha releases).

Caveats recorded from the review: the crate is 0.x with breaking minors, and the 0.14 to 0.15 change switched the default format. MSRV rises to 1.89 for the whole workspace when this crate lands (ARCHITECTURE.md section 16). Whether the official crate's Native path runs over the same HTTP transport as its RowBinary path, and whether it needs compile-time typed rows for dynamic Thadam schemas, are **[V]**.

### Delivery class

- **At-least-once by default.**
- **Effectively-once only when ClickHouse can deduplicate:** for `Replicated*MergeTree`, or for non-replicated `MergeTree` with `non_replicated_deduplication_window` set (default 0, which disables it). Asynchronous inserts additionally need `async_insert_deduplicate` **[V]**.
- **Startup probe.** `open()` probes the server version, `DESCRIBE TABLE`, the table engine, and the deduplication settings, then reports the effective `DeliveryClass` (`IdempotentBatch` or `AtLeastOnce`) in `/status` and in `thadam_sink_delivery_class{class}`. Thadam does not assume effectively-once. A `DESCRIBE TABLE` mismatch with the dataset schema is a `Config` error at `open()`.
- **Dedup token.** `insert_deduplication_token` is the `BatchId`, formatted `{epoch}-{sink}-{first}-{last}`. The in-flight range is persisted in the cursor before sending so a retry after a crash reuses the same token.
- **Fallback for eventual deduplication:** `ReplacingMergeTree(event_id)`. Authoritative deduplication is downstream (D17). Row-level deduplication across broker redelivery needs a content-derived event ID (ARCHITECTURE.md section 4).

### No DDL

Thadam **never runs `CREATE TABLE` or `ALTER TABLE`**. Creating tables is the user's job. The demo ships `clickhouse/init.sql`, run once by an init container. Schema mapping is an explicit column mapping; type mismatches go to quarantine, with no automatic `ALTER` in the MVP.

### Errors and poison rows

Classification is by HTTP status plus the ClickHouse exception code **[V: codes to be listed against the pinned server versions]**:

| Condition | Class |
|---|---|
| Parse and type errors | Permanent, then bisect |
| Memory, too-many-parts, too-many-queries | Transient, long backoff |
| Authentication | Fatal |
| Timeouts and dropped connections | Unknown outcome; retry with the same token |

After `max_attempts_before_bisect`, the batch is bisected to isolate and quarantine poison events. Bisection **changes batch identity**; this is recorded in lineage (ARCHITECTURE.md section 13). How bisected sub-batches derive their dedup tokens is **[OPEN]**: the token must stay deterministic across crash and retry.

### Configuration surface (provisional; implementation plan section 5.10)

`url`, `database`, `table`, `user`, `password_file`, `format`, `compression` (lz4, zstd, or none via HTTP `Content-Encoding`), `insert_settings`, `max_rows`, `max_bytes`, `max_age`, `request_timeout`, `dedup: auto|token|off`, `columns` mapping. The `format` key and the demo's `format: arrow_stream` value are provisional until this ADR decides the client. Credentials are wrapped in `Secret<T>` (ARCHITECTURE.md section 13).

### Per-sink chain

Each sink has its own chain, so a ClickHouse chain may downsample while Parquet keeps full rate. The P1 demo downsamples IMU to 50 Hz for ClickHouse only. How a transform binds to one sink is open item O2 (proposed selector `transforms[].sinks: [ids]`), owned by ADR-0012.

## Alternatives considered

- **Official crate with `insert_native()` (`FORMAT Native`).** Current lean. Vendor-maintained, no hand-written protocol code.
- **Official crate with `clickhouse-ext-arrow`.** Reuses Arrow batches from `thadam-arrow`, which fits the encoder already needed for Parquet. Cons: version 0.1.0, low maturity, type coverage unknown, and it must align with the workspace's pinned `arrow` major **[V]**.
- **Raw HTTP through `reqwest` with `FORMAT ArrowStream`.** Previous lean. Reuses `thadam-arrow` and the `arrow-ipc` sub-crate. Cons: Thadam owns HTTP handling, settings, and error parsing. Chosen only if B8 shows it wins materially.
- **`RowBinary` or `RowBinaryWithNamesAndTypes` through the official crate.** The crate's default path, with schema validation. Cons: row-oriented encoding, and it was designed around compile-time typed rows, while Thadam schemas are dynamic **[V]**.
- **`klickhouse` (native TCP).** Dropped from primary consideration: single maintainer.
- **`clickhouse-arrow`.** Dropped from primary consideration: single maintainer, last release December 2025.
- **`clickhouse-rs` (suharev7).** Rejected: stale alpha releases.
- **Kafka with the ClickHouse Kafka table engine.** Would move delivery and deduplication to a broker that Thadam does not ship. Kafka connectors are an MVP non-goal (ARCHITECTURE.md section 1), and the architecture needs ClickHouse to remain an independent sink.
- **Thadam manages table DDL.** Rejected: widens the permission Thadam needs, risks destructive changes, and conflicts with the explicit-mapping rule. A user-owned init script is the documented path.

## Consequences

- The MVP can ship without ClickHouse. Its absence does not weaken durability, recovery, or replay, because it has its own cursor and chain.
- The delivery claim is only as strong as the probe. A table without a deduplication window reports `AtLeastOnce`, and users see that in `/status`.
- Persisting the in-flight range in the cursor (which already has an optional in-flight range field, ARCHITECTURE.md section 6) before sending makes retries after a crash reuse the same token.
- Users own the DDL and must keep it in step with dataset schemas. A mismatch fails at `open()`.
- The workspace MSRV rises to 1.89 when the crate lands, and the crate's 0.x churn becomes an upgrade cost.
- The official crate's TLS stack must be buildable with the rustls and `ring` baseline, without `aws-lc-sys` or `openssl-sys` **[V]**.
- Earlier plans are updated as follows. The implementation plan's "HTTP, format by ADR-008" sink and its `reqwest` plus `arrow-ipc` dependency list are superseded by this ADR's candidate list (ARCHITECTURE.md section 19). The original plan's B8 list (RowBinary, ArrowStream, Native) now also includes the official crate's paths. The original plan's crate name `thadam-sink-clickhouse` and PR21 to PR23 numbering are replaced by `thadam-clickhouse` and PR19 to PR21. The original plan's "DLQ for incompatible fields" is the quarantine of the current design.

## Evidence and validation required

All of this runs after the M1 gate. The ADR stays Draft until it is attached.

- [ ] **B8 candidate benchmark** on the same ClickHouse server and workload (W1 and W2 shapes from the benchmark plan): rows per second and MB per second for the official crate with Native, the official crate with `clickhouse-ext-arrow`, raw HTTP `ArrowStream`, and official-crate RowBinary. [OPEN: parity and "wins materially" thresholds to be set before B8 runs.]
- [ ] **Batch-size sweep** and **`async_insert` on and off** for each candidate.
- [ ] **Dedup-token overhead:** throughput with and without `insert_deduplication_token`.
- [ ] **Recovery after server downtime:** time to drain the backlog. [OPEN: criterion to be set before the evaluation runs]
- [ ] **Ingest-to-queryable p95** latency. [OPEN: criterion to be set before the evaluation runs]
- [ ] **Parity-rule decision recorded** in this ADR with the B8 numbers attached.
- [ ] **`clickhouse-ext-arrow` type coverage** against Thadam's types: `f32`, narrow integers, typed arrays, timestamps, and the unsigned-64-bit policy for values above `i64::MAX` **[V]**.
- [ ] **Official-crate fit:** dynamic schemas without compile-time row types, the transport used by Native inserts, and the Arrow major version it depends on **[V]**.
- [ ] **Dedup probe** against tables with and without the deduplication window, on replicated and non-replicated engines, with and without async inserts. The reported `DeliveryClass` must match observed duplicate behavior.
- [ ] **Poison-row bisect test** with a type-mismatch row, including determinism of tokens for bisected sub-batches.
- [ ] **Downtime tests A1 to A3** from the demo plan (section 15): ClickHouse down for 10 minutes with Parquet and S3 unaffected; `kill -9` during an insert, with zero duplicates when the window is set and the documented duplicates when it is not; a poison row quarantined while the batch continues.
- [ ] **Error-code table** for the classification above, verified against the ClickHouse versions tested.
- [ ] **ClickHouse LTS and current-stable containers** both pass the batch-sink conformance suite.
- [ ] **Build checks:** MSRV 1.89, rustls with `ring` and no `aws-lc-sys` or `openssl-sys`, `cargo deny` clean, `default-features = false` with explicit features.
- [ ] **Target-user validation** that live queryable telemetry is needed. [OPEN]
- [x] **Owner decision B2** recorded for ClickHouse scope: P1, after the M1 reliability gate (2026-10-07).

## Revisit when

- The M1 gate passes (this ADR then becomes active work).
- The official crate's Arrow extension matures beyond 0.1.0, or the crate changes its default format or API again.
- User pilots show queryable live telemetry is the dominant workflow (the product.md sink-priority revisit trigger).
- A single-maintainer candidate gains a second maintainer or the official crate stalls.
- ClickHouse changes deduplication defaults or setting names.

## References

- ARCHITECTURE.md sections 1 (milestones, cut order), 3 (D6, D17), 5 (BatchSink), 9 (crate rules), 10 (bounded queues, unknown outcomes), 12 (delivery guarantees), 13 (error model), 14 (P1 metrics), 19, 21 (M3), 22.
- ADR-0002 (connector contract), ADR-0003 (execution model), ADR-0011 (delivery guarantees and claims register), ADR-0012 (config; open item O2), ADR-0013 (Parquet layout), ADR-0020 (dependency security and toolchain).
- product.md sections 13, 14, 31, 36 (principles 10 and 11), 37, 40, 46.
- `docs/design/owner-decisions.md` (B2) and `docs/design/product-amendments.md`.
- Ecosystem maturity review (2026-10-07), sections 9, 15, 16, 17. Architecture review, section 6 (retry, bisect, ClickHouse claims). Implementation plan, section 5.10. Demo plan, section 15.
- Upstream: ClickHouse documentation on `insert_deduplication_token`, `non_replicated_deduplication_window`, and `async_insert_deduplicate`; the `clickhouse` crate changelog.
