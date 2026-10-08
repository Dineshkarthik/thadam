# ClickHouse sink

**Status: Planned (P1, after the M1 gate).** ClickHouse is not implemented and no `clickhouse` kind is registered with this binary. It is the first analytical-database sink — valuable for low-latency operational analytics, dashboards, and time-window aggregations — but deliberately *not* a prerequisite for using Thadam and not the project's defining storage path ([sink-priority decision](https://github.com/Dineshkarthik/thadam/blob/main/product.md) in product.md section 31). Design: ADR-0008 (Draft by design); work begins only after the M1 reliability gate.

This page summarizes the planned behavior from ADR-0008 and ARCHITECTURE.md so the delivery story is documented ahead of the code.

## Planned design

- **Batch sink contract.** ClickHouse uses Thadam's `BatchSink` contract: the runtime forms deterministic batches on WAL frame boundaries, retries with the **same batch ID**, and advances the cursor only on success. One in-flight batch (`max_inflight_batches = 1`).
- **Client choice by benchmark.** The official `clickhouse` crate (`insert_native()` or `clickhouse-ext-arrow`) versus raw HTTP with `FORMAT ArrowStream` is decided by benchmark B8 with a parity rule: choose the vendor-maintained crate unless the raw path wins materially (PR19). HTTP/native is not assumed in advance.
- **Schema mapping** with explicit handling of incompatible fields; Thadam **never creates or alters tables** — the operator owns DDL.
- **Poison isolation.** Multi-event batches that keep failing are bisected to isolate and quarantine the offending rows; bisection changes batch identity and is recorded in lineage.
- **Table-per-dataset grouping** is open design work (the batch contract does not yet carry a dataset id; PR20).

## Delivery guarantees

WAL → ClickHouse is **at-least-once**. "Effectively-once" is possible only under specific table settings, and Thadam will **probe them at startup and report the effective delivery class** instead of assuming:

- a replicated table, or
- `non_replicated_deduplication_window` set on the target table (it defaults to 0 — without it there is no deduplication), and
- `async_insert_deduplicate` when using asynchronous inserts.

The probe result is exposed in status and metrics (`thadam_sink_delivery_class{class}`), so what you deployed is what you see. This, with the rest of the [claims register](../concepts/delivery-guarantees.md), gates any public wording about ClickHouse delivery.

## Requirements the implementation must meet (from product.md section 13)

- Batching with configurable flush thresholds.
- Schema/type mapping with explicit handling of incompatible fields, compression, and asynchronous ingestion.
- Retries, backpressure, and clear delivery acknowledgements; documented deduplication/idempotency behavior.
- Integration tests covering server downtime, retries, and duplicate delivery.
- A benchmarked HTTP-versus-native protocol choice.

## Why it is P1, not M1

Robotics data can be impossible to reproduce and edge connectivity is intermittent, so M1 prioritizes what proves the edge-first promise: durable local capture to Parquet segments plus S3-compatible delivery. ClickHouse validates the separate real-time-analytics use case and starts only after that gate ([ongoing status](../roadmap.md)); if capacity tightens, the whole ClickHouse block defers beyond the MVP — nothing in M1 depends on it.
