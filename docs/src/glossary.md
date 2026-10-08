# Glossary

Terms as Thadam uses them. Where a term has a formal definition in an ADR, the ADR governs.

**Ack mode (`ack_mode`)** — When a source message is acknowledged to its broker: `on_commit` (after the WAL commit; at-least-once) or `on_receive` (when pulled; at-most-once across a crash).

**Admin API** — The operational HTTP surface on `127.0.0.1:9464` by default: `/healthz`, `/readyz`, `/metrics`, `/status`. [Reference](operations/admin-api.md).

**At-least-once** — Every accepted message is delivered one or more times; duplicates are possible and expected. Thadam's default class.

**At-most-once** — Delivery without retransmission; a crash can lose messages. Applies to QoS 0 and `ack_mode: on_receive` across a crash.

**Batch sink** — Sink contract for transactional batch targets (ClickHouse, planned): deterministic batches retried under the same batch ID. Compare *segment sink*.

**Capture-first WAL** — The architectural rule (D1) that raw messages are appended to a durable write-ahead log *before* decode, transform, or acknowledgement. See [WAL](concepts/wal-and-replay.md).

**Chain** — A sink's private pipeline: WAL reader and cursor → router → decode → dataset shaping → transform steps → sink driver. See [per-sink chains](concepts/per-sink-chains.md).

**Claims register** — The table in ADR-0011 listing exactly which guarantees Thadam may publish, with conditions and required evidence.

**Config hash** — A canonical hash of the configuration (algorithm version 1) recorded in `thadam.json` and every Parquet footer; one of the inputs to replay equivalence.

**Cursor** — A consumer's durable position in the WAL (`cursors/<name>.cur`). Readers and commits never pass the durable end; positions at or below a durable cursor are never reused.

**Dataset** — An output table: match rule, decoder, identity template, optional event ID/dedup/explode/time rule, and a declared field list. The unit every sink produces. [Reference](configuration/datasets.md).

**Delivery class** — What a sink can guarantee: `AtLeastOnce` or `IdempotentBatch` (for example effectively-once ClickHouse inserts under probed table settings).

**Durable end** — The last WAL position covered by a successful fsync; the bound readers and cursor commits respect.

**Epoch (WAL epoch)** — A random ID created when a data directory initializes; resets WAL positions to 1 and appears in file names and object keys so two epochs never collide.

**Event** — The canonical typed record produced by decoding one source message (or one exploded sample), with time, quality, identity, and payload fields. [Event model](concepts/event-model.md).

**Event time** — The time a sink materializes: corrected time, else source time when the domain is `utc`, else null.

**Field ID** — A stable Parquet `PARQUET:field_id` per column (envelope 1–99, identity 100–199, declared 1000–4,194,303, nested ≥ 8,388,608), never reused. See [field IDs](configuration/datasets.md#field-ids).

**Gap** — An explicit marker in a WAL read sequence — `corrupt`, `evicted`, or `missing` — so loss of position is never silent.

**Group commit** — Batching WAL appends and fsyncing the batch at most once per `fsync.interval`; the latency/throughput trade at the heart of the ack window.

**Identity** — The ordered asset-path hierarchy (`site` → `robot` → …) extracted by templates such as `{topic.2}` and stored as columns.

**Ingest time** — When the Thadam edge received a message. Always present, never changed.

**`on_full`** — The required, explicit policy for a full WAL budget: `block` or `drop_oldest`. See [disk budgets](concepts/backpressure-and-disk-budgets.md#on_full).

**Quarantine** — The segment sink for rejected events, with a fixed schema and its own disk budget.

**Repair mode (`--repair`)** — A run mode that accepts a `thadam.json` pipeline-name or WAL-epoch mismatch and rewrites the binding; never accepts newer format versions.

**Segment** — In the WAL, one append-only file of frames. In sinks, one finalized Parquet (or NDJSON) file for a dataset.

**Segment catalog** — The append-only, CRC-checked log of `Finalized`/`Eligible`/`Uploaded`/`Deleted` segment records (planned).

**Segment sink** — Sink contract for accumulating file targets (Parquet, NDJSON): a watermark reported via `poll_commit`, finalized files, `cursor_committed` for ship eligibility. Compare *batch sink*.

**Shipper** — The service that uploads eligible segments to object storage (planned); not a sink, so a network outage cannot stall capture.

**Source time** — The timestamp the device reported, set once at decode and never overwritten.

**Time domain** — Which clock `source_time` is on: `utc`, `tai`, `monotonic`, `sim`, or `unknown`. Only `utc` becomes event time.

**Warm start** — Rebuilding stateful transform state (dedup, `on_change`) at startup by re-reading `[cursor − window, cursor)` from the WAL without emitting.

**Watermark (sink)** — The WAL position through which a segment sink's files are durable; the runtime commits cursors only up to watermarks.
