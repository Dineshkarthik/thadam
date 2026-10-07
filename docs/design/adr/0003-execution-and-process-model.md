# ADR-0003: Execution and process model

- **Status:** Accepted
- **Date:** 2026-10-07
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"), including decision D1 (capture-first WAL, with the chain running after the commit). The WAL-side evidence from S1 is sufficient for D1. The unticked items in "Evidence and validation required" (chain CPU with N chains, WAL growth during a sink outage, panic isolation, replay-recovery tests, shutdown order) are implementation-time verification in PR8 to PR10 and PR18, not blockers. The on-disk format that D1 relies on is not frozen by this acceptance; that waits on ADR-0004.

## Context

Thadam is a single-process edge runtime for 1 to 4 cores and 256 MB to 2 GB of RAM. This ADR decides where durable capture sits relative to decoding and transforms, how many processes and pipelines there are, which tasks and threads exist, and how work is bounded. The first decision, the placement of the write-ahead log (WAL), is the most consequential: it determines the on-disk format's relationship to the data model, what survives a misconfiguration, and how replay works.

The original MVP plan placed a single delivery WAL after the transforms, so that filtering and downsampling would also reduce disk use (product.md UC-05), with one pipeline task running decode and transforms inline and an optional raw MCAP recording tap for reprocessing. The architecture review found these problems with that placement (Option A below):

- The on-disk format equals the canonical model, so every model change becomes a disk migration.
- A transform bug or a misconfigured filter loses data permanently. The product thesis is that robot data "may be expensive or impossible to reproduce".
- Reprocessing replay needs a second recording mechanism, the MCAP tap.
- Source-acknowledgement latency includes transform time.
- A restarted deduplication or sampling step starts cold.

The review's cost-benefit argument also weakens the main advantage of Option A. The P0 WAL drains into local Parquet regardless of what the network is doing. In steady state it holds roughly `max_age` worth of data (minutes). During a network outage the backlog accumulates as compressed, transformed Parquet, not as WAL. Only an outage on a ClickHouse path (P1) grows the WAL. So transform-before-buffer barely reduces disk use on the P0 path.

## Decision

### Capture-first raw WAL with per-sink chains (D1)

- The WAL stores raw source messages with a minimal envelope (`source_idx`, `ingest_time`, `clock_synced`, interned attributes such as topic, QoS, and retain, and the payload bytes). Nothing is decoded or transformed before the source is acknowledged.
- The engine choice (a custom segmented log, with SQLite as the fallback) and the frame format are ADR-0004, which is Draft: spike S1 has reported and confirmed the engine and layout, and the S1 re-run on reference hardware is still required before the format is frozen. The canonical model never reaches the disk (ADR-0001).
- Each sink has its own **chain**: WAL reader, dataset router, decode, normalize, transform steps, then the sink driver. Chains read the committed WAL from their own cursor.
- Replay is a cursor rewind within the WAL retention window (D24). MCAP is an export and import format only, not a recording tap (reserved ADR-0015).

### Process model

- One process and one binary (`thadam run pipeline.yaml`). One pipeline per process in the MVP, with its own `data_dir` protected by an `flock` lock. Several sources per pipeline are supported (for example MQTT and ROS 2 into one WAL). Several pipelines on one host means several processes (for example systemd units).
- Out-of-process connectors are reserved for a future plugin mechanism (reserved ADR-0017).

### Task and thread topology

- **Main thread:** the CLI builds a Tokio runtime with `worker_threads = min(cores, 4)` and `max_blocking_threads = 8`.
- **One WAL writer OS thread**, the only owner of writable segment descriptors. Tokio's `fs` module issues one `spawn_blocking` per operation and is unsuitable for the hot path.
- **Per source:** a connector liveness task that owns the protocol client, and a `SourceDriver` that pulls batches, submits them to the writer, and releases acknowledgements in order (ADR-0002).
- **Per sink:** a `ChainTask` plus a driver (`SegmentDriver` or `BatchDriver`).
- **Singletons:** the shipper, a maintenance task (retention, budgets, clock sync), the admin server, a signal handler, and an optional OTLP exporter. A supervisor owns a `JoinSet`.
- **Blocking pool:** WAL reader `pread`, Parquet row-group encoding and finalization, catalog and cursor fsyncs, and upload file reads.
- **Ownership:** batches move and are never shared. An `OpenSegment` moves into a `spawn_blocking` closure and back, so no lock is held across encoding. Shared state is limited to `Arc<DiskBudget>` (atomics), `Arc<SegmentCatalog>` (a mutex never held across `.await`), metric handles, immutable `Arc<Shape>`, and the `watch` channel carrying the committed WAL end.
- **Cancellation:** cooperative through a `Shutdown` token (ADR-0002). Blocking jobs are bounded (one row group, one group commit) and always awaited at shutdown.

### Chain rules

- A chain is synchronous, pure, `!Sync`, and owned by exactly one task. A slow chain cannot block capture, because capture is only a WAL append.
- **Chains always use the current configuration from their cursor.** Replaying old data after a configuration fix applies the new configuration. This is stated to users explicitly.
- **Panic isolation per chain.** A poison message is retried `poison_retries` times, then quarantined, and the cursor advances.
- `ChainLimits` (`max_shapes`, `max_dedup_keys`, maximum JSON depth) convert overflow into rejects.
- **Primary chain [A, open item O3].** One chain is designated primary and owns quarantine writes and reject metrics, so several chains seeing the same raw message do not double-count.
- Processing order within a dataset, the transform set, and deduplication placement are in ARCHITECTURE.md section 7. Deduplication responsibility is ADR-0011.

### Determinism means row-set equivalence

For the same WAL range, the same configuration hash, and the same enrichment-file hash, a replay produces the same rows: identical `_event_id`, values, and flags. File boundaries may differ. Byte-identical Parquet is not claimed (ADR-0011). Event IDs must therefore be deterministic and sampling must not use wall time.

### Shutdown and recovery (summary)

Ordered shutdown: cancel intake, drain sources into the WAL, close the WAL with a final group commit and fsync and a `clean_shutdown` marker, let chains and drivers drain up to `shutdown.drain_timeout`, flush telemetry, release the lock, exit. A second signal or `shutdown.timeout` aborts the `JoinSet`, which is safe because the WAL plus recovery covers anything unfinished. Startup recovery runs: lock and format check, WAL tail scan and truncation, cursors, segment sinks and catalog, shipper queue, batch-sink re-send, stateful-transform warm start, then sources last. Exit codes: 0, 1, 2, 3, 130. Details are in ARCHITECTURE.md sections 10 and 11.

### Bounded-queue philosophy

No queue is unbounded. Queues are bounded by count and, where memory matters, by bytes. Backpressure on flow-controlled sources works by withholding acknowledgements while the client keeps polling. The complete queue inventory and the memory estimate printed by `thadam validate` are in ARCHITECTURE.md section 10. The `on_full` policy and delivery wording are ADR-0011.

### What this ADR supersedes

From ARCHITECTURE.md section 19: the post-transform WAL; the optional raw MCAP tap (replaced by WAL retention plus export and import); the single pipeline task running decode and transforms inline (replaced by one `ChainTask` per sink); the 17-crate layout; and the separate `thadam-observability` crate (folded into `thadam-runtime`).

### Appendix: crate granularity (D20)

Ten library crates plus the CLI form the MVP workspace (eleven members), split by dependency weight and change rate. `thadam-clickhouse` joins at M3. All crates are `publish = false` until the naming ADR (reserved ADR-0016).

- `thadam-core`: model, errors, positions, connector traits, clock, shutdown, disk budget.
- `thadam-config`: YAML to typed `PipelineSpec`, interpolation, diagnostics, JSON Schema.
- `thadam-wal`: writer thread, readers, cursors, retention, recovery.
- `thadam-transform`: synchronous and pure router, decoders, shapes, steps, chain compiler.
- `thadam-runtime`: drivers, supervisor, shutdown, replay, observability, admin API.
- `thadam-arrow`: Event to Arrow `RecordBatch`, dataset schema mapping, field IDs.
- `thadam-segments`: Parquet and NDJSON segment sink, catalog, quarantine writer.
- `thadam-object-store`: S3-compatible shipper over the catalog.
- `thadam-mqtt`: MQTT source.
- `thadam-testkit`: memory connectors, conformance suites, fake clock, crash harness (never a production dependency).
- `apps/thadam-cli`: the only composition root. `thadam-clickhouse`: P1 batch sink, excluded from the workspace until M3.

Dependency rules, enforced by `cargo deny` bans and `scripts/check-deps.sh` (with a `--self-test` that must fail on a violating fixture):

- `thadam-core` has no Tokio runtime, Arrow, protocol crates, or YAML crate.
- `thadam-runtime` has no Arrow, Parquet, `object_store`, `rumqttc`, or `reqwest`, and knows connectors only through `thadam-core` traits.
- Connector crates never depend on `thadam-runtime`. Arrow appears only in `thadam-arrow`, `thadam-segments`, and `thadam-clickhouse`, and only as sub-crates.
- Workspace policy includes `unsafe_code = "forbid"` and `panic = "unwind"` in release builds so task-boundary panic isolation works. Open item O1 (where declared-field types live) is resolved by assumption in ARCHITECTURE.md section 9 and decided in PR3.

## Alternatives considered

- **Option A: post-transform canonical WAL (original design).**
  - Pros: transforms run once; filtering reduces WAL bytes.
  - Cons: the format is coupled to the model; a transform bug loses data permanently; replay needs a separate MCAP tap; acknowledgement latency includes transform time; stateful transforms start cold.
  - Not chosen. The disk-reduction benefit is weak, for the reason given in Context: the P0 WAL drains into local Parquet whatever the network does.
- **Option C: both a raw WAL and a canonical WAL.** Gives both properties at twice the write amplification, which is bad for flash endurance (the 50 sensors at 1000 Hz example is about 10 MB/s of payload, ADR-0011). Rejected.
- **Multiple pipelines per process.** Shares resources but complicates isolation and accounting. Not chosen. Multiple sources per pipeline are supported instead, and several pipelines mean several processes. It is a non-goal for the MVP.
- **Separate processes for sources and core.** Gives crash isolation but adds IPC cost and deployment complexity. Not chosen. Out-of-process connectors are reserved for the plugin mechanism.
- **One shared chain for all sinks.** Decode and transforms would run once, but a slow sink would block the others, and chains could not differ per sink (a raw local archive plus a downsampled ClickHouse stream, product.md UC-05). Not chosen.
- **One pipeline task running decode and transforms inline (original).** Simple, but capture and processing share a head-of-line. Not chosen.
- **Tokio `fs` for the WAL.** One `spawn_blocking` per operation on the hot path. Not chosen in favor of a dedicated writer thread.
- **Optional MCAP tap before decoding.** A second recording mechanism with its own disk budget. Replaced by the WAL retention window plus MCAP export and import.
- **About 8 crates (review) or 17 crates (original).** The review's list kept chains and transforms inside `thadam-runtime` and put Arrow conversion, the Parquet and NDJSON writers, and the shipper inside `thadam-segments` (with `object_store` behind a feature). The implementation plan split `thadam-transform`, `thadam-arrow`, and `thadam-object-store` out for dependency-weight reasons, giving ten library crates. 17 concept crates were rejected as premature.

## Consequences

### Easier

- The on-disk format is small and independent of model evolution (ADR-0001, ADR-0004).
- Capture survives transform bugs and misconfiguration. A wrong filter is corrected and replayed from the WAL instead of losing data.
- Chains differ per sink, and a slow or failed sink cannot block capture. Stateful transforms warm-start deterministically from the WAL tail. Replay is a cursor rewind.

### Harder

- Decode and transforms run once per sink, so CPU grows with sink count (one to three in practice). The cost is **not yet measured**.
- WAL bytes during a ClickHouse outage are raw rather than reduced, so the WAL budget must cover that case.
- Quarantine and reject metrics need a primary chain (open item O3).
- Users must understand that chains apply the current configuration to old data from their cursor.
- A single process shares fate: a crash in any connector, including an abort inside a native dependency, takes the process down. Recovery then covers the data.
- The replay window is bounded by `retain_for` and the disk budget. Reprocessing older data needs a re-capture, or MCAP import once M2 delivers it (MCAP is outside v0.1).

### Fixed

- Capture-first placement. The WAL format becomes permanent once released (ADR-0004), so reversing D1 later is expensive. One pipeline per process, the chain rules, and row-set equivalence as the determinism definition are the basis of user-facing documentation.

## Evidence and validation required

Spike S1, report at `docs/design/spikes/S1-wal.md` (reported 2026-10-07). It ran on macOS and a Linux VM, not on x86 NVMe and ARM eMMC (the supported storage baseline decided by the owner on 2026-10-07; SD cards are not a supported target and are optional data points only). Status of what D1 needs from it:

- [x] Raw-WAL commit-to-ack latency (Option B): about 2.4 ms p50 and 4.6 ms p99 at the 10 MB/s reference load on the Linux VM, 5.9 and 9.5 ms with real flushes on macOS, about 2% framing overhead for 200 B records. S1 did not prototype the hypothetical Option A (post-transform WAL), so the comparison against it is not measured. [ ] Hardware re-run (ADR-0004).
- [ ] Chain CPU with N = 1, 2, and 3 chains on workloads W1 (small telemetry) and W2 (joint-state arrays). Not measured by S1 or the D8 report (which measured per-event transform cost, not chain CPU); a PR8 to PR10 benchmark.
- [ ] WAL growth rate during a ClickHouse outage, raw versus transformed. Not measured by S1; a PR18 benchmark (ClickHouse is M3, so this is a later item).
- [x] The engine, fsync, and recovery results ADR-0004 needs (reported; see ADR-0004).

Tests that must pass:

- [ ] Panic isolation test: a poison message panics one chain, is quarantined, and the cursor advances.
- [ ] Misconfigured-filter-then-replay recovery test: the dropped data is recovered after the configuration fix.
- [ ] Row-set-equivalence test: replaying the same frames twice gives equal row sets, and warm-then-process equals a continuous run.
- [ ] Two chains with different transforms produce independently correct outputs.
- [ ] A configuration-hash change is recorded in lineage metadata.
- [ ] Shutdown order and exit codes. Crash-between-steps matrix (ADR-0011).
- [ ] RSS bound under a stalled sink (PR10).
- [x] Owner sign-off on decision D1 (given by accepting this ADR), recorded 2026-10-07, before PR6 and PR10. D1 is not part of the B6 register.

## Revisit when

- The S1 re-run on reference hardware shows raw-WAL commit latency is unacceptable (the first S1 run on a laptop and a Linux VM did not).
- More than three sinks make per-chain CPU material.
- A plugin model needs process isolation.
- Demand for multiple pipelines per host appears.

## References

- ARCHITECTURE.md section 3 (D1, D20, D24), section 6 (capture WAL), section 7 (chains and transforms), section 9 (crate map), section 10 (concurrency), section 11 (shutdown and recovery), section 19, section 22 (O1, O3).
- ADR-0001 (canonical event model), ADR-0002 (connector contract), ADR-0004 (durable buffer), ADR-0011 (delivery guarantees and claims register), reserved ADR-0015 (recording and replay), reserved ADR-0016 (naming), reserved ADR-0017 (plugin mechanism).
- product.md sections 5 (UC-05), 11 (edge-first execution), 12 (buffering).
- Plans: `thadam_architecture_review` section 3 and decision matrix D1, D20, D24; `thadam_rust_implementation_plan` sections 1 to 3 and 4.4 to 4.7; `thadam_mvp_architecture_plan` section B (process model, buffering model).
- Spike report: `docs/design/spikes/S1-wal.md` (2026-10-07).
