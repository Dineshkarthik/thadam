# Readiness gate: re-run of 2026-10-07 (after the owner decisions)

Status: **CONDITIONALLY READY, implementation under way.** All eight sources (`mqtt`, `http`, `grpc`, `mcap`, `modbus`, `can`, `opcua`, `ros2_live`), all three decoders (`json`, `ros2`, `sparkplug_b`), all eleven transform steps (including `pose`, `joint_state`, and experimental `window` with all eleven aggregates), all seven sinks and services (`parquet`, `object_store`, `iceberg`, `nats`, `kafka`, `mqtt_publish`, `clickhouse`), all nine CLI subcommands (`init`, `validate` with `--against-data-dir`, `version`, `schema`, `run`, `status`, `replay`, `export`, `inspect`), the consolidated Prometheus metrics set, deployment and rollback scripts (`deploy/apply-config.sh`), the simulator, the oracle verifier, the chaos suites, the benchmark harness, and the Python SDK are implemented and tested as of 2026-10-14 (`HEAD`). The WAL was built ahead of ADR-0004's acceptance by the owner's choice, so its on-disk format is versioned but not frozen: the ADR-0004 hardware re-run still gates the acceptance, and the format freezes at the v0.1.0 tag, after that re-run. Part A of `docs/design/product-amendments.md` (A.1 to A.9 and A.11) is applied to `product.md` as of 2026-10-13, which closes blocker 2.

Date: 2026-10-07 (updated through 2026-10-14). This document re-runs the checklist in section 1 of the readiness gate plan (`thadam_mvp_readiness_gate`) against the current documents and evidence, after the owner's decisions of 2026-10-07 (ADR acceptances, B7, and the EMQX pin; see `docs/design/owner-decisions.md`). It supersedes the plan's "NOT READY" paragraph for tracking purposes and is updated whenever a blocker moves. The sources of truth it summarizes are `ARCHITECTURE.md`, `docs/design/owner-decisions.md`, `docs/design/adr/`, and the spike reports in `docs/design/spikes/` (S1 WAL, S2 MQTT, S3 ROS 2 live, Sparkplug B, D8 record representation, ADR-0012 YAML, ADR-0018 filter).

Tags: **[A]** assumption, **[V]** validate before relying on it.

## 1. Verdict

**CONDITIONALLY READY.** By the plan's flip rule (section 2, "Flip to READY when"), the design conditions are met except for ADR-0004 (which gates only the WAL work). The repository state and the owner action that were missing are now closed (the owner action on 2026-10-13):

| Flip condition (readiness gate plan) | State | Detail |
|---|---|---|
| B1 merged, with ADRs 0001, 0002, 0003, 0011, 0013, 0020 accepted | **Met** | All six are **Accepted** (owner, 2026-10-07). The workspace scaffold and the docs baseline are committed, so the acceptances are recorded in the repository. |
| B2 and B3 signed by the owner | **Met** | Decided 2026-10-07 (`owner-decisions.md`). |
| S1 and S2 reports in, with ADR-0004 and ADR-0006 accepted | **Half met** | Both reports are in. ADR-0006 is **Accepted** (with `rumqttc` pinned, built with no features and TLS terminated in `thadam-mqtt` since the 2026-10-07 amendment, and EMQX pinned to 5.8.x). ADR-0004 stays **Draft**: the S1 re-run on x86 NVMe and ARM eMMC is required before the on-disk format freezes at the v0.1.0 tag. It no longer blocks the code: `thadam-wal` is built with a versioned but unfrozen format; it gates only the freeze. |
| B6 and B7 decisions recorded | **Met** | B6 (D5, D6, D8, D9, D10) and B7 (ADR-0012 `serde-saphyr`, ADR-0018 structured predicates) are decided and their ADRs are Accepted. |
| B8 folded in | **Met** | Done in `ARCHITECTURE.md` sections 15, 20, and 21; updated for the 2026-10-07 decisions. |
| Owner applies the approved Part A amendments to `product.md` | **Met** | Applied on 2026-10-13: A.1 to A.9 and A.11 verbatim (A.10 records the alternatives and is not applied). `product.md` now matches the frozen scope. Parts B and C remain unapproved. |

### Readiness by component and milestone

The overall verdict is a conjunction. Each row below shows the component or milestone, its original gate, and its current state.

| Work | Needs | State |
|---|---|---|
| PR1 (workspace scaffold and CI guardrails) | Commit | **Done.** Re-verified with workspace MSRV 1.89 (`thadam-iceberg` 1.94). |
| PR2 (docs baseline) | Owner go-ahead to commit | **Done.** |
| PR3, PR4 (`thadam-core` model and traits, `thadam-testkit`) | PR1 and PR2 committed | **Done.** |
| PR5 (`thadam-config`, `thadam init`, `thadam validate`, `thadam schema`, `thadam version`) | PR3; PR1 and PR2 committed | **Done.** Includes `thadam init` (starter `pipeline.yaml` with `on_full: block`) and `thadam validate --against-data-dir`. |
| PR6, PR7 (WAL writer, readers, cursors, retention) | ADR-0004 accepted, which needs the S1 re-run on reference hardware | **Done**, built ahead of the acceptance by the owner's choice. The format is versioned but not frozen; it freezes at the v0.1.0 tag, and ADR-0004's acceptance waits for the same hardware re-run. |
| PR8 (transform part 1) | PR3 | **Done.** |
| PR9 (transform part 2) | PR3 and PR8 | **Done.** ADR-0018 is Accepted; details are recorded in the `thadam-transform` crate documentation. |
| PR10 (`thadam-runtime` core) | PR3 to PR8 | **Done**, including segment-sink appends grouped by dataset and per-sink `required`/`max_lag`/`start_from` in `thadam-config`. |
| PR11 (observability, admin API, `thadam run`, `thadam status`) | PR10 | **Done.** |
| PR12 (`thadam-mqtt`) | PR4 and the runtime sequence | **Done.** ADR-0006 is Accepted (amended 2026-10-07: TLS is terminated in `thadam-mqtt`); EMQX is pinned to 5.8.x for CI. |
| PR13 (`thadam-arrow`) | Earlier PRs | **Done.** ADR-0013 is Accepted; O4, O5, and the unsigned default (`Native`) are closed. |
| PR14 Parquet segments (`thadam-segments`) | Earlier PRs | **Done.** |
| PR15 simulator + demo (`tools/thadam-simulator`, `examples/edge-capture`) | Earlier PRs | **Done.** |
| PR16 S3 shipper (`thadam-object-store`) | Earlier PRs | **Done.** |
| PR17 replay and inspection (`thadam replay`, `thadam inspect`) | Earlier PRs | **Done.** |
| PR18 oracle verifier (`thadam-simulator verify`) | Earlier PRs | **Done.** |
| Chaos suites | Earlier PRs | **Done** (suites built; M1 gate chaos verification run still open). |
| Benchmark harness (`tools/thadam-bench`) | Earlier PRs | **Done** (harness built; reference-hardware numbers still open). |
| HTTP source (`thadam-http`) | Earlier PRs | **Done.** |
| gRPC source (`thadam-grpc`) | Earlier PRs | **Done.** |
| Python SDK (`sdk/python`) | Earlier PRs | **Done.** |
| ROS 2 decoder (`thadam-ros2-msg`) | Earlier PRs | **Done.** |
| Connector metrics hook | Earlier PRs | **Done.** |
| ROS 2 MCAP source (`thadam-mcap`) | Earlier PRs | **Done.** |
| MCAP exporter and `thadam export --mcap` (ADR-0015, Draft) | M2-B | **Done** (runtime lock and schema store integrated). The Foxglove check in ADR-0015 is still open. |
| ROS 2 fuzz targets (`msg_parse`, `cdr_decode`) | M2-A | **Done.** Runs in the `fuzz-smoke` CI job alongside the `thadam-transform` and `thadam-sparkplug` targets. |
| WAL reference-hardware bench script and runbook | ADR-0004 evidence (blocker 3) | **Tooling done** (`scripts/wal-hw-bench.sh`, `docs/design/wal-hardware-runbook.md`). The x86 NVMe and ARM eMMC runs are not done. |
| Quarantine decoupled from the sink cursor (fsynced reject staging) | Runtime follow-up | **Done.** |
| `pose` and `joint_state` validation steps, `INVALID` flag, robotics conventions | M2 (ROS 2 validation) | **Done.** |
| `time` from a message attribute (`time.attr`) | M2 | **Done.** |
| ROS 2 chain decode and data-directory schema store (`decode: ros2`) | M2-A and M2-B | **Done.** |
| Dotted dataset field names and `pose` checks at validate time | M2 | **Done.** |
| `examples/ros2-mcap-to-parquet` and `thadam run --exit-when-sources-finish` | ADR-0007 stage 2 (M2-B) | **Done.** |
| Sparkplug B decoder crate and birth store (`thadam-sparkplug`, `BirthStore`; step 1a) | Sparkplug spike follow-up (O-1, O-2) | **Done.** |
| `decode: sparkplug_b` in the transform chain: one event per message, BIRTH/DEATH events, `SEQUENCE_GAP`/`STALE`/`DECODE_ERROR` mapping, per-sink birth store seeded at chain build and fsynced before a message's events are returned, read-only seeding for `thadam replay`, `decoder_handle` fuzz target in `fuzz-smoke`, ignored Docker test against Mosquitto 2 (step 1b) | Sparkplug spike follow-up | **Done.** Not covered: Template and DataSet metrics, a `seq` gap that spans a restart, tests against real edge devices, throughput benchmarks. |
| ClickHouse sink (`thadam-clickhouse`) | Earlier PRs | **Done.** |
| Modbus TCP source (`thadam-modbus`) | Earlier PRs | **Done.** |
| NATS JetStream sink (`thadam-nats`) | Earlier PRs | **Done.** |
| SocketCAN source (`thadam-can`) | Earlier PRs | **Done** (`vcan` tests run in the `can-vcan` CI job). |
| Connector registration and CI | Earlier PRs | **Done.** |
| MQTT publish sink (`mqtt_publish` in `thadam-mqtt`) | Earlier PRs | **Done.** |
| OPC UA source (`kind: opcua`, subscribe and poll modes, X.509 certificate auth, server-certificate pinning, behind the default-off `opcua` feature) | Phase 3 | **Done.** |
| Live ROS 2 source (`kind: ros2_live` over `rmw_zenoh`, ADR-0007 stage 4, experimental, behind the default-off `ros2-live` feature) | Spike S3 | **Done** (experimental). |
| Apache Kafka sink (`kind: kafka` over `rskafka`, TLS, mutual TLS, SASL PLAIN/SCRAM) | Phase 4 / P1 | **Done.** |
| Iceberg registrar (`kind: iceberg`, P2, experimental): `fast_append` of shipped segments into one table per dataset, behind the default-off `iceberg` feature | Earlier PRs | **Done** (experimental). |
| Iceberg step 2: schema evolution by field ID (add optional, rename, promote; narrowing and ID reuse fatal), concurrent-writer conflict test, HTTPS catalog test with a private CA | Iceberg registrar | **Done** (experimental). |
| Iceberg step 3: catalog authentication (`catalog_token`, `catalog_credential` OAuth2 client credentials with reactive refresh on 401) and mutual TLS (`catalog_client_cert` + `catalog_client_key`), including on direct schema-evolution commits | Iceberg step 2 | **Done** (experimental). Not yet covered: compaction, partitioning. |
| Object-key collision check in `thadam validate` (`object_store` shippers sharing pipeline, normalized URL, Parquet sink `node_id`, and a dataset) | Validation follow-up | **Done.** |
| Phase 5: pipeline versioning (`thadam_build_info`, `/status` fields, Parquet footer hashes), `Pipeline::check` + `thadam validate --against-data-dir`, `deploy/apply-config.sh` with rollback and restart-gap docs, and `thadam_chain_unmatched_total` | `docs/design/fleet-config-and-versioning.md` | **Done.** |
| Phase 6 (steps S1 to S5): `window` transform step (tumbling and sliding via `slide`, ingest and event time, `allowed_lateness`, per-asset watermark, `late` outcome, `idle_close`, `window_gap` and `window_range_start` quality flags, `count`/`sum`/`min`/`max`/`mean`/`first`/`last`/`stddev`/`range`/`delta`/`rms`, cursor hold-back, memory estimate) | `docs/design/advanced-processing.md` | **Done** (experimental). S6/S7 (WASM/inference) deferred per owner decision until a concrete user need appears. |
| Consolidated Prometheus metrics set (`ARCHITECTURE.md` section 14) | Observability follow-up | **Done.** |

### Decisions made while implementing

Where implementation-time refinements are recorded:

- **ADR-0006 amendment (2026-10-07):** every `rumqttc` 0.25.1 TLS feature pins `rustls-webpki` 0.102.8 (four RustSec advisories, no fix), which met exit trigger 3. `rumqttc` is built with no features and TLS is terminated in `thadam-mqtt` through an in-process relay (`tokio-rustls` with `ring`). Revisit when `rumqttc` releases a patched `rustls-webpki`.
- **ADR-0013:** field-ID policy (O5), `_wal_record_idx` not written with ID 13 reserved (O4), unsigned default `Native` with a `Signed` option, list element name `element`, `sample_idx` `UInt16`, 16-byte SHA-256-based schema fingerprint. Explicit field `id`s (`datasets[].fields[].id`) and identity level `id`s (`datasets[].identity.<level>.id`) are implemented in `thadam-config` and round-trip through `thadam-segments`.
- **ADR-0004 (still Draft):** implementation deviations recorded in "Implementation notes (PR6 and PR7)".
- **ADR-0012 and `ARCHITECTURE.md` section 15:** parse budget 50,000 nodes and depth 32, anchors, aliases, merge keys, and custom tags rejected, `--allow-unresolved`, `thadam schema`, identity levels as an ordered list.
- **`ARCHITECTURE.md` section 7:** one `EventBatch` per dataset, identity passed as an ordered list, `ChainSpec` built by the runtime, time domain defaults to `unknown`, skew and missing time flagged and never corrected.

## 2. Checklist re-run (readiness gate plan, section 1)

Each row compares the plan's status of 2026-10-07 (morning) with the state now.

| Checklist item | Plan said | Now | Evidence and what remains |
|---|---|---|---|
| Canonical event model | PARTIAL | **ACCEPTED** | D8 (representation B behind an opaque `Record`), D9 (extended values), and D10 (one event per message plus `explode`) signed in B6; quality granularity signed in B3. ADR-0001 is Accepted. The `Value` size-bound test and builder invariant tests are in `thadam-core`; open item O10 (where processing time is recorded; additive) remains open before 1.0. |
| Connector API | PARTIAL | **ACCEPTED** | D5 and D6 signed. S2 confirmed liveness, cancel-safety, ordered ack, and payload-stripped handles, and added three clauses. ADR-0002 is Accepted. `SinkBatch`, conformance suites, and source/sink connectors are implemented. |
| Pipeline execution semantics | DEFINED, not accepted | **ACCEPTED** | ADR-0003 is Accepted, including D1 (capture-first WAL). S1 supports D1 on the WAL side (about 2% framing overhead; commit-to-ack is one group commit). Implementation-time benchmarks: chain CPU with one to three sinks, WAL growth during a sink outage, and the hypothetical post-transform WAL (Option A). |
| Buffer semantics | DEFINED; engine and format not selected | **Engine selected; format not frozen (ADR-0004 Draft)** | S1: custom segmented log, SQLite as the fallback, redb not for the bulk queue. The frame header is 24 bytes with a reserved word (O9 closed). Provisional fsync default `interval(10ms)`. ADR-0004 stays Draft: the S1 re-run on reference hardware, validation of the prost payload, an ENOSPC loop-device test, and a directory-metadata loss test are outstanding. `on_full` has no default (B3). `thadam-wal` is built with a versioned but unfrozen format; deviations from the ADR text are in ADR-0004 "Implementation notes (PR6 and PR7)". |
| Retry semantics | DEFINED | **ACCEPTED** | ADR-0011 is Accepted. One `[OPEN]` wording item (the power-loss exposure window of the configured fsync policy) can be derived from S1 but is not yet written; no power-loss claim is published until the hardware rig runs. |
| Configuration model | PARTIAL | **DECIDED** | ADR-0012 is Accepted (`serde-saphyr`, `strict_booleans: true`, MSRV 1.89, `serde_norway` fallback) and ADR-0018 is Accepted (structured predicates, CEL reserved). The demo `[IMPL]` options are folded into `ARCHITECTURE.md` section 15. Closed in `thadam-config` and `thadam-transform`: parse budget 50,000 nodes and depth 32, anchors, aliases, merge keys, and custom tags rejected (ADR-0012 "PR5 outcomes"). |
| Error model | DEFINED | **DEFINED** | Unchanged. |
| Observability model | DEFINED, minor drift | **DEFINED** | Metric names consolidated (section 14) and all 46 series implemented in `thadam-runtime` (`crates/thadam-runtime/src/observability.rs`), including `thadam_source_unacked_max` and `thadam_source_stale_acks_total` from S2. |
| Test strategy | DEFINED | **DEFINED, strengthened** | S1 contributes the kill -9 loop, LazyFS, torn-tail, and fsync-fault harness designs; S2 contributes the wire oracle, the cancel-safety and stale-ack scenarios, and the no-`select!`-around-`poll` rule. |
| Benchmark strategy | DEFINED; hardware assumed | **DEFINED; hardware still assumed [A]** | All spikes ran on an arm64 laptop (S1 and S2 also used a Linux VM in Docker Desktop). Re-runs on reference ARM and x86 hardware are required for S1 (before the format is frozen) and recommended for S2, D8, and ADR-0018's evaluator numbers (before any published claim). |
| MQTT implementation | PROVISIONAL | **ACCEPTED, pinned** | `rumqttc` 0.25.1 behind `thadam-mqtt`, with the five wrapper rules and three configuration requirements, `on_commit` default, `v311` default and `v5` experimental. ADR-0006 is Accepted. EMQX is pinned to the last open-source line, 5.8.x, for CI, tests, demos, and documentation; newer Enterprise-licensed images are not used. `rumqttc` is built with no features and TLS is terminated inside `thadam-mqtt` (ADR-0006 amendment, 2026-10-07), because every TLS feature pins a vulnerable `rustls-webpki`. Open, not blocking: S2b against `rumqttc-next` and non-happy QoS 2 paths (QoS 2 stays experimental). |
| Local Parquet capture and recovery | SELECTED | **ACCEPTED** | ADR-0013 is Accepted. The unsigned default (`Native`), O4, and O5 are closed in `thadam-arrow` and `thadam-segments`; the W4 and B6 benchmark criteria and reader compatibility across downstream query engines stay open. |
| Object-store implementation | SELECTED | **SELECTED** | Unchanged. |
| ClickHouse | DEFERRAL DOCUMENTED | **DECIDED (P1 after the M1 gate), in `product.md`** | Owner decision D-B2-3. ADR-0008 stays Draft. `product.md` sections 14, 40, and 46 now describe the M1 path, and ClickHouse appears there only as P1 (M3). |
| ROS 2 strategy | DOCUMENTED; scope unresolved | **DECIDED: M2, not in v0.1** | Owner decision D-B2-2 (MCAP and rosbag2 offline first). ADR-0007 stays Draft; the live-transport spike S3 is M2 work and does not block M1. |
| Major unknowns isolated behind interfaces | MOSTLY | **YES, with one caveat** | The record representation is behind the opaque `Record`; the WAL engine is behind `thadam-wal`; `rumqttc` types never leave `thadam-mqtt`; the ROS 2 transport and ClickHouse format are behind their traits; the YAML crate is behind `serde_json::Value`. The filter syntax is user-facing but is now decided (structured predicates), with `expr:` reserved for a future CEL. Caveat: the WAL on-disk format stays unfrozen until the v0.1.0 tag, which follows the ADR-0004 hardware re-run. |
| MVP scope frozen | NO | **FROZEN** | B2 and B3 are signed (MVP v0.1 = M1; ROS 2 = M2; ClickHouse = P1; HTTP, gRPC, and aggregation after the MVP; explicit `on_full`; wide Parquet; event-level quality; batching not assumed; industrial SSD or eMMC). The approved Part A amendments are applied to `product.md` (2026-10-13), so the product document matches the scope. |

## 3. Remaining blockers, with owners

Ordered by how much work each unblocks. "Owner" is the project owner unless stated.

1. **Commit PR1 and the docs baseline (PR2).** **Closed:** both are committed, so the ADR acceptances are recorded in the repository. (Originally: nothing was committed and the whole repository was untracked.)
2. **Apply the approved Part A amendments (A.1 to A.11) of `docs/design/product-amendments.md` to `product.md`.** **Closed (2026-10-13):** A.1 to A.9 and A.11 are applied verbatim (A.10 is the record of alternatives and is not applied). Parts B and C are not approved and remain for the owner to decide separately.
3. **S1 re-run on x86 NVMe and ARM eMMC, then accept ADR-0004.** Engineering, with the owner providing reference hardware **[A]**. The run is now one script with a runbook (`scripts/wal-hw-bench.sh` and `docs/design/wal-hardware-runbook.md`); the runs themselves have not been done. Scope (from ADR-0004): flush latency and `fdatasync` behavior on the real devices; commit-to-ack latency by fsync mode; device-level write amplification from `/proc/diskstats`; eMMC recovery against the 50 ms criterion; the fsync default (`interval(10ms)` is provisional); Linux preallocation; segment size; validation of the prost `RawFrame` payload (CPU, framing cost, frozen field numbers); an ENOSPC test on a loop device with lazy allocation applied; and a directory-metadata loss test (`dm-flakey`) or the power-cut rig. **Gates the WAL format freeze (at the v0.1.0 tag) and ADR-0004's acceptance only:** `thadam-wal` is already built with a versioned but unfrozen format. The hardware power-cut rig (500 cycles) is separate and is needed only before a power-loss claim (ADR-0011).
4. **Reference hardware access.** Owner. A Raspberry Pi 5 with NVMe or a Jetson (ARM), an industrial eMMC board, and an x86 NVMe machine. Required by blocker 3 and by the D8, S2, and ADR-0018 re-runs on ARM and x86.
5. **ADR-0000 (the process itself) is still Proposed.** Owner. It was not in the 2026-10-07 acceptance list; the lifecycle it defines is already followed, so this is a formality.

Closed in this pass: acceptance of ADRs 0001, 0002, 0003, 0006, 0011, 0012, 0013, 0018, and 0020; the ADR-0012 and ADR-0018 decisions; the EMQX licensing question (pinned to 5.8.x); the Rust floor (1.89).

### Tracked, not blocking

- Implementation-time details: the ADR-0012 "PR5 implementation details" and the ADR-0018 "PR9 implementation details" are implemented (ADR-0012 "PR5 outcomes", `thadam-transform` crate documentation); their remaining unticked test and fixture items stay tracked.
- ADR-0006: S2b (the S2 harness against `rumqttc-next`, at most one day) and the QoS 2 non-happy paths (QoS 2 stays experimental). TLS and mTLS through the relay (`tls_and_mtls` in `docker_e2e` and `docker_publish`), QoS 0 overflow accounting, and exact EMQX 5.8.6 image pins are implemented and tested. Revisit the TLS relay when `rumqttc` releases a patched `rustls-webpki`.
- ADR-0013: explicit Parquet field IDs (`datasets[].fields[].id`) and identity level IDs (`datasets[].identity.<level>.id`) are in `thadam-config` and round-trip through the Parquet writer and reader (`thadam-segments`); unsigned reader support across downstream engines stays **[V]**.
- ADR-0020: the MSRV CI job (`.github/workflows/ci.yml` runs `cargo +1.89 check --workspace --exclude thadam-python --exclude thadam-iceberg --locked`), the `cargo deny` policy (including scoped `opcua`/`ros2-live` exceptions), and `ring` on native aarch64.
- ADR-0016 (name clearance): mitigated by `publish = false` and the provisional binary name.
- CI platform (github.com versus GitHub Enterprise) and ARM64 runner availability.
- The `[V]` items from the maturity review.
- ADR-0007 (ROS 2) and ADR-0008 (ClickHouse): Draft by design; M2 and P1 work.
- ADR-0011's `[OPEN]` wording for the power-loss exposure window, which can now be derived from S1.
- The `Value` layout size bound is tested in `thadam-core`; the D8 re-measurement on reference hardware remains open before publishing hardware numbers.

### Deferred from `product.md`

The following items sketched in `product.md` or `ARCHITECTURE.md` are deferred beyond the current pre-alpha scope:

- **OpenTelemetry trace export (`otlp`):** Prometheus metrics and structured JSON logs are implemented; distributed trace export is deferred (`product.md` sections 15 and 29).
- **`thadam inspect --suggest-schema`:** schema-inference CLI helper (`ARCHITECTURE.md` section 21); `thadam inspect wal|cursors|segments|sample` are implemented.
- **`thadam replay --speed`:** paced replay from the CLI (`product.md` section 25); `thadam replay` runs at disk speed today, while `mcap` sources support `pace`.
- **CLI `connectors`, `pipelines`, and `logs` subcommands:** `thadam schema` and the docs cover connector schemas; one process runs one pipeline and logs go to stderr (`product.md` section 29).
- **Admin config and replay endpoints:** configuration is loaded at startup (`deploy/apply-config.sh` handles validated restart with rollback) and replay runs via `thadam replay` (`product.md` section 29).
- **Application-level encryption at rest:** delegated to filesystem/LUKS encryption per ADR-0020 (`product.md` section 30).
- **WASM and in-chain ML inference transforms (Phase 6 steps S6/S7):** deferred by owner decision until a concrete user need appears (`docs/design/advanced-processing.md`).
- **Remaining Tier-2 / P2 connectors and managed destinations:** Delta Lake, Snowflake, BigQuery, Foxglove/ReductStore sinks, DDS-native / `zenoh-bridge-ros2dds` live ROS 2 modes, liveliness discovery, and serial RTU Modbus (`product.md` sections 13 and 14).

### What flips the project to fully READY

Blockers 1 and 2 are closed. The only remaining gate is, for the WAL format freeze only, blocker 3 (with blocker 4 as its dependency): the reference-hardware WAL bench run that gates the ADR-0004 format freeze and acceptance. With Part A in `product.md`, README, demo, and release wording follow it.

## 4. Caveats on the evidence

- **All spikes ran on an arm64 laptop.** S1 and S2 added a Linux VM in Docker Desktop whose flush semantics are unknown (Linux fsync took about 1.4 ms per group, against about 8 ms for `F_FULLFYNC` on the host). Ratios and macro differences (2 times or more) transfer; absolute numbers do not, and flush-bound macOS rows carry about plus or minus 40% run-to-run noise.
- **S1 did not exercise:** the prost frame payload, real device errors, directory-metadata loss, concurrent readers racing the writer or an unlink, multiple consumers, budget enforcement, backward clock steps, and cold-cache replay. kill -9 never tears a write and LazyFS does not drop metadata operations.
- **S2 simulated commit latency** (no real fsync), ran each throughput cell once, tested keepalive at one value (5 s), smoke-tested MQTT 5 on Mosquitto only, and covered QoS 2 on the happy path only. `rumqttc-next` was not run. EMQX results are from the 5.8.6 build, which is now the pinned line.
- **D8 measured records, not a pipeline.** Strings were copied rather than sliced from WAL bytes, records had 3 to 7 fields, and the real decoder will shift the shares of cost.
- **The ADR-0012 and ADR-0018 spikes** measured on macOS arm64 only. ADR-0012 covered the demo shape of configuration only (no large or deeply anchored files). ADR-0018's ARM criterion for CEL was not met and is no longer gating, because CEL is reserved.
- This pass changed documents and the workspace `rust-version` (1.88 to 1.89). Nothing under `spikes/` (other than two dated update notes in the ADR-0012 and ADR-0018 spike reports), `crates/`, `apps/`, `product.md`, or `thadam-assets/` was changed.

## 5. Corrections adopted from the spikes

For traceability, these design corrections are now recorded in the ADRs and `ARCHITECTURE.md`.

| Correction | Source | Recorded in |
|---|---|---|
| Frame header is 24 bytes with a reserved word that must be zero; CRC covers header bytes `[0, 20)` plus payload; no header CRC; no resynchronization in sealed segments; all-zero header is the end-of-data marker | S1 | ADR-0004, `ARCHITECTURE.md` section 6 |
| Open and recovery must not allocate space (lazy segment creation, delete before allocate) | S1 | ADR-0004, sections 6 and 11 |
| Re-verification after a failed fsync must not trust the page cache | S1 | ADR-0004, section 6 |
| No segment preallocation on macOS | S1 | ADR-0004, section 6 |
| Never reuse a sequence number at or below a durable cursor; readers and cursors bounded by the durable end; a bad segment header fails the open | S1 | ADR-0004, sections 6 and 11 |
| Default fsync `interval(10ms)` (provisional) instead of 50 ms | S1, S2 | ADR-0004, section 3 (D3), section 6 |
| In MQTT 3.1.1 the receive window is the broker's; the incoming queue is never bounded below it and never blocks the poll task | S2 | ADR-0006, ADR-0002, section 10 |
| Poll task only awaits `poll()`; never race it | S2 | ADR-0006, ADR-0002, section 10 |
| Connection-epoch-tagged ack handles; resubscribe on session loss | S2 | ADR-0006, ADR-0002, section 5 |
| `max_incoming_packet_size` from `max_message_bytes`; counted drop of oversized messages is not possible for MQTT | S2 | ADR-0006, ADR-0004, sections 6 and 10 |
| `rumqttc` feature set `use-rustls-no-provider` plus `tokio-rustls` with `ring` (S2). **Superseded 2026-10-07 by the ADR-0006 amendment:** `rumqttc` is built with no features and TLS is terminated in `thadam-mqtt` with `tokio-rustls` and `ring`, because every `rumqttc` TLS feature pins `rustls-webpki` 0.102.8 (four RustSec advisories, no fix) | S2, then `thadam-mqtt` | ADR-0006, section 16 |
| Parquet field IDs: envelope 1 to 99 fixed, identity levels 100 to 199 and declared fields 1000 to 4,194,303 explicit, nested IDs derived, never reused; `_wal_record_idx` not written (ID 13 reserved); unsigned default `Native` | `thadam-arrow` | ADR-0013, sections 8 and 22 (O4, O5) |
| WAL implementation deviations: frames-or-gap reader items, `Gap::Missing`, recovery always skips ahead past a cursor beyond the end, WAL root `data_dir/wal` with `wal.json` (the runtime owns `thadam.json`), cursor batching left to callers, newest segment never deleted, failpoints feature | `thadam-wal` | ADR-0004 "Implementation notes (PR6 and PR7)", section 6 |
| Parse budget 50,000 nodes and depth 32; anchors, aliases, merge keys, and custom tags rejected; `--allow-unresolved`; `thadam schema`; identity levels as an ordered list | `thadam-config` | ADR-0012 "PR5 outcomes", section 15 |
| One `EventBatch` per dataset; identity passed as an ordered list; `ChainSpec` built by the runtime; time domain defaults to `unknown`; skew and missing time flagged, never corrected | `thadam-transform` | Section 7 |
| D8: representation B behind an opaque `Record` with `ShapeKey` and positional accessors | D8 report, owner B6 | ADR-0001, section 4 |
| D9: extended values; typed-array ownership rule; 24-byte `Value` layout evaluated in `thadam-core` | D8 report, owner B6 | ADR-0001, section 4 |
| `serde-saphyr` with `strict_booleans: true`, duplicate keys an error, `reject_unsupported_tags: true`, tightened budget; MSRV 1.89 | ADR-0012 spike, owner B7 | ADR-0012, ADR-0020, section 16 |
| Structured predicates for filters, each with an exact CEL equivalent; `expr:` reserved; explicit integer literal types and `f32` rule | ADR-0018 spike, owner B7 | ADR-0018, section 15, `product-amendments.md` A.11 |
| EMQX pinned to the last open-source line (5.8.x); newer Enterprise-licensed images not used | S2, owner decision | ADR-0006, sections 10, 21, and 22 (O13) |

## 6. Verification performed in this pass

Historical: this is the verification of the original 2026-10-07 pass, when the workspace had 11 members (updated below for the 27-member workspace).

Run from the repository root on 2026-10-07, after raising `rust-version` in the root `Cargo.toml` from 1.88 to 1.89 (toolchain pinned by `rust-toolchain.toml`: rustc 1.99.0).

| Check | Result |
|---|---|
| `cargo fmt --check` | Pass |
| `cargo build --workspace --exclude thadam-python` | Pass |
| `cargo clippy --workspace --exclude thadam-python --all-targets -- -D warnings` | Pass, no issues |
| `cargo nextest run --workspace --exclude thadam-python` | Pass |
| `cargo deny check` | Pass (advisories ok, bans ok, licenses ok, sources ok). It ran outside the sandbox because it updates the advisory database under `~/.cargo`. |
| `scripts/check-deps.sh` | Pass (`check-deps: ok`) |
| `scripts/check-deps.sh --self-test` | Pass (14 violations detected in the fixture) |
| `cargo metadata --no-deps` | All 27 workspace members report `rust_version` 1.89 (except `thadam-iceberg`, which requires Rust 1.94 for upstream `iceberg` 0.10.1) |
| `.github/workflows/ci.yml` MSRV check | The `msrv` job runs `cargo +1.89 check --workspace --exclude thadam-python --exclude thadam-iceberg --locked`; full builds use the toolchain pinned in `rust-toolchain.toml`. |
| Spikes excluded from the root workspace | Unchanged from the earlier pass: `exclude = ["spikes"]`, each spike manifest declares its own `[workspace]`. |

The `clippy.toml` settings (`allow-unwrap-in-tests`, `allow-expect-in-tests`) are exercised across the workspace test suite.

## 7. Next pull requests

Historical: this section is the plan as written before the initial foundation PRs were committed. PR1 to PR18 (including PR11) are done; the text below is kept for the initial scope it recorded.

The repository had no commits of this work when this section was written: every file was untracked. Suggested commit order, all for the owner to approve and a maintainer to run (this pass did not stage, commit, or branch).

### PR2: docs baseline (done, `ba0bb03`)

Done (`ba0bb03`, after PR1 `079b2c6`). `product.md` is under version control, and the owner applied Part A (A.1 to A.9 and A.11) on 2026-10-13 (see `product-amendments.md`). `thadam-assets/` is tracked. `coding-agent-plan-prompts.md` was removed in the release-readiness hygiene pass.

### PR3: `thadam-core` model and errors (first code PR; starts once PR1 and PR2 are committed)

ADR-0001 is Accepted. Scope (the dependency rules in `scripts/check-deps.sh` apply: no Tokio as a direct dependency, no Arrow, `prost`, protocol, or YAML crates in `thadam-core`):

- **Time, quality, identity, positions.** `Timestamp(i64)`, `TimeDomain`, `Times` (source set once, ingest never null, corrected optional with a mandatory quality flag), `Quality` with the flag bitset, `AssetPath`, `WalPos` and `WalRange`.
- **Values and shapes.** The extended `Value` set including typed arrays and nested records; `Shape` with fingerprint; per-chain shape registry with size caps (`max_shapes`) and counted rejects.
- **Opaque `Record`** with `shape_key`, `get_at`, `set_at`, and slow-path `get` and `iter`; no public fields; documented clone semantics; the typed-array ownership rule.
- **`Event`, `EventBatch`, `RawMessage`, `RawBatch`** with private fields and a builder that enforces the invariants. Size budget: 128 bytes excluding heap.
- **Errors.** `ThadamError`, `Classify`, stable error codes, the retry classes.
- **`DiskBudget`** with per-category atomics.
- **Declared-field types** (open item O1) placed in `thadam-core` so `thadam-arrow` and `thadam-transform` do not depend on `thadam-config`.
- The **24-byte `Value` layout evaluation**, recorded in ADR-0001.

Tests: builder invariants (`source_time` immutable, `correct()` always sets a flag, no event without ingest time), the `Event` size budget, proptest (value round trip, widening never narrows, `explode`), error-code snapshot, `loom` on `DiskBudget`, shape cache behavior at 10,000 distinct shapes, time-domain and clock-step cases with a fake clock, and a test that fails if `Record` exposes a public field or an `Arc<Shape>` comparison. These are the first tests in the workspace, so the `clippy.toml` settings are exercised here.

Size: likely above the 600-line review target. If so, split along the line between "time, quality, identity, positions, errors, `DiskBudget`" (3a) and "values, shapes, `Record`, `Event`" (3b), so the D8 work gets its own review.

### PR4: core traits and `thadam-testkit` (ADR-0002 Accepted; follows PR3)

- **Traits and contexts.** `Source` (cancel-safe `next_batch`), `BatchSink`, `SegmentSink`, factories (`SourceFactory`, `BatchSinkFactory`, `SegmentSinkFactory`, `ServiceFactory`) taking `serde_json::Value`, `SourceContext`, `SinkContext`, `AckHandle`, `AckMode`, `SinkBatch` (closes O8, including how `BatchId` is formed and persisted), `Shutdown` (a `tokio-util` cancellation token is allowed in the closure but not as a direct `tokio` dependency), a clock abstraction.
- **`thadam-testkit`.** `MemorySource`, `MemoryBatchSink`, `MemorySegmentSink`, `FakeClock`, and the conformance suites: liveness (stall for three times the keepalive, no lost message), cancel-safety (random drops of `next_batch` and cancellation during connect, receive, and ack), ack ordering under randomized commit completion, epoch-scoped handles (the S2 E1 stale-ack scenario loses zero messages), the protocol task never awaiting the runtime, the lost-token watchdog, and panic isolation. The memory connectors must pass their own suites first.
- **Benchmarks.** Per-batch overhead of `async_trait` boxing and ack bookkeeping, and ack-tracker throughput at 100,000 acks per second.

### PR5: `thadam-config` (ADR-0012 Accepted; follows PR3)

`serde-saphyr` with `strict_booleans: true` behind `thadam-config` (MSRV 1.89), `thadam validate`, `thadam version`, the demo option list in `ARCHITECTURE.md` section 15, the explicit-`on_full` rule, the multi-error collection pass with the path-to-span index, and the ADR-0012 "PR5 implementation details". `serde_norway` is the documented fallback.

### After PR3 to PR5

PR6 and PR7 were gated on ADR-0004 (the S1 re-run) and were built ahead of it by the owner's choice (format unfrozen); PR8 follows PR3; PR9 follows PR8 (ADR-0018 Accepted); PR12 follows PR4 and the runtime (ADR-0006 Accepted, EMQX 5.8.x). The full sequence is `ARCHITECTURE.md` section 21.
