# Design and decisions

Thadam's design is record-first: decisions are accepted as ADRs, evidence comes from spike reports, and only accepted decisions are built on. This page indexes the design corpus; it links rather than copies, so the repository stays the source of truth. Links to repository documents point at the `main` branch on GitHub, because the design records are not part of this published site.

## Top-level design documents

| Document | What it is |
|---|---|
| [ARCHITECTURE.md](https://github.com/Dineshkarthik/thadam/blob/main/ARCHITECTURE.md) | The current-state architecture of the MVP; supersedes earlier, partly contradictory plans. |
| [product.md](https://github.com/Dineshkarthik/thadam/blob/main/product.md) | Product requirements and scope: vision, personas, use cases, roadmap. (Amended in places by the decision records below.) |
| [Readiness tracker](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/readiness.md) | Per-component implementation status and the remaining readiness blockers. |
| [Decision register](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/owner-decisions.md) | The maintainer's decision register (scope, defaults, ADR acceptances), dated. |
| [Product amendments](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/product-amendments.md) | Scope amendments to `product.md` (Part A applied; Parts B and C proposed). |
| [Version inventory](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/version-inventory.md) | Pinned versions of formats, dependencies, and tooling. |

## Architecture decision records

Naming: `NNNN-kebab-title.md`; status lifecycle Draft → Proposed → Accepted → Superseded (process: [ADR-0000](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0000-adr-process.md), itself Proposed). Only the maintainer accepts an ADR.

| ADR | Status | Decision |
|---|---|---|
| [0000 — ADR process](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0000-adr-process.md) | Proposed | How decisions are recorded, accepted, and revisited. |
| [0001 — Canonical event model](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0001-canonical-event-model.md) | **Accepted** | Typed events: time domains, quality, identity, extended values, shape-shared records. |
| [0002 — Connector contract](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0002-connector-contract.md) | **Accepted** | Pull `Source` with liveness/cancel-safety clauses; split `BatchSink`/`SegmentSink`; epoch-scoped acks. |
| [0003 — Execution and process model](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0003-execution-and-process-model.md) | **Accepted** | Single process, per-sink chains, capture-first WAL. |
| [0004 — Durable buffer](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0004-durable-buffer.md) | Draft | Custom segmented WAL, fsync policy, `on_full`. Draft until the WAL hardware re-run; format versioned but not frozen. |
| [0006 — MQTT client](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0006-mqtt-client.md) | **Accepted** | `rumqttc` behind `thadam-mqtt`, wrapper rules, in-process TLS relay, required broker settings, EMQX 5.8.x pin. |
| [0007 — ROS 2 strategy](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0007-ros2-strategy.md) | Draft | MCAP/rosbag2 offline first, live transport chosen by the ROS 2 live spike. |
| [0008 — ClickHouse sink](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0008-clickhouse-sink.md) | Draft (P1) | Batch sink, dedup probe, benchmarked client choice; outside the core v0.1 reliability gate. |
| [0011 — Delivery guarantees and claims register](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0011-delivery-guarantees-and-claims-register.md) | **Accepted** | Per-hop guarantees (`on_full`, ack modes, deduplication, commit-then-ship, memory bounds), and the claims register that gates all public wording. |
| [0012 — Config format and YAML crate](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0012-config-format-and-yaml-crate.md) | **Accepted** | `serde-saphyr` behind `thadam-config`, strict booleans, parse budget, rejected YAML features. |
| [0013 — Parquet segment layout](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0013-parquet-segment-layout.md) | **Accepted** | Wide tables per dataset, field IDs, finalize sequence, keys, catalog. |
| [0018 — Filter and expression language](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0018-filter-and-expression-language.md) | **Accepted** | Structured YAML predicates; CEL reserved; `expr:` reserved. |
| [0020 — Dependency security and toolchain policy](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0020-dependency-security-and-toolchain-policy.md) | **Accepted** | MSRV 1.89 (`thadam-iceberg` 1.94), pinned toolchain, rustls+`ring`, license allow-list, `cargo deny` bans. |
| 0005 Internal representation | Reserved | Folded into ADR-0001. |
| 0009 Object-store architecture | Reserved | Split across ADR-0011 (commit-then-ship) and ADR-0013 (keys); ARCHITECTURE section 8. |
| 0010 Python SDK boundary | Reserved | Post-MVP. |
| 0014 Time and clock semantics | Reserved | Folded into ADR-0001 and ADR-0013. |
| [0015 — Recording and replay format](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0015-recording-and-replay-format.md) | Draft | WAL retention window; MCAP is export/import only; `thadam export --mcap` keeps original encodings and records gaps and provenance. |
| [0016 — Naming and publishing](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0016-naming-and-publishing.md) | Accepted | Name kept (no trademark search done); the CLI is the `thadam` crate; 23 crates go to crates.io and the Python package to PyPI. |
| 0017 Plugin mechanism | Reserved | Compile-time features now; out-of-process later; Rust dylibs rejected. |
| 0019 Transform semantics | Reserved | Folded into ADR-0011 and ARCHITECTURE section 7. |

## Spike reports

Spikes are time-boxed experiments whose reports feed ADRs; the code is throwaway, the reports are evidence.

| Report | Feeds | Question answered |
|---|---|---|
| [WAL spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/S1-wal.md) | ADR-0004 | Custom segmented log vs SQLite vs redb: correctness under kill -9/LazyFS/torn tails, write amplification, recovery time, fsync behavior. |
| [MQTT spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/S2-mqtt.md) | ADR-0006, ADR-0011 | Ack-window throughput model, liveness under backpressure, `poll` cancel-safety, payload-stripped acks, persistent sessions, broker limits (Mosquitto 2.1.2, EMQX 5.8.6). |
| [ROS 2 live spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/S3-ros2-live.md) | ADR-0007 | Live ROS 2 transport over `rmw_zenoh` (`zenoh` 1.10.1), key expressions, 33-byte publisher attachments, and `transient_local` late-join queries on Jazzy and Lyrical. |
| [Sparkplug B and OPC UA PubSub spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/sparkplug-b-and-opcua-pubsub.md) | Phase 3 | Sparkplug B `spBv1.0` protobuf decoding, BIRTH/DEATH alias resolution, and OPC UA PubSub evaluation. |
| [Record representation](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/D8-record-representation.md) | ADR-0001 | Memory layout of the canonical record (shape-shared positional records behind an opaque `Record`). |
| [ADR-0012 YAML spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/ADR-012-yaml.md) | ADR-0012 | YAML crate selection (`serde-saphyr`), strictness and budget measurements. |
| [ADR-0018 filter spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/ADR-018-filter.md) | ADR-0018 | Structured predicates vs an expression language (CEL evaluator cost on ARM). |

A caution from the readiness tracker: every spike ran on an arm64 laptop (the WAL and MQTT spikes added a Linux VM in Docker with unknown flush semantics). Ratios and macro differences transfer; absolute numbers do not, and re-runs on reference ARM and x86 hardware are required before the results back any published claim.
