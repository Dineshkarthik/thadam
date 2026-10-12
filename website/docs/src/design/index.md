# Design and decisions

Thadam's design is record-first: decisions are accepted as ADRs, evidence comes from spike reports, and only accepted decisions are built on. This page indexes the design corpus; it links rather than copies, so the repository stays the source of truth. Links to repository documents point at the `main` branch on GitHub, because the design records are not part of this published site.

## Top-level design documents

| Document | What it is |
|---|---|
| [ARCHITECTURE.md](https://github.com/Dineshkarthik/thadam/blob/main/ARCHITECTURE.md) | The current-state architecture of the MVP; supersedes earlier, partly contradictory plans. |
| [product.md](https://github.com/Dineshkarthik/thadam/blob/main/product.md) | Product requirements and scope: vision, personas, use cases, roadmap. (Amended in places by the owner decisions below.) |
| [Readiness tracker](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/readiness.md) | Per-PR implementation status and the remaining readiness blockers. |
| [Owner decisions](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/owner-decisions.md) | The owner's decision register (scope, defaults, ADR acceptances), dated. |
| [Product amendments](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/product-amendments.md) | Approved amendments to `product.md` not yet applied to it. |
| [Version inventory](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/version-inventory.md) | Pinned versions of formats, dependencies, and tooling. |

## Architecture decision records

Naming: `NNNN-kebab-title.md`; status lifecycle Draft → Proposed → Accepted → Superseded (process: [ADR-0000](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0000-adr-process.md), itself Proposed). Only the project owner accepts an ADR.

| ADR | Status | Decision |
|---|---|---|
| [0000 — ADR process](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0000-adr-process.md) | Proposed | How decisions are recorded, accepted, and revisited. |
| [0001 — Canonical event model](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0001-canonical-event-model.md) | **Accepted** | Typed events: time domains, quality, identity, extended values, shape-shared records. |
| [0002 — Connector contract](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0002-connector-contract.md) | **Accepted** | Pull `Source` with liveness/cancel-safety clauses; split `BatchSink`/`SegmentSink`; epoch-scoped acks. |
| [0003 — Execution and process model](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0003-execution-and-process-model.md) | **Accepted** | Single process, per-sink chains, capture-first WAL (D1). |
| [0004 — Durable buffer](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0004-durable-buffer.md) | Draft | Custom segmented WAL, fsync policy, `on_full`. Draft until the S1 hardware re-run; format versioned but not frozen. |
| [0006 — MQTT client](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0006-mqtt-client.md) | **Accepted** | `rumqttc` behind `thadam-mqtt`, wrapper rules, in-process TLS relay, required broker settings, EMQX 5.8.x pin. |
| [0007 — ROS 2 strategy](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0007-ros2-strategy.md) | Draft (M2) | MCAP/rosbag2 offline first, live transport chosen by spike S3. |
| [0008 — ClickHouse sink](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0008-clickhouse-sink.md) | Draft (P1) | Batch sink, dedup probe, benchmarked client choice; starts after the M1 gate. |
| [0011 — Delivery guarantees and claims register](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0011-delivery-guarantees-and-claims-register.md) | **Accepted** | Per-hop guarantees, D4/D7/D17/D18/D21, and the claims register that gates all public wording. |
| [0012 — Config format and YAML crate](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0012-config-format-and-yaml-crate.md) | **Accepted** | `serde-saphyr` behind `thadam-config`, strict booleans, parse budget, rejected YAML features. |
| [0013 — Parquet segment layout](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0013-parquet-segment-layout.md) | **Accepted** | Wide tables per dataset, field IDs, finalize sequence, keys, catalog. |
| [0018 — Filter and expression language](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0018-filter-and-expression-language.md) | **Accepted** | Structured YAML predicates; CEL reserved; `expr:` reserved. |
| [0020 — Dependency security and toolchain policy](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0020-dependency-security-and-toolchain-policy.md) | **Accepted** | MSRV 1.89, pinned toolchain, rustls+`ring`, license allow-list, `cargo deny` bans. |
| 0005 Internal representation | Reserved | Folded into ADR-0001 (D8). |
| 0009 Object-store architecture | Reserved | Split across ADR-0011 (commit-then-ship) and ADR-0013 (keys); ARCHITECTURE section 8. |
| 0010 Python SDK boundary | Reserved | Post-MVP. |
| 0014 Time and clock semantics | Reserved | Folded into ADR-0001 (D15) and ADR-0013 (D14, D16). |
| 0015 Recording and replay format | Draft | WAL retention window; MCAP is export/import only; `thadam export --mcap` keeps original encodings and records gaps and provenance. |
| 0016 Naming and publishing | Reserved | Name clearance is pending; all crates are `publish = false`. |
| 0017 Plugin mechanism | Reserved | Compile-time features now; out-of-process later; Rust dylibs rejected. |
| 0019 Transform semantics | Reserved | Folded into ADR-0011 and ARCHITECTURE section 7. |

## Spike reports

Spikes are time-boxed experiments whose reports feed ADRs; the code is throwaway, the reports are evidence.

| Report | Feeds | Question answered |
|---|---|---|
| [S1 — WAL](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/S1-wal.md) | ADR-0004 | Custom segmented log vs SQLite vs redb: correctness under kill -9/LazyFS/torn tails, write amplification, recovery time, fsync behavior. |
| [S2 — MQTT](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/S2-mqtt.md) | ADR-0006, ADR-0011 | Ack-window throughput model, liveness under backpressure, `poll` cancel-safety, payload-stripped acks, persistent sessions, broker limits (Mosquitto 2.1.2, EMQX 5.8.6). |
| [D8 — Record representation](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/D8-record-representation.md) | ADR-0001 | Memory layout of the canonical record (representation B behind an opaque `Record`). |
| [ADR-0012 YAML spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/ADR-012-yaml.md) | ADR-0012 | YAML crate selection (`serde-saphyr`), strictness and budget measurements. |
| [ADR-0018 filter spike](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/spikes/ADR-018-filter.md) | ADR-0018 | Structured predicates vs an expression language (CEL evaluator cost on ARM). |

A caution from the readiness tracker: every spike ran on one Apple Silicon (arm64) laptop (S1/S2 added a Docker-Desktop Linux VM with unknown flush semantics). Ratios and macro differences transfer; absolute numbers do not, and re-runs on reference ARM and x86 hardware are required before the results back any published claim.
