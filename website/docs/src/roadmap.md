# Roadmap

The public sequence, organized into milestones (M1, M2, M3) and the product.md phase plan. Live per-component status is in the [readiness tracker](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/readiness.md) and ARCHITECTURE.md section 21.

## Milestones

### M1 — MVP v0.1 (in progress)

The reliability core: **MQTT source → capture WAL → transforms → local Parquet segments → S3-compatible shipping → replay**, plus chaos verification (kill -9, failpoint, and outage suites) and the first published benchmark numbers on x86_64 and ARM64. The M1 gate is the demo acceptance list: zero missing acknowledged samples, zero duplicate rows or objects across induced crashes, disk never above budget plus one segment.

Where it stands today: config, WAL, transforms, MQTT, the runtime, the CLI, Parquet segments, the object-store shipper, replay and inspection, the demo simulator, and the chaos suites and benchmark harness are built. Still open: the chaos verification run that the M1 gate needs, and the first published benchmark numbers, which wait on the ADR-0004 re-run on reference hardware.

### M2 — Robotics: ROS 2

Outside v0.1. Order: `thadam-ros2-msg` (schema-driven CDR decode) → **MCAP/rosbag2 offline ingestion** and `thadam export --mcap` (both done) → live-transport spike (rmw_zenoh over Zenoh, with a `zenoh-bridge-ros2dds` sidecar only if needed; pure-Rust DDS deferred) → live ROS 2 source.

Where it stands today: the `thadam-ros2-msg` decoder (`decode: ros2` in chains, schema store) and the MCAP source (`thadam export --mcap`, `examples/ros2-mcap-to-parquet`) are done, and the ROS 2 fuzz targets run in the `fuzz-smoke` CI job. The live-transport spike is complete. The live source (`kind: ros2_live`, rmw_zenoh transport) is available (experimental, behind the opt-in `ros2-live` feature); `zenoh-bridge-ros2dds` sidecar, liveliness discovery, and ARM reference-hardware numbers remain open.

### M3 — ClickHouse (P1)

The `thadam-clickhouse` sink is built, outside the M1 reliability gate: startup dedup probe, deterministic batch tokens, and it never creates or alters tables. Still open: the HTTP-vs-native client benchmark and the `edge-analytics` example. Deferral rule: if capacity tightens, the remaining M3 work moves out; nothing in M1 depends on it ([ClickHouse design](sinks/clickhouse.md)).

## Product phases (product.md section 32)

| Phase | Contents | Maps to |
|---|---|---|
| 0 — Foundations | Repository, conventions, event model, connector traits, CLI skeleton, observability, benchmark harness, docs, CI | available (workspace scaffold and design baseline) |
| 1 — MVP runtime | MQTT, disk buffer, basic transforms, metrics; object store; HTTP; replay | M1 gate open; HTTP source is built, outside the MVP scope |
| 2 — Robotics | ROS 2, DDS/Zenoh evaluation, trajectory/pose conventions, frame metadata, timestamps | M2: offline ROS 2 and live source (`ros2-live` feature, experimental) available; discovery and ARM numbers open |
| 3 — Industrial | OPC UA, CAN, Modbus, Sparkplug B decode: available (OPC UA behind the off-by-default `opcua` feature). OPC UA PubSub: evaluated, deferred until a user needs it | available; OPC UA PubSub deferred |
| 4 — Lakehouse | Iceberg, Parquet optimization, schema evolution, catalog integration | available (experimental, `iceberg` feature): registrar, schema evolution, catalog auth (with OAuth2 401 refresh) and mTLS; compaction and partitioning open |
| 5 — Fleet / platform | Remote configuration, fleet management, pipeline versioning, deployment tooling, centralized observability | partly done: pipeline versioning, deployment tooling, config apply with rollback; remote configuration and fleet management open |
| 6 — Advanced processing | Stateful windows, WASM operators, anomaly detection, lightweight inference, feature extraction | windows available (experimental); WASM and inference deferred until a user needs them |

## Deliberately not on the roadmap

From the MVP non-goals: GUI, cloud control plane, Kubernetes operator before the single-binary runtime is proven, joins, dynamic plugins, a schema-registry product, application-level encryption at rest (filesystem/LUKS is the documented answer), and publishing crates before [ADR-0016](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0016-naming-and-publishing.md) (Draft) is accepted. Re-evaluated as milestones close, not silently expanded.

## How to influence it

- Comment on the [issue tracker](https://github.com/Dineshkarthik/thadam/issues) with use cases (especially: which protocols, which sinks, what payload shapes).
- Performance claims land only with reproducible benchmarks; contributions of hardware (ARM edge boards, eMMC targets) or benchmark scenarios are disproportionately valuable ([contributing](https://github.com/Dineshkarthik/thadam/blob/main/CONTRIBUTING.md)).
