# Roadmap

The public sequence, from the owner decisions of 2026-10-07 (M1/M2/M3 milestones) and the product.md phase plan. The per-PR live status is in the [readiness tracker](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/readiness.md) and ARCHITECTURE.md section 21.

## Milestones

### M1 — MVP v0.1 (in progress)

The reliability core: **MQTT source → capture WAL → transforms → local Parquet segments → S3-compatible shipping → replay**, plus chaos verification (kill -9, failpoint, and outage suites) and the first published benchmark numbers on x86_64 and ARM64. The M1 gate is the demo acceptance list: zero missing acknowledged samples, zero duplicate rows or objects across induced crashes, disk never above budget plus one segment.

Where it stands today: config, WAL, transforms, MQTT, the runtime core, and the CLI exist; Parquet segments and the object-store shipper are landing now (PR14–PR16); replay, the demo levels, and the chaos/benchmark gate follow (PR17–PR18).

### M2 — Robotics: ROS 2

Not in v0.1 (owner decision). Order: `thadam-ros2-msg` (schema-driven CDR decode) → **MCAP/rosbag2 offline ingestion** and `thadam export --mcap` → spike S3 chooses the live transport (Zenoh-native via `roslibrust`/`zenoh` first, `zenoh-bridge-ros2dds` sidecar for DDS robots second, `r2r` as the rcl fallback; pure-Rust DDS deferred) → live ROS 2 source.

### M3 — ClickHouse (P1)

Starts only after the M1 gate: the B8 HTTP-vs-native benchmark, `thadam-clickhouse` (startup dedup probe, deterministic batch IDs, never creates tables), and the `edge-analytics` example. Deferral rule: if capacity tightens, the whole block moves out — nothing in M1 depends on it ([ClickHouse design](sinks/clickhouse.md)).

## Product phases (product.md section 32)

| Phase | Contents | Maps to |
|---|---|---|
| 0 — Foundations | Repository, conventions, event model, connector traits, CLI skeleton, observability, benchmark harness, docs, CI | done (PR1–PR2) |
| 1 — MVP runtime | MQTT, disk buffer, basic transforms, metrics; object store; HTTP; replay | M1 (HTTP source moved out of MVP scope) |
| 2 — Robotics | ROS 2, DDS/Zenoh evaluation, trajectory/pose conventions, frame metadata, timestamps | M2 |
| 3 — Industrial | OPC UA, CAN, Modbus; Sparkplug B / OPC UA PubSub evaluation | after M2 |
| 4 — Lakehouse | Iceberg, Parquet optimization, schema evolution, catalog integration | after M3 (field IDs in ADR-0013 keep the `fast_append` path cheap) |
| 5 — Fleet / platform | Remote configuration, fleet management, pipeline versioning, deployment tooling, centralized observability | later |
| 6 — Advanced processing | Stateful windows, WASM operators, anomaly detection, lightweight inference, feature extraction | later |

## Deliberately not on the roadmap

From the MVP non-goals: GUI, cloud control plane, Kubernetes operator before the single-binary runtime is proven, MQTT publishing, windowing/aggregation, joins/watermarks, dynamic plugins, a schema-registry product, application-level encryption at rest (filesystem/LUKS is the documented answer), and publishing crates before the naming ADR (0016). Re-evaluated as milestones close, not silently expanded.

## How to influence it

- Comment on the [issue tracker](https://github.com/Dineshkarthik/thadam/issues) with use cases (especially: which protocols, which sinks, what payload shapes).
- Performance claims land only with reproducible benchmarks; contributions of hardware (ARM edge boards, eMMC targets) or benchmark scenarios are disproportionately valuable ([contributing](https://github.com/Dineshkarthik/thadam/blob/main/CONTRIBUTING.md)).
