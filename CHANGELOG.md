# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the project is pre-alpha (pre-1.0), any release may contain breaking changes; versioned on-disk formats refuse unknown versions rather than guessing.

## [Unreleased]

Thadam is early in its first milestone (M1 / MVP v0.1). No releases exist yet; this section summarizes what the development history has built so far. Detailed status: [docs/design/readiness.md](docs/design/readiness.md) and ARCHITECTURE.md section 21.

### Added

- **Workspace and guardrails**: eleven-member Cargo workspace (ten library crates plus the CLI), MSRV Rust 1.89, pinned toolchain, `cargo deny` policy, dependency-direction guard, nextest, CI on Linux x86_64 and ARM64.
- **Design corpus**: `ARCHITECTURE.md`, the ADR set in `docs/design/adr/` (nine accepted on 2026-10-07, including the delivery guarantees and claims register ADR-0011), owner-decision and readiness registers, and five spike reports (S1 WAL, S2 MQTT, D8 record representation, config YAML, filter language).
- **`thadam-core`**: canonical event model (times with time domains, quality status/flags, hierarchical identity, extended values, shape-shared positional records), error model with stable codes, connector traits (`Source`, `BatchSink`, `SegmentSink`), disk budget, conformance-test utilities in `thadam-testkit`.
- **`thadam-config`**: config `version: 1` — YAML loading with a strict dialect (strict booleans; no anchors/aliases/merge keys/tags; parse budget; duplicate-key errors), `${env:}`/`${file:}` secret interpolation with redaction, multi-error diagnostics with paths and line numbers, connector-kind validation through the registry, config hash (algorithm v1), memory estimate, JSON Schema export, Parquet field-ID and identity-level-ID assignment, per-sink WAL consumer policies (`required`, `max_lag`, `start_from`).
- **`thadam-wal`**: custom segmented log — one-writer group commit, `always`/`interval`/`never` fsync policies, durable cursors, retention and `on_full` (`block`/`drop_oldest`), crash recovery with torn-tail truncation, gaps surfaced to readers. **On-disk format v1 is versioned but not frozen** (ADR-0004 pending a reference-hardware re-run).
- **`thadam-transform`**: synchronous pure chains — router, JSON decode, event IDs (WAL-derived or content-derived `event_id.key`), message-level dedup with warm start, `explode` with parent paths, UCUM unit conversion (element-wise on arrays), time extraction with domains and skew flagging; steps `validate`, `on_change`, `filter`, `sample`, `enrich`, `rename`, `project`, `cast` (all config-reachable).
- **`thadam-runtime`**: source/chain/sink drivers, supervisor with component states, ordered shutdown and startup recovery, `thadam.json`/`wal.json` identity split, quarantine routing, primary-chain metric attribution.
- **`thadam-mqtt`**: MQTT 3.1.1 source — persistent sessions, acknowledge-after-commit with ordered acks, epoch-scoped ack handles, session-loss detection with resubscribe, byte-accounted queue with disconnecting guard, QoS 0 counted drops, in-process TLS/mTLS relay with rustls+`ring` (client library built without TLS features).
- **`thadam-arrow`**: event-to-Arrow encoding with envelope/lineage columns and stable Parquet field IDs (ADR-0013 decisions: unsigned default `Native`, list element name, 16-byte schema fingerprint).
- **CLI (`thadam`)**: `validate` (with `--allow-unresolved`), `schema`, `version`, `run` (graceful signal handling, admin override flags, `--repair`), `status` against the admin API.
- **Admin API and observability**: `/healthz`, `/readyz`, `/metrics` (OpenMetrics), `/status` (components, cursors, WAL, budgets); Prometheus metrics; structured lossy logging.
- **Documentation**: README with branding and status banner, this changelog, contributing and security policies, issue/PR templates, and the mdBook user documentation under `docs/`.
- **Demo configuration**: `examples/edge-capture/thadam/pipeline.yaml` with six datasets, transforms, quarantine, and an S3-compatible target.

### Fixed

- **`thadam-config`**: the config now accepts all eight transform kinds the engine implements — `sample`, `enrich`, `rename`, `project`, and `cast` are added to `validate`, `on_change`, and `filter`, using the engine's option names.
- **`thadam-config`**: the config gains the engine's `null:` predicate test, the `on_change` `{ field, deadband }` entry form, and `unit: rfc3339` with `default_offset` for dataset `time`.
- **`thadam-config`**: `dedup` without `event_id.key` is now a validation error (`config.invalid`); it was a warning. This matches ADR-0011 D17.
- **`thadam run`**: the logging guard now stays alive for the whole run; application log lines after startup are no longer silently dropped.
- **Log-format spellings** are unified on `json`/`pretty`: config `observability.logs.format` takes those values with `text` accepted as a deprecated alias for `pretty` (validation warns), and `thadam run` without `--log-format` uses the config value (default `json`) instead of the previously hardcoded `pretty`.

### In progress (not yet usable)

- **Parquet segment sink and object-store shipper** (`thadam-segments`, `thadam-object-store`): the `parquet` and `object_store` kinds are registered as placeholder factories, so `thadam run` cannot yet build a pipeline containing a sink. Lands with PR14–PR16.
- **Replay and inspect** (`thadam replay`, `thadam inspect`), demo automation, chaos suites, and published benchmark results (PR17–PR18).

### Not started

- ClickHouse sink (P1 after the M1 gate; ADR-0008 Draft), ROS 2 (M2; ADR-0007 Draft), Python SDK, plugin mechanism, HTTP/gRPC sources, windowing/aggregation.

### Known issues

_None known at this time._
