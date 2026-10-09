# Summary

[Introduction](introduction.md)

# Getting started

- [Installation](getting-started/installation.md)
- [Your first pipeline](getting-started/first-pipeline.md)
- [Validating configurations](getting-started/validating-configs.md)

# Concepts

- [Architecture overview](concepts/architecture-overview.md)
- [The canonical event model](concepts/event-model.md)
- [Capture-first WAL and replay](concepts/wal-and-replay.md)
- [Per-sink chains](concepts/per-sink-chains.md)
- [Delivery guarantees](concepts/delivery-guarantees.md)
- [Backpressure and disk budgets](concepts/backpressure-and-disk-budgets.md)

# Configuration reference

- [The configuration file](configuration/index.md)
- [`pipeline`](configuration/pipeline.md)
- [`sources` and `sinks`](configuration/sources-sinks.md)
- [`wal` and `quarantine`](configuration/wal.md)
- [`datasets`](configuration/datasets.md)
- [`transforms` and predicates](configuration/transforms.md)
- [`runtime` and `observability`](configuration/runtime-observability.md)

# Sources

- [HTTP](sources/http.md)
- [gRPC](sources/grpc.md)
- [MQTT](sources/mqtt.md)

# Sinks

- [Parquet segments](sinks/parquet.md)
- [Object storage](sinks/object-store.md)
- [ClickHouse](sinks/clickhouse.md)

# Transforms

- [Transform reference](transforms/index.md)

# Operations

- [Data directory layout](operations/data-directory.md)
- [Crash recovery and `--repair`](operations/crash-recovery.md)
- [Retention and disk budgets](operations/retention.md)
- [Admin API](operations/admin-api.md)
- [Metrics reference](operations/metrics.md)
- [Logging](operations/logging.md)
- [Exit codes](operations/exit-codes.md)
- [Error code reference](operations/error-codes.md)

# Reference

- [CLI reference](cli/index.md)
- [Design and decisions](design/index.md)
- [Roadmap](roadmap.md)
- [FAQ](faq.md)
- [Glossary](glossary.md)
