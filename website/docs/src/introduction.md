# Introduction

**Thadam is the open data plane for robotics.** It connects robotic and industrial data sources to modern data platforms with edge-native processing, durable buffering, and flexible sinks.

The name comes from Tamil <span lang="ta">தடம்</span> (thadam): track, trail, trace, path.

Concretely, Thadam is a single Rust binary that runs next to your data producers — on an industrial PC, a robot's edge computer, a factory server, or a laptop — and takes responsibility for the whole journey of a telemetry message:

1. **Capture.** Messages from sources such as MQTT brokers are appended to a local, durable write-ahead log (WAL) and flushed to disk *before* the source is acknowledged. Acknowledged data survives a process crash when `fsync` is enabled ([claims register](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0011-delivery-guarantees-and-claims-register.md)).
2. **Process.** Each sink reads the log independently: messages are decoded, normalized into typed, declared datasets *while keeping device identity, time domains, and data quality as first-class columns*.
3. **Deliver.** Processed datasets are written as Parquet segment files and shipped to S3-compatible object storage. ClickHouse is the first analytical database sink (P1, <span class="pill">Available</span>, outside the core v0.1 reliability gate). While a downstream system is unavailable, data stays in the local log within the configured disk budget, and delivery resumes when it returns; the [delivery guarantees](concepts/delivery-guarantees.md) page gives the conditions.

Thadam is open source (Apache-2.0), Rust-first, and designed for the edge: unreliable networks, limited CPU and memory, intermittent power, and long periods of disconnection. Durability is a design input, not a bolt-on: the delivery guarantees Thadam makes (and the ones it explicitly refuses to make) are recorded in [ADR-0011](design/index.md) and every public claim is gated on benchmark or spike evidence.

## This book

This book is the user documentation for operators and integrators. It covers:

- [Getting started](getting-started/installation.md): build Thadam and set up a broker.
- [Concepts](concepts/architecture-overview.md): the event model, the capture-first WAL, per-sink chains, and the delivery guarantees.
- [Configuration reference](configuration/index.md): every configuration key with types, defaults, and constraints.
- [Sources](sources/mqtt.md) and [sinks](sinks/parquet.md): connector-specific options and operational notes.
- [Operations](operations/data-directory.md): the data directory, crash recovery, retention, the admin API, metrics, logging, exit codes, and error codes.
- [CLI reference](cli/index.md): every subcommand and flag.

Design documents — `ARCHITECTURE.md`, `product.md`, the ADRs, and the spike reports — are linked from [Design and decisions](design/index.md) rather than copied, so this book stays about *using* Thadam.

## What Thadam is not

Thadam's scope is defined as much by what it refuses to be as by what it does. From the product scope ([product.md](https://github.com/Dineshkarthik/thadam/blob/main/product.md) sections 6 and 33), Thadam is deliberately **not**:

- a robot operating system, a robot control or motion-planning stack, or safety-certified control software — Thadam moves data *about* robots, it does not drive them;
- a full SCADA system, an HMI, or an asset-maintenance-management product;
- a general-purpose workflow/BPM orchestrator, a data warehouse, a BI tool, or a full ML platform;
- a large distributed stream-processing cluster — Thadam complements Kafka, Flink, and Spark rather than replacing them;
- a video storage or video analytics platform (events may *reference* large media as object URIs, but media bytes do not travel in the telemetry path);
- a managed cloud platform, a GUI, or proprietary vendor lock-in.

For the current milestone (M1 / MVP v0.1), the reliability gate covers MQTT, the capture WAL, Parquet, S3-compatible storage, and replay. Other sources, sinks, and processing steps are built outside that gate, and some are experimental; the [roadmap](roadmap.md) gives each one's status. Still not built: Delta Lake, hot configuration reload, and dynamic plugins.

## Project status

Thadam is in beta. The status banner in the repository [README](https://github.com/Dineshkarthik/thadam/blob/main/README.md) summarizes what works, and the detailed per-component state is tracked in the [readiness tracker](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/readiness.md). Each page marks its status with the same labels as the homepage: <span class="pill">Available</span> <span class="pill pill-progress">In progress</span> <span class="pill pill-planned">Planned</span>. Wherever a feature described here is not yet wired end to end, its page says **Planned** or **In progress** explicitly.
