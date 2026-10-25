# FAQ

## Is Thadam production-ready?

No — it is in beta. The architecture is settled, the core crates are tested, and `thadam run` executes a pipeline end to end. The WAL's on-disk format is not frozen yet: it freezes at the v0.1.0 tag, after the ADR-0004 re-run on reference hardware. See the [status banner](https://github.com/Dineshkarthik/thadam/blob/main/README.md) and [roadmap](roadmap.md).

## Does Thadam guarantee exactly-once delivery?

No, and it will not claim to. Exactly-once is a sink-specific capability, not a universal property (product.md section 27). Thadam's defaults are at-least-once capture plus durability; "effectively-once" to a sink is possible only with specific downstream settings (for example a replicated ClickHouse table or a deduplication window), and Thadam probes and reports the effective class rather than assuming it. The binding wording is the [claims register](concepts/delivery-guarantees.md).

## What happens when the disk fills?

Whatever your [`wal.on_full`](configuration/wal.md#on_full-policy) says — and there is deliberately no default. `block` stalls sources and moves loss into the broker's queue; `drop_oldest` evicts old segments with the loss counted and surfaced as gaps. This is the only place inside Thadam where acknowledged data can be lost. Details: [backpressure and disk budgets](concepts/backpressure-and-disk-budgets.md).

## What happens when the network is down for hours?

Capture continues into the local WAL within its disk budget; delivery stalls and retries with backoff. When connectivity returns, the WAL drains and uploads resume. Dimension the budget for the longest outage you need to absorb ([retention sizing](operations/retention.md#sizing-sketch)) and size the broker's session queue for the downtime when Thadam is the side that is down.

## What throughput can I expect?

Per connection, roughly `broker inflight window ÷ (commit latency + RTT)` with `ack_mode: on_commit` — about 200–320 msg/s with default broker windows at a 100 ms commit, about 1,700 msg/s with Mosquitto's default window at the provisional 10 ms group commit (MQTT spike; measured on one laptop, ratios transfer, absolute numbers need your environment). The [throughput ceiling](concepts/delivery-guarantees.md#throughput-ceiling) page lists the levers. Published cross-architecture benchmark results arrive with the v0.1 reliability gate; nothing else is claimed.

## Why another data pipeline instead of Kafka Connect / NiFi / a vendor gateway?

Those are strong systems; the gap Thadam aims at is the combination, in one small Rust binary: robotics-aware semantics (time domains, quality, identity as data), edge-native reliability (durable local buffer, replay, explicit loss behavior), and modern analytical sinks (Parquet/object storage, ClickHouse). Thadam complements Kafka/Flink-style backbones rather than replacing them (product.md section 34).

## Does Thadam control my robot or carry commands?

No. Thadam may transport command/event-like messages only where explicitly configured, and it does not present itself as safety-critical control infrastructure. Motion planning, control loops, and safety systems are out of scope ([what Thadam is not](introduction.md#what-thadam-is-not)).

## Which protocols and sinks work today?

Sources: MQTT (3.1.1; v5 experimental), HTTP, gRPC, Modbus TCP, SocketCAN (Linux only), OPC UA (Cargo feature `opcua`), ROS 2 MCAP (offline ingestion), and live ROS 2 over `rmw_zenoh` (Cargo feature `ros2-live`, experimental), with JSON, ROS 2 CDR, and Sparkplug B decoding. Sinks: Parquet (local segments), S3-compatible object storage shipping ([`thadam-object-store`](sinks/object-store.md)), Apache Iceberg table registration (Cargo feature `iceberg`, experimental, P2), NATS JetStream, Apache Kafka, MQTT publish, and the ClickHouse batch sink (P1). The [roadmap](roadmap.md) has the full sequence.

## Can I run Thadam on a Raspberry Pi / Jetson / Mac?

Linux ARM64 and x86_64 are supported and are the only durability targets, on industrial SSD or eMMC that honors flush; SD cards are not a supported target for durability claims. macOS builds work for development but carry no durability claims. Reference-hardware validation (x86 NVMe, ARM eMMC, a Jetson/Pi class board) is an open readiness item.

## How do I keep credentials out of the config file?

`${env:NAME}` or `${file:/absolute/path}` interpolation ([secrets](configuration/index.md#secrets)): values are redacted from logs and diagnostics, hashed as written so rotation does not churn the config hash, and the MQTT password specifically uses `password_file`. Container secret mounts (`/run/secrets/…`) work well, as the demo shows.

## Where is the Python SDK?

The SDK is published on PyPI as `thadam` (version `0.1.0b2`, matching Cargo `0.1.0-beta.2`) and documented on the [Python SDK](sdk/python.md) page. Install it with `pip install thadam` (prebuilt `abi3` wheels for Linux glibc and musl on `x86_64`/`aarch64` and macOS `arm64`/`x86_64`; source distribution for other platforms), or build it from `sdk/python/` in a checkout with `maturin develop`. The PyPI package is the Python library only; install the `thadam` CLI with `cargo install thadam --version 0.1.0-beta.2 --locked`.
