# Rust crates

**Status: Available.** All 22 Thadam library crates and the `thadam` CLI are published on [crates.io](https://crates.io/crates/thadam) as `0.1.0-beta.2` with API documentation on [docs.rs](https://docs.rs/thadam-runtime), and can be embedded in your own Rust binary or service.

## Version pinning and MSRV

Pin exact versions (`=0.1.0-beta.2`) when depending on Thadam crates. Cargo's default caret requirement (`^0.1.0-beta.2`) on a `0.1.0` pre-release also accepts later `0.1.0` betas, and public APIs can change between pre-releases until the compatibility policy is written.

- **MSRV:** Rust **1.89** across the workspace; `thadam-iceberg` requires Rust **1.94**.
- Add the runtime and only the connector crates your binary uses:

```bash
cargo add thadam-runtime@=0.1.0-beta.2 \
  thadam-config@=0.1.0-beta.2 \
  thadam-core@=0.1.0-beta.2 \
  thadam-mqtt@=0.1.0-beta.2 \
  thadam-segments@=0.1.0-beta.2
```

## Crate map

| Crate | What it is | API docs |
| --- | --- | --- |
| **Core, runtime, and encoding** | | |
| `thadam-core` | Canonical in-memory event model, value types, and connector traits | [docs.rs/thadam-core](https://docs.rs/thadam-core) |
| `thadam-config` | YAML pipeline configuration loading, interpolation, validation, and JSON Schema | [docs.rs/thadam-config](https://docs.rs/thadam-config) |
| `thadam-wal` | Capture-first write-ahead log with segmented files, durable cursors, and retention | [docs.rs/thadam-wal](https://docs.rs/thadam-wal) |
| `thadam-transform` | Synchronous transform chain from raw WAL frames to typed event batches | [docs.rs/thadam-transform](https://docs.rs/thadam-transform) |
| `thadam-runtime` | Pipeline assembly, driver tasks, supervision, recovery, and replay runtime | [docs.rs/thadam-runtime](https://docs.rs/thadam-runtime) |
| `thadam-arrow` | Encoding of dataset events into Apache Arrow record batches with stable field IDs | [docs.rs/thadam-arrow](https://docs.rs/thadam-arrow) |
| **Sinks and storage** | | |
| `thadam-segments` | Parquet segment sink, CRC-checked segment catalog, and quarantine writer | [docs.rs/thadam-segments](https://docs.rs/thadam-segments) |
| `thadam-object-store` | Shipper of finalized Parquet segments to S3-compatible object storage | [docs.rs/thadam-object-store](https://docs.rs/thadam-object-store) |
| `thadam-clickhouse` | ClickHouse native-block batch sink connector with schema validation | [docs.rs/thadam-clickhouse](https://docs.rs/thadam-clickhouse) |
| `thadam-nats` | NATS JetStream batch sink connector with message deduplication headers | [docs.rs/thadam-nats](https://docs.rs/thadam-nats) |
| `thadam-kafka` | Pure-Rust Apache Kafka batch sink connector with TLS and SASL | [docs.rs/thadam-kafka](https://docs.rs/thadam-kafka) |
| `thadam-iceberg` | Apache Iceberg table registrar for uploaded Parquet segments *(experimental; MSRV 1.94)* | [docs.rs/thadam-iceberg](https://docs.rs/thadam-iceberg) |
| **Sources** | | |
| `thadam-mqtt` | MQTT source connector with persistent sessions, TLS, and ack-after-commit (also provides the `mqtt_publish` sink) | [docs.rs/thadam-mqtt](https://docs.rs/thadam-mqtt) |
| `thadam-http` | HTTP ingest server and polling source connector with TLS | [docs.rs/thadam-http](https://docs.rs/thadam-http) |
| `thadam-grpc` | gRPC ingest server source connector with TLS and ack-after-commit | [docs.rs/thadam-grpc](https://docs.rs/thadam-grpc) |
| `thadam-modbus` | Modbus TCP register and coil polling source connector | [docs.rs/thadam-modbus](https://docs.rs/thadam-modbus) |
| `thadam-can` | SocketCAN frame capture and DBC signal decoding source connector *(Linux only for SocketCAN capture)* | [docs.rs/thadam-can](https://docs.rs/thadam-can) |
| `thadam-opcua` | Pure-Rust OPC UA subscription and polling client source connector | [docs.rs/thadam-opcua](https://docs.rs/thadam-opcua) |
| `thadam-mcap` | Resumable MCAP and rosbag2 file replay source connector | [docs.rs/thadam-mcap](https://docs.rs/thadam-mcap) |
| `thadam-ros2-live` | Live ROS 2 topic source connector over `rmw_zenoh` *(experimental)* | [docs.rs/thadam-ros2-live](https://docs.rs/thadam-ros2-live) |
| **Decoders** | | |
| `thadam-ros2-msg` | Transport-independent ROS 2 `.msg` schema parser and XCDR1 payload decoder | [docs.rs/thadam-ros2-msg](https://docs.rs/thadam-ros2-msg) |
| `thadam-sparkplug` | Sparkplug B wire schema, topic namespace parser, and stateful BIRTH/alias/sequence decoder | [docs.rs/thadam-sparkplug](https://docs.rs/thadam-sparkplug) |

## Embedding a pipeline

Load and interpolate a YAML configuration file with `thadam_config::load_file`. Construct a `thadam_runtime::Registry` and register only the connector factories your binary needs — connector crates never depend on the runtime, so unused protocols stay out of your dependency graph. Pass the spec and registry to `Pipeline::build`, which validates every connector's configuration and compiles each sink's transform chain. Drive `pipeline.run(shutdown)` on Tokio with a `thadam_core::Shutdown` handle triggered on Ctrl-C or your application's own stop signal.

`Cargo.toml` dependencies (from [`examples/embed-rust`](https://github.com/Dineshkarthik/thadam/tree/main/examples/embed-rust)):

```toml
[dependencies]
thadam-config = "=0.1.0-beta.2"
thadam-core = "=0.1.0-beta.2"
thadam-mqtt = "=0.1.0-beta.2"
thadam-runtime = "=0.1.0-beta.2"
thadam-segments = "=0.1.0-beta.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

`src/main.rs`:

```rust,no_run
use std::path::PathBuf;
use std::process::ExitCode;

use thadam_config::{ProcessEnv, load_file};
use thadam_core::Shutdown;
use thadam_mqtt::MqttSourceFactory;
use thadam_runtime::{ExitStatus, LogFormat, Pipeline, Registry, RuntimeOptions, init_logging};
use thadam_segments::{ParquetSegmentSinkFactory, QuarantineSinkFactory};

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("usage: thadam-embed-example <pipeline.yaml>");
        return ExitCode::from(ExitStatus::InvalidConfig.code());
    };
    let spec = match load_file(&path, &ProcessEnv) {
        Ok(spec) => spec,
        Err(report) => {
            eprint!("{}", report.render(&path.display().to_string()));
            return ExitCode::from(ExitStatus::InvalidConfig.code());
        }
    };
    // Keep the guard alive for the whole run so the non-blocking log writer stays open.
    let _logging = init_logging(LogFormat::Pretty, "info").ok();

    let mut registry = Registry::new();
    registry.source(Box::new(MqttSourceFactory::new()));
    registry.segment_sink(Box::new(ParquetSegmentSinkFactory::new()));
    registry.quarantine_sink(Box::new(QuarantineSinkFactory::new()));

    let pipeline = match Pipeline::build(spec, &registry, RuntimeOptions::default()) {
        Ok(pipeline) => pipeline,
        Err(errs) => {
            for e in &errs {
                eprintln!("error: pipeline build failed: {e}");
            }
            return ExitCode::from(ExitStatus::for_errors(&errs).code());
        }
    };

    let shutdown = Shutdown::new();
    let stop = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop.trigger();
        }
    });

    let status = match pipeline.run(shutdown).await {
        Ok(report) => report.exit,
        Err(e) => {
            eprintln!("error: pipeline run failed: {e}");
            ExitStatus::for_error(&e)
        }
    };
    ExitCode::from(status.code())
}
```

To run the example, start a local Mosquitto broker as shown in [Your first pipeline](../getting-started/first-pipeline.md), then from [`examples/embed-rust`](https://github.com/Dineshkarthik/thadam/tree/main/examples/embed-rust):

```bash
cargo run -- pipeline.yaml
```

### Notes

- `Pipeline::run` takes `thadam_core::Shutdown`, so depend on `thadam-core` directly.
- Keep the guard returned by `init_logging` (`let _logging = ...`) alive in a named binding for the whole run so the non-blocking log writer stays open.
- `ExitStatus::code()`, `ExitStatus::for_errors`, and `ExitStatus::for_error` produce the same process exit codes as the CLI ([Exit codes](../operations/exit-codes.md)).
- A configuration only accepts connector kinds you registered in `Registry`: `Pipeline::build` (and `Pipeline::check`) rejects any unregistered `sources[].kind` or `sinks[].kind` with `config.unknown_kind`, and `registry.kind_policy()` exports the registered set as a `thadam_config::KindPolicy` for `thadam_config::CheckOptions`.

## See also

- [API documentation on docs.rs](https://docs.rs/thadam-runtime)
- [Installation](../getting-started/installation.md)
- [CLI reference](../cli/index.md)
