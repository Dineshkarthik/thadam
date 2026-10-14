# Architecture overview

Thadam is a single-process, single-binary edge runtime running exactly one pipeline per process. Its defining structural decision is **capture-first**: raw messages are written to a durable log and flushed before anything is decoded, filtered, or acknowledged. Everything downstream of that log consumes it independently.

## One picture

```text
                thadam process (one pipeline, one data directory)
  ┌───────────────────────────────────────────────────────────────────┐
  │ MQTT liveness task ──► SourceDriver: next_batch, submit,          │
  │ (owns protocol client)│              ordered ack FIFO             │
  │                       │        │ RawBatch (byte permits)          │
  │                       ▼        ▼                                  │
  │               WAL writer OS thread ── group commit ──► CommitTicket│
  │                       │                                           │
  │                       ▼                                           │
  │               ┌────────────┐                                      │
  │               │  Capture   │    read cursors                      │
  │               │  WAL       ├──────────┬──────────┬─────────►      │
  │               └────────────┘          │          │                │
  │                       ▼               ▼          ▼                │
  │               Chain (sink P): router → decode → transforms ──►    │
  │                       │        SegmentDriver (Parquet per dataset)│
  │                       │ rejects  │        │ finalize → cursor     │
  │                       ▼          ▼        ▼ commit → eligible     │
  │               Quarantine      Segment catalog ──► Shipper ──► S3  │
  │                                                               (M1)│
  │               Chain (sink C): … ──► BatchDriver (accumulate,      │
  │                                        retry, bisect) ──► ClickHouse
  │                                                                 │
  │               Supervisor · maintenance · admin HTTP · metrics    │
  └───────────────────────────────────────────────────────────────────┘
```

The same diagram in mermaid form is in the repository README and in `ARCHITECTURE.md` section 2.

## One message, end to end

1. The MQTT client delivers a message. The source stamps `ingest_time` and queues it.
2. The SourceDriver pulls a batch and submits it to the WAL writer; the commit ticket goes into an ordered FIFO.
3. The WAL writer appends the batch as one frame, group-commits according to the fsync policy, and resolves the ticket.
4. The SourceDriver acknowledges the broker **in receive order, and only now**. Filtering or sink failure later can never affect acknowledgement.
5. Each sink's chain reads the committed WAL from its own cursor: route → decode → dataset shaping → transform steps → the sink driver.
6. The segment sink writes one Parquet writer per dataset. A finalized file advances the sink's watermark; the runtime commits the cursor durably; only then is the file eligible for upload.
7. The shipper uploads eligible files with deterministic keys and create-if-absent semantics, then records the upload in the segment catalog.

Because capture is first and chains read behind, a misconfigured filter leaves the raw WAL intact (fix the config, rewind the cursor, replay within the retained window), a slow sink never blocks capture or its sibling sinks, and the on-disk format never depends on the in-memory event model.

## Milestone boundary

The roadmap in milestones ([roadmap](../roadmap.md)):

- **M1 (MVP v0.1):** MQTT source, capture WAL, local Parquet segments, S3-compatible shipping, replay, chaos verification.
- **M2:** ROS 2 (MCAP/rosbag2 offline ingestion first).
- **M3 (P1):** ClickHouse sink, after the M1 reliability gate.

## The crates

The workspace is split by dependency weight and change rate, with the dependency rules enforced by `scripts/check-deps.sh` and `cargo deny` bans:

| Crate | Responsibility |
|---|---|
| `crates/thadam-core` | Model, errors, positions, connector traits, clock, shutdown, disk budget, schema and birth stores |
| `crates/thadam-config` | YAML → typed and validated pipeline spec, interpolation, diagnostics, JSON Schema |
| `crates/thadam-wal` | Capture WAL: writer thread, readers, cursors, retention, recovery |
| `crates/thadam-transform` | Synchronous, pure chains: router, decoders, shapes, steps, chain compiler |
| `crates/thadam-runtime` | Drivers, supervisor, shutdown, replay, observability, admin API |
| `crates/thadam-arrow` | Event → Arrow RecordBatch, dataset schema mapping, Parquet field IDs |
| `crates/thadam-segments` | Parquet segment sink, segment catalog, quarantine writer |
| `crates/thadam-object-store` | S3-compatible shipper over the segment catalog |
| `crates/thadam-iceberg` | Apache Iceberg REST table registrar service (Cargo feature `iceberg`, Rust 1.94) |
| `crates/thadam-mqtt` | MQTT source and `mqtt_publish` batch sink |
| `crates/thadam-http` | HTTP `listen` and `poll` source |
| `crates/thadam-grpc` | gRPC `Publish` and `PublishStream` source |
| `crates/thadam-mcap` | MCAP / rosbag2 file source and `McapExporter` |
| `crates/thadam-ros2-msg` | ROS 2 `.msg` parser and CDR decoder |
| `crates/thadam-ros2-live` | Live ROS 2 source over `rmw_zenoh` (Cargo feature `ros2-live`, experimental) |
| `crates/thadam-modbus` | Modbus TCP polling source |
| `crates/thadam-can` | Linux SocketCAN source (`raw` and DBC `decoded`) |
| `crates/thadam-opcua` | OPC UA `subscribe` and `poll` source (Cargo feature `opcua`) |
| `crates/thadam-sparkplug` | Sparkplug B `spBv1.0` protobuf decoder |
| `crates/thadam-nats` | NATS JetStream batch sink |
| `crates/thadam-kafka` | Apache Kafka batch sink |
| `crates/thadam-clickhouse` | ClickHouse batch sink (P1, default-on Cargo feature `clickhouse`) |
| `crates/thadam-testkit` | Memory connectors, conformance suites, fake clock, crash harness (dev only) |
| `apps/thadam-cli` | The `thadam` binary; the only composition root |
| `tools/thadam-simulator` | Synthetic fleet to MQTT, with a send-oracle manifest and verifier |
| `tools/thadam-bench` | Scenario runner and benchmark result collection |
| `sdk/python` | `thadam-python` PyO3 extension (excluded from `default-members`) |

`thadam-runtime` knows connectors only through `thadam-core` traits; Arrow and protocol libraries never appear in core APIs. The graph and the rules are in `ARCHITECTURE.md` section 9.

## Delivery discipline

Guarantees are stated per hop and only as strongly as the evidence allows; the details get their own page: [Delivery guarantees](delivery-guarantees.md).
