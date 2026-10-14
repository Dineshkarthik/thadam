# Your first pipeline

This walkthrough goes from zero to a validated pipeline against a local MQTT broker in a few minutes. It uses Docker for the broker; everything else is one config file and the `thadam` binary from [installation](installation.md).

## 1. Start a local MQTT broker

Thadam relies on a persistent session and on the broker's queue surviving while Thadam is down, so the demo broker runs with persistence on and larger queue limits than the defaults ([ADR-0006](../design/index.md) lists the settings that matter in production):

```bash
cat > mosquitto.conf <<'EOF'
listener 1883
allow_anonymous true
persistence true
persistence_location /mosquitto/data/
max_inflight_messages 100
max_queued_messages 200000
EOF

docker run -d --name thadam-mosquitto \
  -p 1883:1883 \
  -v "$PWD/mosquitto.conf:/mosquitto/config/mosquitto.conf" \
  eclipse-mosquitto:2
```

Two broker defaults are worth knowing even in a demo: Mosquitto's inflight window defaults to 20 and its per-client queue to 1,000 messages. Those two numbers bound how many messages a slow or restarting Thadam can leave unacknowledged before the broker starts dropping, and they drive the throughput ceiling described in [Delivery guarantees](../concepts/delivery-guarantees.md#throughput-ceiling).

## 2. Write the pipeline configuration

Save this as `pipeline.yaml`:

```yaml
version: 1

pipeline:
  name: first-pipeline
  node_id: node-01
  data_dir: ./demo-data

sources:
  - id: broker
    kind: mqtt
    config:
      url: mqtt://localhost:1883
      client_id: thadam-demo-01          # stable: names the persistent session
      topics: [{ filter: "robots/+/telemetry", qos: 1 }]
      ack_mode: on_commit               # acknowledge after the WAL commit

wal:
  max_disk: 2GiB                        # WAL disk budget
  fsync: { interval: 10ms }             # group-commit interval
  on_full: block                        # explicit by design; see below
  retain_for: 1h                        # keep consumed data 1h for replay

datasets:
  - id: telemetry
    match: { source: broker, topic: "robots/+/telemetry" }
    decode: json
    identity: { robot: { id: 100, template: "{topic.1}" } }
    event_id: { key: [robot, seq] }
    dedup: { window: 10m, max_keys: 10k }
    time: { field: /ts_us, unit: us, domain: utc, max_skew: 5s }
    fields:
      - { id: 1000, name: seq,    type: u64, from: /seq, required: true }
      - { id: 1001, name: temp_c, type: f32, from: /temp_c }

sinks:
  - id: local
    kind: parquet
    config:
      datasets: [telemetry]

runtime: { memory_target: 256MiB }

observability:
  logs: { format: pretty, level: info }
  admin: { bind: "127.0.0.1:9464" }
```

A few things to notice, because each is a deliberate design rule:

- **`on_full: block` has no default.** Thadam refuses a configuration that does not say what happens when the WAL disk budget fills. `block` applies backpressure (loss risk moves to the broker's bounded queue); `drop_oldest` evicts old segments (loss happens inside Thadam, where it is counted and visible). Details: [Backpressure and disk budgets](../concepts/backpressure-and-disk-budgets.md).
- **`identity` turns topic levels into columns.** `{topic.1}` extracts the second topic level (`robot42` in `robots/robot42/telemetry`) into a `robot` column. Identity is data, not a partition key.
- **`fields` is a declared schema.** Every column is declared with a stable Parquet field `id` that is never reused ([`datasets`](../configuration/datasets.md)).
- **Time is explicit.** `/ts_us` is read as microseconds in the `utc` domain; a timestamp more than `max_skew` away from ingest time is flagged `CLOCK_SKEW`, never silently corrected ([event model](../concepts/event-model.md)).

## 3. Validate the configuration

```text
$ thadam validate pipeline.yaml
pipeline.yaml: configuration is valid
  pipeline `first-pipeline` (node `node-01`): 1 source, 1 dataset, 0 transforms, 1 sink
  config hash: 8bbf3e03… (hash version 1)
estimated Thadam-owned memory: 175.5 MiB (runtime.memory_target: 256.0 MiB)
  source `broker` broker window   100.0 MiB  window 100 (default) x message 1.0 MiB (default)
  wal.max_pending_bytes             8.0 MiB
  sink `local` read and batch       3.0 MiB
  sink `local` open files          64.0 MiB  max_open_files 4 (default) x 2 x row group 8.0 MiB
  dedup windows                   468.8 KiB  10000 keys x 48 B (upper bound)
```

`thadam validate` is a strict, multi-error pass: every problem is reported with a path, a line, and a hint, and the process exits with code 2 on any error. If a config references secrets you do not have (an S3 key read from a file, say), validate the shape anyway with `--allow-unresolved` — see [validating configurations](validating-configs.md).

## 4. Run the pipeline

```bash
thadam run pipeline.yaml
```

While it runs, the admin API listens on `127.0.0.1:9464`:

```bash
thadam status                              # pipeline state as pretty JSON
curl -s http://127.0.0.1:9464/readyz      # ok when every component is up
curl -s http://127.0.0.1:9464/metrics     # OpenMetrics for Prometheus
```

Publish a message and watch it flow:

```bash
docker exec thadam-mosquitto mosquitto_pub -t robots/robot42/telemetry -q 1 \
  -m '{"seq": 1, "ts_us": 1760000000000000, "temp_c": 41.2}'
```

Stop Thadam with Ctrl-C (SIGINT) or SIGTERM: it drains the source queue into the WAL, finalizes open files, commits cursors, and exits 0. A second signal forces an immediate exit; the WAL recover the rest on the next start.

### What lands next

The pipeline above runs end to end today: local Parquet segments, the `object_store` shipper ([object storage](../sinks/object-store.md)), replay ([`thadam replay`](../cli/replay.md)), inspection ([`thadam inspect`](../cli/inspect.md)), the chaos suites, and the oracle verifier are all wired. Still ahead are the chaos verification run and the published benchmark numbers on reference hardware. The README status banner and [readiness](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/readiness.md) track them.

## 5. A larger example: the `edge-capture` demo

The repository ships a full self-checking demo in [`examples/edge-capture/`](https://github.com/Dineshkarthik/thadam/tree/main/examples/edge-capture): a seeded simulator publishes a six-robot fleet (telemetry, joints, IMU, pose, state, events) over Mosquitto, and records every sample in an append-only oracle so delivered rows can be checked against the truth. Thadam runs natively; Mosquitto, Toxiproxy, an optional SeaweedFS S3 level, and an optional Prometheus/Grafana profile come from one compose file:

```bash
cd examples/edge-capture
make up          # broker + toxiproxy + simulator
make run-native  # thadam on thadam/pipeline.local.yaml (local Parquet)
make query       # DuckDB over the segments
make up-s3 && make run-native CONFIG=thadam/pipeline.s3.local.yaml   # level 2: + S3
```

The walkthrough, topology, oracle schema, and chaos notes are in the [demo README](https://github.com/Dineshkarthik/thadam/tree/main/examples/edge-capture). The reference configuration it derives from is [`examples/edge-capture/thadam/pipeline.yaml`](https://github.com/Dineshkarthik/thadam/blob/main/examples/edge-capture/thadam/pipeline.yaml), which checks out of the box on a machine without the demo secrets:

```bash
thadam validate --allow-unresolved examples/edge-capture/thadam/pipeline.yaml
```

## Next

- [Validating configurations](validating-configs.md) — diagnostics, `--allow-unresolved`, JSON Schema.
- [Architecture overview](../concepts/architecture-overview.md) — what just happened under the hood.
- [MQTT source](../sources/mqtt.md) — every option and the required broker settings.
