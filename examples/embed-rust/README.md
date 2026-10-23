# Embedding Thadam in Rust

A standalone binary crate that embeds the Thadam runtime in-process, building an MQTT-to-Parquet pipeline from `pipeline.yaml` and stopping cleanly on Ctrl-C.

Dependencies come from crates.io pinned to `=0.1.0-beta.1` (`thadam-config`, `thadam-core`, `thadam-mqtt`, `thadam-runtime`, `thadam-segments`), with an empty `[workspace]` table so this directory builds outside the repository workspace.

## Run it

1. Start a local MQTT broker (step 2 of the root [README](../../README.md#quick-start)):

```bash
printf 'listener 1883\nallow_anonymous true\npersistence true\npersistence_location /mosquitto/data/\nmax_inflight_messages 100\nmax_queued_messages 200000\n' > mosquitto.conf
docker run -d --name thadam-mosquitto -p 1883:1883 \
  -v "$PWD/mosquitto.conf:/mosquitto/config/mosquitto.conf" \
  eclipse-mosquitto:2
```

2. Run the embedded pipeline from `examples/embed-rust`:

```bash
cargo run -- pipeline.yaml
```

3. In another terminal, publish a test message, then stop the process with Ctrl-C:

```bash
docker exec thadam-mosquitto mosquitto_pub -t robots/robot42/telemetry -q 1 \
  -m "{\"seq\": 1, \"ts_us\": $(date +%s)000000, \"temp_c\": 41.2}"
```

Parquet segments are written under `./demo-data/sinks/local/ready/telemetry/`.
