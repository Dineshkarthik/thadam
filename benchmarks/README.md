# Benchmarks

`thadam-bench` runs the edge-capture demo under a scenario and records what
happened: ingest rates, WAL and sink latency, `thadam` CPU and memory, Parquet
output, and restart time after a crash. It lives in `tools/thadam-bench`.

**Numbers are hardware-specific.** A result is only meaningful with the
environment block of its JSON file (CPU model, core count, OS, rustc, git
SHA). Never quote a figure from this directory without that metadata.

## Requirements

- Docker, for the public `eclipse-mosquitto:2` broker. Set
  `THADAM_BENCH_MQTT_IMAGE` to use another image.
- `curl`, for the `/metrics` and `/readyz` probes.
- Release binaries of `thadam` and `thadam-simulator`:

  ```sh
  cargo build --release -p thadam-cli -p thadam-simulator
  ```

  The runner looks for `thadam` and `thadam-simulator` in this order: the
  `THADAM_BIN` and `THADAM_SIMULATOR_BIN` environment variables, the directory
  of the `thadam-bench` executable, then `target/release/`.

## Run

From the workspace root:

```sh
cargo run --release -p thadam-bench -- run benchmarks/scenarios/m1-steady-state.yaml
```

Options:

- `--out <DIR>`: where the JSON result goes (default `benchmarks/results`).
  Results are never overwritten.
- `--duration <DUR>`: replaces the scenario's generation time, e.g.
  `--duration 10s` for a smoke run. The result is marked as overridden.

A run starts the broker, `thadam run` (with its admin port), and the simulator.
It samples `/metrics` and the `thadam` process (`ps`) once a second, then drains
for the scenario's `drain` time before stopping `thadam`. Logs for a failed run
are kept in a scratch directory whose path is printed to stderr.

To summarise a directory of results:

```sh
cargo run -p thadam-bench -- report benchmarks/results
```

## Scenarios

| File | What it measures |
|---|---|
| `m1-steady-state.yaml` | Default fleet at scale 1 for 60 s. |
| `m2-throughput-batched.yaml` | Scale 10 with batched samples, 60 s. |
| `m2-throughput-unbatched.yaml` | Scale 10 with one sample per message, 60 s. |
| `m3-recovery.yaml` | 30 s run; `thadam` is killed with SIGKILL at 15 s and restart-to-ready is timed through `/readyz`. |

Scenario fields: `name`, `description`, `duration`, `drain`, `pipeline`,
`fsync` (optional override of `wal.fsync`: `always`, `never`, or an interval),
`simulator` (`fleet`, `seed`, `scale`, `unbatched`, rates, `drain_timeout`),
`measure`, and `recovery.kill_after`. Unknown keys are rejected.

## Metrics

- Throughput: deltas of `thadam_source_messages_total` and
  `thadam_source_bytes_total` over the generation window, summed across
  restarts.
- Latency: p50/p95/p99 from the histogram deltas. `thadam_source_ack_latency_seconds`
  and `thadam_batch_write_seconds` are read when exported. The WAL commit
  histogram is not exported by `thadam` today, so that row reads "not exported".
- Resources: CPU% and RSS of the `thadam` process, from `ps -o %cpu=,rss=`.
- Storage: Parquet files and bytes under the sink directory after shutdown.
- Recovery: milliseconds from SIGKILL until `/readyz` returns 200.

## Output

Each run writes `<scenario>-<started_at>.json` with a `schema_version`, the
scenario, the run timing, the environment, and one object per measurement
requested. Measurements that could not be taken are omitted, or reported as
`not exported`.
