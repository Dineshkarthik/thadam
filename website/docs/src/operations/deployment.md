# Deployment

Thadam ships two deployment paths in `deploy/`: a Docker image and a systemd unit. Neither is published to a registry or package repository yet. The commands on this page are the ones run to check them, and each section says what was not exercised. [Applying a configuration change](#applying-a-configuration-change) covers changing the configuration on either path.

## One configuration for many nodes

Give every node the same `pipeline.yaml` and set its identity from the environment: `node_id: ${env:THADAM_NODE_ID}`. The variable is resolved by both `thadam validate` and `thadam run`, and an unset variable is an error rather than an empty node id. The config hash is taken with the placeholder as written, so the whole fleet shares one hash, while [`/status`](admin-api.md#status), the startup log record, and `thadam_build_info` report each node's own `node_id`.

## Docker

**Status: Available as a local build from source.** The image is built from this repository and was run on one machine (linux/arm64 under Docker Desktop for macOS). It is not pushed anywhere.

### Build

```sh
docker build -f deploy/docker/Dockerfile -t thadam:dev .
```

- Builder: `rust:1.99.0-bookworm`, matching `rust-toolchain.toml`. Runtime: `debian:bookworm-20261005-slim`, with `ca-certificates` and `curl` added. Both are public Docker Hub tags. The runtime has no OpenSSL dependency.
- The binary is `thadam`, built with `--locked`, and runs as uid 10001 (user `thadam`).
- The `FEATURES` build argument sets the Cargo features of `thadam-cli`. It is empty by default, which gives the default feature set (`clickhouse`). `--build-arg FEATURES=opcua` builds the OPC UA source; `thadam validate` accepts `kind: opcua` only in that image. The other optional features (`ros2-live`, `iceberg`) were not built.
- Behind a TLS-intercepting proxy, put the proxy's CA bundle at `deploy/docker/secrets/ca.pem` before building. The file is gitignored, is trusted only by the build stage, and is not in the final image. On the verification machine, the edge-capture bundle worked and the host's `/etc/ssl/cert.pem` did not:

```sh
cp examples/edge-capture/secrets/ca.pem deploy/docker/secrets/ca.pem
```

### Run

Mount the configuration, the admin token, and a volume for the data directory:

| Container path | Purpose | Mount |
|---|---|---|
| `/etc/thadam/pipeline.yaml` | Pipeline configuration | read-only file |
| `/etc/thadam/admin-token` | Admin API bearer token | read-only file |
| `/var/lib/thadam` | Data directory; set `pipeline.data_dir` under it | volume |

```sh
openssl rand -hex 24 > admin-token
chmod 0600 admin-token
docker run --rm -v "$PWD/pipeline.yaml:/etc/thadam/pipeline.yaml:ro" thadam:dev validate /etc/thadam/pipeline.yaml
docker run -d --name thadam \
  -p 127.0.0.1:9464:9464 \
  -v thadam-state:/var/lib/thadam \
  -v "$PWD/pipeline.yaml:/etc/thadam/pipeline.yaml:ro" \
  -v "$PWD/admin-token:/etc/thadam/admin-token:ro" \
  thadam:dev
```

- The token file must be readable by uid 10001 inside the container. `chmod 0600` worked under Docker Desktop for macOS. On Linux, make the file readable by uid 10001 (not verified).
- Without the token file, the container exits with code 1 and the message `cannot read admin token file`. The admin API is never started unauthenticated on a non-loopback address.

### Admin API and health

The container runs `thadam run --admin-bind 0.0.0.0:9464 --admin-token-file /etc/thadam/admin-token /etc/thadam/pipeline.yaml`. The admin port is published on the host's loopback only (`-p 127.0.0.1:9464:9464`).

- `/healthz` needs no token. `/readyz`, `/metrics`, and `/status` need `Authorization: Bearer <token>`. See the [admin API](admin-api.md).
- The image's HEALTHCHECK requests `http://127.0.0.1:9464/healthz` every 30 seconds.

```sh
curl -sS http://127.0.0.1:9464/healthz
docker inspect -f '{{.State.Health.Status}}' thadam
```

The first command prints `ok`; the second prints `healthy` once the first probe passes.

If you override `CMD`, keep `--admin-bind` and `--admin-token-file`. They pin the image's contract for any mounted `pipeline.yaml`: without `--admin-bind`, `thadam run` falls back to the config's `observability.admin` section (loopback `127.0.0.1:9464` when the section is absent), so the host-published port stops working even though the in-container healthcheck still passes; a non-loopback bind without `--admin-token-file` is refused.

### Stop

```sh
docker stop -t 40 thadam
docker inspect -f '{{.State.ExitCode}}' thadam
```

- `STOPSIGNAL` is SIGTERM. The first signal stops intake, drains, and finalizes the sinks, then `thadam` exits with code 0. In testing, an idle pipeline with four HTTP messages stopped in about 1 second, exited with code 0, and wrote the WAL `clean_shutdown` marker.
- `thadam` allows up to 30 seconds for shutdown. Docker's default stop timeout is 10 seconds, after which it sends SIGKILL (exit 137), so pass `-t 40` for a pipeline with work in flight.
- A second signal forces the stop (exit 130, see [exit codes](exit-codes.md)). This was not exercised.

### Known limits

- Only the default image and the `opcua` feature image were built and run. The image is not signed or scanned.

## systemd

**Status: In progress.** `deploy/systemd/thadam.service` passes `systemd-analyze verify` on systemd 252 (Debian 12) with the `thadam` binary installed at `/usr/local/bin/thadam`. It has not been started under a running systemd, so restart, stop, and `systemd-analyze security` results are not checked.

Get the binary from the image (the same commands were run here):

```sh
docker create --name thadam-extract thadam:dev
docker cp thadam-extract:/usr/local/bin/thadam ./thadam
docker rm thadam-extract
```

Install (run as root; prefix `sudo` on a host). These commands were run in a Debian 12 container:

```sh
install -m 0755 thadam /usr/local/bin/thadam
useradd --system --home-dir /var/lib/thadam --shell /usr/sbin/nologin thadam
install -d -m 0755 /etc/thadam
install -m 0644 pipeline.yaml /etc/thadam/pipeline.yaml
install -m 0644 deploy/systemd/thadam.service /etc/systemd/system/thadam.service
systemd-analyze verify /etc/systemd/system/thadam.service
```

Then enable and start the `thadam` unit with systemd. That step was not run here.

What the unit does:

- Runs as `thadam`. systemd creates `/var/lib/thadam` (`StateDirectory=`, mode 0750) for the data directory, and `pipeline.data_dir` should point there.
- Starts `thadam run --admin-bind 127.0.0.1:9464 /etc/thadam/pipeline.yaml`. The admin API is loopback-only and needs no token. `thadam run` reads the `observability.admin` section of the configuration; the flag pins the bind to loopback so a non-loopback value in the configuration does not take effect for this unit.
- Restarts on failure after 5 seconds. Exit codes 2 (invalid configuration) and 3 (data directory locked or incompatible) are not restarted, because they do not clear on retry. Exit 0 is a clean stop and is not restarted.
- Stops with SIGTERM and allows 40 seconds (`TimeoutStopSec=40s`) before SIGKILL, above `thadam`'s 30-second shutdown bound.
- Hardening: `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, an empty `CapabilityBoundingSet`, and `RestrictAddressFamilies` limited to `AF_UNIX`, `AF_INET`, `AF_INET6`, and `AF_NETLINK`. The unit also sets the other `Protect*` and `Restrict*` options listed in the file. `LimitNOFILE=65536`.

CAN caveat: the `can` source opens a SocketCAN socket (`AF_CAN`), which this unit blocks. Before using a `can` source, add `AF_CAN` to the `RestrictAddressFamilies=` line in an override. A copy of the unit with `AF_CAN` added also passes `systemd-analyze verify`.

## Applying a configuration change {#applying-a-configuration-change}

**Status: Available as a local script, tested with a stand-in restart.** `deploy/apply-config.sh` was run on one machine against a local `thadam` binary, with a shell stand-in for `systemctl restart`. It has not been run under systemd or with the Docker image, so the unit's stop and start behaviour and the real `systemctl` exit codes were not exercised.

`thadam` reads its configuration once, at start, and has no endpoint that accepts a new one. Applying a change means replacing the file and restarting. The script does the replacement with checks and a rollback. The design record is `docs/design/fleet-config-and-versioning.md`, section 3.

```sh
deploy/apply-config.sh candidate.yaml /etc/thadam/pipeline.yaml
```

The live path defaults to `/etc/thadam/pipeline.yaml`. The script does four things:

1. Copies the candidate to `pipeline.yaml.new` next to the live file and runs `thadam validate --against-data-dir` on it. That is the check `thadam run` makes before it starts: read-only, with the node's real secrets. If it fails, the script removes the `.new` file and exits with validate's code. The live file is not touched and the node is not restarted.
2. Copies the live file to `pipeline.yaml.prev`, then renames `.new` over the live file. The rename is atomic on one filesystem.
3. Runs `THADAM_RESTART_CMD`, by default `systemctl restart thadam`.
4. Polls `thadam status` until `/status` reports `"phase": "running"`, `"ready": true`, and the `config_hash` that step 1 printed, within `THADAM_READY_TIMEOUT_SECS`. `ready` is false while any component is starting or has failed, so a source that is still starting when the timeout ends also rolls the change back. Raise the timeout for slow brokers.

If the restart command fails, or the node does not report the candidate's hash in time, the script moves `.prev` back over the live file, runs the restart command again, and exits 1. If there was no live file before the apply, the rollback removes the candidate. The rollback restores the file; it does not undo what the failed run wrote. Cursors and segments written under the candidate's hash stay, which is acceptable because each chain applies the current configuration from its cursor.

| Variable | Default | Meaning |
|---|---|---|
| `THADAM_BIN` | `thadam` | The binary that runs `validate` and `status`. |
| `THADAM_RESTART_CMD` | `systemctl restart thadam` | Run with `sh -c`. For the Docker image, use `docker restart thadam` (not run). |
| `THADAM_ADMIN_URL` | `http://127.0.0.1:9464` | The admin API that step 4 polls. |
| `THADAM_ADMIN_TOKEN_FILE` | empty | Bearer token file, when the admin API uses one. |
| `THADAM_READY_TIMEOUT_SECS` | `30` | Whole seconds to wait for the candidate's hash. |

Exit codes: `0`, applied, and the node is ready on the new hash. `1`, the restart or the readiness check failed and the previous file was restored; or `validate` printed no config hash, and nothing changed. `2` or `3`, or any other non-zero code from `thadam validate`, means the candidate was refused and nothing changed. `2` is also the usage error code. A `3` covers a data-directory refusal such as a pipeline name change; the script does not pass `--repair`, so that change needs a person. See [exit codes](exit-codes.md).

### Which changes are safe on restart {#which-changes-are-safe-on-restart}

Every change is a restart. The useful distinction is what a restart does to data. Rows marked *(read)* come from reading the code and were not run.

| Change | What a restart does |
|---|---|
| Transforms, filters, sampling, dedup | Applies from each sink's cursor to WAL not yet processed. Finalized segments are not rewritten. Stateful steps warm-start from the WAL tail. |
| Add a dataset field (new field id), sink tuning, log or admin settings | *(read)* Nothing in the WAL depends on them. New segments carry the new schema fingerprint; shipped files keep theirs. Removing or retyping a field was not checked. |
| Add a sink | Its cursor starts at `start_from`. `earliest` (the default) backfills the whole retained WAL; `latest` skips it. |
| Remove a sink or shipper | Stops pinning WAL retention. *(read)* Its cursor file and `sinks/<id>/` directory stay on disk, and nothing ships its unshipped finalized segments. Re-adding the same id resumes the stale cursor and may hit an evicted-data gap. Drain first: `lag_frames` is 0 and nothing is `eligible`. |
| Remove a source with its datasets | Frames already in the WAL that no dataset matches are counted as `unmatched` and dropped. They appear in `thadam_chain_unmatched_total{sink}` and in `/status`. Wait for each sink to pass the source's last frame before removing it. |
| `wal.on_full`, `wal.max_disk` | Changes the one place acknowledged data can be dropped. Treat it as a reviewed change, not a routine one. |
| `pipeline.name`, `node_id` | A name change is refused (exit 3) unless `thadam run --repair`. A `node_id` change makes the WAL refuse to open. Whether `--repair` covers it was not checked. Not a remote-apply case. |
| `pipeline.data_dir` | Not refused; starts a fresh WAL. A migration, not an apply. |

### What a restart costs each source {#restart-gap-per-source}

Every apply stops intake, drains (up to 10 seconds for chains, 30 seconds overall), then recovers and starts. Data that arrives in that interval depends on the source. The rows come from each source's delivery notes. None was tested through a restart, except the local timing below.

| Source (`kind`) | Delivery | During a restart | Status |
|---|---|---|---|
| `mqtt`, QoS 1, persistent session (`clean_start: false`, the default) | At-least-once with `ack_mode: on_commit` | The broker holds the session, so messages published during the gap wait in its queue, up to the broker's limits (Mosquitto `max_queued_messages`, EMQX `mqtt.max_mqueue_len`). Past the limit the broker drops them. A clean shutdown sends DISCONNECT so the session keeps unacknowledged messages. Not tested here: a Thadam restart with messages queued at the broker. See [MQTT](../sources/mqtt.md). | Available |
| `mqtt`, QoS 0, or `clean_start: true` | At-most-once | Messages published during the gap are dropped. MQTT does not require a broker to queue QoS 0 for an offline subscriber. `clean_start: true` discards the broker's session at each connect. | Available |
| `http`, `listen` | At-least-once from the client's view (`204` after commit) | Requests during the gap fail or time out. The client must retry. Nothing is acknowledged before it is stored. See [HTTP](../sources/http.md). | Available |
| `http`, `poll` | At-most-once (`on_receive`) | A poll missed during the gap is not made up. The next poll returns the current response. | Available |
| `grpc`, `Publish` and `PublishStream` | At-least-once from the client's view (`OK` after commit) | Calls during the gap fail with `UNAVAILABLE` or a dropped connection. The client must retry. See [gRPC](../sources/grpc.md). | Available |
| `mcap`, file | Resume from `resume.json`, at-least-once across a crash in the commit-to-state window | The file continues after its last committed message, so the gap costs replay time, not data. Changing `topics`, `start`, or `end` replays the selected files from the start. See [ROS 2 restarts and resume](../sources/ros2.md#restarts-and-resume). | Available |
| `modbus`, `poll` | At-most-once per poll (`ack_mode: none`) | Polls during the gap are not made up. Register values cannot be replayed, so readings in the gap are missing, and the next poll reads the current value. See [Modbus](../sources/modbus.md). | Available |
| `opcua`, `subscribe` or `poll` | At-most-once (`ack_mode: none`) | Subscribe: values published during the gap are not replayed, and the first response after the restart carries the current values. Poll: the next poll reads the current values. Behind the opt-in `opcua` build feature. See [OPC UA](../sources/opcua.md). | Available (opt-in `opcua` build feature) |
| `can`, SocketCAN (Linux) | At-most-once, no acknowledgement | CAN has no store. Frames sent while Thadam is not reading the socket are gone. See [CAN](../sources/can.md). | Available |
| `ros2_live` | At-most-once (`ack_mode: none`), no re-read | Everything published while the source is down is gone, and capture restarts live. Sequence gaps on reconnect are counted in `ros2.dropped`. See [ROS 2 live](../sources/ros2-live.md). | In progress (experimental) |

### Measured restart time {#measured-restart-time}

The `apply_config` integration test (`apps/thadam-cli/tests/apply_config.rs`) times the restart. The clock starts when the restart command writes its first line, before it stops the old process, and stops at the first `/status` poll that reports the candidate's hash as running and ready. Seven runs of the valid-change test on one machine (macOS, Darwin, arm64), with a debug build, one idle `http` source, and a local data directory: 92, 96, 98, 100, 101, 101, and 103 ms, a median of 100 ms. The full `apply-config.sh` run, including `thadam validate` and the first readiness poll, took 687 to 727 ms across the same runs.

Not measured: the stop and start under the systemd unit, the Docker image, a release build, and a node with a backlog or in-flight batches. A node with more to drain takes longer to stop.

### Detecting config drift {#detecting-config-drift}

Each `thadam run` records `thadam_build_info{version,config_hash,node_id,pipeline} 1` once, at start (see [metrics](metrics.md)). The rules below use it. The first counts the distinct hashes each pipeline runs; more than one means the fleet is mixed. A rolling restart is briefly mixed, so the alert waits 15 minutes. The second names nodes that are not on the hash the delivery tool wants. The delivery tool replaces the placeholder with the hash that `thadam validate` printed for the desired configuration.

```yaml
groups:
  - name: thadam-config
    rules:
      - record: thadam:config_hashes:count
        expr: count by (pipeline) (count by (pipeline, config_hash) (thadam_build_info))
      - alert: ThadamConfigHashDrift
        expr: thadam:config_hashes:count > 1
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.pipeline }} runs {{ $value }} config hashes"
      - alert: ThadamNodeOffDesiredConfig
        expr: thadam_build_info{config_hash!="REPLACE_WITH_DESIRED_HASH"}
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "{{ $labels.node_id }} runs config {{ $labels.config_hash }}, not the desired one"
```

Checked: the metric name and labels against a scrape of a local `thadam run` (`/metrics`), and the YAML parses. Not checked: `promtool check rules`, and Prometheus evaluating these rules against live data. A node that stops reporting drops out of `thadam_build_info`, so the second rule does not cover it; alert on the `up` metric as well.
