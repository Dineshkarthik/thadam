# Deployment

Thadam ships two deployment paths in `deploy/`: a Docker image and a systemd unit. Neither is published to a registry or package repository yet. The commands on this page are the ones run to check them, and each section says what was not exercised.

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

If you override `CMD`, keep `--admin-bind` and `--admin-token-file`. Without them, `thadam run` starts no admin server and the container reports `unhealthy`; this happened in testing (`curl` exit 7, connection refused).

### Stop

```sh
docker stop -t 40 thadam
docker inspect -f '{{.State.ExitCode}}' thadam
```

- `STOPSIGNAL` is SIGTERM. The first signal stops intake, drains, and finalizes the sinks, then `thadam` exits with code 0. In testing, an idle pipeline with four HTTP messages stopped in about 1 second, exited with code 0, and wrote the WAL `clean_shutdown` marker.
- `thadam` allows up to 30 seconds for shutdown. Docker's default stop timeout is 10 seconds, after which it sends SIGKILL (exit 137), so pass `-t 40` for a pipeline with work in flight.
- A second signal forces the stop (exit 130, see [exit codes](exit-codes.md)). This was not exercised.

### Known limits

- `thadam run` applies only the admin bind and token from its flags. The `observability.admin` section of the configuration is validated but not applied. The image passes the flags for this reason.
- `docker logs` showed nothing while the container ran. After a stop it showed only the first runtime log line (`wal recovered`), not the later lines. To watch a running pipeline, use `/status` and `/metrics`.
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
- Starts `thadam run --admin-bind 127.0.0.1:9464 /etc/thadam/pipeline.yaml`. The admin API is loopback-only and needs no token. `--admin-bind` is required because `thadam run` ignores the `observability.admin` section, as noted above.
- Restarts on failure after 5 seconds. Exit codes 2 (invalid configuration) and 3 (data directory locked or incompatible) are not restarted, because they do not clear on retry. Exit 0 is a clean stop and is not restarted.
- Stops with SIGTERM and allows 40 seconds (`TimeoutStopSec=40s`) before SIGKILL, above `thadam`'s 30-second shutdown bound.
- Hardening: `NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`, `PrivateTmp`, an empty `CapabilityBoundingSet`, and `RestrictAddressFamilies` limited to `AF_UNIX`, `AF_INET`, `AF_INET6`, and `AF_NETLINK`. The unit also sets the other `Protect*` and `Restrict*` options listed in the file. `LimitNOFILE=65536`.

CAN caveat: the `can` source opens a SocketCAN socket (`AF_CAN`), which this unit blocks. Before using a `can` source, add `AF_CAN` to the `RestrictAddressFamilies=` line in an override. A copy of the unit with `AF_CAN` added also passes `systemd-analyze verify`.
