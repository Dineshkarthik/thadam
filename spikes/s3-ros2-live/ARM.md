# Spike S3 step 3 — ARM reference run (owner follow-up)

The spike runs on the development laptop are the x86-class reference. The owner criterion
that still needs ARM evidence is:

> 1 kHz IMU + 100 Hz joint_states + 30 Hz /tf at **<25 % of one core**, with
> **no unaccounted drops**, on a Raspberry Pi 5 / Jetson (arm64).

This file is the exact recipe. Everything below uses only Docker Hub public
images and a local checkout of the Thadam repo; no cloud resources, no
uploads — results stay on the device and are copied back by hand.

## 1. Hardware / OS prerequisites

| Item | Requirement |
|---|---|
| Board | Raspberry Pi 5 (8 GB) or Jetson Orin Nano / NX |
| OS | 64-bit userspace: Ubuntu 24.04+ aarch64 or 64-bit Raspberry Pi OS (Pi 5); JetPack 6.x on Jetson |
| Docker | any recent (>= 25) engine with BuildKit; `docker run` permission |
| Rust | `rustup` stable (same pinned toolchain family as the repo, 1.99.x) |
| Disk | ~4 GB free (images) |

Everything needed has been verified multi-arch on Docker Hub (2026-10-12):
`ros:lyrical-ros-base` and `ros:humble-ros-base` publish `linux/arm64/v8`,
`eclipse/zenoh-bridge-ros2dds:1.10.1` publishes `linux/arm64`, so the exact
commands below build/run on ARM unchanged.

## 2. Get the code and build

```bash
# same checkout as the spike (workspace + spikes/ subtree)
git clone <the thadam repo> thadam && cd thadam/spikes/s3-ros2-live   # or rsync the tree

# Publisher images (arm64 builds resolve the pinned manifest digests to arm64)
docker build -f docker/Dockerfile.lyrical -t s3-ros2-zenoh:lyrical-1 .
# optional (bridge+best-effort variants)
docker build -f docker/Dockerfile.cyclone -t s3-ros2-zenoh:jazzy-cyclone-1 .
docker build -f docker/Dockerfile.humble  -t s3-ros2-zenoh:humble-1  .
docker pull eclipse/zenoh-bridge-ros2dds:1.10.1

# Host-side capture binary (path deps to ../../crates/<…> — keep repo layout)
export CARGO_TARGET_DIR=$PWD/target
cargo build --release
```

## 3. Primary run: Lyrical + rmw_zenoh (candidate recommended by steps 1–3)

```bash
docker/run.sh lyrical-zenoh up
# 60 s baseline
scripts/run-capture.sh results/arm-capture-lyrical-rmw-baseline-60s \
  --docker-stats s3-router -- --duration-secs 60
# 60 s overload (collector sleeps 2 ms/message; drops must be COUNTED)
scripts/run-capture.sh results/arm-capture-lyrical-rmw-overload-60s \
  --docker-stats s3-router -- --duration-secs 60 --slow-ms 2
docker/run.sh lyrical-zenoh down
```

## 4. Optional: Lyrical + default DDS via the bridge

```bash
docker/run.sh lyrical-dds-bridge up
scripts/run-capture.sh results/arm-capture-lyrical-bridge-baseline-60s \
  --offset-container s3-sensor-pub-dds --pub-container s3-sensor-pub-dds \
  --docker-stats s3-bridge -- --transport bridge --bridge-zid 57a9ed --duration-secs 60
scripts/run-capture.sh results/arm-capture-lyrical-bridge-overload-60s \
  --offset-container s3-sensor-pub-dds --pub-container s3-sensor-pub-dds \
  --docker-stats s3-bridge -- --transport bridge --bridge-zid 57a9ed --duration-secs 60 --slow-ms 2
docker/run.sh lyrical-dds-bridge down
```

Note: `docker/run.sh <setup> down` removes all four `s3-*` containers and the
`s3ros2` network for any setup.

## 5. Criteria mapping — what to check before sending back

| Owner criterion | Where in the artifacts | PASS shape |
|---|---|---|
| 1 kHz IMU (best_effort) + 100 Hz joint_states + 30 Hz /tf received | `*.json` `topics[].received` vs `topics[].expected_by_seq`; publisher ground truth in `*.publog` | ≈59 k/6 k/1.8 k over 60 s (publisher is rclpy-timer limited to ≈984 Hz imu, same as laptop reference) |
| No unaccounted drops | `*.json` `missing_by_seq` + `gap_events` (rmw: `seq_source=attachment`; bridge: `seq_source=payload` + `*.publog` deltas) | baseline: `missing=0`, `gap_events=0`; overload: every published message lands in exactly one of {received, counted-gap, startup-burst, tail-undrained} — repeat the arithmetic shown in the report's §11.5.2 footnote |
| <25 % of one core (subscriber) | `*.ps` rows: column 1 = %CPU, column 2 = RSS KiB; averaged | `mean(cpu) < 25.0`; record max too (laptop reference: 6.2–7.5 % avg, ≤9.1 % max) |
| bounded memory | same file, column 2 | flat RSS, no growth trend (laptop reference: 10–21 MiB) |
| late-join control (nice-to-have) | `capture` `--only /tf_static` §3 recipe equivalent | rmw: exactly 1 replayed sample through the `@adv` query; bridge: 1 via route-activation replay |

The sampling loop in `scripts/run-capture.sh` uses `ps -o %cpu=,rss= -p` —
POSIX, works on Linux. `%cpu` on Linux ps is lifetime-average of the
process, so also report `docker stats`-style first/last deltas if you want
interval CPU (laptop runs used `ps`, so Pi numbers stay comparable if you use
the same script unchanged).

## 6. What to send back

- `results/arm-capture-*.{json,log,ps,offset,publog,dockerstats}`
- `uname -a` output (kernel, arch — confirms you are on aarch64, not armv7)
- board/SoC and RAM: `cat /proc/device-tree/model` (Pi) or `tegrastats`/`grep -m1 'model name' /proc/cpuinfo` (Jetson)
- `docker images` listing after the build (proves arm64 images, e.g. platform column)
- any deviations you had to make (e.g. you killed background daemons, used a different OS)

## 7. Known platform gotchas

- **USB/SD throttling**: run from a decent SD card or SSD; heavy docker
  builds on SD will dominate runtime but won't skew the 60 s captures.
- **Jetson GPU containers**: plain `docker run` is enough here (no GPU use).
- **Pi 5 thermals**: capture CPU% throttles if the board overheats — log
  `vcgencmd measure_temp` during the overload run if the board has no heatsink.
- **Multicast**: irrelevant for rmw_zenoh (disabled in every config here).
  For bridge runs, DDS multicast must work between the two containers on
  `s3ros2` (verified on macOS Docker; plain Linux docker bridge is simpler).
