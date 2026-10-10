# WAL hardware runbook (ADR-0004 S1 re-run)

Owner: the person who runs the bench on the reference devices.
Consumer: the maintainer, who folds the results into ADR-0004.

ADR-0004 (durable WAL) stays Draft until the S1 spike is re-run on reference hardware. This runbook covers that run. It takes about 1 to 1.5 hours per device and produces one report directory and one `.tar.gz` for each device.

## What the run answers

The ADR-0004 criteria in "Remaining before this ADR can leave Draft", item 6:

| Criterion | Threshold | Decision it feeds |
|---|---|---|
| Commit p99 at the reference load | <= 2 x (fsync latency + interval) | Fsync default (`interval(10ms)` is provisional), ack-window ceiling in ADR-0006 |
| Steady disk | <= backlog + one segment | Segment size, reclaim behaviour |
| Recovery, 64 MiB last segment | <= 50 ms on eMMC | Segment size, Linux preallocation |
| Kill -9 loop | zero failures in >= 1000 iterations | Correctness |
| LazyFS | zero failures in >= 300 iterations | Correctness (un-fsynced data loss only) |

The run also records raw flush latency, device-level write amplification from `/proc/diskstats`, and sustained MB/s by fsync mode. Those feed the Evidence section of ADR-0004 and the Linux preallocation decision.

## Hardware

| Slot | Device | Required? | Notes |
|---|---|---|---|
| A | x86-64 Linux box with NVMe SSD (any distro, kernel 5.15 or newer) | Required | Reference for the NVMe column |
| B | Raspberry Pi 5 with an NVMe HAT, or NVIDIA Jetson with eMMC | Required (one of the two) | The ARM eMMC column. The Pi 5 NVMe is a proxy only; the criterion names eMMC, so a Jetson or eMMC module is preferred |
| C | Raspberry Pi with SD card | Informational only | Do not use for ADR decisions. ADR-0004 says SD is unsupported storage (owner decision B3) |

Storage for the device under test must be the production class: industrial SSD or eMMC. Do not run on a USB stick or an SD card for the decision runs.

Record the exact model of each device (`lsblk` model column, Jetson module, NVMe model) in the runbook notes. The script records them automatically in `storage.txt`.

## Prerequisites

Packages (Debian or Ubuntu names; use the equivalents elsewhere):

```sh
sudo apt-get install -y build-essential pkg-config jq perl util-linux
# optional, for the flush probe (preferred over the dd fallback)
sudo apt-get install -y fio
```

- `jq` (1.6 or newer), `perl`, `awk`, `tar`, `findmnt` (util-linux) are required. The script exits early if any is missing.
- `fio` is optional. Without it the flush probe uses a `dd oflag=dsync` mean-only approximation, which gives no percentiles.
- `lazyfs` is optional. Without it the LazyFS step is recorded as skipped. Build it from <https://github.com/dsrhaslab/lazyfs> (the `lazyfs/` subdirectory). The Docker recipe in `spikes/s1-wal/docker/Dockerfile` shows the build steps. Requires FUSE 3 (`fuse3`, `libfuse3-dev`), `cmake`, and `g++`. Put the built `lazyfs` binary on `PATH`. Set `user_allow_other` in `/etc/fuse.conf` if the mount is refused.
- Rust toolchain: the repo pins `1.99.0` in `rust-toolchain.toml`. Install rustup, then from the repo root run `rustup show`. It installs the pinned toolchain automatically.

Filesystem and mount: the data dir must be on the device under test, on the production filesystem (ext4 with default options is the reference). Confirm the mount with `findmnt -T <data-dir>`. Do not run on tmpfs or on a loop device backed by the root filesystem.

Recovery cold-cache numbers need root on Linux (`/proc/sys/vm/drop_caches`). Run the script with `sudo` for the decision run, with the same toolchain. Use `sudo env "PATH=$PATH" "CARGO_TARGET_DIR=$CARGO_TARGET_DIR" scripts/wal-hw-bench.sh ...` so rustup and the cargo cache are found. If root is not possible, the report marks caches as not dropped and the recovery numbers are warm-cache.

Power and thermals: plug in the board, use the official power supply, and disable any power-saving governor you did not configure on purpose. Note the governor in the run notes.

Close other heavy workloads. Do not run other builds on the device while the bench runs.

## Before the run

1. Clone the repo at the commit under test and check it out. Record `git rev-parse HEAD`. The script also records the commit and the number of dirty paths.
2. Create a data directory on the device under test, for example `/mnt/nvme/wal-dut`. The script writes all scratch data under `<data-dir>/wal-hw-work` and deletes it on exit. It also creates `<data-dir>/lazyfs-root` during the LazyFS step and removes it afterwards.
3. Free space: at least 2 GiB for a full run (512 MiB for `--quick`).

## Smoke run first

Run the quick mode on the same device before the full run. It took about 1 minute on an Apple Silicon (arm64) once the toolchain was warm (about 1.5 minutes cold). Expect longer on the reference boards. It checks the toolchain, the paths, and the parsers. Quick results are not valid for ADR decisions.

```sh
scripts/wal-hw-bench.sh --quick --data-dir /mnt/nvme/wal-dut --out ~/wal-bench-smoke
```

If the smoke run fails, see "Troubleshooting" below before starting the full run.

## Full run (per device)

Run from the repo root at the commit under test. Use the `--out` path on a disk with room for the report (it is small, a few MB, plus the raw logs).

Device A (x86 NVMe):

```sh
cd <repo>
scripts/wal-hw-bench.sh \
  --data-dir /mnt/nvme/wal-dut \
  --out ~/wal-bench-x86-nvme-$(date +%Y-%m-%d)
```

Device B (ARM eMMC, or the Pi 5 NVMe proxy):

```sh
cd <repo>
scripts/wal-hw-bench.sh \
  --data-dir /mnt/emmc/wal-dut \
  --out ~/wal-bench-arm-emmc-$(date +%Y-%m-%d)
```

Device C (SD, informational only):

```sh
scripts/wal-hw-bench.sh --data-dir /mnt/sd/wal-dut --out ~/wal-bench-arm-sd-$(date +%Y-%m-%d)
```

Add `--skip-lazyfs` if LazyFS is not installed or you want to skip it. Add `sudo` for the decision run (see Prerequisites).

The script exits 0 when every measurement ran, even if a criterion fails. A non-zero exit means the run itself broke. Check `<out>/run.log` and `<out>/status.tsv`.

### Expected runtime

| Step | Full run (approximate) |
|---|---|
| Build S1 harness and thadam-wal benches (first time) | 5 to 10 minutes; faster on rerun |
| Flush probe | 30 s (fio), or about 1 minute |
| Commit-latency matrix (13 runs, 11 s each) | 3 to 5 minutes |
| Reference load (2 runs) | 30 s |
| Churn (2 GiB total) | 2 to 10 minutes depending on device throughput |
| Recovery (5 reps) | 1 minute |
| Crash loop (1000 iterations) | 10 to 25 minutes |
| Criterion benches | 2 to 3 minutes |
| LazyFS (300 + 150 + 30 iterations, if present) | 15 to 40 minutes |

## What the script measures

The script is `scripts/wal-hw-bench.sh`. It records each step in `<out>/run.log`. The table below says where each number comes from.

| Measurement | Source | Notes |
|---|---|---|
| Environment | `uname`, `lscpu` or `sysctl`, `/proc/meminfo`, `rustc`, `cargo`, `git` | `environment.txt` |
| Block device, filesystem, mount options | `findmnt`, `lsblk -o NAME,MODEL,ROTA,TRAN,SIZE` (Linux); `diskutil info`, `mount` (macOS) | `storage.txt` |
| Raw flush latency | `fio --fdatasync=1` (preferred); `dd oflag=dsync` mean (Linux fallback); per-op `dd conv=fsync` loop (macOS smoke) | `raw/flush.jsonl` |
| Commit-to-ack latency p50, p99, p99.9 by fsync mode and group size | S1 `bench` subcommand, seglog engine | `raw/append.jsonl` |
| Reference load latency | S1 `bench --rate-mbps 10 --window 256` | `raw/rate.jsonl` |
| Sustained MB/s | S1 `bench` | `raw/append.jsonl` |
| Process-level write amplification | S1 `os_write_amp` | From `/proc/self/io` (Linux) or `proc_pid_rusage` (macOS) |
| Device-level write amplification | `/proc/diskstats` sectors written before and after each S1 run | Linux only |
| Steady disk and reclaim | S1 `churn` subcommand | `raw/churn.jsonl` |
| Recovery after kill -9, 64 MiB backlog | S1 `recovery` subcommand, 5 reps | Open time only, see Method notes in the report |
| Kill -9 loop | `cargo test -p thadam-wal --test crash_loop` with `THADAM_WAL_CRASH_ITERS` | 1000 iterations for the criterion |
| thadam-wal criterion benches | `cargo bench -p thadam-wal --bench wal` | Production code, batch throughput only |
| LazyFS | S1 `crash --after-kill` on a LazyFS mount (seglog, always / interval 10 ms / never control) | Skipped if `lazyfs` is missing |

The S1 harness is in `spikes/s1-wal`. It is a separate Cargo workspace, not a member of the root workspace. The script builds it with the repo's pinned toolchain.

## Reading the report

`<out>/report.md` starts with the criteria table. Each criterion is PASS, FAIL, or NOT MEASURED. NOT MEASURED means the run did not reach the threshold (for example a quick run with 20 crash iterations) or the step was skipped. The report then lists:

- environment and storage, verbatim
- raw flush latency
- commit-to-ack latency by fsync mode and group size
- reference load
- churn
- recovery
- kill -9 loop
- LazyFS
- criterion benches
- method notes (read these before quoting a number)

The ADR-0004 decisions depend on these numbers:

| Decision | Look at | Rule of thumb |
|---|---|---|
| Fsync default | Commit p99 at reference load, per mode, and flush latency | Keep `interval(10ms)` if its p99 passes and `always` is too slow for the target load. Move to `always` if `interval` fails the commit criterion. Use `never` only if loss on power cut is acceptable, which it is not for the default |
| Linux preallocation | `append_small_always_prealloc_*` against the default run, and churn disk use | Enable preallocation if the default run is clearly slower or fragments; disable it if disk overhead breaks the steady-disk criterion |
| Segment size | Recovery open time and steady disk | The 64 MiB segment passes if recovery is under 50 ms on eMMC and steady disk fits backlog + one segment. Use a smaller segment if recovery fails; a larger one if reclaim is too coarse |
| Device WA | Device-level WA column | Feeds the flash endurance math in ADR-0004 and the claims register (ADR-0011) |

## What to send back

From each device, send the report directory's tarball and nothing else:

- `<out>.tar.gz` (contains `report.md`, `raw/`, `logs/`, `environment.txt`, `storage.txt`, `status.tsv`, `run.log`, `git-status.txt`, `summary.json`)
- one line per device: device model, data-dir mount, and whether LazyFS ran

Do not send the data dir. It is deleted on exit anyway.

## Troubleshooting

- `error: $DATA has ... KiB free`: free space or pick another mount. Full runs need 2 GiB.
- `error: S1 harness build failed`: see `<out>/logs/build-s1.log`. Usually a missing C compiler (install `build-essential`) or a toolchain mismatch (run `rustup show` in the repo root).
- `error: jq not found`: install `jq`.
- Recovery `caches_dropped` is false: the run was not root. Rerun as root for cold-cache numbers.
- LazyFS skipped as `lazyfs not on PATH`: install LazyFS (see Prerequisites) or pass `--skip-lazyfs` to acknowledge the skip.
- LazyFS `did not mount`: check `<out>/logs/lazyfs-mount.log`. Usually FUSE permissions (`user_allow_other`) or FUSE 3 missing.
- Crash loop FAIL: the report shows the summary line. Look at `<out>/logs/crash-loop.log` for the failing iteration. Do not rerun until the maintainer has seen the log.
- A criterion FAIL is a result, not a script error. Send the report as-is.

## Limits

- Process kill -9 (crash loop, S1 crash) is not power loss. The power-loss claim in ADR-0011 needs the separate hardware power-cut rig (500 cycles on a relay-switched board). This run does not cover it.
- LazyFS drops un-fsynced data only. Directory ordering (create, rename, unlink) passes through, so it is not tested. ADR-0004 item 4 (dm-flakey or power-cut rig) covers that.
- The crash-loop test uses a 5 ms interval fixed in the test, not 10 ms. Its segments are 256 KiB.
- Recovery is measured by the S1 harness, not the production `thadam-wal` code, and uses a 64 MiB backlog that is not exactly one 64 MiB segment. See the report's Method notes.
- macOS runs are smoke runs only. macOS `fsync` does not flush the drive cache, so the script uses F_FULLFSYNC there, and the flush probe is per-op and includes process startup.
