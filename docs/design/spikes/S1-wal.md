# Spike report: S1 durable buffer (WAL) engine and on-disk format

- **Status:** spike report. Evidence for ADR-0004 (durable buffer engine and on-disk format), D1 (capture-first raw WAL), D2 (WAL engine) and D3 (fsync default). It is not an ADR and changes no decision by itself.
- **Date:** 2026-10-07
- **Code:** `spikes/s1-wal/` (standalone Cargo project, own empty `[workspace]`; throwaway)
- **Raw outputs:** `spikes/s1-wal/results/` (one JSON object per run; `report_tables.py` renders every table below from them)

## 1. Summary

**Recommendation.**

- **D2 / ADR-0004 engine: the custom segmented log.** In this spike it matched SQLite on every correctness test, wrote about half the bytes, reclaimed disk by `unlink` (orders of magnitude cheaper than SQLite's vacuum plus checkpoint), had bounded recovery time, and used 3 to 8 MiB RSS. Keep SQLite as the documented fallback. Do not use redb as the bulk queue.
- **Frame format: freeze the 24-byte header as implemented here**, with the reserved 4 bytes must-be-zero, CRC32C over header bytes `[0, 20)` followed by the payload, little-endian, and an all-zero header as the end-of-data marker. Details and three required behavior changes are in section 7.
- **D3 fsync default: do not ship `interval(50ms)` as the default together with `ack_mode: on_commit`.** It caps per-source throughput at the inflight window divided by 50 ms. Use a short bounded interval (provisionally `interval(10ms)`), keep `always` for low latency, and re-measure on reference eMMC before freezing. Section 7.3 has the evidence.
- **D1 capture-first raw WAL: supported on the WAL side, not closed.** The raw-envelope commit path costs one group commit (about 2.4 ms p50 on the Linux VM at 10 MB/s, 5.9 ms on macOS with real flushes) and stays near 1.0x write amplification. S1 did not measure chain CPU with 1 to 3 sinks or WAL growth during a sink outage, which D1 also lists.
- **ADR-0004 can move to Proposed for engine and format.** It cannot yet tick its hardware boxes: no x86 NVMe or ARM eMMC was available. The Linux numbers come from a Docker Desktop VM whose flush semantics are unknown. Power-loss claims stay blocked on the rig (ADR-0011).

**Headline numbers.** All throughput is payload MB/s, i.e. raw record bytes excluding framing. Group commit uses 4 producers, each with up to 16 uncommitted frames. One frame is 64 records of 200 B, or 16 records of 4 KiB.

| | custom log | SQLite (WAL mode) | redb 4.3 |
|---|---|---|---|
| Append, `always`, macOS with `F_FULLFSYNC`, 200 B / 4 KiB records | 101 / 497 MB/s | 85 / 200 MB/s | 96 / 380 MB/s |
| Same, commit p50 / p99 (200 B) | 8.0 / 15.9 ms | 8.0 / 22.1 ms | 8.1 / 13.8 ms |
| Append, `always`, Linux VM (`fdatasync`), 200 B / 4 KiB | 539 / 1037 MB/s | 360 / 354 MB/s | 322 / 664 MB/s |
| Commit p50 / p99 at 10 MB/s offered, `always`, Linux VM | 2.4 / 4.6 ms | 2.3 / 9.0 ms | 2.9 / 6.1 ms |
| Engine capacity, `interval(50ms)`, large window, Linux VM, 200 B | 2812 MB/s | 556 MB/s | 1041 MB/s |
| OS-reported bytes written per payload byte, saturated append (higher at light load, see 5.3) | 1.00 to 1.05 | 2.0 to 2.2 (3.1 to 3.2 with reclaim) | 1.3 to 2.2 |
| Disk after consumers catch up (Linux churn, 256 MiB backlog) | 16 MiB (one preallocated segment) | 0.6 MiB | 328 MiB (never shrinks) |
| Time spent deleting during 2 GiB of churn (Linux / macOS) | 0.13 / 0.54 s | 4.5 / 11.2 s | 0.24 / 2.2 s |
| Recovery after kill -9, cold cache, 2,076 to 2,599 MiB backlog (Linux) | 7.0 ms | 6.4 ms | 720 ms (2.7 ms with `quick_repair`) |
| Peak RSS during appends | 3 to 8 MiB | 6 to 8 MiB | 0.2 to 1.4 GiB with the default cache (37 to 96 MiB with a 32 MiB cache) |
| kill -9 iterations, committed loss or stale frames | 0 failures (see 5.7) | 0 failures | 0 failures (except `never`, by design) |
| LazyFS (drop all un-fsynced data at the kill point), `always` + `interval` | 0 of 300 failed | 0 of 300 failed | 0 of 300 failed |
| After one failed fsync, next commit on the same handle | refused (spike's poison flag) | **succeeds** (no latch) | refused (`PreviousIo`) |
| Can delete consumed data on a full disk | no: open allocates a new segment (fixable but untested; section 7.2) | no (DELETE needs space) | no (COW needs space) |

## 2. Question and decision context

- **ADR-0004** needs an engine choice (custom log vs SQLite vs redb) and a frozen on-disk format: O9 header size, CRC coverage, resynchronization in sealed segments, and the end-of-data marker.
- **D1** asks whether a capture-first raw WAL is affordable on the ack path.
- **D2** recommends the custom log, with SQLite as fallback if S1 shows correctness risk in the custom log.
- **D3** proposes `interval(50ms)` with acks after fsync.
- Design followed: implementation plan sections 4.2, 4.7 and 5.3; architecture review section 5.

## 3. Environment

| | macOS host | Linux |
|---|---|---|
| Machine | arm64 laptop, 18 cores, 48 GB | Docker Desktop VM on the same host: 18 vCPU, 7.9 GB RAM |
| OS / kernel | macOS, arm64 | Debian 12.15 (`rust:1-bookworm`), linuxkit kernel, aarch64 |
| Storage | Internal SSD, APFS | ext4 on `/dev/vda1`, a virtio disk backed by a file on the host's APFS volume. Mounted as a Docker named volume, not a bind mount. Guest reports a write-back cache and no FUA |
| Flush primitive | `fsync()` does **not** flush the drive cache; `F_FULLFSYNC` does; `F_BARRIERFSYNC` is ordering only | `fdatasync`, but whether the hypervisor turns guest flushes into `F_FULLFSYNC` on the host is unknown (see limitations) |
| Toolchain | Rust 1.95.0 | Rust 1.99.0 |

Crate versions: redb 4.3.0, rusqlite 0.40.2 with bundled SQLite 3.53.2, crc32c 0.6.8.

The host was shared with other work during the runs, including at least one Docker container (`s2-mosquitto`) and cargo builds. Section 6 covers how that affects the numbers.

## 4. Method

### 4.1 Engines behind one trait

`Engine` has these operations: `append_group(frames)` (stage), `flush()` (end of group), `sync()` (durable), `read(after, max_bytes)`, `commit_cursor` (durable on return), `load_cursor`, `delete_consumed(upto)`, plus `next_seq` and `first_seq`. One frame equals one source batch, as the plan specifies.

- **Custom log** (`src/seglog.rs`, about 590 lines):
  - **Segment header** (64 bytes): magic `THADMWAL`, `format_version u16`, reserved u16, `epoch u64`, `base_seq u64`, `created_unix u64`, reserved zeros, CRC32C over bytes `[0, 60)`.
  - **Frame header** (24 bytes): `magic u16`, `version u8`, `flags u8`, `len u32`, `frame_seq u64`, reserved u32 (must be 0), and `crc32c u32` over header bytes `[0, 20)` followed by the payload.
  - **Writes:** one `pwrite` per group. Segments are created as temp file, header, optional preallocation, fsync, rename, then directory fsync. On roll the old segment is truncated to its data and fsynced.
  - **Recovery:** scans the last segment frame by frame (magic, version, reserved, `len` against `frame_max` and file size, `frame_seq` continuity, CRC). At the first failure it truncates, fsyncs, and opens a new segment. An empty last segment is reused after truncation to its header.
  - **Cursors:** temp file, fsync, rename, directory fsync.
  - **Reclaim:** whole-segment `unlink`, each followed by a directory fsync.
  - **Errors:** any failure from write, roll, or fsync poisons the writer until it is reopened.
  - **Sync kinds:** `data` is `fdatasync` on Linux and plain `fsync` on macOS; `full` is `fsync` on Linux and `F_FULLFSYNC` on macOS; `barrier` is `F_BARRIERFSYNC` on macOS.
  - **Preallocation:** `fallocate` on Linux; `F_PREALLOCATE` plus `ftruncate` on macOS.
- **SQLite** (`src/sqlite.rs`): WAL mode, a `q(seq INTEGER PRIMARY KEY, data BLOB)` table, cursors stored in the same database, `auto_vacuum=INCREMENTAL`, and `journal_size_limit` set to 16 MiB.
  - `always`: `synchronous=FULL`, one transaction per group.
  - `interval`: `synchronous=FULL`, with groups accumulating in one open transaction that is committed at the interval. Per-group commits with `synchronous=OFF` were rejected because an unsynced checkpoint can corrupt already-acknowledged data on power loss.
  - `never`: `synchronous=OFF`.
  - `full` sync kind sets `PRAGMA fullfsync` and `checkpoint_fullfsync`.
  - Reclaim: `DELETE`, then `PRAGMA incremental_vacuum` stepped to completion, then `PRAGMA wal_checkpoint(TRUNCATE)`. Section 5.5 explains why all three are needed.
- **redb** (`src/redbq.rs`): a `u64 -> &[u8]` table plus a cursors table.
  - `always` and `interval` use `Durability::Immediate`; `interval` keeps one open write transaction until the interval elapses. `never` uses `Durability::None`.
  - redb calls `File::sync_data`, which Rust std maps to `F_FULLFSYNC` on macOS, so **redb on macOS is always a "full" flush**. Compare it with the custom log and SQLite rows labeled `(full)`.
  - The database is created under a temporary name and renamed into place. Section 5.8 explains why.

### 4.2 Harness (`src/main.rs` subcommands)

- **`bench`: append throughput and commit latency.**
  - Four producer threads submit frames to one writer thread over a bounded channel. The writer drains up to 4 MiB per group, does one append and flush, then syncs according to the fsync mode and resolves tickets after the sync. This is the plan's `WalWriter`.
  - Each producer allows at most `window` uncommitted frames, which models an MQTT inflight window.
  - Latency runs from submit to durable ack. 10 s are measured after a 1 s warm-up. `--rate-mbps` paces producers open-loop and measures latency from the scheduled send time, which avoids coordinated omission.
  - "OS write amp" is the bytes this process caused to be written to storage, divided by payload bytes. On macOS that is `proc_pid_rusage` `ri_diskio_byteswritten`; on Linux it is `/proc/self/io` `write_bytes`.
- **`read`:** writes 1 GiB, reopens the engine (dropping engine caches; the OS page cache stays warm), then reads from cursor 0 in 1 MiB chunks, checking sequence continuity.
- **`churn`:** single-threaded. Appends 2 GiB at `interval(50ms)` while a consumer trails 256 MiB behind, committing its cursor and deleting every 16 groups (about 3.3 MB). Disk usage is sampled throughout. At the end the consumer catches up fully, and reclaim time and remaining disk are measured. Segments are 16 MiB (the edge profile).
- **`recovery`:** a child process writes a backlog, keeps appending, and is killed with SIGKILL 100 ms later. The parent then times open plus the first query. On Linux, `/proc/sys/vm/drop_caches` was written before opening (cold cache); on macOS the cache stayed warm.
- **`crash`: kill -9 loop.**
  - Each child opens the engine, which runs recovery, then prints `OPEN <next_seq>`. It appends groups of 1 to 4 frames of mixed 200 B and 4 KiB records. Every payload encodes `(run, seq)`, and every byte is derived from them.
  - The child prints `ACK <seq>` only after the covering commit returned per its fsync mode, and `CUR <seq>` after a cursor commit returned. Every 8 groups it commits a cursor and deletes up to it.
  - Segments are 256 KiB, so rolls and deletions happen constantly.
  - The parent kills the child after a random delay: 10% of delays are 0 to 5 ms, which kills during open and recovery; the rest are 5 to 250 ms. The parent then reopens in-process and checks:
    - every retained frame is contiguous from `first_seq` to the end;
    - every frame is byte-exact for the run that wrote that seq (catches stale or resurrected frames);
    - the last frame is at or above the highest ACK ever printed;
    - the cursor did not regress, is not beyond the end, and nothing past it was deleted;
    - the child's `OPEN` value equals the `next_seq` the parent just verified.
  - `--after-kill` runs a command between the kill and verification. With LazyFS it drops all un-fsynced data.
- **`torn`:** exhaustive custom-log corruption cases, with and without preallocation:
  - truncate the last frame at every byte offset;
  - flip one bit at every byte of the last frame;
  - zero a middle frame while later frames stay intact (out-of-order writeback);
  - garbage after the last frame.

  Each case reopens, checks survivors byte-for-byte, appends from a new run, then reopens twice more.
- **`fsync-fault`:** Linux only, run under `LD_PRELOAD=fault/failsync.c`. Creating an arm file makes the next `fsync`/`fdatasync` return `EIO`, either "once" or "sticky". Three commits, one faulted commit, then two more on the same handle, then reopen and check which seqs are present. SQLite and redb are driven through their raw APIs, so the result reflects the library, not the spike's wrappers.
- **`enospc`:** append with `always` on a 24 MiB tmpfs until an error, then reopen, verify acked frames, reclaim, and try to append again.

### 4.3 Bugs found and fixed during the spike

None of these affect the reported numbers, either because the affected path was not exercised by that measurement or because the measurement was re-run.

- **Custom log:** the read-ahead cache could hold bytes past the written end of a preallocated active segment, so a reader tailing the live segment could see stale zeros. This was found by an independent review. Benchmarks and verifiers only read after a reopen, so no measurement was affected. Fixed.
- **SQLite:** `rusqlite::execute_batch` steps a statement once, so `PRAGMA incremental_vacuum` freed about one page per call. Even when stepped to completion, files only shrink at checkpoint. SQLite churn and crash runs were re-run with the corrected reclaim path.
- **torn:** the first version could not fail on the resurrection path (section 5.7). Fixed and validated with a mutant.

## 5. Results

The complete tables are in the appendix. This section picks the comparable rows.

### 5.1 Append throughput and commit latency (closed loop, window 16 per producer)

**macOS, real flushes** (`F_FULLFSYNC`; redb always uses it):

| engine | record size | MB/s | commit p50 ms | p99 ms | p99.9 ms | fsyncs/s | frames per group |
|---|---|---|---|---|---|---|---|
| custom log | 200 B | 101.5 | 8.0 | 15.9 | 27.1 | 244 | 32.6 |
| SQLite | 200 B | 85.4 | 8.0 | 22.1 | 28.0 | 205 | 32.2 |
| redb | 200 B | 96.0 | 8.1 | 13.8 | 20.0 | 225 | 33.1 |
| custom log | 4 KiB | 497.3 | 8.0 | 16.2 | 19.5 | 234 | 32.3 |
| SQLite | 4 KiB | 199.6 | 20.0 | 34.1 | 57.0 | 93 | 32.4 |
| redb | 4 KiB | 380.4 | 11.0 | 16.0 | 24.0 | 176 | 32.8 |

All three engines are bound by the cost of `F_FULLFSYNC`, about 5 to 8 ms on the host SSD. Group commit amortizes it: about 32 frames, roughly 400 KB, per fsync. With plain macOS `fsync`, which is not durable against power loss, the custom log does 3.2 to 4.6 GB/s and SQLite about 0.56 to 0.59 GB/s. That gap reflects per-transaction CPU and I/O, not durability. `F_BARRIERFSYNC` gave 477 to 1496 MB/s but with p99 of 31 to 77 ms.

**Linux VM** (`fdatasync`):

| engine | record size | MB/s | commit p50 ms | p99 ms | fsyncs/s |
|---|---|---|---|---|---|
| custom log (fallocate) | 200 B | 539.3 | 1.4 | 3.0 | 1311 |
| custom log (no prealloc) | 200 B | 523.5 | 1.6 | 2.8 | 1243 |
| SQLite | 200 B | 359.7 | 1.6 | 7.1 | 869 |
| redb | 200 B | 322.2 | 2.3 | 4.9 | 774 |
| custom log (fallocate) | 4 KiB | 1036.6 | 4.0 | 7.2 | 487 |
| custom log (no prealloc) | 4 KiB | 1118.4 | 3.5 | 7.4 | 514 |
| SQLite | 4 KiB | 353.7 | 11.9 | 17.4 | 166 |
| redb | 4 KiB | 663.8 | 6.3 | 9.2 | 312 |

On Linux, `fsync` and `fdatasync` performed the same for the custom log. Preallocation with `fallocate` made no measurable difference on this virtual disk.

**Run-to-run variance:** a later macOS rerun of custom log `always`/`full` with 200 B records gave 143 MB/s and 6.0 / 11.9 ms, against 101.5 MB/s and 8.0 / 15.9 ms in the matrix. Treat macOS flush-bound numbers as roughly ±40%. Each configuration ran once (section 6).

### 5.2 Commit latency at a fixed offered load

10 MB/s offered is the architecture review's reference workload (50 sensors at 1 kHz with 200 B samples). Records are 200 B, 64 per frame.

| engine | fsync | macOS (full flush) p50 / p99 / max ms | Linux VM p50 / p99 / max ms |
|---|---|---|---|
| custom log | always | 5.9 / 9.5 / 15.2 | 2.4 / 4.6 / 13.3 |
| SQLite | always | 6.5 / 18.4 / 24.4 | 2.3 / 9.0 / 15.3 |
| redb | always | 7.3 / 10.7 / 17.2 | 2.9 / 6.1 / 36.2 |
| custom log | interval(50ms) | 30.6 / 56.4 / 67.2 | 28.5 / 53.9 / 56.9 |
| SQLite | interval(50ms) | 34.1 / 64.3 / 79.3 | 30.0 / 59.0 / 64.2 |
| redb | interval(50ms) | 32.8 / 64.2 / 141.3 | 29.4 / 55.0 / 56.4 |

Even the slowest configuration measured (SQLite, macOS full flush, window 16, 85 MB/s) has more than 8x headroom over this load. Commit latency is dominated by the flush, or by the interval, not by framing or engine CPU.

### 5.3 Interval mode, ack windows, and write amplification at light load

- **The 50 ms interval caps throughput, regardless of engine.** In the closed-loop runs (64 frames in flight in total), `interval(50ms)` delivered 14.2 to 14.9 MB/s for every engine on both OSes. That is close to the expected ceiling of 64 frames × 12.8 KB / 50 ms ≈ 16 MB/s (the real period is the interval plus the fsync).
  - The general ceiling is: per-source ack rate ≤ inflight window / (interval + fsync latency).
  - Derived, not measured: with a broker inflight window of 20 single-message publishes, that is about 400 messages/s per session at 50 ms.
- **Engine capacity in interval mode**, with a 4,096-frame window: custom log 2,812 MB/s, redb 1,041 MB/s, SQLite 556 MB/s on Linux. On macOS with full flushes: 1,339, 412 and 303 MB/s.
- **Unbatched small messages under `always` amplify writes heavily.** At one 200 B message per frame and 5,000 messages/s, groups average 1.8 to 4 frames. Each fsync rewrites a partly filled page plus metadata. OS-reported write amplification:

  | engine | Linux | macOS |
  |---|---|---|
  | custom log | 12.8x | 6.2x |
  | SQLite | 16.1x | 12.1x |
  | redb | 69.2x | 30.1x |

  At 10 MB/s with batching, under `interval(50ms)`, the custom log is at 1.03x.

**Interval sweep** (macOS, custom log, `F_FULLFSYNC`; `results/macos-interval-sweep.jsonl`):

| fsync | 1 MB/s unbatched (200 B × 1): commit p50 / p99 ms | fsyncs/s | OS write amp | closed loop (window 16, 200 B × 64): MB/s | p50 ms |
|---|---|---|---|---|---|
| always | 4.8 / 8.6 | 316 | 6.24 | 143.4 | 6.0 |
| interval(5ms) | 5.8 / 9.1 | 197 | 4.34 | 151.8 | 5.0 |
| interval(10ms) | 8.6 / 13.8 | 99 | 2.78 | 72.7 | 11.0 |
| interval(50ms) | 28.7 / 53.2 | 20 | 1.54 | 16.1 | 50.2 |

### 5.4 Replay from a cursor

Warm page cache. Every engine replays well above any plausible sink rate:

| engine | Linux | macOS |
|---|---|---|
| custom log | 11.0 to 11.5 GB/s | 13.1 to 14.1 GB/s |
| SQLite | 12.9 to 13.6 GB/s | 7.3 to 8.8 GB/s |
| redb | 3.6 to 4.2 GB/s | 5.1 to 7.1 GB/s |

The custom log includes CRC verification on read. redb's RSS reached 1.0 to 2.0 GiB while reading because of its default cache. Cold-cache replay was not measured.

### 5.5 Disk reclaim and steady-state disk usage

Churn: 2 GiB appended with a 256 MiB consumer lag.

| engine | steady disk avg / max | after full catch-up | reclaim on full catch-up | time in delete during run | OS write amp |
|---|---|---|---|---|---|
| custom log, Linux (16 MiB segments, fallocate) | 272 / 272 MiB | 16.0 MiB | 18 ms | 129 ms | 1.03 |
| custom log, macOS (16 MiB, no prealloc) | 265 / 272 MiB | 5.6 MiB | 67 ms | 544 ms | 1.03 |
| SQLite, Linux | 257 / 257 MiB | 0.6 MiB | 484 ms | 4,491 ms | 3.06 |
| SQLite, macOS | 266 / 273 MiB | 0.6 MiB | 631 ms | 11,170 ms | 3.20 |
| redb, Linux | 328 / 328 MiB | 328 MiB | 8 ms | 241 ms | 1.31 |
| redb, macOS | 328 / 328 MiB | 328 MiB | 25 ms | 2,197 ms | 1.44 |

- **Custom log:** disk stays at the backlog plus one segment. That is the PR7 acceptance shape, although the spike has no budget enforcement.
- **SQLite:** tight on disk, but only because reclaim does all three of:
  - delete;
  - `incremental_vacuum` stepped to completion (rusqlite's `execute_batch` steps once, which frees about one page);
  - `wal_checkpoint(TRUNCATE)`, because in WAL mode freed pages only leave the files at checkpoint.

  Before the fix, SQLite still held about 274 MiB after full catch-up. The cost of doing it correctly is time (53% of the macOS churn run and 61% of the Linux run spent deleting) and writes (3.1 to 3.2x).
- **redb:** reuses freed pages, so usage plateaus at about 1.3x the backlog. The file never shrinks without `compact()`, which was not tested.

### 5.6 Recovery time vs backlog

Open plus first query after kill -9.

| engine | Linux, page cache dropped | macOS, warm cache |
|---|---|---|
| custom log | 11.5 / 9.3 / 7.0 ms at 128 / 576 / 2,174 MiB | 5.2 / 6.0 / 6.3 ms at 435 / 831 / 2,413 MiB |
| SQLite | 1.6 / 2.0 / 6.4 ms at 119 / 531 / 2,076 MiB | 14.5 / 1.2 / 3.5 ms at 139 / 604 / 2,133 MiB |
| redb (default) | 43.7 / 202 / 720 ms at 116 / 683 / 2,599 MiB | 50 / 169 / 796 ms at 88 / 651 / 2,583 MiB |
| redb (`quick_repair`) | 1.7 / 1.8 / 2.7 ms | 13.2 / 13.7 / 15.1 ms |

- **Custom log:** cost is bounded by the last segment (at most 64 MiB scanned), not by the backlog.
- **redb (default):** cost is O(database size), because it rebuilds allocator state by walking the database. `quick_repair` makes recovery near-instant at the price of a two-phase commit.
- **Torn tails:** none of the kill -9 recoveries found a torn tail (`torn=false`). A SIGKILL does not interrupt an in-flight `pwrite` to a regular file. Torn tails are therefore covered by the `torn` and LazyFS tests, not by kill -9.

### 5.7 Crash consistency

**kill -9 loops.** Each row is one configuration on one OS. "Failures" means any acknowledged loss, gap, stale or corrupt frame, cursor regression, deletion past the cursor, or open/verify error.

| engine | fsync | macOS iterations / failures | Linux iterations / failures |
|---|---|---|---|
| custom log | always | 300 / 0, plus **1,000 / 0** in a separate run (572,820 ACKs, 77 kills during open) | 300 / 0 |
| custom log | interval(50ms) | 200 / 0 | 200 / 0 |
| custom log | never | 100 / 0 | 100 / 0 |
| SQLite | always | 300 / 0 | 300 / 0 |
| SQLite | interval(50ms) | 200 / 0 | 200 / 0 |
| SQLite | never | 100 / 0 | 100 / 0 |
| redb | always | 300 / 0 | 300 / 0 |
| redb | interval(50ms) | 200 / 0 | 200 / 0 |
| redb | never | stopped at 10 failures: acknowledged loss | stopped at 10 failures: acknowledged loss |

- About 5% of kills landed inside open and recovery (0 to 5 ms).
- In the `always` runs, the parent observed 8,939 to 198,694 ACKs per configuration (mostly 38,000 to 199,000; redb on macOS is the low one because each commit pays `F_FULLFSYNC`).
- The `interval` runs observed only 278 to 391 ACKs, because children rarely lived long past the first interval. That is a weak test of interval-specific logic.
- **redb `never`:** `Durability::None` commits live in redb's process memory, so a plain process crash loses them. The custom log and SQLite in `never` mode had already handed data to the kernel, which survives a process crash. After the first loss, later redb `never` failures cascade from the stale acknowledged high-water mark.
- **This also serves as a positive control:** the verifier does detect acknowledged loss.

**LazyFS** (Linux, privileged container, LazyFS at `fa7d32e`). After every kill, `lazyfs::clear-cache` discards all file data not yet fsynced, which simulates power loss at the kill point:

| engine | always | interval(50ms) | never (positive control) |
|---|---|---|---|
| custom log | 200 / 0 failures | 100 / 0 | acknowledged loss on the first iteration (expected) |
| SQLite | 200 / 0 | 100 / 0 | **"database disk image is malformed"** after the first iteration: corruption, not just loss |
| redb | 200 / 0 | 100 / 0 | acknowledged loss (expected) |

LazyFS models loss of un-fsynced data only. Create, rename, unlink and truncate pass straight through, so directory-fsync ordering is **not** exercised.

**Torn-tail and corruption cases** (custom log, deterministic; `results/macos-torn.json`). All 2,248 cases passed, with and without preallocation:

- truncation at each of the 560 byte offsets of the last frame;
- one bit flipped at each of its 560 bytes;
- 6 zeroed-middle-frame cases;
- 1 garbage-tail case.

In each case the damaged frame and everything after it was dropped, survivors were byte-exact, and new appends were never mixed with stale frames.

The first version of the zeroed-middle case was vacuous. This was confirmed with a mutant that skips truncation and appends in place: the mutant passed. Re-appended frames had exactly the old frames' sizes and overwrote the whole stale region. The fixed case appends exactly one frame. Against the mutant it now fails, because the old frames `k+1..N` become valid again after one append ("next_seq 25 expected 3"). The real code passes. **This is direct evidence that "truncate, then start a new segment" is load-bearing.** Appending in place at the recovered end resurrects stale frames whenever a re-appended frame lines up with the lost one.

### 5.8 Failure behavior

**fsync returns EIO** (Linux, `LD_PRELOAD` shim; results were identical for "once" and "sticky"):

| engine | faulted commit | next 2 commits on the same handle | after reopen: seqs 1..6 present |
|---|---|---|---|
| custom log | `Err` (EIO) | refused: poisoned | 1, 2, 3, 4 |
| SQLite (raw) | `Err("disk I/O error")` | **`Ok`, `Ok`** | 1, 2, 3, 5, 6 (4 rolled back) |
| redb (raw) | `Err` (EIO) | refused: "Previous I/O error occurred. Please close and re-open" | 1, 2, 3, 4 |

- **SQLite does not latch after a failed fsync.** Thadam would have to treat `SQLITE_IOERR_FSYNC` as fatal for the connection itself. This is the fsyncgate pattern.
- **Seq 4 reappears after reopen for the custom log and redb.** The shim leaves the page cache intact, so the reopened log reads the never-acknowledged frame from cache. That is allowed (it was never acked), but it shows that **"re-verify from disk" after a failed fsync reads the page cache, not the device**, unless it uses `O_DIRECT` or evicts pages first.
- No real device error, dm-flakey, or dm-error was tried. Only the API reaction is tested.

**ENOSPC** (24 MiB tmpfs, not the loop device the ADR asks for):

| engine | first error | reopen | acked data intact | delete consumed | append after reclaim |
|---|---|---|---|---|---|
| custom log (1 MiB segments, fallocate) | at segment roll (fallocate), before any frame bytes were written | **fails** with ENOSPC | not checked (could not open) | not reached | not reached |
| custom log (no prealloc) | `pwrite` ENOSPC | **fails** with ENOSPC | not checked | not reached | not reached |
| SQLite | `SQLITE_FULL` | ok | yes | **fails** (DELETE needs space) | fails |
| redb | ENOSPC | ok | yes | **fails** (COW needs space) | fails |

- No engine corrupted or lost acknowledged data.
- **None could make progress on a full disk.** For the custom log this is purely a spike design flaw: open always creates and preallocates a new segment, and its reclaim path (`unlink`) needs no space. SQLite and redb need free space to delete.
- **redb file creation is not crash-atomic.** A kill -9 during the first `Database::create` left a file with a zeroed magic number, and every later open failed with "Not a redb database: magic number mismatch". This happened in 14 of 30 reproductions with 0 to 4 ms kill delays. The spike works around it by creating the database under a temporary name and renaming it.

### 5.9 Memory

- **Custom log:** peak RSS 3 to 8 MiB in every test.
- **SQLite:** 6 to 8 MiB.
- **redb with its default cache:** 0.2 to 1.4 GiB while appending and up to 2.0 GiB while replaying.
- **redb with a 32 MiB cache:** 37 to 96 MiB. Throughput was the same for 200 B records and 21% lower for 4 KiB records on Linux.

## 6. Limitations (read before using the numbers)

- **No reference hardware.** There was no x86 NVMe, no ARM eMMC, and no SD card. The ADR's hardware checklist items remain open.
- **The Linux disk is virtual.** Linux runs used ext4 on a virtio disk inside Docker Desktop, backed by a file on the host's SSD. Linux fsync took about 1.4 ms per group, against about 8 ms for `F_FULLFSYNC` on the host. That suggests guest flushes do not reach the drive with full-flush semantics, but the hypervisor's behavior was not confirmed. Use the Linux numbers to compare engines on the Linux code path, not as device performance.
- **macOS flush semantics.** Plain macOS `fsync` rows (`data`) are not durable against power loss. Only the `full` rows (and redb, which uses `F_FULLFSYNC`) are.
- **Write amplification is process-level.** "OS write amp" counts bytes attributed to this process by the OS. It excludes flash FTL amplification and, on Linux, ext4 journal (jbd2) writes, which belong to a kernel thread. `/proc/diskstats` device-level amplification was not measured, because inside the VM it would describe the virtual disk.
- **Single runs.** Each configuration ran once (10 s measured after 1 s warm-up) on a shared laptop with other containers and builds active. Macro differences such as 2x or 10x are reliable. Differences under about 40% on macOS flush-bound rows are not.
- **Most reads and recoveries used a warm page cache.** Only Linux recovery used dropped caches.
- **Coverage of the kill -9 loop.** A SIGKILL never tears a write, and LazyFS does not drop metadata operations. So directory-fsync ordering (segment creation, rename, unlink) was only reasoned about, not tested under loss. No hardware power-cut rig was used.
- **Not tested:**
  - concurrent readers racing the writer or a segment unlink;
  - multiple consumers;
  - pinned-cursor eviction and `Gap` surfacing;
  - backwards clock steps;
  - disk-budget enforcement;
  - the `LOCK` file;
  - `on_full`;
  - quarantine of corrupt sealed segments;
  - cold-cache replay.
- **The spike reader serves written-but-unsynced frames.** The design requires readers to stop at the durable committed end. The crash consumer only commits cursors up to acknowledged seqs, so this did not affect results, but production must enforce it.
- **Harness caveats found by an independent review:**
  - Closed-loop latency starts when a frame is submitted, so time a producer spends waiting for a free window slot is not counted. Only the `--rate-mbps` rows correct for this.
  - Frames resolved after the measurement window are dropped from the statistics.
  - `syncs_per_s` divides by measured time plus warm-up.
  - The `fill` child behind the `recovery` test syncs every group even in `interval` mode. That affects how the backlog is built, not the measured recovery time.
  - On every segment roll the custom log does three fsyncs (old segment, new segment, directory), even in `interval` and `never` modes. These are not counted in `syncs_per_s` (about one roll per 16 to 64 MiB).
  - In `interval` mode, SQLite and redb keep groups in one open transaction until the interval commit, while the custom log calls `write()` once per group. Both are durable after the sync, but SQLite and redb do less work between syncs, which flatters them in the interval-mode benchmarks.
- **Spike code is not production code.** The custom log is about 590 lines with no concurrency, retention, budget, or lock handling. The architecture review's estimate of 3,000 to 4,000 lines including tests still looks right.
- **Frame payload encoding.** Frame payloads were a simple length-prefixed record list, not the prost `RawFrame`. Framing overhead (frame header plus per-record length prefix) was 2.3% for 200 B records and 0.2% for 4 KiB records, so encoding choice barely affects these results. Prost encode and decode CPU was not measured.

## 7. Recommendation for ADR-0004 and D1

### 7.1 Engine (D2): custom segmented log, SQLite as the documented fallback

- **Correctness parity in everything tested.** Custom log and SQLite tied. The custom log had zero failures in 2,200 kill -9 iterations across both OSes (1,600 macOS, 600 Linux), 300 LazyFS iterations, and 2,248 deterministic torn-tail cases. D2's condition for falling back to SQLite ("if S1 shows custom-log correctness risk") was not triggered. The residual risk is code that is not written yet (section 6), not the format or the recovery rule.
- **Operational behavior favors the custom log**, as product.md section 12 asks to be weighed:
  - Write amplification is about 1.0x against about 2x for SQLite, rising to about 3.1x once reclaim is included. On flash that alone roughly halves device wear.
  - Reclaim is an `unlink` that costs milliseconds, needs no free space, and gives predictable disk usage (backlog plus one segment). SQLite needs vacuum and checkpoint, spending 53 to 61% of churn time.
  - Recovery is bounded by one segment.
  - RSS is single-digit MiB.
- **SQLite as fallback:** mature, and its recovery and replay were excellent. But its writes double, reclaim is expensive and easy to get wrong, it does not latch on a failed fsync, `synchronous=OFF` can corrupt the database on power loss, and it has a single writer lock: cursor commits from sink threads would contend with the WAL writer.
- **redb: not for the bulk queue.**
  - Default recovery is O(database size), or every commit pays a two-phase commit with `quick_repair`.
  - The file never shrinks.
  - The default cache costs about 1 GiB of RSS.
  - `Durability::None` loses data on a plain process crash.
  - Database creation is not crash-atomic.
  - It is still reasonable for small metadata if needed, with a set cache size and temp-plus-rename creation.

### 7.2 On-disk format and behavior to freeze

1. **Frame header, 24 bytes, little-endian** (settles O9): `magic u16 | version u8 | flags u8 | len u32 | frame_seq u64 | reserved u32 (must be 0) | crc32c u32`. CRC32C covers header bytes `[0, 20)` followed by the payload.
   - Recovery and readers check magic, version, flags, reserved, `len` against `frame_max_bytes` and the remaining file size, `frame_seq` continuity, and the CRC. This caught every single-bit flip and every truncation point in the tests.
   - **Do not add a separate header CRC**, and **do not resynchronize by scanning for the next magic inside a sealed segment.** Quarantine from the first bad frame and surface a `Gap`. Resync is the only feature that would need a header CRC, and it risks accepting a stale frame.
   - The reserved word keeps 8-byte alignment and can carry a header CRC under a version bump if ever needed.
2. **End-of-data marker:** an all-zero 24-byte header. The magic is non-zero, so no `len = 0` sentinel is needed. Count "zero tail" and "torn tail" (non-zero garbage) as separate metrics.
3. **Segment header, 64 bytes:** magic at `[0, 8)`, `format_version u16` at `[8, 10)`, reserved u16, `epoch u64` at `[12, 20)`, `base_seq u64` at `[20, 28)`, `created_unix u64` at `[28, 36)`, zeros to byte 60, then CRC32C of `[0, 60)` at `[60, 64)`.
   - Create segments as temp file, header, (prealloc), fsync, rename, then directory fsync, so a half-written segment never has a valid name.
   - Truncate a segment to its data when sealing it.
4. **Keep "truncate at the first invalid frame, fsync, then append to a fresh segment"** exactly as specified. The mutant in 5.7 shows that appending in place resurrects stale frames. Reusing an empty last segment after truncating it to its header was safe in every test.
5. **New requirement: open and recovery must not allocate.**
   - Create the next segment lazily on the first append, and order deletion before any allocation, so that consumers can drain and reclaim on a full disk.
   - Keep `max_disk` enforcement well below filesystem capacity so ENOSPC is a fallback, not the normal path.
6. **New requirement: re-verification after a failed fsync must not trust the page cache.**
   - Either re-read with `O_DIRECT` or after `posix_fadvise(DONTNEED)`, or treat everything after the last successful fsync offset as unacknowledged: seal there and let sources redeliver.
   - Never retry the fsync. The spike's poison flag behaved as the ADR requires: no ticket resolved Ok after an EIO.
7. **Preallocation:**
   - Do not preallocate on macOS. On APFS, `F_PREALLOCATE` plus `ftruncate` made appends 1.3x to 2x slower in the matrix (200 B: 1,604 against 3,202 MB/s, p99 4.7 against 0.8 ms; 4 KiB: 3,456 against 4,575 MB/s; plain fsync) and doubled bytes written (2.03 to 2.08x against 1.01 to 1.04x).
   - On Linux, `fallocate` made no measurable difference on the VM. Keep it optional and decide on reference hardware.
   - Recovery must treat a zero tail beyond the data as a clean end. It did, in every test.
8. **New requirement: never reuse sequence numbers at or below a durable cursor.**
   - Readers and `commit_cursor` must be bounded by the durable end, i.e. the last successful fsync, not the written end.
   - On open, if a cursor is at or beyond `next_seq`, which can happen after power loss in `interval` or `never` mode, either start the next segment at cursor + 1 or refuse to open.
   - The spike does not enforce this. Its crash consumer only commits cursors up to acked seqs, so no test hit it. SQLite and redb avoid it here because their open computes `max(last row, max cursor) + 1`.
9. **New requirement: a bad segment header must fail the open.** The spike quarantines the segment and carries on. If the quarantined segment was the last one, `next_seq` rewinds. The only safe exception is an empty last segment.
10. **Cursor files** (temp file, fsync, rename, directory fsync) were correct in every crash test. They cost two fsyncs per commit, so keep the plan's 200 ms batching.

### 7.3 fsync default (D3)

- **Measured trade-off:**
  - `always` (group commit) gives the lowest latency (2.4 ms p50 on the VM, 4.8 to 6 ms with real macOS flushes) and full throughput.
  - At light, unbatched load it issues one fsync per message, so the OS writes 6x to 13x the payload for the custom log, and more for SQLite and redb.
  - `interval(50ms)` keeps write amplification near 1x, but caps throughput at the inflight window divided by 50 ms and adds about 30 ms p50 and 55 to 64 ms p99 to every ack.
- **Recommendation:** default to a short bounded group commit, provisionally **`interval(10ms)`**.
  - In the macOS sweep it gave 99 fsyncs/s, 2.8x write amplification at 1 MB/s unbatched, 8.6 ms p50, and a window cap 4.5x higher than 50 ms.
  - Keep `always` and longer intervals as explicit choices, and document producer batching.
- **Re-measure on reference eMMC before freezing.** If its flush latency is at or above 10 ms, `interval` and `always` converge there.
- **For ADR-0006:** export the formula `max acks/s per source ≈ inflight / (interval + fsync latency)` and use it for the ack-window curve.

### 7.4 D1 capture-first

- **What the evidence supports:** the WAL side is cheap. A raw-envelope frame per source batch adds about 2% framing for 200 B records. Commit-to-ack is one group commit. The 10 MB/s reference load is 2% (Linux VM) to 10% (macOS, full flush) of the custom log's measured `always` throughput, using under 8 MiB of RSS. Replay at 11 GB/s or more makes "replay is a cursor rewind" practical.
- **What it does not cover:** S1 did not prototype Option A (post-transform WAL), did not measure chain CPU for 1 to 3 sinks, and did not measure WAL growth during a sink outage. D1 needs those from other work. Nothing in S1 argues against capture-first.

### 7.5 Still open for ADR-0004

- x86 NVMe and ARM eMMC runs, including `/proc/diskstats` device-level write amplification.
- The ENOSPC test on a loop device with the lazy-allocation fix.
- A directory-metadata loss test, for example dm-flakey or a power-cut rig.
- Reader-versus-unlink and multi-consumer tests.
- Pinned-cursor eviction, clock-step, and budget tests, which belong in PR6 and PR7.
- Numeric pass criteria for the `[OPEN]` items. Proposed from this data:
  - commit p99 at the reference load at or below 2x fsync latency plus the interval;
  - steady disk usage at or below the backlog plus one segment;
  - recovery at or below 50 ms for a 64 MiB last segment on eMMC;
  - zero failures in at least 1,000 kill -9 iterations and 300 LazyFS iterations.

## 8. How to reproduce

Run from `spikes/s1-wal/`. Data goes under `/tmp` so that multi-GB files stay out of the repository.

**macOS:**

```sh
cargo build --release
./target/release/s1-wal torn --dir /tmp/s1wal/torn > results/macos-torn.json
./run-matrix.sh macos /tmp/s1wal            # sections: append interval_capacity rate read churn recovery crash
./target/release/s1-wal crash --engine seglog --fsync always --prealloc false \
  --segment-bytes 262144 --iters 1000 --seed 7 --dir /tmp/s1wal/c1000 > results/macos-crash-1000.jsonl
for m in always interval:5ms interval:10ms interval:50ms; do
  ./target/release/s1-wal bench --engine seglog --fsync $m --sync-kind full --prealloc false \
    --rec-size 200 --batch 1 --rate-mbps 1 --window 256 --secs 10 --dir /tmp/s1wal/run >> results/macos-interval-sweep.jsonl
  ./target/release/s1-wal bench --engine seglog --fsync $m --sync-kind full --prealloc false \
    --rec-size 200 --batch 64 --window 16 --secs 10 --dir /tmp/s1wal/run >> results/macos-interval-sweep.jsonl
done
python3 report_tables.py results/macos.jsonl
```

**Linux** (Docker). For networks that intercept TLS inside containers, the image builds offline: crates and LazyFS sources are fetched on the host and passed as named build contexts.

```sh
cargo vendor --locked /tmp/s1wal-vendor
git clone --depth 1 https://github.com/dsrhaslab/lazyfs.git /tmp/s1wal-lazyfs        # run used fa7d32e
git clone --depth 1 --branch v3.7.1 https://github.com/ToruNiina/toml11 /tmp/s1wal-lazyfs/_deps/toml
curl -sSL https://github.com/gabime/spdlog/archive/v1.10.0.tar.gz | tar xz -C /tmp/s1wal-lazyfs/_deps \
  && mv /tmp/s1wal-lazyfs/_deps/spdlog-1.10.0 /tmp/s1wal-lazyfs/_deps/spdlog
docker build -f docker/Dockerfile --build-context vendor=/tmp/s1wal-vendor \
  --build-context lazyfs=/tmp/s1wal-lazyfs -t s1wal-linux:latest .
docker volume create s1wal-data
docker run -d --name s1wal-linux --privileged --device /dev/fuse \
  -v s1wal-data:/data --tmpfs /small:size=24m s1wal-linux:latest sleep infinity
docker exec s1wal-linux ./run-matrix.sh linux /data/s1
docker exec s1wal-linux ./fault-tests.sh linux-faults
docker exec s1wal-linux ./target/release/s1-wal enospc --engine seglog --prealloc false \
  --segment-bytes 1048576 --dir /small/x
docker exec s1wal-linux ./lazyfs-crash.sh linux-lazyfs 200
docker cp s1wal-linux:/spike/results/. results/
docker rm -f s1wal-linux && docker volume rm s1wal-data && docker rmi s1wal-linux:latest
```

All containers, volumes and images created for this spike were removed afterwards.

## Appendix A: complete macOS tables (`results/macos.jsonl`)

Labels: `seglog` is the custom log. `(data)` is plain `fsync()` on macOS, which is **not** power-loss durable. `(full)` is `F_FULLFSYNC`. `(barrier)` is `F_BARRIERFSYNC`. redb always uses `F_FULLFSYNC`.

### Append, 200 B records x 64 per frame, 4 producers, window 16

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data) | always | 3202.1 | 16,010,733 | 0.2 | 0.8 | 3.0 | 38.2 | 6654 | 1.04 | 5 |
| seglog (full) | always | 101.5 | 507,270 | 8.0 | 15.9 | 27.1 | 32.6 | 244 | 1.05 | 5 |
| seglog (barrier) | always | 476.7 | 2,383,590 | 0.6 | 77.4 | 112.3 | 35.9 | 1035 | 1.05 | 5 |
| seglog (data) | interval:50ms | 14.3 | 71,680 | 58.1 | 60.1 | 60.3 | 33.8 | 18 | 1.04 | 4 |
| seglog (full) | interval:50ms | 14.4 | 72,090 | 57.4 | 63.0 | 81.3 | 36.0 | 18 | 1.04 | 4 |
| seglog (data) | never | 5598.5 | 27,992,525 | 0.1 | 1.2 | 2.8 | 38.4 | 0 | 1.02 | 5 |
| seglog (data, prealloc) | always | 1604.0 | 8,019,968 | 0.2 | 4.7 | 9.3 | 37.8 | 3322 | 2.08 | 5 |
| sqlite (data) | always | 563.6 | 2,817,798 | 0.8 | 5.5 | 10.0 | 36.0 | 1213 | 2.17 | 7 |
| sqlite (full) | always | 85.4 | 426,784 | 8.0 | 22.1 | 28.0 | 32.2 | 205 | 2.17 | 7 |
| sqlite (data) | interval:50ms | 14.3 | 71,680 | 57.0 | 64.0 | 65.6 | 34.9 | 18 | 2.11 | 7 |
| sqlite (full) | interval:50ms | 14.2 | 70,861 | 57.0 | 70.0 | 131.8 | 34.9 | 18 | 2.12 | 7 |
| sqlite (data) | never | 767.2 | 3,835,917 | 0.5 | 5.5 | 9.3 | 32.4 | 0 | 0.37 | 7 |
| redb | always | 96.0 | 480,205 | 8.1 | 13.8 | 20.0 | 33.1 | 225 | 1.60 | 1389 |
| redb | interval:50ms | 14.3 | 71,270 | 59.0 | 63.0 | 63.0 | 35.4 | 18 | 1.63 | 265 |
| redb | never | 309.7 | 1,548,301 | 1.7 | 7.7 | 13.2 | 34.5 | 0 | 1.38 | 713 |
| redb (cache 32 MiB) | always | 102.4 | 512,115 | 8.0 | 13.9 | 16.1 | 33.3 | 237 | 1.60 | 49 |

### Append, 200 B records x 64 per frame, 4 producers, window 4096

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (full) | interval:50ms | 1338.6 | 6,693,235 | 118.5 | 166.0 | 175.6 | 321.0 | 18 | 1.02 | 8 |
| sqlite (full) | interval:50ms | 302.8 | 1,514,093 | 418.6 | 573.4 | 589.3 | 320.8 | 15 | 2.25 | 8 |
| redb | interval:50ms | 411.7 | 2,058,515 | 638.5 | 1073.2 | 1323.0 | 314.0 | 4 | 1.29 | 1393 |

### Append, 4096 B records x 16 per frame, 4 producers, window 16

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data) | always | 4575.2 | 1,116,986 | 0.9 | 1.9 | 9.0 | 36.5 | 1884 | 1.01 | 8 |
| seglog (full) | always | 497.3 | 121,411 | 8.0 | 16.2 | 19.5 | 32.3 | 234 | 1.01 | 8 |
| seglog (barrier) | always | 1496.1 | 365,269 | 1.6 | 31.3 | 53.9 | 33.2 | 667 | 1.01 | 8 |
| seglog (data) | interval:50ms | 72.6 | 17,715 | 59.6 | 62.8 | 64.4 | 36.5 | 17 | 1.00 | 8 |
| seglog (full) | interval:50ms | 72.6 | 17,715 | 59.0 | 66.2 | 70.1 | 35.5 | 17 | 1.00 | 8 |
| seglog (data) | never | 6646.2 | 1,622,614 | 0.5 | 1.8 | 8.9 | 36.2 | 0 | 1.00 | 8 |
| seglog (data, prealloc) | always | 3455.5 | 843,640 | 0.9 | 6.2 | 18.2 | 36.4 | 1447 | 2.03 | 8 |
| sqlite (data) | always | 589.4 | 143,890 | 6.8 | 13.7 | 26.4 | 34.4 | 260 | 2.07 | 8 |
| sqlite (full) | always | 199.6 | 48,736 | 20.0 | 34.1 | 57.0 | 32.4 | 93 | 2.07 | 8 |
| sqlite (data) | interval:50ms | 73.0 | 17,818 | 58.4 | 64.1 | 64.7 | 36.4 | 18 | 2.05 | 8 |
| sqlite (full) | interval:50ms | 74.2 | 18,125 | 57.7 | 69.1 | 81.2 | 34.1 | 18 | 2.06 | 8 |
| sqlite (data) | never | 907.4 | 221,539 | 4.0 | 7.3 | 12.7 | 32.2 | 0 | 0.71 | 8 |
| redb | always | 380.4 | 92,878 | 11.0 | 16.0 | 24.0 | 32.8 | 176 | 2.06 | 1353 |
| redb | interval:50ms | 73.8 | 18,022 | 57.3 | 64.6 | 67.8 | 34.3 | 18 | 2.07 | 1279 |
| redb | never | 965.5 | 235,723 | 1.8 | 5.9 | 973.3 | 33.7 | 0 | 2.15 | 608 |
| redb (cache 32 MiB) | always | 400.3 | 97,731 | 10.1 | 16.0 | 24.7 | 33.1 | 185 | 2.06 | 96 |

### Append, 4096 B records x 16 per frame, 4 producers, window 4096

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (full) | interval:50ms | 1364.4 | 333,107 | 436.5 | 586.8 | 596.0 | 64.0 | 17 | 1.00 | 8 |
| sqlite (full) | interval:50ms | 353.6 | 86,323 | 1611.8 | 1741.8 | 1775.6 | 63.9 | 20 | 2.08 | 8 |
| redb | interval:50ms | 798.6 | 194,970 | 1061.9 | 1611.8 | 1614.8 | 64.0 | 5 | 2.01 | 1205 |

### Open-loop offered load (latency from scheduled send time)

| engine | fsync | offered | record x batch | achieved MB/s | commit p50 ms | p99 ms | p99.9 ms | max ms | frames/group | OS write amp |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (full) | always | 10 MB/s | 200 B x 64 | 10.00 | 5.9 | 9.5 | 13.4 | 15.2 | 2.6 | 1.47 |
| sqlite (full) | always | 10 MB/s | 200 B x 64 | 9.99 | 6.5 | 18.4 | 22.1 | 24.4 | 2.9 | 2.90 |
| redb | always | 10 MB/s | 200 B x 64 | 10.00 | 7.3 | 10.7 | 14.6 | 17.2 | 3.2 | 3.98 |
| seglog (full) | interval:50ms | 10 MB/s | 200 B x 64 | 10.03 | 30.6 | 56.4 | 62.1 | 67.2 | 1.5 | 1.05 |
| sqlite (full) | interval:50ms | 10 MB/s | 200 B x 64 | 10.01 | 34.1 | 64.3 | 74.1 | 79.3 | 1.5 | 2.15 |
| redb | interval:50ms | 10 MB/s | 200 B x 64 | 10.00 | 32.8 | 64.2 | 131.0 | 141.3 | 1.6 | 1.78 |
| seglog (full) | always | 1 MB/s | 200 B x 1 | 1.00 | 5.0 | 8.3 | 12.1 | 15.4 | 15.9 | 6.20 |
| sqlite (full) | always | 1 MB/s | 200 B x 1 | 1.00 | 5.1 | 11.0 | 20.0 | 25.9 | 16.2 | 12.13 |
| redb | always | 1 MB/s | 200 B x 1 | 1.00 | 5.3 | 8.6 | 12.2 | 16.0 | 16.9 | 30.14 |

### Replay from cursor 0 (warm page cache, engine reopened first)

| engine | record size | backlog MiB | MB/s | records/s | peak RSS MiB |
|---|---|---|---|---|---|
| seglog (data) | 200 | 1024 | 13123 | 65,615,890 | 5 |
| sqlite (data) | 200 | 1024 | 7273 | 36,364,058 | 6 |
| redb | 200 | 1024 | 5053 | 25,265,726 | 2078 |
| seglog (data) | 4096 | 1025 | 14052 | 3,430,590 | 8 |
| sqlite (data) | 4096 | 1025 | 8827 | 2,155,082 | 7 |
| redb | 4096 | 1025 | 7075 | 1,727,317 | 1166 |

### Churn: 2 GiB appended, consumer trails 256 MiB behind, fsync interval:50ms

| engine | MB/s | steady disk avg MiB | steady max MiB | after full consume MiB | full-consume reclaim ms | time in delete ms | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|
| seglog (full) | 213 | 265 | 272 | 5.6 | 67 | 544 | 1.03 | 3 |
| redb | 239 | 328 | 328 | 328.0 | 25 | 2197 | 1.44 | 450 |
| sqlite (full) | 100 | 266 | 273 | 0.6 | 631 | 11170 | 3.20 | 8 |

### Recovery after kill -9 vs backlog

| engine | backlog on disk MiB | open + first query ms | page cache dropped | detail |
|---|---|---|---|---|
| seglog (data) | 435 | 5.2 | False | segments=8 scanned_last_segment_bytes=54269888 truncated_bytes=0 torn=false quarantined=0 |
| sqlite (data) | 139 | 14.5 | False |  |
| redb | 88 | 50.1 | False |  |
| redb (quick_repair) | 85 | 13.2 | False |  |
| seglog (data) | 831 | 6.0 | False | segments=14 scanned_last_segment_bytes=67051584 truncated_bytes=0 torn=false quarantined=0 |
| sqlite (data) | 604 | 1.2 | False |  |
| redb | 651 | 169.0 | False |  |
| redb (quick_repair) | 649 | 13.7 | False |  |
| seglog (data) | 2413 | 6.3 | False | segments=39 scanned_last_segment_bytes=49241024 truncated_bytes=0 torn=false quarantined=0 |
| sqlite (data) | 2133 | 3.5 | False |  |
| redb | 2583 | 796.2 | False |  |
| redb (quick_repair) | 2582 | 15.1 | False |  |

### kill -9 loops

| engine | fsync | after kill | iterations | kills < 5 ms | ACKs observed | max retained frames | recovery p50 ms | p99 ms | failures |
|---|---|---|---|---|---|---|---|---|---|
| seglog | always | - | 300 | 15 | 198,694 | 217 | 1.1 | 12.3 | 0 |
| seglog | interval:50ms | - | 200 | 11 | 391 | 1303 | 2.4 | 11.2 | 0 |
| seglog | never | - | 100 | 2 | 85,968 | 210 | 1.0 | 14.0 | 0 |
| redb | always | - | 300 | 15 | 8,939 | 248 | 19.6 | 33.1 | 0 |
| redb | interval:50ms | - | 200 | 11 | 278 | 2636 | 20.3 | 34.2 | 0 |
| redb | never | - | 31 | 1 | 15,138 | 174 | 20.5 | 49.7 | 11: iter 4: acknowledged loss: last frame 3444 < max acked 3496 |
| sqlite | always | - | 300 | 15 | 190,059 | 216 | 0.8 | 3.7 | 0 |
| sqlite | interval:50ms | - | 200 | 11 | 322 | 1578 | 2.9 | 18.4 | 0 |
| sqlite | never | - | 100 | 2 | 87,399 | 201 | 1.1 | 5.0 | 0 |

## Appendix B: complete Linux tables (`results/linux.jsonl`, `linux-faults.jsonl`, `linux-lazyfs.jsonl`)

`(data)` is `fdatasync`. `(full)` is `fsync`. `prealloc` is `fallocate`.

### Append, 200 B records x 64 per frame, 4 producers, window 16

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data, prealloc) | always | 539.3 | 2,696,570 | 1.4 | 3.0 | 5.5 | 32.1 | 1311 | 1.03 | 4 |
| seglog (full, prealloc) | always | 549.7 | 2,748,256 | 1.4 | 3.1 | 5.1 | 32.1 | 1324 | 1.03 | 6 |
| seglog (data, prealloc) | interval:50ms | 14.7 | 73,728 | 55.4 | 62.9 | 64.2 | 32.8 | 18 | 1.03 | 5 |
| seglog (data, prealloc) | never | 2710.6 | 13,553,190 | 0.1 | 9.6 | 20.3 | 32.3 | 0 | 1.02 | 6 |
| seglog (data) | always | 523.5 | 2,617,408 | 1.6 | 2.8 | 4.3 | 32.1 | 1243 | 1.03 | 4 |
| sqlite (data) | always | 359.7 | 1,798,675 | 1.6 | 7.1 | 8.5 | 32.1 | 869 | 2.12 | 7 |
| sqlite (data) | interval:50ms | 14.9 | 74,547 | 55.2 | 72.2 | 72.4 | 33.7 | 18 | 2.09 | 7 |
| sqlite (data) | never | 1038.7 | 5,193,645 | 0.3 | 3.1 | 33.1 | 32.1 | 0 | 1.05 | 7 |
| redb | always | 322.2 | 1,611,091 | 2.3 | 4.9 | 8.4 | 32.1 | 774 | 1.36 | 1037 |
| redb | interval:50ms | 14.7 | 73,318 | 56.1 | 62.0 | 62.1 | 35.9 | 18 | 1.32 | 203 |
| redb | never | 579.9 | 2,899,488 | 0.7 | 7.3 | 15.1 | 33.3 | 0 | 1.20 | 543 |
| redb (cache 32 MiB) | always | 310.3 | 1,551,290 | 2.3 | 5.0 | 8.4 | 32.1 | 693 | 1.36 | 37 |

### Append, 200 B records x 64 per frame, 4 producers, window 4096

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data, prealloc) | interval:50ms | 2812.0 | 14,059,776 | 54.9 | 210.0 | 365.6 | 320.9 | 18 | 1.02 | 8 |
| sqlite (data) | interval:50ms | 555.9 | 2,779,603 | 268.0 | 400.9 | 404.5 | 320.9 | 14 | 2.06 | 8 |
| redb | interval:50ms | 1041.4 | 5,207,014 | 193.7 | 346.6 | 373.5 | 315.4 | 11 | 1.29 | 1041 |

### Append, 4096 B records x 16 per frame, 4 producers, window 16

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data, prealloc) | always | 1036.6 | 253,066 | 4.0 | 7.2 | 10.8 | 32.2 | 487 | 1.00 | 8 |
| seglog (full, prealloc) | always | 1123.1 | 274,184 | 3.7 | 7.4 | 11.1 | 32.3 | 523 | 1.00 | 8 |
| seglog (data, prealloc) | interval:50ms | 76.3 | 18,637 | 54.4 | 62.2 | 66.1 | 34.1 | 18 | 1.00 | 8 |
| seglog (data, prealloc) | never | 3118.8 | 761,426 | 0.6 | 16.7 | 21.4 | 32.1 | 0 | 1.00 | 8 |
| seglog (data) | always | 1118.4 | 273,050 | 3.5 | 7.4 | 13.2 | 32.1 | 514 | 1.00 | 8 |
| sqlite (data) | always | 353.7 | 86,349 | 11.9 | 17.4 | 19.5 | 32.1 | 166 | 2.04 | 7 |
| sqlite (data) | interval:50ms | 74.2 | 18,125 | 56.4 | 64.4 | 66.3 | 36.1 | 18 | 2.04 | 7 |
| sqlite (data) | never | 1245.8 | 304,155 | 2.8 | 14.5 | 53.3 | 32.1 | 0 | 1.04 | 8 |
| redb | always | 663.8 | 162,050 | 6.3 | 9.2 | 11.2 | 32.1 | 312 | 2.02 | 1061 |
| redb | interval:50ms | 75.5 | 18,432 | 56.2 | 63.4 | 63.9 | 33.8 | 18 | 2.01 | 1061 |
| redb | never | 982.9 | 239,974 | 1.8 | 4.8 | 1089.5 | 32.2 | 0 | 1.95 | 532 |
| redb (cache 32 MiB) | always | 523.3 | 127,770 | 8.3 | 10.7 | 11.6 | 32.4 | 241 | 2.02 | 37 |

### Append, 4096 B records x 16 per frame, 4 producers, window 4096

| engine | fsync | MB/s | records/s | commit p50 ms | p99 ms | p99.9 ms | frames/group | fsyncs/s | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data, prealloc) | interval:50ms | 2925.5 | 714,240 | 212.1 | 418.3 | 443.1 | 64.0 | 19 | 1.00 | 8 |
| sqlite (data) | interval:50ms | 595.6 | 145,408 | 989.2 | 1131.5 | 1164.3 | 64.0 | 16 | 2.03 | 7 |
| redb | interval:50ms | 927.4 | 226,406 | 958.5 | 1187.8 | 1194.0 | 64.0 | 7 | 2.15 | 1060 |

### Open-loop offered load (latency from scheduled send time)

| engine | fsync | offered | record x batch | achieved MB/s | commit p50 ms | p99 ms | p99.9 ms | max ms | frames/group | OS write amp |
|---|---|---|---|---|---|---|---|---|---|---|
| seglog (data, prealloc) | always | 10 MB/s | 200 B x 64 | 10.00 | 2.4 | 4.6 | 9.7 | 13.3 | 1.8 | 1.20 |
| sqlite (data) | always | 10 MB/s | 200 B x 64 | 10.00 | 2.3 | 9.0 | 13.8 | 15.3 | 1.8 | 2.79 |
| redb | always | 10 MB/s | 200 B x 64 | 10.00 | 2.9 | 6.1 | 31.0 | 36.2 | 1.9 | 2.46 |
| seglog (data, prealloc) | interval:50ms | 10 MB/s | 200 B x 64 | 10.02 | 28.5 | 53.9 | 55.4 | 56.9 | 1.5 | 1.03 |
| sqlite (data) | interval:50ms | 10 MB/s | 200 B x 64 | 9.98 | 30.0 | 59.0 | 63.6 | 64.2 | 1.4 | 2.10 |
| redb | interval:50ms | 10 MB/s | 200 B x 64 | 9.98 | 29.4 | 55.0 | 56.2 | 56.4 | 1.4 | 1.34 |
| seglog (data, prealloc) | always | 1 MB/s | 200 B x 1 | 1.00 | 0.5 | 3.2 | 11.3 | 15.5 | 1.8 | 12.79 |
| sqlite (data) | always | 1 MB/s | 200 B x 1 | 1.00 | 1.4 | 4.1 | 5.5 | 7.7 | 4.0 | 16.14 |
| redb | always | 1 MB/s | 200 B x 1 | 1.00 | 0.6 | 4.4 | 36.5 | 44.6 | 2.4 | 69.19 |

### Replay from cursor 0 (warm page cache, engine reopened first)

| engine | record size | backlog MiB | MB/s | records/s | peak RSS MiB |
|---|---|---|---|---|---|
| seglog (data, prealloc) | 200 | 1024 | 10981 | 54,905,392 | 7 |
| sqlite (data) | 200 | 1024 | 12893 | 64,463,352 | 6 |
| redb | 200 | 1024 | 4246 | 21,232,055 | 1035 |
| seglog (data, prealloc) | 4096 | 1025 | 11533 | 2,815,791 | 7 |
| sqlite (data) | 4096 | 1025 | 13611 | 3,322,972 | 6 |
| redb | 4096 | 1025 | 3591 | 876,594 | 1029 |

### Churn: 2 GiB appended, consumer trails 256 MiB behind, fsync interval:50ms

| engine | MB/s | steady disk avg MiB | steady max MiB | after full consume MiB | full-consume reclaim ms | time in delete ms | OS write amp | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|
| seglog (data, prealloc) | 585 | 272 | 272 | 16.0 | 18 | 129 | 1.03 | 3 |
| sqlite (data) | 288 | 257 | 257 | 0.6 | 484 | 4491 | 3.06 | 6 |
| redb | 911 | 328 | 328 | 328.0 | 8 | 241 | 1.31 | 334 |

### Recovery after kill -9 vs backlog

| engine | backlog on disk MiB | open + first query ms | page cache dropped | detail |
|---|---|---|---|---|
| seglog (data, prealloc) | 128 | 11.5 | True | segments=3 scanned_last_segment_bytes=66213440 truncated_bytes=895424 torn=false quarantined=0 |
| sqlite (data) | 119 | 1.6 | True |  |
| redb | 116 | 43.7 | True |  |
| redb (quick_repair) | 111 | 1.7 | True |  |
| seglog (data, prealloc) | 576 | 9.3 | True | segments=10 scanned_last_segment_bytes=66003904 truncated_bytes=1104960 torn=false quarantined=0 |
| sqlite (data) | 531 | 2.0 | True |  |
| redb | 683 | 202.0 | True |  |
| redb (quick_repair) | 676 | 1.8 | True |  |
| seglog (data, prealloc) | 2174 | 7.0 | True | segments=35 scanned_last_segment_bytes=7543360 truncated_bytes=59565504 torn=false quarantined=0 |
| sqlite (data) | 2076 | 6.4 | True |  |
| redb | 2599 | 720.0 | True |  |
| redb (quick_repair) | 2599 | 2.7 | True |  |

### kill -9 loops

| engine | fsync | after kill | iterations | kills < 5 ms | ACKs observed | max retained frames | recovery p50 ms | p99 ms | failures |
|---|---|---|---|---|---|---|---|---|---|
| seglog | always | - | 300 | 15 | 38,275 | 241 | 2.8 | 7.8 | 0 |
| seglog | interval:50ms | - | 200 | 11 | 368 | 508 | 1.2 | 4.2 | 0 |
| seglog | never | - | 100 | 2 | 35,753 | 212 | 2.4 | 4.1 | 0 |
| sqlite | always | - | 300 | 15 | 63,919 | 208 | 0.7 | 3.9 | 0 |
| sqlite | interval:50ms | - | 200 | 11 | 291 | 3094 | 2.2 | 30.8 | 0 |
| sqlite | never | - | 100 | 2 | 99,286 | 217 | 0.8 | 4.2 | 0 |
| redb | always | - | 300 | 15 | 115,384 | 226 | 0.9 | 2.5 | 0 |
| redb | interval:50ms | - | 200 | 11 | 285 | 4834 | 5.0 | 24.9 | 0 |
| redb | never | - | 11 | 0 | 9,989 | 176 | 1.7 | 3.9 | 11: iter 1: acknowledged loss: last frame 1069 < max acked 1124 |

### fsync EIO injection (LD_PRELOAD)

| engine | mode | faulted commit | next commit, same handle | 2nd commit | after reopen: seqs 1..6 present |
|---|---|---|---|---|---|
| seglog | once | Err("fsync failed: Input/output error (os error 5)") | Err("wal poisoned: fsync failed: Input/output error (os error 5)") | Err("wal poisoned: fsync failed: Input/o | [True, True, True, True, False, False] |
| seglog | sticky | Err("fsync failed: Input/output error (os error 5)") | Err("wal poisoned: fsync failed: Input/output error (os error 5)") | Err("wal poisoned: fsync failed: Input/o | [True, True, True, True, False, False] |
| sqlite | once | Err("disk I/O error") | Ok(()) | Ok(()) | [True, True, True, False, True, True] |
| sqlite | sticky | Err("disk I/O error") | Ok(()) | Ok(()) | [True, True, True, False, True, True] |
| redb | once | Err("I/O error: Input/output error (os error 5)") | Err("Previous I/O error occurred. Please close and re-open the databas | Err("Previous I/O error occurred. Please | [True, True, True, True, False, False] |
| redb | sticky | Err("I/O error: Input/output error (os error 5)") | Err("Previous I/O error occurred. Please close and re-open the databas | Err("Previous I/O error occurred. Please | [True, True, True, True, False, False] |

### ENOSPC (24 MiB tmpfs)

| engine | acked before error | first error | reopen | acked intact | reclaim | append after reclaim+reopen |
|---|---|---|---|---|---|---|
| seglog | 7291 | segment roll failed: No space left on device (os error 28) | error: No space left on device (os error | None | None | None |
| sqlite | 5169 | database or disk is full | ok | True | error: database or disk is full | error: database or disk is full |
| redb | 6035 | I/O error: No space left on device (os error 28) | ok | True | error: I/O error: No space left on devic | error: I/O error: No space left on device (os erro |

### kill -9 loops

| engine | fsync | after kill | iterations | kills < 5 ms | ACKs observed | max retained frames | recovery p50 ms | p99 ms | failures |
|---|---|---|---|---|---|---|---|---|---|
| seglog | always | LazyFS clear-cache | 200 | 11 | 58,305 | 220 | 3.9 | 16.6 | 0 |
| seglog | interval:50ms | LazyFS clear-cache | 100 | 2 | 187 | 429 | 6.5 | 10.0 | 0 |
| seglog | never | LazyFS clear-cache | 12 | 0 | 3,139 | 159 | 4.0 | 5.8 | 10: iter 0: acknowledged loss: last frame 0 < max acked 3 |
| sqlite | always | LazyFS clear-cache | 200 | 11 | 7,549 | 253 | 9.4 | 59.4 | 0 |
| sqlite | interval:50ms | LazyFS clear-cache | 100 | 2 | 76 | 284 | 5.3 | 99.7 | 0 |
| sqlite | never | LazyFS clear-cache | 1 | 0 | 29 | 0 | 2.9 | 2.9 | 1: iter 1: open failed: database disk image is malformed |
| redb | always | LazyFS clear-cache | 200 | 11 | 42,044 | 269 | 8.8 | 20.0 | 0 |
| redb | interval:50ms | LazyFS clear-cache | 100 | 2 | 102 | 2849 | 17.2 | 110.6 | 0 |
| redb | never | LazyFS clear-cache | 6 | 0 | 2,699 | 0 | 11.6 | 17.6 | 10: iter 0: acknowledged loss: last frame 0 < max acked 72 |
