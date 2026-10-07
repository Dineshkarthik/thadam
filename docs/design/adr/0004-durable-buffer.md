# ADR-0004: Durable buffer

- **Status:** Draft
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (spike S1 evidence and recommendations folded in; PR6 and PR7 implementation deviations recorded in "Implementation notes (PR6 and PR7)"; status unchanged)
- **Deciders:** Project owner

**Implementation status.** The owner decided to build the WAL now from the S1 design and to re-validate it on reference hardware (x86 NVMe, ARM eMMC) later. `thadam-wal` (PR6 and PR7) is implemented in commit `0c4cf6f`. Format v1 is **versioned but not frozen**: segment headers, `wal.json`, and cursor files carry format versions and a binary refuses a version it does not know, but the `interval(10ms)` fsync default, Linux preallocation, the segment size, and the `RawFrame` field numbers stay provisional until that re-run. This ADR stays **Draft**. Where the implementation differs from the text below, "Implementation notes (PR6 and PR7)" is the current behavior.

**Why this ADR stays Draft.** Spike S1 has reported (`docs/design/spikes/S1-wal.md`), and its evidence settles the engine choice and the frame layout. The on-disk format is permanent once released, and S1 ran without reference hardware: there was no x86 NVMe and no ARM eMMC, and the Linux numbers come from a Docker Desktop virtual machine whose flush semantics are unknown. **The S1 re-run on x86 NVMe and ARM eMMC is still required before the on-disk format is frozen**, so this ADR is not promoted to Proposed or Accepted yet. Exactly what must happen before it can be promoted is listed in "Remaining before this ADR can leave Draft". Everything in the Decision section is the S1 recommendation, adopted as the current design; it becomes binding when the owner accepts the ADR.

## Context

Thadam acknowledges a source only after the message is durable on local disk (ARCHITECTURE.md section 2, step 4). The component that provides that durability is the capture write-ahead log (WAL): an ordered, append-only record of raw source messages that survives a process crash and, on storage that honors flush, a power loss. `product.md` section 12 requires a local durable buffer with bounded disk usage, configurable retention, checksummed records, recovery after restart, replay, corruption detection, backpressure, and sink-specific acknowledgements. It also says not to pick an implementation purely because it benchmarks well in isolation. Section 27 requires detecting corrupt local buffers and forbids claiming exactly-once or zero-loss universally.

Where the WAL sits in the pipeline (before any decoding or transformation, with one processing chain per sink) is decided in ADR-0003 (D1). This ADR covers the engine (D2), the on-disk format, the fsync policy (D3), cursors, retention, the disk budget and `on_full` policy (D4), recovery, and format versioning. Reader-facing guarantees are claimed in ADR-0011.

Forces:

- **The format becomes permanent once released.** A released WAL format has to be read by every later binary, so the design must be right before PR6 and PR7 start (ARCHITECTURE.md section 21, do-not-code-yet gates).
- **Ack latency is on the hot path.** Commit-to-ack latency bounds MQTT throughput under `ack_mode: on_commit` (ADR-0006).
- **Edge storage is hostile.** Flash endurance, flush semantics on eMMC and SD cards, and clock steps on devices without a real-time clock all affect correctness. The owner decided on 2026-10-07 (B3, D-B3-5) that industrial SSD or eMMC that honors flush is the supported baseline, and that SD cards are NOT a supported target for durability claims. That the actual device honors flush remains an assumption **[A]** until the S1 re-run and the power-cut rig produce evidence.
- **No maintained library fits.** The ecosystem maturity review (as checked 2026-10-07) found that no maintained Rust crate provides a segmented first-in-first-out log with multiple cursors. redb, fjall, and SQLite are healthy but have key-value or table semantics, so deletes, compaction, and disk reclaim behave differently from a queue. S1 measured that difference (see "Evidence from spike S1").

### Evidence from spike S1

All figures come from `docs/design/spikes/S1-wal.md`. The spike ran on one Apple Silicon (arm64) laptop (macOS with real flushes) and on a Linux virtual machine inside Docker Desktop on the same machine. Macro differences of 2 times or more are reliable; differences under about 40% on the flush-bound macOS rows are not, because each configuration ran once on a shared machine.

| | custom log | SQLite (WAL mode) | redb 4.3 |
|---|---|---|---|
| Append, `always`, macOS with `F_FULLFSYNC`, 200 B / 4 KiB records | 101 / 497 MB/s | 85 / 200 MB/s | 96 / 380 MB/s |
| Append, `always`, Linux VM (`fdatasync`), 200 B / 4 KiB | 539 / 1037 MB/s | 360 / 354 MB/s | 322 / 664 MB/s |
| Commit p50 / p99 at the 10 MB/s reference load, `always`, Linux VM | 2.4 / 4.6 ms | 2.3 / 9.0 ms | 2.9 / 6.1 ms |
| OS-reported bytes written per payload byte, saturated append | 1.00 to 1.05 | 2.0 to 2.2 (3.1 to 3.2 with reclaim) | 1.3 to 2.2 |
| Disk after consumers catch up (Linux churn, 256 MiB backlog) | 16 MiB (one segment) | 0.6 MiB | 328 MiB (never shrinks) |
| Time spent deleting during 2 GiB of churn (Linux / macOS) | 0.13 / 0.54 s | 4.5 / 11.2 s | 0.24 / 2.2 s |
| Recovery after kill -9, cold cache, 2.1 to 2.6 GiB backlog (Linux) | 7.0 ms | 6.4 ms | 720 ms (2.7 ms with `quick_repair`) |
| Peak RSS during appends | 3 to 8 MiB | 6 to 8 MiB | 0.2 to 1.4 GiB (37 to 96 MiB with a 32 MiB cache) |
| kill -9 loops, committed loss or stale frames | 0 failures | 0 failures | 0 failures (except `never`, by design) |
| LazyFS (drop all un-fsynced data at the kill point), `always` and `interval` | 0 of 300 failed | 0 of 300 failed | 0 of 300 failed |
| After one failed fsync, next commit on the same handle | refused (the spike's poison flag) | **succeeds** (no latch) | refused |
| Can delete consumed data on a full disk | no, because the spike's open allocates a new segment (fixable) | no (DELETE needs space) | no (copy-on-write needs space) |

Further evidence:

- The custom log had zero failures in 2,200 kill -9 iterations (1,600 on macOS, including a separate run of 1,000 with 572,820 acknowledgements and 77 kills during open, and 600 on Linux), 300 LazyFS iterations, and 2,248 deterministic torn-tail and corruption cases (truncation at every byte of the last frame, a bit flip at every byte, zeroed middle frames, garbage tails), with and without preallocation. A mutant that appended in place at the recovered end instead of starting a new segment passed the first version of the zeroed-middle test and failed the corrected one, which is direct evidence that "truncate, then start a new segment" is load-bearing.
- redb `Durability::None` lost acknowledged data on a plain process crash. SQLite in `synchronous=OFF` mode reported "database disk image is malformed" after the first LazyFS iteration, so it corrupts rather than only losing data. redb database creation is not crash-atomic: a kill during the first create left a file with a zeroed magic in 14 of 30 reproductions.
- Framing overhead (frame header plus per-record length prefix) was 2.3% for 200 B records and 0.2% for 4 KiB records.
- The 50 ms interval caps throughput regardless of engine: `interval(50ms)` delivered 14.2 to 14.9 MB/s for every engine on both operating systems in the closed-loop runs, close to the expected ceiling of 16 MB/s for 64 frames in flight. The general ceiling is `per-source ack rate <= inflight window / (interval + fsync latency)`.

## Decision

**Provisional until the owner accepts this ADR.** The Decision below is the recommendation of spike S1, adopted as the current design. Nothing becomes binding until S1 has been re-run on reference hardware and the owner accepts the ADR.

### Engine (D2)

Thadam implements a custom segmented append-only log in the `thadam-wal` crate. **SQLite in WAL mode through `rusqlite` is the documented fallback.** **redb is not used for the bulk queue.**

- The fallback condition in D2 was "if S1 shows correctness risk in the custom log". It was not triggered. The custom log matched SQLite on every correctness test, wrote about half the bytes (1.0 times against about 2 times, or about 3.1 times once reclaim is included), reclaimed disk by an `unlink` that costs milliseconds and needs no free space, bounded recovery by one segment, and used single-digit MiB of RSS. The residual risk is code that is not yet written (concurrent readers, retention, budget enforcement, the lock file), not the format or the recovery rule. The architecture review's estimate of 3,000 to 4,000 lines including tests still looks right; the spike's log is about 590 lines with no concurrency.
- SQLite remains the fallback because its recovery and replay were excellent. Its costs: writes double, reclaim is expensive and easy to get wrong (it needs a delete, an `incremental_vacuum` stepped to completion, and a `wal_checkpoint(TRUNCATE)`, which took 53 to 61% of the churn run), it does not latch after a failed fsync (the next commit on the same handle succeeded), `synchronous=OFF` can corrupt the database on power loss, and it has a single writer lock, so cursor commits from sink threads would contend with the WAL writer.
- redb is rejected as the bulk queue: default recovery is proportional to database size (720 ms at 2.6 GiB), the file never shrinks without `compact()`, the default cache costs about 1 GiB of RSS, `Durability::None` loses data on a process crash, and database creation is not crash-atomic. It remains reasonable for small metadata such as cursors and shipper state if a cache size is set and the database is created under a temporary name and renamed into place. The current design uses plain cursor files instead (see Cursors).

### Contents

The WAL stores raw source messages plus a minimal envelope. It never decodes events and never depends on the canonical model (ADR-0001), so model changes are never disk migrations. S1 supports capture-first on the WAL side: a raw-envelope frame per source batch adds about 2% framing for 200 B records, commit-to-ack is one group commit, and the 10 MB/s reference load is 2% (Linux VM) to 10% (macOS, full flush) of the log's measured `always` throughput. S1 did not measure chain CPU with one to three sinks or WAL growth during a sink outage, which D1 also lists; those belong to ADR-0003's evidence, not to this ADR.

### On-disk format (frame layout recommended for freezing as built; the format is not frozen until this ADR leaves Draft)

All integers are little-endian.

- **Segment header, 64 bytes:** magic `THADMWAL` at bytes `[0, 8)`, `format_version u16` at `[8, 10)`, a reserved `u16` at `[10, 12)`, `epoch u64` at `[12, 20)`, `base_seq u64` at `[20, 28)`, `created_unix u64` at `[28, 36)`, zeros to byte 60, then a CRC32C of bytes `[0, 60)` at `[60, 64)`. A segment is created as a temporary file, header written, optionally preallocated, fsynced, renamed to its final name, and the directory fsynced, so a half-written segment never has a valid name. A segment is truncated to its data when it is sealed. The file name `wal/{baseOffset:020}.seg` is provisional **[A]**.
- **Frame header, 24 bytes (settles open item O9):** `magic u16 | version u8 | flags u8 | len u32 | frame_seq u64 | reserved u32 | crc32c u32`. The fields total 24 bytes (2 + 1 + 1 + 4 + 8 + 4 + 4); the earlier "20 versus 24" inconsistency came from omitting the reserved word. The reserved word **must be zero**, keeps the header 8-byte aligned, and can carry a header CRC under a version bump if one is ever needed.
- **CRC coverage:** CRC32C over header bytes `[0, 20)` followed by the payload. Recovery and readers check the magic, version, flags, reserved word, `len` against `frame_max_bytes` and the remaining file size, `frame_seq` continuity, and the CRC. This caught every single-bit flip and every truncation point in the S1 tests.
- **No header CRC and no resynchronization.** Do not add a separate header CRC, and do not resynchronize by scanning for the next magic inside a sealed segment. A corrupt frame in a sealed segment is quarantined from that frame on and surfaced as `Gap::Corrupt`. Resynchronization is the only feature that would need a header CRC, and it risks accepting a stale frame. This closes the "resynchronize inside a sealed segment" question the earlier draft left open.
- **End-of-data marker:** an all-zero 24-byte header. The magic is non-zero, so no `len = 0` sentinel is needed. "Zero tail" (clean end of a preallocated segment) and "torn tail" (non-zero garbage) are counted as separate metrics.
- **Frame payload:** a prost-encoded `RawFrame { source_idx, clock_synced, dict: repeated string, records: repeated RawRecord { ingest_time_ns, attr_refs, payload: bytes } }`. Attributes such as topic, QoS, and retain are interned through the per-frame dictionary. **S1 did not exercise this payload.** It used a simple length-prefixed record list, and prost encode and decode CPU was not measured. The frame header layout above is independent of the payload encoding, but the payload schema (the `RawFrame` field numbers) is also permanent once released, so it is part of what must be validated before the format is frozen.
- **One frame per source batch**, capped at `frame_max_bytes` (4 MiB). A message larger than `max_message_bytes` (default 8 MiB) is rejected at the source with a counted drop. (For MQTT, the oversize case is handled by the client's packet-size limit, not by a drop; see ADR-0006.)
- **Segments** are 64 MiB by default and 16 MiB on the edge profile. Whether segments are preallocated is a platform decision (see Preallocation).

### Writer, group commit, and fsync policy (D3)

- One OS thread exclusively owns writable segment files. Submissions arrive through a bounded channel plus a byte semaphore. The writer drains pending submissions up to `group_max_bytes` (4 MiB) or until the fsync interval elapses, then performs one `write` and one `fdatasync` per group. Commit tickets resolve in submission order, which gives per-source in-order acknowledgement.
- `fsync` is `always`, `interval(ms)`, or `never`, with acknowledgement after the fsync. `never` survives a process crash only. (In the spike, `never` on the custom log and on SQLite had already handed data to the kernel, which survives a process crash; redb's `Durability::None` did not.)
- **The default is provisionally `interval(10ms)`, not `interval(50ms)`.** The original D3 default of 50 ms, combined with `ack_mode: on_commit`, caps per-source throughput at the inflight window divided by about 50 ms (a window of 20 gives at most about 400 acknowledgements per second per session) and adds about 30 ms at p50 and 55 to 64 ms at p99 to every acknowledgement. In the macOS sweep (custom log, `F_FULLFSYNC`, 1 MB/s of unbatched 200 B messages) `interval(10ms)` gave 99 fsyncs per second, 2.8 times write amplification (against 1.5 for 50 ms and 6.2 for `always`), 8.6 ms p50, and a window ceiling about 4.5 times higher than 50 ms. `always` and longer intervals stay as explicit choices, and producer batching is documented as the way to keep write amplification near 1 times at light load. The unbatched-message amplification matters on flash: at one 200 B message per frame and 5,000 messages per second under `always`, the OS wrote 12.8 times the payload for the custom log (6.2 times on macOS).
- **The 10 ms default is provisional.** It must be re-measured on reference eMMC before it is frozen: if that device's flush latency is at or above 10 ms, `interval` and `always` converge there.
- **The formula to carry into ADR-0006 and the user documentation:** `max acknowledgements per second per source ~ inflight window / (interval + fsync latency)`. S2 reproduced the underlying window model to within 6%.
- Power-loss durability is claimed only on storage that honors flush, and only after a hardware power-cut rig produces evidence. That claim is owned by ADR-0011. On macOS, plain `fsync()` does not flush the drive cache; the spike used `F_FULLFSYNC` for real-flush numbers, and macOS is a development platform only.

### fsync failure rule ("fsyncgate") and re-verification

After a failed fsync, Linux may drop the dirty pages and clear the error, so a retried fsync can report success over lost data. A failed fsync is therefore fatal for that segment: seal it, stop acknowledging, restart the writer, and re-verify. The fsync is never retried. No commit ticket resolves successfully after a failed fsync. The S1 fault test confirmed that the custom log and redb latch the failure (the next commits on the same handle were refused) while SQLite does not (it returned `Ok` for the next two commits), so SQLite as a fallback would need the same rule enforced above it.

**Correction from S1: re-verification after a failed fsync must not trust the page cache.** The S1 fault shim left the page cache intact, so after the reopen a frame that was never acknowledged was read back from cache. That is allowed for a frame that was never acknowledged, but it shows that "re-verify from disk" reads the page cache, not the device, unless it is made not to. The design therefore requires one of two approaches: re-read with `O_DIRECT` or after `posix_fadvise(DONTNEED)`, or treat everything after the last successful fsync offset as unacknowledged, seal the segment there, and let the sources redeliver. The second needs no device access and is the recommended default. Only the API reaction was tested; no real device error, `dm-flakey`, or `dm-error` was tried.

### Readers, committed end, and sequence numbers

Each reader opens its own read-only descriptors and never reads past the **durable end**, which the writer publishes through a `watch` channel. The durable end is the last successful fsync, not the written end. With `fsync: never`, the durable end is the written end. Unlinking a segment does not invalidate an open descriptor on Linux. `mmap` is rejected because of the SIGBUS risk.

**Correction from S1: never reuse a sequence number at or below a durable cursor.** Readers and `commit_cursor` are bounded by the durable end. On open, if a cursor is at or beyond `next_seq` (which can happen after a power loss under `interval` or `never`), either start the next segment at cursor + 1 or refuse to open. (As implemented, recovery always skips ahead and reports the skipped range; Implementation notes 3.) The spike does not enforce this; its crash consumer only commits cursors up to acknowledged sequences, so no test reached it. SQLite and redb avoid it because their open computes the maximum of the last row and the maximum cursor, plus one. The spike's reader also served written-but-unsynced frames, which production must not.

### Preallocation

**Correction from S1: no segment preallocation on macOS.** On APFS, `F_PREALLOCATE` plus `ftruncate` made appends 1.3 to 2 times slower in the matrix (200 B records: 1,604 against 3,202 MB/s, p99 4.7 against 0.8 ms; 4 KiB: 3,456 against 4,575 MB/s, with plain `fsync`) and doubled the bytes written (2.03 to 2.08 times against 1.01 to 1.04 times). On Linux, `fallocate` made no measurable difference on the virtual disk, so it stays optional there and is decided on reference hardware. Preallocated segments need the all-zero end-of-data header so readers and recovery can tell written bytes from zero fill, and recovery must treat a zero tail beyond the data as a clean end (it did, in every test).

### Cursors

Each consumer has one file, `cursors/<name>.cur`, holding epoch, committed position, an optional in-flight range, and a CRC. It is written through a temporary file, `fdatasync`, rename, and directory fsync, batched to at most every `cursor_commit_interval` (200 ms) or when a segment finalizes. Cursor files were correct in every S1 crash test. They cost two fsyncs per commit, so the 200 ms batching stays. The trade-off is a larger redelivery window after a crash, in exchange for fewer metadata flushes on flash. (As implemented, the WAL commits a cursor on every call and leaves the batching to its callers; Implementation notes 5.)

### Retention

Whole segments are deleted once every required sink's cursor has passed them, subject to `retain_for`, a reprocessing window bounded by the disk budget. Each sink is `required: true|false` with an optional `max_lag`. A sink that exceeds `max_lag` is evicted from retention, which is an explicit, counted loss for that sink. All time-based decisions use monotonic durations and WAL offsets, never wall-clock comparisons. Reclaim is an `unlink` followed by a directory fsync; S1 measured steady disk at the backlog plus one segment.

### Disk budget and `on_full` (D4)

`DiskBudget` keeps per-category atomics for the WAL, segments, and quarantine. `on_full` has no default. The operator chooses `block` (withhold acknowledgements, which moves loss risk to the broker) or `drop_oldest` (evict the oldest segments, surface `Gap::Evicted` to readers, increment `thadam_wal_evicted_bytes_total`). `thadam init` writes `block` with an explanatory comment. This policy is owned jointly with ADR-0011. Decided by the owner on 2026-10-07 (B3, D-B3-1): no default, and configuration validation fails if `on_full` is missing. Budget enforcement was not part of S1.

**Correction from S1: open and recovery must not allocate space.** In the spike, open always created and preallocated a new segment, so after an ENOSPC the log could not even be reopened, although its reclaim path (`unlink`) needs no space. (SQLite and redb could reopen but could not delete on a full disk either, because their deletes need free space.) The design therefore requires: create the next segment lazily on the first append, never in `open` or recovery; order deletion before any allocation so that consumers can drain and reclaim on a full disk; and keep `max_disk` enforcement well below filesystem capacity so ENOSPC is a fallback, not the normal path. This has not yet been tested on a loop device with the fix applied.

### Recovery and identity

- On startup, validate segment headers and scan the last segment frame by frame, checking magic, length, `frame_seq` continuity, and CRC32C. Cost is bounded by the last segment (at most 64 MiB scanned): 5 to 12 ms in the S1 runs, independent of backlog size.
- At the first failure, truncate to the end of the last valid frame, fsync, **and start a new segment**, so stale frames beyond the truncation point can never be resurrected. Reusing an empty last segment after truncating it to its header was safe in every test.
- **Correction from S1: a bad segment header must fail the open.** The spike quarantines the segment and carries on, and if the quarantined segment was the last one, `next_seq` rewinds. The only safe exception is an empty last segment.
- A corrupt frame in a sealed segment is quarantined and reported to readers as `Gap::Corrupt`.
- The data directory carries an `epoch`, a `node_id`, and a machine-id binding in `thadam.json`. A cursor with the wrong epoch, or a position beyond the WAL end (for example after the WAL was restored from backup), is refused. A data directory copied to another node is detected through the machine-id binding. **As implemented:** the WAL keeps the epoch and the node and machine binding in `data_dir/wal/wal.json`, the runtime owns `thadam.json`, and a cursor beyond the WAL end is not refused: recovery skips ahead and reports it (Implementation notes 3 and 4).

### Format versioning

An older binary that finds a newer WAL, cursor, or state format refuses to start with a clear error. It never guesses.

### Summary of the three design corrections requested by S1

1. **Open and recovery must not allocate space** (create segments lazily; delete before allocating).
2. **Post-fsync-failure re-verification must not trust the page cache** (`O_DIRECT` or `posix_fadvise(DONTNEED)`, or treat everything after the last good fsync offset as unacknowledged and let sources redeliver).
3. **No segment preallocation on macOS** (Linux preallocation stays optional pending reference hardware).

S1 also found two further requirements, recorded above: never reuse a sequence number at or below a durable cursor, and a bad segment header must fail the open.

### Superseded by this ADR (ARCHITECTURE.md section 19)

- The post-transform canonical WAL and the optional raw MCAP tap (replaced by capture-first plus the retention window; ADR-0003, ADR-0015 reserved).
- WAL frames as prost-encoded `EventBatch` with layout `[magic u16][len u32][crc32c u32][type u8][payload]`.
- `on_full: block | drop_oldest` with `block` as the default.
- The original estimate of about 1.5 to 2k lines of code for the log, replaced by the review's estimate of 3 to 4k including tests.
- The `interval(50ms)` fsync default (ARCHITECTURE.md D3), replaced provisionally by `interval(10ms)`.
- The statement that the frame header is 24 bytes while its fields total 20 (open item O9), settled as 24 bytes with a reserved word.

## Implementation notes (PR6 and PR7)

Recorded from the `thadam-wal` crate documentation and source (commit `0c4cf6f`). The sections above are the S1-based design; where they differ, this section is what the code does. None of it changes the engine choice, and none of it freezes the format.

1. **Readers return frames or a gap.** `WalReader::next` returns `ReadItem::Frames(FrameChunk)` (consecutive frames, with their range) or `ReadItem::Gap(Gap)`. Gaps are always delivered as items, never skipped silently. Readers never commit; the consumer calls `Wal::commit_cursor` after processing a range.
2. **`Gap::Missing`.** The `Gap` enum (non-exhaustive) has three variants: `Corrupt { range }` (failed validation; nothing after a corrupt frame in the same segment is trusted), `Evicted { range, cursor }` (segments deleted before this reader read them, by `on_full: drop_oldest` or by retention while the consumer was not pinning them), and `Missing { range }`. `Missing` is new relative to the design above: positions that were never durably written (for example after a power loss under `fsync: never`), or that were skipped so a position at or below a durable cursor is never reused.
3. **Recovery always skips ahead.** The design allowed either starting the next segment at cursor + 1 or refusing the open. The implementation always skips: if any cursor's high-water mark (its committed position, or the end of its in-flight range if higher) is at or beyond the recovered `next_seq`, `next_seq` becomes that mark plus one, the skipped range is returned in `RecoveryReport::skipped`, and readers see it as `Gap::Missing`. The open is not refused for this. A cursor with a different epoch, or a bad cursor file (corruption, unknown version), is still refused.
4. **Directory layout.** The WAL root is `data_dir/wal` (the `dir` passed to `open`), not the whole data directory. It holds `LOCK` (flock), `wal.json`, the `clean_shutdown` marker, `segments/{base:020}.seg`, and `cursors/<name>.cur`. `wal.json` carries `kind`, the format versions (`wal_format_version`, `frame_format_version`, `cursor_format_version`), the `epoch`, the creation time, and the `node_id` and `machine_id` binding (a later open with a different `node_id` or `machine_id` is refused). **The runtime owns `data_dir/thadam.json`**; the WAL does not read or write it. This replaces the statement under "Recovery and identity" that `thadam.json` carries the epoch, `node_id`, and machine-id binding for the WAL: those live in `wal.json`. The segment file name is `segments/{base:020}.seg`, replacing the provisional `wal/{baseOffset:020}.seg`.
5. **Cursor commit batching is left to callers.** `Wal::commit_cursor` (and `commit_cursor_blocking`) durably writes the cursor on every call (temporary file, flush, rename, directory flush), and refuses a position past the durable end. The WAL itself does not batch on a `cursor_commit_interval` or on segment finalization as the Cursors section describes; callers batch (the 200 ms guidance stands) because each commit costs two flushes. The cursor file is 44 bytes: magic `RCUR`, format version, a flag for an in-flight range, epoch, committed position, the in-flight first and last positions, and a CRC32C.
6. **The newest retained segment is never deleted.** Retention deletes whole segments, oldest first, and never the newest one, because the newest segment carries the sequence high-water mark across restarts. (Deleting by `unlink` plus one directory flush needs no free space.)
7. **A failed write or fsync truncates and stops.** The design offered two ways to avoid trusting the page cache after a failed fsync. The implementation takes the second: no ticket resolves `Ok` after a failed write or `fsync`, the writer truncates whatever was not yet acknowledged, stops (writer state `Failed`), and the WAL must be reopened, which re-verifies it.
8. **Failpoints are a cargo feature.** `thadam-wal` has a `failpoints` feature (also on under `cfg(test)`) that exposes `thadam_wal::failpoints` to other crates. A `Failpoints` handle is attached to one WAL through `WalOptions::failpoints`, so concurrent tests do not interfere. Switches: fail the next N segment fsyncs, fail the next N writes with a given errno, crash on close (the writer exits with no final write, fsync, seal, or clean-shutdown marker), abort the process after a group is written and before it is flushed (`wal::after_write_before_fsync`), and report the last synced length so a test can truncate to it.
9. **Format v1 is versioned, not frozen.** See "Implementation status" at the top: the format versions are checked on open, and the provisional items (fsync default, Linux preallocation, segment size, `RawFrame` field numbers) wait for the reference-hardware re-run.

Defaults as implemented in `WalOptions::new(on_full)`: `fsync` `interval(10ms)`, `segment_bytes` 64 MiB, `frame_max_bytes` 4 MiB, `group_max_bytes` 4 MiB, at most 8 MiB and 256 submitted-but-unwritten batches, `retain_for` zero, no preallocation, and no `node_id` or `machine_id`. `on_full` has no default.

## Alternatives considered

- **Custom segmented log (chosen; frozen only when this ADR is accepted).** Sequential writes, no compaction, whole-segment deletion gives predictable reclaim, replay is a sequential scan (11 GB/s or more on a warm cache in S1), no C dependency on ARM. Cost: Thadam owns the correctness. The review estimates 3 to 4k lines with tests. The original plan cites the Kafka and Vector disk-buffer lineage as evidence that recovery is simple and well understood; that is design lineage, and S1 now provides the measurements for this design.
- **SQLite in WAL mode via `rusqlite` (documented fallback).** Extremely mature and power-loss tested, with good tooling and a single file. `rusqlite` 0.40.2 (MIT) bundles SQLite (public domain) and has only old advisories (as checked 2026-10-07 in the ecosystem maturity review). Costs measured in S1: about 2 times the bytes written (about 3.1 times with reclaim), reclaim that spends 53 to 61% of the time on vacuum and checkpoint, no fsync latch, `synchronous=OFF` corruption, a single writer lock, and a C dependency (which cross-compiles easily). Kept as the fallback.
- **redb 4.3.** Stable file format, crash-safe by default, MIT/Apache, but primarily one maintainer and three major versions in about 18 months. Not chosen as the WAL, for the S1 reasons listed under Engine. Suitable for small metadata only.
- **fjall 3.1 (LSM tree).** Its disk format is documented as stable and its README recommends crashing and restarting on I/O errors. FIFO deletes produce tombstones, compaction costs CPU and I/O on ARM, and space reclamation lags, so disk use is less predictable. Not chosen. Not measured in S1.
- **rocksdb.** Yearly releases and a heavy C++ build; same LSM objections. Not chosen.
- **sled, okaywal, commitlog.** sled is a 1.0 alpha since years (last alpha October 2024); okaywal's last release was 2023. Both rejected in the maturity review (section 13). `commitlog` appears in that review's reject list (section 16) but section 13 gives no version or date evidence for it **[V]**.
- **heed (LMDB).** Memory-mapped with a fixed map size (maturity review section 13, which gives no verdict). Not pursued: Thadam already rejects `mmap` for readers because of the SIGBUS risk, and a fixed map size fits poorly with a budget-managed queue **[A]**.
- **surrealkv, persy.** surrealkv 0.21 is pre-1.0. persy is licensed MPL-2.0, which is not on the allow-list (ADR-0020). Not pursued.
- **Parquet segments as the buffer.** Rejected: the footer is written at close, so durability at low latency would force tiny files. This was also product.md's third candidate approach.
- **MCAP as the WAL.** Plausible (chunk CRCs, readable when truncated) but MCAP has no cursor or deletion model. MCAP remains an export and import format only (D24).
- **Post-transform canonical WAL.** Superseded; see ADR-0003.
- **Both a raw and a canonical WAL.** Rejected: two times the write amplification, which is bad for flash endurance.
- **A separate header CRC, or resynchronizing by scanning for the next magic.** Rejected on S1 evidence (see On-disk format).
- **Retaining `interval(50ms)` as the default.** Rejected on S1 evidence (see Writer).

## Consequences

**Easier**

- The WAL format is small and independent of model evolution.
- Replay is a cursor rewind; a misconfigured filter is corrected by replaying from the retained window.
- Retention is whole-segment deletion, so disk reclaim is predictable and write amplification is about 1 times at batched load.
- Recovery cost is bounded by one segment.
- No extra native dependency on ARM.

**Harder**

- Thadam owns the correctness of the log. Model-based state-machine tests against an in-memory reference, kill -9 loops, and LazyFS (unsynced-write loss) runs are mandatory, not optional.
- Directory fsyncs after segment creation, deletion, and cursor rename, concurrent readers on the active segment, the durable-end rule, and preallocation end-of-data handling are all Thadam's to get right. S1 reasoned about directory-fsync ordering but did not test it under loss: a SIGKILL never tears a write and LazyFS does not drop metadata operations.
- Unbatched small messages amplify writes under `always` and, to a lesser degree, under a short interval. The default interval trades latency for amplification, and users with unbatched producers need the documented formula.
- CRC32C detects corruption; it is not tamper protection. Encryption at rest is delegated to LUKS or filesystem encryption (`product.md` section 25; ARCHITECTURE.md section 16).
- `product.md` section 12 lists FIFO or priority policies. The MVP has FIFO only; priority queues are a stated non-goal.
- Flash endurance math must be published with the claims register (ADR-0011). The review's example is 50 sensors at 1000 Hz with 200-byte samples: about 10 MB/s, roughly 860 GB a day of WAL writes before Parquet writes.

**Fixed once released**

- The segment and frame layout, the all-zero end-of-data marker, the CRC coverage, the `RawFrame` payload schema, the cursor format, and the epoch and machine-id binding.
- The decision not to resynchronize inside a sealed segment.

## Evidence and validation required

### Remaining before this ADR can leave Draft

These items are all required before the on-disk format is frozen and the ADR is promoted. None of them is expected to change the engine choice.

1. **S1 re-run on reference hardware: x86 NVMe and ARM eMMC.** Measure flush latency and `fdatasync` behavior on the real device, commit-to-ack latency by fsync mode, device-level write amplification from `/proc/diskstats`, recovery time on eMMC, and sustained MB/s. This settles the fsync default (`interval(10ms)` is provisional), whether to preallocate on Linux, and the segment size. SD cards, if run at all, are informational only.
2. **Validation of the real frame payload.** Encode and decode the prost `RawFrame` and measure its CPU and framing cost, then freeze its field numbers.
3. **ENOSPC test on a loop device** with lazy segment allocation and delete-before-allocate applied.
4. **Directory-metadata loss test** (for example `dm-flakey`) or a power-cut rig, covering segment creation, rename, and unlink ordering.
5. **The two further format requirements** (durable-end and cursor/sequence reuse rule; bad segment header fails open) written into the PR6 design and tests.
6. **Numeric pass criteria** adopted for the re-run (proposed from the S1 data): commit p99 at the reference load at or below 2 times (fsync latency plus the interval); steady disk at or below the backlog plus one segment; recovery at or below 50 ms for a 64 MiB last segment on eMMC; zero failures in at least 1,000 kill -9 iterations and 300 LazyFS iterations.

Not required for freezing the format, but required for the claims: the hardware power-cut rig (500 cycles on a relay-switched board) before ADR-0011 may claim power-loss survival.

### Checklist

**Engine comparison (custom log versus SQLite versus redb)**

- [x] Raw-WAL commit-to-ack latency p50 and p99 per fsync mode (`always`, `interval`, `never`) and per group size, on macOS with real flushes and on a Linux VM. Done. Hardware re-run on x86 NVMe and ARM eMMC: [ ] open. The result feeds the ack-window ceiling in ADR-0006 (formula above).
- [x] Sustained MB/s by fsync mode. Done on macOS and a Linux VM. [ ] Hardware re-run.
- [x] Write amplification by process-level OS counters (about 1.0 times for the custom log at batched load, 12.8 times for unbatched 200 B messages under `always`). [ ] Device-level amplification via `/proc/diskstats` on real hardware (not meaningful inside the VM).
- [x] Recovery time versus backlog (5 to 12 ms for the custom log). [ ] eMMC run against the 50 ms criterion.
- [x] Disk reclaim under churn (backlog plus one segment for the custom log).
- [x] Replay MB/s and RSS (11 GB/s or more warm; 3 to 8 MiB RSS). Cold-cache replay was not measured.
- [x] A written comparison that weighs operational behavior (bounded disk, predictable reclaim) and not raw speed alone, as `product.md` section 12 requires (S1 section 7.1 and this ADR).

**Durability and failure behavior**

- [x] fsync-error behavior via an `LD_PRELOAD` fault shim: no ticket resolved Ok after a failed fsync in the custom log. Only the API reaction was tested, not a real device error.
- [x] kill -9 loops: zero committed-ticket loss across 1,000 crash iterations (plus 1,200 more), and no stale-frame resurrection. The PR6 acceptance criterion is satisfied by the spike's log; PR6 must meet it again on production code.
- [x] LazyFS runs: 0 of 300 failed for `always` and `interval`; `never` shows the expected loss. LazyFS drops only un-fsynced data, so directory ordering is not exercised.
- [ ] Optional hardware power-cut rig (relay-switched Raspberry Pi with NVMe, 500 cycles). It is the only real evidence for a power-loss claim. Without it, ADR-0011 must not claim power-loss survival.

**Behavioral tests from the implementation plan, section 5.3**

- [x] Torn tail at every byte offset of the last frame (and a bit flip at every byte): 2,248 cases passed.
- [x] `frame_seq` continuity and stale-frame non-resurrection (with the mutant check).
- [ ] ENOSPC on a loop device (the spike used a tmpfs and found the open-allocates flaw).
- [ ] Pinned-cursor eviction (`Gap::Evicted` surfaced, counter incremented) (PR7).
- [ ] Backwards clock step: retention and rotation decisions unaffected (PR7).
- [ ] Reader racing a segment unlink; multiple consumers (not tested in S1; PR6 and PR7).
- [ ] Disk stays at or below budget plus one segment under budget enforcement (PR7 acceptance).

**Interaction and format decisions**

- [x] The ack-window interaction with commit latency, measured against the ADR-0006 ack-window curve. S1 supplies the commit-latency inputs; S2 supplies the window model.
- [x] The O9 header-size decision (24 bytes with a reserved word), the CRC coverage, the no-resynchronization rule, and the end-of-data marker, each recorded in this ADR (frozen only when the ADR is accepted).
- [x] The owner decisions B3 (`on_full`: no default; storage media: industrial SSD or eMMC, SD unsupported) recorded in `docs/design/owner-decisions.md` on 2026-10-07.

## Revisit when

- The S1 re-run on reference hardware shows a correctness or latency problem in the custom log (switch to the SQLite fallback or redesign), or flush latency that changes the fsync default.
- A maintained Rust crate appears that provides a segmented log with multiple cursors.
- Cursor commits on flash prove too expensive in practice.
- The product owner changes the storage media assumptions (the 2026-10-07 baseline excludes consumer SD cards; supporting them would reopen this ADR).
- Priority or multi-class buffering becomes a requirement.
- A real device or `dm-flakey` fault test shows the "treat everything after the last good fsync as unacknowledged" rule is insufficient.

## References

- ARCHITECTURE.md sections 2, 3 (D1 to D4), 6, 10, 11, 12, 19, 21, 22 (O9).
- ADR-0003 (execution and process model, WAL placement), ADR-0006 (MQTT client, ack-window ceiling), ADR-0011 (delivery guarantees, claims register, disk-full and flash endurance claims), ADR-0013 (segments and quarantine budgets), ADR-0020 (license allow-list, dependency policy).
- `docs/design/spikes/S1-wal.md` (2026-10-07; section 7 is the recommendation, sections 5 and 6 the results and limitations).
- `product.md` sections 12, 25, 26, 27.
- Design history: architecture review sections 3 and 5; Rust implementation plan sections 4.7 and 5.3; original MVP plan section G; ecosystem maturity review section 13 (versions and maintenance status as checked 2026-10-07).
