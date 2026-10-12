# Capture-first WAL and replay

The write-ahead log [`thadam-wal`](architecture-overview.md#the-crates) is the spine of the pipeline. It stores raw source messages, is the single source of truth for "what was acknowledged", and is the only component whose disk format is permanent (once frozen; see below). Everything else — chains, sinks, quarantine — is a consumer with a cursor.

## Contents

The WAL is a custom segmented append-only log (ADR-0004, validated against SQLite and redb in spike [S1](../design/index.md)):

- **Segments** (default 16 MiB on the edge profile; `segment_bytes` in [`wal`](../configuration/wal.md)) contain a 64-byte header (`THADMWAL` magic, format version, epoch, base sequence, CRC) followed by frames. A segment is created as a temporary file, sealed, fsynced, and renamed, so a half-written segment never has a valid name. A bad segment header fails the open.
- **Frames** (max 4 MiB, one per source batch) carry a 24-byte header — magic, version, flags, length, `frame_seq`, a reserved word, and a CRC32C over header and payload — and a prost-encoded raw frame: the source index, the clock-sync flag, a string dictionary, and records of `ingest_time`, attribute references, and payload bytes. An all-zero header is the end-of-data marker.
- The WAL **never decodes**. It stores raw messages only and has no dependency on the canonical event model, so the format does not churn with model changes.

One dedicated OS thread owns all writable files. Submissions are grouped: one `write` and one `fdatasync` per group, every `fsync.interval` or when 4 MiB accumulates, whichever first. Commit tickets resolve in submission order, which is what makes acknowledge-after-commit in receive order possible.

## Durability and fsync policy

[`wal.fsync`](../configuration/wal.md#fsync) is one of `always`, `{ interval: <duration> }` (default `10ms`, provisional pending a hardware re-run), or `never`:

- With `fsync` other than `never`, acknowledged data survives a **process crash** (verified by kill-9 loops in spike S1: 2,200 iterations with zero committed-ticket loss, on one Apple Silicon (arm64) laptop and a Linux VM — the WAL evidence anecdotes; the absolute numbers do not transfer).
- Power-loss survival additionally requires storage that honors flush and is claimed only on flush-honoring industrial SSD or eMMC on Linux — and only once a hardware power-cut rig (500 cycles) has produced evidence. **No power-loss claim is published yet.** `fsync: never` survives a process crash only.
- After a failed fsync the segment is sealed and the writer stops rather than trusting the page cache ("fsyncgate" rule). No ticket resolves `Ok` after a failed fsync; recovery re-verifies.

> **Format status.** ADR-0004 is still **Draft**. The WAL format is versioned (segment headers, `wal.json`, and cursor files all carry versions, and a binary refuses unknown versions) but **not frozen**: a spike re-run on x86 NVMe and ARM eMMC reference hardware must finish before it is frozen. The provisional defaults — `interval(10ms)`, segment size, Linux preallocation — can change before the freeze.

## Readers, cursors, and gaps

Each consumer (chain, replay) opens its own read-only descriptors and reads only up to the **durable end** (the last successful fsync), published through a watch channel; a deleted segment stays readable through an open descriptor. Cursors are durable per-consumer files (`cursors/<name>.cur`) updated through temp-file + fsync + rename. Loss of position is never silent: readers receive explicit gap items — `corrupt`, `evicted`, or `missing` — which surface as `thadam_wal_gap_total` metrics and, for dataset consumers, may be flagged into quality columns.

## Retention, budget, and `on_full`

The WAL holds a bounded, retained window:

- `wal.max_disk` is the hard budget; enforcement stays well below filesystem capacity, and open/recovery never allocate space, so consumers can drain and reclaim on a full disk.
- `wal.retain_for` keeps consumed data available for replay, bounded by the budget.
- Retention deletes whole segments, oldest first, once every *required* sink's cursor has passed them (see [retention and disk budgets](../operations/retention.md) for `required`, `max_lag`, and `start_from`).

What happens at the budget is a required explicit choice, [`wal.on_full`](../configuration/wal.md#on_full-policy):

- **`block`** — stop acknowledging; the broker's bounded session queue fills and loss happens there, invisibly.
- **`drop_oldest`** — evict the oldest unconsumed segments; readers see `Gap::Evicted` and `thadam_wal_evicted_bytes_total` counts the loss.

Loss under either policy is explicit. This is the single place inside Thadam where acknowledged data can be lost. Details and throughput interplay: [Backpressure and disk budgets](backpressure-and-disk-budgets.md).

## Replay

Replay is a cursor rewind, not a separate storage path: reset a sink reader into the retained window and reprocess. Because chains always run the current configuration, replaying after a config fix applies the new config to old data — recovering, for example, events a broken filter dropped.

The defined replay property is **row-set equivalence** for the same WAL range, the same config hash, and the same enrichment-file hash: identical `_event_id`s, values, and flags. Replay is deliberately **not** byte-identical — rotation uses durations, file boundaries may differ, and enrichment files may change. Event IDs are deterministic and sampling/transforms never read the wall clock, so equivalence is testable. Replay output always goes to a separate directory or isolated prefix (`.../replays/{replay_id}/`), never the production prefix ([ADR-0013](../design/index.md)).

The [`thadam replay`](../cli/replay.md) subcommand runs this rewind from the command line. It re-runs one sink's chain over the retained range into `--output` (default `<data_dir>/replays/<id>/<sink>`), leaves the production sink directory and cursor untouched, and prints the replay id and output path.

### Replay a fix

1. Correct the transform or filter in the configuration file.
2. Find the range to recover with [`thadam inspect wal`](../cli/inspect.md#thadam-inspect-wal) (first and durable positions) or pick a duration such as `-2h`.
3. Replay it into a scratch directory:

   ```text
   thadam replay pipeline.yaml --from -2h --sink pq --output /tmp/replay-fix
   ```

4. Check the output and the printed row counts, then move the rows you want into place by your own process. Replay never writes to the production prefix.

The replay runs the current configuration, so the corrected chain applies to the retained frames. Frames already evicted by retention cannot be replayed.

## Recovery

Startup recovery is bounded and allocates nothing: validate segment headers, scan the last segment frame-by-frame, truncate at the first invalid frame, start a new segment (so stale frames cannot resurrect), refuse cursor/epoch mismatches, and never reuse a sequence number at or below a durable cursor. The full sequence, including segment-catalog and sink recovery, is in [Crash recovery](../operations/crash-recovery.md).
