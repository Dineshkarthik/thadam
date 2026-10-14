# Crash recovery and `--repair`

With `fsync` enabled (`fsync` other than `never`) and `ack_mode: on_commit`, acknowledged data survives a process crash within the configured disk budget: the WAL holds every committed batch, and every consumer resumes from its durable cursor. This page describes the shutdown and startup sequences and the one manual intervention: `--repair`.

## Graceful shutdown {#graceful-shutdown}

On SIGTERM or SIGINT, in order:

1. The intake token is cancelled. `/readyz` flips to 503; `/healthz` stays 200.
2. SourceDrivers stop pulling, drain the connector queue into the WAL, and wait for every commit ticket and acknowledgement. The MQTT source keeps polling until the last ack is flushed, then sends DISCONNECT so the persistent session keeps unacknowledged messages.
3. The submission channel closes; the WAL writer does a final group commit + fsync, writes the `clean_shutdown` marker, and is joined.
4. Chains and drivers keep draining until caught up to the WAL end or the 10 s drain timeout. The segment driver finalizes open files, commits cursors, marks files eligible; the shipper finishes or aborts its upload; a batch sink finishes its in-flight request.
5. Metrics/logs flush, the `flock` releases, exit 0.
6. The 30 s shutdown timeout or a **second signal** aborts everything and exits 130 — safe by construction, because recovery covers anything unfinished.

The two timeouts are fixed in code today (`shutdown.timeout` 30 s, `shutdown.drain_timeout` 10 s).

## Startup recovery

1. **Data directory.** `flock` on `data_dir/LOCK` — a second Thadam on the same directory fails fast (exit 3). Read `thadam.json`: pipeline name, config hash (+ previous), format versions, and the WAL epoch binding. A different pipeline name or WAL epoch is refused with exit 3 unless `--repair` is given; a changed config hash is accepted and recorded; a newer format version is always refused. The WAL then validates its own `wal/LOCK` and `wal.json` identity and machine binding.
2. **WAL.** Validate segment headers (a bad header fails the open); scan the last segment frame by frame (magic, version, reserved word, length, sequence continuity, CRC32C — bounded by one segment); truncate at the first invalid frame, fsync, and start a **new** segment so stale frames cannot resurrect. Corrupt frames in sealed segments are quarantined and surfaced as `Gap::Corrupt`. Recovery never allocates space.
3. **Cursors.** A missing cursor uses the sink's `start_from`. An epoch mismatch is refused. A cursor at or beyond `next_seq` (possible after power loss under `interval`/`never` fsync) makes recovery skip to `cursor + 1` and report the skipped range — readers see `Gap::Missing`, and no sequence number at or below a durable cursor is ever reused.
4. **Segment sinks.** Delete `staging/*.inprogress`; replay `catalog.log`; scan `ready/` for files missing from the catalog and rebuild entries from footers; advance the cursor to the highest contiguous finalized range; only then mark files eligible.
5. **Shipper.** Requeue eligible files without an `Uploaded` record; interrupted uploads restart idempotently (deterministic key + conditional put).
6. **Batch sinks.** Re-send any in-flight range with the same batch ID before new data.
7. **Stateful transforms.** Each chain warm-starts from `[cursor − window, cursor)` without emitting, so dedup and `on_change` survive restarts.
8. **Start order:** maintenance, then sinks and chains, then the shipper, then the admin server, then sources last.

## Restart with a changed configuration {#restart-with-a-changed-configuration}

A changed config hash is accepted: transform changes apply to data processed after the restart — from each cursor — so unprocessed WAL is processed with the new configuration. A new sink starts from its `start_from`. To reprocess data already consumed, [`thadam replay`](../cli/replay.md) re-runs the chain over the retained window.

## `--repair` {#--repair}

```bash
thadam run pipeline.yaml --repair
```

Repair mode **accepts a `thadam.json` mismatch — pipeline name or WAL binding — and rewrites it**. It never accepts a format version newer than the binary knows. Use it deliberately, after you have verified the situation:

- **You renamed the pipeline on purpose.** Verify the directory content really belongs to the new name, then run once with `--repair`.
- **You replaced `wal/` (new epoch) but kept `sinks/`.** Repair re-binds and the sinks resynchronize per their `start_from`; without it the runtime refuses, because old WAL positions would silently skip or duplicate data.

Repair is recorded (the rewritten `thadam.json`), and it is a per-incident decision, not a configuration default — there is no config key that disables these refuse-first checks.

## Failure semantics you can rely on

- After a failed fsync, the segment is sealed and the writer stops; no ticket resolves `Ok`, so nothing un-durable is acknowledged. Reopen re-verifies.
- `kill -9` at any point: sources redeliver the unacknowledged in-flight window (MQTT DUP), the WAL truncates to the last valid frame, cursors resume, segments recover from staging/ready. Evidence: the WAL spike kill-loop and torn-write suites ([reports](../design/index.md)).
- Exit codes are meaningful and stable: see [exit codes](exit-codes.md).
