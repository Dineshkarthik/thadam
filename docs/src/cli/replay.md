# `thadam replay` {#thadam-replay}

```text
Re-run a sink's chain over a retained WAL range into a separate directory

Usage: thadam replay [OPTIONS] --from <POINT> --sink <ID> <PATH>

Arguments:
  <PATH>  The pipeline configuration file

Options:
      --from <POINT>    The first point to replay, inclusive: a WAL position, a duration before now (`-45m`, `-1h30m`), or an RFC 3339 time
      --to <POINT>      The last point to replay, inclusive; the durable end when absent. Takes the same forms as `--from`
      --sink <ID>       The sink whose chain processes the replayed frames
      --output <DIR>    Where to write the output. Must lie outside the data directory's `sinks/` and `wal/`. Defaults to `<data_dir>/replays/<id>/<sink>`
      --replay-id <ID>  Names this replay: `[A-Za-z0-9_.-]`, 1 to 100 characters. Defaults to `replay-<unix milliseconds>`
  -h, --help            Print help
```

Replays the frames retained in the WAL between `--from` and `--to` through the chain of the sink named by `--sink`, writing the result to a separate output directory. The replay never touches the production sink directory, never commits the production cursor, and refuses an output directory that already holds data at or after `--from`.

`--from` and `--to` each accept:

- a **WAL position**: a plain integer sequence number, as shown by [`thadam inspect wal`](inspect.md#thadam-inspect-wal);
- a **duration before now**: `-45m`, `-1h30m`, `-2d` (units `s`, `m`, `h`, `d`);
- an **RFC 3339 time**: `2026-10-09T14:00:00Z`.

Time bounds resolve to the first (for `--from`) or last (for `--to`) retained frame whose ingest time falls within the bound, so a replay covers whole frames.

On success the command prints the replay id, the output path, the sink, the frame range, and the counts:

```text
replay id: replay-1760000000000
output: /var/lib/thadam/replays/replay-1760000000000/pq
sink: pq
frames: 4..=9
frames read: 6, events: 6, rejects: 0, gaps: 0
```

Use the replay id to refer to the run later; the output directory holds Parquet files with the same row set the original sink produced for that range (row-set equivalence, not byte equality; see [replay](../concepts/wal-and-replay.md#replay)).

## Replaying after a fix

A typical use is to correct a transform and recover data a broken filter dropped:

```text
thadam replay pipeline.yaml --from -2h --sink pq --output /tmp/replay-fix
```

The replay runs the **current** configuration, so the corrected chain applies to the old frames. The production sink and its cursor are unchanged.

## Exit codes

Exit codes follow the [exit codes](../operations/exit-codes.md) table: 0 on success, 1 for a runtime failure during the replay, 2 for invalid configuration or usage (including a malformed `--from` or `--to`, and an `--output` inside the data directory's `sinks/` or `wal/`), 3 for a data directory that cannot be read, and 130 when interrupted.
