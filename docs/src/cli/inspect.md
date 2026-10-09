# `thadam inspect` {#thadam-inspect}

```text
Read-only views of a data directory: WAL, cursors, segments, samples

Usage: thadam inspect <COMMAND>

Commands:
  wal       Show the WAL's segments, durable end, epoch, and retained bytes
  cursors   Show each durable cursor's committed position and in-flight range
  segments  Show a sink's file counts and bytes per catalog state, newest files first
  sample    Print the raw payloads of the newest frames, truncated. Prints at most the first 5 records of each frame
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

`thadam inspect` reads a data directory without changing it. It is safe to run while `thadam run` holds the data directory: it never takes the data-directory lock or the WAL's writer lock, and it never repairs, truncates, or rewrites a file. Each subcommand prints human-readable text by default and JSON with `--json`.

Exit codes: 0 on success, 2 for a usage error, 3 when the data directory is not a readable WAL (see [exit codes](../operations/exit-codes.md)), 1 when a file cannot be read.

## `thadam inspect wal` {#thadam-inspect-wal}

```text
Show the WAL's segments, durable end, epoch, and retained bytes

Usage: thadam inspect wal [OPTIONS] <DATA_DIR>

Arguments:
  <DATA_DIR>  The data directory (`pipeline.data_dir` in the configuration)

Options:
      --json  Print JSON instead of text
  -h, --help  Print help
```

Prints the WAL epoch, node and machine identity, whether the last shutdown was clean, the first and next positions, the durable end (the last position that survived an fsync), retained bytes, torn and zero tails, and each segment file.

The `--json` output has these keys: `dir`, `epoch`, `node_id`, `machine_id`, `clean_shutdown`, `first_seq`, `next_seq`, `durable_end`, `retained_bytes`, `torn_tail`, `zero_tail`, `segments` (each with `base`, `path`, `bytes`), and `cursor_count`.

## `thadam inspect cursors` {#thadam-inspect-cursors}

```text
Show each durable cursor's committed position and in-flight range

Usage: thadam inspect cursors [OPTIONS] <DATA_DIR>

Arguments:
  <DATA_DIR>  The data directory (`pipeline.data_dir` in the configuration)

Options:
      --json  Print JSON instead of text
  -h, --help  Print help
```

One entry per durable cursor. A sink's cursor is named `sink.<id>`. Each entry shows the committed position and, when a batch is being processed, the in-flight range (`inflight` is `null` otherwise).

The `--json` output is `{"cursors": [{"name", "sink", "committed", "inflight"}]}`, where `inflight` is `{"first", "last", "frames"}` or `null`.

## `thadam inspect segments` {#thadam-inspect-segments}

```text
Show a sink's file counts and bytes per catalog state, newest files first

Usage: thadam inspect segments [OPTIONS] --sink <ID> <DATA_DIR>

Arguments:
  <DATA_DIR>  The data directory (`pipeline.data_dir` in the configuration)

Options:
      --sink <ID>  The sink whose catalog to read
      --json       Print JSON instead of text
  -h, --help       Print help
```

Reads the sink's segment catalog (`sinks/<id>/catalog.log`) and prints the file count and bytes for each state: `finalized`, `eligible`, `uploaded`, and `deleted`, followed by the newest files with their dataset, status, position range, row count, and bytes.

The `--json` output is `{"sink", "totals", "files"}`, where `totals` maps each state to `{"files", "bytes"}` and each entry in `files` has `id`, `dataset`, `status`, `path`, `first`, `last`, `rows`, and `bytes`.

## `thadam inspect sample` {#thadam-inspect-sample}

```text
Print the raw payloads of the newest frames, truncated. Prints at most the first 5 records of each frame

Usage: thadam inspect sample [OPTIONS] <DATA_DIR>

Arguments:
  <DATA_DIR>  The data directory (`pipeline.data_dir` in the configuration)

Options:
  -n, --count <N>  How many of the newest frames to print [default: 5]
      --json       Print JSON instead of text
  -h, --help       Print help
```

Prints the raw payloads of the newest retained frames, before any chain runs. Each payload is cut at 200 bytes on a character boundary and marked with `...`; at most 5 records are printed per frame.

The `--json` output is `{"frames": [{"seq", "source", "clock_synced", "record_count", "records"}]}`, where each record has `ingest_time_ns`, `payload_bytes`, `payload`, and `truncated`.
