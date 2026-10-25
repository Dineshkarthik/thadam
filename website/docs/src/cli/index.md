# CLI reference

The `thadam` binary is the only interface: validation, runtime, status, replay, WAL inspection, and schema/version introspection. Output below is verbatim from `thadam --help` (beta; the grammar is stable for config version 1).

## `thadam`

```text
Edge-first data capture

Usage: thadam <COMMAND>

Commands:
  init      Write a commented starter configuration file
  validate  Check a configuration file and print the memory estimate
  version   Print version and format information
  schema    Print the JSON Schema of the configuration file
  run       Run a pipeline configuration
  status    Check the admin server status
  replay    Re-run a sink's chain over a retained WAL range into a separate directory
  export    Export a retained WAL range to one MCAP file
  inspect   Read-only views of a data directory: WAL, cursors, segments, samples
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

Exit codes for every command: [exit codes](../operations/exit-codes.md) (0 ok, 1 runtime failure, 2 invalid configuration/usage, 3 data directory, 130 interrupted).

## `thadam init` {#thadam-init}

```text
Write a commented starter configuration file

Usage: thadam init [PATH]

Arguments:
  [PATH]  Where to write the starter configuration file [default: pipeline.yaml]

Options:
  -h, --help  Print help
```

Writes a minimal, commented starter `pipeline.yaml` (one MQTT source, one JSON dataset, one local Parquet sink) with `wal.on_full: block` and an explanatory comment on the `block` versus `drop_oldest` trade-off ([backpressure and disk budgets](../concepts/backpressure-and-disk-budgets.md#on_full)). The written file passes `thadam validate` out of the box. Refuses to overwrite an existing file (exits 2); exits 0 on success or 1 on an I/O failure.

## `thadam validate` {#thadam-validate}

```text
Check a configuration file and print the memory estimate

Usage: thadam validate [OPTIONS] <PATH>

Arguments:
  <PATH>  The pipeline configuration file

Options:
      --allow-unresolved  Treat an unset `${env:}` or unreadable `${file:}` as
                          a warning, so a configuration can be checked on a
                          machine that lacks the secrets
      --against-data-dir  Also check the configuration the way `run` would
                          start it: every connector's configuration, every
                          chain, and the data directory `pipeline.data_dir`
                          (read-only). Needs the real secrets, so it cannot be
                          combined with `--allow-unresolved`
  -h, --help              Print help
```

Prints all diagnostics, then the pipeline summary, config hash, and memory estimate. Exits 0 if the configuration is valid (warnings allowed), 2 on any error (or 3 with `--against-data-dir` when the data directory is incompatible). Details: [validating configurations](../getting-started/validating-configs.md).

## `thadam schema`

```text
Print the JSON Schema of the configuration file

Usage: thadam schema
```

Draft 2020-12 JSON Schema for the whole config file (and each connector's `config` block through the registry); for editor integration. Exits 0, or 1 if the schema cannot be rendered.

## `thadam version`

```text
Print version and format information

Usage: thadam version
```

```text
thadam 0.1.0-beta.2
config version: 1
config hash version: 1
```

The binary version plus the configuration-format and config-hash-algorithm versions it understands.

On an interactive terminal, `thadam --help`, `thadam version`, and `thadam --version` print the Thadam logo and the tagline "The open data plane for robotics." before the output shown above. Terminals that support the Kitty graphics protocol or iTerm2 inline images show the brand icon; all other terminals, including tmux and screen, show a block-character mark. Piped or redirected output is exactly as shown above. `THADAM_BANNER=off` turns the logo off, and `THADAM_BANNER=blocks` or `THADAM_BANNER=image` forces a style. `thadam --version` prints the same output as `thadam version`.

## `thadam run` {#thadam-run}

```text
Run a pipeline configuration

Usage: thadam run [OPTIONS] <PATH>

Arguments:
  <PATH>  The pipeline configuration file

Options:
      --repair                   Run in repair mode, accepting a `thadam.json`
                                 mismatch (pipeline name or WAL binding) and
                                 rewriting it. Never accepts a format version
                                 newer than this binary knows
      --log-format <FORMAT>      The log output format: `json` or `pretty`.
                                 Defaults to `observability.logs.format` in
                                 the config file (`json` when the section is
                                 absent)
      --log-level <LEVEL>        The minimum log level (e.g., `info`, `debug`,
                                 `warn`) [default: info]
      --admin-bind <ADDR>        The admin server bind address
                                 (e.g., `127.0.0.1:9464`)
      --admin-token-file <PATH>  Path to a file containing the admin server
                                 bearer token
      --exit-when-sources-finish Exit once every source has reached end of
                                 stream, after draining and finalizing. For
                                 finite inputs such as a recording; live sources
                                 never end, so leave this off for them
  -h, --help                     Print help
```

Builds and runs the pipeline from the config file, handling SIGINT/SIGTERM for graceful shutdown ([lifecycle](../operations/crash-recovery.md#graceful-shutdown)). The CLI flags override the matching `observability` config keys for that run. With `--admin-bind`, the [admin API](../operations/admin-api.md) serves for the duration of the run. With `--exit-when-sources-finish`, the run ends by itself once every source is drained, which is how the [ROS 2 MCAP example](https://github.com/Dineshkarthik/thadam/tree/main/examples/ros2-mcap-to-parquet) completes; without it, the pipeline runs until SIGINT/SIGTERM.

## `thadam status`

```text
Check the admin server status

Usage: thadam status [OPTIONS]

Options:
      --admin-url <URL>    The admin server URL
                           (default: http://127.0.0.1:9464)
      --token-file <PATH>  Path to a file containing the admin server bearer
                           token
  -h, --help               Print help
```

Fetches `<admin-url>/status` and pretty-prints the JSON [status snapshot](../operations/admin-api.md#status). Exit 0 on HTTP 200; 1 if the server is unreachable, the token is rejected, or a non-200 status is returned.

## `thadam replay`

Re-runs one sink's chain over the retained WAL range between `--from` and `--to` (a WAL position, a duration such as `-45m`, or an RFC 3339 time) into a separate output directory, and prints the replay id and output path. Full usage, output format, and exit codes: [`thadam replay`](replay.md).

## `thadam export`

Exports a retained WAL range to one MCAP file, with the channels, encodings, schemas, gaps, and provenance described in [`thadam export`](export.md). Full usage, output format, and exit codes: [`thadam export`](export.md).

## `thadam inspect`

Read-only views of a data directory: `wal`, `cursors`, `segments --sink`, and `sample`, each with `--json`. Safe to run while `thadam run` holds the data directory. Full usage: [`thadam inspect`](inspect.md).

## `thadam help`

Prints the same help as `--help` for the binary or a subcommand (`thadam help run`).

## Roadmap flags (planned)

`product.md` and ARCHITECTURE.md section 21 sketch additional CLI flags **not implemented today**: `--suggest-schema` on `thadam inspect` and `--speed` on `thadam replay` (benchmark runs live in the standalone `tools/thadam-bench` workspace tool rather than a `thadam` subcommand). This page will grow with them.
