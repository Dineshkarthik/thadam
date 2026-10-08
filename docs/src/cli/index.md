# CLI reference

The `thadam` binary is the only interface: validation, runtime, status, and schema/version introspection. Output below is verbatim from `thadam --help` (pre-alpha; the grammar is stable for config version 1 and the subcommand set may grow with replay/inspect in later PRs).

## `thadam`

```text
Edge-first data capture

Usage: thadam <COMMAND>

Commands:
  validate  Check a configuration file and print the memory estimate
  version   Print version and format information
  schema    Print the JSON Schema of the configuration file
  run       Run a pipeline configuration
  status    Check the admin server status
  help      Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

Exit codes for every command: [exit codes](../operations/exit-codes.md) (0 ok, 1 runtime failure, 2 invalid configuration/usage, 3 data directory, 130 interrupted).

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
  -h, --help              Print help
```

Prints all diagnostics, then the pipeline summary, config hash, and memory estimate. Exits 0 if the configuration is valid (warnings allowed), 2 on any error. Details: [validating configurations](../getting-started/validating-configs.md).

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
thadam 0.0.0
config version: 1
config hash version: 1
```

The binary version plus the configuration-format and config-hash-algorithm versions it understands.

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
      --log-format <FORMAT>      The log output format: `json` or `pretty`
                                 [default: pretty]
      --log-level <LEVEL>        The minimum log level (e.g., `info`, `debug`,
                                 `warn`) [default: info]
      --admin-bind <ADDR>        The admin server bind address
                                 (e.g., `127.0.0.1:9464`)
      --admin-token-file <PATH>  Path to a file containing the admin server
                                 bearer token
  -h, --help                     Print help
```

Builds and runs the pipeline from the config file, handling SIGINT/SIGTERM for graceful shutdown ([lifecycle](../operations/crash-recovery.md#graceful-shutdown)). The CLI flags override the matching `observability` config keys for that run. With `--admin-bind`, the [admin API](../operations/admin-api.md) serves for the duration of the run.

> At this stage of the build-out `thadam run` fails the pipeline build once a config contains a sink, because the registered sink factories are placeholders ([status](../introduction.md#project-status)).

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

## `thadam help`

Prints the same help as `--help` for the binary or a subcommand (`thadam help run`).

## Roadmap subcommands (planned)

`product.md` and ARCHITECTURE.md section 21 sketch additional subcommands as later PRs, **not implemented today**: `thadam replay` and `thadam inspect` (PR17 — replay into isolated output, WAL/cursor/segment inspection, `--suggest-schema`), `thadam export --mcap` (M2), and benchmark tooling (`tools/thadam-bench`, not a CLI subcommand). This page will grow with them.
