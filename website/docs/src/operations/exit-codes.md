# Exit codes

The `thadam` binary uses five stable exit codes (ARCHITECTURE.md section 11; implemented in `thadam-runtime` and mapped by the CLI):

| Code | Name | Meaning |
|---|---|---|
| `0` | ok | Clean shutdown; every component stopped normally. Also the success code for `init`, `validate`, `schema`, `version`, `status`, `replay`, `export`, and `inspect`. |
| `1` | runtime failure | A component failed or the runtime hit an unrecoverable error — including a sink connector that cannot be built, `thadam status` failing to reach the admin server, or an I/O or execution failure in `init`, `replay`, `export`, or `inspect`. |
| `2` | invalid configuration | The configuration (including connector configuration) is invalid, or CLI usage was wrong (`validate`, `run`, `replay`, `export`, or `inspect` with bad flags, an unknown `version:`, `init` refusing to overwrite an existing file, or `export` refusing to overwrite `--mcap` without `--force`). |
| `3` | data directory | The data directory is locked by another process (`run`, `replay`, `export`) or is incompatible (`run`, `validate --against-data-dir`, `replay`, `export`, `inspect`: pipeline-name or WAL-epoch mismatch without `--repair`, or a newer on-disk format version). Not recoverable inline: fix the situation and re-run. |
| `130` | interrupted | The shutdown deadline passed or a second signal arrived; every task was aborted. The WAL plus recovery cover anything unfinished. |

## Mapping rules

- An error's code decides its exit: `config.incompatible` and `buffer.incompatible` → 3; any other `config.*` → 2; everything else → 1.
- When startup collected several errors, the most specific wins: data-directory (3) beats configuration (2), which beats runtime (1).
- Plain `validate` is authoritative for file-level configuration problems — it exits 2 on errors, 0 on success, and never touches a data directory. With `--against-data-dir`, it also runs the connector, chain, and read-only data-directory checks `run` makes at startup, exiting 3 on a data-directory incompatibility and 2 on configuration, connector, or chain errors (never writing or locking the directory).

Scripting example:

```bash
if thadam validate pipeline.yaml; then
  echo "config ok"
else
  case $? in
    2) echo "fix the configuration" ;;
    *) echo "unexpected validation failure" ;;
  esac
fi
```
