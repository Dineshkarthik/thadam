# Exit codes

The `thadam` binary uses five stable exit codes (ARCHITECTURE.md section 11; implemented in `thadam-runtime` and mapped by the CLI):

| Code | Name | Meaning |
|---|---|---|
| `0` | ok | Clean shutdown; every component stopped normally. Also the success code for `validate`, `schema`, `version`, and `status`. |
| `1` | runtime failure | A component failed or the runtime hit an unrecoverable error — including a sink connector that cannot be built, or `thadam status` failing to reach the admin server. |
| `2` | invalid configuration | The configuration (including connector configuration) is invalid, or CLI usage was wrong (`validate`, `run` with bad flags, an unknown `version:`). |
| `3` | data directory | The data directory is locked by another process or is incompatible (pipeline-name or WAL-epoch mismatch without `--repair`; a newer on-disk format version). Not recoverable inline: fix the situation and re-run. |
| `130` | interrupted | The shutdown deadline passed or a second signal arrived; every task was aborted. The WAL plus recovery cover anything unfinished. |

## Mapping rules

- An error's code decides its exit: `config.incompatible` and `buffer.incompatible` → 3; any other `config.*` → 2; everything else → 1.
- When startup collected several errors, the most specific wins: data-directory (3) beats configuration (2), which beats runtime (1).
- `validate`'s output is authoritative for configuration problems — it exits 2 on errors, 0 on success, and never touches a data directory.

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
