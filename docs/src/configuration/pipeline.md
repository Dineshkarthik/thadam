# `pipeline`

Pipeline identity and storage location. Required.

```yaml
pipeline:
  name: edge-capture
  node_id: edge-01
  data_dir: /var/lib/thadam
```

| Key | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | The pipeline name. Used in object keys (`{prefix}/thadam/v1/{pipeline}/…`). A different name on restart is refused with exit code 3 unless `--repair` is given. |
| `node_id` | string | yes | The node identity. Used in Parquet file names and object keys so files from different nodes never collide. |
| `data_dir` | path | yes | The data directory holding the WAL, sink state, quarantine, and `thadam.json`. See [data directory layout](../operations/data-directory.md). |

## Constraints

- `name` and `node_id` are 1–64 characters: letters, digits, `.`, `_`, `-`, starting with a letter or digit (they appear in file names and object keys).
- `data_dir` must not be empty.

## Notes

- A different `pipeline.name` against an existing data directory means "this data belongs to another pipeline" — Thadam stops with `config.incompatible` (exit 3) rather than mixing two pipelines' state. [`--repair`](../operations/crash-recovery.md#--repair) rewrites the binding after you have verified the directory really belongs to this pipeline.
- The data directory binds to the machine (`machine_id` in `wal/wal.json`): a directory copied to another node is refused, which prevents two nodes from producing colliding object keys.
