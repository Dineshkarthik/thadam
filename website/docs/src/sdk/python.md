# Python SDK

Thadam ships a Python SDK built with PyO3 and maturin, at `sdk/python/` in the
repository. It exposes configuration validation and hashing, reads of the
Parquet sink's ready segments, and in-process pipeline runs.

- Requires Python 3.9 or newer and `pyarrow` 14 or newer.
- One wheel per platform covers all supported Pythons (PyO3 `abi3-py39`).
- Build from a checkout with `maturin develop`; see
  `sdk/python/README.md` for the commands.

## `validate(path)`

Validates a configuration file and returns a list of `Diagnostic` objects —
errors first, then warnings. An empty list means the file is valid for the
connector kinds the build registers. A file that cannot be read is reported
as a diagnostic, never an exception.

```python
import thadam

for d in thadam.validate("pipeline.yaml"):
    where = f":{d.line}:{d.column}" if d.line else ""
    print(f"{d.severity}[{d.code}] {d.path}{where}: {d.message}")
    if d.hint:
        print(f"  hint: {d.hint}")
```

`Diagnostic` has `code`, `path`, `line`, `column`, `message`, `hint`, and
`severity` (`"error"` or `"warning"`).

## `config_hash(path)`

Returns the stable 32-hex-digit configuration hash — the value the runtime
records in `thadam.json`. The hash follows the content, not the path, and
ignores interpolation, so rotating a secret does not move it. Raises
`thadam.ThadamError` with domain `config` when the file does not load.

## `read_segments(sink_dir, dataset=None)`

Reads a Parquet sink's ready segments (the files under
`<data_dir>/sinks/<id>/ready/`) into a `pyarrow.Table`, zero-copy over the
Arrow PyCapsule interface:

```python
table = thadam.read_segments("/var/lib/thadam/sinks/local", dataset="readings")
df = table.to_pandas()
```

Files are listed through the sink's `catalog.log` when one exists (every
record that still has a file on disk, oldest WAL range first); without a
catalog, `ready/` is walked directly. All files must share a schema — they
do when one sink wrote them. An empty, columnless table means nothing was
ready to read.

## `Pipeline`

Builds a pipeline from a configuration file and runs it in-process on a
Rust-owned Tokio runtime, releasing the GIL so other Python threads keep
working:

```python
import threading

import thadam

pipeline = thadam.Pipeline("pipeline.yaml")
t = threading.Thread(target=pipeline.run, kwargs={"timeout": 300}, daemon=True)
t.start()
# ... other work ...
pipeline.stop()   # graceful shutdown, like SIGINT to the CLI
t.join()
```

- `run(timeout=None)` returns a process-style exit code (0, 1, 2, 3, 130;
  see [Exit codes](../operations/exit-codes.md)). A `timeout` in seconds
  triggers graceful shutdown when it elapses; it is not a hard kill.
- `stop()` triggers that same graceful shutdown from any thread and is
  idempotent.
- A `Pipeline` runs once; construct a new one to run again.

## Errors

Operations raise `thadam.ThadamError`, which carries the stable `code`
(e.g. `config.invalid`), the `domain` (e.g. `config`), and `message`:

```python
try:
    thadam.config_hash("missing.yaml")
except thadam.ThadamError as e:
    print(e.code, e.domain, e.message)
```

`validate` is the exception: it reports problems as return values, matching
`thadam validate`.
