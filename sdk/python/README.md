# thadam

Python SDK for the [Thadam](https://thadam.dineshkarthik.me) edge capture runtime, built with PyO3 and maturin.

- `thadam.validate(path)` — validate a pipeline configuration, returning every
  error and warning with positions and hints.
- `thadam.config_hash(path)` — the stable configuration hash the runtime
  records in `thadam.json`.
- `thadam.read_segments(sink_dir, dataset=None)` — read a Parquet sink's ready
  segments into a `pyarrow.Table`, zero-copy over the Arrow PyCapsule
  interface.
- `thadam.Pipeline(path)` — build and run a pipeline in-process on a
  Rust-owned Tokio runtime, with the GIL released and graceful shutdown from
  another thread via `stop()`.

## Install

```bash
pip install thadam
```

To pin the current pre-release explicitly:

```bash
pip install thadam==0.1.0b2
```

Requires Python >= 3.9; `pyarrow >= 14` is installed as a dependency. Prebuilt `abi3` wheels (CPython 3.9+) are published for Linux glibc (`manylinux_2_28`) and musl (`musllinux_1_2`) on `x86_64` and `aarch64`, and for macOS `arm64` (11.0+) and `x86_64`. Other platforms (such as Windows) build from the source distribution and need a Rust toolchain (1.89 or newer).

This package is the Python SDK; the `thadam` command-line tool installs with `cargo install thadam --version 0.1.0-beta.2 --locked`.

## Examples

### Validate a configuration

```python
import thadam

for d in thadam.validate("pipeline.yaml"):
    where = f":{d.line}:{d.column}" if d.line else ""
    print(f"{d.severity}[{d.code}] {d.path}{where}: {d.message}")
    if d.hint:
        print(f"  hint: {d.hint}")
```

### The configuration hash

```python
assert thadam.config_hash("pipeline.yaml") == "1734a596e8a251683f1e829377f1e2c5"
```

### Read a Parquet sink

```python
table = thadam.read_segments("/var/lib/thadam/sinks/local", dataset="readings")
print(table.schema)
print(table.num_rows)
df = table.to_pandas()
```

`read_segments` lists files through the sink's `catalog.log` when one exists
(every record that still has a file on disk, oldest WAL range first) or walks
`ready/` directly when there is no catalog (fixtures, copied-out data). All
files must share a schema — they do when one sink wrote them.

### Run a pipeline in-process

```python
import threading

import thadam

pipeline = thadam.Pipeline("pipeline.yaml")
t = threading.Thread(target=lambda: print("exit", pipeline.run(timeout=300)), daemon=True)
t.start()
# ... other work ...
pipeline.stop()   # graceful shutdown, like SIGINT to the CLI
t.join()
exit_code = pipeline.run(timeout=300)  # raises: a Pipeline runs once
```

`run(timeout=seconds)` triggers graceful shutdown when the timeout elapses
(the shutdown deadline then bounds the drain); it is not a hard kill. Exit
codes mirror the CLI: 0 clean, 1 runtime failure, 2 invalid configuration,
3 data directory, 130 interrupted.

### Catching errors

```python
try:
    h = thadam.config_hash("missing.yaml")
except thadam.ThadamError as e:
    print(e.code, e.domain)  # config.invalid config
    print(e.message)
```

## Building from a checkout

For contributors working on the SDK from a checkout of the [Thadam repository](https://github.com/Dineshkarthik/thadam), building from source requires the workspace Rust toolchain ([`rust-toolchain.toml`](https://github.com/Dineshkarthik/thadam/blob/main/rust-toolchain.toml)) and a CPython >= 3.9 build on `PATH` (`PYO3_PYTHON` to pick a specific one):

```bash
cd sdk/python
python3 -m venv .venv
. .venv/bin/activate
pip install maturin pytest pyarrow
maturin develop
pytest tests
```

`maturin develop` builds the `thadam._native` cdylib with
`pyo3/extension-module` (no libpython linked) and installs the package in the
virtualenv. Cargo's plain `cargo test -p thadam-python` builds without that
feature (rlib + libpython) so the Rust unit tests run too.

## Arrow versions

The workspace writes segments with arrow/parquet 60, while `pyo3-arrow` 0.19
(the newest release) pairs with arrow 59 — no release tracks arrow 60 yet.
`read_segments` therefore opens the Parquet files itself with parquet 59 and
hands the batches to pyo3-arrow; the decode types never cross between majors,
and the Python-side transfer stays zero-copy through the Arrow C stream
capsule. When `pyo3-arrow` publishes an arrow-60 release this constraint goes
away. One wheel per platform covers all supported Pythons via PyO3's abi3
(`abi3-py39`); pyo3-arrow's capsule and buffer-protocol APIs are
stable-ABI-safe.

## Layout

```
sdk/python/
  Cargo.toml            Rust crate `thadam-python`, cdylib `thadam`
  pyproject.toml        maturin build, Python >= 3.9, abi3 wheel
  src/                  PyO3 bindings and Rust unit tests
  python/thadam/         pure-Python package (re-exports, .pyi stubs, py.typed)
  tests/                pytest suite (run inside a venv, see above)
```
