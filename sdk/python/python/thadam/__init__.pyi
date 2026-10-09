"""Type stubs for the public `thadam` package.

The implementation lives in the `thadam._native` extension module; these stubs
describe the re-exported surface.
"""

from typing import List, Literal, Optional

import pyarrow

class Diagnostic:
    """One configuration finding, returned by `thadam.validate`."""

    code: str
    """A stable code from `thadam_core.codes`, such as `config.invalid`."""
    path: str
    """The document path, such as `sinks[1].config.bucket`; `$` is the root."""
    line: Optional[int]
    """The 1-based line in the source, when known."""
    column: Optional[int]
    """The 1-based column in characters, when known."""
    message: str
    """What is wrong. Never contains values from `${env:}` or `${file:}`."""
    hint: Optional[str]
    """How to fix it, when there is a concrete suggestion."""
    severity: Literal["error", "warning"]
    def __repr__(self) -> str: ...

class ThadamError(Exception):
    """Error raised by the `thadam` bindings.

    Carries the stable `code` (e.g. `config.invalid`) and the `domain`
    (e.g. `config`); `str(e)` and `message` carry the detail.
    """

    code: str
    domain: str
    message: str

def validate(path: str) -> List[Diagnostic]:
    """Validates a pipeline configuration file.

    Returns every diagnostic found, errors first (`severity == "error"`) then
    warnings; an empty list means the file is valid for the connector kinds
    this build registers. Never raises for a bad configuration — a file that
    cannot be read or parsed is reported as diagnostics, like `thadam
    validate`.
    """
    ...

def config_hash(path: str) -> str:
    """The stable 32-hex-digit configuration hash of a valid file.

    The hash follows the content, not the path, and is taken before
    interpolation, so a rotated secret does not move it. Raises `ThadamError`
    (domain `config`) when the file does not load.
    """
    ...

def read_segments(sink_dir: str, dataset: Optional[str] = None) -> pyarrow.Table:
    """Reads a Parquet sink's ready segments into a `pyarrow.Table`.

    `sink_dir` is the sink's directory (the one holding `catalog.log` and
    `ready/`, e.g. `<data_dir>/sinks/local`). With `dataset`, only that
    dataset's files are read. Rows arrive oldest WAL range first. The
    hand-off is zero-copy over the Arrow PyCapsule interface. An empty table
    (no columns) is returned when nothing is ready to read.

    Raises `ThadamError` (domain `sink`) on IO problems and when files disagree
    on their Arrow schema.
    """
    ...

class Pipeline:
    """A built Thadam pipeline, ready to run in-process.

    Construction loads and validates the configuration and builds every
    connector (raising `ThadamError` on failure). A Pipeline runs once:
    construct a new one to run again.
    """

    def __init__(self, path: str) -> None:
        """Loads `path` and builds the pipeline. Raises `ThadamError`."""
        ...
    def run(self, timeout: Optional[float] = None) -> int:
        """Runs the pipeline to completion, returning a process-style exit code.

        Exit codes are 0 (clean), 1 (runtime failure), 2 (invalid
        configuration), 3 (data directory), 130 (interrupted). Runs on a
        Rust-owned Tokio runtime with the GIL released, so other Python
        threads keep working and may call `stop()`. With `timeout` (seconds),
        a graceful shutdown is triggered when it elapses — the timeout is not
        a hard kill.
        """
        ...
    def stop(self) -> None:
        """Triggers graceful shutdown from any thread. Idempotent."""
        ...
    @property
    def stopping(self) -> bool:
        """Whether `stop` has been triggered."""
        ...
