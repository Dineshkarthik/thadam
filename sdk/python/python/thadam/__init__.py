"""Python SDK for the Thadam edge capture runtime.

Stable public surface, re-exported from the `thadam._native` extension module:
`validate`, `config_hash`, `read_segments`, `Pipeline`, `Diagnostic`, and the
`ThadamError` exception.
"""

from thadam._native import (
    Diagnostic,
    Pipeline,
    ThadamError,
    config_hash,
    read_segments,
    validate,
)

__all__ = [
    "Diagnostic",
    "Pipeline",
    "ThadamError",
    "config_hash",
    "read_segments",
    "validate",
]
