//! Python bindings for Thadam: configuration validation and hashing, Parquet
//! segment reads, and in-process pipeline runs.
//!
//! The extension module is `thadam._native`; the `thadam` Python package
//! re-exports a stable surface from it (see `python/thadam/__init__.py`).

mod config_api;
mod diagnostics;
mod error;
mod pipeline_api;
mod registry;
mod segments_api;

use pyo3::prelude::*;

use crate::diagnostics::Diagnostic;
use crate::error::ThadamError;

/// The `thadam._native` extension module.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Diagnostic>()?;
    m.add("ThadamError", m.py().get_type::<ThadamError>())?;
    config_api::register(m)?;
    segments_api::register(m)?;
    pipeline_api::register(m)?;
    Ok(())
}
