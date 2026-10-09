//! The `thadam.ThadamError` Python exception and conversions from Thadam errors.

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use thadam_core::Classify;

pyo3::create_exception!(
    "thadam._native",
    ThadamError,
    PyException,
    "Error raised by the `thadam` bindings.\n\nCarries the stable `code` (e.g. `config.invalid`), the `domain`\n(e.g. `config`), and `message` (also `args[0]` and `str(e)`)."
);

/// Builds the exception for one [`thadam_core::ThadamError`], with `code`,
/// `domain`, and `message` set as instance attributes.
///
/// Must be called with the GIL held (attribute assignment touches the
/// exception instance); the conversions back from detached work happen in the
/// `map_err` step outside `py.detach`.
pub fn make_err(py: Python<'_>, err: &thadam_core::ThadamError) -> PyErr {
    let py_err = PyErr::new::<ThadamError, _>((err.message.clone(),));
    let value = py_err.value(py);
    // Instance attributes on a heap exception type cannot fail; ignore the
    // result so an exotic interpreter build cannot turn an error path into
    // a second error.
    let _ = value.setattr("code", err.code);
    let _ = value.setattr("domain", err.domain.as_str());
    let _ = value.setattr("message", err.message.clone());
    py_err
}

/// Converts anything [`Classify`] + `Display` (e.g. `ConfigReport`) into the
/// Python exception.
pub fn classify_to_py_err(py: Python<'_>, err: &(impl Classify + std::fmt::Display)) -> PyErr {
    make_err(py, &err.to_thadam_error())
}

/// Folds a list of runtime errors (pipeline build failures) into a single
/// exception: the first error's code and domain, all messages joined.
pub fn combine_to_py_err(py: Python<'_>, errs: &[thadam_core::ThadamError]) -> PyErr {
    match errs {
        [] => pyo3::exceptions::PyRuntimeError::new_err("unknown error"),
        [single] => make_err(py, single),
        [first, ..] => {
            let joined = errs
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n");
            make_err(
                py,
                &thadam_core::ThadamError::new(first.domain, first.code, first.retry, joined),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    // These tests build `ThadamError` values only; the Python attribute
    // mapping is covered by the pytest suite (it needs an interpreter).
    #[test]
    fn combine_logic_holds() {
        use thadam_core::{ErrorDomain, Retry};

        let a = thadam_core::ThadamError::permanent(ErrorDomain::Config, "config.invalid", "first");
        let b = thadam_core::ThadamError::permanent(ErrorDomain::Sink, "sink.io", "second");
        let joined = [&a, &b]
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(joined.contains("first"));
        assert!(joined.contains("second"));

        let merged = thadam_core::ThadamError::new(a.domain, a.code, a.retry, joined);
        assert_eq!(merged.domain, ErrorDomain::Config);
        assert_eq!(merged.code, "config.invalid");
        assert_eq!(merged.retry, Retry::Permanent);
    }
}
