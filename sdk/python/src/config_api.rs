//! `thadam.validate` and `thadam.config_hash`.

use std::path::{Path, PathBuf};

use pyo3::prelude::*;
use thadam_config::{CheckOptions, ProcessEnv, check_file};

use crate::diagnostics::Diagnostic;
use crate::error::classify_to_py_err;
use crate::registry::build_registry;

/// The raw check used by both the binding and the unit tests: every error,
/// then every warning, each with its severity.
pub fn validate_impl(path: &Path) -> Vec<Diagnostic> {
    let registry = build_registry();
    let opts = CheckOptions::new().kinds(registry.kind_policy().clone());
    let checked = check_file(path, &ProcessEnv, &opts);
    let mut out: Vec<Diagnostic> = checked
        .report
        .errors
        .iter()
        .map(|d| Diagnostic::from_config(d, "error"))
        .collect();
    out.extend(
        checked
            .report
            .warnings
            .iter()
            .map(|d| Diagnostic::from_config(d, "warning")),
    );
    out
}

/// Validates a pipeline configuration file.
///
/// Returns every diagnostic found, errors first (with `severity == "error"`)
/// then warnings; an empty list means the file is valid for the connector
/// kinds this build registers. Never raises for a bad configuration — a file
/// that cannot be read or parsed is reported as diagnostics, matching
/// `thadam validate`.
#[pyfunction]
fn validate(py: Python<'_>, path: &str) -> PyResult<Vec<Diagnostic>> {
    let path = PathBuf::from(path);
    py.detach(move || Ok(validate_impl(&path)))
}

/// The stable 32-hex-digit configuration hash of a valid configuration file.
///
/// The hash is taken after typed normalization but before interpolation, so a
/// rotated secret does not move it. Raises [`ThadamError`][crate::error::ThadamError]
/// (domain `config`) when the file does not load.
#[pyfunction]
fn config_hash(py: Python<'_>, path: &str) -> PyResult<String> {
    let path = PathBuf::from(path);
    let result = py.detach(move || thadam_config::load_file(&path, &ProcessEnv));
    match result {
        Ok(spec) => Ok(spec.config_hash_hex()),
        Err(report) => Err(classify_to_py_err(py, &report)),
    }
}

/// Registers the config functions on the extension module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(config_hash, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/thadam-config/tests/fixtures")
    }

    #[test]
    fn valid_fixture_has_no_error_diagnostics() {
        let out = validate_impl(&fixtures().join("valid/minimal.yaml"));
        assert!(
            out.iter().all(|d| d.severity == "warning"),
            "unexpected errors: {out:?}"
        );
    }

    #[test]
    fn unknown_kind_is_caught_by_the_registry_policy() {
        let out = validate_impl(&fixtures().join("invalid/unknown_connector_kind.yaml"));
        assert!(
            out.iter().any(|d| d.code == "config.unknown_kind"),
            "expected config.unknown_kind in {out:?}"
        );
    }

    #[test]
    fn every_invalid_fixture_reports_at_least_one_error() {
        let dir = fixtures().join("invalid");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<_, _>>()
            .unwrap_or_else(|e| panic!("entry: {e}"));
        files.sort();
        assert!(!files.is_empty());
        for file in files {
            let out = validate_impl(&file);
            assert!(
                out.iter().any(|d| d.severity == "error"),
                "{}: expected an error diagnostic, got {out:?}",
                file.display()
            );
        }
    }

    #[test]
    fn missing_file_is_a_diagnostic_not_a_panic() {
        let out = validate_impl(Path::new("/definitely/not/here.yaml"));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].severity, "error");
        assert!(out[0].message.contains("cannot read"));
    }

    #[test]
    fn config_hash_is_32_hex_digits_and_stable_per_content() {
        let spec = thadam_config::load_file(
            &fixtures().join("valid/minimal.yaml"),
            &thadam_config::ProcessEnv,
        )
        .unwrap_or_else(|r| panic!("load: {}", r.render("minimal.yaml")));
        let hash = spec.config_hash_hex();
        assert_eq!(hash.len(), 32);
        assert!(hash.bytes().all(|b| b.is_ascii_hexdigit()));

        // Same content, different file name: the hash follows the content.
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::copy(fixtures().join("valid/minimal.yaml"), tmp.path()).expect("copy");
        let spec2 = thadam_config::load_file(tmp.path(), &thadam_config::ProcessEnv)
            .unwrap_or_else(|r| panic!("load copy: {}", r.render("copy")));
        assert_eq!(hash, spec2.config_hash_hex());
    }
}
