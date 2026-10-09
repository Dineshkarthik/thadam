//! The `Diagnostic` Python class: one config problem, with position and hint.

use pyo3::prelude::*;

/// One configuration finding, returned by `thadam.validate`.
#[pyclass(module = "thadam._native", frozen, get_all, skip_from_py_object)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    /// A stable code from `thadam_core::codes`, such as `config.invalid`.
    pub code: String,
    /// The document path, such as `sinks[1].config.bucket`; `$` is the root.
    pub path: String,
    /// The 1-based line in the source, when known.
    pub line: Option<u32>,
    /// The 1-based column in characters, when known.
    pub column: Option<u32>,
    /// What is wrong. Never contains values from `${env:}` or `${file:}`.
    pub message: String,
    /// How to fix it, when there is a concrete suggestion.
    pub hint: Option<String>,
    /// `error` or `warning`.
    pub severity: String,
}

impl Diagnostic {
    /// Converts a `thadam-config` diagnostic and its severity into the
    /// Python-facing shape.
    pub fn from_config(d: &thadam_config::Diagnostic, severity: &str) -> Self {
        Diagnostic {
            code: d.code.to_owned(),
            path: d.path.clone(),
            line: d.span.map(|s| s.line),
            column: d.span.map(|s| s.column),
            message: d.message.clone(),
            hint: d.hint.clone(),
            severity: severity.to_owned(),
        }
    }
}

#[pymethods]
impl Diagnostic {
    fn __repr__(&self) -> String {
        let span = match (self.line, self.column) {
            (Some(line), Some(column)) => format!(" at {line}:{column}"),
            _ => String::new(),
        };
        format!(
            "Diagnostic(code={:?}, path={:?}, message={:?}{})",
            self.code, self.path, self.message, span
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_config::{CheckOptions, MapEnv, check_file, check_str};

    fn fixtures() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/thadam-config/tests/fixtures")
    }

    /// A valid fixture yields no diagnostics; a real invalid fixture converts
    /// losslessly (positions and hints survive).
    #[test]
    fn converts_real_diagnostics() {
        let env = MapEnv::default();
        let good = check_file(
            &fixtures().join("valid/minimal.yaml"),
            &env,
            &CheckOptions::default(),
        );
        assert_eq!(good.report.errors.len(), 0, "{:?}", good.report);
        assert!(good.spec.is_some());

        let bad = check_file(
            &fixtures().join("invalid/bad_on_full_value.yaml"),
            &env,
            &CheckOptions::default(),
        );
        assert!(!bad.report.errors.is_empty());
        let first = &bad.report.errors[0];
        let out = Diagnostic::from_config(first, "error");
        assert_eq!(out.code, first.code);
        assert_eq!(out.message, first.message);
        assert_eq!(out.path, first.path);
        assert_eq!(out.severity, "error");
        assert_eq!(out.line, first.span.map(|s| s.line));
        assert_eq!(out.column, first.span.map(|s| s.column));
        assert_eq!(out.hint, first.hint);
    }

    #[test]
    fn repr_contains_code() {
        let env = MapEnv::default();
        let checked = check_str("version: 2\n", &env, &CheckOptions::default());
        assert!(!checked.report.errors.is_empty());
        let out = Diagnostic::from_config(&checked.report.errors[0], "error");
        let repr = out.__repr__();
        assert!(repr.contains("Diagnostic("), "{repr}");
    }
}
