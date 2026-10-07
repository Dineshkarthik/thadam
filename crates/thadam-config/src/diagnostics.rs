//! Diagnostics: every problem found, with a path, a position, and a hint.

use std::fmt;

use thadam_core::{Classify, ErrorDomain, Retry, codes};

/// A 1-based line and column in the YAML source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LineCol {
    /// The 1-based line.
    pub line: u32,
    /// The 1-based column, counted in characters.
    pub column: u32,
}

impl fmt::Display for LineCol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

/// One problem in a configuration file.
///
/// Messages never contain a value that came from `${env:}` or `${file:}`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Diagnostic {
    /// A stable code from [`thadam_core::codes`], such as `config.invalid`.
    pub code: &'static str,
    /// The document path, such as `sinks[1].config.bucket`; `$` is the root.
    pub path: String,
    /// Where the problem is in the source file, when known.
    pub span: Option<LineCol>,
    /// What is wrong.
    pub message: String,
    /// How to fix it, when there is a concrete suggestion.
    pub hint: Option<String>,
}

impl Diagnostic {
    /// Renders the diagnostic in a compiler-like form, naming `source` as the file.
    pub fn render(&self, severity: &str, source: &str) -> String {
        let mut out = format!("{severity}[{}]: {}\n", self.code, self.message);
        match self.span {
            Some(span) => out.push_str(&format!("  --> {source}:{span} ({})\n", self.path)),
            None => out.push_str(&format!("  --> {source} ({})\n", self.path)),
        }
        if let Some(hint) = &self.hint {
            out.push_str(&format!("  = hint: {hint}\n"));
        }
        out
    }
}

/// Every error and warning found while loading a configuration.
///
/// Loading collects all problems rather than stopping at the first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConfigReport {
    /// Problems that make the configuration unusable.
    pub errors: Vec<Diagnostic>,
    /// Problems that do not prevent startup.
    pub warnings: Vec<Diagnostic>,
}

impl ConfigReport {
    /// Whether any error was found.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Renders all errors, then all warnings, then a summary line.
    pub fn render(&self, source: &str) -> String {
        let mut out = String::new();
        for d in &self.errors {
            out.push_str(&d.render("error", source));
        }
        for d in &self.warnings {
            out.push_str(&d.render("warning", source));
        }
        out.push_str(&self.summary());
        out.push('\n');
        out
    }

    /// `2 errors, 1 warning`.
    pub fn summary(&self) -> String {
        let plural = |n: usize, word: &str| {
            if n == 1 {
                format!("{n} {word}")
            } else {
                format!("{n} {word}s")
            }
        };
        format!(
            "{}, {}",
            plural(self.errors.len(), "error"),
            plural(self.warnings.len(), "warning")
        )
    }
}

impl fmt::Display for ConfigReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.render("<config>").trim_end())
    }
}

impl std::error::Error for ConfigReport {}

impl Classify for ConfigReport {
    fn domain(&self) -> ErrorDomain {
        ErrorDomain::Config
    }

    fn code(&self) -> &'static str {
        self.errors
            .first()
            .map_or(codes::CONFIG_INVALID, |d| d.code)
    }

    fn retry(&self) -> Retry {
        Retry::Fatal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_path_position_and_hint() {
        let report = ConfigReport {
            errors: vec![Diagnostic {
                code: codes::CONFIG_INVALID,
                path: "wal.retain_for".into(),
                span: Some(LineCol {
                    line: 25,
                    column: 15,
                }),
                message: "bad duration".into(),
                hint: Some("use `1h`".into()),
            }],
            warnings: vec![],
        };
        let text = report.render("pipeline.yaml");
        assert!(text.contains("error[config.invalid]: bad duration"));
        assert!(text.contains("pipeline.yaml:25:15 (wal.retain_for)"));
        assert!(text.contains("hint: use `1h`"));
        assert!(text.ends_with("1 error, 0 warnings\n"));
    }

    #[test]
    fn classifies_as_config_domain() {
        let r = ConfigReport::default();
        assert_eq!(r.domain(), ErrorDomain::Config);
        assert_eq!(r.to_thadam_error().code, codes::CONFIG_INVALID);
    }
}
