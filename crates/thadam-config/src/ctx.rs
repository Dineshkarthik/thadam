//! The collector every pass reports into.

use thadam_core::codes;

use crate::diagnostics::{ConfigReport, Diagnostic, LineCol};
use crate::path::{self, Seg};
use crate::secret::REDACTED;
use crate::spans::{Anchor, SpanIndex};

/// Values produced by interpolation at one path, kept only while loading so
/// messages can be scrubbed.
pub(crate) struct Resolved {
    pub path: Vec<Seg>,
    pub values: Vec<String>,
}

pub(crate) struct Ctx<'a> {
    spans: &'a SpanIndex,
    errors: Vec<Diagnostic>,
    warnings: Vec<Diagnostic>,
    resolved: Vec<Resolved>,
}

/// Interpolated values this long are removed from every message, wherever they appear.
const SCRUB_EVERYWHERE_MIN_LEN: usize = 8;

impl<'a> Ctx<'a> {
    pub(crate) fn new(spans: &'a SpanIndex) -> Self {
        Ctx {
            spans,
            errors: Vec::new(),
            warnings: Vec::new(),
            resolved: Vec::new(),
        }
    }

    pub(crate) fn spans(&self) -> &SpanIndex {
        self.spans
    }

    pub(crate) fn error_count(&self) -> usize {
        self.errors.len()
    }

    pub(crate) fn add_resolved(&mut self, r: Resolved) {
        self.resolved.push(r);
    }

    fn scrub(&self, path: &[Seg], text: &str) -> String {
        let mut text = text.to_owned();
        for r in &self.resolved {
            let same_path = r.path == path;
            for v in &r.values {
                if !v.is_empty() && (same_path || v.len() >= SCRUB_EVERYWHERE_MIN_LEN) {
                    text = text.replace(v.as_str(), REDACTED);
                }
            }
        }
        text
    }

    fn diagnostic(
        &self,
        code: &'static str,
        path: &[Seg],
        anchor: Anchor,
        message: &str,
        hint: Option<String>,
    ) -> Diagnostic {
        Diagnostic {
            code,
            path: path::display(path),
            span: self.spans.locate(path, anchor),
            message: self.scrub(path, message),
            hint: hint.map(|h| self.scrub(path, &h)),
        }
    }

    pub(crate) fn error(
        &mut self,
        code: &'static str,
        path: &[Seg],
        anchor: Anchor,
        message: impl AsRef<str>,
        hint: Option<String>,
    ) {
        let d = self.diagnostic(code, path, anchor, message.as_ref(), hint);
        self.errors.push(d);
    }

    /// Reports at an explicit position, for problems the index cannot locate.
    pub(crate) fn error_at(
        &mut self,
        code: &'static str,
        path: &[Seg],
        span: LineCol,
        message: impl AsRef<str>,
        hint: Option<String>,
    ) {
        let mut d = self.diagnostic(code, path, Anchor::Key, message.as_ref(), hint);
        d.span = Some(span);
        self.errors.push(d);
    }

    pub(crate) fn invalid(&mut self, path: &[Seg], anchor: Anchor, message: impl AsRef<str>) {
        self.error(codes::CONFIG_INVALID, path, anchor, message, None);
    }

    pub(crate) fn invalid_hint(
        &mut self,
        path: &[Seg],
        anchor: Anchor,
        message: impl AsRef<str>,
        hint: impl Into<String>,
    ) {
        self.error(
            codes::CONFIG_INVALID,
            path,
            anchor,
            message,
            Some(hint.into()),
        );
    }

    pub(crate) fn warn(
        &mut self,
        code: &'static str,
        path: &[Seg],
        anchor: Anchor,
        message: impl AsRef<str>,
        hint: Option<String>,
    ) {
        let d = self.diagnostic(code, path, anchor, message.as_ref(), hint);
        self.warnings.push(d);
    }

    /// Diagnostics in file order; ones with no position come last.
    pub(crate) fn into_report(mut self) -> ConfigReport {
        let by_position =
            |d: &Diagnostic| d.span.map_or((u32::MAX, u32::MAX), |s| (s.line, s.column));
        self.errors.sort_by_key(by_position);
        self.warnings.sort_by_key(by_position);
        ConfigReport {
            errors: self.errors,
            warnings: self.warnings,
        }
    }
}
