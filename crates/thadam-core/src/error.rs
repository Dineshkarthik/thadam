//! The error model: one [`ThadamError`] shape and a [`Classify`] trait that maps
//! each crate's own error enum into it (ARCHITECTURE.md section 13).

use std::fmt;

/// Where in the pipeline an error arose.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ErrorDomain {
    /// A source or protocol connection.
    Connector,
    /// Decoding a payload.
    Decode,
    /// Declared schemas and shapes.
    Schema,
    /// A transform step.
    Transform,
    /// Data-quality rules.
    Quality,
    /// The capture WAL and disk budget.
    Buffer,
    /// A sink or the shipper.
    Sink,
    /// Authentication and authorization.
    Auth,
    /// Configuration.
    Config,
}

impl ErrorDomain {
    /// Every domain, in declaration order.
    pub const ALL: [ErrorDomain; 9] = [
        ErrorDomain::Connector,
        ErrorDomain::Decode,
        ErrorDomain::Schema,
        ErrorDomain::Transform,
        ErrorDomain::Quality,
        ErrorDomain::Buffer,
        ErrorDomain::Sink,
        ErrorDomain::Auth,
        ErrorDomain::Config,
    ];

    /// The stable lowercase name, also the prefix of every code in the domain.
    pub const fn as_str(self) -> &'static str {
        match self {
            ErrorDomain::Connector => "connector",
            ErrorDomain::Decode => "decode",
            ErrorDomain::Schema => "schema",
            ErrorDomain::Transform => "transform",
            ErrorDomain::Quality => "quality",
            ErrorDomain::Buffer => "buffer",
            ErrorDomain::Sink => "sink",
            ErrorDomain::Auth => "auth",
            ErrorDomain::Config => "config",
        }
    }
}

impl fmt::Display for ErrorDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the runtime does about an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Retry {
    /// Retry the same batch with capped, jittered backoff.
    Transient,
    /// Retrying cannot help; a per-event error becomes a reject.
    Permanent,
    /// The component cannot continue and is marked `Failed`.
    Fatal,
}

/// How loudly an error is reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Severity {
    /// Expected under normal operation, such as a retried disconnect.
    Warning,
    /// Needs attention but the pipeline continues.
    Error,
    /// The component has stopped.
    Critical,
}

impl Severity {
    /// The severity an error with the given retry class has unless overridden.
    pub const fn for_retry(retry: Retry) -> Severity {
        match retry {
            Retry::Transient => Severity::Warning,
            Retry::Permanent => Severity::Error,
            Retry::Fatal => Severity::Critical,
        }
    }
}

/// Structured key-value detail attached to an error.
///
/// Never put payloads or secrets here: errors are logged and exported.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrorContext(Vec<(&'static str, String)>);

impl ErrorContext {
    /// Adds a pair.
    pub fn push(&mut self, key: &'static str, value: impl Into<String>) {
        self.0.push((key, value.into()));
    }

    /// The first value stored under `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Iterates pairs in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> {
        self.0.iter().map(|(k, v)| (*k, v.as_str()))
    }

    /// Whether there is no context.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The one error shape every component reports in.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{domain}/{code}: {message}")]
pub struct ThadamError {
    /// Where the error arose.
    pub domain: ErrorDomain,
    /// A stable code from [`crate::codes`].
    pub code: &'static str,
    /// Human-readable detail. Never contains payloads or secrets.
    pub message: String,
    /// The retry class.
    pub retry: Retry,
    /// The reporting severity.
    pub severity: Severity,
    /// Structured detail.
    pub context: ErrorContext,
}

impl ThadamError {
    /// Builds an error with an explicit retry class and its default severity.
    pub fn new(
        domain: ErrorDomain,
        code: &'static str,
        retry: Retry,
        message: impl Into<String>,
    ) -> Self {
        ThadamError {
            domain,
            code,
            message: message.into(),
            retry,
            severity: Severity::for_retry(retry),
            context: ErrorContext::default(),
        }
    }

    /// A [`Retry::Transient`] error.
    pub fn transient(domain: ErrorDomain, code: &'static str, message: impl Into<String>) -> Self {
        Self::new(domain, code, Retry::Transient, message)
    }

    /// A [`Retry::Permanent`] error.
    pub fn permanent(domain: ErrorDomain, code: &'static str, message: impl Into<String>) -> Self {
        Self::new(domain, code, Retry::Permanent, message)
    }

    /// A [`Retry::Fatal`] error.
    pub fn fatal(domain: ErrorDomain, code: &'static str, message: impl Into<String>) -> Self {
        Self::new(domain, code, Retry::Fatal, message)
    }

    /// Overrides the severity.
    #[must_use]
    pub fn with_severity(mut self, severity: Severity) -> Self {
        self.severity = severity;
        self
    }

    /// Adds a context pair.
    #[must_use]
    pub fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push(key, value);
        self
    }

    /// Whether the runtime should retry the operation.
    pub fn is_transient(&self) -> bool {
        self.retry == Retry::Transient
    }
}

/// Maps a crate's own error type into the shared model.
pub trait Classify {
    /// The domain of the error.
    fn domain(&self) -> ErrorDomain;
    /// The stable code, one of [`crate::codes`].
    fn code(&self) -> &'static str;
    /// The retry class.
    fn retry(&self) -> Retry;
    /// The reporting severity; by default derived from [`Classify::retry`].
    fn severity(&self) -> Severity {
        Severity::for_retry(self.retry())
    }

    /// Converts into a [`ThadamError`] using the `Display` text as the message.
    fn to_thadam_error(&self) -> ThadamError
    where
        Self: fmt::Display,
    {
        ThadamError {
            domain: self.domain(),
            code: self.code(),
            message: self.to_string(),
            retry: self.retry(),
            severity: self.severity(),
            context: ErrorContext::default(),
        }
    }
}

/// A per-event failure inside a batch: the event at `index` is routed to
/// quarantine and the rest of the batch proceeds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reject {
    /// Position of the rejected event within the batch. For a segment sink,
    /// the position in `SegmentBatch::events` (datasets in order, then events).
    pub index: u32,
    /// Why it was rejected.
    pub error: ThadamError,
}

impl Reject {
    /// Rejects the event at `index`.
    pub fn new(index: u32, error: ThadamError) -> Self {
        Reject { index, error }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes;

    #[derive(Debug, thiserror::Error)]
    enum Demo {
        #[error("link down")]
        LinkDown,
        #[error("bad credentials")]
        BadCreds,
    }

    impl Classify for Demo {
        fn domain(&self) -> ErrorDomain {
            match self {
                Demo::LinkDown => ErrorDomain::Connector,
                Demo::BadCreds => ErrorDomain::Auth,
            }
        }
        fn code(&self) -> &'static str {
            match self {
                Demo::LinkDown => codes::CONNECTOR_DISCONNECTED,
                Demo::BadCreds => codes::AUTH_FAILED,
            }
        }
        fn retry(&self) -> Retry {
            match self {
                Demo::LinkDown => Retry::Transient,
                Demo::BadCreds => Retry::Fatal,
            }
        }
    }

    #[test]
    fn classify_builds_a_thadam_error() {
        let e = Demo::LinkDown.to_thadam_error();
        assert_eq!(e.domain, ErrorDomain::Connector);
        assert_eq!(e.code, "connector.disconnected");
        assert_eq!(e.message, "link down");
        assert_eq!(e.severity, Severity::Warning);
        assert!(e.is_transient());

        let e = Demo::BadCreds.to_thadam_error();
        assert_eq!((e.retry, e.severity), (Retry::Fatal, Severity::Critical));
    }

    #[test]
    fn display_and_context() {
        let e = ThadamError::permanent(
            ErrorDomain::Decode,
            codes::DECODE_INVALID_PAYLOAD,
            "bad json",
        )
        .with_context("offset", "17")
        .with_severity(Severity::Warning);
        assert_eq!(e.to_string(), "decode/decode.invalid_payload: bad json");
        assert_eq!(e.context.get("offset"), Some("17"));
        assert_eq!(e.context.get("missing"), None);
        assert_eq!(e.severity, Severity::Warning);
    }

    #[test]
    fn default_severity_follows_retry_class() {
        assert_eq!(Severity::for_retry(Retry::Transient), Severity::Warning);
        assert_eq!(Severity::for_retry(Retry::Permanent), Severity::Error);
        assert_eq!(Severity::for_retry(Retry::Fatal), Severity::Critical);
    }
}
