//! The connector's error type and its mapping into the shared error model.

use std::time::Duration;

use thadam_core::{Classify, ErrorDomain, Retry, Severity, ThadamError, codes};

/// Why an HTTP source operation failed.
///
/// Messages never include request bodies, bearer tokens, or URL query strings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HttpError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// TLS material could not be read or turned into a configuration.
    #[error("TLS: {0}")]
    Tls(String),
    /// The listen address could not be bound.
    #[error("cannot listen on {addr}: {detail}")]
    Bind {
        /// The configured address.
        addr: String,
        /// The underlying reason.
        detail: String,
    },
    /// The HTTP server stopped serving.
    #[error("HTTP server stopped: {0}")]
    Serve(String),
    /// A poll request failed before a response arrived.
    #[error("request to {url} failed: {detail}")]
    Request {
        /// The URL without its query string.
        url: String,
        /// The underlying reason.
        detail: String,
    },
    /// A poll response carried a status other than 2xx.
    #[error("{url} answered with status {status}")]
    Status {
        /// The URL without its query string.
        url: String,
        /// The HTTP status code.
        status: u16,
    },
    /// A poll response body exceeded `max_message_bytes`.
    #[error("the response from {url} exceeds the {limit} byte message limit")]
    TooLarge {
        /// The URL without its query string.
        url: String,
        /// The configured limit.
        limit: usize,
    },
    /// A poll request did not finish within the configured timeout.
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    /// The operation was cancelled by shutdown.
    #[error("cancelled by shutdown")]
    Cancelled,
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
}

impl Classify for HttpError {
    fn domain(&self) -> ErrorDomain {
        match self {
            HttpError::Config(_) | HttpError::Tls(_) => ErrorDomain::Config,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            HttpError::Config(_) | HttpError::Tls(_) => codes::CONFIG_INVALID,
            HttpError::Bind { .. } | HttpError::Request { .. } => codes::CONNECTOR_CONNECT_FAILED,
            HttpError::Serve(_) => codes::CONNECTOR_DISCONNECTED,
            HttpError::Status { .. } | HttpError::State(_) => codes::CONNECTOR_PROTOCOL,
            HttpError::TooLarge { .. } => codes::CONNECTOR_MESSAGE_TOO_LARGE,
            HttpError::Timeout(_) => codes::CONNECTOR_TIMEOUT,
            HttpError::Cancelled => codes::CONNECTOR_CANCELLED,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            HttpError::Config(_) | HttpError::Tls(_) | HttpError::State(_) => Retry::Fatal,
            HttpError::Bind { .. }
            | HttpError::Serve(_)
            | HttpError::Request { .. }
            | HttpError::Status { .. }
            | HttpError::Timeout(_) => Retry::Transient,
            HttpError::TooLarge { .. } => Retry::Permanent,
            HttpError::Cancelled => Retry::Permanent,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            HttpError::Cancelled => Severity::Warning,
            other => Severity::for_retry(other.retry()),
        }
    }
}

impl From<HttpError> for ThadamError {
    fn from(e: HttpError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_are_fatal_config_errors() {
        let e: ThadamError = HttpError::Config("bad".into()).into();
        assert_eq!(e.domain, ErrorDomain::Config);
        assert_eq!(e.code, codes::CONFIG_INVALID);
        assert_eq!(e.retry, Retry::Fatal);
    }

    #[test]
    fn network_failures_are_transient() {
        let e: ThadamError = HttpError::Timeout(Duration::from_secs(1)).into();
        assert_eq!(e.code, codes::CONNECTOR_TIMEOUT);
        assert!(e.is_transient());
        let e: ThadamError = HttpError::Bind {
            addr: "127.0.0.1:1".into(),
            detail: "in use".into(),
        }
        .into();
        assert_eq!(e.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(e.is_transient());
    }

    #[test]
    fn cancellation_is_a_warning() {
        let e: ThadamError = HttpError::Cancelled.into();
        assert_eq!(e.code, codes::CONNECTOR_CANCELLED);
        assert_eq!(e.severity, Severity::Warning);
    }
}
