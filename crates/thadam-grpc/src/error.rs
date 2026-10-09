//! The connector's error type and its mapping into the shared error model.

use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, Severity, codes};

/// Why a gRPC source operation failed.
///
/// Messages never include payloads or bearer tokens.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum GrpcError {
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
    /// The gRPC server stopped serving.
    #[error("gRPC server stopped: {0}")]
    Serve(String),
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
}

impl Classify for GrpcError {
    fn domain(&self) -> ErrorDomain {
        match self {
            GrpcError::Config(_) | GrpcError::Tls(_) => ErrorDomain::Config,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            GrpcError::Config(_) | GrpcError::Tls(_) => codes::CONFIG_INVALID,
            GrpcError::Bind { .. } => codes::CONNECTOR_CONNECT_FAILED,
            GrpcError::Serve(_) => codes::CONNECTOR_DISCONNECTED,
            GrpcError::State(_) => codes::CONNECTOR_PROTOCOL,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            GrpcError::Config(_) | GrpcError::Tls(_) | GrpcError::State(_) => Retry::Fatal,
            GrpcError::Bind { .. } | GrpcError::Serve(_) => Retry::Transient,
        }
    }

    fn severity(&self) -> Severity {
        Severity::for_retry(self.retry())
    }
}

impl From<GrpcError> for ThadamError {
    fn from(e: GrpcError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_are_fatal_config_errors() {
        let e: ThadamError = GrpcError::Config("bad".into()).into();
        assert_eq!(e.domain, ErrorDomain::Config);
        assert_eq!(e.code, codes::CONFIG_INVALID);
        assert_eq!(e.retry, Retry::Fatal);
    }

    #[test]
    fn bind_failures_are_transient() {
        let e: ThadamError = GrpcError::Bind {
            addr: "127.0.0.1:1".into(),
            detail: "in use".into(),
        }
        .into();
        assert_eq!(e.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(e.is_transient());
    }
}
