//! The connector's error type and its mapping into the shared error model.

use std::path::PathBuf;

use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, Severity, codes};

/// Why a SocketCAN source operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CanError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The DBC file could not be read or parsed.
    #[error("dbc {path}: {detail}")]
    Dbc {
        /// The configured path.
        path: PathBuf,
        /// The underlying reason.
        detail: String,
    },
    /// The interface could not be opened.
    #[error("cannot open {interface}: {detail}")]
    Connect {
        /// The configured `interface`.
        interface: String,
        /// The underlying reason.
        detail: String,
    },
    /// Receiving failed while the interface was in use.
    #[error("receive failed: {0}")]
    Transport(String),
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
    /// The operation was cancelled by shutdown.
    #[error("cancelled by shutdown")]
    Cancelled,
}

impl Classify for CanError {
    fn domain(&self) -> ErrorDomain {
        match self {
            CanError::Config(_) | CanError::Dbc { .. } => ErrorDomain::Config,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            CanError::Config(_) | CanError::Dbc { .. } => codes::CONFIG_INVALID,
            CanError::Connect { .. } => codes::CONNECTOR_CONNECT_FAILED,
            CanError::Transport(_) => codes::CONNECTOR_DISCONNECTED,
            CanError::State(_) => codes::CONNECTOR_PROTOCOL,
            CanError::Cancelled => codes::CONNECTOR_CANCELLED,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            CanError::Config(_) | CanError::Dbc { .. } | CanError::State(_) => Retry::Fatal,
            CanError::Connect { .. } | CanError::Transport(_) => Retry::Transient,
            CanError::Cancelled => Retry::Permanent,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            CanError::Cancelled => Severity::Warning,
            other => Severity::for_retry(other.retry()),
        }
    }
}

impl From<CanError> for ThadamError {
    fn from(e: CanError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_and_dbc_errors_are_fatal_config_errors() {
        for e in [
            CanError::Config("bad".into()),
            CanError::Dbc {
                path: PathBuf::from("x.dbc"),
                detail: "no such file".into(),
            },
        ] {
            let e: ThadamError = e.into();
            assert_eq!(e.domain, ErrorDomain::Config);
            assert_eq!(e.code, codes::CONFIG_INVALID);
            assert_eq!(e.retry, Retry::Fatal);
        }
    }

    #[test]
    fn socket_failures_are_transient_connector_errors() {
        let connect: ThadamError = CanError::Connect {
            interface: "can9".into(),
            detail: "no such device".into(),
        }
        .into();
        assert_eq!(connect.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(connect.is_transient());

        let transport: ThadamError = CanError::Transport("bus off".into()).into();
        assert_eq!(transport.code, codes::CONNECTOR_DISCONNECTED);
        assert!(transport.is_transient());
    }
}
