//! The connector's error type and its mapping into the shared error model.

use std::time::Duration;

use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, Severity, codes};

/// Why a Modbus source operation failed.
///
/// Connection-level variants end the session; the poll loop reconnects with
/// backoff. [`ModbusError::Exception`] never ends a session: the poll loop
/// reports it for one read and keeps going.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ModbusError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The server could not be reached.
    #[error("cannot connect to {address}: {detail}")]
    Connect {
        /// The configured `host:port`.
        address: String,
        /// The underlying reason.
        detail: String,
    },
    /// Connecting or a read did not finish within `timeout`.
    #[error("no answer within {0:?}")]
    Timeout(Duration),
    /// The connection failed while in use.
    #[error("connection lost: {0}")]
    Transport(String),
    /// The server sent a response that does not match the request.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// The server refused one read with a Modbus exception code.
    #[error("read `{read}` refused with exception 0x{code:02X}")]
    Exception {
        /// The read's `name`.
        read: String,
        /// The Modbus exception code.
        code: u8,
    },
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
    /// The operation was cancelled by shutdown.
    #[error("cancelled by shutdown")]
    Cancelled,
}

impl Classify for ModbusError {
    fn domain(&self) -> ErrorDomain {
        match self {
            ModbusError::Config(_) => ErrorDomain::Config,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ModbusError::Config(_) => codes::CONFIG_INVALID,
            ModbusError::Connect { .. } => codes::CONNECTOR_CONNECT_FAILED,
            ModbusError::Timeout(_) => codes::CONNECTOR_TIMEOUT,
            ModbusError::Transport(_) => codes::CONNECTOR_DISCONNECTED,
            ModbusError::Protocol(_) | ModbusError::Exception { .. } | ModbusError::State(_) => {
                codes::CONNECTOR_PROTOCOL
            }
            ModbusError::Cancelled => codes::CONNECTOR_CANCELLED,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            ModbusError::Config(_) | ModbusError::State(_) => Retry::Fatal,
            ModbusError::Connect { .. }
            | ModbusError::Timeout(_)
            | ModbusError::Transport(_)
            | ModbusError::Protocol(_) => Retry::Transient,
            ModbusError::Exception { .. } | ModbusError::Cancelled => Retry::Permanent,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            ModbusError::Cancelled => Severity::Warning,
            other => Severity::for_retry(other.retry()),
        }
    }
}

impl From<ModbusError> for ThadamError {
    fn from(e: ModbusError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_are_fatal_config_errors() {
        let e: ThadamError = ModbusError::Config("bad".into()).into();
        assert_eq!(e.domain, ErrorDomain::Config);
        assert_eq!(e.code, codes::CONFIG_INVALID);
        assert_eq!(e.retry, Retry::Fatal);
    }

    #[test]
    fn session_failures_are_transient() {
        let connect: ThadamError = ModbusError::Connect {
            address: "127.0.0.1:502".into(),
            detail: "refused".into(),
        }
        .into();
        assert_eq!(connect.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(connect.is_transient());

        let timeout: ThadamError = ModbusError::Timeout(Duration::from_secs(1)).into();
        assert_eq!(timeout.code, codes::CONNECTOR_TIMEOUT);
        assert!(timeout.is_transient());
    }

    #[test]
    fn exceptions_are_permanent_for_their_read() {
        let e: ThadamError = ModbusError::Exception {
            read: "bad".into(),
            code: 0x02,
        }
        .into();
        assert_eq!(e.retry, Retry::Permanent);
        assert!(e.message.contains("0x02"), "{}", e.message);
    }
}
