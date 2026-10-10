//! The connector's error type and its mapping into the shared error model.

use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, Severity, codes};

/// Why an MQTT operation failed.
///
/// Variants carry text, never client-library types, so replacing the library
/// does not change this type.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MqttError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// Reading a credential, certificate, or key file failed.
    #[error("cannot read {what}: {detail}")]
    Credentials {
        /// Which file was being read.
        what: &'static str,
        /// The underlying reason.
        detail: String,
    },
    /// The TLS configuration could not be built or the handshake failed.
    #[error("TLS: {0}")]
    Tls(String),
    /// A network-level failure: connection refused, reset, or timed out.
    #[error("connection failed: {0}")]
    Network(String),
    /// The broker refused the connection in a way retrying cannot fix.
    #[error("broker refused the connection: {0}")]
    Refused(String),
    /// The broker refused the connection transiently (for example while starting).
    #[error("broker unavailable: {0}")]
    Unavailable(String),
    /// The broker rejected a subscription.
    #[error("broker rejected subscription: {0}")]
    SubscribeRejected(String),
    /// The broker sent a packet larger than the configured limit, repeatedly.
    #[error(
        "the broker keeps sending a packet of {size} bytes, above the {limit} byte incoming limit \
         derived from max_message_bytes; align the broker's message size limit"
    )]
    PacketTooLarge {
        /// The offending packet's declared size.
        size: usize,
        /// The configured incoming limit.
        limit: usize,
    },
    /// More messages were unacknowledged than the configured guard allows,
    /// repeatedly.
    #[error(
        "{unacked} messages were unacknowledged, above the guard of {limit}; \
         raise expected_broker_window to the broker's inflight window"
    )]
    UnackedGuard {
        /// Unacknowledged messages when the guard tripped.
        unacked: usize,
        /// The guard threshold.
        limit: usize,
    },
    /// The operation was cancelled by shutdown.
    #[error("cancelled by shutdown")]
    Cancelled,
    /// The connection did not become ready in time.
    #[error("timed out after {0:?} waiting for the broker")]
    OpenTimeout(std::time::Duration),
    /// A published message was not acknowledged in time.
    #[error("no acknowledgement from the broker within {0:?}")]
    PublishTimeout(std::time::Duration),
    /// A connector task panicked.
    #[error("connector task panicked")]
    Panic,
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
}

impl Classify for MqttError {
    fn domain(&self) -> ErrorDomain {
        match self {
            MqttError::Config(_) => ErrorDomain::Config,
            MqttError::Credentials { .. } => ErrorDomain::Config,
            MqttError::Refused(_) | MqttError::SubscribeRejected(_) => ErrorDomain::Auth,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            MqttError::Config(_) | MqttError::Credentials { .. } => codes::CONFIG_INVALID,
            MqttError::Tls(_) | MqttError::Network(_) | MqttError::Unavailable(_) => {
                codes::CONNECTOR_CONNECT_FAILED
            }
            MqttError::Refused(_) => codes::AUTH_FAILED,
            MqttError::SubscribeRejected(_) => codes::AUTH_FORBIDDEN,
            MqttError::PacketTooLarge { .. } => codes::CONNECTOR_MESSAGE_TOO_LARGE,
            MqttError::UnackedGuard { .. } | MqttError::State(_) => codes::CONNECTOR_PROTOCOL,
            MqttError::Cancelled => codes::CONNECTOR_CANCELLED,
            MqttError::OpenTimeout(_) | MqttError::PublishTimeout(_) => codes::CONNECTOR_TIMEOUT,
            MqttError::Panic => codes::CONNECTOR_PANIC,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            MqttError::Tls(_)
            | MqttError::Network(_)
            | MqttError::Unavailable(_)
            | MqttError::OpenTimeout(_)
            | MqttError::PublishTimeout(_) => Retry::Transient,
            MqttError::Cancelled => Retry::Permanent,
            MqttError::Config(_)
            | MqttError::Credentials { .. }
            | MqttError::Refused(_)
            | MqttError::SubscribeRejected(_)
            | MqttError::PacketTooLarge { .. }
            | MqttError::UnackedGuard { .. }
            | MqttError::Panic
            | MqttError::State(_) => Retry::Fatal,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            MqttError::Cancelled => Severity::Warning,
            other => Severity::for_retry(other.retry()),
        }
    }
}

impl From<MqttError> for ThadamError {
    fn from(e: MqttError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_follows_the_error_model() {
        let refused: ThadamError = MqttError::Refused("not authorized".into()).into();
        assert_eq!(refused.domain, ErrorDomain::Auth);
        assert_eq!(refused.code, codes::AUTH_FAILED);
        assert_eq!(refused.retry, Retry::Fatal);

        let net: ThadamError = MqttError::Network("reset".into()).into();
        assert_eq!(net.domain, ErrorDomain::Connector);
        assert_eq!(net.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(net.is_transient());

        let big: ThadamError = MqttError::PacketTooLarge { size: 9, limit: 4 }.into();
        assert_eq!(big.code, codes::CONNECTOR_MESSAGE_TOO_LARGE);
        assert_eq!(big.retry, Retry::Fatal);

        let cancelled: ThadamError = MqttError::Cancelled.into();
        assert_eq!(cancelled.code, codes::CONNECTOR_CANCELLED);
        assert_eq!(cancelled.severity, Severity::Warning);
    }

    #[test]
    fn messages_never_mention_client_library_types() {
        let e = MqttError::SubscribeRejected("robots/#".into());
        assert!(e.to_string().contains("robots/#"));
        assert_eq!(e.to_thadam_error().code, codes::AUTH_FORBIDDEN);
    }
}
