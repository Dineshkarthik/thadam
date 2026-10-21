//! The connector's error type and its mapping into the shared error model.
//!
//! Connect, timeout, transport, and protocol failures are transient: the
//! session task reconnects. Authentication failures are fatal — retrying with
//! the same credentials only hammers the server.

use std::time::Duration;

use thadam_core::{Classify, ErrorDomain, Retry, Severity, ThadamError, codes};

/// Why an OPC UA source operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OpcUaError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// The server could not be reached, discovered, or handshaken.
    #[error("cannot connect to {endpoint}: {detail}")]
    Connect {
        /// The configured `opc.tcp://` URL.
        endpoint: String,
        /// The underlying reason.
        detail: String,
    },
    /// The server rejected the identity token or the channel security.
    ///
    /// Fatal: reconnecting with the same credentials can never succeed and
    /// each attempt is one more timing sample (see the crate docs on
    /// RUSTSEC-2023-0071).
    #[error("authentication failed: {0}")]
    Authenticate(String),
    /// An established session dropped or misbehaved.
    #[error("session lost: {0}")]
    Transport(String),
    /// The server or client library violated the protocol.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// A protocol operation did not finish within `timeout`.
    #[error("no answer within {0:?}")]
    Timeout(Duration),
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
    /// The operation was cancelled by shutdown.
    #[error("cancelled by shutdown")]
    Cancelled,
}

impl Classify for OpcUaError {
    fn domain(&self) -> ErrorDomain {
        match self {
            OpcUaError::Config(_) => ErrorDomain::Config,
            OpcUaError::Authenticate(_) => ErrorDomain::Auth,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            OpcUaError::Config(_) => codes::CONFIG_INVALID,
            OpcUaError::Connect { .. } => codes::CONNECTOR_CONNECT_FAILED,
            OpcUaError::Authenticate(_) => codes::AUTH_FAILED,
            OpcUaError::Transport(_) => codes::CONNECTOR_DISCONNECTED,
            OpcUaError::Protocol(_) | OpcUaError::State(_) => codes::CONNECTOR_PROTOCOL,
            OpcUaError::Timeout(_) => codes::CONNECTOR_TIMEOUT,
            OpcUaError::Cancelled => codes::CONNECTOR_CANCELLED,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            OpcUaError::Config(_) | OpcUaError::Authenticate(_) | OpcUaError::State(_) => {
                Retry::Fatal
            }
            OpcUaError::Connect { .. }
            | OpcUaError::Transport(_)
            | OpcUaError::Protocol(_)
            | OpcUaError::Timeout(_) => Retry::Transient,
            OpcUaError::Cancelled => Retry::Permanent,
        }
    }

    fn severity(&self) -> Severity {
        match self {
            OpcUaError::Cancelled => Severity::Warning,
            other => Severity::for_retry(other.retry()),
        }
    }
}

impl From<OpcUaError> for ThadamError {
    fn from(e: OpcUaError) -> Self {
        e.to_thadam_error()
    }
}

/// A failure to reach a usable session. Identity and certificate rejections
/// that surface while connecting are fatal like any other rejection.
pub(crate) fn from_connect(
    endpoint: &str,
    status: opcua::types::StatusCode,
    detail: String,
) -> OpcUaError {
    match from_status(status) {
        fatal @ OpcUaError::Authenticate(_) => fatal,
        _ => OpcUaError::Connect {
            endpoint: endpoint.to_owned(),
            detail,
        },
    }
}

/// Maps an OPC UA status code from a service response to an error. Identity,
/// user, and certificate failures are fatal; everything else is transient.
pub(crate) fn from_status(status: opcua::types::StatusCode) -> OpcUaError {
    use opcua::types::StatusCode as S;
    let name = status_name(&status);
    if matches!(
        status,
        S::BadIdentityTokenInvalid
            | S::BadIdentityTokenRejected
            | S::BadUserAccessDenied
            | S::BadUserSignatureInvalid
            | S::BadSecurityChecksFailed
            | S::BadCertificateInvalid
            | S::BadCertificateUntrusted
            | S::BadCertificateHostNameInvalid
            | S::BadCertificateUriInvalid
            | S::BadCertificateUseNotAllowed
            | S::BadCertificateRevoked
            | S::BadCertificateRevocationUnknown
            | S::BadCertificateChainIncomplete
            | S::BadCertificateTimeInvalid
            | S::BadNoValidCertificates
            | S::BadTcpSecureChannelUnknown
    ) {
        OpcUaError::Authenticate(format!("the server answered {name}"))
    } else {
        OpcUaError::Protocol(format!("the server answered {name}"))
    }
}

/// The OPC UA name of a status code for messages and attributes
/// (`BadSensorFailure`), or its hex value for codes this table does not
/// know. `async-opcua` exposes the constants but no name lookup, so the
/// codes a Thadam operator is likely to meet are spelled out here.
pub(crate) fn status_name(status: &opcua::types::StatusCode) -> String {
    use opcua::types::StatusCode as S;
    let name = match *status {
        S::Good => "Good",
        S::Uncertain => "Uncertain",
        S::Bad => "Bad",
        S::BadUnexpectedError => "BadUnexpectedError",
        S::BadInternalError => "BadInternalError",
        S::BadTimeout => "BadTimeout",
        S::BadCommunicationError => "BadCommunicationError",
        S::BadNotConnected => "BadNotConnected",
        S::BadDeviceFailure => "BadDeviceFailure",
        S::BadSensorFailure => "BadSensorFailure",
        S::BadOutOfService => "BadOutOfService",
        S::BadNodeIdInvalid => "BadNodeIdInvalid",
        S::BadNodeIdUnknown => "BadNodeIdUnknown",
        S::BadAttributeIdInvalid => "BadAttributeIdInvalid",
        S::BadIndexRangeInvalid => "BadIndexRangeInvalid",
        S::BadNotReadable => "BadNotReadable",
        S::BadUserAccessDenied => "BadUserAccessDenied",
        S::BadSubscriptionIdInvalid => "BadSubscriptionIdInvalid",
        S::BadMonitoredItemIdInvalid => "BadMonitoredItemIdInvalid",
        S::BadNothingToDo => "BadNothingToDo",
        S::BadTooManyOperations => "BadTooManyOperations",
        S::BadTimestampsToReturnInvalid => "BadTimestampsToReturnInvalid",
        S::BadMonitoringModeInvalid => "BadMonitoringModeInvalid",
        S::BadDeadbandFilterInvalid => "BadDeadbandFilterInvalid",
        S::BadNoSubscription => "BadNoSubscription",
        S::BadTooManySubscriptions => "BadTooManySubscriptions",
        S::BadTooManyPublishRequests => "BadTooManyPublishRequests",
        S::BadServiceUnsupported => "BadServiceUnsupported",
        S::BadSecureChannelClosed => "BadSecureChannelClosed",
        S::BadSessionClosed => "BadSessionClosed",
        S::BadSessionIdInvalid => "BadSessionIdInvalid",
        S::BadSecureChannelTokenUnknown => "BadSecureChannelTokenUnknown",
        S::BadIdentityTokenInvalid => "BadIdentityTokenInvalid",
        S::BadIdentityTokenRejected => "BadIdentityTokenRejected",
        S::BadUserSignatureInvalid => "BadUserSignatureInvalid",
        S::BadSecurityChecksFailed => "BadSecurityChecksFailed",
        S::BadCertificateInvalid => "BadCertificateInvalid",
        S::BadCertificateUntrusted => "BadCertificateUntrusted",
        S::BadCertificateHostNameInvalid => "BadCertificateHostNameInvalid",
        S::BadCertificateUriInvalid => "BadCertificateUriInvalid",
        S::BadCertificateUseNotAllowed => "BadCertificateUseNotAllowed",
        S::BadCertificateRevoked => "BadCertificateRevoked",
        S::BadCertificateRevocationUnknown => "BadCertificateRevocationUnknown",
        S::BadCertificateChainIncomplete => "BadCertificateChainIncomplete",
        S::BadCertificateTimeInvalid => "BadCertificateTimeInvalid",
        S::BadNoValidCertificates => "BadNoValidCertificates",
        S::BadTcpSecureChannelUnknown => "BadTcpSecureChannelUnknown",
        S::UncertainNoCommunicationLastUsableValue => "UncertainNoCommunicationLastUsableValue",
        S::UncertainLastUsableValue => "UncertainLastUsableValue",
        S::UncertainSubstituteValue => "UncertainSubstituteValue",
        S::UncertainInitialValue => "UncertainInitialValue",
        S::UncertainSensorNotAccurate => "UncertainSensorNotAccurate",
        S::UncertainEngineeringUnitsExceeded => "UncertainEngineeringUnitsExceeded",
        S::UncertainSubNormal => "UncertainSubNormal",
        S::GoodLocalOverride => "GoodLocalOverride",
        S::GoodSubNormal => "GoodSubNormal",
        _ => return format!("0x{:08X}", status.bits()),
    };
    name.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::types::StatusCode;

    #[test]
    fn connect_failures_are_transient() {
        let e: ThadamError = OpcUaError::Connect {
            endpoint: "opc.tcp://plc:4840".into(),
            detail: "refused".into(),
        }
        .into();
        assert_eq!(e.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(e.is_transient());
    }

    #[test]
    fn connect_time_rejections_keep_their_classification() {
        let fatal = from_connect(
            "opc.tcp://plc:4840",
            StatusCode::BadCertificateUntrusted,
            "rejected".into(),
        );
        assert!(matches!(fatal, OpcUaError::Authenticate(_)), "{fatal:?}");

        let transient = from_connect("opc.tcp://plc:4840", StatusCode::BadTimeout, "slow".into());
        assert!(
            matches!(transient, OpcUaError::Connect { .. }),
            "{transient:?}"
        );
    }

    #[test]
    fn auth_failures_are_fatal_auth_domain_errors() {
        let e: ThadamError = from_status(StatusCode::BadIdentityTokenRejected).into();
        assert_eq!(e.domain, ErrorDomain::Auth);
        assert_eq!(e.code, codes::AUTH_FAILED);
        assert_eq!(e.retry, Retry::Fatal);

        let e: ThadamError = from_status(StatusCode::BadSubscriptionIdInvalid).into();
        assert!(e.is_transient(), "{e}");
    }
}
