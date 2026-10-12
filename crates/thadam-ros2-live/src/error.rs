//! The connector's error type and its mapping into the shared error model.

use std::path::PathBuf;

use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, Severity, codes};

/// Why a live ROS 2 source operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Ros2LiveError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// A `.msg` file under `msg_dirs` could not be read, or a type's
    /// assembled schema text failed to compile.
    #[error("msg schema {type_name}: {detail}")]
    Schema {
        /// The ROS type whose schema failed, or the layout root.
        type_name: String,
        /// The underlying reason.
        detail: String,
    },
    /// A `.msg` file could not be found under any of the `msg_dirs`.
    #[error("no `{relative}` under any msg_dirs entry (looked in {}, and {} more)", tried.first().map(|p| p.display().to_string()).unwrap_or_default(), tried.len().saturating_sub(1))]
    MsgNotFound {
        /// The relative file that was searched, e.g. `sensor_msgs/msg/Imu.msg`.
        relative: String,
        /// The `msg_dirs` entries that were tried.
        tried: Vec<PathBuf>,
    },
    /// The Zenoh session or a subscription could not be established.
    #[error("connect {endpoint}: {detail}")]
    Connect {
        /// The configured `endpoint`.
        endpoint: String,
        /// The underlying reason.
        detail: String,
    },
    /// The session was lost after a successful connect.
    #[error("{0}")]
    Transport(String),
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
    /// A schema-store write failed before the first message referencing the
    /// schema could reach the WAL.
    #[error("schema store: {0}")]
    Store(String),
}

impl Classify for Ros2LiveError {
    fn domain(&self) -> ErrorDomain {
        match self {
            Ros2LiveError::Config(_) | Ros2LiveError::MsgNotFound { .. } => ErrorDomain::Config,
            Ros2LiveError::Schema { .. } => ErrorDomain::Schema,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Ros2LiveError::Config(_) | Ros2LiveError::MsgNotFound { .. } => codes::CONFIG_INVALID,
            Ros2LiveError::Schema { .. } => codes::SCHEMA_INVALID_SHAPE,
            Ros2LiveError::Connect { .. } => codes::CONNECTOR_CONNECT_FAILED,
            Ros2LiveError::Transport(_) => codes::CONNECTOR_DISCONNECTED,
            Ros2LiveError::State(_) => codes::CONNECTOR_PROTOCOL,
            Ros2LiveError::Store(_) => codes::BUFFER_IO,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            Ros2LiveError::Connect { .. } | Ros2LiveError::Transport(_) => Retry::Transient,
            _ => Retry::Fatal,
        }
    }

    fn severity(&self) -> Severity {
        Severity::for_retry(self.retry())
    }
}

impl From<Ros2LiveError> for ThadamError {
    fn from(e: Ros2LiveError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_and_msg_errors_are_fatal() {
        for e in [
            Ros2LiveError::Config("bad".into()),
            Ros2LiveError::MsgNotFound {
                relative: "sensor_msgs/msg/Imu.msg".into(),
                tried: vec![PathBuf::from("/opt/ros/jazzy/share")],
            },
        ] {
            let e: ThadamError = e.into();
            assert_eq!(e.domain, ErrorDomain::Config);
            assert_eq!(e.code, codes::CONFIG_INVALID);
            assert_eq!(e.retry, Retry::Fatal);
        }
    }

    #[test]
    fn connect_is_a_transient_connector_error() {
        let e: ThadamError = Ros2LiveError::Connect {
            endpoint: "tcp/127.0.0.1:7447".into(),
            detail: "refused".into(),
        }
        .into();
        assert_eq!(e.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(e.is_transient());
    }

    #[test]
    fn schema_errors_are_in_the_schema_domain() {
        let e: ThadamError = Ros2LiveError::Schema {
            type_name: "a_msgs/msg/Broken".into(),
            detail: "unknown field type".into(),
        }
        .into();
        assert_eq!(e.domain, ErrorDomain::Schema);
        assert_eq!(e.retry, Retry::Fatal);
    }
}
