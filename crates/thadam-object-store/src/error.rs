//! Ship errors and their classification (ARCHITECTURE.md section 13, plan
//! 5.8 "Error model").

use object_store::Error as StoreError;
use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, codes};

/// One failure the shipper can report.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ShipError {
    /// An `object_store` operation failed; classification comes from its
    /// variant (auth errors are fatal, the rest transient).
    #[error("store: {0}")]
    Store(StoreError),
    /// A segment-catalog operation failed.
    #[error("catalog: {0}")]
    Catalog(thadam_segments::SegmentError),
    /// Reading the local segment file failed.
    #[error("{path}: {message}")]
    LocalFile {
        /// The segment file.
        path: String,
        /// What went wrong.
        message: String,
    },
    /// A create-if-absent conflict found an object with different content
    /// (epoch or node-id misconfiguration; ADR-0013).
    #[error("object key collision at {key}: {detail}")]
    Collision {
        /// The object key.
        key: String,
        /// How the existing object differs.
        detail: String,
    },
    /// `conditional_put: create` on a store that does not enforce
    /// If-None-Match (detected by the startup probe).
    #[error("the store ignores If-None-Match; conditional_put: create cannot be honored")]
    UnsupportedConditional,
    /// Shutdown happened mid-upload; control flow only, never surfaced.
    #[error("cancelled")]
    Cancelled,
    /// A crash failpoint fired (`failpoints` feature and unit tests).
    #[error("crashed at failpoint {0}")]
    Crashed(&'static str),
    /// An internal invariant broke (task panic, unresolvable segment shape).
    #[error("internal: {0}")]
    Internal(String),
}

fn store_class(error: &StoreError) -> (Retry, ErrorDomain, &'static str) {
    match error {
        StoreError::Unauthenticated { .. } => (Retry::Fatal, ErrorDomain::Auth, codes::AUTH_FAILED),
        StoreError::PermissionDenied { .. } => {
            (Retry::Fatal, ErrorDomain::Auth, codes::AUTH_FORBIDDEN)
        }
        StoreError::NotImplemented { .. } | StoreError::NotSupported { .. } => {
            (Retry::Fatal, ErrorDomain::Config, codes::CONFIG_INVALID)
        }
        _ => (Retry::Transient, ErrorDomain::Sink, codes::SINK_UNAVAILABLE),
    }
}

impl Classify for ShipError {
    fn domain(&self) -> ErrorDomain {
        match self {
            ShipError::Store(e) => store_class(e).1,
            ShipError::Catalog(e) => e.domain(),
            ShipError::LocalFile { .. } => ErrorDomain::Sink,
            ShipError::Collision { .. } => ErrorDomain::Sink,
            ShipError::UnsupportedConditional => ErrorDomain::Config,
            ShipError::Cancelled => ErrorDomain::Sink,
            ShipError::Crashed(_) => ErrorDomain::Sink,
            ShipError::Internal(_) => ErrorDomain::Sink,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ShipError::Store(e) => store_class(e).2,
            ShipError::Catalog(e) => e.code(),
            ShipError::LocalFile { .. } => codes::SINK_IO,
            ShipError::Collision { .. } => codes::SINK_OBJECT_KEY_COLLISION,
            ShipError::UnsupportedConditional => codes::CONFIG_INCOMPATIBLE,
            ShipError::Cancelled => codes::SINK_UNAVAILABLE,
            ShipError::Crashed(_) => codes::SINK_IO,
            ShipError::Internal(_) => codes::SINK_PANIC,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            ShipError::Store(e) => store_class(e).0,
            ShipError::Catalog(e) => e.retry(),
            ShipError::LocalFile { .. } => Retry::Fatal,
            ShipError::Collision { .. } => Retry::Fatal,
            ShipError::UnsupportedConditional => Retry::Fatal,
            ShipError::Cancelled => Retry::Transient,
            ShipError::Crashed(_) => Retry::Fatal,
            ShipError::Internal(_) => Retry::Fatal,
        }
    }
}

impl From<ShipError> for ThadamError {
    fn from(error: ShipError) -> Self {
        error.to_thadam_error()
    }
}

impl From<object_store::Error> for ShipError {
    fn from(error: object_store::Error) -> Self {
        ShipError::Store(error)
    }
}

impl From<thadam_segments::SegmentError> for ShipError {
    fn from(error: thadam_segments::SegmentError) -> Self {
        ShipError::Catalog(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_error(kind: StoreError) -> ShipError {
        ShipError::Store(kind)
    }

    fn generic(message: &str) -> StoreError {
        StoreError::Generic {
            store: "mem",
            source: message.into(),
        }
    }

    #[test]
    fn auth_failures_are_fatal_and_authenticate_domain() {
        for (variant, domain, code) in [
            (
                StoreError::Unauthenticated {
                    path: "k".into(),
                    source: "bad sig".into(),
                },
                ErrorDomain::Auth,
                codes::AUTH_FAILED,
            ),
            (
                StoreError::PermissionDenied {
                    path: "k".into(),
                    source: "403".into(),
                },
                ErrorDomain::Auth,
                codes::AUTH_FORBIDDEN,
            ),
        ] {
            let e = store_error(variant);
            assert_eq!((e.domain(), e.retry()), (domain, Retry::Fatal));
            assert_eq!(e.code(), code);
        }
    }

    #[test]
    fn generic_store_failures_are_transient() {
        let e = store_error(generic("connection refused"));
        assert_eq!(
            (e.domain(), e.retry()),
            (ErrorDomain::Sink, Retry::Transient)
        );
        assert_eq!(e.code(), codes::SINK_UNAVAILABLE);
    }

    #[test]
    fn collision_is_fatal_with_the_dedicated_code() {
        let e = ShipError::Collision {
            key: "k".into(),
            detail: "sha differs".into(),
        };
        assert_eq!(
            (e.retry(), e.code()),
            (Retry::Fatal, "sink.object_key_collision")
        );
    }
}
