//! Iceberg registrar errors and their classification (ARCHITECTURE.md section
//! 13; design: docs/design/iceberg-append.md).

use iceberg::ErrorKind as IceKind;
use object_store::Error as StoreError;
use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, codes};

/// The named Iceberg table must exist but does not (`create_table: false`).
pub const CODE_TABLE_MISSING: &str = "sink.iceberg_table_missing";
/// A segment's Parquet footer does not map into Iceberg types at all (for
/// example `UInt64` under the `unsigned: native` Arrow policy).
pub const CODE_UNSUPPORTED_TYPE: &str = "sink.iceberg_unsupported_type";
/// The table's current schema is not assignable from a segment's schema.
pub const CODE_SCHEMA_MISMATCH: &str = "sink.iceberg_schema_mismatch";
/// A commit kept conflicting with another writer past the bounded retry.
pub const CODE_COMMIT: &str = "sink.iceberg_commit";

/// One failure the registrar can report.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IcebergError {
    /// An `object_store` operation failed (metadata, manifests, footer
    /// reads); classified like the shipper's store errors.
    #[error("store: {0}")]
    Store(StoreError),
    /// The Iceberg catalog or the `iceberg` crate reported an error.
    /// Classification is derived from its kind and message in
    /// [`classify_iceberg`].
    #[error("iceberg: {0}")]
    Catalog(iceberg::Error),
    /// A segment-catalog operation failed (local catalog log).
    #[error("segments: {0}")]
    Segments(thadam_segments::SegmentError),
    /// The configured table does not exist and `create_table` is false.
    #[error("table {table} does not exist in the catalog (create_table is false)")]
    TableMissing {
        /// `namespace.table`.
        table: String,
    },
    /// A dataset name cannot be used as an Iceberg table name.
    #[error(
        "dataset name `{dataset}` cannot be an Iceberg table name (use [A-Za-z0-9_-]+; dots are excluded)"
    )]
    BadTableName {
        /// The dataset.
        dataset: String,
    },
    /// A segment's footer contains a type Iceberg cannot represent; the
    /// message names the remedy (`unsigned: signed`).
    #[error("segment footer schema cannot become an Iceberg schema: {0}")]
    UnsupportedType(String),
    /// The existing table's schema is not assignable from the segment's
    /// footer schema.
    #[error("table {table}: {detail}")]
    SchemaMismatch {
        /// `namespace.table`.
        table: String,
        /// Which field failed, and how.
        detail: String,
    },
    /// A commit kept conflicting with another writer past the bounded
    /// retry; the next catalog tick tries again (transient).
    #[error("commit to table {table} conflicted with another writer repeatedly (segment {key})")]
    CommitFailed {
        /// `namespace.table`.
        table: String,
        /// The segment object key.
        key: String,
    },
    /// Shutdown happened mid-registration; control flow only, never surfaced.
    #[error("cancelled")]
    Cancelled,
    /// An internal invariant broke (task panic, unparseable state).
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

/// Classifies an `iceberg::Error` for the retry loop:
///
/// - 401/403-flavored messages are Fatal auth (the REST client folds the
///   status into the error text; there is no structured accessor in 0.10.1).
///   shortcut: classify by message scan; upgrade when iceberg exposes the
///   HTTP status or a dedicated auth kind.
/// - `TableNotFound`/`NamespaceNotFound` (existing-table mode),
///   `DataInvalid`/`FeatureUnsupported` (schema and 4xx client errors) are
///   Fatal: retrying cannot heal them.
/// - Everything else — `Unexpected` (network layer, 5xx, malformed
///   responses) and commit conflicts that escaped the bounded retry — is
///   Transient. Conflicts themselves are handled inside the append loop
///   before this runs.
pub(crate) fn classify_iceberg(error: &iceberg::Error) -> (Retry, ErrorDomain, &'static str) {
    let text = error.to_string().to_ascii_lowercase();
    if text.contains("401") || text.contains("403") || text.contains("unauthorized") {
        return (Retry::Fatal, ErrorDomain::Auth, codes::AUTH_FAILED);
    }
    if text.contains("forbidden") || text.contains("permission denied") {
        return (Retry::Fatal, ErrorDomain::Auth, codes::AUTH_FORBIDDEN);
    }
    match error.kind() {
        IceKind::TableNotFound | IceKind::NamespaceNotFound => {
            (Retry::Fatal, ErrorDomain::Config, CODE_TABLE_MISSING)
        }
        IceKind::DataInvalid | IceKind::FeatureUnsupported | IceKind::PreconditionFailed => {
            (Retry::Fatal, ErrorDomain::Sink, CODE_SCHEMA_MISMATCH)
        }
        _ => (Retry::Transient, ErrorDomain::Sink, codes::SINK_UNAVAILABLE),
    }
}

impl Classify for IcebergError {
    fn domain(&self) -> ErrorDomain {
        match self {
            IcebergError::Store(e) => store_class(e).1,
            IcebergError::Catalog(e) => classify_iceberg(e).1,
            IcebergError::Segments(e) => e.domain(),
            IcebergError::TableMissing { .. } | IcebergError::BadTableName { .. } => {
                ErrorDomain::Config
            }
            IcebergError::UnsupportedType(_) => ErrorDomain::Sink,
            IcebergError::SchemaMismatch { .. } => ErrorDomain::Sink,
            IcebergError::CommitFailed { .. } => ErrorDomain::Sink,
            IcebergError::Cancelled => ErrorDomain::Sink,
            IcebergError::Internal(_) => ErrorDomain::Sink,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            IcebergError::Store(e) => store_class(e).2,
            IcebergError::Catalog(e) => classify_iceberg(e).2,
            IcebergError::Segments(e) => e.code(),
            IcebergError::TableMissing { .. } => CODE_TABLE_MISSING,
            IcebergError::BadTableName { .. } => codes::CONFIG_INVALID,
            IcebergError::UnsupportedType(_) => CODE_UNSUPPORTED_TYPE,
            IcebergError::SchemaMismatch { .. } => CODE_SCHEMA_MISMATCH,
            IcebergError::CommitFailed { .. } => CODE_COMMIT,
            IcebergError::Cancelled => codes::SINK_UNAVAILABLE,
            IcebergError::Internal(_) => codes::SINK_PANIC,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            IcebergError::Store(e) => store_class(e).0,
            IcebergError::Catalog(e) => classify_iceberg(e).0,
            IcebergError::Segments(e) => e.retry(),
            IcebergError::TableMissing { .. }
            | IcebergError::BadTableName { .. }
            | IcebergError::UnsupportedType(_)
            | IcebergError::SchemaMismatch { .. }
            | IcebergError::Internal(_) => Retry::Fatal,
            IcebergError::CommitFailed { .. } | IcebergError::Cancelled => Retry::Transient,
        }
    }
}

impl From<IcebergError> for ThadamError {
    fn from(error: IcebergError) -> Self {
        error.to_thadam_error()
    }
}

impl From<object_store::Error> for IcebergError {
    fn from(error: object_store::Error) -> Self {
        IcebergError::Store(error)
    }
}

impl From<thadam_segments::SegmentError> for IcebergError {
    fn from(error: thadam_segments::SegmentError) -> Self {
        IcebergError::Segments(error)
    }
}

#[cfg(test)]
mod tests {
    use iceberg::ErrorKind;

    use super::*;

    #[test]
    fn auth_text_is_fatal() {
        let e = iceberg::Error::new(ErrorKind::Unexpected, "unexpected status: 401 Unauthorized");
        let e = IcebergError::Catalog(e);
        assert_eq!((e.retry(), e.domain()), (Retry::Fatal, ErrorDomain::Auth));

        let e = iceberg::Error::new(ErrorKind::DataInvalid, "403 Forbidden while loading table");
        assert_eq!(classify_iceberg(&e).0, Retry::Fatal);
    }

    #[test]
    fn unexpected_is_transient() {
        let e = iceberg::Error::new(ErrorKind::Unexpected, "connection refused");
        assert_eq!(
            classify_iceberg(&e),
            (Retry::Transient, ErrorDomain::Sink, codes::SINK_UNAVAILABLE)
        );
    }

    #[test]
    fn table_not_found_is_fatal_config() {
        let e = iceberg::Error::new(ErrorKind::TableNotFound, "no such table");
        assert_eq!(classify_iceberg(&e).0, Retry::Fatal);
        assert_eq!(classify_iceberg(&e).2, CODE_TABLE_MISSING);
    }

    #[test]
    fn store_auth_is_fatal() {
        let e = IcebergError::Store(StoreError::PermissionDenied {
            path: "k".into(),
            source: "403".into(),
        });
        assert_eq!((e.retry(), e.code()), (Retry::Fatal, codes::AUTH_FORBIDDEN));
    }
}
