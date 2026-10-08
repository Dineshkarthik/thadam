//! The crate's error type and its mapping into the shared error model.

use std::fmt;
use std::io;
use std::path::PathBuf;

use thadam_arrow::SchemaError;
use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, codes};

/// Everything that can go wrong in `thadam-segments`.
///
/// The classification follows the error model of the implementation plan
/// (section 5.7): running out of configured segment budget is transient
/// budget pressure, a failed fsync or rename is fatal for the component (the
/// WAL regenerates the segment on restart), and a corrupt or incompatible
/// catalog is fatal.
#[derive(Debug)]
#[non_exhaustive]
pub enum SegmentError {
    /// A filesystem operation failed.
    Io {
        /// What was being attempted, such as `rename` or `fsync`.
        op: &'static str,
        /// The path the operation failed on.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// The segment budget refused a reservation (`budget.limit`) or the disk
    /// reported ENOSPC.
    Budget {
        /// Bytes the operation needed.
        needed: u64,
        /// Bytes in use in the budget category.
        used: u64,
        /// The configured limit, if any.
        limit: Option<u64>,
    },
    /// A Parquet write failed after per-event schema checks passed.
    Encode {
        /// The dataset being written.
        dataset: String,
        /// What the Parquet crate reported.
        message: String,
    },
    /// A dataset schema failed to build from configuration.
    Schema {
        /// The dataset, if the failure is dataset-scoped.
        dataset: Option<String>,
        /// The schema error.
        source: SchemaError,
    },
    /// The catalog log or a Parquet footer was corrupt or written by a newer
    /// format.
    Catalog {
        /// What is wrong.
        message: String,
    },
    /// A sink call arrived in a state that cannot serve it (for example an
    /// append after the sink latched a crash).
    State(&'static str),
    /// A blocking task did not finish (the blocking pool shut down).
    Join(String),
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SegmentError::Io { op, path, source } => {
                write!(f, "{op} {}: {source}", path.display())
            }
            SegmentError::Budget {
                needed,
                used,
                limit,
            } => {
                let limit = limit.map_or_else(|| "none".to_owned(), |l| l.to_string());
                write!(
                    f,
                    "segment budget pressure: {needed} bytes needed, {used} in use, limit {limit}"
                )
            }
            SegmentError::Encode { dataset, message } => {
                write!(f, "encoding {dataset}: {message}")
            }
            SegmentError::Schema { dataset, source } => match dataset {
                Some(d) => write!(f, "schema of dataset {d}: {source}"),
                None => write!(f, "schema: {source}"),
            },
            SegmentError::Catalog { message } => write!(f, "catalog: {message}"),
            SegmentError::State(s) => write!(f, "sink state: {s}"),
            SegmentError::Join(m) => write!(f, "blocking task failed: {m}"),
        }
    }
}

impl std::error::Error for SegmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SegmentError::Io { source, .. } => Some(source),
            SegmentError::Schema { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl SegmentError {
    pub(crate) fn io(op: &'static str, path: impl Into<PathBuf>, source: io::Error) -> Self {
        SegmentError::Io {
            op,
            path: path.into(),
            source,
        }
    }

    pub(crate) fn catalog(message: impl Into<String>) -> Self {
        SegmentError::Catalog {
            message: message.into(),
        }
    }

    pub(crate) fn encode(dataset: &str, message: impl Into<String>) -> Self {
        SegmentError::Encode {
            dataset: dataset.to_owned(),
            message: message.into(),
        }
    }

    pub(crate) fn schema(dataset: String, source: SchemaError) -> Self {
        SegmentError::Schema {
            dataset: Some(dataset),
            source,
        }
    }

    fn is_no_space(&self) -> bool {
        matches!(
            self,
            SegmentError::Io { source, .. } if source.raw_os_error() == Some(rustix::io::Errno::NOSPC.raw_os_error())
        )
    }
}

impl Classify for SegmentError {
    fn domain(&self) -> ErrorDomain {
        match self {
            SegmentError::Budget { .. } => ErrorDomain::Buffer,
            SegmentError::Schema { .. } => ErrorDomain::Schema,
            SegmentError::Catalog { .. } => ErrorDomain::Buffer,
            _ => ErrorDomain::Sink,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            SegmentError::Io { .. } => {
                if self.is_no_space() {
                    codes::BUFFER_IO
                } else {
                    codes::SINK_IO
                }
            }
            SegmentError::Budget { .. } => codes::BUFFER_BUDGET_EXHAUSTED,
            SegmentError::Encode { .. } => codes::SINK_WRITE_FAILED,
            SegmentError::Schema { .. } => codes::SCHEMA_INVALID_SHAPE,
            SegmentError::Catalog { .. } => codes::BUFFER_CORRUPT,
            SegmentError::State(_) => codes::SINK_IO,
            SegmentError::Join(_) => codes::SINK_IO,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            SegmentError::Budget { .. } => Retry::Transient,
            SegmentError::Io { .. } if self.is_no_space() => Retry::Transient,
            SegmentError::Encode { .. } => Retry::Permanent,
            SegmentError::Schema { .. } => Retry::Permanent,
            SegmentError::State(_) => Retry::Fatal,
            _ => Retry::Fatal,
        }
    }
}

impl From<SegmentError> for ThadamError {
    fn from(e: SegmentError) -> Self {
        e.to_thadam_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_pressure_is_a_transient_buffer_error() {
        let e = SegmentError::Budget {
            needed: 10,
            used: 5,
            limit: Some(6),
        };
        assert_eq!(e.retry(), Retry::Transient);
        let r: ThadamError = e.into();
        assert_eq!(r.code, codes::BUFFER_BUDGET_EXHAUSTED);
        assert!(r.is_transient());
    }

    #[test]
    fn enospc_is_transient_budget_pressure() {
        let e = SegmentError::io(
            "write",
            "/tmp/x",
            io::Error::from_raw_os_error(rustix::io::Errno::NOSPC.raw_os_error()),
        );
        assert_eq!(e.retry(), Retry::Transient);
        let r: ThadamError = e.into();
        assert_eq!(r.domain, ErrorDomain::Sink);
    }

    #[test]
    fn catalog_corruption_is_fatal() {
        let r: ThadamError = SegmentError::catalog("bad crc").into();
        assert_eq!(r.retry, Retry::Fatal);
        assert_eq!(r.code, codes::BUFFER_CORRUPT);
    }
}
