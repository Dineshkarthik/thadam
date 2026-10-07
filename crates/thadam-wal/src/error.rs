//! `WalError` and its mapping into the shared error model.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thadam_core::{BudgetExceeded, Classify, ErrorDomain, Retry, codes};

/// Errors from the WAL.
///
/// Cloneable so one failure can resolve many commit tickets.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WalError {
    /// A filesystem operation failed.
    #[error("wal {op} failed on {}: {source}", path.display())]
    Io {
        /// What was being done.
        op: &'static str,
        /// The file or directory involved.
        path: PathBuf,
        /// The OS error.
        source: Arc<io::Error>,
    },
    /// An `fsync` (or a write that precedes it) failed. Nothing written since
    /// the last successful `fsync` is acknowledged; the WAL must be reopened,
    /// which re-verifies it.
    #[error(
        "wal {op} failed; the writer stopped acknowledging and the WAL must be reopened: {source}"
    )]
    FsyncFailed {
        /// `fsync` or `write`.
        op: &'static str,
        /// The OS error.
        source: Arc<io::Error>,
    },
    /// The writer stopped after an earlier failure.
    #[error("wal writer stopped after an earlier failure: {0}")]
    Poisoned(Arc<WalError>),
    /// On-disk data failed validation.
    #[error("wal data is corrupt: {0}")]
    Corrupt(String),
    /// On-disk data has a format, epoch, or binding this binary refuses.
    #[error("incompatible wal data: {0}")]
    Incompatible(String),
    /// Another process or handle holds the directory lock.
    #[error("wal directory {} is locked by another process or handle", path.display())]
    Locked {
        /// The lock file.
        path: PathBuf,
    },
    /// The disk budget is exhausted and the `on_full` policy cannot free space.
    #[error(transparent)]
    BudgetExhausted(#[from] BudgetExceeded),
    /// A source batch encodes to more than `frame_max_bytes`.
    #[error("batch encodes to {size} bytes, more than frame_max_bytes ({max})")]
    FrameTooLarge {
        /// Encoded payload size.
        size: usize,
        /// The limit.
        max: usize,
    },
    /// A caller passed an argument the WAL refuses, such as a cursor beyond
    /// the durable end or an invalid cursor name.
    #[error("{0}")]
    InvalidArgument(String),
    /// The options are inconsistent.
    #[error("invalid wal options: {0}")]
    InvalidOptions(String),
    /// The writer has shut down.
    #[error("the wal writer is closed")]
    Closed,
}

impl WalError {
    pub(crate) fn io(op: &'static str, path: &Path, source: io::Error) -> Self {
        WalError::Io {
            op,
            path: path.to_owned(),
            source: Arc::new(source),
        }
    }

    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        WalError::InvalidArgument(msg.into())
    }

    fn root(&self) -> &WalError {
        match self {
            WalError::Poisoned(cause) => cause.root(),
            other => other,
        }
    }
}

impl Classify for WalError {
    fn domain(&self) -> ErrorDomain {
        match self.root() {
            WalError::FrameTooLarge { .. } => ErrorDomain::Connector,
            WalError::InvalidArgument(_) | WalError::InvalidOptions(_) => ErrorDomain::Config,
            _ => ErrorDomain::Buffer,
        }
    }

    fn code(&self) -> &'static str {
        match self.root() {
            WalError::Io { .. } | WalError::Closed => codes::BUFFER_IO,
            WalError::FsyncFailed { .. } => codes::BUFFER_FSYNC_FAILED,
            WalError::Corrupt(_) => codes::BUFFER_CORRUPT,
            WalError::Incompatible(_) | WalError::Locked { .. } => codes::BUFFER_INCOMPATIBLE,
            WalError::BudgetExhausted(_) => codes::BUFFER_BUDGET_EXHAUSTED,
            WalError::FrameTooLarge { .. } => codes::CONNECTOR_MESSAGE_TOO_LARGE,
            WalError::InvalidArgument(_) | WalError::InvalidOptions(_) => codes::CONFIG_INVALID,
            WalError::Poisoned(_) => codes::BUFFER_FSYNC_FAILED,
        }
    }

    fn retry(&self) -> Retry {
        match self.root() {
            WalError::Io { source, .. } => io_retry(source),
            WalError::BudgetExhausted(_) => Retry::Transient,
            WalError::Corrupt(_)
            | WalError::FrameTooLarge { .. }
            | WalError::InvalidArgument(_)
            | WalError::Closed => Retry::Permanent,
            WalError::FsyncFailed { .. }
            | WalError::Incompatible(_)
            | WalError::Locked { .. }
            | WalError::InvalidOptions(_)
            | WalError::Poisoned(_) => Retry::Fatal,
        }
    }
}

fn io_retry(e: &io::Error) -> Retry {
    use io::ErrorKind::*;
    match e.kind() {
        Interrupted | WouldBlock | TimedOut | StorageFull | QuotaExceeded => Retry::Transient,
        _ => Retry::Fatal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::BudgetCategory;

    #[test]
    fn classification() {
        let fsync = WalError::FsyncFailed {
            op: "fsync",
            source: Arc::new(io::Error::other("eio")),
        };
        assert_eq!(fsync.code(), codes::BUFFER_FSYNC_FAILED);
        assert_eq!(fsync.retry(), Retry::Fatal);
        let poisoned = WalError::Poisoned(Arc::new(fsync));
        assert_eq!(poisoned.code(), codes::BUFFER_FSYNC_FAILED);

        let full = WalError::io(
            "write",
            Path::new("/x"),
            io::Error::from(io::ErrorKind::StorageFull),
        );
        assert_eq!(
            (full.code(), full.retry()),
            (codes::BUFFER_IO, Retry::Transient)
        );

        let budget = WalError::from(BudgetExceeded {
            category: BudgetCategory::Wal,
            requested: 1,
            used: 1,
            limit: 1,
        });
        assert_eq!(budget.code(), codes::BUFFER_BUDGET_EXHAUSTED);
        assert_eq!(budget.domain(), ErrorDomain::Buffer);
        assert_eq!(WalError::Corrupt("x".into()).retry(), Retry::Permanent);
        assert_eq!(
            WalError::Incompatible("x".into()).to_thadam_error().code,
            codes::BUFFER_INCOMPATIBLE
        );
        let big = WalError::FrameTooLarge { size: 2, max: 1 };
        assert_eq!(big.domain(), ErrorDomain::Connector);
    }
}
