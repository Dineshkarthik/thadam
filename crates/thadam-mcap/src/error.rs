//! The connector's error type and its mapping into the shared error model.
//!
//! Messages name the file involved but never carry message payload bytes.

use std::path::PathBuf;

use thadam_core::{Classify, ErrorDomain, Retry, ThadamError, Severity, codes};

/// Why an MCAP source operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum McapError {
    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// A file or directory could not be opened or read.
    #[error("cannot open {path}: {detail}")]
    Open {
        /// The path that failed.
        path: PathBuf,
        /// The underlying reason.
        detail: String,
    },
    /// A file is not a well-formed MCAP stream (bad magic, a truncated or
    /// corrupt record, a failed chunk CRC, or an unsupported compression).
    #[error("{path}: {detail}")]
    Parse {
        /// The file that failed to parse.
        path: PathBuf,
        /// What the parser saw; never payload bytes.
        detail: String,
    },
    /// The source was used in the wrong state.
    #[error("{0}")]
    State(&'static str),
}

impl Classify for McapError {
    fn domain(&self) -> ErrorDomain {
        match self {
            McapError::Config(_) => ErrorDomain::Config,
            _ => ErrorDomain::Connector,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            McapError::Config(_) => codes::CONFIG_INVALID,
            McapError::Open { .. } => codes::CONNECTOR_CONNECT_FAILED,
            McapError::Parse { .. } | McapError::State(_) => codes::CONNECTOR_PROTOCOL,
        }
    }

    fn retry(&self) -> Retry {
        match self {
            McapError::Config(_) | McapError::State(_) => Retry::Fatal,
            // A missing file may appear once the recording is copied into place.
            McapError::Open { .. } => Retry::Transient,
            // Retrying never fixes a corrupt file; a restart re-reads it identically.
            McapError::Parse { .. } => Retry::Permanent,
        }
    }

    fn severity(&self) -> Severity {
        Severity::for_retry(self.retry())
    }
}

impl From<McapError> for ThadamError {
    fn from(e: McapError) -> Self {
        e.to_thadam_error()
    }
}

impl McapError {
    /// A parse failure in `path`, stringifying the MCAP library error.
    pub(crate) fn parse(path: &std::path::Path, error: mcap::McapError) -> Self {
        McapError::Parse {
            path: path.to_path_buf(),
            detail: error.to_string(),
        }
    }

    /// An I/O failure in `path`.
    pub(crate) fn open(path: &std::path::Path, error: std::io::Error) -> Self {
        McapError::Open {
            path: path.to_path_buf(),
            detail: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_are_fatal_config_errors() {
        let e: ThadamError = McapError::Config("bad".into()).into();
        assert_eq!(e.domain, ErrorDomain::Config);
        assert_eq!(e.code, codes::CONFIG_INVALID);
        assert_eq!(e.retry, Retry::Fatal);
    }

    #[test]
    fn missing_files_are_transient_and_corrupt_ones_permanent() {
        let e: ThadamError = McapError::Open {
            path: PathBuf::from("/tmp/missing.mcap"),
            detail: "No such file or directory".into(),
        }
        .into();
        assert_eq!(e.code, codes::CONNECTOR_CONNECT_FAILED);
        assert!(e.is_transient());

        let e: ThadamError = McapError::Parse {
            path: PathBuf::from("/tmp/bad.mcap"),
            detail: "Bad magic number".into(),
        }
        .into();
        assert_eq!(e.code, codes::CONNECTOR_PROTOCOL);
        assert_eq!(e.retry, Retry::Permanent);
        assert!(format!("{e}").contains("bad.mcap"));
    }
}
