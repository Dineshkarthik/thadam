//! The runner's error type. Every failure ends the run with a message that
//! names the step that broke, plus the log path when a child process is
//! involved.

use thiserror::Error;

/// Anything that stops a benchmark run or a report.
#[derive(Debug, Error)]
pub enum BenchError {
    /// A scenario file or command-line value is invalid.
    #[error("config: {0}")]
    Config(String),
    /// A required binary or file is missing.
    #[error("missing: {0}")]
    Missing(String),
    /// A child process, Docker, or the admin API failed.
    #[error("run: {0}")]
    Run(String),
    /// Filesystem failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON encode or decode failure.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// The crate-wide result alias.
pub type Result<T> = std::result::Result<T, BenchError>;
