//! The simulator's error type.

/// Everything that can fail in a simulator run or oracle operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SimError {
    /// A fleet configuration problem (parse, validation, unreadable file).
    #[error("fleet configuration: {0}")]
    Config(String),

    /// The broker URL did not have the required `mqtt://` shape.
    #[error("broker URL: {0}")]
    BrokerUrl(String),

    /// Publishing to the broker failed fatally (client channel closed).
    #[error("mqtt publish: {0}")]
    Publish(String),

    /// An oracle I/O problem.
    #[error("oracle {path}: {source}")]
    Oracle {
        /// The oracle path involved.
        path: std::path::PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// Generic I/O (logs, config files).
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl SimError {
    /// Whether the error is a usage/configuration problem (exit 2) rather
    /// than a runtime failure (exit 1).
    pub fn is_config(&self) -> bool {
        matches!(self, SimError::Config(_) | SimError::BrokerUrl(_))
    }
}
