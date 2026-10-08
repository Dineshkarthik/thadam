//! Structured logging setup for the runtime.
//!
//! This module provides initialization of the tracing subscriber with support
//! for both JSON and pretty-printed output. The writer is configured as lossy,
//! meaning it will drop log lines rather than block the pipeline when the
//! buffer is full.

use thadam_core::{ErrorDomain, ThadamError, codes};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{EnvFilter, fmt};

/// The output format for log messages.
///
/// Controls whether logs are emitted as JSON or in a human-readable pretty
/// format. The writer is lossy: when the buffer is full, log lines are dropped
/// rather than blocking the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// JSON-formatted output, suitable for machine parsing.
    Json,
    /// Human-readable, pretty-printed output with ANSI colors.
    Pretty,
}

/// A guard that holds the worker for the non-blocking writer.
///
/// When this guard is dropped, the worker is flushed and any remaining log
/// messages are written. The writer is lossy: when the buffer is full, log
/// lines are dropped rather than blocking the pipeline.
pub struct LoggingGuard {
    _worker: WorkerGuard,
}

/// Initializes the global tracing subscriber with the specified format and
/// filter.
///
/// # Arguments
///
/// * `format` - The output format (JSON or pretty-printed).
/// * `filter` - An env-filter string controlling which log levels and targets
///   are emitted (e.g., `"info"`, `"thadam_runtime=debug,warn"`).
///
/// # Returns
///
/// Returns a [`LoggingGuard`] on success. Dropping the guard flushes the
/// non-blocking writer. Returns a `ThadamError` if:
/// - The filter string cannot be parsed.
/// - A global subscriber is already set.
///
/// # Notes
///
/// The writer is lossy: when the buffer is full, log lines are dropped rather
/// than blocking the pipeline. This prevents logging from becoming a
/// bottleneck under high load.
pub fn init_logging(format: LogFormat, filter: &str) -> Result<LoggingGuard, ThadamError> {
    // Build the non-blocking writer with lossy behavior.
    let (non_blocking, worker) = NonBlockingBuilder::default()
        .lossy(true)
        .finish(std::io::stderr());

    // Parse the filter string.
    let env_filter = EnvFilter::try_new(filter).map_err(|e| {
        ThadamError::permanent(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            format!("invalid filter: {e}"),
        )
    })?;

    // Build the subscriber with the appropriate formatter.
    let subscriber = fmt().with_env_filter(env_filter).with_writer(non_blocking);

    // Apply format-specific settings and finish building.
    // Use a trait object to handle both Json and Pretty formats.
    let subscriber: Box<dyn tracing::Subscriber + Send + Sync> = match format {
        LogFormat::Json => Box::new(subscriber.json().finish()),
        LogFormat::Pretty => Box::new(subscriber.pretty().finish()),
    };

    // Try to install the subscriber.
    tracing::subscriber::set_global_default(subscriber).map_err(|e| {
        ThadamError::permanent(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            format!("subscriber already set: {e}"),
        )
    })?;

    Ok(LoggingGuard { _worker: worker })
}
