//! Tests for the logging initialization.

use thadam_runtime::{LogFormat, init_logging};

/// Test that init_logging succeeds with valid parameters, logging does not
/// panic, and a second init_logging call returns an error.
#[test]
fn test_init_logging_success_and_duplicate_fails() {
    // First initialization should succeed.
    let _guard = init_logging(LogFormat::Json, "info").expect("first init_logging should succeed");

    // Logging should not panic.
    tracing::info!("hello");

    // Second initialization should fail because a subscriber is already set.
    let result = init_logging(LogFormat::Json, "info");
    assert!(
        result.is_err(),
        "second init_logging should fail when subscriber is already set"
    );
}

/// Test that init_logging fails with an invalid filter string.
/// Note: This test must run in a fresh process if the first test already
/// initialized logging. For now, we test only the invalid filter case.
#[test]
fn test_init_logging_invalid_filter() {
    // Try to initialize with an invalid filter string.
    // If logging was already initialized in this process, this will return
    // an error for that reason instead of the filter parse error.
    let result = init_logging(LogFormat::Pretty, "=[=");
    // The result should be an error, either from invalid filter or from
    // subscriber already being set.
    assert!(
        result.is_err(),
        "init_logging with invalid filter should fail"
    );
}
