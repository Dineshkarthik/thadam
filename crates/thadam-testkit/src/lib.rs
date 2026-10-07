//! Shared test infrastructure. Dev-dependency only.
//!
//! # Responsibility
//!
//! - Memory connectors: source, batch sink, and segment sink.
//! - A fake clock and temporary data directories.
//! - Failpoint helpers.
//! - Conformance suites for sources and sinks.
//! - Container helpers for integration tests.
//! - The crash harness.
//!
//! # Deliberately not exposed
//!
//! - A production dependency path: no non-test crate may depend on this crate.
