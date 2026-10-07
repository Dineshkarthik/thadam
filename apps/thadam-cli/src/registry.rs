//! The connector kinds this binary knows.
//!
//! Connector factories are registered here once their crates exist; until then
//! `thadam validate` checks only that each configured `kind` is one of these.

/// Registered source kinds.
pub const SOURCE_KINDS: &[&str] = &["mqtt"];

/// Registered sink kinds.
pub const SINK_KINDS: &[&str] = &["parquet", "object_store"];
