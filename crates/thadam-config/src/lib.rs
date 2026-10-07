//! Pipeline configuration loading and validation.
//!
//! # Responsibility
//!
//! - Load and interpolate YAML into a typed, validated pipeline spec.
//! - Diagnostics with paths and line numbers.
//! - JSON Schema output for editors.
//! - A stable config hash.
//!
//! # Deliberately not exposed
//!
//! - The YAML library and its AST.
//! - Connector-specific config types; connector factories validate those.
//! - Hot reload.
//! - Multiple pipelines per file.
