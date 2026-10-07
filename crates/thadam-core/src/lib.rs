//! Canonical in-memory model and contracts shared by every Thadam crate.
//!
//! # Responsibility
//!
//! - The event model: event, value, shape, times, quality, identity.
//! - Errors and their classification.
//! - WAL position types.
//! - Connector traits.
//! - Clock, shutdown, and disk budget primitives.
//!
//! # Deliberately not exposed
//!
//! - Event field mutability beyond invariant-preserving setters.
//! - Interning internals.
//! - Any WAL encoding, Arrow types, protocol types, YAML types, or the Tokio runtime.
//! - `contract_version` and broad capability sets.
