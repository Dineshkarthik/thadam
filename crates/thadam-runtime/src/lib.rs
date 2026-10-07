//! Pipeline assembly and execution.
//!
//! # Responsibility
//!
//! - The connector registry and build-time validation.
//! - Drivers: source, chain, segment sink, batch sink, and service tasks.
//! - The supervisor, health state machine, and shutdown tree.
//! - Maintenance, replay, and offline inspection.
//! - Observability: metrics registry, tracing setup, admin HTTP.
//!
//! # Deliberately not exposed
//!
//! - Driver internals and channels.
//! - Restart and backoff knobs beyond config.
//! - Hot reload and multiple pipelines.
//! - A dynamic plugin loader.
//! - A gRPC API (post-MVP).
