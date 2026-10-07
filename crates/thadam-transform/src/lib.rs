//! Synchronous, pure processing from raw WAL frames to event batches.
//!
//! # Responsibility
//!
//! - The dataset router and topic-template identity extraction.
//! - Decoders (JSON in the MVP) and the shape registry.
//! - Transform steps and the chain compiler.
//! - Per-event errors become rejects.
//!
//! # Deliberately not exposed
//!
//! - A user-defined step trait for external crates (sealed until a plugin ADR).
//! - Windowing and aggregation.
//! - Arrow.
//! - Async.
//! - Any I/O beyond loading the enrichment file at compile time.
