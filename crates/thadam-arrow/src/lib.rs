//! Encoding of events into Arrow record batches.
//!
//! # Responsibility
//!
//! - Encode events of one dataset into Arrow record batches using a declared dataset schema.
//! - Envelope and lineage columns, type mapping, and widening.
//!
//! # Deliberately not exposed
//!
//! - Schema inference.
//! - Arrow compute kernels.
//! - Arrow-to-event decoding (not needed in the MVP).
