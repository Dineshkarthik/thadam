//! Conformance suites. Each connector crate runs the suite for its kind against
//! its own fixture; the memory connectors must pass their own suites first.

pub mod ack_fifo;
pub mod batch_sink;
pub mod segment_sink;
pub mod source;
