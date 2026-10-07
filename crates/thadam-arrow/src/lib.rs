//! Encoding of events into Arrow record batches.
//!
//! # Responsibility
//!
//! - Encode events of one dataset into Arrow record batches using a declared
//!   [`DatasetSchema`].
//! - Envelope and lineage columns, type mapping, and widening.
//! - Stable Parquet field IDs on every column, nested ones included
//!   ([`columns`] documents the policy, open item O5).
//!
//! # Use
//!
//! ```
//! use std::sync::Arc;
//! use thadam_arrow::{ArrowOptions, BatchEncoder, DatasetSchema};
//! use thadam_core::{FieldDef, FieldType};
//!
//! let schema = DatasetSchema::builder("telemetry")
//!     .level("robot", 100)
//!     .field(FieldDef::new("soc_pct", FieldType::F32).with_field_id(1000))
//!     .options(ArrowOptions::default())
//!     .build()
//!     .unwrap();
//! let mut encoder = BatchEncoder::new(Arc::new(schema), 1024);
//! // encoder.push(&event)?; ... encoder.finish() -> RecordBatch
//! assert!(encoder.finish().num_rows() == 0);
//! ```
//!
//! # Deliberately not exposed
//!
//! - Schema inference (development mode lives in `thadam-segments`).
//! - Arrow compute kernels.
//! - Arrow-to-event decoding (not needed in the MVP).
//!
//! # Not yet written
//!
//! `_wal_record_idx` (open item O4) is not a column; field ID
//! [`columns::RESERVED_WAL_RECORD_IDX`] is reserved for it.

mod column;
pub mod columns;
mod encoder;
mod error;
mod fingerprint;
mod options;
mod schema;

pub use encoder::{BatchContext, BatchEncoder};
pub use error::SchemaError;
pub use options::{ArrowOptions, TsUnit, UnsignedPolicy};
pub use schema::{DatasetSchema, DatasetSchemaBuilder, IdentityLevel, field_id};
