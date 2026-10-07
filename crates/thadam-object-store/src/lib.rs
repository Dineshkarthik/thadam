//! Shipping of finalized segments to S3-compatible object storage.
//!
//! # Responsibility
//!
//! - Ship finalized, eligible segments with deterministic keys.
//! - Create-if-absent uploads with HEAD verification; multipart above a threshold.
//! - Retry with backoff.
//! - Probe store capabilities and clock skew at startup.
//! - Drive local retention after upload.
//!
//! # Deliberately not exposed
//!
//! - `object_store` types in config.
//! - Credentials (only redacting secret types).
//! - Custom key templates (fixed in the MVP).
//! - GCS and Azure (untested, so disabled).
//! - Bandwidth shaping.
