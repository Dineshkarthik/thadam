//! Shipping of finalized segments to S3-compatible object storage (PR16;
//! ARCHITECTURE.md section 8 "Shipper", ADR-0011 D18, ADR-0013 key layout).
//!
//! # Responsibility
//!
//! - The [`ObjectStoreShipperFactory`] (`kind: object_store`, a
//!   [`thadam_core::ServiceFactory`]): configured under `sinks:`, started as
//!   a service after chains are ready. Its `from` names a segment sink whose
//!   catalog it consumes.
//! - Oldest-first uploads with bounded concurrency, create-if-absent
//!   (`PutMode::Create`) with HEAD size + `thadam-sha256` verification on
//!   AlreadyExists (equal: success; different: Fatal
//!   `sink.object_key_collision`), multipart above a threshold with abort on
//!   failure.
//! - A startup probe ([`StoreCapabilities`]): conditional-put support,
//!   clock skew, user-metadata round-trip.
//! - Unbounded jittered per-file retry; local retention after `Uploaded`.
//!
//! # Key layout (ADR-0013)
//!
//! `{prefix}/thadam/v1/{pipeline}/{dataset}/date=YYYY-MM-DD/hour=HH/{node}_{epoch:016x}_{first:020}_{last:020}.parquet`
//!
//! (or `.../{dataset}/unsynced/{node}_...` when the clock was not synced).
//! Deterministic keys plus immutable files make every object effectively
//! written once on stores that enforce `If-None-Match`.
//!
//! # Configuration
//!
//! ```yaml
//! sinks:
//!   - id: s3
//!     kind: object_store
//!     config:
//!       from: local
//!       pipeline: edge-capture
//!       url: s3://bucket/prefix
//!       endpoint: http://minio:9000
//!       region: us-east-1
//!       allow_http: true
//!       access_key_id: "${file:/run/secrets/key_id}"
//!       secret_access_key: "${file:/run/secrets/key_secret}"
//!       upload_concurrency: 2
//!       multipart_threshold: 32MiB
//!       part_size: 8MiB
//!       conditional_put: auto
//!       retry: { initial: 1s, max: 60s, jitter: true }
//!       local_retention: { mode: delete_after_upload, keep_for: 30m }
//! ```
//!
//! # Metrics
//!
//! There is no typed channel from a service to the runtime's `MetricsSink`
//! yet (the trait lives in `thadam-runtime`, which connector crates cannot
//! depend on), so progress is reported through [`ShipMetrics`] and
//! structured `tracing` fields named after the planned `thadam_ship_*`
//! metric names. A Prometheus mapping lands when the runtime grows that
//! channel.
//!
//! # Deliberately not exposed
//!
//! - `object_store` types in configuration.
//! - Credentials (only a redacted wrapper).
//! - Custom key templates (fixed in the MVP), GCS and Azure (untested),
//!   bandwidth shaping.

mod backoff;
mod config;
mod error;
mod factory;
mod failpoints;
mod keys;
mod metrics;
mod probe;
mod shipper;
mod store;

pub use config::{
    ConditionalPut, Credentials, RetentionConfig, RetentionMode, RetryConfig, ShipConfig,
    parse_ship_config, ship_config_schema,
};
pub use error::ShipError;
pub use factory::ObjectStoreShipperFactory;
pub use metrics::{NoopShipMetrics, ShipMetrics};
pub use probe::StoreCapabilities;
pub use shipper::Shipper;

#[cfg(any(test, feature = "failpoints"))]
pub use failpoints::{Failpoint, Failpoints};

#[cfg(test)]
mod tests;
