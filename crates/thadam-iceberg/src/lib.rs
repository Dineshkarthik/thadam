//! Iceberg append (`kind: iceberg`, P2 experimental; design:
//! `docs/design/iceberg-append.md`; phase-4 plan: "Iceberg append via the
//! `iceberg` crate `fast_append` — Parquet field IDs already written").
//!
//! # Responsibility
//!
//! - The [`IcebergRegistrarFactory`] (`kind: iceberg`, a
//!   [`thadam_core::ServiceFactory`]): configured under `sinks:`, runs as a
//!   service after chains are ready. Its `from` names the segment sink
//!   whose catalog it consumes — normally the same sink the `object_store`
//!   shipper ships.
//! - Every uploaded segment (fsynced `Uploaded` catalog record, object
//!   verified by the shipper) is registered with its dataset's Iceberg
//!   table via `fast_append`: the already-shipped Parquet files become
//!   table data files. No Parquet rewrite — the field IDs ADR-0013 put in
//!   place are what Iceberg needs.
//! - Idempotent across crashes and retries (committed keys ride in the
//!   snapshot summaries; see [`crate::table`]).
//! - The table schema evolves ahead of each append, driven by Parquet field
//!   IDs: new columns are added optional, a new name on a known ID renames,
//!   int/float/decimal widen; anything else is a fatal schema mismatch (see
//!   `schema` and `rest`, and the design doc for the crash ordering).
//!
//! # Configuration
//!
//! ```yaml
//! sinks:
//!   - id: iceberg
//!     kind: iceberg
//!     config:
//!       from: local
//!       pipeline: edge-capture
//!       catalog_uri: https://iceberg-rest:8181
//!       catalog_ca_file: /etc/thadam/catalog-ca.pem   # optional; replaces platform roots
//!       catalog_token: "${file:/run/secrets/catalog_token}"  # or catalog_credential
//!       catalog_client_cert: /etc/thadam/catalog-client.pem   # optional mutual TLS, with
//!       catalog_client_key: /etc/thadam/catalog-client-key.pem  # catalog_client_key
//!       url: s3://warehouse/lake
//!       namespace: robotics
//!       create_table: false
//!       store:
//!         endpoint: http://seaweedfs:9000
//!         region: us-east-1
//!         allow_http: true
//!         access_key_id: "${file:/run/secrets/key_id}"
//!         secret_access_key: "${file:/run/secrets/key_secret}"
//!       retry: { initial: 1s, max: 60s, jitter: true }
//! ```
//!
//! Datasets registered with Iceberg need the `unsigned: signed` Arrow
//! policy (Iceberg has no unsigned types; the default `Native` envelope has
//! `UInt64` lineage columns the conversion rejects).
//!
//! # Dependency surface (audit, docs/design/iceberg-append.md)
//!
//! `iceberg` 0.10.1 + `iceberg-catalog-rest` 0.10.1, pinned exactly. The
//! `iceberg` crate builds arrow/parquet 58 (the workspace builds 60 — a
//! measured duplicate tree; no arrow types cross the boundary). Storage for
//! Iceberg file IO is implemented here over the workspace's audited
//! `object_store` S3 stack: the `iceberg-storage-opendal` alternative pulls
//! `aws-lc-sys`, which `deny.toml` bans. TLS is rustls + ring only; the
//! `iceberg` feature is default-off in `thadam-cli`.

mod config;
mod error;
mod factory;
mod registrar;
mod rest;
mod schema;
pub mod storage;
mod table;

pub use config::{
    CatalogAuth, CatalogCredential, Credentials, IcebergConfig, RetryConfig, Secret, StoreConfig,
    iceberg_config_schema, parse_iceberg_config,
};
pub use error::IcebergError;
pub use factory::IcebergRegistrarFactory;
pub use registrar::Registrar;
pub use storage::{S3Io, S3IoFactory, build_s3};
