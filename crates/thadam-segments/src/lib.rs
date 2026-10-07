//! Parquet segment sink, segment catalog, and quarantine writer.
//!
//! # Responsibility
//!
//! - The segment sink implementation for Parquet, with dataset partitioning and rotation.
//! - The staging-to-ready file lifecycle.
//! - The segment catalog: the contract with the shipper.
//! - Local retention and budget accounting.
//! - The quarantine writer.
//!
//! # Deliberately not exposed
//!
//! - Writers, file handles, staging paths, the catalog log format.
//! - Iceberg and Delta.
//! - Arbitrary user-defined partition templates (fixed layout in the MVP).
//! - A narrow (long) layout.
