//! gRPC source connector.
//!
//! A tonic server implements the `thadam.ingest.v1.Ingest` service
//! (`proto/thadam/ingest/v1/ingest.proto`). Each request's payload becomes one
//! message. A response is sent only after the message is durably committed, so
//! an OK status means the message is durable.

mod config;
#[cfg(test)]
mod conformance;
mod error;
mod factory;
pub mod proto;
mod queue;
mod server;
mod source;
mod tls;

pub use config::{GrpcConfig, TlsConfig, config_schema};
pub use error::GrpcError;
pub use factory::GrpcSourceFactory;
pub use source::GrpcSource;
