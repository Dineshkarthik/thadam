//! Building the real S3-compatible store from configuration.
//!
//! TLS: reqwest is built `rustls-no-provider`; the single crypto provider
//! compiled into the tree is `ring` (ADR-0020), which rustls picks up
//! automatically. No `aws-lc-sys`, no `openssl-sys`.

use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};

use crate::config::{ConditionalPut, ShipConfig};
use crate::error::ShipError;

/// An `AmazonS3` from validated configuration. Does no network I/O: the
/// shipper's startup probe is the first thing that talks to the store.
pub(crate) fn build_s3(config: &ShipConfig) -> Result<AmazonS3, ShipError> {
    let mut builder = AmazonS3Builder::new()
        .with_url(&config.url)
        .with_region(&config.region)
        .with_allow_http(config.allow_http)
        .with_access_key_id(config.credentials.access_key_id())
        .with_secret_access_key(config.credentials.secret_access_key());
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    // `Auto` and `Create` keep the crate-default ETagMatch (If-None-Match);
    // the probe settles what `Auto` resolves to. `Overwrite` opts out
    // explicit.
    if matches!(config.conditional_put, ConditionalPut::Overwrite) {
        builder = builder.with_conditional_put(S3ConditionalPut::Disabled);
    }
    builder.build().map_err(ShipError::Store)
}
