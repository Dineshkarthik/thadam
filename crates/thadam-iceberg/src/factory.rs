//! The `iceberg` `ServiceFactory`.

use std::sync::Arc;

use thadam_core::{BuildContext, ErrorDomain, Service, ServiceFactory, ThadamError, codes};

use crate::config::parse_iceberg_config;
use crate::registrar::Registrar;
use crate::storage::{S3Io, S3IoFactory, build_s3};

/// Builds `kind: iceberg` services (the Iceberg registrar).
///
/// Configured under `sinks:` (the registrar consumes the named segment
/// sink's catalog, like the `object_store` shipper), started as a service
/// after chains are ready. Registered only in `thadam` builds with the
/// `iceberg` feature.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergRegistrarFactory;

impl IcebergRegistrarFactory {
    /// The factory instance.
    pub fn new() -> Self {
        IcebergRegistrarFactory
    }
}

impl ServiceFactory for IcebergRegistrarFactory {
    fn kind(&self) -> &'static str {
        "iceberg"
    }

    fn config_schema(&self) -> serde_json::Value {
        crate::config::iceberg_config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        parse_iceberg_config(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Service>, ThadamError> {
        let config = parse_iceberg_config(&cfg).map_err(merge_errors)?;
        let store = build_s3(&config.url, &config.store).map_err(ThadamError::from)?;
        let io = Arc::new(S3Io::new(Arc::new(store), config.bucket.as_str()));
        Ok(Box::new(Registrar::new(
            ctx.component_id.as_ref(),
            config,
            S3IoFactory::new(io),
        )))
    }
}

fn merge_errors(errors: Vec<ThadamError>) -> ThadamError {
    let mut message = String::new();
    for (i, e) in errors.iter().enumerate() {
        if i > 0 {
            message.push_str("; ");
        }
        message.push_str(&e.message);
    }
    let mut merged = ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message);
    if let Some(path) = errors.first().and_then(|e| e.context.get("path")) {
        merged = merged.with_context("path", path);
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_validates() {
        let factory = IcebergRegistrarFactory::new();
        assert_eq!(factory.kind(), "iceberg");
        assert!(factory.validate(&serde_json::json!({})).is_err());
        let cfg = serde_json::json!({
            "from": "local",
            "pipeline": "p",
            "catalog_uri": "http://127.0.0.1:8181",
            "url": "s3://warehouse",
            "namespace": "robotics",
            "store": { "access_key_id": "k", "secret_access_key": "s" },
        });
        assert!(factory.validate(&cfg).is_ok());
    }
}
