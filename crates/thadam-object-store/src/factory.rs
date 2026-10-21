//! The `object_store` `ServiceFactory`.

use std::sync::Arc;

use thadam_core::{BuildContext, ErrorDomain, Service, ServiceFactory, ThadamError, codes};

use crate::config::parse_ship_config;
use crate::shipper::Shipper;
use crate::store::build_s3;

/// Builds `kind: object_store` services (the segment shipper).
///
/// Configured under `sinks:` (the shipper consumes the named segment sink's
/// catalog), started as a service after chains are ready.
#[derive(Clone, Copy, Debug, Default)]
pub struct ObjectStoreShipperFactory;

impl ObjectStoreShipperFactory {
    /// The factory instance.
    pub fn new() -> Self {
        ObjectStoreShipperFactory
    }
}

impl ServiceFactory for ObjectStoreShipperFactory {
    fn kind(&self) -> &'static str {
        "object_store"
    }

    fn config_schema(&self) -> serde_json::Value {
        crate::config::ship_config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        parse_ship_config(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Service>, ThadamError> {
        let config = parse_ship_config(&cfg).map_err(merge_errors)?;
        let store = build_s3(&config).map_err(ThadamError::from)?;
        Ok(Box::new(Shipper::new(
            ctx.component_id.as_ref(),
            config,
            Arc::new(store),
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
