//! `GrpcSourceFactory`: how `kind: grpc` becomes a source.

use thadam_core::{BuildContext, ThadamError, Source, SourceFactory};

use crate::config::{GrpcConfig, config_schema};
use crate::source::GrpcSource;

/// Builds [`GrpcSource`]s of kind `grpc`.
#[derive(Clone, Debug, Default)]
pub struct GrpcSourceFactory;

impl GrpcSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "grpc";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }
}

impl SourceFactory for GrpcSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        GrpcConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = GrpcConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })?;
        Ok(Box::new(GrpcSource::new(config)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use thadam_core::{AckMode, SystemClock, codes};
    use serde_json::json;

    use super::*;

    fn ctx() -> BuildContext {
        BuildContext::new("grpc-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = GrpcSourceFactory::new();
        assert_eq!(factory.kind(), "grpc");
        assert_eq!(factory.config_schema()["title"], "gRPC source");
        assert!(factory.validate(&json!({})).is_ok());
        let errors = factory
            .validate(&json!({ "queue_capacity": 1 }))
            .unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_INVALID);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let cfg = json!({ "queue_capacity": 1, "max_message_bytes": 0 });
        let err = GrpcSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("and 1 more"), "{}", err.message);
    }

    #[test]
    fn sources_ack_on_commit() {
        let source = GrpcSourceFactory::new().build(json!({}), &ctx()).unwrap();
        assert_eq!(source.capabilities().ack_mode, AckMode::OnCommit);
    }
}
