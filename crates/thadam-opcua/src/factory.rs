//! `OpcUaSourceFactory`: how `kind: opcua` becomes a source.

use thadam_core::{BuildContext, ThadamError, Source, SourceFactory};

use crate::config::{OpcUaConfig, config_schema};
use crate::source::OpcUaSource;

/// Builds [`OpcUaSource`]s of kind `opcua`.
#[derive(Clone, Debug, Default)]
pub struct OpcUaSourceFactory;

impl OpcUaSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "opcua";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }
}

impl SourceFactory for OpcUaSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        OpcUaConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = OpcUaConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })?;
        Ok(Box::new(OpcUaSource::new(config)))
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
        BuildContext::new("opcua-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    fn valid() -> serde_json::Value {
        json!({
            "endpoint": "opc.tcp://192.0.2.10:4840",
            "security_policy": "none",
            "security_mode": "none",
            "nodes": [{"node_id": "ns=2;s=Temp", "name": "temp"}]
        })
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = OpcUaSourceFactory::new();
        assert_eq!(factory.kind(), "opcua");
        assert_eq!(factory.config_schema()["title"], "OPC UA source");
        assert!(factory.validate(&valid()).is_ok());
        let errors = factory.validate(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let cfg = json!({
            "endpoint": "not-a-url",
            "security_policy": "none",
            "security_mode": "none",
            "nodes": []
        });
        let err = OpcUaSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("more problem(s)"), "{}", err.message);
    }

    #[test]
    fn opcua_sources_do_not_ack() {
        let source = OpcUaSourceFactory::new().build(valid(), &ctx()).unwrap();
        assert_eq!(source.capabilities().ack_mode, AckMode::None);
    }
}
