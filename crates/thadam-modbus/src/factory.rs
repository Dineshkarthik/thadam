//! `ModbusSourceFactory`: how `kind: modbus` becomes a source.

use thadam_core::{BuildContext, Source, SourceFactory, ThadamError};

use crate::config::{ModbusConfig, config_schema};
use crate::source::ModbusSource;

/// Builds [`ModbusSource`]s of kind `modbus`.
#[derive(Clone, Debug, Default)]
pub struct ModbusSourceFactory;

impl ModbusSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "modbus";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }
}

impl SourceFactory for ModbusSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        ModbusConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = ModbusConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })?;
        Ok(Box::new(ModbusSource::new(config)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use serde_json::json;
    use thadam_core::{AckMode, SystemClock, codes};

    use super::*;

    fn ctx() -> BuildContext {
        BuildContext::new("modbus-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    fn valid() -> serde_json::Value {
        json!({
            "address": "127.0.0.1:502",
            "reads": [{"name": "t", "kind": "holding", "start": 0, "count": 2, "data_type": "f32"}]
        })
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = ModbusSourceFactory::new();
        assert_eq!(factory.kind(), "modbus");
        assert_eq!(factory.config_schema()["title"], "Modbus source");
        assert!(factory.validate(&valid()).is_ok());
        let errors = factory.validate(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let cfg = json!({"address": "nope", "reads": []});
        let err = ModbusSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("more problem(s)"), "{}", err.message);
    }

    #[test]
    fn modbus_sources_do_not_ack() {
        let source = ModbusSourceFactory::new().build(valid(), &ctx()).unwrap();
        assert_eq!(source.capabilities().ack_mode, AckMode::None);
    }
}
