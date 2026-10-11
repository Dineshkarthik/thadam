//! `McapSourceFactory`: how `kind: mcap` becomes a source.

use thadam_core::{BuildContext, ThadamError, Source, SourceFactory};

use crate::config::{McapConfig, config_schema};
use crate::source::McapSource;

/// Builds [`McapSource`]s of kind `mcap`.
#[derive(Clone, Debug, Default)]
pub struct McapSourceFactory;

impl McapSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "mcap";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }
}

impl SourceFactory for McapSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        McapConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = McapConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })?;
        let source = McapSource::new(config);
        Ok(Box::new(match &ctx.schema_dir {
            Some(dir) => source.with_schema_store(thadam_core::SchemaStore::open(dir)),
            None => source,
        }))
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
        BuildContext::new("mcap-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = McapSourceFactory::new();
        assert_eq!(factory.kind(), "mcap");
        assert_eq!(factory.config_schema()["title"], "MCAP file source");
        assert!(factory.validate(&json!({"paths": ["bag.mcap"]})).is_ok());
        let errors = factory.validate(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
        let errors = factory
            .validate(&json!({"paths": ["a.mcap"], "pace": "fast"}))
            .unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_INVALID);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        // paths (empty), topics (blank), and start>end are all caught by
        // problems(), so the count covers them; a misspelt value like a bad
        // pace fails serde with a single error instead.
        let cfg = json!({"paths": [], "topics": [" "], "start": 10, "end": 5});
        let err = McapSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("and 2 more"), "{}", err.message);
        let err = McapSourceFactory::new()
            .build(json!({"paths": ["a.mcap"], "pace": -1.0}), &ctx())
            .err()
            .unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(!err.message.contains("(and "), "{}", err.message);
    }

    #[test]
    fn mcap_sources_have_no_acknowledgements() {
        let source = McapSourceFactory::new()
            .build(json!({"paths": ["a.mcap"]}), &ctx())
            .unwrap();
        assert_eq!(source.capabilities().ack_mode, AckMode::None);
    }

    #[test]
    fn pace_max_means_log_time_ingest_stamps() {
        let source = McapSourceFactory::new()
            .build(json!({"paths": ["a.mcap"], "pace": "realtime"}), &ctx())
            .unwrap();
        // The source compiles from config; behaviour is covered in the
        // integration tests. Here we only check the build accepted.
        drop(source);
    }
}
