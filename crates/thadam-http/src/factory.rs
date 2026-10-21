//! `HttpSourceFactory`: how `kind: http` becomes a source.

use thadam_core::{BuildContext, Source, SourceFactory, ThadamError};

use crate::config::{HttpConfig, config_schema};
use crate::source::HttpSource;

/// Builds [`HttpSource`]s of kind `http`.
#[derive(Clone, Debug, Default)]
pub struct HttpSourceFactory;

impl HttpSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "http";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }
}

impl SourceFactory for HttpSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        HttpConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = HttpConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })?;
        Ok(Box::new(HttpSource::new(config)))
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
        BuildContext::new("http-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = HttpSourceFactory::new();
        assert_eq!(factory.kind(), "http");
        assert_eq!(factory.config_schema()["title"], "HTTP source");
        assert!(factory.validate(&json!({})).is_ok());
        let errors = factory.validate(&json!({ "mode": "poll" })).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_INVALID);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let cfg = json!({ "mode": "poll", "poll": { "urls": [] }, "queue_capacity": 1 });
        let err = HttpSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("and 1 more"), "{}", err.message);
    }

    #[test]
    fn listen_sources_ack_on_commit_and_poll_sources_on_receive() {
        let listen = HttpSourceFactory::new().build(json!({}), &ctx()).unwrap();
        assert_eq!(listen.capabilities().ack_mode, AckMode::OnCommit);

        let poll = json!({ "mode": "poll", "poll": { "urls": ["http://127.0.0.1:1/x"] } });
        let source = HttpSourceFactory::new().build(poll, &ctx()).unwrap();
        assert_eq!(source.capabilities().ack_mode, AckMode::OnReceive);
    }
}
