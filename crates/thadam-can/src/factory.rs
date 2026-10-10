//! `CanSourceFactory`: how `kind: can` becomes a source.

use thadam_core::{BuildContext, ThadamError, Source, SourceFactory};

use crate::config::{CanConfig, config_schema};
#[cfg(not(target_os = "linux"))]
use crate::error::CanError;
#[cfg(target_os = "linux")]
use crate::source::CanSource;

/// Builds [`CanSource`]s of kind `can`.
#[derive(Clone, Debug, Default)]
pub struct CanSourceFactory;

impl CanSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "can";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }

    fn configured(cfg: serde_json::Value) -> Result<CanConfig, ThadamError> {
        CanConfig::from_value(&cfg).map_err(|mut errors| {
            let extra = errors.len() - 1;
            let mut first = errors.swap_remove(0);
            if extra > 0 {
                first
                    .message
                    .push_str(&format!(" (and {extra} more problem(s))"));
            }
            first
        })
    }
}

impl SourceFactory for CanSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        CanConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = Self::configured(cfg)?;
        #[cfg(not(target_os = "linux"))]
        {
            let _ = &config;
            let error: ThadamError = CanError::Config(crate::source::NOT_LINUX.into()).into();
            Err(error)
        }
        #[cfg(target_os = "linux")]
        Ok(Box::new(CanSource::new(config).map_err(ThadamError::from)?))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use thadam_core::{SystemClock, codes};
    use serde_json::json;

    use super::*;

    fn ctx() -> BuildContext {
        BuildContext::new("can-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    fn valid() -> serde_json::Value {
        json!({"interface": "vcan0"})
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = CanSourceFactory::new();
        assert_eq!(factory.kind(), "can");
        assert_eq!(factory.config_schema()["title"], "SocketCAN source");
        assert!(factory.validate(&valid()).is_ok());
        let errors = factory.validate(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let cfg = json!({"interface": "", "emit": "decoded"});
        let err = CanSourceFactory::new().build(cfg, &ctx()).err().unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("more problem(s)"), "{}", err.message);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn can_sources_do_not_ack() {
        let source = CanSourceFactory::new().build(valid(), &ctx()).unwrap();
        assert_eq!(source.capabilities().ack_mode, thadam_core::AckMode::None);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn build_on_non_linux_is_a_clear_config_error() {
        let err = CanSourceFactory::new()
            .build(valid(), &ctx())
            .err()
            .unwrap();
        assert_eq!(err.domain, thadam_core::ErrorDomain::Config);
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(
            err.message.contains("SocketCAN requires Linux"),
            "{}",
            err.message
        );
    }
}
