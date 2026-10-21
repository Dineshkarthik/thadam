//! `Ros2LiveSourceFactory`: how `kind: ros2_live` becomes a source.

use thadam_core::{BuildContext, Source, SourceFactory, ThadamError};

use crate::config::{Ros2LiveConfig, config_schema};
use crate::source::Ros2LiveSource;

/// Builds [`Ros2LiveSource`]s of kind `ros2_live`.
#[derive(Clone, Debug, Default)]
pub struct Ros2LiveSourceFactory;

impl Ros2LiveSourceFactory {
    /// The `kind` string sources of this factory are configured with.
    pub const KIND: &'static str = "ros2_live";

    /// A factory. It holds no state; every source is built from its config.
    pub fn new() -> Self {
        Self
    }

    fn configured(cfg: serde_json::Value) -> Result<Ros2LiveConfig, ThadamError> {
        Ros2LiveConfig::from_value(&cfg).map_err(|mut errors| {
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

impl SourceFactory for Ros2LiveSourceFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        Ros2LiveConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        let config = Self::configured(cfg)?;
        let source = Ros2LiveSource::new(config).map_err(ThadamError::from)?;
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

    use serde_json::json;
    use thadam_core::{SystemClock, codes};

    use super::*;

    fn ctx() -> BuildContext {
        BuildContext::new("ros2-live-1", Arc::new(SystemClock::new()), PathBuf::new())
    }

    fn valid() -> serde_json::Value {
        json!({
            "endpoint": "tcp/127.0.0.1:7447",
            "msg_dirs": ["/opt/ros/jazzy/share"],
            "topics": [{"name": "/imu", "type": "sensor_msgs/msg/Imu"}],
        })
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = Ros2LiveSourceFactory::new();
        assert_eq!(factory.kind(), "ros2_live");
        assert_eq!(factory.config_schema()["title"], "Live ROS 2 source");
        assert!(factory.validate(&valid()).is_ok());
        let errors = factory.validate(&json!({})).unwrap_err();
        assert_eq!(errors[0].code, codes::CONFIG_MISSING);
    }

    #[test]
    fn build_reports_the_first_problem_and_counts_the_rest() {
        let cfg = json!({"endpoint": "", "msg_dirs": ["/x"], "topics": []});
        let err = Ros2LiveSourceFactory::new()
            .build(cfg, &ctx())
            .err()
            .unwrap();
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("more problem(s)"), "{}", err.message);
    }

    #[test]
    fn build_assembles_schemas_at_build_time() {
        // The msg_dirs do not contain the type's definition, so the build
        // fails with the search error (schemas are assembled at build, not
        // at first batch).
        let dir = tempfile::tempdir().expect("temp dir");
        let cfg = json!({
            "endpoint": "tcp/127.0.0.1:7447",
            "msg_dirs": [dir.path()],
            "topics": [{"name": "/imu", "type": "sensor_msgs/msg/Imu"}],
        });
        let err = Ros2LiveSourceFactory::new()
            .build(cfg, &ctx())
            .err()
            .unwrap();
        assert_eq!(err.domain, thadam_core::ErrorDomain::Config);
        assert!(err.message.contains("Imu.msg"), "{err}");
    }
}
