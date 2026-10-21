//! `MqttPublishSinkFactory`: how `kind: mqtt_publish` becomes a batch sink.

use thadam_core::{BatchSink, BatchSinkFactory, BuildContext, ErrorDomain, ThadamError, codes};

use super::config::{MqttPublishConfig, config_schema};
use super::sink::MqttPublishSink;

/// Builds [`MqttPublishSink`]s of kind `mqtt_publish`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MqttPublishSinkFactory;

impl MqttPublishSinkFactory {
    /// The `kind` string sinks of this factory are configured with.
    pub const KIND: &'static str = "mqtt_publish";

    /// A factory instance.
    pub fn new() -> Self {
        MqttPublishSinkFactory
    }
}

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// The first problem, with a count of the rest, for the single-error `build`
/// boundary.
fn first_problem(mut errors: Vec<ThadamError>) -> ThadamError {
    let extra = errors.len().saturating_sub(1);
    let mut first = if errors.is_empty() {
        config_error("config", "invalid mqtt_publish sink config")
    } else {
        errors.swap_remove(0)
    };
    if extra > 0 {
        first
            .message
            .push_str(&format!(" (and {extra} more problem(s))"));
    }
    first
}

impl BatchSinkFactory for MqttPublishSinkFactory {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn config_schema(&self) -> serde_json::Value {
        config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        MqttPublishConfig::from_value(cfg).map(drop)
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn BatchSink>, ThadamError> {
        let config = MqttPublishConfig::from_value(&cfg).map_err(first_problem)?;
        let [decl] = ctx.datasets.as_ref() else {
            let names = ctx
                .datasets
                .iter()
                .map(|d| d.id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(config_error(
                "datasets",
                format!(
                    "a `kind: mqtt_publish` sink publishes exactly one dataset (got {}: [{}]); \
                     give it a `config.datasets: [<id>]` selector listing one dataset and add one sink \
                     entry per dataset",
                    ctx.datasets.len(),
                    names,
                ),
            ));
        };
        let template = config
            .topic_template()
            .map_err(|e| config_error("topic", e.0))?;
        let missing: Vec<&str> = template
            .variables()
            .into_iter()
            .filter(|v| *v != "dataset")
            .filter(|v| !decl.levels.iter().any(|l| &*l.name == *v))
            .collect();
        if !missing.is_empty() {
            return Err(config_error(
                "topic",
                format!(
                    "topic placeholders {:?} are not identity levels of dataset `{}` ({:?})",
                    missing,
                    decl.id,
                    decl.levels
                        .iter()
                        .map(|l| l.name.to_string())
                        .collect::<Vec<_>>(),
                ),
            ));
        }
        Ok(Box::new(MqttPublishSink::new(
            config,
            decl.id.to_string(),
            template,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use serde_json::json;
    use thadam_core::{DatasetDecl, LevelDecl, SystemClock};

    use super::*;

    fn cfg() -> serde_json::Value {
        json!({
            "url": "mqtt://127.0.0.1:1",
            "client_id": "thadam-sink-01",
            "topic": "thadam/{dataset}/{robot}"
        })
    }

    fn dataset(levels: &[&str]) -> DatasetDecl {
        DatasetDecl::new(
            "imu",
            Vec::new(),
            levels
                .iter()
                .enumerate()
                .map(|(i, name)| LevelDecl::new(*name, 100 + i as u32))
                .collect(),
            false,
        )
    }

    fn ctx(datasets: Arc<[DatasetDecl]>) -> BuildContext {
        BuildContext::new(
            "mqtt-out",
            Arc::new(SystemClock::new()),
            PathBuf::from("/tmp/x"),
        )
        .with_datasets(datasets)
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = MqttPublishSinkFactory::new();
        assert_eq!(factory.kind(), "mqtt_publish");
        assert_eq!(factory.config_schema()["title"], "MQTT sink");
        factory.validate(&cfg()).expect("valid");
        let mut bad = cfg();
        bad["qos"] = json!(2);
        assert!(factory.validate(&bad).is_err());
    }

    #[test]
    fn build_is_pure_and_does_not_connect() {
        let factory = MqttPublishSinkFactory::new();
        let sink = factory
            .build(cfg(), &ctx(Arc::from([dataset(&["robot"])])))
            .expect("builds without a broker");
        drop(sink);
    }

    #[test]
    fn build_requires_exactly_one_dataset() {
        let factory = MqttPublishSinkFactory::new();
        let err = factory
            .build(cfg(), &ctx(Arc::from([])))
            .err()
            .expect("no datasets fails");
        assert!(err.message.contains("exactly one dataset"), "{err}");

        let two: Arc<[DatasetDecl]> = Arc::from([dataset(&["robot"]), dataset(&["robot"])]);
        let err = factory
            .build(cfg(), &ctx(two))
            .err()
            .expect("two datasets fails");
        assert!(err.message.contains("exactly one dataset"), "{err}");
    }

    #[test]
    fn build_rejects_unknown_topic_levels() {
        let factory = MqttPublishSinkFactory::new();
        let err = factory
            .build(cfg(), &ctx(Arc::from([dataset(&["site"])])))
            .err()
            .expect("robot is not a level");
        assert!(err.message.contains("not identity levels"), "{err}");
        assert!(err.message.contains("robot"), "{err}");
    }

    #[test]
    fn build_reports_the_first_config_problem() {
        let mut bad = cfg();
        bad["qos"] = json!(2);
        bad["client_id"] = json!("");
        let err = MqttPublishSinkFactory::new()
            .build(bad, &ctx(Arc::from([dataset(&["robot"])])))
            .err()
            .expect("invalid config fails");
        assert_eq!(err.code, codes::CONFIG_INVALID);
        assert!(err.message.contains("more problem(s)"), "{}", err.message);
    }
}
