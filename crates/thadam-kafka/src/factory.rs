//! The `kafka` [`BatchSinkFactory`].

use thadam_core::{BatchSink, BatchSinkFactory, BuildContext, ErrorDomain, ThadamError, codes};

use crate::config::{self, KafkaConfig};
use crate::sink::KafkaSink;

/// Builds [`KafkaSink`]s. Registered in the CLI registry under `kind: kafka`.
#[derive(Clone, Copy, Debug, Default)]
pub struct KafkaSinkFactory;

impl KafkaSinkFactory {
    /// A factory instance.
    pub fn new() -> Self {
        KafkaSinkFactory
    }
}

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// The first error of a list, for the single-error `build` boundary (the
/// pattern `thadam-segments` established).
fn first_error(mut errors: Vec<ThadamError>) -> ThadamError {
    errors.drain(..).next().unwrap_or_else(|| {
        ThadamError::permanent(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            "invalid kafka sink config",
        )
    })
}

impl BatchSinkFactory for KafkaSinkFactory {
    fn kind(&self) -> &'static str {
        "kafka"
    }

    fn config_schema(&self) -> serde_json::Value {
        config::config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        config::parse_config(cfg).map(|_| ())
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn BatchSink>, ThadamError> {
        let kafka_cfg: KafkaConfig = config::parse_config(&cfg).map_err(first_error)?;
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
                    "a `kind: kafka` sink publishes exactly one dataset (got {}: [{}]); \
                     give it a `datasets: [<id>]` selector listing one dataset and add one sink \
                     entry per dataset. This restriction lifts when the batch contract groups \
                     events per dataset (ADR-0002 open work)",
                    ctx.datasets.len(),
                    names,
                ),
            ));
        };
        // Every placeholder that is not `dataset` (topic and key) or
        // `event_id` (key only) must name a declared identity level, so a
        // typo fails at build instead of rejecting every event.
        let key_variables: Vec<&str> = kafka_cfg
            .key
            .as_ref()
            .map(|k| k.variables())
            .unwrap_or_default()
            .into_iter()
            .filter(|v| !matches!(*v, "dataset" | "event_id"))
            .collect();
        let mut unknown: Vec<(&str, &str)> = kafka_cfg
            .topic
            .variables()
            .into_iter()
            .filter(|v| *v != "dataset")
            .filter(|v| !decl.levels.iter().any(|l| &*l.name == *v))
            .map(|v| ("topic", v))
            .collect();
        unknown.extend(
            key_variables
                .into_iter()
                .filter(|v| !decl.levels.iter().any(|l| &*l.name == *v))
                .map(|v| ("key", v)),
        );
        if !unknown.is_empty() {
            return Err(config_error(
                "topic",
                format!(
                    "placeholders {:?} are not identity levels of dataset `{}` ({:?})",
                    unknown,
                    decl.id,
                    decl.levels
                        .iter()
                        .map(|l| l.name.to_string())
                        .collect::<Vec<_>>(),
                ),
            ));
        }
        Ok(Box::new(KafkaSink::new(
            kafka_cfg,
            decl.id.to_string(),
            ctx.component_id.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::{DatasetDecl, LevelDecl, SystemClock};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn cfg() -> serde_json::Value {
        serde_json::json!({
            "bootstrap": ["kafka.internal:9092"],
            "topic": "telemetry.{dataset}.{robot}",
            "key": "{robot}-{event_id}"
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
            "kafka-out",
            Arc::new(SystemClock::new()),
            PathBuf::from("/tmp/x"),
        )
        .with_datasets(datasets)
    }

    #[test]
    fn kind_and_schema_anchor() {
        let factory = KafkaSinkFactory::new();
        assert_eq!(factory.kind(), "kafka");
        let schema = factory.config_schema();
        assert!(schema["properties"]["topic"].is_object());
        assert!(schema["properties"]["bootstrap"].is_object());
        factory.validate(&cfg()).expect("valid");
        let mut bad = cfg();
        bad["bootstrap"] = serde_json::json!([]);
        assert!(factory.validate(&bad).is_err());
    }

    #[test]
    fn build_requires_exactly_one_dataset() {
        let factory = KafkaSinkFactory::new();
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

        let sink = factory
            .build(cfg(), &ctx(Arc::from([dataset(&["robot"])])))
            .expect("one dataset");
        drop(sink);
    }

    #[test]
    fn build_rejects_unknown_template_levels() {
        let factory = KafkaSinkFactory::new();
        let err = factory
            .build(cfg(), &ctx(Arc::from([dataset(&["site"])])))
            .err()
            .expect("robot is not a level");
        assert!(err.message.contains("not identity levels"), "{err}");
        assert!(err.message.contains("robot"), "{err}");

        let mut v = cfg();
        v["topic"] = serde_json::json!("telemetry.{dataset}");
        v["key"] = serde_json::json!("{site}");
        let err = factory
            .build(v, &ctx(Arc::from([dataset(&["robot"])])))
            .err()
            .expect("site is not a level");
        assert!(err.message.contains("site"), "{err}");

        // `event_id` is a valid key placeholder on any dataset.
        let mut v = cfg();
        v["topic"] = serde_json::json!("telemetry.{dataset}");
        v["key"] = serde_json::json!("{event_id}");
        factory
            .build(v, &ctx(Arc::from([dataset(&["robot"])])))
            .expect("event_id key");
    }
}
