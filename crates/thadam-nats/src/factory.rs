//! The `nats` [`BatchSinkFactory`].

use thadam_core::{BatchSink, BatchSinkFactory, BuildContext, ErrorDomain, ThadamError, codes};

use crate::config::{self, NatsConfig};
use crate::sink::NatsSink;

/// Builds [`NatsSink`]s. Registered in the CLI registry under `kind: nats`.
#[derive(Clone, Copy, Debug, Default)]
pub struct NatsSinkFactory;

impl NatsSinkFactory {
    /// A factory instance.
    pub fn new() -> Self {
        NatsSinkFactory
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
            "invalid nats sink config",
        )
    })
}

impl BatchSinkFactory for NatsSinkFactory {
    fn kind(&self) -> &'static str {
        "nats"
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
        let nats_cfg: NatsConfig = config::parse_config(&cfg).map_err(first_error)?;
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
                    "a `kind: nats` sink publishes exactly one dataset (got {}: [{}]); \
                     give it a `datasets: [<id>]` selector listing one dataset and add one sink \
                     entry per dataset. This restriction lifts when the batch contract groups \
                     events per dataset (ADR-0002 open work)",
                    ctx.datasets.len(),
                    names,
                ),
            ));
        };
        // Every non-`dataset` placeholder must name a declared identity
        // level, so a typo fails at build instead of rejecting every event.
        let missing: Vec<&str> = nats_cfg
            .subject
            .variables()
            .into_iter()
            .filter(|v| *v != "dataset")
            .filter(|v| !decl.levels.iter().any(|l| &*l.name == *v))
            .collect();
        if !missing.is_empty() {
            return Err(config_error(
                "subject",
                format!(
                    "subject placeholders {:?} are not identity levels of dataset `{}` ({:?})",
                    missing,
                    decl.id,
                    decl.levels
                        .iter()
                        .map(|l| l.name.to_string())
                        .collect::<Vec<_>>(),
                ),
            ));
        }
        Ok(Box::new(NatsSink::new(
            nats_cfg,
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
            "url": "nats://nats.internal:4222",
            "subject": "thadam.{dataset}.{robot}",
            "stream": "THADAM"
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
            "nats-out",
            Arc::new(SystemClock::new()),
            PathBuf::from("/tmp/x"),
        )
        .with_datasets(datasets)
    }

    #[test]
    fn kind_and_schema_anchor() {
        let factory = NatsSinkFactory::new();
        assert_eq!(factory.kind(), "nats");
        let schema = factory.config_schema();
        assert!(schema["properties"]["stream"].is_object());
        factory.validate(&cfg()).expect("valid");
        let mut bad = cfg();
        bad["url"] = serde_json::json!("http://nope");
        assert!(factory.validate(&bad).is_err());
    }

    #[test]
    fn build_requires_exactly_one_dataset() {
        let factory = NatsSinkFactory::new();
        let err = factory
            .build(cfg(), &ctx(Arc::from([])))
            .err()
            .expect("no datasets fails");
        assert!(err.message.contains("exactly one dataset"), "{err}");

        let two: Arc<[DatasetDecl]> = Arc::from([dataset(&[]), dataset(&[])]);
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
    fn build_rejects_unknown_subject_levels() {
        let factory = NatsSinkFactory::new();
        let err = factory
            .build(cfg(), &ctx(Arc::from([dataset(&["site"])])))
            .err()
            .expect("robot is not a level");
        assert!(err.message.contains("not identity levels"), "{err}");
        assert!(err.message.contains("robot"), "{err}");
    }
}
