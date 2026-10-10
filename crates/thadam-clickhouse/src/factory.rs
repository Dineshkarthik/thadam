//! The `clickhouse` [`BatchSinkFactory`].

use std::sync::Arc;

use thadam_arrow::{ArrowOptions, DatasetSchema};
use thadam_core::{
    BatchSink, BatchSinkFactory, BuildContext, DatasetDecl, ErrorDomain, ThadamError, codes,
};

use crate::config::{self, ChConfig};
use crate::sink::ClickHouseSink;

/// Builds [`ClickHouseSink`]s. Registered in the CLI registry under
/// `kind: clickhouse`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClickHouseSinkFactory;

impl ClickHouseSinkFactory {
    /// A factory instance.
    pub fn new() -> Self {
        ClickHouseSinkFactory
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
            "invalid clickhouse sink config",
        )
    })
}

fn dataset_schema(decl: &DatasetDecl) -> Result<DatasetSchema, ThadamError> {
    // Micro timestamps, lineage columns, native unsigned (ADR-0013
    // defaults): timestamp columns map onto `DateTime64`, `_event_id` onto
    // `UUID` (the ADR-0008 ReplacingMergeTree fallback), and ClickHouse has
    // native unsigned integers, so no policy knobs are needed.
    let options = ArrowOptions::default();
    let mut builder = DatasetSchema::builder(&*decl.id)
        .exploded(decl.exploded)
        .options(options);
    for level in &decl.levels {
        builder = builder.level(&*level.name, level.id);
    }
    builder = builder.fields(decl.fields.iter().cloned());
    builder.build().map_err(|e| {
        config_error(
            "datasets",
            format!(
                "cannot build the Arrow schema of dataset `{}`: {e}",
                decl.id
            ),
        )
    })
}

impl BatchSinkFactory for ClickHouseSinkFactory {
    fn kind(&self) -> &'static str {
        "clickhouse"
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
        Ok(Box::new(build_sink(&cfg, ctx)?))
    }
}

/// Builds the sink from validated configuration. Pure: no network, no
/// trust-store access; the HTTP client is created later, by `open`.
fn build_sink(cfg: &serde_json::Value, ctx: &BuildContext) -> Result<ClickHouseSink, ThadamError> {
    let ch_cfg: ChConfig = config::parse_config(cfg).map_err(first_error)?;
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
                "a `kind: clickhouse` sink consumes exactly one dataset (got {}: [{}]); \
                 give it a `datasets: [<id>]` selector listing one dataset and add one sink \
                 entry per dataset. This restriction lifts when the batch contract groups \
                 events per dataset (ADR-0002 open work)",
                ctx.datasets.len(),
                names,
            ),
        ));
    };
    let schema = Arc::new(dataset_schema(decl)?);
    Ok(ClickHouseSink::new(
        ch_cfg,
        ctx.component_id.to_string(),
        schema,
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;

    use thadam_core::{FieldDef, FieldType, LevelDecl, SystemClock};

    use super::*;

    fn cfg() -> serde_json::Value {
        serde_json::json!({
            "url": "http://clickhouse.internal:8123",
            "database": "telemetry",
            "table": "imu",
            "datasets": ["imu"]
        })
    }

    fn dataset(id: &str) -> DatasetDecl {
        DatasetDecl::new(
            id,
            vec![FieldDef::new("temp", FieldType::F64).with_field_id(1000)],
            vec![LevelDecl::new("robot", 100)],
            false,
        )
    }

    fn ctx(datasets: Arc<[DatasetDecl]>) -> BuildContext {
        BuildContext::new(
            "ch-imu",
            Arc::new(SystemClock::new()),
            PathBuf::from("/tmp/thadam-clickhouse-test"),
        )
        .with_datasets(datasets)
    }

    #[test]
    fn kind_schema_and_validation() {
        let factory = ClickHouseSinkFactory::new();
        assert_eq!(factory.kind(), "clickhouse");
        let schema = factory.config_schema();
        assert_eq!(schema["required"], serde_json::json!(["url", "table"]));
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
        factory.validate(&cfg()).expect("valid");

        let mut bad = cfg();
        bad["url"] = serde_json::json!("ftp://nope");
        bad["dedup"] = serde_json::json!("sometimes");
        let errs = factory.validate(&bad).expect_err("invalid");
        assert!(!errs.is_empty());
    }

    #[test]
    fn build_requires_exactly_one_dataset() {
        let factory = ClickHouseSinkFactory::new();
        let err = factory
            .build(cfg(), &ctx(Arc::from([])))
            .err()
            .expect("no datasets");
        assert!(err.message.contains("exactly one dataset"), "{err}");

        let two: Arc<[DatasetDecl]> = Arc::from([dataset("imu"), dataset("gps")]);
        let err = factory.build(cfg(), &ctx(two)).err().expect("two datasets");
        assert!(err.message.contains("exactly one dataset"), "{err}");
        assert!(
            err.message.contains("imu, gps"),
            "names the datasets: {err}"
        );

        factory
            .build(cfg(), &ctx(Arc::from([dataset("imu")])))
            .expect("one dataset builds");
    }

    #[test]
    fn build_is_pure_and_leaves_the_client_to_open() {
        use std::sync::atomic::Ordering;

        let factory = ClickHouseSinkFactory::new();
        factory.validate(&cfg()).expect("valid");
        let c = ctx(Arc::from([dataset("imu")]));
        let sink = build_sink(&cfg(), &c).expect("builds");
        assert!(
            !sink.has_client(),
            "the client is created by open, not build"
        );
        drop(factory.build(cfg(), &c).expect("trait build"));
        assert_eq!(
            crate::sink::CLIENTS_BUILT.load(Ordering::SeqCst),
            0,
            "no unit test may load the OS trust store or open a socket"
        );
    }

    #[test]
    fn build_surfaces_config_errors_first() {
        let factory = ClickHouseSinkFactory::new();
        let mut bad = cfg();
        bad["table"] = serde_json::json!("");
        let err = factory
            .build(bad, &ctx(Arc::from([dataset("imu")])))
            .err()
            .expect("bad table");
        assert!(err.message.contains("table name"), "{err}");
    }

    #[test]
    fn unbuildable_dataset_schemas_are_config_errors() {
        // A declared field without a field id cannot become an Arrow schema.
        let broken = DatasetDecl::new(
            "imu",
            vec![FieldDef::new("temp", FieldType::F64)],
            Vec::new(),
            false,
        );
        let err = ClickHouseSinkFactory::new()
            .build(cfg(), &ctx(Arc::from([broken])))
            .err()
            .expect("no field id");
        assert!(
            err.message.contains("cannot build the Arrow schema"),
            "{err}"
        );
    }
}
