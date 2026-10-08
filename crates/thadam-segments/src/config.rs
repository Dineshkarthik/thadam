//! Typed configuration for the two factories, parsed from `serde_json::Value`
//! (D22), each with a JSON Schema for `thadam validate`.
//!
//! Dataset schemas are declared once, at pipeline level. The runtime attaches
//! them to the sink's [`thadam_core::BuildContext`] (restricted to the sink's
//! `datasets:` selector) and this crate turns them into
//! [`thadam_arrow::DatasetSchema`]s in [`ParquetSinkOptions::with_datasets`];
//! a `schemas:` block in the sink's own `config:` is refused and points back
//! at the pipeline-level `datasets:` section. Field and level ids are the
//! explicit Parquet field IDs of ADR-0013.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thadam_arrow::{ArrowOptions, DatasetSchema, TsUnit, UnsignedPolicy};
use thadam_core::{DatasetDecl, ErrorDomain, ThadamError, codes};
use serde::Deserialize;

use crate::error::SegmentError;
use crate::partition::{dataset_component_ok, node_component_ok};

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// The first error of a list, for the single-error factory `build` boundary.
pub(crate) fn first_error(mut errors: Vec<ThadamError>) -> ThadamError {
    errors.drain(..).next().unwrap_or_else(|| {
        ThadamError::permanent(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            "invalid parquet sink config",
        )
    })
}

/// A parsed, validated Parquet segment-sink configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ParquetSinkConfig {
    /// Directory the sink states under (`staging/`, `ready/`, `catalog.log`).
    pub dir: PathBuf,
    /// Node identifier written into file names; must be a safe path component.
    pub node_id: String,
    /// The dataset schemas, keyed by dataset id, built from the declared
    /// datasets the runtime passed in the build context.
    pub datasets: BTreeMap<String, Arc<DatasetSchema>>,
    /// The compression codec in effect (`auto` already resolved).
    pub compression: Compression,
    /// Flush a row group when its anticipated size reaches this many bytes.
    pub row_group_bytes: usize,
    /// Rotation thresholds.
    pub rotation: RotationConfig,
    /// Maximum simultaneously open segment files, per sink.
    pub max_open_files: usize,
    /// Optional segment budget limit for this sink, applied to the shared
    /// budget's `Segments` category at recovery.
    pub max_disk: Option<u64>,
    /// Timestamp unit for new files (`timestamp_unit`).
    pub timestamp_unit: TsUnit,
    /// Unsigned-integer storage policy.
    pub unsigned: UnsignedPolicy,
    /// Whether `finalize_all` writes pending segments at shutdown.
    pub finalize_on_shutdown: bool,
    /// `thadam.config_hash` footer value.
    pub config_hash: String,
    /// `thadam.enrichment_hash` footer value (empty when there is no
    /// enrichment file).
    pub enrichment_hash: String,
}

/// A parsed `kind: parquet` `config:` block before the declared datasets are
/// attached; produced by [`parse_sink_config`], completed by
/// [`ParquetSinkOptions::with_datasets`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ParquetSinkOptions {
    /// Directory the sink states under.
    pub dir: PathBuf,
    /// Node identifier written into file names.
    pub node_id: String,
    /// The dataset ids named by `config.datasets`; empty selects every
    /// dataset the runtime offers. The runtime reads the same key for the
    /// chain, so this list can only validate what it already restricted.
    pub selector: Vec<String>,
    /// The compression codec in effect (`auto` already resolved).
    pub compression: Compression,
    /// Flush a row group when its anticipated size reaches this many bytes.
    pub row_group_bytes: usize,
    /// Rotation thresholds.
    pub rotation: RotationConfig,
    /// Maximum simultaneously open segment files, per sink.
    pub max_open_files: usize,
    /// Optional segment budget limit for this sink.
    pub max_disk: Option<u64>,
    /// Timestamp unit for new files.
    pub timestamp_unit: TsUnit,
    /// Unsigned-integer storage policy.
    pub unsigned: UnsignedPolicy,
    /// Whether `finalize_all` writes pending segments at shutdown.
    pub finalize_on_shutdown: bool,
    /// `thadam.config_hash` footer value.
    pub config_hash: String,
    /// `thadam.enrichment_hash` footer value.
    pub enrichment_hash: String,
}

impl ParquetSinkOptions {
    /// Builds a [`thadam_arrow::DatasetSchema`] per declared dataset and
    /// returns the finished sink configuration. Every problem found is
    /// reported, in declaration order: an empty context (schemas no longer
    /// come from the sink config, so there is nothing to build from),
    /// selector entries that name no declared dataset, duplicate ids, ids
    /// unsafe for paths, and schema build failures such as a missing field
    /// ID.
    pub fn with_datasets(
        self,
        datasets: &[DatasetDecl],
    ) -> Result<ParquetSinkConfig, Vec<ThadamError>> {
        let mut errors = Vec::new();
        if datasets.is_empty() {
            errors.push(config_error(
                "datasets",
                "the build context holds no datasets: declare every dataset once in the pipeline-level `datasets:` section and list the ones this sink consumes in its `config.datasets`",
            ));
        }
        let mut seen: HashSet<&str> = HashSet::new();
        let mut selected: Vec<&DatasetDecl> = Vec::new();
        for d in datasets {
            if !seen.insert(&d.id) {
                errors.push(config_error(
                    "datasets",
                    format!("dataset {:?} is declared more than once", &*d.id),
                ));
                continue;
            }
            if self.selector.is_empty() || self.selector.iter().any(|s| s == &*d.id) {
                selected.push(d);
            }
        }
        for id in &self.selector {
            if !datasets.iter().any(|d| &*d.id == id) {
                errors.push(config_error(
                    "datasets",
                    format!(
                        "this sink selects dataset {id:?}, which is not declared: add it to the pipeline-level `datasets:` section"
                    ),
                ));
            }
        }

        let options = ArrowOptions {
            ts_unit: self.timestamp_unit,
            lineage_columns: true,
            unsigned: self.unsigned,
        };
        let mut schemas: BTreeMap<String, Arc<DatasetSchema>> = BTreeMap::new();
        for d in selected {
            let name = d.id.to_string();
            if !dataset_component_ok(&name) {
                errors.push(config_error(
                    format!("datasets.{name}").as_str(),
                    "dataset ids must be single path components",
                ));
                continue;
            }
            let mut builder = DatasetSchema::builder(&*d.id)
                .exploded(d.exploded)
                .options(options);
            for level in &d.levels {
                builder = builder.level(&*level.name, level.id);
            }
            builder = builder.fields(d.fields.iter().cloned());
            match builder.build() {
                Ok(schema) => {
                    schemas.insert(name, Arc::new(schema));
                }
                Err(e) => errors.push(SegmentError::schema(name, e).into()),
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        Ok(ParquetSinkConfig {
            dir: self.dir,
            node_id: self.node_id,
            datasets: schemas,
            compression: self.compression,
            row_group_bytes: self.row_group_bytes,
            rotation: self.rotation,
            max_open_files: self.max_open_files,
            max_disk: self.max_disk,
            timestamp_unit: self.timestamp_unit,
            unsigned: self.unsigned,
            finalize_on_shutdown: self.finalize_on_shutdown,
            config_hash: self.config_hash,
            enrichment_hash: self.enrichment_hash,
        })
    }
}

/// Rotation thresholds: first of `max_bytes`, `max_rows`, or `max_age`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RotationConfig {
    /// Finalize once the file's exact flushed bytes plus the in-progress row
    /// group's estimate reach this size. Production default 128 MiB.
    pub max_bytes: Option<u64>,
    /// Finalize after this many rows.
    pub max_rows: Option<u64>,
    /// Finalize once the open segment is this old (monotonic time).
    pub max_age: Option<Duration>,
}

impl Default for RotationConfig {
    /// Production defaults: `max_bytes` 128 MiB, `max_age` 5 minutes.
    fn default() -> Self {
        RotationConfig {
            max_bytes: Some(128 << 20),
            max_rows: None,
            max_age: Some(Duration::from_secs(300)),
        }
    }
}

impl RotationConfig {
    /// No rotation: files close only at `finalize_all`. Test-only convenience.
    #[cfg(test)]
    pub(crate) fn never() -> Self {
        RotationConfig {
            max_bytes: None,
            max_rows: None,
            max_age: None,
        }
    }
}

/// The codec of written files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// No compression.
    None,
    /// Snappy.
    Snappy,
    /// Zstandard at level 1.
    Zstd1,
    /// LZ4.
    Lz4,
}

impl Compression {
    /// `auto`: `zstd(1)` on x86_64, `snappy` on aarch64 (pending benchmark B6).
    pub(crate) fn auto() -> Self {
        if cfg!(target_arch = "x86_64") {
            Compression::Zstd1
        } else {
            Compression::Snappy
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSinkConfig {
    #[serde(default)]
    datasets: Vec<String>,
    #[serde(default)]
    compression: Option<RawCompression>,
    #[serde(default = "default_row_group_bytes")]
    row_group_bytes: u64,
    #[serde(default)]
    rotation: RawRotation,
    #[serde(default = "default_max_open_files")]
    max_open_files: u64,
    #[serde(default)]
    max_disk: Option<u64>,
    #[serde(default)]
    timestamp_unit: RawTsUnit,
    #[serde(default)]
    unsigned: RawUnsigned,
    #[serde(default = "default_finalize_on_shutdown")]
    finalize_on_shutdown: bool,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    config_hash: String,
    #[serde(default)]
    enrichment_hash: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawCompression {
    Auto,
    Zstd,
    Snappy,
    Lz4,
    None,
}

impl RawCompression {
    fn resolve(self) -> Compression {
        match self {
            RawCompression::Auto => Compression::auto(),
            RawCompression::Zstd => Compression::Zstd1,
            RawCompression::Snappy => Compression::Snappy,
            RawCompression::Lz4 => Compression::Lz4,
            RawCompression::None => Compression::None,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRotation {
    #[serde(default = "default_rotation_max_bytes")]
    max_bytes: Option<u64>,
    #[serde(default)]
    max_rows: Option<u64>,
    #[serde(default = "default_rotation_max_age")]
    max_age: Option<u64>,
}

fn default_row_group_bytes() -> u64 {
    8 << 20
}

fn default_max_open_files() -> u64 {
    4
}

fn default_finalize_on_shutdown() -> bool {
    true
}

fn default_rotation_max_bytes() -> Option<u64> {
    Some(128 << 20)
}

fn default_rotation_max_age() -> Option<u64> {
    Some(300)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawTsUnit {
    #[default]
    Micros,
    Nanos,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawUnsigned {
    #[default]
    Native,
    Signed,
}

/// Parses and validates the `config:` block of a `kind: parquet` sink into
/// [`ParquetSinkOptions`]; `dir` and the `node_id` default come from the
/// build context. A `schemas:` block is refused: datasets are declared once
/// at pipeline level, and the runtime attaches them through the build
/// context, which [`ParquetSinkOptions::with_datasets`] turns into the
/// sink's schemas. Every problem found is reported, in configuration order.
pub fn parse_sink_config(
    value: &serde_json::Value,
    dir: PathBuf,
    default_node_id: &str,
) -> Result<ParquetSinkOptions, Vec<ThadamError>> {
    if value.get("schemas").is_some() {
        return Err(vec![config_error(
            "config.schemas",
            "dataset schemas are declared once, in the pipeline-level `datasets:` section: move each entry of `schemas:` there (field `id`s on `datasets[].fields`, level `id`s on `datasets[].identity`, `explode` beside them) and list the ids this sink consumes in its `config.datasets`",
        )]);
    }
    let raw: RawSinkConfig = match serde_json::from_value(value.clone()) {
        Ok(raw) => raw,
        Err(e) => return Err(vec![config_error("config", e.to_string())]),
    };
    parse_sink_raw(raw, dir, default_node_id)
}

fn parse_sink_raw(
    raw: RawSinkConfig,
    dir: PathBuf,
    default_node_id: &str,
) -> Result<ParquetSinkOptions, Vec<ThadamError>> {
    let mut errors = Vec::new();
    let node_id = raw.node_id.unwrap_or_else(|| default_node_id.to_owned());
    if !node_component_ok(&node_id) {
        errors.push(config_error(
            "node_id",
            "node_id must be a non-empty single path component without path separators",
        ));
    }
    if raw.row_group_bytes == 0 {
        errors.push(config_error("row_group_bytes", "must be at least 1"));
    }
    if raw.max_open_files == 0 {
        errors.push(config_error("max_open_files", "must be at least 1"));
    }
    if let Some(rotation_max_bytes) = raw.rotation.max_bytes
        && rotation_max_bytes == 0
    {
        errors.push(config_error(
            "rotation.max_bytes",
            "must be null or at least 1",
        ));
    }
    if let Some(0) = raw.rotation.max_rows {
        errors.push(config_error(
            "rotation.max_rows",
            "must be null or at least 1",
        ));
    }
    if let Some(0) = raw.rotation.max_age {
        errors.push(config_error(
            "rotation.max_age",
            "must be null or at least 1 second",
        ));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(ParquetSinkOptions {
        dir,
        node_id,
        selector: raw.datasets,
        compression: raw
            .compression
            .map_or_else(Compression::auto, RawCompression::resolve),
        row_group_bytes: raw.row_group_bytes as usize,
        rotation: RotationConfig {
            max_bytes: raw.rotation.max_bytes,
            max_rows: raw.rotation.max_rows,
            max_age: raw.rotation.max_age.map(Duration::from_secs),
        },
        max_open_files: raw.max_open_files as usize,
        max_disk: raw.max_disk,
        timestamp_unit: match raw.timestamp_unit {
            RawTsUnit::Micros => TsUnit::Micros,
            RawTsUnit::Nanos => TsUnit::Nanos,
        },
        unsigned: match raw.unsigned {
            RawUnsigned::Native => UnsignedPolicy::Native,
            RawUnsigned::Signed => UnsignedPolicy::Signed,
        },
        finalize_on_shutdown: raw.finalize_on_shutdown,
        config_hash: raw.config_hash,
        enrichment_hash: raw.enrichment_hash,
    })
}

/// The JSON Schema for a `kind: parquet` sink's `config:` block.
pub fn sink_config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Parquet segment sink",
        "description": "Writes one Parquet segment per dataset with rotation, the commit-then-ship finalize sequence (D18), and a segment catalog. Dataset schemas are the pipeline-level `datasets:` declarations (field and identity-level IDs live there); this `config:` block selects which of them the sink consumes and tunes the writer.",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "datasets": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
                "uniqueItems": true,
                "description": "Ids from the pipeline-level `datasets:` section this sink consumes. The runtime reads the same list to route frames, so the sink's declared schemas always match its chain. Empty or absent consumes every declared dataset."
            },
            "compression": {"type": "string", "enum": ["auto","zstd","snappy","lz4","none"], "default": "auto",
                "description": "auto is zstd(1) on x86_64 and snappy elsewhere."},
            "row_group_bytes": {"type": "integer", "minimum": 1, "default": 8388608,
                "description": "Flush a row group when its anticipated size reaches this many bytes."},
            "rotation": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "max_bytes": {"type": ["integer", "null"], "minimum": 1, "default": 134217728},
                    "max_rows": {"type": ["integer", "null"], "minimum": 1, "default": null},
                    "max_age": {"type": ["integer", "null"], "minimum": 1, "default": 300,
                        "description": "Seconds since the segment opened (monotonic)."}
                }
            },
            "max_open_files": {"type": "integer", "minimum": 1, "default": 4,
                "description": "Open segment files per sink; the least recently written segment is finalized early when reached."},
            "max_disk": {"type": ["integer", "null"], "minimum": 1, "default": null,
                "description": "This sink's segment budget limit; enforced with the shared budget's segments category."},
            "timestamp_unit": {"type": "string", "enum": ["micros", "nanos"], "default": "micros"},
            "unsigned": {"type": "string", "enum": ["native", "signed"], "default": "native"},
            "finalize_on_shutdown": {"type": "boolean", "default": true},
            "node_id": {"type": "string", "description": "File-name node id; defaults to the sink id."},
            "config_hash": {"type": "string", "default": ""},
            "enrichment_hash": {"type": "string", "default": ""}
        }
    })
}

/// The JSON Schema for the quarantine block (`quarantine:`), matching
/// `thadam_config::QuarantineSpec`.
pub fn quarantine_config_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Quarantine segment sink",
        "description": "Writes rejects to Parquet segments with the fixed quarantine schema, on their own disk budget.",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "format": {"type": "string", "enum": ["parquet", "ndjson"], "default": "parquet",
                "description": "ndjson is reserved; this build supports parquet only."},
            "max_disk": {"type": "integer", "minimum": 1, "default": 268435456},
            "compression": {"type": "string", "enum": ["auto","zstd","snappy","lz4","none"], "default": "auto"},
            "rotation": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "max_bytes": {"type": ["integer", "null"], "minimum": 1, "default": 134217728},
                    "max_rows": {"type": ["integer", "null"], "minimum": 1, "default": null},
                    "max_age": {"type": ["integer", "null"], "minimum": 1, "default": 300}
                }
            }
        }
    })
}

/// Parsed quarantine configuration.
#[derive(Clone, Debug)]
pub struct QuarantineConfig {
    /// Rotation thresholds.
    pub rotation: RotationConfig,
    /// The codec.
    pub compression: Compression,
    /// The quarantine budget limit (`max_disk`), applied to the quarantine
    /// budget category at recovery.
    pub max_disk: Option<u64>,
    /// The timestamp unit of `_ingest_time`.
    pub timestamp_unit: TsUnit,
}

impl Default for QuarantineConfig {
    fn default() -> Self {
        QuarantineConfig {
            rotation: RotationConfig::default(),
            compression: Compression::auto(),
            max_disk: Some(256 << 20),
            timestamp_unit: TsUnit::Micros,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawQuarantineConfig {
    #[serde(default)]
    format: RawQuarantineFormat,
    #[serde(default = "default_quarantine_max_disk")]
    max_disk: Option<u64>,
    #[serde(default)]
    compression: Option<RawCompression>,
    #[serde(default)]
    rotation: RawRotation,
}

fn default_quarantine_max_disk() -> Option<u64> {
    Some(256 << 20)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawQuarantineFormat {
    #[default]
    Parquet,
    Ndjson,
}

/// Parses and validates the quarantine configuration.
pub fn parse_quarantine_config(
    value: &serde_json::Value,
) -> Result<QuarantineConfig, Vec<ThadamError>> {
    let raw: RawQuarantineConfig = match serde_json::from_value(value.clone()) {
        Ok(raw) => raw,
        Err(e) => return Err(vec![config_error("quarantine", e.to_string())]),
    };
    let mut errors = Vec::new();
    if matches!(raw.format, RawQuarantineFormat::Ndjson) {
        errors.push(config_error(
            "quarantine.format",
            "ndjson quarantine segments are not implemented in this build; use parquet",
        ));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(QuarantineConfig {
        rotation: RotationConfig {
            max_bytes: raw.rotation.max_bytes,
            max_rows: raw.rotation.max_rows,
            max_age: raw.rotation.max_age.map(Duration::from_secs),
        },
        compression: raw
            .compression
            .map_or_else(Compression::auto, RawCompression::resolve),
        max_disk: raw.max_disk,
        timestamp_unit: TsUnit::Micros,
    })
}

#[cfg(test)]
mod tests {
    use thadam_core::{FieldDef, FieldType, LevelDecl, Shape, ShapeRef};

    use super::*;

    fn base_json() -> serde_json::Value {
        serde_json::json!({
            "rotation": {"max_rows": 100, "max_age": null},
            "node_id": "edge01",
            "config_hash": "abc123"
        })
    }

    fn telemetry_decl() -> DatasetDecl {
        let pose = Shape::new(vec![
            FieldDef::new("x", FieldType::F64).required(),
            FieldDef::new("y", FieldType::F64),
        ])
        .unwrap_or_else(|e| panic!("pose shape: {e}"));
        DatasetDecl::new(
            "telemetry",
            vec![
                FieldDef::new("soc_pct", FieldType::F32)
                    .with_field_id(1000)
                    .with_unit("1"),
                FieldDef::new("temps", FieldType::List(Box::new(FieldType::F32)))
                    .with_field_id(1001),
                FieldDef::new("pose", FieldType::Record(ShapeRef::new(pose))).with_field_id(1002),
            ],
            vec![LevelDecl::new("robot", 100)],
            false,
        )
    }

    fn build(
        value: &serde_json::Value,
        decls: &[DatasetDecl],
    ) -> Result<ParquetSinkConfig, Vec<ThadamError>> {
        parse_sink_config(value, PathBuf::from("/data"), "sink")
            .and_then(|options| options.with_datasets(decls))
    }

    #[test]
    fn parses_valid_config_and_builds_schemas_from_decls() {
        let cfg = build(&base_json(), &[telemetry_decl()]).unwrap_or_else(|es| panic!("{es:?}"));
        assert_eq!(cfg.node_id, "edge01");
        assert_eq!(cfg.max_open_files, 4);
        assert_eq!(cfg.row_group_bytes, 8 << 20);
        assert_eq!(cfg.rotation.max_bytes, Some(128 << 20));
        assert_eq!(cfg.rotation.max_rows, Some(100));
        assert_eq!(cfg.rotation.max_age, None);
        assert!(cfg.finalize_on_shutdown);
        assert_eq!(cfg.config_hash, "abc123");
        let schema = &cfg.datasets["telemetry"];
        assert_eq!(schema.name(), "telemetry");
        assert_eq!(&*schema.levels()[0].name, "robot");
        assert!(!schema.is_exploded());
    }

    #[test]
    fn selector_restricts_and_must_match_declared_datasets() {
        let imu = DatasetDecl::new(
            "imu",
            vec![FieldDef::new("ax", FieldType::F32).with_field_id(1000)],
            vec![],
            true,
        );
        let mut v = base_json();
        v["datasets"] = serde_json::json!(["imu"]);
        let cfg = build(&v, &[telemetry_decl(), imu.clone()]).unwrap_or_else(|es| panic!("{es:?}"));
        assert!(cfg.datasets.contains_key("imu"));
        assert!(!cfg.datasets.contains_key("telemetry"));
        assert!(cfg.datasets["imu"].is_exploded());

        v["datasets"] = serde_json::json!(["other"]);
        let errs = build(&v, &[telemetry_decl()]).unwrap_err();
        let messages: Vec<_> = errs.iter().map(|e| e.message.clone()).collect();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("\"other\"") && m.contains("pipeline-level `datasets:`")),
            "{messages:?}"
        );
    }

    #[test]
    fn collects_multiple_errors_across_phases() {
        let bad = serde_json::json!({
            "max_open_files": 0,
            "rotation": {"max_rows": 0}
        });
        let errs = parse_sink_config(&bad, PathBuf::from("/data"), "sink").unwrap_err();
        let messages: Vec<_> = errs.iter().map(|e| e.message.clone()).collect();
        assert!(
            messages.iter().all(|m| m.contains("at least 1")),
            "{messages:?}"
        );
        assert_eq!(messages.len(), 2, "{messages:?}");

        let missing_id = DatasetDecl::new(
            "telemetry",
            vec![FieldDef::new("a", FieldType::F32)],
            vec![],
            false,
        );
        let bad_name = DatasetDecl::new(
            "bad/name",
            vec![FieldDef::new("b", FieldType::F32).with_field_id(1000)],
            vec![],
            false,
        );
        let errs = build(&base_json(), &[missing_id, bad_name]).unwrap_err();
        let messages: Vec<_> = errs.iter().map(|e| e.message.clone()).collect();
        assert!(
            messages.iter().any(|m| m.contains("path component")),
            "{messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("no field id")),
            "{messages:?}"
        );
    }

    #[test]
    fn refuses_a_schemas_block_pointing_at_pipeline_datasets() {
        let mut v = base_json();
        v["schemas"] = serde_json::json!({
            "telemetry": {"fields": [{"name": "a", "type": "f32", "id": 1000}]}
        });
        let errs = parse_sink_config(&v, PathBuf::from("/data"), "sink").unwrap_err();
        let messages: Vec<_> = errs.iter().map(|e| e.message.clone()).collect();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("pipeline-level `datasets:`")),
            "{messages:?}"
        );
    }

    #[test]
    fn requires_datasets_in_the_build_context() {
        let options = parse_sink_config(&base_json(), PathBuf::from("/data"), "sink")
            .unwrap_or_else(|es| panic!("{es:?}"));
        let errs = options.with_datasets(&[]).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| e.message.contains("pipeline-level `datasets:`")),
            "{errs:?}"
        );

        let dupes = [telemetry_decl(), telemetry_decl()];
        let options = parse_sink_config(&base_json(), PathBuf::from("/data"), "sink")
            .unwrap_or_else(|es| panic!("{es:?}"));
        let errs = options.with_datasets(&dupes).unwrap_err();
        assert!(
            errs.iter()
                .any(|e| e.message.contains("declared more than once")),
            "{errs:?}"
        );
    }

    #[test]
    fn rejects_unknown_keys_and_bad_node_id() {
        let mut v = base_json();
        v["bogus"] = serde_json::json!(1);
        let errs = parse_sink_config(&v, PathBuf::from("/data"), "sink").unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("bogus")));

        let mut v = base_json();
        v["node_id"] = serde_json::json!("a/b");
        let errs = parse_sink_config(&v, PathBuf::from("/data"), "sink").unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("node_id")));

        // Underscores are allowed: the name parser splits from the right.
        let mut v = base_json();
        v["node_id"] = serde_json::json!("edge_01");
        parse_sink_config(&v, PathBuf::from("/data"), "sink").unwrap_or_else(|es| panic!("{es:?}"));
    }

    #[test]
    fn quarantine_defaults_and_ndjson_refusal() {
        let cfg =
            parse_quarantine_config(&serde_json::json!({})).unwrap_or_else(|es| panic!("{es:?}"));
        assert_eq!(cfg.max_disk, Some(256 << 20));
        let errs = parse_quarantine_config(&serde_json::json!({"format": "ndjson"})).unwrap_err();
        assert!(errs.iter().any(|e| e.message.contains("ndjson")));
    }
}
