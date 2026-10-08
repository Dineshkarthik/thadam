//! Typed configuration for the two factories, parsed from `serde_json::Value`
//! (D22), each with a JSON Schema for `thadam validate`.
//!
//! The Parquet sink's dataset schemas are embedded in the sink's own `config:`
//! block under `schemas:` (a map from dataset id to its declared fields and
//! identity levels) because [`thadam_core::BuildContext`] does not carry the
//! pipeline specification. Field and level ids are the explicit Parquet field
//! IDs of ADR-0013; the chain selector `datasets:` (the list of ids the
//! runtime reads) stays separate.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thadam_arrow::{ArrowOptions, DatasetSchema, TsUnit, UnsignedPolicy};
use thadam_core::{ErrorDomain, FieldDef, FieldType, ThadamError, Shape, ShapeRef, codes};
use serde::Deserialize;

use crate::error::SegmentError;
use crate::partition::{dataset_component_ok, node_component_ok};

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path.to_owned())
}

/// A parsed, validated Parquet segment-sink configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ParquetSinkConfig {
    /// Directory the sink states under (`staging/`, `ready/`, `catalog.log`).
    pub dir: PathBuf,
    /// Node identifier written into file names; must be a safe path component.
    pub node_id: String,
    /// The dataset schemas, keyed by dataset id, in configuration order.
    pub datasets: BTreeMap<String, Arc<DatasetSchema>>,
    /// Whether the dataset explodes samples: one entry per dataset id.
    pub exploded: BTreeMap<String, bool>,
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
    schemas: BTreeMap<String, RawDataset>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDataset {
    #[serde(default)]
    explode: bool,
    #[serde(default)]
    identity: Vec<RawLevel>,
    fields: Vec<RawField>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLevel {
    name: String,
    id: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawField {
    name: String,
    #[serde(rename = "type")]
    ty: RawFieldType,
    id: Option<u32>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    unit: Option<String>,
}

/// Field types as JSON: a scalar name, `{"list": <type>}`, or
/// `{"record": {"fields": [...]}}`.
#[derive(Deserialize)]
#[serde(untagged)]
enum RawFieldType {
    Scalar(String),
    List { list: Box<RawFieldType> },
    Record { record: RawRecord },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRecord {
    fields: Vec<RawField>,
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

fn parse_field_type(
    path: &str,
    raw: &RawFieldType,
    errors: &mut Vec<ThadamError>,
) -> Option<FieldType> {
    match raw {
        RawFieldType::Scalar(name) => Some(match name.as_str() {
            "bool" => FieldType::Bool,
            "i8" => FieldType::I8,
            "i16" => FieldType::I16,
            "i32" => FieldType::I32,
            "i64" => FieldType::I64,
            "u8" => FieldType::U8,
            "u16" => FieldType::U16,
            "u32" => FieldType::U32,
            "u64" => FieldType::U64,
            "f32" => FieldType::F32,
            "f64" => FieldType::F64,
            "str" => FieldType::Str,
            "bytes" => FieldType::Bytes,
            "ts" => FieldType::Ts,
            other => {
                errors.push(config_error(path, format!("unknown field type {other:?}")));
                return None;
            }
        }),
        RawFieldType::List { list } => {
            parse_field_type(path, list, errors).map(|inner| FieldType::List(Box::new(inner)))
        }
        RawFieldType::Record { record } => {
            if record.fields.is_empty() {
                errors.push(config_error(path, "a record type needs at least one field"));
                return None;
            }
            let mut defs = Vec::with_capacity(record.fields.len());
            for f in &record.fields {
                let ty = parse_field_type(&format!("{path}.{}", f.name), &f.ty, errors)?;
                let mut def = FieldDef::new(f.name.as_str(), ty);
                if f.required {
                    def = def.required();
                }
                if let Some(unit) = &f.unit {
                    def = def.with_unit(unit.as_str());
                }
                defs.push(def);
            }
            match Shape::new(defs) {
                Ok(shape) => Some(FieldType::Record(ShapeRef::new(shape))),
                Err(e) => {
                    errors.push(config_error(path, format!("invalid record: {e}")));
                    None
                }
            }
        }
    }
}

fn build_dataset_schema(
    name: &str,
    raw: &RawDataset,
    options: ArrowOptions,
) -> Result<DatasetSchema, Vec<ThadamError>> {
    let mut errors = Vec::new();
    let mut builder = DatasetSchema::builder(name)
        .exploded(raw.explode)
        .options(options);
    for level in &raw.identity {
        builder = builder.level(level.name.as_str(), level.id);
    }
    for f in &raw.fields {
        let path = format!("schemas.{name}.fields.{}", f.name);
        let Some(ty) = parse_field_type(&path, &f.ty, &mut errors) else {
            continue;
        };
        let mut def = FieldDef::new(f.name.as_str(), ty);
        if let Some(id) = f.id {
            def = def.with_field_id(id);
        }
        if f.required {
            def = def.required();
        }
        if let Some(unit) = &f.unit {
            def = def.with_unit(unit.as_str());
        }
        builder = builder.field(def);
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    builder
        .build()
        .map_err(|e| vec![SegmentError::schema(name.to_owned(), e).into()])
}

/// Parses and validates the `config:` block of a `kind: parquet` sink.
/// `dir` and the `node_id` default come from the build context. Every problem
/// found is reported, in configuration order.
pub fn parse_sink_config(
    value: &serde_json::Value,
    dir: PathBuf,
    default_node_id: &str,
) -> Result<ParquetSinkConfig, Vec<ThadamError>> {
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
) -> Result<ParquetSinkConfig, Vec<ThadamError>> {
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

    let options = ArrowOptions {
        ts_unit: match raw.timestamp_unit {
            RawTsUnit::Micros => TsUnit::Micros,
            RawTsUnit::Nanos => TsUnit::Nanos,
        },
        lineage_columns: true,
        unsigned: match raw.unsigned {
            RawUnsigned::Native => UnsignedPolicy::Native,
            RawUnsigned::Signed => UnsignedPolicy::Signed,
        },
    };
    let timestamp_unit = options.ts_unit;
    let unsigned = options.unsigned;

    let mut datasets: BTreeMap<String, Arc<DatasetSchema>> = BTreeMap::new();
    let mut exploded: BTreeMap<String, bool> = BTreeMap::new();
    if raw.schemas.is_empty() {
        errors.push(config_error(
            "schemas",
            "at least one dataset schema is required",
        ));
    }
    for (name, ds) in &raw.schemas {
        if !dataset_component_ok(name) {
            errors.push(config_error(
                format!("schemas.{name}").as_str(),
                "dataset names must be single path components",
            ));
            continue;
        }
        match build_dataset_schema(name, ds, options) {
            Ok(schema) => {
                exploded.insert(name.clone(), ds.explode);
                datasets.insert(name.clone(), Arc::new(schema));
            }
            Err(mut es) => errors.append(&mut es),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(ParquetSinkConfig {
        dir,
        node_id,
        datasets,
        exploded,
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
        timestamp_unit,
        unsigned,
        finalize_on_shutdown: raw.finalize_on_shutdown,
        config_hash: raw.config_hash,
        enrichment_hash: raw.enrichment_hash,
    })
}

/// The JSON Schema for a `kind: parquet` sink's `config:` block.
pub fn sink_config_schema() -> serde_json::Value {
    let field_type = serde_json::json!({
        "oneOf": [
            {"type": "string", "enum": ["bool","i8","i16","i32","i64","u8","u16","u32","u64","f32","f64","str","bytes","ts"]},
            {"type": "object", "required": ["list"], "properties": {"list": {"$ref": "#/$defs/fieldType"}}, "additionalProperties": false},
            {"type": "object", "required": ["record"], "properties": {"record": {"type": "object", "required": ["fields"], "properties": {"fields": {"type": "array", "minItems": 1, "items": {"$ref": "#/$defs/recordField"}}}, "additionalProperties": false}}, "additionalProperties": false}
        ]
    });
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Parquet segment sink",
        "description": "Writes one Parquet segment per dataset with rotation, the commit-then-ship finalize sequence (D18), and a segment catalog.",
        "type": "object",
        "additionalProperties": false,
        "required": ["schemas"],
        "$defs": {
            "fieldType": field_type,
            "field": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name", "type"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "type": {"$ref": "#/$defs/fieldType"},
                    "id": {"type": "integer", "minimum": 1000, "maximum": 4194303,
                        "description": "Explicit Parquet field ID (ADR-0013); never reused."},
                    "required": {"type": "boolean", "default": false},
                    "unit": {"type": "string", "description": "UCUM unit"}
                }
            },
            "recordField": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name", "type"],
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "type": {"$ref": "#/$defs/fieldType"},
                    "required": {"type": "boolean", "default": false},
                    "unit": {"type": "string"}
                }
            }
        },
        "properties": {
            "schemas": {
                "type": "object",
                "minProperties": 1,
                "description": "Dataset schemas, keyed by dataset id. The runtime selects which of these flow to this sink through the separate `datasets:` id list.",
                "additionalProperties": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["fields"],
                    "properties": {
                        "explode": {"type": "boolean", "default": false, "description": "Exploded datasets carry sample_idx."},
                        "identity": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["name", "id"],
                                "properties": {
                                    "name": {"type": "string", "minLength": 1},
                                    "id": {"type": "integer", "minimum": 100, "maximum": 199}
                                }
                            }
                        },
                        "fields": {"type": "array", "items": {"$ref": "#/$defs/field"}}
                    }
                }
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
    use super::*;

    fn base_json() -> serde_json::Value {
        serde_json::json!({
            "schemas": {
                "telemetry": {
                    "identity": [{"name": "robot", "id": 100}],
                    "explode": false,
                    "fields": [
                        {"name": "soc_pct", "type": "f32", "id": 1000, "unit": "1"},
                        {"name": "temps", "type": {"list": "f32"}, "id": 1001},
                        {"name": "pose", "type": {"record": {"fields": [
                            {"name": "x", "type": "f64", "required": true},
                            {"name": "y", "type": "f64"}
                        ]}}, "id": 1002}
                    ]
                }
            },
            "rotation": {"max_rows": 100, "max_age": null},
            "node_id": "edge01",
            "config_hash": "abc123"
        })
    }

    #[test]
    fn parses_valid_config() {
        let cfg = parse_sink_config(&base_json(), PathBuf::from("/data"), "sink")
            .unwrap_or_else(|es| panic!("{es:?}"));
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
        assert!(!cfg.exploded["telemetry"]);
    }

    #[test]
    fn collects_multiple_errors() {
        let bad = serde_json::json!({
            "schemas": {
                "bad/name": {"fields": []},
                "telemetry": {"fields": [{"name": "a", "type": "f16"}]},
                "telemetry2": {"fields": [{"name": "b", "type": "f32"}]}
            },
            "max_open_files": 0,
            "rotation": {"max_rows": 0}
        });
        let errs = parse_sink_config(&bad, PathBuf::from("/data"), "sink").unwrap_err();
        let messages: Vec<_> = errs.iter().map(|e| e.message.clone()).collect();
        assert!(
            messages.iter().any(|m| m.contains("path component")),
            "{messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("unknown field type")),
            "{messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("at least 1")),
            "{messages:?}"
        );
        // Missing field IDs are caught by the schema builder (explicit IDs).
        assert!(
            messages
                .iter()
                .any(|m| m.contains("field id") || m.contains("id")),
            "{messages:?}"
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
