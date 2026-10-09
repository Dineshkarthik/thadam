//! The typed pipeline specification (config `version: 1`).
//!
//! Key spellings are frozen with the first fixture set: `kind`, `where`,
//! `wal`, `match`, `from`, `convert_to`, `on_full`. Adding a key is a minor
//! release because every struct rejects unknown keys. The Parquet field ID is
//! spelled `id` on a declared field and on an identity level, which is
//! required on both.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::RangeInclusive;
use std::path::PathBuf;

use thadam_core::{FieldDef, QualityStatus};
use schemars::JsonSchema;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

use crate::path::{PathVec, Seg};
use crate::predicate::Predicate;
use crate::scalars::{CountSpec, DurationSpec, FieldTypeSpec, SizeSpec, TimeDomainSpec};
use crate::secret::{REDACTED, Secret};

/// The only config version this binary reads.
pub const CONFIG_VERSION: u32 = 1;

/// A whole pipeline: one per file, one per process.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PipelineSpec {
    /// The config format version; always 1.
    #[schemars(range(min = 1, max = 1))]
    pub version: u32,
    /// Names and the data directory.
    pub pipeline: PipelineMeta,
    /// Where data comes from.
    pub sources: Vec<ConnectorSpec>,
    /// The capture WAL.
    pub wal: WalSpec,
    /// Where rejected events go.
    #[serde(default)]
    pub quarantine: QuarantineSpec,
    /// The output tables.
    pub datasets: Vec<DatasetSpec>,
    /// Steps applied to datasets, in order.
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,
    /// Where data goes.
    pub sinks: Vec<SinkSpec>,
    /// Process-level settings.
    #[serde(default)]
    pub runtime: RuntimeSpec,
    /// Logs and the admin API.
    #[serde(default)]
    pub observability: ObservabilitySpec,
    /// Scalars that came from `${env:}` or `${file:}`, kept as written so the
    /// config hash never depends on a secret's value.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) interpolations: Vec<InterpolationSite>,
}

/// A value that was produced by interpolation, identified by its path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InterpolationSite {
    pub path: PathVec,
    pub template: String,
}

impl InterpolationSite {
    pub(crate) fn is_under(&self, prefix: &[Seg]) -> bool {
        self.path.starts_with(prefix)
    }
}

/// Pipeline identity and storage location.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PipelineMeta {
    /// The pipeline name; used in object keys and refused if changed on restart.
    pub name: String,
    /// The node identity; used in file names and object keys.
    pub node_id: String,
    /// The data directory holding the WAL, segments, and `thadam.json`.
    pub data_dir: PathBuf,
}

/// A source or sink: an `id`, a registered `kind`, and an opaque `config`.
///
/// `config` stays a `serde_json::Value` (D22); the connector factory validates
/// it. Values that came from `${env:}` or `${file:}` are listed in
/// [`ConnectorSpec::secret_paths`] and redacted by `Debug`.
#[derive(Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ConnectorSpec {
    /// A unique identifier; used in metric labels and file names.
    pub id: String,
    /// The registered connector kind, such as `mqtt` or `parquet`.
    pub kind: String,
    /// Connector-specific configuration.
    #[serde(default = "empty_config")]
    #[schemars(with = "serde_json::Map<String, serde_json::Value>")]
    pub config: serde_json::Value,
    /// JSON Pointers (relative to `config`) of values produced by interpolation.
    #[serde(skip)]
    #[schemars(skip)]
    pub secret_paths: Vec<String>,
}

fn empty_config() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

fn redact(config: &serde_json::Value, secret_paths: &[String]) -> serde_json::Value {
    let mut cfg = config.clone();
    for ptr in secret_paths {
        if let Some(slot) = cfg.pointer_mut(ptr) {
            *slot = serde_json::Value::String(REDACTED.to_owned());
        }
    }
    cfg
}

fn interpolated(
    config: &serde_json::Value,
    secret_paths: &[String],
) -> Vec<(String, Secret<String>)> {
    secret_paths
        .iter()
        .filter_map(|ptr| {
            let s = config.pointer(ptr)?.as_str()?;
            Some((ptr.clone(), Secret::new(s.to_owned())))
        })
        .collect()
}

impl ConnectorSpec {
    /// `config` with every interpolated value replaced by `<redacted>`.
    pub fn redacted_config(&self) -> serde_json::Value {
        redact(&self.config, &self.secret_paths)
    }

    /// The interpolated string values, as `(pointer, secret)` pairs.
    pub fn secrets(&self) -> Vec<(String, Secret<String>)> {
        interpolated(&self.config, &self.secret_paths)
    }
}

/// Serializes with interpolated values redacted, so serializing a spec can
/// never write a secret.
impl Serialize for ConnectorSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(3))?;
        m.serialize_entry("id", &self.id)?;
        m.serialize_entry("kind", &self.kind)?;
        m.serialize_entry("config", &self.redacted_config())?;
        m.end()
    }
}

impl fmt::Debug for ConnectorSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorSpec")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("config", &self.redacted_config())
            .finish()
    }
}

fn default_required() -> bool {
    true
}

/// Where a sink starts when it has no committed WAL cursor yet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StartFromSpec {
    /// The oldest frame the WAL still holds.
    #[default]
    Earliest,
    /// The frame after the current end of the WAL; older data is skipped.
    Latest,
}

impl StartFromSpec {
    /// The key spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            StartFromSpec::Earliest => "earliest",
            StartFromSpec::Latest => "latest",
        }
    }
}

/// How far a required sink may fall behind the WAL before it stops holding
/// data back: `{ bytes: <size> }` or `{ age: <duration> }`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MaxLagSpec {
    /// WAL bytes between the sink's position and the end of the WAL.
    Bytes(SizeSpec),
    /// Age of the oldest WAL data the sink has not consumed.
    Age(DurationSpec),
}

impl<'de> Deserialize<'de> for MaxLagSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = MaxLagSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a mapping `{ bytes: <size> }` or `{ age: <duration> }`")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<MaxLagSpec, A::Error> {
                let Some(key) = map.next_key::<String>()? else {
                    return Err(de::Error::custom(
                        "`max_lag` needs one key, `bytes` or `age`",
                    ));
                };
                let lag = match key.as_str() {
                    "bytes" => MaxLagSpec::Bytes(map.next_value::<SizeSpec>()?),
                    "age" => MaxLagSpec::Age(map.next_value::<DurationSpec>()?),
                    other => {
                        return Err(de::Error::custom(format!(
                            "unknown `max_lag` key `{other}`, expected `bytes` or `age`"
                        )));
                    }
                };
                if map.next_key::<String>()?.is_some() {
                    return Err(de::Error::custom(
                        "`max_lag` takes one key, `bytes` or `age`, not both",
                    ));
                }
                Ok(lag)
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for MaxLagSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(1))?;
        match self {
            MaxLagSpec::Bytes(b) => m.serialize_entry("bytes", b)?,
            MaxLagSpec::Age(a) => m.serialize_entry("age", a)?,
        }
        m.end()
    }
}

impl JsonSchema for MaxLagSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "MaxLagSpec".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let size = generator.subschema_for::<SizeSpec>();
        let age = generator.subschema_for::<DurationSpec>();
        schemars::json_schema!({
            "description": "`{ bytes: <size> }` or `{ age: <duration> }`: the most a required sink may lag before it stops holding WAL data back.",
            "oneOf": [
                {
                    "type": "object",
                    "properties": { "bytes": size },
                    "required": ["bytes"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": { "age": age },
                    "required": ["age"],
                    "additionalProperties": false
                }
            ]
        })
    }
}

/// A sink: a [`ConnectorSpec`]'s `id`, `kind`, and `config`, plus how it takes
/// part in WAL retention.
///
/// `config` stays a `serde_json::Value` (D22). Values that came from `${env:}`
/// or `${file:}` are listed in [`SinkSpec::secret_paths`] and redacted by
/// `Debug` and `Serialize`. The three policy keys are omitted from the
/// serialized (and hashed) form when they hold their default, so configs
/// written before they existed keep their hash.
#[derive(Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SinkSpec {
    /// A unique identifier; used in metric labels, cursor names, and file names.
    pub id: String,
    /// The registered sink kind, such as `parquet` or `object_store`.
    pub kind: String,
    /// Connector-specific configuration.
    #[serde(default = "empty_config")]
    #[schemars(with = "serde_json::Map<String, serde_json::Value>")]
    pub config: serde_json::Value,
    /// Whether the WAL keeps data until this sink has consumed it (default
    /// `true`). A sink with `required: false` never holds retention back; if it
    /// falls behind retention it sees a WAL gap.
    #[serde(default = "default_required")]
    pub required: bool,
    /// How far a required sink may lag before it stops holding WAL data back
    /// (an explicit, counted loss for that sink). Needs `required: true`.
    #[serde(default)]
    pub max_lag: Option<MaxLagSpec>,
    /// Where a sink with no committed WAL cursor starts: `earliest` (default)
    /// or `latest`.
    #[serde(default)]
    pub start_from: StartFromSpec,
    /// JSON Pointers (relative to `config`) of values produced by interpolation.
    #[serde(skip)]
    #[schemars(skip)]
    pub secret_paths: Vec<String>,
}

impl SinkSpec {
    /// `config` with every interpolated value replaced by `<redacted>`.
    pub fn redacted_config(&self) -> serde_json::Value {
        redact(&self.config, &self.secret_paths)
    }

    /// The interpolated string values, as `(pointer, secret)` pairs.
    pub fn secrets(&self) -> Vec<(String, Secret<String>)> {
        interpolated(&self.config, &self.secret_paths)
    }
}

impl Serialize for SinkSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("id", &self.id)?;
        m.serialize_entry("kind", &self.kind)?;
        m.serialize_entry("config", &self.redacted_config())?;
        if !self.required {
            m.serialize_entry("required", &false)?;
        }
        if let Some(lag) = &self.max_lag {
            m.serialize_entry("max_lag", lag)?;
        }
        if self.start_from != StartFromSpec::default() {
            m.serialize_entry("start_from", &self.start_from)?;
        }
        m.end()
    }
}

impl fmt::Debug for SinkSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkSpec")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("config", &self.redacted_config())
            .field("required", &self.required)
            .field("max_lag", &self.max_lag)
            .field("start_from", &self.start_from)
            .finish()
    }
}

/// What the WAL does when `max_disk` is reached. There is no default.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnFull {
    /// Stop accepting data and apply backpressure to sources.
    Block,
    /// Evict the oldest unconsumed data; the loss is counted.
    DropOldest,
}

/// When the WAL calls `fsync`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FsyncSpec {
    /// After every commit.
    Always,
    /// At most once per interval (group commit).
    Interval(DurationSpec),
    /// Never; durability depends on the OS.
    Never,
}

impl Default for FsyncSpec {
    fn default() -> Self {
        FsyncSpec::Interval(DurationSpec(std::time::Duration::from_millis(10)))
    }
}

impl<'de> Deserialize<'de> for FsyncSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = FsyncSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`always`, `never`, or `{ interval: <duration> }`")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<FsyncSpec, E> {
                match v {
                    "always" => Ok(FsyncSpec::Always),
                    "never" => Ok(FsyncSpec::Never),
                    other => Err(E::unknown_variant(other, &["always", "never"])),
                }
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<FsyncSpec, A::Error> {
                let mut interval = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "interval" {
                        if interval.is_some() {
                            return Err(de::Error::duplicate_field("interval"));
                        }
                        interval = Some(map.next_value::<DurationSpec>()?);
                    } else {
                        return Err(de::Error::unknown_field(&key, &["interval"]));
                    }
                }
                interval
                    .map(FsyncSpec::Interval)
                    .ok_or_else(|| de::Error::missing_field("interval"))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for FsyncSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            FsyncSpec::Always => s.serialize_str("always"),
            FsyncSpec::Never => s.serialize_str("never"),
            FsyncSpec::Interval(d) => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("interval", d)?;
                m.end()
            }
        }
    }
}

impl JsonSchema for FsyncSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FsyncSpec".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let duration = generator.subschema_for::<DurationSpec>();
        schemars::json_schema!({
            "description": "`always`, `never`, or `{ interval: <duration> }` (default `{ interval: 10ms }`).",
            "oneOf": [
                { "type": "string", "enum": ["always", "never"] },
                {
                    "type": "object",
                    "properties": { "interval": duration },
                    "required": ["interval"],
                    "additionalProperties": false
                }
            ]
        })
    }
}

fn default_segment_bytes() -> SizeSpec {
    SizeSpec(16 << 20)
}

fn default_max_pending_bytes() -> SizeSpec {
    SizeSpec(8 << 20)
}

/// The capture WAL (ADR-0004).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WalSpec {
    /// The disk budget for the WAL.
    pub max_disk: SizeSpec,
    /// The size at which a segment file is sealed.
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: SizeSpec,
    /// The fsync policy.
    #[serde(default)]
    pub fsync: FsyncSpec,
    /// What happens at `max_disk`. Required: data loss must be an explicit choice.
    pub on_full: OnFull,
    /// How long consumed data stays available for replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_for: Option<DurationSpec>,
    /// Bytes a source may have submitted but not yet committed.
    #[serde(default = "default_max_pending_bytes")]
    pub max_pending_bytes: SizeSpec,
}

/// The format of the quarantine segments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QuarantineFormat {
    /// Parquet with the fixed quarantine schema.
    #[default]
    Parquet,
    /// Newline-delimited JSON.
    Ndjson,
}

/// Where rejected events go, with their own disk budget.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct QuarantineSpec {
    /// The segment format.
    #[serde(default)]
    pub format: QuarantineFormat,
    /// The disk budget for quarantine segments.
    #[serde(default = "default_quarantine_disk")]
    pub max_disk: SizeSpec,
    /// Rotation thresholds for the quarantine segment sink.
    #[serde(default, skip_serializing_if = "QuarantineRotationSpec::is_default")]
    pub rotation: QuarantineRotationSpec,
}

fn default_quarantine_disk() -> SizeSpec {
    SizeSpec(256 << 20)
}

/// Rotation thresholds for the quarantine segments; the same shape as a
/// Parquet sink's `rotation` block. Keys left out use the sink's defaults
/// (128 MiB or 300 seconds, whichever first).
///
/// A quarantined reject caps the owning sink's durable cursor until the
/// quarantine segment holding it finalizes, so `max_age` bounds the shipping
/// pause after any quarantined record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
#[non_exhaustive]
pub struct QuarantineRotationSpec {
    /// Finalize the quarantine segment once it reaches this size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<SizeSpec>,
    /// Finalize the quarantine segment after this many rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    /// Finalize the quarantine segment once it is this old, seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<u64>,
}

impl QuarantineRotationSpec {
    fn is_default(&self) -> bool {
        self.max_bytes.is_none() && self.max_rows.is_none() && self.max_age.is_none()
    }
}

impl Default for QuarantineSpec {
    fn default() -> Self {
        QuarantineSpec {
            format: QuarantineFormat::default(),
            max_disk: default_quarantine_disk(),
            rotation: QuarantineRotationSpec::default(),
        }
    }
}

/// How a dataset's payloads are decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DecodeFormat {
    /// A JSON object per message.
    Json,
}

/// Which messages belong to a dataset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct MatchSpec {
    /// The `id` of a source.
    pub source: String,
    /// A topic filter; for MQTT, `+` and `#` wildcards apply. Omit to match every message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

/// The Parquet field IDs a declared top-level field may use (ADR-0013, "PR13
/// implementation decisions"). The same range as `thadam-arrow`'s
/// `FIRST_DECLARED_ID..=LAST_DECLARED_ID`; a test in this crate keeps them equal.
pub const DECLARED_FIELD_ID_RANGE: RangeInclusive<u32> = 1000..=4_194_303;

/// The Parquet field IDs an identity level may use. The same range as
/// `thadam-arrow`'s `LEVEL_ID_RANGE`; a test in this crate keeps them equal.
pub const IDENTITY_LEVEL_ID_RANGE: RangeInclusive<u32> = 100..=199;

/// Reads a field ID: any integer that fits `u32`. The range that applies to
/// the position is checked afterwards, so a bad ID does not hide other errors.
fn de_id<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
    struct V;
    impl Visitor<'_> for V {
        type Value = u32;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a field ID, an integer from 0 to {}", u32::MAX)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u32, E> {
            u32::try_from(v).map_err(|_| E::invalid_value(de::Unexpected::Unsigned(v), &self))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u32, E> {
            u32::try_from(v).map_err(|_| E::invalid_value(de::Unexpected::Signed(v), &self))
        }
    }
    d.deserialize_u64(V)
}

/// One level of the asset path, such as
/// `site: { id: 100, template: "{topic.2}" }`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct IdentityLevel {
    /// The level name; becomes a column.
    pub name: String,
    /// The Parquet field ID of the level's column, in [`IDENTITY_LEVEL_ID_RANGE`]
    /// and unique within the dataset.
    pub id: u32,
    /// The template; `{topic.N}` is the Nth topic level (0-based).
    pub template: String,
}

/// The value written under an identity level's name.
#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "IdentityLevel")]
struct IdentityLevelBody {
    /// The stable Parquet field ID of the level's column (100 to 199, unique in the dataset).
    #[serde(deserialize_with = "de_id")]
    #[schemars(range(min = 100, max = 199))]
    id: u32,
    /// The template; `{topic.N}` is the Nth topic level (0-based).
    template: String,
}

/// Wraps [`IdentityLevelBody`] so a bare string (the pre-ID spelling) is
/// refused with a message that shows the new form.
struct LevelEntry(IdentityLevelBody);

impl<'de> Deserialize<'de> for LevelEntry {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = LevelEntry;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a mapping `{ id: <100..=199>, template: <string> }`")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<LevelEntry, A::Error> {
                IdentityLevelBody::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(LevelEntry)
            }
        }
        d.deserialize_any(V)
    }
}

fn de_identity<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<IdentityLevel>, D::Error> {
    let map = BTreeMap::<String, LevelEntry>::deserialize(d)?;
    Ok(map
        .into_iter()
        .map(|(name, LevelEntry(body))| IdentityLevel {
            name,
            id: body.id,
            template: body.template,
        })
        .collect())
}

fn ser_identity<S: Serializer>(levels: &[IdentityLevel], s: S) -> Result<S::Ok, S::Error> {
    let mut m = s.serialize_map(Some(levels.len()))?;
    for l in levels {
        let body = IdentityLevelBody {
            id: l.id,
            template: l.template.clone(),
        };
        m.serialize_entry(&l.name, &body)?;
    }
    m.end()
}

/// Identity from a stable key rather than the WAL position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EventIdSpec {
    /// Identity levels and declared fields whose values form the event ID.
    pub key: Vec<String>,
}

/// Best-effort message-level deduplication, before `explode`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DedupSpec {
    /// How long a key is remembered.
    pub window: DurationSpec,
    /// The most keys remembered.
    pub max_keys: CountSpec,
}

/// Splits one message into one event per element of a list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ExplodeSpec {
    /// A JSON Pointer to the list.
    pub path: String,
    /// A column that receives each element's index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_field: Option<String>,
}

/// The unit of a numeric source timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TimeUnit {
    /// Seconds.
    S,
    /// Milliseconds.
    Ms,
    /// Microseconds.
    Us,
    /// Nanoseconds.
    Ns,
    /// RFC 3339 text such as `2026-10-07T12:34:56+02:00`, not a number.
    Rfc3339,
}

impl TimeUnit {
    /// The unit as `thadam-transform` spells it: `s`, `ms`, `us`, `ns`, or
    /// `rfc3339`.
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeUnit::S => "s",
            TimeUnit::Ms => "ms",
            TimeUnit::Us => "us",
            TimeUnit::Ns => "ns",
            TimeUnit::Rfc3339 => "rfc3339",
        }
    }
}

/// How the source timestamp is read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TimeSpec {
    /// A JSON Pointer to the timestamp.
    pub field: String,
    /// The unit of the number, or `rfc3339` for text.
    pub unit: TimeUnit,
    /// The clock the device's time is on.
    pub domain: TimeDomainSpec,
    /// Beyond this distance from ingest time, the event is flagged `CLOCK_SKEW`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_skew: Option<DurationSpec>,
    /// The UTC offset assumed for `rfc3339` text that carries none, such as
    /// `+02:00`. Only valid with `unit: rfc3339`; without it, offset-less text
    /// is a runtime reject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_offset: Option<String>,
}

/// One declared field of a dataset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FieldSpec {
    /// The column name.
    pub name: String,
    /// The stable Parquet field ID (1000 to 4194303, unique in the dataset). Never reuse the ID of a removed field.
    #[serde(deserialize_with = "de_id")]
    #[schemars(range(min = 1000, max = 4194303))]
    pub id: u32,
    /// The declared type.
    #[serde(rename = "type")]
    pub ty: FieldTypeSpec,
    /// A JSON Pointer into the payload; `^/` reads from the parent message of an exploded sample.
    pub from: String,
    /// Reject the event when the field is missing or null.
    #[serde(default)]
    pub required: bool,
    /// The unit of the source value (a UCUM code such as `mV`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Convert to this unit; requires `unit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convert_to: Option<String>,
}

impl FieldSpec {
    /// The `thadam-core` field definition: `required` makes it non-nullable,
    /// the unit is the one the stored value is in (`convert_to` if set), and
    /// `field_id` is the declared `id`.
    pub fn to_field_def(&self) -> FieldDef {
        let mut def =
            FieldDef::new(self.name.as_str(), self.ty.field_type().clone()).with_field_id(self.id);
        if let Some(unit) = self.convert_to.as_ref().or(self.unit.as_ref()) {
            def = def.with_unit(unit.as_str());
        }
        if self.required {
            def = def.required();
        }
        def
    }
}

/// An output table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DatasetSpec {
    /// A unique identifier; used in paths, object keys, and metric labels.
    pub id: String,
    /// Which messages feed the dataset.
    #[serde(rename = "match")]
    pub match_: MatchSpec,
    /// How payloads are decoded.
    pub decode: DecodeFormat,
    /// The asset path levels, in the order written.
    #[serde(
        default,
        deserialize_with = "de_identity",
        serialize_with = "ser_identity"
    )]
    #[schemars(with = "BTreeMap<String, IdentityLevelBody>")]
    pub identity: Vec<IdentityLevel>,
    /// A content-derived event ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<EventIdSpec>,
    /// Message-level deduplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedup: Option<DedupSpec>,
    /// Message-to-events splitting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explode: Option<ExplodeSpec>,
    /// Source timestamp extraction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<TimeSpec>,
    /// The declared schema.
    pub fields: Vec<FieldSpec>,
}

impl DatasetSpec {
    /// The declared fields as `thadam-core` definitions, in declaration order,
    /// each carrying its explicit `field_id`.
    pub fn field_defs(&self) -> Vec<FieldDef> {
        self.fields.iter().map(FieldSpec::to_field_def).collect()
    }

    /// The identity levels as `(name, field ID)` pairs, in the order written.
    ///
    /// Pass each pair to `thadam_arrow::DatasetSchemaBuilder::level`, in this
    /// order, together with [`DatasetSpec::field_defs`].
    pub fn level_ids(&self) -> Vec<(&str, u32)> {
        self.identity
            .iter()
            .map(|l| (l.name.as_str(), l.id))
            .collect()
    }
}

/// The quality status a failed `validate` rule assigns.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RuleStatus {
    /// `good`
    Good,
    /// `uncertain`
    Uncertain,
    /// `bad`
    Bad,
}

impl RuleStatus {
    /// The `thadam-core` status.
    pub fn quality_status(self) -> QualityStatus {
        match self {
            RuleStatus::Good => QualityStatus::Good,
            RuleStatus::Uncertain => QualityStatus::Uncertain,
            RuleStatus::Bad => QualityStatus::Bad,
        }
    }
}

/// A per-field range rule of the `validate` transform.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ValidateRule {
    /// A numeric field, or a list of numbers (every element must be in range).
    pub field: String,
    /// The inclusive lower bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// The inclusive upper bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
    /// The quality flag raised on violation, such as `OUT_OF_RANGE`.
    pub flag: String,
    /// The status assigned on violation.
    pub status: RuleStatus,
}

/// A field `on_change` watches, with an optional absolute deadband.
///
/// Written as a bare field name or as `{ field, deadband }`. A deadband of zero
/// serializes as the bare name, so both spellings of the same step hash alike.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct WatchField {
    /// The watched field.
    pub field: String,
    /// A numeric change must exceed this to count; zero when absent.
    pub deadband: f64,
}

impl WatchField {
    /// A watch on `field` with the given deadband.
    pub fn new(field: impl Into<String>, deadband: f64) -> Self {
        WatchField {
            field: field.into(),
            deadband,
        }
    }
}

/// The keys of the mapping form. Refusing an unknown key while reading the key
/// itself (as derived code does) keeps it in the error's path.
enum WatchKey {
    Field,
    Deadband,
}

impl<'de> Deserialize<'de> for WatchKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = WatchKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`field` or `deadband`")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<WatchKey, E> {
                match v {
                    "field" => Ok(WatchKey::Field),
                    "deadband" => Ok(WatchKey::Deadband),
                    other => Err(E::unknown_field(other, &["field", "deadband"])),
                }
            }
        }
        d.deserialize_identifier(V)
    }
}

impl<'de> Deserialize<'de> for WatchField {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = WatchField;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a field name, or a mapping `{ field: <name>, deadband: <number> }`")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<WatchField, E> {
                Ok(WatchField::new(v, 0.0))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<WatchField, A::Error> {
                let mut field: Option<String> = None;
                let mut deadband: Option<f64> = None;
                while let Some(key) = map.next_key::<WatchKey>()? {
                    match key {
                        WatchKey::Field => {
                            if field.is_some() {
                                return Err(de::Error::duplicate_field("field"));
                            }
                            field = Some(map.next_value()?);
                        }
                        WatchKey::Deadband => {
                            if deadband.is_some() {
                                return Err(de::Error::duplicate_field("deadband"));
                            }
                            deadband = Some(map.next_value()?);
                        }
                    }
                }
                let field = field.ok_or_else(|| de::Error::missing_field("field"))?;
                Ok(WatchField::new(field, deadband.unwrap_or(0.0)))
            }
        }
        d.deserialize_any(V)
    }
}

impl Serialize for WatchField {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.deadband == 0.0 {
            return s.serialize_str(&self.field);
        }
        let mut m = s.serialize_map(Some(2))?;
        m.serialize_entry("field", &self.field)?;
        m.serialize_entry("deadband", &self.deadband)?;
        m.end()
    }
}

impl JsonSchema for WatchField {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "WatchField".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "A field name, or `{ field, deadband }`: the field is passed on only when it moved by more than the deadband (numeric fields only; default 0).",
            "oneOf": [
                { "type": "string" },
                {
                    "type": "object",
                    "properties": {
                        "field": { "type": "string" },
                        "deadband": { "type": "number", "minimum": 0 }
                    },
                    "required": ["field"],
                    "additionalProperties": false
                }
            ]
        })
    }
}

/// What `enrich` does when the key has no row in the table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnMissSpec {
    /// The added fields are null.
    #[default]
    Null,
    /// The event is rejected.
    Reject,
    /// The event is dropped and counted.
    Drop,
}

/// The file format of an `enrich` table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EnrichFormat {
    /// A CSV file with a header row.
    Csv,
    /// A YAML list of rows, or a mapping from key to row.
    Yaml,
}

/// The join between an event and an `enrich` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EnrichKeySpec {
    /// The event's field or identity level to look up; a string or integer.
    pub field: String,
    /// The table column that holds the key.
    pub column: String,
}

/// A field `enrich` adds from a table column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EnrichFieldSpec {
    /// The new field's name.
    pub name: String,
    /// The new field's type; a `ts` field needs `unit`.
    #[serde(rename = "type")]
    pub ty: FieldTypeSpec,
    /// The table column to read; the field name when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// The unit of the value (a UCUM code such as `mV`); for a `ts` field, the
    /// epoch unit (`s`, `ms`, `us`, `ns`) or `rfc3339`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

fn is_default<T: Default + PartialEq>(v: &T) -> bool {
    *v == T::default()
}

/// A step applied to one dataset.
///
/// Every step takes the `dataset` it applies to and an optional `sinks` list
/// restricting it to those sinks (open item O2; absent means all sinks).
/// Option names and meanings match the steps of `thadam-transform`.
#[derive(Clone, Debug, PartialEq, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum TransformSpec {
    /// Assigns quality flags and status from per-field rules.
    Validate {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// The rules.
        rules: Vec<ValidateRule>,
    },
    /// Passes an event only when selected fields changed, or a heartbeat is due.
    OnChange {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// The fields compared with the previous event, each with an optional
        /// deadband.
        fields: Vec<WatchField>,
        /// An event is passed at least this often.
        #[serde(skip_serializing_if = "Option::is_none")]
        heartbeat: Option<DurationSpec>,
    },
    /// Drops events that do not match the predicate.
    Filter {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// The predicate; events for which it is false are dropped and counted.
        #[serde(rename = "where")]
        where_: Predicate,
    },
    /// Keeps every Nth event and/or events at least an interval apart, per asset.
    Sample {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// Keeps an asset's first event and every Nth after it; at least 1.
        #[serde(skip_serializing_if = "Option::is_none")]
        every_n: Option<CountSpec>,
        /// Keeps an event only when this much ingest time passed since the
        /// asset's last kept event; greater than zero.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_interval: Option<DurationSpec>,
    },
    /// Adds fields from a static CSV or YAML table.
    Enrich {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// The table file; a relative path resolves against the config file's
        /// directory. It is read when the transform chain is compiled, not
        /// when the configuration is validated.
        file: String,
        /// The table format; inferred from the file extension when absent.
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<EnrichFormat>,
        /// The join key.
        key: EnrichKeySpec,
        /// The fields to add.
        add: Vec<EnrichFieldSpec>,
        /// What happens when the key has no row; `null` when absent.
        #[serde(default, skip_serializing_if = "is_default")]
        on_miss: OnMissSpec,
    },
    /// Renames fields without touching values.
    Rename {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// Old field name to new field name.
        fields: BTreeMap<String, String>,
    },
    /// Keeps only the listed fields, in the listed order.
    Project {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// The fields to keep.
        fields: Vec<String>,
    },
    /// Widens field types; narrowing is an error.
    Cast {
        /// The dataset the step applies to.
        dataset: String,
        /// Restricts the step to these sink ids.
        #[serde(skip_serializing_if = "Option::is_none")]
        sinks: Option<Vec<String>>,
        /// Field name to the wider type.
        fields: BTreeMap<String, FieldTypeSpec>,
    },
}

impl TransformSpec {
    /// The dataset the step applies to.
    pub fn dataset(&self) -> &str {
        match self {
            TransformSpec::Validate { dataset, .. }
            | TransformSpec::OnChange { dataset, .. }
            | TransformSpec::Filter { dataset, .. }
            | TransformSpec::Sample { dataset, .. }
            | TransformSpec::Enrich { dataset, .. }
            | TransformSpec::Rename { dataset, .. }
            | TransformSpec::Project { dataset, .. }
            | TransformSpec::Cast { dataset, .. } => dataset,
        }
    }

    /// The sinks the step is restricted to; `None` means every sink.
    pub fn sinks(&self) -> Option<&[String]> {
        match self {
            TransformSpec::Validate { sinks, .. }
            | TransformSpec::OnChange { sinks, .. }
            | TransformSpec::Filter { sinks, .. }
            | TransformSpec::Sample { sinks, .. }
            | TransformSpec::Enrich { sinks, .. }
            | TransformSpec::Rename { sinks, .. }
            | TransformSpec::Project { sinks, .. }
            | TransformSpec::Cast { sinks, .. } => sinks.as_deref(),
        }
    }

    /// The `kind` spelling.
    pub fn kind(&self) -> &'static str {
        match self {
            TransformSpec::Validate { .. } => "validate",
            TransformSpec::OnChange { .. } => "on_change",
            TransformSpec::Filter { .. } => "filter",
            TransformSpec::Sample { .. } => "sample",
            TransformSpec::Enrich { .. } => "enrich",
            TransformSpec::Rename { .. } => "rename",
            TransformSpec::Project { .. } => "project",
            TransformSpec::Cast { .. } => "cast",
        }
    }
}

/// Process-level settings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RuntimeSpec {
    /// Tokio worker threads; the runtime default when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_threads: Option<CountSpec>,
    /// `thadam validate` warns when the estimate of Thadam-owned memory exceeds this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_target: Option<SizeSpec>,
}

/// The log output format: `json` or `pretty`. The older spelling `text` is
/// still read as a deprecated alias for `pretty` (validation warns); the
/// serialized, and therefore hashed, form is always `pretty`, so a config
/// that spells it `text` hashes as if it said `pretty` from this version on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogFormat {
    /// One JSON object per line.
    #[default]
    Json,
    /// Human-readable output.
    Pretty,
}

impl<'de> Deserialize<'de> for LogFormat {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        match s.as_str() {
            "json" => Ok(LogFormat::Json),
            "pretty" | "text" => Ok(LogFormat::Pretty),
            other => Err(de::Error::unknown_variant(other, &["json", "pretty"])),
        }
    }
}

/// The minimum log level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogLevel {
    /// `trace`
    Trace,
    /// `debug`
    Debug,
    /// `info`
    #[default]
    Info,
    /// `warn`
    Warn,
    /// `error`
    Error,
}

/// Log settings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LogsSpec {
    /// The output format.
    #[serde(default)]
    pub format: LogFormat,
    /// The minimum level.
    #[serde(default)]
    pub level: LogLevel,
}

fn default_admin_bind() -> String {
    "127.0.0.1:9464".to_owned()
}

/// The admin API (`/healthz`, `/readyz`, `/metrics`, `/status`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AdminSpec {
    /// The listen address; a non-loopback address requires `token_file`.
    #[serde(default = "default_admin_bind")]
    pub bind: String,
    /// A file holding the bearer token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
}

impl Default for AdminSpec {
    fn default() -> Self {
        AdminSpec {
            bind: default_admin_bind(),
            token_file: None,
        }
    }
}

/// Logs and the admin API.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ObservabilitySpec {
    /// Log settings.
    #[serde(default)]
    pub logs: LogsSpec,
    /// The admin API.
    #[serde(default)]
    pub admin: AdminSpec,
}
