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

/// A sink has the same shape as a source.
pub type SinkSpec = ConnectorSpec;

impl ConnectorSpec {
    /// `config` with every interpolated value replaced by `<redacted>`.
    pub fn redacted_config(&self) -> serde_json::Value {
        let mut cfg = self.config.clone();
        for ptr in &self.secret_paths {
            if let Some(slot) = cfg.pointer_mut(ptr) {
                *slot = serde_json::Value::String(REDACTED.to_owned());
            }
        }
        cfg
    }

    /// The interpolated string values, as `(pointer, secret)` pairs.
    pub fn secrets(&self) -> Vec<(String, Secret<String>)> {
        self.secret_paths
            .iter()
            .filter_map(|ptr| {
                let s = self.config.pointer(ptr)?.as_str()?;
                Some((ptr.clone(), Secret::new(s.to_owned())))
            })
            .collect()
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
}

fn default_quarantine_disk() -> SizeSpec {
    SizeSpec(256 << 20)
}

impl Default for QuarantineSpec {
    fn default() -> Self {
        QuarantineSpec {
            format: QuarantineFormat::default(),
            max_disk: default_quarantine_disk(),
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
}

/// How the source timestamp is read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TimeSpec {
    /// A JSON Pointer to the timestamp.
    pub field: String,
    /// The unit of the number.
    pub unit: TimeUnit,
    /// The clock the device's time is on.
    pub domain: TimeDomainSpec,
    /// Beyond this distance from ingest time, the event is flagged `CLOCK_SKEW`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_skew: Option<DurationSpec>,
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

/// A step applied to one dataset.
///
/// Every step takes the `dataset` it applies to and an optional `sinks` list
/// restricting it to those sinks (open item O2; absent means all sinks).
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
        /// The fields compared with the previous event.
        fields: Vec<String>,
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
}

impl TransformSpec {
    /// The dataset the step applies to.
    pub fn dataset(&self) -> &str {
        match self {
            TransformSpec::Validate { dataset, .. }
            | TransformSpec::OnChange { dataset, .. }
            | TransformSpec::Filter { dataset, .. } => dataset,
        }
    }

    /// The sinks the step is restricted to; `None` means every sink.
    pub fn sinks(&self) -> Option<&[String]> {
        match self {
            TransformSpec::Validate { sinks, .. }
            | TransformSpec::OnChange { sinks, .. }
            | TransformSpec::Filter { sinks, .. } => sinks.as_deref(),
        }
    }

    /// The `kind` spelling.
    pub fn kind(&self) -> &'static str {
        match self {
            TransformSpec::Validate { .. } => "validate",
            TransformSpec::OnChange { .. } => "on_change",
            TransformSpec::Filter { .. } => "filter",
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

/// The log output format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogFormat {
    /// One JSON object per line.
    #[default]
    Json,
    /// Human-readable text.
    Text,
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
