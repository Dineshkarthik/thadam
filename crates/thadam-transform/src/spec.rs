//! The chain configuration, deserializable from a `serde_json::Value` so the
//! runtime can build it from the parsed pipeline config (O1).
//!
//! Key spellings follow the demo pipeline. A filter is `kind: filter` with a
//! structured `where:`; `expr:` is reserved for a future expression language
//! (ADR-0018) and refused.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use thadam_core::{ErrorDomain, FieldType, ThadamError, codes};

/// Everything one chain needs: its datasets and the `transforms:` list.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ChainSpec {
    /// The datasets, in declaration order.
    #[serde(default)]
    pub datasets: Vec<DatasetSpec>,
    /// Transform steps in listed order; each applies to the datasets its
    /// `dataset:` selector names.
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,
    /// Directory relative enrichment paths resolve against. Not part of the
    /// config text; the runtime sets it to the config file's directory.
    #[serde(skip)]
    pub base_dir: Option<PathBuf>,
    /// The pipeline's schema-store directory, where dataset `decode: ros2`
    /// resolves schema text. Not part of the config text; the runtime sets
    /// it to `<data_dir>/schemas` (see [`thadam_core::SchemaStore`]).
    #[serde(skip)]
    pub schema_dir: Option<PathBuf>,
    /// The Sparkplug birth-store directory for dataset `decode: sparkplug_b`
    /// ([`thadam_core::BirthStore`]). Not part of the config text; the runtime
    /// sets it to `<data_dir>/sparkplug/births/<sink>`.
    #[serde(skip)]
    pub births_dir: Option<PathBuf>,
    /// Seed from the birth store but never write it (replay).
    #[serde(skip)]
    pub births_read_only: bool,
}

impl ChainSpec {
    /// Builds a spec from the `datasets` and `transforms` of a parsed config.
    pub fn from_value(value: serde_json::Value) -> Result<ChainSpec, ThadamError> {
        serde_json::from_value(value).map_err(|e| {
            ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, e.to_string())
        })
    }

    /// Sets the directory relative enrichment paths resolve against.
    #[must_use]
    pub fn with_base_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.base_dir = Some(dir.into());
        self
    }

    /// Sets the schema-store directory `decode: ros2` datasets resolve
    /// schema text from.
    #[must_use]
    pub fn with_schema_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.schema_dir = Some(dir.into());
        self
    }

    /// Sets the birth-store directory `decode: sparkplug_b` datasets seed
    /// from and write BIRTH/DEATH state to.
    #[must_use]
    pub fn with_births_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.births_dir = Some(dir.into());
        self
    }

    /// Makes the birth store seed-only: the chain reads it at build and
    /// never writes it. For replay, which must not overwrite production
    /// seeds with old BIRTHs.
    #[must_use]
    pub fn with_births_read_only(mut self) -> Self {
        self.births_read_only = true;
        self
    }
}

/// How a dataset selects messages.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct MatchSpec {
    /// The source id; any source if absent.
    #[serde(default)]
    pub source: Option<String>,
    /// An MQTT topic filter over the `topic` attribute; any topic if absent.
    #[serde(default)]
    pub topic: Option<String>,
    /// Further attributes that must equal the given text.
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

/// The payload decoder.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DecodeSpec {
    /// A JSON document.
    #[default]
    Json,
    /// A ROS 2 CDR payload from an MCAP source. The message must carry
    /// `mcap.schema_hash`; the schema text resolves from the schema store
    /// the runtime pointed the chain at ([`ChainSpec::schema_dir`]), and
    /// the decoded record's flattened dotted field names
    /// (`header.stamp`, `pose.position.x`, …) become the payload keys.
    Ros2,
    /// A Sparkplug B protobuf. The chain keeps one stateful decoder seeded
    /// from the birth store ([`ChainSpec::births_dir`]); each message is read
    /// as the JSON document `crate::sparkplug` builds (`group`, `node`,
    /// `device`, `kind`, `seq`, `bd_seq`, `timestamp`, `metrics`, and
    /// `samples` for an `explode` dataset).
    SparkplugB,
}

/// Identity levels in hierarchy order, each a template such as `{topic.2}`.
///
/// Written as a mapping or as a list of single-entry mappings. A mapping is
/// read in the order the deserializer yields its keys, which is alphabetical
/// for `serde_json::Value`; use the list form when hierarchy order matters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IdentitySpec(pub Vec<(String, String)>);

impl<'de> Deserialize<'de> for IdentitySpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = IdentitySpec;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a mapping, or a list of single-entry mappings, of level to template")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut levels = Vec::new();
                while let Some((k, v)) = map.next_entry::<String, String>()? {
                    levels.push((k, v));
                }
                Ok(IdentitySpec(levels))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut levels = Vec::new();
                while let Some(entry) = seq.next_element::<BTreeMap<String, String>>()? {
                    let mut it = entry.into_iter();
                    match (it.next(), it.next()) {
                        (Some(kv), None) => levels.push(kv),
                        _ => {
                            return Err(de::Error::custom(
                                "each identity list entry must have exactly one level",
                            ));
                        }
                    }
                }
                Ok(IdentitySpec(levels))
            }
        }
        d.deserialize_any(V)
    }
}

/// Content-derived event ids.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EventIdSpec {
    /// Identity levels and declared fields whose values form the id.
    pub key: Vec<String>,
}

/// A duration written as text such as `500ms`, `5s`, `10m`, `1h`, or `1d`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

/// Parses `<digits><unit>` with unit `ns`, `us`, `ms`, `s`, `m`, `h`, or `d`.
pub(crate) fn parse_duration(s: &str) -> Option<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = s.split_at(split);
    let n: u64 = digits.parse().ok()?;
    let nanos_per = match unit {
        "ns" => 1u64,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60 * 1_000_000_000,
        "h" => 3_600 * 1_000_000_000,
        "d" => 86_400 * 1_000_000_000,
        _ => return None,
    };
    let nanos = u128::from(n) * u128::from(nanos_per);
    let secs = u64::try_from(nanos / 1_000_000_000).ok()?;
    Some(Duration::new(secs, (nanos % 1_000_000_000) as u32))
}

impl<'de> Deserialize<'de> for HumanDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_duration(&s).map(HumanDuration).ok_or_else(|| {
            de::Error::custom(format!(
                "invalid duration `{s}`; use e.g. 500ms, 5s, 10m, 1h"
            ))
        })
    }
}

/// A count written as an integer or as text such as `500k` or `2M`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Count(pub usize);

pub(crate) fn parse_count(s: &str) -> Option<usize> {
    let (digits, mult) = match s.as_bytes().last()? {
        b'k' | b'K' => (&s[..s.len() - 1], 1_000usize),
        b'm' | b'M' => (&s[..s.len() - 1], 1_000_000),
        b'g' | b'G' => (&s[..s.len() - 1], 1_000_000_000),
        _ => (s, 1),
    };
    digits.parse::<usize>().ok()?.checked_mul(mult)
}

impl<'de> Deserialize<'de> for Count {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Count;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a count such as 500000 or 500k")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Count, E> {
                usize::try_from(v)
                    .map(Count)
                    .map_err(|_| E::custom("count too large"))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Count, E> {
                u64::try_from(v)
                    .map_err(|_| E::custom("count must not be negative"))
                    .and_then(|v| self.visit_u64(v))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Count, E> {
                parse_count(v)
                    .map(Count)
                    .ok_or_else(|| E::custom(format!("invalid count `{v}`")))
            }
        }
        d.deserialize_any(V)
    }
}

/// Message-level deduplication, before `explode`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DedupSpec {
    /// How long a key is remembered, in ingest time.
    pub window: HumanDuration,
    /// Most keys remembered; the oldest is evicted past this.
    pub max_keys: Count,
}

/// Turns an array in the message into one event per element.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ExplodeSpec {
    /// JSON Pointer to the array.
    pub path: String,
    /// The name of the sample-index column. It is carried as the event's
    /// sample index, not as a record field.
    pub index_field: String,
}

/// Where a dataset's source time lives and how to read it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TimeSpec {
    /// JSON Pointer to the time; `^/` reads from the parent message. Exactly
    /// one of `field` or `attr` is set.
    #[serde(default)]
    pub field: Option<String>,
    /// Message attribute holding the time as text. Exactly one of `field` or
    /// `attr` is set.
    #[serde(default)]
    pub attr: Option<String>,
    /// `s`, `ms`, `us`, `ns` for epoch numbers, or `rfc3339` for text.
    pub unit: String,
    /// `utc`, `tai`, `monotonic`, `sim`, or `unknown`. Only `utc` becomes the
    /// event time; the default is `unknown` so a mapping is always explicit.
    #[serde(default)]
    pub domain: Option<String>,
    /// Flags `CLOCK_SKEW` when a UTC source time differs from the ingest time
    /// by more than this.
    #[serde(default)]
    pub max_skew: Option<HumanDuration>,
    /// The UTC offset assumed for `rfc3339` text that carries none, such as
    /// `+02:00`. Without it, such text is a reject.
    #[serde(default)]
    pub default_offset: Option<String>,
}

/// One declared output field.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FieldSpec {
    /// The column name.
    pub name: String,
    /// The declared type: `bool`, `i8` to `i64`, `u8` to `u64`, `f32`, `f64`,
    /// `str`, `ts`, or `list<T>`.
    #[serde(rename = "type")]
    pub ty: String,
    /// JSON Pointer to the value; defaults to `/<name>`. `^/` reads from the
    /// parent message of an exploded sample.
    #[serde(default)]
    pub from: Option<String>,
    /// A missing or null value rejects the message.
    #[serde(default)]
    pub required: bool,
    /// The UCUM code of the source value; for a `ts` field, the epoch unit.
    #[serde(default)]
    pub unit: Option<String>,
    /// The UCUM code to convert to. The field's declared unit is this one.
    #[serde(default)]
    pub convert_to: Option<String>,
}

/// A dataset: how to route, decode, identify, and shape messages.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DatasetSpec {
    /// The dataset id.
    pub id: String,
    /// Which messages belong to it.
    #[serde(rename = "match", default)]
    pub matcher: MatchSpec,
    /// The payload decoder.
    #[serde(default)]
    pub decode: DecodeSpec,
    /// Identity levels.
    #[serde(default)]
    pub identity: IdentitySpec,
    /// Content-derived event ids; without it ids come from the WAL position.
    #[serde(default)]
    pub event_id: Option<EventIdSpec>,
    /// Deduplication; requires `event_id`.
    #[serde(default)]
    pub dedup: Option<DedupSpec>,
    /// Explodes an array into events.
    #[serde(default)]
    pub explode: Option<ExplodeSpec>,
    /// Source-time extraction.
    #[serde(default)]
    pub time: Option<TimeSpec>,
    /// The declared fields.
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
}

/// The datasets a transform applies to: one id, a list, or all if omitted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DatasetSelector(pub Option<Vec<String>>);

impl<'de> Deserialize<'de> for DatasetSelector {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(String),
            Many(Vec<String>),
        }
        Ok(DatasetSelector(Option::<OneOrMany>::deserialize(d)?.map(
            |v| match v {
                OneOrMany::One(s) => vec![s],
                OneOrMany::Many(v) => v,
            },
        )))
    }
}

/// A literal in a predicate or rule.
///
/// Integers keep full `i64` and `u64` range, so a `u64` above `i64::MAX` is
/// exact. Typing against the field happens at compile time.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Lit {
    /// `true` or `false`.
    Bool(bool),
    /// A negative or small integer.
    Int(i64),
    /// An integer above `i64::MAX`.
    UInt(u64),
    /// A number with a fraction or exponent.
    Float(f64),
    /// Text.
    Str(String),
}

impl<'de> Deserialize<'de> for Lit {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Lit;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a boolean, number, or string")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Lit, E> {
                Ok(Lit::Bool(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Lit, E> {
                Ok(Lit::Int(v))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Lit, E> {
                Ok(i64::try_from(v).map_or(Lit::UInt(v), Lit::Int))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Lit, E> {
                Ok(Lit::Float(v))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Lit, E> {
                Ok(Lit::Str(v.to_owned()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Lit, E> {
                Ok(Lit::Str(v))
            }
        }
        d.deserialize_any(V)
    }
}

/// A structured predicate: a combinator (`all`, `any`, `not`) or a leaf
/// (`field` plus comparisons). Exactly one of the four heads is required.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PredicateSpec {
    /// True when every child is.
    #[serde(default)]
    pub all: Option<Vec<PredicateSpec>>,
    /// True when any child is.
    #[serde(default)]
    pub any: Option<Vec<PredicateSpec>>,
    /// True when the child is not.
    #[serde(default)]
    pub not: Option<Box<PredicateSpec>>,
    /// The field a leaf tests: a dataset field, an identity level,
    /// `quality.status`, or `quality.flags`.
    #[serde(default)]
    pub field: Option<String>,
    /// Equal to.
    #[serde(default)]
    pub eq: Option<Lit>,
    /// Not equal to.
    #[serde(default)]
    pub ne: Option<Lit>,
    /// Less than.
    #[serde(default)]
    pub lt: Option<Lit>,
    /// Less than or equal to.
    #[serde(default)]
    pub le: Option<Lit>,
    /// Greater than.
    #[serde(default)]
    pub gt: Option<Lit>,
    /// Greater than or equal to.
    #[serde(default)]
    pub ge: Option<Lit>,
    /// One of the listed literals.
    #[serde(default, rename = "in")]
    pub in_: Option<Vec<Lit>>,
    /// At least this, inclusive; on a list field, every element.
    #[serde(default)]
    pub min: Option<Lit>,
    /// At most this, inclusive; on a list field, every element.
    #[serde(default)]
    pub max: Option<Lit>,
    /// `true` tests that the value is null or missing, `false` that it is not.
    #[serde(default)]
    pub null: Option<bool>,
}

/// One `validate` rule: a range check that raises a flag and a status.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RuleSpec {
    /// The dataset field to check.
    pub field: String,
    /// Smallest allowed value, inclusive.
    #[serde(default)]
    pub min: Option<Lit>,
    /// Largest allowed value, inclusive.
    #[serde(default)]
    pub max: Option<Lit>,
    /// The quality flag to raise, such as `OUT_OF_RANGE`.
    #[serde(default)]
    pub flag: Option<String>,
    /// The status to raise to: `good`, `uncertain`, or `bad`.
    #[serde(default)]
    pub status: Option<String>,
}

/// `validate`: flags events whose fields break a rule.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ValidateSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The rules.
    pub rules: Vec<RuleSpec>,
}

fn default_pose_prefix() -> String {
    "pose".to_owned()
}

fn default_frame_field() -> Option<String> {
    Some("header.frame_id".to_owned())
}

fn default_quaternion_tolerance() -> f64 {
    1e-3
}

/// `pose`: flags malformed poses — missing or non-finite components, a
/// quaternion whose norm drifts from 1, or an absent frame id. The pose is
/// read from the flattened dotted fields the ROS 2 decoder writes, following
/// the robotics conventions (`docs/concepts/robotics-conventions.md`).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PoseSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The prefix of the flattened pose: the step reads `<prefix>.position.{x,y,z}`
    /// and `<prefix>.orientation.{x,y,z,w}`.
    #[serde(default = "default_pose_prefix")]
    pub prefix: String,
    /// The field holding the frame id. Setting it to `null` skips the frame
    /// check; omitting it checks `header.frame_id`.
    #[serde(default = "default_frame_field")]
    pub frame_field: Option<String>,
    /// How far the quaternion norm may drift from 1 before the pose is
    /// flagged `OUT_OF_RANGE`; greater than 0 and less than 1.
    #[serde(default = "default_quaternion_tolerance")]
    pub quaternion_tolerance: f64,
}

fn default_name_field() -> String {
    "name".to_owned()
}

fn default_position_field() -> String {
    "position".to_owned()
}

fn default_velocity_field() -> String {
    "velocity".to_owned()
}

fn default_effort_field() -> String {
    "effort".to_owned()
}

/// `joint_state`: flags malformed joint-state arrays — a length mismatch
/// against the joint names, duplicate names, or non-finite values. Reads the
/// flattened fields the ROS 2 decoder writes for `sensor_msgs/JointState`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct JointStateSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The list-of-strings field holding the joint names.
    #[serde(default = "default_name_field")]
    pub name_field: String,
    /// The float-array field holding the joint positions.
    #[serde(default = "default_position_field")]
    pub position_field: String,
    /// The float-array field holding the joint velocities.
    #[serde(default = "default_velocity_field")]
    pub velocity_field: String,
    /// The float-array field holding the joint efforts.
    #[serde(default = "default_effort_field")]
    pub effort_field: String,
}

/// A field `on_change` watches, with an optional absolute deadband.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchSpec {
    /// The field name.
    pub field: String,
    /// A numeric change must exceed this to count; default 0.
    pub deadband: f64,
}

impl<'de> Deserialize<'de> for WatchSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full {
            field: String,
            #[serde(default)]
            deadband: f64,
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Name(String),
            Full(Full),
        }
        Ok(match Either::deserialize(d)? {
            Either::Name(field) => WatchSpec {
                field,
                deadband: 0.0,
            },
            Either::Full(f) => WatchSpec {
                field: f.field,
                deadband: f.deadband,
            },
        })
    }
}

/// `on_change`: keeps an event only when a watched field changed, or when the
/// heartbeat elapsed. Lossy by design; it is not deduplication.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct OnChangeSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The watched fields: names, or `{field, deadband}`.
    pub fields: Vec<WatchSpec>,
    /// Keeps an event once this much ingest time passed since the last one
    /// kept, even if nothing changed.
    #[serde(default)]
    pub heartbeat: Option<HumanDuration>,
}

/// `filter`: drops events that do not satisfy the predicate.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FilterSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The structured predicate.
    #[serde(rename = "where", default)]
    pub where_: Option<Box<PredicateSpec>>,
    /// Reserved for a future expression language; setting it is an error.
    #[serde(default)]
    pub expr: Option<serde_json::Value>,
}

/// `sample`: keeps every Nth event and/or events at least an interval apart,
/// per asset, in ingest order.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SampleSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// Keeps the first event and every Nth after it.
    #[serde(default)]
    pub every_n: Option<u64>,
    /// Keeps an event only when this much ingest time passed since the last
    /// one kept.
    #[serde(default)]
    pub min_interval: Option<HumanDuration>,
}

/// What `enrich` does when the key has no row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OnMiss {
    /// The added fields are null.
    #[default]
    Null,
    /// The event is rejected.
    Reject,
    /// The event is dropped.
    Drop,
}

/// The join key of an enrichment table.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EnrichKeySpec {
    /// The event's field or identity level to look up.
    pub field: String,
    /// The table column holding the key.
    pub column: String,
}

/// A field `enrich` adds from a table column.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EnrichFieldSpec {
    /// The new field name.
    pub name: String,
    /// Its declared type.
    #[serde(rename = "type")]
    pub ty: String,
    /// The table column; defaults to the field name.
    #[serde(default)]
    pub column: Option<String>,
    /// Its unit, if any.
    #[serde(default)]
    pub unit: Option<String>,
}

/// `enrich`: adds fields from a static CSV or YAML table. The file is read at
/// compile time and its SHA-256 is recorded for lineage.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EnrichSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The table file; relative paths resolve against the chain's base dir.
    pub file: String,
    /// `csv` or `yaml`; inferred from the extension if absent.
    #[serde(default)]
    pub format: Option<String>,
    /// The join key.
    pub key: EnrichKeySpec,
    /// The fields to add.
    pub add: Vec<EnrichFieldSpec>,
    /// What to do when the key has no row.
    #[serde(default)]
    pub on_miss: OnMiss,
}

/// `rename`: renames fields without touching values.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RenameSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// Old name to new name.
    pub fields: BTreeMap<String, String>,
}

/// `project`: keeps only the listed fields, in the listed order.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ProjectSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The fields to keep.
    pub fields: Vec<String>,
}

/// `cast`: widens field types. Narrowing is a compile error.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CastSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// Field name to the wider type.
    pub fields: BTreeMap<String, String>,
}

/// How `window` reduces the values of one field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AggFn {
    /// How many non-null values.
    Count,
    /// Their sum.
    Sum,
    /// The smallest.
    Min,
    /// The largest.
    Max,
    /// The mean.
    Mean,
    /// The earliest value in the window.
    First,
    /// The latest value in the window.
    Last,
    /// The population standard deviation.
    Stddev,
    /// `max - min`.
    Range,
    /// `last - first`.
    Delta,
    /// The root mean square.
    Rms,
}

/// One aggregate of a `window`: an output column over one input field.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AggregateSpec {
    /// The input field.
    pub field: String,
    /// The reduction.
    #[serde(rename = "fn")]
    pub func: AggFn,
    /// The output column.
    #[serde(rename = "as")]
    pub name: String,
}

/// Which timestamp a `window` step assigns events to windows and advances the
/// watermark by.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WindowTime {
    /// WAL ingest time.
    #[default]
    Ingest,
    /// UTC event time (`Times::event_time()`).
    Event,
}

/// `window`: tumbling windows on ingest or event time, one aggregate event per
/// asset and window.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct WindowSpec {
    /// The datasets it applies to.
    #[serde(default)]
    pub dataset: DatasetSelector,
    /// The runtime's sink selector; ignored by the chain.
    #[serde(default)]
    pub sinks: Option<Vec<String>>,
    /// The window length.
    pub size: HumanDuration,
    /// How often a sliding window starts (`size` when absent).
    #[serde(default)]
    pub slide: Option<HumanDuration>,
    /// Which timestamp assigns an event to a window and advances the watermark.
    #[serde(default)]
    pub time: WindowTime,
    /// How far behind the asset's maximum event time a window stays open
    /// (`time: event` only).
    #[serde(default)]
    pub allowed_lateness: Option<HumanDuration>,
    /// Closes a silent asset's open window once another asset advances ingest
    /// time by this much.
    #[serde(default)]
    pub idle_close: Option<HumanDuration>,
    /// The output columns.
    pub aggregates: Vec<AggregateSpec>,
}

/// A transform step. `kind` selects the variant.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TransformSpec {
    /// Range rules that raise quality flags.
    Validate(ValidateSpec),
    /// Keeps events whose watched fields changed.
    OnChange(OnChangeSpec),
    /// Drops events failing a structured predicate.
    Filter(FilterSpec),
    /// Every-N and minimum-interval sampling.
    Sample(SampleSpec),
    /// Adds fields from a static table.
    Enrich(EnrichSpec),
    /// Renames fields.
    Rename(RenameSpec),
    /// Keeps a subset of fields.
    Project(ProjectSpec),
    /// Widens field types.
    Cast(CastSpec),
    /// Flags malformed flattened poses.
    Pose(PoseSpec),
    /// Flags malformed joint-state arrays.
    JointState(JointStateSpec),
    /// Tumbling-window aggregates per asset.
    Window(WindowSpec),
}

impl TransformSpec {
    pub(crate) fn selector(&self) -> &DatasetSelector {
        match self {
            TransformSpec::Validate(s) => &s.dataset,
            TransformSpec::OnChange(s) => &s.dataset,
            TransformSpec::Filter(s) => &s.dataset,
            TransformSpec::Sample(s) => &s.dataset,
            TransformSpec::Enrich(s) => &s.dataset,
            TransformSpec::Rename(s) => &s.dataset,
            TransformSpec::Project(s) => &s.dataset,
            TransformSpec::Cast(s) => &s.dataset,
            TransformSpec::Pose(s) => &s.dataset,
            TransformSpec::JointState(s) => &s.dataset,
            TransformSpec::Window(s) => &s.dataset,
        }
    }

    /// The `kind` text.
    pub fn kind(&self) -> &'static str {
        match self {
            TransformSpec::Validate(_) => "validate",
            TransformSpec::OnChange(_) => "on_change",
            TransformSpec::Filter(_) => "filter",
            TransformSpec::Sample(_) => "sample",
            TransformSpec::Enrich(_) => "enrich",
            TransformSpec::Rename(_) => "rename",
            TransformSpec::Project(_) => "project",
            TransformSpec::Cast(_) => "cast",
            TransformSpec::Pose(_) => "pose",
            TransformSpec::JointState(_) => "joint_state",
            TransformSpec::Window(_) => "window",
        }
    }
}

/// Parses a declared type such as `f32` or `list<f64>`.
pub fn parse_field_type(s: &str) -> Result<FieldType, String> {
    if let Some(inner) = s.strip_prefix("list<").and_then(|r| r.strip_suffix('>')) {
        return match parse_field_type(inner)? {
            FieldType::List(_) => Err("nested lists are not supported".into()),
            elem => Ok(FieldType::List(Box::new(elem))),
        };
    }
    Ok(match s {
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
        "str" | "string" => FieldType::Str,
        "ts" | "timestamp" => FieldType::Ts,
        other => return Err(format!("unsupported type `{other}`")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn durations_and_counts_parse() {
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("10m"), Some(Duration::from_secs(600)));
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86_400)));
        assert_eq!(parse_duration("5"), None);
        assert_eq!(parse_duration("s"), None);
        assert_eq!(parse_duration("5x"), None);
        assert_eq!(parse_count("500k"), Some(500_000));
        assert_eq!(parse_count("2M"), Some(2_000_000));
        assert_eq!(parse_count("17"), Some(17));
        assert_eq!(parse_count("k"), None);
        assert_eq!(parse_count("99999999999999999999g"), None);
    }

    #[test]
    fn field_types_parse() {
        assert_eq!(parse_field_type("u64"), Ok(FieldType::U64));
        assert_eq!(
            parse_field_type("list<f32>"),
            Ok(FieldType::List(Box::new(FieldType::F32)))
        );
        assert!(parse_field_type("list<list<f32>>").is_err());
        assert!(parse_field_type("decimal").is_err());
    }

    #[test]
    fn demo_dataset_deserializes() {
        let spec = ChainSpec::from_value(json!({
            "datasets": [{
                "id": "telemetry",
                "match": { "source": "fleet-mqtt", "topic": "thadam-demo/v1/+/+/telemetry" },
                "decode": "json",
                "identity": [{ "site": "{topic.2}" }, { "robot": "{topic.3}" }],
                "event_id": { "key": ["site", "robot", "seq"] },
                "dedup": { "window": "10m", "max_keys": "500k" },
                "time": { "field": "/ts_us", "unit": "us", "domain": "utc", "max_skew": "5s" },
                "fields": [
                    { "name": "seq", "type": "u64", "from": "/seq", "required": true },
                    { "name": "voltage_v", "type": "f32", "from": "/voltage_mv",
                      "unit": "mV", "convert_to": "V" }
                ]
            }],
            "transforms": [
                { "kind": "validate", "dataset": "telemetry",
                  "rules": [{ "field": "motor_temp_c", "min": -40, "max": 150,
                              "flag": "OUT_OF_RANGE", "status": "bad" }] },
                { "kind": "on_change", "dataset": "state", "fields": ["mode"], "heartbeat": "5m" },
                { "kind": "filter", "dataset": ["events"],
                  "where": { "field": "level", "ne": "debug" } }
            ]
        }))
        .unwrap();
        assert_eq!(spec.datasets.len(), 1);
        assert_eq!(spec.datasets[0].identity.0[1].0, "robot");
        assert_eq!(
            spec.datasets[0].dedup.as_ref().unwrap().max_keys,
            Count(500_000)
        );
        assert_eq!(spec.transforms.len(), 3);
        assert_eq!(spec.transforms[2].kind(), "filter");
    }

    #[test]
    fn unknown_keys_and_kinds_are_errors() {
        let e = ChainSpec::from_value(json!({ "datasets": [{ "id": "x", "bogus": 1 }] }));
        assert!(e.is_err());
        let e = ChainSpec::from_value(json!({ "transforms": [{ "kind": "teleport" }] }));
        assert!(e.is_err());
        let e = ChainSpec::from_value(json!({
            "transforms": [{ "kind": "filter", "where": { "fild": "x" } }]
        }));
        assert!(e.is_err());
    }

    #[test]
    fn pose_and_joint_state_deserialize_with_defaults() {
        let pose: TransformSpec =
            serde_json::from_value(json!({ "kind": "pose", "dataset": "poses" })).unwrap();
        assert_eq!(pose.kind(), "pose");
        let TransformSpec::Pose(p) = &pose else {
            panic!("{pose:?}")
        };
        assert_eq!(p.prefix, "pose");
        assert_eq!(p.frame_field.as_deref(), Some("header.frame_id"));
        assert_eq!(p.quaternion_tolerance, 1e-3);

        let pose: TransformSpec = serde_json::from_value(
            json!({ "kind": "pose", "frame_field": null, "prefix": "base_pose" }),
        )
        .unwrap();
        let TransformSpec::Pose(p) = &pose else {
            panic!("{pose:?}")
        };
        assert_eq!(p.prefix, "base_pose");
        assert_eq!(
            p.frame_field, None,
            "an explicit null skips the frame check"
        );

        let js: TransformSpec =
            serde_json::from_value(json!({ "kind": "joint_state", "dataset": "joints" })).unwrap();
        assert_eq!(js.kind(), "joint_state");
        let TransformSpec::JointState(j) = &js else {
            panic!("{js:?}")
        };
        assert_eq!(j.name_field, "name");
        assert_eq!(j.position_field, "position");
        assert_eq!(j.velocity_field, "velocity");
        assert_eq!(j.effort_field, "effort");

        assert!(
            serde_json::from_value::<TransformSpec>(json!({ "kind": "pose", "bogus": 1 })).is_err()
        );
        assert!(
            serde_json::from_value::<TransformSpec>(json!({ "kind": "joint_state", "bogus": 1 }))
                .is_err()
        );
    }

    #[test]
    fn u64_literals_above_i64_max_stay_exact() {
        let p: PredicateSpec =
            serde_json::from_value(json!({ "field": "seq", "eq": 18_446_744_073_709_551_615u64 }))
                .unwrap();
        assert_eq!(p.eq, Some(Lit::UInt(u64::MAX)));
        let p: PredicateSpec = serde_json::from_value(json!({ "field": "x", "ge": 10.0 })).unwrap();
        assert_eq!(p.ge, Some(Lit::Float(10.0)));
        let p: PredicateSpec = serde_json::from_value(json!({ "field": "x", "ge": -3 })).unwrap();
        assert_eq!(p.ge, Some(Lit::Int(-3)));
    }

    #[test]
    fn identity_map_form_is_accepted() {
        let d: IdentitySpec = serde_json::from_value(json!({ "site": "{topic.2}" })).unwrap();
        assert_eq!(d.0, vec![("site".to_owned(), "{topic.2}".to_owned())]);
        let bad: Result<IdentitySpec, _> = serde_json::from_value(json!([{ "a": "x", "b": "y" }]));
        assert!(bad.is_err());
    }
}
