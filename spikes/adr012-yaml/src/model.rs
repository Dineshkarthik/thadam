//! Serde model of the demo pipeline (demo plan, section 6). Every struct uses
//! `deny_unknown_fields`, as ADR-0012 requires. Connector `config` blocks stay
//! `serde_json::Value` (D22). Durations, sizes and counts are strings in YAML
//! and are parsed in custom `Deserialize` impls, so their errors exercise the
//! "custom error inside a visitor" path of each YAML crate.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct Dur(pub Duration);
#[derive(Debug, Clone, PartialEq)]
pub struct Size(pub u64);
#[derive(Debug, Clone, PartialEq)]
pub struct Count(pub u64);

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        humantime::parse_duration(&s)
            .map(Dur)
            .map_err(|e| serde::de::Error::custom(format!("bad duration {s:?}: {e}")))
    }
}
impl Serialize for Dur {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u128(self.0.as_nanos())
    }
}
impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse::<bytesize::ByteSize>()
            .map(|b| Size(b.as_u64()))
            .map_err(|e| serde::de::Error::custom(format!("bad size {s:?}: {e}")))
    }
}
impl Serialize for Size {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(self.0)
    }
}
impl<'de> Deserialize<'de> for Count {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let (num, mul) = match s.strip_suffix(['k', 'K']) {
            Some(n) => (n, 1_000),
            None => match s.strip_suffix(['m', 'M']) {
                Some(n) => (n, 1_000_000),
                None => (s.as_str(), 1),
            },
        };
        num.parse::<u64>()
            .map(|n| Count(n * mul))
            .map_err(|e| serde::de::Error::custom(format!("bad count {s:?}: {e}")))
    }
}
impl Serialize for Count {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(self.0)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub version: u32,
    pub pipeline: PipelineMeta,
    pub sources: Vec<Connector>,
    pub wal: Wal,
    pub quarantine: Quarantine,
    pub datasets: Vec<Dataset>,
    pub transforms: Vec<Transform>,
    pub sinks: Vec<Connector>,
    pub runtime: Runtime,
    pub observability: Observability,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineMeta {
    pub name: String,
    pub node_id: String,
    pub data_dir: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Connector {
    pub id: String,
    pub kind: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Wal {
    pub max_disk: Size,
    pub segment_bytes: Size,
    pub fsync: Fsync,
    pub on_full: OnFull,
    pub retain_for: Dur,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fsync {
    pub interval: Dur,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnFull {
    Block,
    DropOldest,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Quarantine {
    pub format: String,
    pub max_disk: Size,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub id: String,
    #[serde(rename = "match")]
    pub match_: Match,
    pub decode: String,
    pub identity: Identity,
    pub event_id: Option<EventId>,
    pub dedup: Option<Dedup>,
    pub explode: Option<Explode>,
    pub time: Time,
    pub fields: Vec<Field>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Match {
    pub source: String,
    pub topic: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub site: String,
    pub robot: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventId {
    pub key: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Dedup {
    pub window: Dur,
    pub max_keys: Count,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Explode {
    pub path: String,
    pub index_field: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Time {
    pub field: String,
    pub unit: String,
    pub domain: String,
    pub max_skew: Dur,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub from: String,
    pub required: Option<bool>,
    pub unit: Option<String>,
    pub convert_to: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Transform {
    Validate {
        dataset: String,
        rules: Vec<Rule>,
    },
    OnChange {
        dataset: String,
        fields: Vec<String>,
        heartbeat: Dur,
    },
    Filter {
        dataset: String,
        expr: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub field: String,
    pub min: f64,
    pub max: f64,
    pub flag: String,
    pub status: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    pub worker_threads: u32,
    pub memory_target: Size,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Observability {
    pub logs: Logs,
    pub admin: Admin,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Logs {
    pub format: String,
    pub level: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Admin {
    pub bind: String,
    pub token_file: String,
}
