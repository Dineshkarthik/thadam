//! The stateful half of a Sparkplug B consumer — the part a data plane must
//! get right (Sparkplug spec 3.0.0, chapters 5 and 6):
//!
//! - BIRTH certificates define metric names, datatypes and aliases; DATA
//!   messages may carry only aliases, so decoding one message depends on
//!   earlier messages.
//! - `bdSeq` identifies an edge node's NBIRTH..NDEATH session; alias tables
//!   are session-scoped and are rebuilt on every NBIRTH.
//! - `seq` walks 0..=255 across every message of a session; a non-adjacent
//!   value means a gap.
//! - DEATH certificates mark the node's (resp. device's) metrics stale.

use std::collections::HashMap;
use std::fmt;

use prost::Message as _;

use crate::pb;
use crate::topic::{Kind, Topic};

/// The metric an NBIRTH/NDEATH carries to identify its session.
pub const BD_SEQ_METRIC: &str = "bdSeq";

/// A gap on the walking 0..=255 `seq` (spec 6.4.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqGap {
    pub expected: u64,
    pub got: u64,
}

impl fmt::Display for SeqGap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "expected seq {}, got {}", self.expected, self.got)
    }
}

fn next_seq(seq: u64) -> u64 {
    (seq + 1) % 256
}

/// A metric definition captured from a BIRTH certificate.
#[derive(Clone, Debug)]
pub struct MetricMeta {
    pub name: Box<str>,
    pub datatype: u32,
}

/// The name/alias tables of one BIRTH scope (an edge node or one device),
/// plus the staleness its DEATH certificate sets.
#[derive(Debug, Default)]
struct Scope {
    dead: bool,
    by_alias: HashMap<u64, MetricMeta>,
    by_name: HashMap<Box<str>, MetricMeta>,
}

impl Scope {
    /// Registers the BIRTH metrics; `bdSeq` is session plumbing, not data.
    fn register(&mut self, metrics: &[pb::payload::Metric]) -> usize {
        let mut count = 0usize;
        for metric in metrics {
            let Some(name) = metric.name.as_deref() else {
                continue;
            };
            if name == BD_SEQ_METRIC {
                continue;
            }
            let meta = MetricMeta {
                name: name.into(),
                datatype: metric.datatype.unwrap_or(0),
            };
            if let Some(alias) = metric.alias {
                self.by_alias.insert(alias, meta.clone());
            }
            self.by_name.insert(meta.name.clone(), meta);
            count += 1;
        }
        count
    }
}

/// Session state of one edge node: its own metrics plus one table per device.
#[derive(Debug, Default)]
pub struct NodeState {
    bd_seq: Option<u64>,
    last_seq: Option<u64>,
    node: Scope,
    devices: HashMap<Box<str>, Scope>,
}

/// A metric's wire value, with bulky payloads folded to their kind.
#[derive(Clone, Debug)]
pub enum MetricValue {
    Null,
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Text(Box<str>),
    Bytes(usize),
    DataSet,
    Template,
    Extension,
}

impl fmt::Display for MetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricValue::Null => write!(f, "<null>"),
            MetricValue::U32(v) => write!(f, "{v}"),
            MetricValue::U64(v) => write!(f, "{v}"),
            MetricValue::F32(v) => write!(f, "{v}"),
            MetricValue::F64(v) => write!(f, "{v}"),
            MetricValue::Bool(v) => write!(f, "{v}"),
            MetricValue::Text(v) => write!(f, "\"{v}\""),
            MetricValue::Bytes(n) => write!(f, "<{n}B>"),
            MetricValue::DataSet => write!(f, "<dataset>"),
            MetricValue::Template => write!(f, "<template>"),
            MetricValue::Extension => write!(f, "<extension>"),
        }
    }
}

fn value_of(metric: &pb::payload::Metric) -> MetricValue {
    use pb::payload::metric::Value;
    if metric.is_null == Some(true) {
        return MetricValue::Null;
    }
    match metric.value.as_ref() {
        None => MetricValue::Null,
        Some(Value::IntValue(v)) => MetricValue::U32(*v),
        Some(Value::LongValue(v)) => MetricValue::U64(*v),
        Some(Value::FloatValue(v)) => MetricValue::F32(*v),
        Some(Value::DoubleValue(v)) => MetricValue::F64(*v),
        Some(Value::BooleanValue(v)) => MetricValue::Bool(*v),
        Some(Value::StringValue(v)) => MetricValue::Text(v.as_str().into()),
        Some(Value::BytesValue(b)) => MetricValue::Bytes(b.len()),
        Some(Value::DatasetValue(_)) => MetricValue::DataSet,
        Some(Value::TemplateValue(_)) => MetricValue::Template,
        Some(Value::ExtensionValue(_)) => MetricValue::Extension,
    }
}

/// One metric after resolution against the BIRTH tables.
#[derive(Clone, Debug)]
pub struct ResolvedMetric {
    /// The registered name, or an `alias:N` stand-in when the alias is
    /// unknown (the note records it; the message is not dropped).
    pub name: Box<str>,
    pub datatype: u32,
    pub value: MetricValue,
    pub timestamp: Option<u64>,
    pub alias: Option<u64>,
    /// Set when the owning node/device is past its DEATH certificate.
    pub stale: bool,
}

impl ResolvedMetric {
    /// One-line rendering for the spike's output.
    pub fn describe(&self) -> String {
        let stale = if self.stale { "stale:" } else { "" };
        format!(
            "{stale}{}={} ({})",
            self.name,
            self.value,
            datatype_name(self.datatype)
        )
    }
}

/// The shape a BIRTH/DEATH changed, for the scenario checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Birth,
    Rebirth,
    Death,
    DeviceBirth,
    DeviceDeath,
}

/// What one processed payload did: the resolved metrics plus the
/// conformance observations the run checks.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    pub notes: Vec<String>,
    pub resolved: Vec<ResolvedMetric>,
    pub gap: Option<SeqGap>,
    pub change: Option<Change>,
    pub registered: usize,
    /// Node staleness after the message (BIRTH/DEATH); `None` for DATA.
    pub node_dead: Option<bool>,
}

/// The payload was not a decodable Sparkplug B protobuf message.
#[derive(Debug)]
pub struct DecodeError(pub String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DecodeError {}

/// The consumer: one decoder instance per subscribed namespace.
#[derive(Debug, Default)]
pub struct Decoder {
    nodes: HashMap<(Box<str>, Box<str>), NodeState>,
}

/// BIRTH/DEATH/DATA run at node level or at one device below it.
enum Level<'a> {
    Node,
    Device(&'a str),
}

impl Decoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes one MQTT payload under an already-parsed Sparkplug topic.
    pub fn handle(&mut self, topic: &Topic<'_>, payload: &[u8]) -> Result<Outcome, DecodeError> {
        let mut out = Outcome::default();
        if !topic.well_formed {
            out.notes
                .push(format!("unusual topic shape for {}", topic.kind.as_str()));
        }
        // Host applications publish STATE as retained JSON; a passive
        // consumer must neither decode it as protobuf nor republish it.
        if topic.kind == Kind::State {
            out.notes.push(
                "host STATE passthrough (host-application JSON; observed, not decoded)".into(),
            );
            return Ok(out);
        }
        let msg = pb::Payload::decode(payload)
            .map_err(|e| DecodeError(format!("malformed Sparkplug payload: {e}")))?;
        Ok(match topic.kind {
            Kind::NBirth => self.on_birth(topic, &msg, out, true),
            Kind::DBirth => self.on_birth(topic, &msg, out, false),
            Kind::NDeath => self.on_death(topic, &msg, out, true),
            Kind::DDeath => self.on_death(topic, &msg, out, false),
            Kind::NData | Kind::DData => self.on_data(&msg, out, level_of(topic), topic),
            Kind::NCmd | Kind::DCmd => {
                out.notes.push(
                    "command topic: a passive consumer records it but never acts on it".into(),
                );
                out
            }
            Kind::State => out,
        })
    }

    /// Whether a node's metrics are stale (past NDEATH), when known.
    #[must_use]
    pub fn node_stale(&self, group: &str, node: &str) -> Option<bool> {
        self.nodes
            .get(&(group.into(), node.into()))
            .map(|state| state.node.dead)
    }

    fn node(&mut self, topic: &Topic<'_>) -> &mut NodeState {
        self.nodes
            .entry((topic.group.into(), topic.node.into()))
            .or_default()
    }

    fn on_birth(
        &mut self,
        topic: &Topic<'_>,
        msg: &pb::Payload,
        mut out: Outcome,
        node_level: bool,
    ) -> Outcome {
        let node = self.node(topic);
        if node_level {
            let bd = bd_seq_of(msg);
            let rebirth = node.bd_seq.is_some() && node.bd_seq != bd;
            // A new session invalidates every alias table of the node
            // (spec 6.4.7): devices re-register through their own DBIRTH.
            node.node = Scope::default();
            for device in node.devices.values_mut() {
                *device = Scope {
                    dead: true,
                    ..Scope::default()
                };
            }
            node.bd_seq = bd;
            node.last_seq = msg.seq;
            out.registered = node.node.register(&msg.metrics);
            out.change = Some(if rebirth {
                Change::Rebirth
            } else {
                Change::Birth
            });
            out.node_dead = Some(false);
            if rebirth {
                out.notes.push(format!(
                    "rebirth: bdSeq {} replaces all alias tables",
                    bd.map_or_else(|| "?".into(), |b| b.to_string())
                ));
            }
            out
        } else {
            let device = topic.device.unwrap_or("");
            if device.is_empty() {
                out.notes.push("DBIRTH without a device segment".into());
                return out;
            }
            if node.bd_seq.is_none() {
                out.notes
                    .push("DBIRTH before any NBIRTH for the node".into());
            }
            track_seq(&mut node.last_seq, msg.seq, &mut out);
            let scope = node.devices.entry(device.into()).or_default();
            *scope = Scope::default();
            out.registered = scope.register(&msg.metrics);
            out.change = Some(Change::DeviceBirth);
            out
        }
    }

    fn on_death(
        &mut self,
        topic: &Topic<'_>,
        msg: &pb::Payload,
        mut out: Outcome,
        node_level: bool,
    ) -> Outcome {
        let node = self.node(topic);
        track_seq(&mut node.last_seq, msg.seq, &mut out);
        if node_level {
            match (node.bd_seq, bd_seq_of(msg)) {
                (Some(current), Some(reported)) if current != reported => {
                    out.notes.push(format!(
                        "NDEATH for bdSeq {reported} ignored: current session is bdSeq {current}"
                    ));
                    out.node_dead = Some(node.node.dead);
                }
                (None, _) => {
                    out.notes
                        .push("NDEATH for a session no NBIRTH announced".into());
                    mark_dead(node);
                    out.change = Some(Change::Death);
                    out.node_dead = Some(true);
                }
                _ => {
                    mark_dead(node);
                    out.change = Some(Change::Death);
                    out.node_dead = Some(true);
                }
            }
            out
        } else {
            let device = topic.device.unwrap_or("");
            let scope = node.devices.entry(device.into()).or_default();
            scope.dead = true;
            out.change = Some(Change::DeviceDeath);
            out
        }
    }

    fn on_data(
        &mut self,
        msg: &pb::Payload,
        mut out: Outcome,
        level: Level<'_>,
        topic: &Topic<'_>,
    ) -> Outcome {
        let node = self.node(topic);
        let _ = topic;
        track_seq(&mut node.last_seq, msg.seq, &mut out);
        let scope = match level {
            Level::Node => {
                if node.bd_seq.is_none() && node.node.by_alias.is_empty() {
                    out.notes.push("NDATA before any NBIRTH".into());
                }
                &mut node.node
            }
            Level::Device(device) => {
                let absent = !node.devices.contains_key(device);
                let scope = node.devices.entry(device.into()).or_default();
                if absent {
                    out.notes
                        .push(format!("DDATA for `{device}` before its DBIRTH"));
                }
                scope
            }
        };
        let stale = scope.dead;
        if stale {
            out.notes
                .push("data arrived for a scope past its DEATH (stale)".into());
        }
        for metric in &msg.metrics {
            let resolved = resolve(scope, metric, stale, &mut out.notes);
            out.resolved.push(resolved);
        }
        out
    }
}

fn level_of<'a>(topic: &Topic<'a>) -> Level<'a> {
    match topic.kind {
        Kind::DBirth | Kind::DDeath | Kind::DData | Kind::DCmd => {
            Level::Device(topic.device.unwrap_or(""))
        }
        _ => Level::Node,
    }
}

fn mark_dead(node: &mut NodeState) {
    node.node.dead = true;
    for device in node.devices.values_mut() {
        device.dead = true;
    }
}

fn track_seq(last: &mut Option<u64>, seq: Option<u64>, out: &mut Outcome) {
    let Some(new) = seq else {
        out.notes
            .push("no seq on a session message (Sparkplug requires it)".into());
        return;
    };
    if let Some(prev) = last.replace(new)
        && new != next_seq(prev)
    {
        let gap = SeqGap {
            expected: next_seq(prev),
            got: new,
        };
        out.gap = Some(gap);
        out.notes.push(format!("seq gap: {gap}"));
    }
}

fn bd_seq_of(msg: &pb::Payload) -> Option<u64> {
    msg.metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(BD_SEQ_METRIC))
        .and_then(|m| match m.value.as_ref() {
            Some(pb::payload::metric::Value::IntValue(v)) => Some(u64::from(*v)),
            Some(pb::payload::metric::Value::LongValue(v)) => Some(*v),
            _ => None,
        })
}

fn resolve(
    scope: &Scope,
    metric: &pb::payload::Metric,
    stale: bool,
    notes: &mut Vec<String>,
) -> ResolvedMetric {
    let registered = match (metric.alias, metric.name.as_deref()) {
        (Some(alias), _) => scope
            .by_alias
            .get(&alias)
            .or_else(|| metric.name.as_deref().and_then(|n| scope.by_name.get(n))),
        (None, Some(name)) => scope.by_name.get(name),
        (None, None) => None,
    };
    match (metric.alias, metric.name.as_deref(), registered) {
        (Some(alias), _, None) => notes.push(format!(
            "alias {alias} not in the BIRTH table ({} aliases)",
            scope.by_alias.len()
        )),
        (None, Some(name), None) => {
            notes.push(format!("metric `{name}` not in the BIRTH table"));
        }
        (None, None, _) => notes.push("metric has neither name nor alias".into()),
        _ => {}
    }
    let (name, datatype) = match registered {
        Some(meta) => (meta.name.clone(), meta.datatype),
        None => (
            metric.name.as_deref().map_or_else(
                || {
                    metric
                        .alias
                        .map_or_else(|| "unknown".into(), |a| format!("alias:{a}").into())
                },
                Into::into,
            ),
            metric.datatype.unwrap_or(0),
        ),
    };
    ResolvedMetric {
        name,
        datatype,
        value: value_of(metric),
        timestamp: metric.timestamp,
        alias: metric.alias,
        stale,
    }
}

/// The decoded datatype for log rendering.
#[must_use]
pub fn datatype_name(datatype: u32) -> &'static str {
    i32::try_from(datatype)
        .ok()
        .and_then(|v| pb::DataType::try_from(v).ok())
        .map_or("unknown", |d| d.as_str_name())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::topic;

    fn metric(
        name: Option<&str>,
        alias: Option<u64>,
        datatype: pb::DataType,
        value: Option<pb::payload::metric::Value>,
    ) -> pb::payload::Metric {
        let is_null = value.is_none();
        pb::payload::Metric {
            name: name.map(Into::into),
            alias,
            datatype: Some(datatype as i32 as u32),
            value,
            is_null: Some(is_null),
            ..Default::default()
        }
    }

    fn nbirth(node_alias: u64, name: &str, seq: u64, bd: u64) -> Vec<u8> {
        pb::Payload {
            seq: Some(seq),
            metrics: vec![
                metric(
                    Some(BD_SEQ_METRIC),
                    None,
                    pb::DataType::Int64,
                    Some(pb::payload::metric::Value::LongValue(bd)),
                ),
                metric(
                    Some(name),
                    Some(node_alias),
                    pb::DataType::Float,
                    Some(pb::payload::metric::Value::FloatValue(1.0)),
                ),
            ],
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn birth_topic() -> topic::Topic<'static> {
        topic::parse("spBv1.0/g/NBIRTH/e1").unwrap()
    }

    #[test]
    fn birth_data_death_rebirth_lifecycle() {
        let mut decoder = Decoder::new();
        let birth = decoder
            .handle(&birth_topic(), &nbirth(1, "temp", 0, 7))
            .unwrap();
        assert_eq!(
            (birth.change, birth.registered, birth.node_dead),
            (Some(Change::Birth), 1, Some(false))
        );

        let data_topic = topic::parse("spBv1.0/g/NDATA/e1").unwrap();
        let data = pb::Payload {
            seq: Some(1),
            metrics: vec![metric(
                None,
                Some(1),
                pb::DataType::Float,
                Some(pb::payload::metric::Value::FloatValue(21.5)),
            )],
            ..Default::default()
        }
        .encode_to_vec();
        let out = decoder.handle(&data_topic, &data).unwrap();
        assert_eq!(out.resolved.len(), 1);
        assert_eq!(&*out.resolved[0].name, "temp");
        assert!(!out.resolved[0].stale);

        // Skip seq 2 -> gap expected 2 got 3.
        let data = pb::Payload {
            seq: Some(3),
            metrics: vec![],
            ..Default::default()
        }
        .encode_to_vec();
        let out = decoder.handle(&data_topic, &data).unwrap();
        assert_eq!(
            out.gap,
            Some(SeqGap {
                expected: 2,
                got: 3
            })
        );

        let death = pb::Payload {
            seq: Some(4),
            metrics: vec![metric(
                Some(BD_SEQ_METRIC),
                None,
                pb::DataType::Int64,
                Some(pb::payload::metric::Value::LongValue(7)),
            )],
            ..Default::default()
        }
        .encode_to_vec();
        let death_topic = topic::parse("spBv1.0/g/NDEATH/e1").unwrap();
        let out = decoder.handle(&death_topic, &death).unwrap();
        assert_eq!(
            (out.change, out.node_dead),
            (Some(Change::Death), Some(true))
        );
        assert_eq!(decoder.node_stale("g", "e1"), Some(true));

        // Data after death is stale; a rebirth with a higher bdSeq clears it.
        let out = decoder.handle(&data_topic, &data).unwrap();
        assert!(out.resolved.is_empty());
        let rebirth = decoder
            .handle(&birth_topic(), &nbirth(1, "temp", 0, 8))
            .unwrap();
        assert_eq!(
            (rebirth.change, rebirth.node_dead),
            (Some(Change::Rebirth), Some(false))
        );
        assert_eq!(decoder.node_stale("g", "e1"), Some(false));
    }

    #[test]
    fn seq_wraps_at_255() {
        assert_eq!(next_seq(255), 0);
        let mut last = Some(255u64);
        let mut out = Outcome::default();
        track_seq(&mut last, Some(0), &mut out);
        assert_eq!(out.gap, None);
        track_seq(&mut last, Some(2), &mut out);
        assert_eq!(
            out.gap,
            Some(SeqGap {
                expected: 1,
                got: 2
            })
        );
    }
}
