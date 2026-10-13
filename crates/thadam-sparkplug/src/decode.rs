//! The stateful half of a Sparkplug B consumer (Sparkplug specification
//! 3.0.0, chapters 5 and 6):
//!
//! - BIRTH certificates define metric names, datatypes and aliases; DATA
//!   messages may carry only aliases, so decoding one message depends on
//!   earlier messages.
//! - `bdSeq` identifies an edge node's NBIRTH..NDEATH session; alias tables
//!   are session-scoped and are rebuilt on every NBIRTH.
//! - `seq` walks 0..=255 across every message of a session; a value that is
//!   not the successor of the previous one is a gap.
//! - DEATH certificates mark the node's (resp. device's) metrics stale.
//!
//! The decoder is pure: no clock, no I/O. Durable state changes come back in
//! [`Outcome::store`] for the caller to apply with [`StoreUpdate::apply`]
//! **before** it commits the dependent messages, and a restart rebuilds the
//! decoder from [`BirthStore::load_all`] with [`Decoder::seeded`].
//!
//! Quality mapping (design: docs/design/spikes/sparkplug-b-and-opcua-pubsub.md
//! section 4.3), carried in [`Outcome::flags`] and explained by
//! [`Outcome::notes`]:
//!
//! | observation | flag | note |
//! |---|---|---|
//! | `seq` discontinuity | `SEQUENCE_GAP` | `sparkplug.seq expected N got M` |
//! | alias missing from the BIRTH table, unsupported value, missing `seq`, bad topic shape | `DECODE_ERROR` | `sparkplug alias N`, ... |
//! | DATA for a scope past its DEATH | `STALE` (status `Uncertain`) | `sparkplug.past-ndeath` |

use std::collections::HashMap;
use std::fmt;

use prost::Message as _;
use thadam_core::{
    BirthKey, BirthRecord, BirthStore, ErrorDomain, QualityFlags, QualityStatus, ThadamError, codes,
};

use crate::pb::{self, payload::metric::Value};
use crate::topic::{Kind, Topic};

/// The metric an NBIRTH/NDEATH carries to identify its session.
pub const BD_SEQ_METRIC: &str = "bdSeq";

/// A discontinuity on the walking 0..=255 `seq`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeqGap {
    /// The successor of the previous `seq`.
    pub expected: u64,
    /// The `seq` that arrived.
    pub got: u64,
}

impl fmt::Display for SeqGap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sparkplug.seq expected {} got {}",
            self.expected, self.got
        )
    }
}

fn next_seq(seq: u64) -> u64 {
    (seq + 1) % 256
}

/// A metric's value, interpreted through its datatype.
///
/// Packed array datatypes (22..=34) arrive as [`MetricValue::Bytes`] holding
/// the raw little-endian packing; their datatype tells the reader how to
/// unpack.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    /// Explicit null (`is_null`), or a metric that carried no value.
    Null,
    /// Int8, Int16, Int32 and Int64, sign-extended.
    Int(i64),
    /// UInt8..UInt64 and DateTime (ms since the epoch).
    UInt(u64),
    /// Float.
    F32(f32),
    /// Double.
    F64(f64),
    /// Boolean.
    Bool(bool),
    /// String, Text and UUID.
    Text(String),
    /// Bytes, File and packed arrays.
    Bytes(Vec<u8>),
    /// DataSet, Template or an extension value: not decoded (the message is
    /// flagged `DECODE_ERROR`).
    Unsupported,
}

/// One metric after resolution against the BIRTH tables.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedMetric {
    /// The registered name, or `alias:N` when the alias is unknown.
    pub name: Box<str>,
    /// The registered datatype, else the one on the wire (0 when absent).
    pub datatype: u32,
    /// The value; [`MetricValue::Null`] iff `is_null`.
    pub value: MetricValue,
    /// Explicit null, distinct from any value.
    pub is_null: bool,
    /// Acquisition time of the metric, ms since the epoch.
    pub timestamp: Option<u64>,
    /// The alias the metric carried.
    pub alias: Option<u64>,
}

/// What a BIRTH/DEATH did to the session state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    /// NBIRTH for a node with no live session.
    Birth,
    /// NBIRTH whose `bdSeq` replaced a different known session.
    Rebirth,
    /// NDEATH of the current session.
    Death,
    /// DBIRTH.
    DeviceBirth,
    /// DDEATH.
    DeviceDeath,
}

/// A durable birth-store change a message calls for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreUpdate {
    /// Store the latest BIRTH of a scope.
    Put(BirthKey, BirthRecord),
    /// Mark a scope stale.
    Dead(BirthKey),
}

impl StoreUpdate {
    /// Applies the update; a failure is the caller's to surface, never to
    /// swallow (a BIRTH that is not durable cannot seed a restart).
    pub fn apply(&self, store: &BirthStore) -> Result<(), ThadamError> {
        match self {
            StoreUpdate::Put(key, record) => store.put(key, record),
            StoreUpdate::Dead(key) => store.mark_dead(key),
        }
    }
}

/// What one message did: the resolved metrics plus the quality observations.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    /// The payload-level `seq`.
    pub seq: Option<u64>,
    /// The node's current session counter, when known.
    pub bd_seq: Option<u64>,
    /// The payload-level timestamp, ms since the epoch.
    pub timestamp: Option<u64>,
    /// Resolved metrics (BIRTH and DATA; `bdSeq` is not a data metric).
    pub metrics: Vec<ResolvedMetric>,
    /// The session change a BIRTH/DEATH made.
    pub change: Option<Change>,
    /// Event-level quality flags (`SEQUENCE_GAP`, `STALE`, `DECODE_ERROR`).
    pub flags: QualityFlags,
    /// The seq discontinuity, when `flags` has `SEQUENCE_GAP`.
    pub gap: Option<SeqGap>,
    /// Human-readable reasons for the flags, and other observations.
    pub notes: Vec<String>,
    /// Birth-store changes to make durable before committing the message.
    pub store: Vec<StoreUpdate>,
}

impl Outcome {
    /// `Uncertain` for stale data, else `Good` (values are trustworthy even
    /// across a gap; ordering is not).
    pub fn status(&self) -> QualityStatus {
        if self.flags.contains(QualityFlags::STALE) {
            QualityStatus::Uncertain
        } else {
            QualityStatus::Good
        }
    }

    fn flag(&mut self, flag: QualityFlags, note: String) {
        self.flags |= flag;
        self.notes.push(note);
    }
}

#[derive(Debug)]
struct Meta {
    name: Box<str>,
    datatype: u32,
}

/// The tables of one BIRTH scope (an edge node or one device) plus the
/// staleness its DEATH sets.
#[derive(Debug, Default)]
struct Scope {
    dead: bool,
    by_alias: HashMap<u64, Meta>,
    by_name: HashMap<Box<str>, u32>,
}

impl Scope {
    fn from_birth(msg: &pb::Payload, dead: bool) -> Scope {
        let mut scope = Scope {
            dead,
            ..Scope::default()
        };
        for metric in &msg.metrics {
            let Some(name) = metric.name.as_deref().filter(|n| *n != BD_SEQ_METRIC) else {
                continue;
            };
            let datatype = metric.datatype.unwrap_or(0);
            if let Some(alias) = metric.alias {
                scope.by_alias.insert(
                    alias,
                    Meta {
                        name: name.into(),
                        datatype,
                    },
                );
            }
            scope.by_name.insert(name.into(), datatype);
        }
        scope
    }
}

/// Session state of one edge node: its own metrics plus one table per device.
#[derive(Debug, Default)]
struct NodeState {
    bd_seq: Option<u64>,
    /// `None` until the first seq of a session (and after a restart: the
    /// live cursor is not persisted, so the first seq re-anchors it).
    last_seq: Option<u64>,
    node: Scope,
    devices: HashMap<Box<str>, Scope>,
}

/// The consumer: one decoder per subscribed Sparkplug namespace.
// shortcut: no cap on tracked nodes/devices (the birth store has none
// either); upgrade when a broker's topic space is not trusted to be bounded.
#[derive(Debug, Default)]
pub struct Decoder {
    nodes: HashMap<(Box<str>, Box<str>), NodeState>,
}

impl Decoder {
    /// An empty decoder: every alias is unknown until a BIRTH arrives.
    pub fn new() -> Self {
        Self::default()
    }

    /// A decoder rebuilt from stored births ([`BirthStore::load_all`]), so
    /// DATA resolves after a restart without a new BIRTH.
    ///
    /// The `seq` cursor is deliberately not seeded: the stored `seq` is the
    /// BIRTH's, and seeding from it would flag a false gap on the first
    /// message after nearly every restart. The first seq re-anchors it, so a
    /// gap that spans the restart goes undetected.
    ///
    /// A device record is stale-on-arrival when its node's session moved on
    /// (`bd_seq` differs from the node record's) or the node is dead. A
    /// stored BIRTH that no longer decodes is an error, not skipped state.
    // shortcut: seq cursor not persisted (see above); upgrade by persisting it
    // on a timer if gaps across restarts matter.
    pub fn seeded(
        records: impl IntoIterator<Item = (BirthKey, BirthRecord)>,
    ) -> Result<Self, ThadamError> {
        let mut decoder = Decoder::new();
        let (nodes, devices): (Vec<_>, Vec<_>) = records
            .into_iter()
            .partition(|(key, _)| key.device.is_none());
        for (key, record) in nodes {
            let state = decoder.nodes.entry(node_id(&key)).or_default();
            state.bd_seq = record.bd_seq;
            state.node = Scope::from_birth(&parse_stored(&key, &record)?, record.dead);
        }
        for (key, record) in devices {
            let Some(device) = key.device.clone() else {
                continue;
            };
            let known = decoder.nodes.contains_key(&node_id(&key));
            let state = decoder.nodes.entry(node_id(&key)).or_default();
            let session_moved = known && state.bd_seq != record.bd_seq;
            let scope = if session_moved {
                Scope {
                    dead: true,
                    ..Scope::default()
                }
            } else {
                Scope::from_birth(
                    &parse_stored(&key, &record)?,
                    record.dead || state.node.dead,
                )
            };
            state.devices.insert(device, scope);
        }
        Ok(decoder)
    }

    /// Decodes one MQTT payload under an already-parsed Sparkplug topic.
    ///
    /// A payload that is not a Sparkplug B protobuf is a `decode.invalid_payload`
    /// error. STATE (host JSON) and CMD topics are observed, not decoded.
    pub fn handle(&mut self, topic: &Topic<'_>, payload: &[u8]) -> Result<Outcome, ThadamError> {
        let mut out = Outcome::default();
        match topic.kind {
            Kind::State => {
                out.notes
                    .push("host STATE passthrough (host-application JSON; not decoded)".into());
                return Ok(out);
            }
            Kind::NCmd | Kind::DCmd => {
                out.notes
                    .push("command topic: recorded, never acted on".into());
                return Ok(out);
            }
            _ => {}
        }
        let msg = pb::Payload::decode(payload).map_err(|e| {
            ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_INVALID_PAYLOAD,
                format!("malformed Sparkplug payload: {e}"),
            )
        })?;
        out.seq = msg.seq;
        out.timestamp = msg.timestamp;
        if !topic.well_formed {
            out.flag(
                QualityFlags::DECODE_ERROR,
                format!("unusual topic shape for {}", topic.kind.as_str()),
            );
        }
        match topic.kind {
            Kind::NBirth => self.node_birth(topic, &msg, payload, &mut out),
            Kind::DBirth => self.device_birth(topic, &msg, payload, &mut out),
            Kind::NDeath => self.node_death(topic, &msg, &mut out),
            Kind::DDeath => self.device_death(topic, &msg, &mut out),
            Kind::NData | Kind::DData => self.data(topic, &msg, &mut out),
            Kind::NCmd | Kind::DCmd | Kind::State => {}
        }
        Ok(out)
    }

    fn node_birth(&mut self, topic: &Topic<'_>, msg: &pb::Payload, raw: &[u8], out: &mut Outcome) {
        let bd_seq = bd_seq_of(msg);
        let state = self.nodes.entry(topic_node_id(topic)).or_default();
        let rebirth = state.bd_seq.is_some() && state.bd_seq != bd_seq;
        // A new session invalidates every alias table of the node (spec
        // chapter 6): devices re-register through their own DBIRTH, and
        // until then their DATA is stale, not resolved against old aliases.
        state.node = Scope::from_birth(msg, false);
        for device in state.devices.values_mut() {
            *device = Scope {
                dead: true,
                ..Scope::default()
            };
        }
        state.bd_seq = bd_seq;
        // The seq stream restarts with the session: anchor, never a gap.
        state.last_seq = None;
        track_seq(&mut state.last_seq, msg.seq, out);
        out.bd_seq = bd_seq;
        out.change = Some(if rebirth {
            Change::Rebirth
        } else {
            Change::Birth
        });
        out.store.push(StoreUpdate::Put(
            BirthKey::new(topic.group, topic.node, None),
            BirthRecord {
                birth: raw.to_vec(),
                bd_seq,
                seq: msg.seq,
                dead: false,
            },
        ));
        out.metrics = resolve_all(Some(&state.node), msg, out);
    }

    fn device_birth(
        &mut self,
        topic: &Topic<'_>,
        msg: &pb::Payload,
        raw: &[u8],
        out: &mut Outcome,
    ) {
        let Some(device) = topic.device else {
            out.flag(
                QualityFlags::DECODE_ERROR,
                "DBIRTH without a device segment".into(),
            );
            return;
        };
        let state = self.nodes.entry(topic_node_id(topic)).or_default();
        if state.bd_seq.is_none() {
            out.notes
                .push("DBIRTH before any NBIRTH for the node".into());
        }
        track_seq(&mut state.last_seq, msg.seq, out);
        let scope = Scope::from_birth(msg, state.node.dead);
        out.store.push(StoreUpdate::Put(
            BirthKey::new(topic.group, topic.node, Some(device)),
            BirthRecord {
                birth: raw.to_vec(),
                bd_seq: state.bd_seq,
                seq: msg.seq,
                dead: scope.dead,
            },
        ));
        out.bd_seq = state.bd_seq;
        out.change = Some(Change::DeviceBirth);
        out.metrics = resolve_all(Some(&scope), msg, out);
        state.devices.insert(device.into(), scope);
    }

    fn node_death(&mut self, topic: &Topic<'_>, msg: &pb::Payload, out: &mut Outcome) {
        let Some(state) = self.nodes.get_mut(&topic_node_id(topic)) else {
            out.notes
                .push("NDEATH for a node no NBIRTH announced".into());
            return;
        };
        out.bd_seq = state.bd_seq;
        // A death certificate for another session is a late Will of a
        // session the node already replaced: ignore it (spec chapter 5).
        if let (Some(current), Some(reported)) = (state.bd_seq, bd_seq_of(msg))
            && current != reported
        {
            out.notes.push(format!(
                "NDEATH for bdSeq {reported} ignored: current session is bdSeq {current}"
            ));
            return;
        }
        state.node.dead = true;
        for device in state.devices.values_mut() {
            device.dead = true;
        }
        // The death certificate is the Will: it carries no session seq.
        state.last_seq = None;
        out.change = Some(Change::Death);
        out.store.push(StoreUpdate::Dead(BirthKey::new(
            topic.group,
            topic.node,
            None,
        )));
    }

    fn device_death(&mut self, topic: &Topic<'_>, msg: &pb::Payload, out: &mut Outcome) {
        let Some(state) = self.nodes.get_mut(&topic_node_id(topic)) else {
            out.notes
                .push("DDEATH for a node no NBIRTH announced".into());
            return;
        };
        track_seq(&mut state.last_seq, msg.seq, out);
        out.bd_seq = state.bd_seq;
        let Some(device) = topic.device else {
            out.flag(
                QualityFlags::DECODE_ERROR,
                "DDEATH without a device segment".into(),
            );
            return;
        };
        let Some(scope) = state.devices.get_mut(device) else {
            out.notes
                .push(format!("DDEATH for unknown device `{device}`"));
            return;
        };
        scope.dead = true;
        out.change = Some(Change::DeviceDeath);
        out.store.push(StoreUpdate::Dead(BirthKey::new(
            topic.group,
            topic.node,
            Some(device),
        )));
    }

    fn data(&mut self, topic: &Topic<'_>, msg: &pb::Payload, out: &mut Outcome) {
        let state = self.nodes.get_mut(&topic_node_id(topic));
        let scope = match state {
            Some(state) => {
                track_seq(&mut state.last_seq, msg.seq, out);
                out.bd_seq = state.bd_seq;
                match (topic.kind, topic.device) {
                    (Kind::DData, Some(device)) => state.devices.get(device),
                    (Kind::DData, None) => None,
                    _ => Some(&state.node),
                }
            }
            None => None,
        };
        if scope.is_some_and(|s| s.dead) {
            out.flag(QualityFlags::STALE, "sparkplug.past-ndeath".into());
        }
        out.metrics = resolve_all(scope, msg, out);
    }
}

fn node_id(key: &BirthKey) -> (Box<str>, Box<str>) {
    (key.group.clone(), key.node.clone())
}

fn topic_node_id(topic: &Topic<'_>) -> (Box<str>, Box<str>) {
    (topic.group.into(), topic.node.into())
}

fn parse_stored(key: &BirthKey, record: &BirthRecord) -> Result<pb::Payload, ThadamError> {
    pb::Payload::decode(record.birth.as_slice()).map_err(|e| {
        ThadamError::permanent(
            ErrorDomain::Decode,
            codes::DECODE_INVALID_PAYLOAD,
            format!(
                "stored Sparkplug BIRTH for {}/{}/{} does not decode: {e}",
                key.group,
                key.node,
                key.device.as_deref().unwrap_or("-"),
            ),
        )
    })
}

/// Advances the cursor and records a gap or a missing seq on `out`.
fn track_seq(last: &mut Option<u64>, seq: Option<u64>, out: &mut Outcome) {
    let Some(new) = seq else {
        // The stream cannot be validated across a message with no seq.
        *last = None;
        out.flag(
            QualityFlags::DECODE_ERROR,
            "sparkplug.seq missing on a session message".into(),
        );
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
        out.flag(QualityFlags::SEQUENCE_GAP, gap.to_string());
    }
}

fn bd_seq_of(msg: &pb::Payload) -> Option<u64> {
    msg.metrics
        .iter()
        .find(|m| m.name.as_deref() == Some(BD_SEQ_METRIC))
        .and_then(|m| match m.value.as_ref() {
            Some(Value::IntValue(v)) => Some(u64::from(*v)),
            Some(Value::LongValue(v)) => Some(*v),
            _ => None,
        })
}

fn resolve_all(scope: Option<&Scope>, msg: &pb::Payload, out: &mut Outcome) -> Vec<ResolvedMetric> {
    msg.metrics
        .iter()
        .filter(|m| m.name.as_deref() != Some(BD_SEQ_METRIC))
        .map(|m| resolve(scope, m, out))
        .collect()
}

fn resolve(
    scope: Option<&Scope>,
    metric: &pb::payload::Metric,
    out: &mut Outcome,
) -> ResolvedMetric {
    let registered = match (metric.alias, metric.name.as_deref()) {
        (Some(alias), _) => scope
            .and_then(|s| s.by_alias.get(&alias))
            .map(|m| (&*m.name, m.datatype)),
        (None, Some(name)) => scope
            .and_then(|s| s.by_name.get(name))
            .map(|datatype| (name, *datatype)),
        (None, None) => None,
    };
    let (name, datatype): (Box<str>, u32) = match (registered, metric.name.as_deref(), metric.alias)
    {
        (Some((name, datatype)), _, _) => (name.into(), datatype),
        // Named on the wire (a BIRTH metric, or a name-only DATA metric the
        // BIRTH did not list): fully decodable, no flag.
        (None, Some(name), _) => (name.into(), metric.datatype.unwrap_or(0)),
        (None, None, Some(alias)) => {
            out.flag(
                QualityFlags::DECODE_ERROR,
                format!("sparkplug alias {alias}"),
            );
            (
                format!("alias:{alias}").into(),
                metric.datatype.unwrap_or(0),
            )
        }
        (None, None, None) => {
            out.flag(
                QualityFlags::DECODE_ERROR,
                "sparkplug metric has neither name nor alias".into(),
            );
            ("unknown".into(), metric.datatype.unwrap_or(0))
        }
    };
    let value = match value_of(metric, datatype) {
        Some(value) => value,
        None => {
            out.flag(
                QualityFlags::DECODE_ERROR,
                format!("sparkplug unsupported value for {name}"),
            );
            MetricValue::Unsupported
        }
    };
    ResolvedMetric {
        name,
        datatype,
        is_null: value == MetricValue::Null,
        value,
        timestamp: metric.timestamp,
        alias: metric.alias,
    }
}

/// `None` for values this decoder does not interpret.
fn value_of(metric: &pb::payload::Metric, datatype: u32) -> Option<MetricValue> {
    use pb::DataType as D;
    if metric.is_null == Some(true) {
        return Some(MetricValue::Null);
    }
    Some(match metric.value.as_ref() {
        None => MetricValue::Null,
        // Signed integers travel as two's complement in the low bits of
        // the unsigned wire field (senders disagree on sign-extension), so
        // truncate to the declared width.
        Some(Value::IntValue(v)) => match datatype {
            d if d == D::Int8 as u32 => MetricValue::Int(i64::from(*v as u8 as i8)),
            d if d == D::Int16 as u32 => MetricValue::Int(i64::from(*v as u16 as i16)),
            d if d == D::Int32 as u32 => MetricValue::Int(i64::from(*v as i32)),
            _ => MetricValue::UInt(u64::from(*v)),
        },
        Some(Value::LongValue(v)) if datatype == D::Int64 as u32 => MetricValue::Int(*v as i64),
        Some(Value::LongValue(v)) => MetricValue::UInt(*v),
        Some(Value::FloatValue(v)) => MetricValue::F32(*v),
        Some(Value::DoubleValue(v)) => MetricValue::F64(*v),
        Some(Value::BooleanValue(v)) => MetricValue::Bool(*v),
        Some(Value::StringValue(v)) => MetricValue::Text(v.clone()),
        Some(Value::BytesValue(v)) => MetricValue::Bytes(v.clone()),
        Some(Value::DatasetValue(_) | Value::TemplateValue(_) | Value::ExtensionValue(_)) => {
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::topic;
    use pb::DataType as D;

    fn metric(
        name: Option<&str>,
        alias: Option<u64>,
        dt: D,
        value: Option<Value>,
    ) -> pb::payload::Metric {
        pb::payload::Metric {
            name: name.map(Into::into),
            alias,
            datatype: Some(dt as u32),
            is_null: value.is_none().then_some(true),
            value,
            ..Default::default()
        }
    }

    fn bd(n: u64) -> pb::payload::Metric {
        metric(
            Some(BD_SEQ_METRIC),
            None,
            D::Int64,
            Some(Value::LongValue(n)),
        )
    }

    fn payload(seq: Option<u64>, metrics: Vec<pb::payload::Metric>) -> Vec<u8> {
        pb::Payload {
            timestamp: Some(1_700_000_000_000),
            seq,
            metrics,
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn float(v: f32) -> Option<Value> {
        Some(Value::FloatValue(v))
    }

    /// NBIRTH: temp (alias 1, float) and rpm (alias 2, int32), session `b`.
    fn nbirth(b: u64) -> Vec<u8> {
        payload(
            Some(0),
            vec![
                bd(b),
                metric(Some("temp"), Some(1), D::Float, float(1.0)),
                metric(Some("rpm"), Some(2), D::Int32, Some(Value::IntValue(0))),
            ],
        )
    }

    /// DBIRTH for `dev1`: motor.speed alias 10, motor.amps alias 11.
    fn dbirth(seq: u64, speed: u64, amps: u64) -> Vec<u8> {
        payload(
            Some(seq),
            vec![
                metric(
                    Some("motor.speed"),
                    Some(speed),
                    D::Double,
                    Some(Value::DoubleValue(0.0)),
                ),
                metric(
                    Some("motor.amps"),
                    Some(amps),
                    D::Double,
                    Some(Value::DoubleValue(0.0)),
                ),
            ],
        )
    }

    fn ndata(seq: Option<u64>, alias: u64, v: f32) -> Vec<u8> {
        payload(seq, vec![metric(None, Some(alias), D::Float, float(v))])
    }

    fn t(s: &str) -> topic::Topic<'_> {
        topic::parse(s).unwrap()
    }

    fn feed(d: &mut Decoder, topic: &str, bytes: &[u8]) -> Outcome {
        d.handle(&t(topic), bytes).unwrap()
    }

    const NB: &str = "spBv1.0/g/NBIRTH/e1";
    const ND: &str = "spBv1.0/g/NDATA/e1";
    const NDTH: &str = "spBv1.0/g/NDEATH/e1";
    const DB: &str = "spBv1.0/g/DBIRTH/e1/dev1";
    const DD: &str = "spBv1.0/g/DDATA/e1/dev1";

    #[test]
    fn birth_registers_and_alias_only_data_resolves() {
        let mut d = Decoder::new();
        let birth = feed(&mut d, NB, &nbirth(7));
        assert_eq!(
            (birth.change, birth.bd_seq, birth.seq),
            (Some(Change::Birth), Some(7), Some(0))
        );
        let names: Vec<_> = birth.metrics.iter().map(|m| &*m.name).collect();
        assert_eq!(
            names,
            ["temp", "rpm"],
            "bdSeq is session plumbing, not data"
        );
        assert!(birth.flags.is_empty());

        let out = feed(&mut d, ND, &ndata(Some(1), 1, 21.5));
        assert_eq!(out.metrics.len(), 1);
        let m = &out.metrics[0];
        assert_eq!(
            (&*m.name, m.datatype, m.alias),
            ("temp", D::Float as u32, Some(1))
        );
        assert_eq!(
            (m.value.clone(), m.is_null),
            (MetricValue::F32(21.5), false)
        );
        assert_eq!(out.timestamp, Some(1_700_000_000_000));
        assert!(out.flags.is_empty());
        assert_eq!(out.status(), QualityStatus::Good);
    }

    #[test]
    fn device_tables_are_separate_and_signed_ints_sign_extend() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(0));
        let birth = feed(&mut d, DB, &dbirth(1, 1, 2));
        // Device aliases 1/2 do not collide with the node's 1/2.
        assert_eq!(birth.change, Some(Change::DeviceBirth));

        let data = payload(
            Some(2),
            vec![metric(
                None,
                Some(2),
                D::Double,
                Some(Value::DoubleValue(3.5)),
            )],
        );
        let out = feed(&mut d, DD, &data);
        assert_eq!(&*out.metrics[0].name, "motor.amps");

        // Node alias 2 is rpm (Int32): -5 arrives as 0xFFFFFFFB.
        let neg = payload(
            Some(3),
            vec![metric(
                None,
                Some(2),
                D::Int32,
                Some(Value::IntValue(0xFFFF_FFFB)),
            )],
        );
        let out = feed(&mut d, ND, &neg);
        assert_eq!(
            (&*out.metrics[0].name, out.metrics[0].value.clone()),
            ("rpm", MetricValue::Int(-5))
        );

        // Int8 -1 whether the sender masked (0xFF) or sign-extended.
        let int8 = |raw| {
            value_of(
                &metric(None, None, D::Int8, Some(Value::IntValue(raw))),
                D::Int8 as u32,
            )
        };
        assert_eq!(int8(0xFF), Some(MetricValue::Int(-1)));
        assert_eq!(int8(0xFFFF_FFFF), Some(MetricValue::Int(-1)));
    }

    #[test]
    fn unknown_alias_keeps_the_event_and_flags_decode_error() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(0));
        let out = feed(&mut d, ND, &ndata(Some(1), 99, 1.0));
        assert_eq!(&*out.metrics[0].name, "alias:99");
        assert_eq!(out.metrics[0].value, MetricValue::F32(1.0));
        assert_eq!(out.flags, QualityFlags::DECODE_ERROR);
        assert_eq!(out.notes, ["sparkplug alias 99"]);

        // No BIRTH at all for the node: same outcome, nothing created.
        let mut fresh = Decoder::new();
        let out = feed(&mut fresh, "spBv1.0/g/NDATA/other", &ndata(Some(5), 1, 1.0));
        assert_eq!(out.flags, QualityFlags::DECODE_ERROR);
        assert!(fresh.nodes.is_empty());
    }

    #[test]
    fn name_only_data_resolves_and_null_and_unsupported_values() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(0));
        let data = payload(
            Some(1),
            vec![
                metric(Some("temp"), None, D::Float, float(2.0)),
                metric(Some("rpm"), None, D::Int32, None),
                pb::payload::Metric {
                    name: Some("tbl".into()),
                    datatype: Some(D::DataSet as u32),
                    value: Some(Value::DatasetValue(pb::payload::DataSet::default())),
                    ..Default::default()
                },
            ],
        );
        let out = feed(&mut d, ND, &data);
        assert_eq!(out.metrics[0].value, MetricValue::F32(2.0));
        assert!(out.metrics[1].is_null && out.metrics[1].value == MetricValue::Null);
        assert_eq!(out.metrics[1].datatype, D::Int32 as u32);
        assert_eq!(out.metrics[2].value, MetricValue::Unsupported);
        assert_eq!(out.flags, QualityFlags::DECODE_ERROR);
    }

    #[test]
    fn seq_gap_wrap_and_missing_seq() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(0));
        // Walk 1..=255 then wrap to 0, 1: no gap anywhere.
        for seq in (1..=255).chain([0, 1]) {
            let out = feed(&mut d, ND, &ndata(Some(seq), 1, 0.0));
            assert!(out.flags.is_empty(), "seq {seq}: {:?}", out.notes);
        }
        // 3 skipped 2.
        let out = feed(&mut d, ND, &ndata(Some(3), 1, 0.0));
        assert_eq!(
            out.gap,
            Some(SeqGap {
                expected: 2,
                got: 3
            })
        );
        assert_eq!(out.flags, QualityFlags::SEQUENCE_GAP);
        assert_eq!(out.notes, ["sparkplug.seq expected 2 got 3"]);
        assert_eq!(out.status(), QualityStatus::Good);
        // The cursor moved on: 4 is clean again.
        assert!(feed(&mut d, ND, &ndata(Some(4), 1, 0.0)).flags.is_empty());

        // A message with no seq cannot be validated; the next one re-anchors.
        let out = feed(&mut d, ND, &ndata(None, 1, 0.0));
        assert_eq!(out.flags, QualityFlags::DECODE_ERROR);
        assert!(feed(&mut d, ND, &ndata(Some(9), 1, 0.0)).flags.is_empty());

        // Device messages share the node's stream.
        feed(&mut d, DB, &dbirth(10, 10, 11));
        let out = feed(&mut d, DD, &payload(Some(12), vec![]));
        assert_eq!(
            out.gap,
            Some(SeqGap {
                expected: 11,
                got: 12
            })
        );
    }

    #[test]
    fn death_marks_stale_and_a_late_will_is_ignored() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(7));
        feed(&mut d, DB, &dbirth(1, 10, 11));

        // The Will of an older session (bdSeq 6) must not kill session 7.
        let late = feed(&mut d, NDTH, &payload(None, vec![bd(6)]));
        assert_eq!((late.change, late.store.len()), (None, 0));
        assert!(feed(&mut d, ND, &ndata(Some(2), 1, 0.0)).flags.is_empty());

        let death = feed(&mut d, NDTH, &payload(None, vec![bd(7)]));
        assert_eq!(death.change, Some(Change::Death));
        assert_eq!(
            death.store,
            [StoreUpdate::Dead(BirthKey::new("g", "e1", None))]
        );
        // Stale data still resolves; the flag and status say it is stale.
        let out = feed(&mut d, ND, &ndata(Some(3), 1, 4.0));
        assert_eq!(&*out.metrics[0].name, "temp");
        assert_eq!(out.flags, QualityFlags::STALE);
        assert_eq!(out.notes, ["sparkplug.past-ndeath"]);
        assert_eq!(out.status(), QualityStatus::Uncertain);
        let dev = feed(
            &mut d,
            DD,
            &payload(
                Some(4),
                vec![metric(
                    None,
                    Some(10),
                    D::Double,
                    Some(Value::DoubleValue(1.0)),
                )],
            ),
        );
        assert!(dev.flags.contains(QualityFlags::STALE));
        assert_eq!(&*dev.metrics[0].name, "motor.speed");
    }

    #[test]
    fn device_death_stales_only_that_device() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(0));
        feed(&mut d, DB, &dbirth(1, 10, 11));
        let out = feed(
            &mut d,
            "spBv1.0/g/DDEATH/e1/dev1",
            &payload(Some(2), vec![]),
        );
        assert_eq!(out.change, Some(Change::DeviceDeath));
        assert_eq!(
            out.store,
            [StoreUpdate::Dead(BirthKey::new("g", "e1", Some("dev1")))]
        );
        assert!(
            feed(&mut d, DD, &payload(Some(3), vec![]))
                .flags
                .contains(QualityFlags::STALE)
        );
        assert!(feed(&mut d, ND, &ndata(Some(4), 1, 0.0)).flags.is_empty());

        let unknown = feed(
            &mut d,
            "spBv1.0/g/DDEATH/e1/ghost",
            &payload(Some(5), vec![]),
        );
        assert_eq!(unknown.change, None);
    }

    #[test]
    fn rebirth_clears_staleness_rebinds_aliases_and_resets_seq_without_a_gap() {
        let mut d = Decoder::new();
        feed(&mut d, NB, &nbirth(7));
        feed(&mut d, DB, &dbirth(1, 10, 11));
        feed(&mut d, NDTH, &payload(None, vec![bd(7)]));

        // Rebirth: bdSeq 8, alias 1 now means `vibration`, seq back to 0.
        let again = payload(
            Some(0),
            vec![
                bd(8),
                metric(Some("vibration"), Some(1), D::Float, float(0.0)),
            ],
        );
        let out = feed(&mut d, NB, &again);
        assert_eq!((out.change, out.bd_seq), (Some(Change::Rebirth), Some(8)));
        assert!(out.flags.is_empty(), "a seq reset on NBIRTH is not a gap");

        let out = feed(&mut d, ND, &ndata(Some(1), 1, 3.0));
        assert_eq!(&*out.metrics[0].name, "vibration");
        assert!(out.flags.is_empty());
        // The old session's alias 2 (rpm) is gone.
        assert_eq!(
            feed(&mut d, ND, &ndata(Some(2), 2, 0.0)).flags,
            QualityFlags::DECODE_ERROR
        );

        // The device was invalidated by the NBIRTH: stale and unresolved
        // until its own DBIRTH, which brings it back with swapped aliases.
        let dev = feed(
            &mut d,
            DD,
            &payload(
                Some(3),
                vec![metric(
                    None,
                    Some(10),
                    D::Double,
                    Some(Value::DoubleValue(1.0)),
                )],
            ),
        );
        assert_eq!(dev.flags, QualityFlags::STALE | QualityFlags::DECODE_ERROR);
        feed(&mut d, DB, &dbirth(4, 11, 10));
        let dev = feed(
            &mut d,
            DD,
            &payload(
                Some(5),
                vec![metric(
                    None,
                    Some(10),
                    D::Double,
                    Some(Value::DoubleValue(1.0)),
                )],
            ),
        );
        assert!(dev.flags.is_empty());
        assert_eq!(&*dev.metrics[0].name, "motor.amps");
    }

    #[test]
    fn state_cmd_and_odd_topics() {
        let mut d = Decoder::new();
        // STATE is retained JSON: never parsed as protobuf, never an error.
        let out = feed(&mut d, "spBv1.0/g/STATE/host-1", br#"{"online":true}"#);
        assert!(out.metrics.is_empty() && out.flags.is_empty());
        assert_eq!(out.notes.len(), 1);
        let out = feed(&mut d, "spBv1.0/g/NCMD/e1", b"\xff\xff");
        assert!(out.flags.is_empty());

        // DBIRTH on a node-shaped topic is flagged, not attributed.
        let out = feed(&mut d, "spBv1.0/g/DBIRTH/e1", &dbirth(0, 1, 2));
        assert_eq!(out.flags, QualityFlags::DECODE_ERROR);
        assert!(out.store.is_empty());

        let err = d.handle(&t(NB), b"\x0a\xff\xff\xff").unwrap_err();
        assert_eq!(err.code, codes::DECODE_INVALID_PAYLOAD);
    }

    #[test]
    fn restart_with_seed_resolves_data_without_a_new_birth() {
        let dir = tempfile::tempdir().unwrap();
        let store = BirthStore::open(dir.path().join("births"));
        let mut d = Decoder::new();
        for (topic, bytes) in [
            (NB, nbirth(7)),
            (DB, dbirth(1, 10, 11)),
            ("spBv1.0/g/NBIRTH/e2", nbirth(3)),
            ("spBv1.0/g/NDEATH/e2", payload(None, vec![bd(3)])),
        ] {
            for update in feed(&mut d, topic, &bytes).store {
                update.apply(&store).unwrap();
            }
        }

        // Restart: a new decoder from the store alone.
        let mut d = Decoder::seeded(store.load_all().unwrap()).unwrap();
        // First seq after the restart anchors the cursor: no false gap, even
        // though the BIRTH's seq was 0.
        let out = feed(&mut d, ND, &ndata(Some(41), 1, 9.0));
        assert_eq!(&*out.metrics[0].name, "temp");
        assert_eq!(out.bd_seq, Some(7));
        assert!(out.flags.is_empty(), "{:?}", out.notes);
        assert!(
            feed(&mut d, ND, &ndata(Some(43), 1, 9.0))
                .flags
                .contains(QualityFlags::SEQUENCE_GAP)
        );
        let dev = feed(
            &mut d,
            DD,
            &payload(
                Some(44),
                vec![metric(
                    None,
                    Some(11),
                    D::Double,
                    Some(Value::DoubleValue(1.0)),
                )],
            ),
        );
        assert_eq!(&*dev.metrics[0].name, "motor.amps");
        assert!(dev.flags.is_empty());

        // The death persisted: node e2 comes back stale.
        let out = feed(&mut d, "spBv1.0/g/NDATA/e2", &ndata(Some(9), 1, 0.0));
        assert_eq!(&*out.metrics[0].name, "temp");
        assert_eq!(out.flags, QualityFlags::STALE);
    }

    #[test]
    fn seeded_device_from_an_older_session_or_dead_node_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let store = BirthStore::open(dir.path().join("births"));
        let mut d = Decoder::new();
        for (topic, bytes) in [(NB, nbirth(7)), (DB, dbirth(1, 10, 11)), (NB, nbirth(8))] {
            for update in feed(&mut d, topic, &bytes).store {
                update.apply(&store).unwrap();
            }
        }
        // The store holds device bd 7 under node bd 8: the DBIRTH never came
        // back after the rebirth, exactly as in memory.
        let mut seeded = Decoder::seeded(store.load_all().unwrap()).unwrap();
        let dev = payload(
            Some(2),
            vec![metric(
                None,
                Some(10),
                D::Double,
                Some(Value::DoubleValue(1.0)),
            )],
        );
        let out = feed(&mut seeded, DD, &dev);
        assert_eq!(out.flags, QualityFlags::STALE | QualityFlags::DECODE_ERROR);

        // A live device record under a dead node loads dead.
        let mut live = Decoder::new();
        let mut updates = feed(&mut live, NB, &nbirth(7)).store;
        updates.extend(feed(&mut live, DB, &dbirth(1, 10, 11)).store);
        updates.extend(feed(&mut live, NDTH, &payload(None, vec![bd(7)])).store);
        let dir2 = tempfile::tempdir().unwrap();
        let store2 = BirthStore::open(dir2.path().join("births"));
        for u in &updates {
            u.apply(&store2).unwrap();
        }
        let mut seeded = Decoder::seeded(store2.load_all().unwrap()).unwrap();
        let out = feed(&mut seeded, DD, &dev);
        assert_eq!(out.flags, QualityFlags::STALE);
        assert_eq!(&*out.metrics[0].name, "motor.speed");
    }

    #[test]
    fn a_stored_birth_that_does_not_decode_fails_seeding() {
        let record = BirthRecord {
            birth: b"\x0a\xff\xff\xff".to_vec(),
            bd_seq: None,
            seq: None,
            dead: false,
        };
        let err = Decoder::seeded([(BirthKey::new("g", "e1", None), record)]).unwrap_err();
        assert_eq!(err.code, codes::DECODE_INVALID_PAYLOAD);
    }
}
