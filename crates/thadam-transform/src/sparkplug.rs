//! The dataset `decode: sparkplug_b` payload decoder.
//!
//! One [`SparkplugDecode`] per chain wraps the stateful
//! [`thadam_sparkplug::decode::Decoder`] and turns each message into the JSON
//! document dataset fields read, exactly like the `decode: ros2` bridge:
//!
//! ```text
//! { "group": "plantA", "node": "edge1", "device": "dev1" | null,
//!   "kind": "DDATA", "seq": 12 | null, "bd_seq": 7 | null,
//!   "timestamp": 1700000000000 | null,        // payload time, ms since epoch
//!   "metrics": { "motor.speed": 1.5, ... },   // metric name -> value
//!   "samples": [ { "name", "datatype", "value", "is_null",
//!                  "timestamp", "alias" }, ... ] }   // only for `explode`
//! ```
//!
//! **Why one event per message, with `explode` available.** A message's
//! metrics are one atomic observation (siblings of one scan); one row per
//! message keeps that, matches `decode: json`/`ros2`, and keeps the row rate
//! at the message rate. Declared fields must be typed, so the object form
//! (`from: /metrics/motor.speed`, JSON-Pointer `~1` for a `/` in a name)
//! fits a known metric set. Per-metric rows are the existing `explode` over
//! `/samples`: it fits homogeneous or unknown-width metric sets, at the cost
//! of one declared `value` type for every metric (a metric of another type
//! rejects that row), so mixed sets should use the object form.
//!
//! BIRTH and DEATH messages are emitted like DATA (`kind` says which; a
//! BIRTH's `metrics` are its initial values, a DEATH's are empty). STATE
//! and CMD messages are not telemetry and Thadam never publishes either: they
//! are dropped (counted as `filtered`); the raw bytes stay in the WAL.
//!
//! Quality: the decoder's flags become the event's quality flags and, for
//! `STALE`, status `Uncertain`; the reason text names the observation
//! (`sparkplug.seq expected N got M`, `sparkplug alias N`,
//! `sparkplug.past-ndeath`).
//!
//! Durability: BIRTH/DEATH state changes are fsynced into the birth store
//! **before** the message's events are returned, so a restart can seed the
//! decoder when the WAL no longer holds the BIRTH. A failed write rejects
//! the message with the store's `buffer.*` error; it is never swallowed.
//! Errors never contain payload bytes.

use std::path::Path;

use thadam_core::{
    BirthStore, ErrorDomain, Quality, QualityFlags, QualityStatus, ThadamError, Sym, codes,
};
use thadam_sparkplug::decode::{Decoder, MetricValue, Outcome, datatype_name};
use thadam_sparkplug::topic;
use serde_json::{Map, Value as Json, json};

use crate::input::RawRecordView;

/// How many reason notes an event's `_quality_reason` carries before it
/// summarizes the rest.
const MAX_REASONS: usize = 3;

/// A decoded message: the JSON document and the quality it carries.
pub(crate) struct SparkplugMsg {
    pub(crate) root: Json,
    pub(crate) quality: Quality,
}

pub(crate) struct SparkplugDecode {
    decoder: Decoder,
    /// `None` when seed-only (replay).
    store: Option<BirthStore>,
}

impl SparkplugDecode {
    /// A decoder seeded from the birth store at `dir`; writes back to it
    /// unless `read_only`. A damaged store is an error, never skipped state.
    pub(crate) fn new(dir: &Path, read_only: bool) -> Result<Self, ThadamError> {
        let store = BirthStore::open(dir);
        let decoder = Decoder::seeded(store.load_all()?)?;
        Ok(SparkplugDecode {
            decoder,
            store: (!read_only).then_some(store),
        })
    }

    /// Decodes one message; `Ok(None)` for STATE and CMD, which produce no
    /// event. `persist: false` (warm-up) updates memory but not the store:
    /// the messages were persisted when they were first processed.
    pub(crate) fn decode(
        &mut self,
        view: &RawRecordView<'_>,
        persist: bool,
        samples: bool,
    ) -> Result<Option<SparkplugMsg>, ThadamError> {
        let invalid = |message: &str| {
            ThadamError::permanent(ErrorDomain::Decode, codes::DECODE_INVALID_PAYLOAD, message)
        };
        let topic = view
            .topic()
            .ok_or_else(|| invalid("the message carries no `topic` attribute"))?;
        let topic = topic::parse(topic)
            .ok_or_else(|| invalid("the topic is not in the Sparkplug `spBv1.0` namespace"))?;
        let outcome = self.decoder.handle(&topic, view.payload)?;
        if !topic.kind.carries_seq() {
            return Ok(None);
        }
        if persist && let Some(store) = &self.store {
            for update in &outcome.store {
                update.apply(store)?;
            }
        }
        Ok(Some(message(&topic, outcome, samples)))
    }
}

fn message(topic: &topic::Topic<'_>, outcome: Outcome, samples: bool) -> SparkplugMsg {
    let mut flags = outcome.flags;
    let mut notes = outcome.notes.clone();
    let mut metrics = Map::new();
    let mut list = Vec::new();
    for m in &outcome.metrics {
        let (value, lossy) = value_json(&m.value);
        if lossy {
            flags |= QualityFlags::DECODE_ERROR;
            notes.push(format!("sparkplug non-finite float for {}", m.name));
        }
        if samples {
            list.push(json!({
                "name": &*m.name,
                "datatype": datatype_name(m.datatype),
                "value": value,
                "is_null": m.is_null,
                "timestamp": m.timestamp,
                "alias": m.alias,
            }));
        }
        metrics.insert(m.name.to_string(), value);
    }
    let mut root = json!({
        "group": topic.group,
        "node": topic.node,
        "device": topic.device,
        "kind": topic.kind.as_str(),
        "seq": outcome.seq,
        "bd_seq": outcome.bd_seq,
        "timestamp": outcome.timestamp,
        "metrics": metrics,
    });
    if samples && let Some(object) = root.as_object_mut() {
        object.insert("samples".into(), Json::Array(list));
    }
    let status = if flags.contains(QualityFlags::STALE) {
        QualityStatus::Uncertain
    } else {
        QualityStatus::Good
    };
    SparkplugMsg {
        root,
        quality: Quality {
            status,
            flags,
            reason: reason(flags, &notes),
        },
    }
}

/// Notes only explain flags: informational notes on a clean message would
/// add a reason to every row.
fn reason(flags: QualityFlags, notes: &[String]) -> Option<Sym> {
    if flags.is_empty() || notes.is_empty() {
        return None;
    }
    let mut text = notes
        .iter()
        .take(MAX_REASONS)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("; ");
    if notes.len() > MAX_REASONS {
        text.push_str(&format!(" (+{} more)", notes.len() - MAX_REASONS));
    }
    Some(Sym::from(text.as_str()))
}

/// The JSON spelling of a value, and whether it was lossy (a non-finite
/// float, which JSON cannot carry, becomes null).
fn value_json(value: &MetricValue) -> (Json, bool) {
    match value {
        MetricValue::Null | MetricValue::Unsupported => (Json::Null, false),
        MetricValue::Int(v) => (Json::from(*v), false),
        MetricValue::UInt(v) => (Json::from(*v), false),
        MetricValue::F32(v) => finite(f64::from(*v)),
        MetricValue::F64(v) => finite(*v),
        MetricValue::Bool(v) => (Json::from(*v), false),
        MetricValue::Text(v) => (Json::from(v.as_str()), false),
        MetricValue::Bytes(v) => (Json::from(v.clone()), false),
    }
}

fn finite(v: f64) -> (Json, bool) {
    serde_json::Number::from_f64(v).map_or((Json::Null, true), |n| (Json::Number(n), false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_finite_floats_become_null_and_are_flagged_lossy() {
        assert_eq!(value_json(&MetricValue::F64(f64::NAN)), (Json::Null, true));
        assert_eq!(
            value_json(&MetricValue::F32(f32::INFINITY)),
            (Json::Null, true)
        );
        assert_eq!(value_json(&MetricValue::F32(21.5)), (json!(21.5), false));
        assert_eq!(value_json(&MetricValue::Int(-5)), (json!(-5), false));
        assert_eq!(
            value_json(&MetricValue::UInt(u64::MAX)),
            (json!(u64::MAX), false)
        );
    }

    #[test]
    fn reasons_cap_and_only_accompany_flags() {
        let notes: Vec<String> = (0..5).map(|i| format!("sparkplug alias {i}")).collect();
        assert_eq!(reason(QualityFlags::empty(), &notes), None);
        let text = reason(QualityFlags::DECODE_ERROR, &notes).unwrap();
        assert_eq!(
            &*text,
            "sparkplug alias 0; sparkplug alias 1; sparkplug alias 2 (+2 more)"
        );
    }
}
