//! Rejects as events of the fixed quarantine schema, and the per-sink
//! quarantine writer.
//!
//! Schema (ARCHITECTURE.md section 8, plus the sink and dataset the
//! implementation plan lists): `_wal_epoch`, `_wal_pos`, `_record_index`,
//! `_ingest_time`, `source_id`, `topic`, `sink_id`, `dataset`,
//! `error_domain`, `error_code`, `reason`, `raw` (truncated at 64 KiB), and
//! `raw_truncated`. A reject raised by a sink after decoding has no raw bytes:
//! `raw` is null and `_record_index` and `source_id` are unknown.

use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use thadam_core::{
    Event, EventBatch, EventId, FieldDef, FieldType, Record, ThadamError, SegmentSink, Shape,
    ShapeRef, SinkContext, Sym, TimeDomain, Times, Timestamp, Utf8Bytes, Value, WalEpoch, WalPos,
    WalRange,
};
use thadam_transform::RejectedRaw;

use crate::metrics::MetricEvent;
use crate::status::{Component, add};

/// Raw payloads are cut to this many bytes in quarantine.
pub const QUARANTINE_RAW_MAX: usize = 64 << 10;

/// The quarantine record shape.
pub fn quarantine_shape() -> ShapeRef {
    static SHAPE: OnceLock<ShapeRef> = OnceLock::new();
    SHAPE
        .get_or_init(|| {
            let f = |name: &str, ty: FieldType| FieldDef::new(name, ty);
            let fields = vec![
                f("_wal_epoch", FieldType::U64).required(),
                f("_wal_pos", FieldType::U64).required(),
                f("_record_index", FieldType::U32),
                f("_ingest_time", FieldType::Ts),
                f("source_id", FieldType::Str),
                f("topic", FieldType::Str),
                f("sink_id", FieldType::Str).required(),
                f("dataset", FieldType::Str),
                f("error_domain", FieldType::Str).required(),
                f("error_code", FieldType::Str).required(),
                f("reason", FieldType::Str).required(),
                f("raw", FieldType::Bytes),
                f("raw_truncated", FieldType::Bool).required(),
            ];
            match Shape::new(fields) {
                Ok(s) => ShapeRef::new(s),
                Err(e) => unreachable!("the quarantine shape is static and valid: {e}"),
            }
        })
        .clone()
}

/// One rejected record or event.
pub(crate) enum Rejected {
    /// The chain could not turn a raw record into an event.
    Raw(Box<RejectedRaw>),
    /// A sink refused a decoded event.
    Event {
        epoch: WalEpoch,
        pos: WalPos,
        ingest: Timestamp,
        dataset: Option<Sym>,
        error: ThadamError,
    },
}

impl Rejected {
    fn pos(&self) -> WalPos {
        match self {
            Rejected::Raw(r) => r.pos,
            Rejected::Event { pos, .. } => *pos,
        }
    }
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

fn text(s: &str) -> Value {
    Value::Str(Utf8Bytes::from(s))
}

fn to_event(r: Rejected, sink_id: &str, seq: u64) -> Option<Event> {
    let shape = quarantine_shape();
    let (epoch, pos, idx, ingest, source, topic, dataset, error, raw) = match r {
        Rejected::Raw(r) => {
            let r = *r;
            let topic = r.attrs.get("topic").map(str::to_owned);
            (
                r.epoch,
                r.pos,
                Some(r.record_index),
                r.ingest_time,
                Some(r.source.as_str().to_owned()),
                topic,
                r.dataset,
                r.error,
                Some(r.payload),
            )
        }
        Rejected::Event {
            epoch,
            pos,
            ingest,
            dataset,
            error,
        } => (epoch, pos, None, ingest, None, None, dataset, error, None),
    };
    let (raw, truncated) = match raw {
        Some(p) if p.len() > QUARANTINE_RAW_MAX => (Some(p.slice(..QUARANTINE_RAW_MAX)), true),
        other => (other, false),
    };
    let opt = |s: Option<String>| s.map_or(Value::Null, |s| text(&s));
    let values = vec![
        Value::U64(epoch.get()),
        Value::U64(pos.get()),
        idx.map_or(Value::Null, Value::U32),
        Value::Ts(ingest),
        opt(source),
        opt(topic),
        text(sink_id),
        dataset.map_or(Value::Null, |d| text(&d)),
        text(error.domain.as_str()),
        text(error.code),
        text(&error.message),
        raw.map_or(Value::Null, |b: Bytes| Value::bytes(b)),
        Value::Bool(truncated),
    ];
    let record = Record::new(shape, values).ok()?;
    let hi = mix(epoch.get() ^ mix(pos.get()));
    let lo = mix(u64::from(idx.unwrap_or(u32::MAX)) ^ (seq << 32) ^ 0x5155_4152);
    let id = EventId::from_u128((u128::from(hi) << 64) | u128::from(lo));
    Event::builder(id, pos, Times::new(ingest, TimeDomain::Unknown))
        .payload(record)
        .build()
        .ok()
}

/// A sink's quarantine instance. The cursor of the sink it belongs to must
/// stay below [`Quarantine::floor`] until the rejects are durable.
pub(crate) struct Quarantine {
    pub sink: Box<dyn SegmentSink>,
    pub ctx: SinkContext,
    owner: String,
    component: Arc<Component>,
    /// Positions at or below this are already in quarantine.
    floor_pos: u64,
    watermark: u64,
    pending: VecDeque<WalRange>,
}

impl Quarantine {
    pub fn new(
        sink: Box<dyn SegmentSink>,
        ctx: SinkContext,
        owner: String,
        component: Arc<Component>,
    ) -> Self {
        Quarantine {
            sink,
            ctx,
            owner,
            component,
            floor_pos: 0,
            watermark: 0,
            pending: VecDeque::new(),
        }
    }

    pub async fn recover(&mut self) -> Result<u64, ThadamError> {
        let pos = self.sink.recover(&self.ctx).await?.map_or(0, WalPos::get);
        self.floor_pos = self.floor_pos.max(pos);
        self.watermark = self.watermark.max(pos);
        Ok(pos)
    }

    /// The highest position the owner's cursor may reach, if rejects are
    /// waiting to become durable.
    pub fn floor(&self) -> Option<u64> {
        self.pending
            .front()
            .map(|r| r.first().get().saturating_sub(1))
    }

    /// Appends `rejected` (all inside `origin`) as one batch. Rejects at or
    /// below what quarantine already holds are skipped, so re-reading after a
    /// restart or rewind never duplicates them.
    pub async fn push(
        &mut self,
        origin: WalRange,
        clock_synced: bool,
        rejected: Vec<Rejected>,
    ) -> Result<(), ThadamError> {
        let floor = self.floor_pos;
        let rejected: Vec<Rejected> = rejected
            .into_iter()
            .filter(|r| r.pos().get() > floor)
            .collect();
        if rejected.is_empty() || origin.last().get() <= floor {
            return Ok(());
        }
        let first = origin.first().get().max(floor + 1);
        let range =
            WalRange::new(origin.epoch(), WalPos::new(first), origin.last()).unwrap_or(origin);
        let events: Vec<Event> = rejected
            .into_iter()
            .enumerate()
            .filter_map(|(i, r)| to_event(r, &self.owner, i as u64))
            .collect();
        let n = events.len();
        let refused = self
            .sink
            .append(EventBatch::new(events, range, clock_synced))
            .await?;
        if !refused.is_empty() {
            tracing::error!(sink = %self.owner, count = refused.len(), "quarantine refused rejects");
            self.component
                .metrics()
                .record(MetricEvent::RejectsDropped {
                    sink: &self.owner,
                    count: refused.len(),
                });
        }
        let kept = n - refused.len().min(n);
        add(&self.component.counters.quarantined, kept as u64);
        self.component.metrics().record(MetricEvent::Quarantined {
            sink: &self.owner,
            count: kept,
        });
        self.floor_pos = range.last().get();
        self.pending.push_back(range);
        Ok(())
    }

    fn settle(&mut self, wm: Option<WalPos>) {
        if let Some(w) = wm {
            self.watermark = self.watermark.max(w.get());
        }
        while self
            .pending
            .front()
            .is_some_and(|r| r.last().get() <= self.watermark)
        {
            self.pending.pop_front();
        }
    }

    pub async fn poll(&mut self) -> Result<(), ThadamError> {
        let wm = self.sink.poll_commit().await?;
        self.settle(wm);
        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), ThadamError> {
        let wm = self.sink.finalize_all().await?;
        self.settle(wm);
        Ok(())
    }

    pub fn cursor_committed(&mut self, pos: u64) {
        let p = pos.min(self.watermark);
        if p > 0 {
            self.sink.cursor_committed(WalPos::new(p));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::{Attributes, ErrorDomain, SourceId, codes};

    #[test]
    fn raw_rejects_become_quarantine_rows_with_truncation() {
        let mut attrs = Attributes::new();
        attrs.insert("topic", "a/b");
        let raw = RejectedRaw {
            error: ThadamError::permanent(ErrorDomain::Decode, codes::DECODE_INVALID_PAYLOAD, "bad"),
            dataset: Some(Sym::from("d")),
            source: SourceId::new("src"),
            epoch: WalEpoch::new(9),
            pos: WalPos::new(4),
            record_index: 2,
            sample_idx: None,
            ingest_time: Timestamp::from_nanos(5),
            attrs,
            payload: Bytes::from(vec![7u8; QUARANTINE_RAW_MAX + 10]),
        };
        let e = to_event(Rejected::Raw(Box::new(raw)), "sink", 0).unwrap();
        let rec = e.payload().as_record().unwrap();
        assert_eq!(rec.get("_wal_pos"), Some(&Value::U64(4)));
        assert_eq!(rec.get("topic"), Some(&text("a/b")));
        assert_eq!(
            rec.get("error_code"),
            Some(&text(codes::DECODE_INVALID_PAYLOAD))
        );
        assert_eq!(rec.get("raw_truncated"), Some(&Value::Bool(true)));
        match rec.get("raw") {
            Some(Value::Bytes(b)) => assert_eq!(b.len(), QUARANTINE_RAW_MAX),
            other => panic!("raw is {other:?}"),
        }
        assert_eq!(e.wal_pos(), WalPos::new(4));
    }
}
