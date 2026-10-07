//! Builders for messages, events, and batches used across the memory
//! connectors and conformance suites.

use std::sync::OnceLock;

use bytes::Bytes;
use thadam_core::{
    Event, EventBatch, EventId, FieldDef, FieldType, Record, Shape, ShapeRef, TimeDomain, Times,
    Timestamp, Value, WalEpoch, WalPos, WalRange,
};

/// A payload whose first eight bytes are `id` in big-endian order, so suites
/// can tell which message they are looking at after it travelled through a
/// connector.
pub fn id_payload(id: u64) -> Bytes {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(b"thadam-testkit");
    Bytes::from(buf)
}

/// The id [`id_payload`] encoded, if `payload` is one.
pub fn payload_id(payload: &[u8]) -> Option<u64> {
    let head: [u8; 8] = payload.get(..8)?.try_into().ok()?;
    Some(u64::from_be_bytes(head))
}

fn test_shape() -> ShapeRef {
    static SHAPE: OnceLock<ShapeRef> = OnceLock::new();
    SHAPE
        .get_or_init(|| {
            let fields = vec![FieldDef::new("n", FieldType::I64).required()];
            ShapeRef::new(Shape::new(fields).unwrap_or_else(|e| panic!("static test shape: {e}")))
        })
        .clone()
}

/// An event captured in WAL frame `wal_pos`, whose single field `n` is `n`.
pub fn test_event(n: i64, wal_pos: u64) -> Event {
    let times = Times::new(Timestamp::from_nanos(n), TimeDomain::Utc);
    let record = Record::new(test_shape(), vec![Value::I64(n)])
        .unwrap_or_else(|e| panic!("test record: {e}"));
    Event::builder(
        EventId::from_u128(u128::from(wal_pos)),
        WalPos::new(wal_pos),
        times,
    )
    .payload(record)
    .build()
    .unwrap_or_else(|e| panic!("test event: {e}"))
}

/// A batch read from frames `first..=last` of epoch `epoch`, holding one event
/// per frame.
pub fn test_batch(epoch: u64, first: u64, last: u64) -> EventBatch {
    let origin = WalRange::new(WalEpoch::new(epoch), WalPos::new(first), WalPos::new(last))
        .unwrap_or_else(|e| panic!("test batch range: {e}"));
    let events = (first..=last)
        .map(|pos| test_event(i64::try_from(pos).unwrap_or(i64::MAX), pos))
        .collect();
    EventBatch::new(events, origin, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_ids_round_trip() {
        for id in [0, 1, u64::MAX, 0xdead_beef] {
            assert_eq!(payload_id(&id_payload(id)), Some(id));
        }
        assert_eq!(payload_id(b"short"), None);
    }

    #[test]
    fn test_batches_cover_their_range() {
        let b = test_batch(3, 5, 8);
        assert_eq!(b.len(), 4);
        assert_eq!(b.origin.first(), WalPos::new(5));
        assert_eq!(b.origin.last(), WalPos::new(8));
        assert_eq!(b.events[0].wal_pos(), WalPos::new(5));
        assert!(b.events.iter().all(|e| e.payload().as_record().is_some()));
    }
}
