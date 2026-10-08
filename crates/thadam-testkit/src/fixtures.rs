//! Builders for messages, events, and batches used across the memory
//! connectors and conformance suites.

use std::sync::OnceLock;

use bytes::Bytes;
use thadam_core::{
    DatasetEvents, Event, EventBatch, EventId, FieldDef, FieldType, Record, SegmentBatch, Shape,
    ShapeRef, TimeDomain, Times, Timestamp, Value, WalEpoch, WalPos, WalRange,
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

/// The dataset [`test_segment_batch`] puts its events under.
pub const TEST_DATASET: &str = "events";

/// [`test_batch`] as a [`SegmentBatch`] holding the single dataset
/// [`TEST_DATASET`].
pub fn test_segment_batch(epoch: u64, first: u64, last: u64) -> SegmentBatch {
    SegmentBatch::single(TEST_DATASET, test_batch(epoch, first, last))
}

/// A batch over frames `first..=last` with one event per frame, spread over
/// `datasets` by frame position (`frame % datasets.len()`), every dataset
/// listed even when it receives no event.
///
/// # Panics
///
/// If `datasets` is empty.
pub fn test_segment_batch_across(
    epoch: u64,
    first: u64,
    last: u64,
    datasets: &[&str],
) -> SegmentBatch {
    assert!(!datasets.is_empty(), "at least one dataset");
    let source = test_batch(epoch, first, last);
    let mut groups: Vec<DatasetEvents> = datasets
        .iter()
        .map(|d| DatasetEvents::new(*d, Vec::new()))
        .collect();
    let width = datasets.len() as u64;
    for event in source.events {
        let slot = usize::try_from(event.wal_pos().get() % width).unwrap_or(0);
        groups[slot].events.push(event);
    }
    SegmentBatch::new(source.origin, source.clock_synced, groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_batches_group_events_by_dataset() {
        let b = test_segment_batch_across(1, 1, 6, &["a", "b", "c"]);
        assert_eq!(b.datasets.len(), 3);
        assert_eq!(b.len(), 6);
        let frames = |name: &str| -> Vec<u64> {
            b.dataset(name)
                .unwrap()
                .events
                .iter()
                .map(|e| e.wal_pos().get())
                .collect()
        };
        assert_eq!(frames("a"), vec![3, 6]);
        assert_eq!(frames("b"), vec![1, 4]);
        assert_eq!(frames("c"), vec![2, 5]);

        let sparse = test_segment_batch_across(1, 4, 4, &["a", "b", "c"]);
        assert_eq!(sparse.datasets.len(), 3, "empty datasets stay listed");
        assert_eq!(sparse.len(), 1);

        let one = test_segment_batch(2, 1, 3);
        assert_eq!(one.len(), 3);
        assert_eq!(&*one.datasets[0].dataset, TEST_DATASET);
    }

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
