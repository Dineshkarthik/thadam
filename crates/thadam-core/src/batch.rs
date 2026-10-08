//! Batches of raw messages (source side) and events (sink side).

use bytes::Bytes;

use crate::event::Event;
use crate::identity::{Attributes, SourceId};
use crate::position::WalRange;
use crate::text::Sym;
use crate::time::Timestamp;

/// One message exactly as a source received it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawMessage {
    /// When the edge received it. Mandatory.
    pub ingest_time: Timestamp,
    /// Protocol metadata such as topic and QoS.
    pub attrs: Attributes,
    /// The payload bytes.
    pub payload: Bytes,
}

impl RawMessage {
    /// A message with no attributes.
    pub fn new(ingest_time: Timestamp, payload: Bytes) -> Self {
        RawMessage {
            ingest_time,
            attrs: Attributes::new(),
            payload,
        }
    }
}

/// Messages from one source, committed to the WAL as a unit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawBatch {
    /// The source the messages came from.
    pub source: SourceId,
    /// The messages in receive order.
    pub messages: Vec<RawMessage>,
    /// Whether the kernel reported the clock as synchronized when received.
    pub clock_synced: bool,
}

impl RawBatch {
    /// A batch of `messages` from `source`.
    pub fn new(source: SourceId, messages: Vec<RawMessage>, clock_synced: bool) -> Self {
        RawBatch {
            source,
            messages,
            clock_synced,
        }
    }

    /// Number of messages.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Whether the batch has no messages. An empty batch is still
    /// acknowledged, in order.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Total payload bytes, for byte accounting.
    pub fn payload_bytes(&self) -> usize {
        self.messages.iter().map(|m| m.payload.len()).sum()
    }
}

/// Decoded events formed from one or more WAL frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventBatch {
    /// The events in WAL order.
    pub events: Vec<Event>,
    /// The WAL range the events were read from, including frames that
    /// produced no events for this consumer.
    pub origin: WalRange,
    /// Whether the kernel reported the clock as synchronized.
    pub clock_synced: bool,
}

impl EventBatch {
    /// A batch of `events` read from `origin`.
    pub fn new(events: Vec<Event>, origin: WalRange, clock_synced: bool) -> Self {
        EventBatch {
            events,
            origin,
            clock_synced,
        }
    }

    /// Number of events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the batch has no events.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// The dataset a [`SegmentSink`](crate::SegmentSink) quarantine instance
/// receives rejects under.
pub const QUARANTINE_DATASET: &str = "_quarantine";

/// One dataset's events inside a [`SegmentBatch`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetEvents {
    /// The dataset id from the configuration.
    pub dataset: Sym,
    /// The dataset's events, in WAL order. May be empty.
    pub events: Vec<Event>,
}

impl DatasetEvents {
    /// `events` of dataset `dataset`.
    pub fn new(dataset: impl Into<Sym>, events: Vec<Event>) -> Self {
        DatasetEvents {
            dataset: dataset.into(),
            events,
        }
    }
}

/// What one chain read gives a [`SegmentSink`](crate::SegmentSink): the events
/// of every dataset of the chain, grouped by dataset, over one WAL range.
///
/// `datasets` lists every dataset of the chain in declaration order, including
/// those with no events in this range, so a sink that keeps one open file per
/// dataset advances them all together. The `origin` covers the frames the read
/// consumed, including frames that produced no events at all; a sink may treat
/// the whole range as seen once `append` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentBatch {
    /// The WAL range every dataset's events were read from.
    pub origin: WalRange,
    /// Whether the kernel reported the clock as synchronized.
    pub clock_synced: bool,
    /// The events per dataset.
    pub datasets: Vec<DatasetEvents>,
}

impl SegmentBatch {
    /// A batch of `datasets` read from `origin`.
    pub fn new(origin: WalRange, clock_synced: bool, datasets: Vec<DatasetEvents>) -> Self {
        SegmentBatch {
            origin,
            clock_synced,
            datasets,
        }
    }

    /// A batch holding one dataset, taken from an [`EventBatch`].
    pub fn single(dataset: impl Into<Sym>, batch: EventBatch) -> Self {
        SegmentBatch {
            origin: batch.origin,
            clock_synced: batch.clock_synced,
            datasets: vec![DatasetEvents::new(dataset, batch.events)],
        }
    }

    /// Events across all datasets.
    pub fn len(&self) -> usize {
        self.datasets.iter().map(|d| d.events.len()).sum()
    }

    /// Whether no dataset has an event.
    pub fn is_empty(&self) -> bool {
        self.datasets.iter().all(|d| d.events.is_empty())
    }

    /// Every event with its dataset: datasets in declaration order, events in
    /// WAL order within each. A [`Reject::index`](crate::Reject::index) returned
    /// from `append` counts in this order.
    pub fn events(&self) -> impl Iterator<Item = (&Sym, &Event)> {
        self.datasets
            .iter()
            .flat_map(|d| d.events.iter().map(move |e| (&d.dataset, e)))
    }

    /// The events of dataset `id`, if the batch lists it.
    pub fn dataset(&self, id: &str) -> Option<&DatasetEvents> {
        self.datasets.iter().find(|d| &*d.dataset == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::{WalEpoch, WalPos};

    fn range(first: u64, last: u64) -> WalRange {
        WalRange::new(WalEpoch::new(1), WalPos::new(first), WalPos::new(last)).unwrap()
    }

    #[test]
    fn segment_batch_counts_and_orders_events_by_dataset() {
        let empty = SegmentBatch::new(
            range(1, 3),
            true,
            vec![
                DatasetEvents::new("a", Vec::new()),
                DatasetEvents::new("b", Vec::new()),
            ],
        );
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.events().count(), 0);
        assert!(empty.dataset("a").is_some());
        assert!(empty.dataset("c").is_none());

        let single = SegmentBatch::single("only", EventBatch::new(Vec::new(), range(4, 4), false));
        assert_eq!(single.origin, range(4, 4));
        assert!(!single.clock_synced);
        assert_eq!(&*single.datasets[0].dataset, "only");
    }

    #[test]
    fn raw_batch_accounts_payload_bytes() {
        let t = Timestamp::from_nanos(1);
        let batch = RawBatch::new(
            SourceId::new("s"),
            vec![
                RawMessage::new(t, Bytes::from_static(b"abc")),
                RawMessage::new(t, Bytes::from_static(b"de")),
            ],
            true,
        );
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.payload_bytes(), 5);
        assert!(!batch.is_empty());
        assert!(RawBatch::new(SourceId::new("s"), vec![], false).is_empty());
    }
}
