//! Batches of raw messages (source side) and events (sink side).

use bytes::Bytes;

use crate::event::Event;
use crate::identity::{Attributes, SourceId};
use crate::position::WalRange;
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

#[cfg(test)]
mod tests {
    use super::*;

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
