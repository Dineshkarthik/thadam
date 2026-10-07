//! The borrowed input a chain reads, and the reject it hands back.

use bytes::Bytes;
use thadam_core::{
    Attributes, Reject, ThadamError, SourceId, Sym, Timestamp, WalEpoch, WalPos, WalRange,
};

/// One raw WAL record, borrowed.
///
/// The runtime adapts a `thadam-wal` frame to a slice of these without copying:
/// every field is a reference or a small `Copy` value. The chain keeps no
/// reference past the call.
#[derive(Clone, Copy, Debug)]
pub struct RawRecordView<'a> {
    /// The source that produced the message.
    pub source: &'a SourceId,
    /// When the edge received it.
    pub ingest_time: Timestamp,
    /// Protocol metadata such as the MQTT `topic`.
    pub attrs: &'a Attributes,
    /// The payload bytes, exactly as received.
    pub payload: &'a Bytes,
    /// The WAL epoch of the frame.
    pub epoch: WalEpoch,
    /// The WAL frame the record was read from.
    pub pos: WalPos,
    /// The record's index within its frame. Part of the default event id.
    pub record_index: u32,
}

impl<'a> RawRecordView<'a> {
    /// Views one message of a frame.
    pub fn new(
        source: &'a SourceId,
        message: &'a thadam_core::RawMessage,
        epoch: WalEpoch,
        pos: WalPos,
        record_index: u32,
    ) -> Self {
        RawRecordView {
            source,
            ingest_time: message.ingest_time,
            attrs: &message.attrs,
            payload: &message.payload,
            epoch,
            pos,
            record_index,
        }
    }

    /// The value of the `topic` attribute, if the source set one.
    pub fn topic(&self) -> Option<&'a str> {
        self.attrs.get("topic")
    }
}

/// The records of one or more consecutive WAL frames, in WAL order.
#[derive(Clone, Copy, Debug)]
pub struct FrameInput<'a> {
    /// The records, ordered by position then index.
    pub records: &'a [RawRecordView<'a>],
    /// Whether the kernel reported the clock as synchronized for these frames.
    pub clock_synced: bool,
}

impl<'a> FrameInput<'a> {
    /// Input over `records`.
    pub fn new(records: &'a [RawRecordView<'a>], clock_synced: bool) -> Self {
        FrameInput {
            records,
            clock_synced,
        }
    }

    /// The WAL range the records span, or `None` if there are none or they
    /// span more than one epoch.
    pub fn origin(&self) -> Option<WalRange> {
        let first = self.records.first()?;
        let last = self.records.last()?;
        if first.epoch != last.epoch {
            return None;
        }
        WalRange::new(first.epoch, first.pos, last.pos).ok()
    }
}

/// A record the chain could not turn into an event, with the raw bytes kept
/// so the runtime can write it to quarantine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectedRaw {
    /// Why it was rejected. Never contains payload text.
    pub error: ThadamError,
    /// The dataset that rejected it; `None` for a failure before routing.
    pub dataset: Option<Sym>,
    /// The source of the message.
    pub source: SourceId,
    /// The WAL epoch of the frame.
    pub epoch: WalEpoch,
    /// The WAL frame.
    pub pos: WalPos,
    /// The record's index within its frame.
    pub record_index: u32,
    /// The sample index when only one exploded sample failed.
    pub sample_idx: Option<u32>,
    /// When the edge received the message.
    pub ingest_time: Timestamp,
    /// The message attributes.
    pub attrs: Attributes,
    /// The raw payload, shared with the WAL read buffer.
    pub payload: Bytes,
}

impl RejectedRaw {
    pub(crate) fn new(
        view: &RawRecordView<'_>,
        dataset: Option<&Sym>,
        sample_idx: Option<u32>,
        error: ThadamError,
    ) -> Self {
        RejectedRaw {
            error,
            dataset: dataset.cloned(),
            source: view.source.clone(),
            epoch: view.epoch,
            pos: view.pos,
            record_index: view.record_index,
            sample_idx,
            ingest_time: view.ingest_time,
            attrs: view.attrs.clone(),
            payload: view.payload.clone(),
        }
    }

    /// The core [`Reject`] for this record, as it sits at `index` in a batch.
    pub fn to_reject(&self, index: u32) -> Reject {
        Reject::new(index, self.error.clone())
    }
}
