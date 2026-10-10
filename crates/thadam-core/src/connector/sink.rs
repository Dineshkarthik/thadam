//! The two sink shapes (ADR-0002, D6): batch-transactional and segment-
//! accumulating.

use async_trait::async_trait;

use super::context::SinkContext;
use crate::batch::{EventBatch, SegmentBatch};
use crate::error::{Reject, ThadamError};
use crate::event::Event;
use crate::position::{BatchId, WalPos};

/// A sink's effective delivery class, reported by `open` from probing the
/// target rather than assumed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum DeliveryClass {
    /// A retried batch may be applied twice.
    #[default]
    AtLeastOnce,
    /// A retried batch with the same [`BatchId`] is applied once.
    IdempotentBatch,
    /// A write that failed in flight may be lost; it is never applied twice.
    AtMostOnce,
}

impl DeliveryClass {
    /// The stable name used in metrics and `/status`.
    pub const fn as_str(self) -> &'static str {
        match self {
            DeliveryClass::AtLeastOnce => "at_least_once",
            DeliveryClass::IdempotentBatch => "idempotent_batch",
            DeliveryClass::AtMostOnce => "at_most_once",
        }
    }
}

/// What a [`BatchSink`] can do. The delivery class is the only capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct SinkCapabilities {
    /// The effective delivery class.
    pub delivery: DeliveryClass,
}

impl SinkCapabilities {
    /// Capabilities with the given delivery class.
    pub const fn new(delivery: DeliveryClass) -> Self {
        SinkCapabilities { delivery }
    }
}

/// A batch handed to a [`BatchSink`], with its identity.
///
/// The id is always derived from the batch's WAL origin, so the two cannot
/// disagree. The batch driver forms batches on frame boundaries, persists the
/// in-flight range in the cursor before sending, and after a crash rebuilds the
/// same `SinkBatch` from that range. It keeps the batch and passes
/// `&SinkBatch` on every retry, so retries never clone.
#[derive(Debug)]
pub struct SinkBatch {
    id: BatchId,
    batch: EventBatch,
}

impl SinkBatch {
    /// Wraps `batch`, deriving the id from its origin.
    pub fn new(batch: EventBatch) -> Self {
        SinkBatch {
            id: BatchId::from_range(batch.origin),
            batch,
        }
    }

    /// The batch identity, identical on every retry.
    pub fn id(&self) -> BatchId {
        self.id
    }

    /// The events and their origin.
    pub fn batch(&self) -> &EventBatch {
        &self.batch
    }

    /// The events.
    pub fn events(&self) -> &[Event] {
        &self.batch.events
    }

    /// Unwraps the batch.
    pub fn into_batch(self) -> EventBatch {
        self.batch
    }
}

/// The result of a successful [`BatchSink::write`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteOutcome {
    /// Events the sink refused individually; they go to quarantine and the
    /// rest of the batch counts as written.
    pub rejected: Vec<Reject>,
}

impl WriteOutcome {
    /// An outcome with the given rejects.
    pub fn with_rejects(rejected: Vec<Reject>) -> Self {
        WriteOutcome { rejected }
    }
}

/// A sink that applies whole batches (ClickHouse, a single object PUT).
///
/// The runtime retries the same [`SinkBatch`] after a transient error, so
/// `write` must tolerate seeing a batch id twice. An unknown outcome (for
/// example a dropped HTTP request) is reported as a transient error.
#[async_trait]
pub trait BatchSink: Send + 'static {
    /// Probes the target and reports the effective delivery class.
    async fn open(&mut self, ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError>;

    /// Applies one batch. The same [`BatchId`] is presented on every retry.
    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError>;

    /// Releases the target.
    async fn close(&mut self) -> Result<(), ThadamError>;
}

/// A sink that accumulates events into open state and reports a committed
/// watermark (Parquet, NDJSON).
///
/// The runtime advances its cursor only to a committed watermark, at frame
/// granularity, then calls [`SegmentSink::cursor_committed`]. After a restart
/// it resumes from the position [`SegmentSink::recover`] returns.
///
/// Events arrive grouped by dataset in a [`SegmentBatch`]: one `append` per
/// chain read, covering one WAL range for every dataset. The runtime never
/// calls `poll_commit`, `finalize_all`, or `recover` in the middle of an
/// `append`, and successive batches have strictly increasing, non-overlapping
/// origins, so a sink may derive its watermark from the origins it has
/// appended.
#[async_trait]
pub trait SegmentSink: Send + 'static {
    /// Recovers from a crash and returns the position the sink is durable
    /// through, or `None` if it holds nothing.
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError>;

    /// Takes ownership of a batch and may buffer it. Returns the events the
    /// sink refused, each indexed by its position in
    /// [`SegmentBatch::events`].
    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError>;

    /// Finalizes files that are due and returns the new committed watermark,
    /// if it advanced.
    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError>;

    /// Finalizes everything and returns the final watermark.
    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError>;

    /// Tells the sink the runtime's cursor passed `pos`, so finalized files up
    /// to it become eligible for shipping.
    fn cursor_committed(&mut self, pos: WalPos);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::{WalEpoch, WalRange};

    #[test]
    fn batch_id_is_derived_from_the_origin() {
        let origin = WalRange::new(WalEpoch::new(0xab), WalPos::new(3), WalPos::new(9)).unwrap();
        let batch = SinkBatch::new(EventBatch::new(Vec::new(), origin, true));
        assert_eq!(batch.id(), BatchId::from_range(origin));
        assert_eq!(batch.id().to_string(), "ab-3-9");
        assert_eq!(batch.batch().origin, origin);
        assert!(batch.events().is_empty());
        assert_eq!(batch.into_batch().origin, origin);
    }

    #[test]
    fn delivery_class_defaults_to_the_weaker_guarantee() {
        assert_eq!(
            SinkCapabilities::default().delivery,
            DeliveryClass::AtLeastOnce
        );
        assert_eq!(DeliveryClass::IdempotentBatch.as_str(), "idempotent_batch");
    }
}
