//! The row-encoder abstraction the sink drives through `spawn_blocking`
//! ownership ping-pong: every dataset encoder owns mutable Arrow builders,
//! is single-threaded behind the sink's `&mut self`, and yields
//! [`RecordBatch`] chunks to be written to the open segment file.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use thadam_arrow::{BatchContext, BatchEncoder};
use thadam_core::{Event, Reject};

use crate::error::SegmentError;

/// One dataset's encoder.
///
/// `finish_batch` is cheap enough per batch and infallible in practice: both
/// encoders build arrays that match their schema by construction, so a
/// mismatch is a bug and reported as a permanent encode error rather than a
/// panic.
pub(crate) trait RowsEncoder: Send {
    /// WAL epoch and clock-sync flag to stamp onto subsequently pushed rows
    /// (quarantine rows carry their own; it is a no-op there).
    fn set_context(&mut self, ctx: BatchContext);
    /// Appends one event as a row, atomically: on failure nothing is written.
    fn push(&mut self, event: &Event) -> Result<(), Reject>;
    /// The schema of finished batches.
    fn arrow_schema(&self) -> SchemaRef;
    /// Takes the pending rows as a batch, leaving the encoder empty.
    fn finish_batch(&mut self) -> Result<RecordBatch, SegmentError>;
}

/// Wraps [`BatchEncoder`] (normal datasets).
pub(crate) struct ArrowRowsEncoder {
    encoder: BatchEncoder,
}

impl ArrowRowsEncoder {
    /// An encoder for `schema`, reserving room for `capacity_hint` rows.
    pub(crate) fn new(
        schema: std::sync::Arc<thadam_arrow::DatasetSchema>,
        capacity_hint: usize,
    ) -> Self {
        ArrowRowsEncoder {
            encoder: BatchEncoder::new(schema, capacity_hint),
        }
    }
}

impl RowsEncoder for ArrowRowsEncoder {
    fn set_context(&mut self, ctx: BatchContext) {
        self.encoder.set_context(ctx);
    }

    fn push(&mut self, event: &Event) -> Result<(), Reject> {
        self.encoder.push(event)
    }

    fn arrow_schema(&self) -> SchemaRef {
        self.encoder.schema().arrow_schema()
    }

    fn finish_batch(&mut self) -> Result<RecordBatch, SegmentError> {
        Ok(self.encoder.finish())
    }
}
