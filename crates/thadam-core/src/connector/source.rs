//! The pull-based source contract (ADR-0002, D5).

use std::time::Duration;

use async_trait::async_trait;

use super::ack::AckHandle;
use super::context::SourceContext;
use crate::batch::RawBatch;
use crate::error::ThadamError;

/// Bounds for one [`Source::next_batch`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchLimits {
    /// Most messages in the batch.
    pub max_messages: usize,
    /// Most payload bytes in the batch. A single larger message is still
    /// returned alone.
    pub max_bytes: usize,
    /// The longest to wait for the batch to fill once it has a message.
    pub linger: Duration,
}

impl Default for BatchLimits {
    fn default() -> Self {
        BatchLimits {
            max_messages: 1024,
            max_bytes: 1 << 20,
            linger: Duration::from_millis(5),
        }
    }
}

/// A batch and the permission to acknowledge it.
#[derive(Debug)]
#[must_use = "dropping the ack handle loses the token and stalls the source"]
pub struct SourceBatch {
    /// The messages in receive order.
    pub batch: RawBatch,
    /// Completed by the runtime, in receive order, after the WAL commit.
    pub ack: AckHandle,
}

/// When a source's messages are acknowledged to the protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum AckMode {
    /// The source has no acknowledgements (best-effort transports, QoS 0).
    None,
    /// Acknowledge after the WAL commit. At-least-once.
    #[default]
    OnCommit,
    /// Acknowledge on receipt. At-most-once across a crash; opt-in.
    OnReceive,
}

impl AckMode {
    /// The configuration spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            AckMode::None => "none",
            AckMode::OnCommit => "on_commit",
            AckMode::OnReceive => "on_receive",
        }
    }
}

/// What a source can do. Ack mode is the only capability in the MVP.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct SourceCapabilities {
    /// How the source acknowledges.
    pub ack_mode: AckMode,
}

impl SourceCapabilities {
    /// Capabilities with the given ack mode.
    pub const fn new(ack_mode: AckMode) -> Self {
        SourceCapabilities { ack_mode }
    }
}

/// A pull-based message source.
///
/// # Contract
///
/// Every implementation must satisfy these clauses; `thadam-testkit` has a
/// conformance suite for each.
///
/// - **Cancel-safe `next_batch`.** Dropping the future never loses a message:
///   messages stay in the connector's queue until the future returns. All
///   other cancellation is cooperative through [`SourceContext::shutdown`]. The
///   runtime never drops any other connector future mid-operation.
/// - **Liveness.** Protocol liveness (keepalive pings, heartbeats, session
///   maintenance) continues however slowly the runtime pulls. Backpressure
///   works by withholding acknowledgements, not by stopping the protocol task.
/// - **The protocol task never awaits the runtime.** It waits only on the
///   protocol library, never on a queue the runtime drains, and is never raced
///   against a timer or token inside a library call.
/// - **Ordered acknowledgement.** The runtime completes [`AckHandle`]s in
///   receive order after the corresponding WAL commit. An empty batch is still
///   acknowledged.
/// - **Epoch-scoped handles.** A handle is valid only for the connection it was
///   issued in; issue handles through an [`EpochGate`](super::EpochGate) and
///   advance it on every session loss.
/// - **Payload-free handles.** A handle carries an [`AckToken`](super::AckToken),
///   not message bytes.
/// - **End of stream.** `next_batch` returns `Ok(None)` when the source is
///   exhausted or shutdown was triggered and nothing is queued.
#[async_trait]
pub trait Source: Send + 'static {
    /// Connects and subscribes. Observes [`SourceContext::shutdown`].
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError>;

    /// Returns the next batch, waiting at most `limits.linger` for it to fill
    /// once it holds a message, or `Ok(None)` at end of stream.
    ///
    /// **Must be cancel-safe.**
    async fn next_batch(&mut self, limits: BatchLimits)
    -> Result<Option<SourceBatch>, ThadamError>;

    /// Releases the connection. Messages not yet acknowledged are redelivered
    /// by the peer.
    async fn close(&mut self) -> Result<(), ThadamError>;

    /// The source's capabilities.
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_contract() {
        let limits = BatchLimits::default();
        assert_eq!(limits.linger, Duration::from_millis(5));
        assert_eq!(SourceCapabilities::default().ack_mode, AckMode::OnCommit);
        assert_eq!(AckMode::OnReceive.as_str(), "on_receive");
    }
}
