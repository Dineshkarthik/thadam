//! The bounded hand-off between the gRPC handlers and `next_batch`.
//!
//! A message moves from `queue` to `inflight` when a batch is drained and
//! leaves `inflight` only once its batch is released in receive order (see
//! [`Shared::ack`]). Capacity counts both, so a slow commit pushes back on
//! ingress. `next_batch` only touches the queue in its final synchronous step,
//! which keeps it cancel-safe.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use thadam_core::{
    AckHandle, AckOutcome, AckTarget, AckToken, Attributes, BatchLimits, ConnectionEpoch,
    EpochGate, RawMessage, Timestamp,
};
use tokio::sync::{Notify, oneshot};

/// One message waiting to be batched.
pub(crate) struct Message {
    pub(crate) payload: Bytes,
    pub(crate) attrs: Attributes,
    /// Answers the RPC that carried the message once its batch is released.
    pub(crate) reply: Option<oneshot::Sender<AckOutcome>>,
}

struct Batch {
    messages: Vec<Message>,
    /// `None` until the runtime completes the batch's handle.
    outcome: Option<AckOutcome>,
}

/// Why a message was not queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushError {
    /// Queued plus in-flight messages already reach capacity.
    Full,
    /// The source is shutting down.
    Closed,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Message>,
    queued_bytes: usize,
    /// Drained batches, keyed by batch number. Released from the lowest key
    /// upward, so acknowledgements leave in receive order.
    inflight: BTreeMap<u64, Batch>,
    /// Queued plus in-flight messages: the quantity capacity bounds.
    pending: usize,
    next_batch: u64,
    /// Set once a batch is abandoned: nothing after it may be acknowledged,
    /// since acknowledging past a gap would lose the abandoned messages.
    halted: bool,
    closed: bool,
    /// Set only by the conformance harness; a gRPC source's stream never ends.
    finished: bool,
    #[cfg(test)]
    acked: Vec<Bytes>,
}

pub(crate) struct Shared {
    capacity: usize,
    gate: EpochGate,
    arrived: Notify,
    released: Notify,
    state: Mutex<State>,
}

impl Shared {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Shared {
            capacity,
            gate: EpochGate::new(),
            arrived: Notify::new(),
            released: Notify::new(),
            state: Mutex::new(State::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(crate) fn gate(&self) -> &EpochGate {
        &self.gate
    }

    /// Queues `message` without waiting. Never blocks, so a request handler
    /// that finds the queue full can answer at once.
    pub(crate) fn try_push(&self, message: Message) -> Result<(), PushError> {
        let mut st = self.lock();
        if st.closed {
            return Err(PushError::Closed);
        }
        if st.pending >= self.capacity {
            return Err(PushError::Full);
        }
        st.pending += 1;
        st.queued_bytes += message.payload.len();
        st.queue.push_back(message);
        drop(st);
        self.arrived.notify_one();
        Ok(())
    }

    /// Queued message count, queued payload bytes, and whether the stream has
    /// ended.
    pub(crate) fn snapshot(&self) -> (usize, usize, bool) {
        let st = self.lock();
        (st.queue.len(), st.queued_bytes, st.finished)
    }

    #[cfg(test)]
    pub(crate) fn finish(&self) {
        self.lock().finished = true;
        self.arrived.notify_one();
    }

    /// Resolves once a message may have been queued.
    pub(crate) async fn arrived(&self) {
        self.arrived.notified().await;
    }

    /// Moves up to `limits` worth of queued messages into one in-flight batch.
    /// This is the only step of the drain that mutates the queue.
    pub(crate) fn take(
        self: &Arc<Self>,
        limits: BatchLimits,
        now: Timestamp,
    ) -> Option<(AckHandle, Vec<RawMessage>)> {
        let mut st = self.lock();
        let mut taken: Vec<Message> = Vec::new();
        let mut bytes = 0usize;
        while let Some(len) = st.queue.front().map(|m| m.payload.len()) {
            if taken.len() >= limits.max_messages
                || (!taken.is_empty() && bytes + len > limits.max_bytes)
            {
                break;
            }
            let Some(message) = st.queue.pop_front() else {
                break;
            };
            bytes += len;
            taken.push(message);
        }
        if taken.is_empty() {
            return None;
        }

        st.queued_bytes -= bytes;
        let batch_no = st.next_batch;
        st.next_batch += 1;
        let token = AckToken::new(batch_no, taken.len() as u64);
        let raw = taken
            .iter()
            .map(|m| RawMessage {
                ingest_time: now,
                attrs: m.attrs.clone(),
                payload: m.payload.clone(),
            })
            .collect();
        st.inflight.insert(
            batch_no,
            Batch {
                messages: taken,
                outcome: None,
            },
        );
        let target: Arc<dyn AckTarget> = self.clone();
        let handle = self.gate.issue(token, target);
        Some((handle, raw))
    }

    /// Stops accepting messages and drops every queued message's reply, which
    /// clients see as `UNAVAILABLE`.
    pub(crate) fn close(&self) {
        let abandoned = {
            let mut st = self.lock();
            st.closed = true;
            st.pending -= st.queue.len();
            st.queued_bytes = 0;
            std::mem::take(&mut st.queue)
        };
        drop(abandoned);
        self.released.notify_waiters();
    }

    /// Drops every in-flight message that the runtime did not release in time.
    pub(crate) fn abandon_inflight(&self) {
        let abandoned = {
            let mut st = self.lock();
            let n: usize = st.inflight.values().map(|b| b.messages.len()).sum();
            st.pending -= n;
            std::mem::take(&mut st.inflight)
        };
        drop(abandoned);
        self.released.notify_waiters();
    }

    /// Messages queued or drained but not yet released.
    pub(crate) fn pending(&self) -> usize {
        self.lock().pending
    }

    /// Resolves when the released count may have changed.
    pub(crate) fn released(&self) -> &Notify {
        &self.released
    }

    #[cfg(test)]
    pub(crate) fn lose_session(&self) {
        let mut st = self.lock();
        self.gate.advance();
        let mut restored: Vec<Message> = Vec::new();
        for (_, batch) in std::mem::take(&mut st.inflight) {
            restored.extend(batch.messages);
        }
        restored.extend(std::mem::take(&mut st.queue));
        st.queued_bytes = restored.iter().map(|m| m.payload.len()).sum();
        st.pending = restored.len();
        st.queue = restored.into();
        st.halted = false;
        drop(st);
        self.arrived.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn acked(&self) -> Vec<Bytes> {
        self.lock().acked.clone()
    }
}

#[cfg(test)]
fn record_acked(st: &mut State, message: &Message, outcome: AckOutcome) {
    if outcome == AckOutcome::Committed {
        st.acked.push(message.payload.clone());
    }
}

impl AckTarget for Shared {
    fn ack(&self, epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome) {
        let mut released: Vec<(oneshot::Sender<AckOutcome>, AckOutcome)> = Vec::new();
        {
            let mut st = self.lock();
            if epoch != self.gate.current() {
                return;
            }
            let Some(batch) = st.inflight.get_mut(&token.a()) else {
                return;
            };
            batch.outcome = Some(outcome);

            while let Some((_, batch)) = st.inflight.first_key_value() {
                let Some(batch_outcome) = batch.outcome else {
                    break;
                };
                let Some((_, batch)) = st.inflight.pop_first() else {
                    break;
                };
                st.pending -= batch.messages.len();
                let final_outcome = match (st.halted, batch_outcome) {
                    (false, AckOutcome::Committed) => AckOutcome::Committed,
                    _ => {
                        st.halted = true;
                        AckOutcome::Abandoned
                    }
                };
                for message in batch.messages {
                    #[cfg(test)]
                    record_acked(&mut st, &message, final_outcome);
                    if let Some(reply) = message.reply {
                        released.push((reply, final_outcome));
                    }
                }
            }
        }
        for (reply, outcome) in released {
            let _ = reply.send(outcome);
        }
        self.released.notify_waiters();
    }
}
