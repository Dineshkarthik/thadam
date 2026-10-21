//! State shared between the poll task, the ack task, and the source.
//!
//! One mutex guards the incoming queue and the ack ledger. Every critical
//! section is short and synchronous: the poll task takes it only to push, and
//! never waits for the runtime to release it for longer than a queue
//! operation. Acks are sent to the ack task while the lock is held so the
//! order in which acknowledgements are requested is the order of the ledger.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use thadam_core::{
    AckOutcome, AckTarget, AckToken, Attributes, BatchLimits, ConnectionEpoch, ConnectorMetric,
    EpochGate, MetricsRecorder, NoopMetricsRecorder, RawMessage, Sym, ThadamError, Utf8Bytes,
};
use tokio::sync::{Notify, mpsc, watch};

use crate::stats::MqttStats;

const WARN_EVERY: u64 = 1_000;

/// Commands for the ack task, executed strictly in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckCmd {
    /// Acknowledge one message. Dropped if `epoch` is no longer current.
    Ack {
        epoch: ConnectionEpoch,
        pkid: u16,
        qos: u8,
    },
    /// (Re-)send the SUBSCRIBE for every configured topic.
    Subscribe,
}

/// Where the connection is, for `open` and for fatal errors.
#[derive(Debug, Clone)]
pub(crate) enum Status {
    Connecting,
    Ready,
    Failed(ThadamError),
}

/// What the poll task knows about the loss of messages, carried to the runtime
/// as attributes on the first message after the gap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GapInfo {
    pub session_lost: bool,
    pub dropped_queue_full: u64,
    pub dropped_oversize: u64,
}

impl GapInfo {
    fn is_empty(&self) -> bool {
        *self == GapInfo::default()
    }

    fn write_attributes(&self, attrs: &mut Attributes) {
        let mut reasons = Vec::with_capacity(3);
        if self.session_lost {
            reasons.push("session_lost");
        }
        if self.dropped_queue_full > 0 {
            reasons.push("queue_full");
        }
        if self.dropped_oversize > 0 {
            reasons.push("oversize");
        }
        attrs.insert(Sym::from("mqtt.gap"), Utf8Bytes::from(reasons.join(",")));
        let dropped = self.dropped_queue_full + self.dropped_oversize;
        if dropped > 0 {
            attrs.insert(
                Sym::from("mqtt.gap_dropped"),
                Utf8Bytes::from(dropped.to_string()),
            );
        }
    }
}

struct Queued {
    seq: u64,
    epoch: ConnectionEpoch,
    qos: u8,
    gap: Option<GapInfo>,
    msg: RawMessage,
}

#[derive(Clone, Copy, Debug)]
struct LedgerEntry {
    seq: u64,
    pkid: u16,
    qos: u8,
    epoch: ConnectionEpoch,
    /// Received but never queued (oversized): acknowledged in order with its
    /// neighbours so the PUBACK sequence stays in receive order.
    dropped: bool,
}

#[derive(Default)]
struct Inner {
    queue: VecDeque<Queued>,
    queued_bytes: usize,
    qos0_bytes: usize,
    ledger: VecDeque<LedgerEntry>,
    next_seq: u64,
    pending_gap: GapInfo,
    fatal: Option<ThadamError>,
}

/// Limits fixed at build time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub qos0_queue_bytes: usize,
    pub guard_limit: usize,
    pub on_receive: bool,
}

/// What `push` did with a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PushOutcome {
    Queued { guard_tripped: bool },
    DroppedQueueFull,
}

/// How full the queue is relative to a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Readiness {
    Empty,
    Partial,
    Full,
}

/// A drained batch of messages with its acknowledgement range.
pub(crate) struct Drained {
    pub messages: Vec<RawMessage>,
    pub first_seq: u64,
    pub last_seq: u64,
}

pub(crate) struct Shared {
    pub gate: EpochGate,
    pub stats: MqttStats,
    limits: Limits,
    inner: Mutex<Inner>,
    wake: Notify,
    status: watch::Sender<Status>,
    ack_tx: mpsc::UnboundedSender<AckCmd>,
    guard_streak: AtomicU64,
    metrics: Mutex<(Sym, Arc<dyn MetricsRecorder>)>,
}

impl Shared {
    pub fn new(
        limits: Limits,
        ack_tx: mpsc::UnboundedSender<AckCmd>,
    ) -> (Self, watch::Receiver<Status>) {
        let (status, rx) = watch::channel(Status::Connecting);
        (
            Shared {
                gate: EpochGate::new(),
                stats: MqttStats::default(),
                limits,
                inner: Mutex::new(Inner::default()),
                wake: Notify::new(),
                status,
                ack_tx,
                guard_streak: AtomicU64::new(0),
                metrics: Mutex::new((Sym::from(""), Arc::new(NoopMetricsRecorder))),
            },
            rx,
        )
    }

    pub fn set_metrics(&self, source_id: Sym, metrics: Arc<dyn MetricsRecorder>) {
        *self.metrics.lock().unwrap_or_else(PoisonError::into_inner) = (source_id, metrics);
    }

    pub fn record_stale_ack(&self) {
        let (ref id, ref rec) = *self.metrics.lock().unwrap_or_else(PoisonError::into_inner);
        if !id.is_empty() {
            rec.record(ConnectorMetric::SourceStaleAcks {
                source: id,
                count: 1,
            });
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.wake.notified()
    }

    pub fn wake(&self) {
        self.wake.notify_one();
    }

    pub fn subscribe_status(&self) -> watch::Receiver<Status> {
        self.status.subscribe()
    }

    pub fn set_ready(&self) {
        self.status.send_if_modified(|s| {
            if matches!(s, Status::Connecting) {
                *s = Status::Ready;
                true
            } else {
                false
            }
        });
    }

    pub fn fatal(&self) -> Option<ThadamError> {
        self.lock().fatal.clone()
    }

    /// Records a fatal error: `open` and `next_batch` surface it.
    pub fn fail(&self, error: ThadamError) {
        {
            let mut inner = self.lock();
            if inner.fatal.is_some() {
                return;
            }
            inner.fatal = Some(error.clone());
        }
        self.status.send_replace(Status::Failed(error));
        self.wake();
    }

    fn send_ack(&self, epoch: ConnectionEpoch, pkid: u16, qos: u8) {
        if qos > 0 {
            self.stats.acks_requested.fetch_add(1, Ordering::Relaxed);
            if self.ack_tx.send(AckCmd::Ack { epoch, pkid, qos }).is_err() {
                self.note_ack_send_drop("ack");
            }
        }
    }

    pub fn request_subscribe(&self) {
        if self.ack_tx.send(AckCmd::Subscribe).is_err() {
            self.note_ack_send_drop("subscribe");
        }
    }

    fn note_ack_send_drop(&self, command: &'static str) {
        let dropped = self.stats.acks_dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if dropped == 1 || dropped.is_multiple_of(WARN_EVERY) {
            tracing::warn!(
                command,
                dropped,
                "MQTT ack channel closed; dropping command"
            );
        }
    }

    /// Queues one received message. Never blocks.
    pub fn push(&self, msg: RawMessage, qos: u8, pkid: u16) -> PushOutcome {
        let len = msg.payload.len();
        let mut guard_tripped = false;
        {
            let mut inner = self.lock();
            let epoch = self.gate.current();
            if qos == 0 && inner.qos0_bytes + len > self.limits.qos0_queue_bytes {
                inner.pending_gap.dropped_queue_full += 1;
                self.stats
                    .dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                return PushOutcome::DroppedQueueFull;
            }
            let seq = inner.next_seq;
            inner.next_seq += 1;
            if qos == 0 {
                inner.qos0_bytes += len;
            } else {
                inner.ledger.push_back(LedgerEntry {
                    seq,
                    pkid,
                    qos,
                    epoch,
                    dropped: false,
                });
                let unacked = inner.ledger.len();
                self.stats
                    .unacked_max
                    .fetch_max(unacked as u64, Ordering::Relaxed);
                if unacked > self.limits.guard_limit {
                    guard_tripped = true;
                    self.stats.guard_trips.fetch_add(1, Ordering::Relaxed);
                }
            }
            let gap = std::mem::take(&mut inner.pending_gap);
            inner.queued_bytes += len;
            inner.queue.push_back(Queued {
                seq,
                epoch,
                qos,
                gap: (!gap.is_empty()).then_some(gap),
                msg,
            });
        }
        self.wake();
        PushOutcome::Queued { guard_tripped }
    }

    /// Records a message that was received but is not delivered because it
    /// exceeds `max_message_bytes`. It is acknowledged in receive order so the
    /// broker does not redeliver it forever.
    pub fn push_dropped_oversize(&self, qos: u8, pkid: u16) {
        let mut inner = self.lock();
        inner.pending_gap.dropped_oversize += 1;
        self.stats.dropped_oversize.fetch_add(1, Ordering::Relaxed);
        let seq = inner.next_seq;
        inner.next_seq += 1;
        if qos == 0 {
            return;
        }
        let epoch = self.gate.current();
        if inner.ledger.is_empty() {
            self.send_ack(epoch, pkid, qos);
        } else {
            inner.ledger.push_back(LedgerEntry {
                seq,
                pkid,
                qos,
                epoch,
                dropped: true,
            });
        }
    }

    /// The connection dropped: handles issued so far must not acknowledge
    /// anything on a later connection.
    pub fn on_disconnect(&self) {
        let _guard = self.lock();
        self.gate.advance();
    }

    /// A `ConnAck` arrived: start a new epoch and reconcile with the session.
    ///
    /// With a resumed session the broker redelivers every unacknowledged
    /// message with DUP, so queued QoS 1 and 2 messages from the old epoch are
    /// discarded rather than delivered twice. After a lost session nothing
    /// will be redelivered, so the messages we already hold are kept; their
    /// packet identifiers belong to the dead session and are never
    /// acknowledged.
    pub fn on_connack(&self, session_present: bool, session_expected: bool) -> ConnAckEffect {
        let mut inner = self.lock();
        let epoch = self.gate.advance();
        let purged_ledger = inner.ledger.len();
        inner.ledger.clear();
        let mut purged_messages = 0usize;
        if session_present {
            let mut kept = VecDeque::with_capacity(inner.queue.len());
            let mut bytes = 0usize;
            let mut qos0_bytes = 0usize;
            for item in inner.queue.drain(..) {
                if item.qos == 0 {
                    bytes += item.msg.payload.len();
                    qos0_bytes += item.msg.payload.len();
                    kept.push_back(item);
                } else if item.epoch >= epoch {
                    bytes += item.msg.payload.len();
                    kept.push_back(item);
                } else {
                    purged_messages += 1;
                }
            }
            inner.queue = kept;
            inner.queued_bytes = bytes;
            inner.qos0_bytes = qos0_bytes;
        }
        let session_lost = !session_present && session_expected;
        if session_lost {
            inner.pending_gap.session_lost = true;
        }
        self.stats
            .purged_on_resume
            .fetch_add(purged_messages as u64, Ordering::Relaxed);
        self.stats
            .stale_ledger_entries
            .fetch_add(purged_ledger as u64, Ordering::Relaxed);
        ConnAckEffect {
            epoch,
            session_lost,
            purged_messages,
        }
    }

    pub fn readiness(&self, limits: &BatchLimits) -> Readiness {
        let inner = self.lock();
        if inner.queue.is_empty() {
            Readiness::Empty
        } else if inner.queue.len() >= limits.max_messages || inner.queued_bytes >= limits.max_bytes
        {
            Readiness::Full
        } else {
            Readiness::Partial
        }
    }

    /// Removes up to the batch limits from the queue in one step, so a dropped
    /// `next_batch` future can never hold messages.
    pub fn drain(&self, limits: &BatchLimits) -> Option<Drained> {
        let mut inner = self.lock();
        let mut messages = Vec::new();
        let mut bytes = 0usize;
        let (mut first_seq, mut last_seq) = (0, 0);
        while let Some(front) = inner.queue.front() {
            let len = front.msg.payload.len();
            if !messages.is_empty()
                && (messages.len() >= limits.max_messages || bytes + len > limits.max_bytes)
            {
                break;
            }
            let Some(item) = inner.queue.pop_front() else {
                break;
            };
            inner.queued_bytes -= len;
            if item.qos == 0 {
                inner.qos0_bytes -= len;
            }
            if messages.is_empty() {
                first_seq = item.seq;
            }
            last_seq = item.seq;
            bytes += len;
            let mut msg = item.msg;
            if let Some(gap) = item.gap {
                gap.write_attributes(&mut msg.attrs);
            }
            messages.push(msg);
        }
        if messages.is_empty() {
            return None;
        }
        self.stats
            .messages_delivered
            .fetch_add(messages.len() as u64, Ordering::Relaxed);
        if self.limits.on_receive {
            self.complete_range(&mut inner, first_seq, last_seq, true);
        }
        Some(Drained {
            messages,
            first_seq,
            last_seq,
        })
    }

    /// Acknowledges (or abandons) the ledger entries of one batch.
    fn complete_range(&self, inner: &mut Inner, first: u64, last: u64, committed: bool) {
        let lo = inner.ledger.partition_point(|e| e.seq < first);
        let mut hi = inner.ledger.partition_point(|e| e.seq <= last);
        while inner.ledger.get(hi).is_some_and(|e| e.dropped) {
            hi += 1;
        }
        let current = self.gate.current();
        let entries: Vec<LedgerEntry> = inner.ledger.drain(lo..hi).collect();
        for entry in entries {
            if !committed {
                self.stats.abandoned.fetch_add(1, Ordering::Relaxed);
            } else if entry.epoch != current {
                self.stats
                    .stale_ledger_entries
                    .fetch_add(1, Ordering::Relaxed);
                self.record_stale_ack();
            } else {
                self.send_ack(entry.epoch, entry.pkid, entry.qos);
            }
        }
        if committed {
            self.guard_streak.store(0, Ordering::Relaxed);
        }
    }

    /// The number of consecutive guard trips without an acknowledgement
    /// completing in between.
    pub fn note_guard_trip(&self) -> u64 {
        self.guard_streak.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn snapshot_sizes(&self) -> QueueSizes {
        let inner = self.lock();
        QueueSizes {
            queued_messages: inner.queue.len(),
            queued_bytes: inner.queued_bytes,
            unacked: inner.ledger.len(),
        }
    }
}

/// What a `ConnAck` changed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnAckEffect {
    pub epoch: ConnectionEpoch,
    pub session_lost: bool,
    pub purged_messages: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueueSizes {
    pub queued_messages: usize,
    pub queued_bytes: usize,
    pub unacked: usize,
}

impl AckTarget for Shared {
    fn ack(&self, _epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome) {
        let committed = matches!(outcome, AckOutcome::Committed);
        let mut inner = self.lock();
        self.complete_range(&mut inner, token.a(), token.b(), committed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use thadam_core::{AckStatus, Timestamp};

    use super::*;

    fn limits() -> Limits {
        Limits {
            qos0_queue_bytes: 100,
            guard_limit: 1000,
            on_receive: false,
        }
    }

    fn shared_with(limits: Limits) -> (Arc<Shared>, mpsc::UnboundedReceiver<AckCmd>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (shared, _status) = Shared::new(limits, tx);
        (Arc::new(shared), rx)
    }

    fn msg(len: usize) -> RawMessage {
        RawMessage::new(Timestamp::from_nanos(1), Bytes::from(vec![7u8; len]))
    }

    fn batch_limits(max_messages: usize) -> BatchLimits {
        BatchLimits {
            max_messages,
            ..BatchLimits::default()
        }
    }

    fn acks(rx: &mut mpsc::UnboundedReceiver<AckCmd>) -> Vec<u16> {
        let mut out = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            if let AckCmd::Ack { pkid, .. } = cmd {
                out.push(pkid);
            }
        }
        out
    }

    fn complete(shared: &Arc<Shared>, drained: &Drained) -> AckStatus {
        shared
            .gate
            .issue(
                AckToken::new(drained.first_seq, drained.last_seq),
                shared.clone(),
            )
            .complete(AckOutcome::Committed)
    }

    #[test]
    fn acks_follow_receive_order_after_commit() {
        let (shared, mut rx) = shared_with(limits());
        for pkid in 1..=5 {
            assert_eq!(
                shared.push(msg(4), 1, pkid),
                PushOutcome::Queued {
                    guard_tripped: false
                }
            );
        }
        let first = shared.drain(&batch_limits(2)).unwrap();
        let second = shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(first.messages.len(), 2);
        assert_eq!(second.messages.len(), 3);
        assert!(
            acks(&mut rx).is_empty(),
            "nothing is acknowledged before commit"
        );

        assert_eq!(complete(&shared, &first), AckStatus::Delivered);
        assert_eq!(acks(&mut rx), [1, 2]);
        assert_eq!(complete(&shared, &second), AckStatus::Delivered);
        assert_eq!(acks(&mut rx), [3, 4, 5]);
        assert_eq!(shared.snapshot_sizes().unacked, 0);
    }

    #[test]
    fn abandoned_batches_are_never_acknowledged() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(1), 1, 9);
        let batch = shared.drain(&batch_limits(10)).unwrap();
        let status = shared
            .gate
            .issue(
                AckToken::new(batch.first_seq, batch.last_seq),
                shared.clone(),
            )
            .complete(AckOutcome::Abandoned);
        assert_eq!(status, AckStatus::Delivered);
        assert!(acks(&mut rx).is_empty());
        assert_eq!(shared.stats.abandoned.load(Ordering::Relaxed), 1);
        assert_eq!(shared.snapshot_sizes().unacked, 0);
    }

    #[test]
    fn qos0_messages_have_no_acknowledgement() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(4), 0, 0);
        shared.push(msg(4), 1, 3);
        let batch = shared.drain(&batch_limits(10)).unwrap();
        complete(&shared, &batch);
        assert_eq!(acks(&mut rx), [3]);
    }

    #[test]
    fn qos0_overflow_drops_the_newest_and_marks_a_gap() {
        let (shared, _rx) = shared_with(limits());
        let mut dropped = 0;
        for _ in 0..30 {
            if shared.push(msg(10), 0, 0) == PushOutcome::DroppedQueueFull {
                dropped += 1;
            }
        }
        assert_eq!(dropped, 20, "100 bytes hold ten 10-byte messages");
        assert_eq!(shared.stats.dropped_queue_full.load(Ordering::Relaxed), 20);

        let drained = shared.drain(&batch_limits(100)).unwrap();
        assert_eq!(drained.messages.len(), 10);
        assert_eq!(shared.stats.messages_delivered.load(Ordering::Relaxed), 10);
        assert_eq!(
            drained.messages[0].attrs.get("mqtt.gap"),
            None,
            "drops come after the queued messages"
        );

        assert!(matches!(
            shared.push(msg(10), 0, 0),
            PushOutcome::Queued { .. }
        ));
        let next = shared.drain(&batch_limits(100)).unwrap();
        assert_eq!(next.messages[0].attrs.get("mqtt.gap"), Some("queue_full"));
        assert_eq!(next.messages[0].attrs.get("mqtt.gap_dropped"), Some("20"));
        assert!(next.messages.len() == 1);
    }

    #[test]
    fn qos1_is_never_dropped_by_the_qos0_budget() {
        let (shared, _rx) = shared_with(limits());
        for pkid in 1..=50 {
            assert!(matches!(
                shared.push(msg(50), 1, pkid),
                PushOutcome::Queued { .. }
            ));
        }
        assert_eq!(shared.snapshot_sizes().queued_bytes, 2500);
        assert_eq!(shared.stats.unacked_max.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn the_guard_trips_above_its_limit_without_blocking() {
        let (shared, _rx) = shared_with(Limits {
            guard_limit: 3,
            ..limits()
        });
        let outcomes: Vec<_> = (1..=5).map(|p| shared.push(msg(1), 1, p)).collect();
        assert_eq!(
            outcomes,
            vec![
                PushOutcome::Queued {
                    guard_tripped: false
                },
                PushOutcome::Queued {
                    guard_tripped: false
                },
                PushOutcome::Queued {
                    guard_tripped: false
                },
                PushOutcome::Queued {
                    guard_tripped: true
                },
                PushOutcome::Queued {
                    guard_tripped: true
                },
            ]
        );
        assert_eq!(shared.stats.guard_trips.load(Ordering::Relaxed), 2);
        assert_eq!(shared.note_guard_trip(), 1);
        assert_eq!(shared.note_guard_trip(), 2);
    }

    #[test]
    fn resumed_session_discards_queued_qos1_and_keeps_qos0() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(8), 1, 1);
        shared.push(msg(8), 0, 0);
        shared.push(msg(8), 1, 2);
        let effect = shared.on_connack(true, true);
        assert_eq!(effect.purged_messages, 2);
        assert!(!effect.session_lost);
        let sizes = shared.snapshot_sizes();
        assert_eq!(
            (sizes.queued_messages, sizes.queued_bytes, sizes.unacked),
            (1, 8, 0)
        );
        assert!(acks(&mut rx).is_empty());

        shared.push(msg(8), 1, 1);
        let batch = shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(batch.messages[0].attrs.get("mqtt.gap"), None);
    }

    #[test]
    fn lost_session_keeps_queued_messages_but_never_acknowledges_them() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(8), 1, 1);
        shared.push(msg(8), 1, 2);
        let effect = shared.on_connack(false, true);
        assert!(effect.session_lost);
        assert_eq!(effect.purged_messages, 0);

        shared.push(msg(8), 1, 1);
        let batch = shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(batch.messages.len(), 3);
        assert_eq!(
            batch.messages[0].attrs.get("mqtt.gap"),
            None,
            "held before the loss"
        );
        assert_eq!(batch.messages[1].attrs.get("mqtt.gap"), None);
        assert_eq!(
            batch.messages[2].attrs.get("mqtt.gap"),
            Some("session_lost"),
            "the first message of the new session carries the marker"
        );

        complete(&shared, &batch);
        assert_eq!(
            acks(&mut rx),
            [1],
            "only the new session's message is acknowledged"
        );
    }

    #[test]
    fn session_loss_is_only_flagged_when_a_session_was_expected() {
        let (shared, _rx) = shared_with(limits());
        assert!(!shared.on_connack(false, false).session_lost);
        assert!(!shared.on_connack(true, true).session_lost);
        assert!(shared.on_connack(false, true).session_lost);
    }

    #[test]
    fn stale_handles_are_dropped_by_the_gate_and_never_reach_the_ledger() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(8), 1, 1);
        let old = shared.drain(&batch_limits(10)).unwrap();
        let old_handle = shared
            .gate
            .issue(AckToken::new(old.first_seq, old.last_seq), shared.clone());

        shared.on_disconnect();
        shared.on_connack(false, true);
        shared.push(msg(8), 1, 1);
        assert_eq!(old_handle.complete(AckOutcome::Committed), AckStatus::Stale);
        assert!(
            acks(&mut rx).is_empty(),
            "the stale ack must not acknowledge the new message"
        );
        assert_eq!(shared.gate.stale_acks(), 1);
        assert_eq!(shared.snapshot_sizes().unacked, 1);

        let new = shared.drain(&batch_limits(10)).unwrap();
        complete(&shared, &new);
        assert_eq!(acks(&mut rx), [1]);
    }

    #[test]
    fn entries_of_an_old_epoch_are_not_acknowledged_after_a_reconnect_race() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(8), 1, 4);
        let batch = shared.drain(&batch_limits(10)).unwrap();
        let handle = shared.gate.issue(
            AckToken::new(batch.first_seq, batch.last_seq),
            shared.clone(),
        );
        // The completion reaches the target after the epoch moved on but the
        // gate screened it first: simulate the target being called directly.
        shared.on_disconnect();
        shared.ack(
            handle.epoch(),
            AckToken::new(batch.first_seq, batch.last_seq),
            AckOutcome::Committed,
        );
        assert!(acks(&mut rx).is_empty());
        assert_eq!(shared.stats.stale_ledger_entries.load(Ordering::Relaxed), 1);
        let _ = handle.complete(AckOutcome::Abandoned);
    }

    #[test]
    fn oversized_messages_are_acknowledged_in_receive_order() {
        let (shared, mut rx) = shared_with(limits());
        shared.push(msg(4), 1, 1);
        shared.push_dropped_oversize(1, 2);
        shared.push(msg(4), 1, 3);
        shared.push_dropped_oversize(1, 4);

        let first = shared.drain(&batch_limits(1)).unwrap();
        complete(&shared, &first);
        assert_eq!(
            acks(&mut rx),
            [1, 2],
            "the dropped message follows its predecessor"
        );
        let second = shared.drain(&batch_limits(1)).unwrap();
        assert_eq!(second.messages[0].attrs.get("mqtt.gap"), Some("oversize"));
        assert_eq!(second.messages[0].attrs.get("mqtt.gap_dropped"), Some("1"));
        complete(&shared, &second);
        assert_eq!(acks(&mut rx), [3, 4]);
        assert_eq!(shared.stats.dropped_oversize.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn an_oversized_message_alone_is_acknowledged_immediately() {
        let (shared, mut rx) = shared_with(limits());
        shared.push_dropped_oversize(1, 7);
        assert_eq!(acks(&mut rx), [7]);
        shared.push_dropped_oversize(0, 0);
        assert!(acks(&mut rx).is_empty());
    }

    #[test]
    fn drain_respects_message_and_byte_limits_but_returns_one_oversize_batch() {
        let (shared, _rx) = shared_with(limits());
        shared.push(msg(40), 1, 1);
        shared.push(msg(40), 1, 2);
        shared.push(msg(40), 1, 3);
        let by_bytes = BatchLimits {
            max_bytes: 50,
            ..BatchLimits::default()
        };
        assert_eq!(shared.drain(&by_bytes).unwrap().messages.len(), 1);
        let tiny = BatchLimits {
            max_bytes: 1,
            ..BatchLimits::default()
        };
        assert_eq!(shared.drain(&tiny).unwrap().messages.len(), 1);
        assert_eq!(shared.readiness(&batch_limits(10)), Readiness::Partial);
        assert_eq!(shared.readiness(&batch_limits(1)), Readiness::Full);
        shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(shared.readiness(&batch_limits(10)), Readiness::Empty);
        assert!(shared.drain(&batch_limits(10)).is_none());
    }

    #[test]
    fn on_receive_acknowledges_when_the_runtime_pulls() {
        let (shared, mut rx) = shared_with(Limits {
            on_receive: true,
            ..limits()
        });
        shared.push(msg(4), 1, 1);
        shared.push(msg(4), 1, 2);
        assert!(acks(&mut rx).is_empty());
        shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(acks(&mut rx), [1, 2]);
    }

    #[test]
    fn dropping_a_future_cannot_lose_messages_because_drain_is_atomic() {
        let (shared, _rx) = shared_with(limits());
        for pkid in 1..=3 {
            shared.push(msg(4), 1, pkid);
        }
        assert_eq!(shared.snapshot_sizes().queued_messages, 3);
        let drained = shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(drained.messages.len(), 3);
        assert_eq!(shared.snapshot_sizes().queued_messages, 0);
    }

    #[test]
    fn fatal_errors_are_sticky_and_visible_to_open() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (shared, status) = Shared::new(limits(), tx);
        let err = ThadamError::fatal(
            thadam_core::ErrorDomain::Auth,
            thadam_core::codes::AUTH_FAILED,
            "no",
        );
        shared.fail(err.clone());
        shared.fail(ThadamError::fatal(
            thadam_core::ErrorDomain::Connector,
            thadam_core::codes::CONNECTOR_PANIC,
            "later",
        ));
        assert_eq!(shared.fatal(), Some(err));
        assert!(matches!(&*status.borrow(), Status::Failed(e) if e.message == "no"));
    }

    #[test]
    fn ready_is_set_once_and_never_overrides_failure() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (shared, status) = Shared::new(limits(), tx);
        shared.set_ready();
        assert!(matches!(&*status.borrow(), Status::Ready));
        shared.fail(ThadamError::fatal(
            thadam_core::ErrorDomain::Auth,
            thadam_core::codes::AUTH_FAILED,
            "x",
        ));
        shared.set_ready();
        assert!(matches!(&*status.borrow(), Status::Failed(_)));
    }

    #[test]
    fn gap_attributes_combine_reasons() {
        let mut attrs = Attributes::new();
        GapInfo {
            session_lost: true,
            dropped_queue_full: 2,
            dropped_oversize: 1,
        }
        .write_attributes(&mut attrs);
        assert_eq!(
            attrs.get("mqtt.gap"),
            Some("session_lost,queue_full,oversize")
        );
        assert_eq!(attrs.get("mqtt.gap_dropped"), Some("3"));
        let mut attrs = Attributes::new();
        GapInfo {
            session_lost: true,
            ..GapInfo::default()
        }
        .write_attributes(&mut attrs);
        assert_eq!(attrs.get("mqtt.gap_dropped"), None);
    }

    #[test]
    fn closed_ack_channel_counts_dropped_acks_and_subscribes() {
        let (shared, rx) = shared_with(limits());
        drop(rx);
        shared.push(msg(4), 1, 1);
        let batch = shared.drain(&batch_limits(10)).unwrap();
        assert_eq!(complete(&shared, &batch), AckStatus::Delivered);
        assert_eq!(shared.stats.acks_requested.load(Ordering::Relaxed), 1);
        assert_eq!(shared.stats.acks_dropped.load(Ordering::Relaxed), 1);

        shared.request_subscribe();
        assert_eq!(shared.stats.acks_dropped.load(Ordering::Relaxed), 2);
    }
}
