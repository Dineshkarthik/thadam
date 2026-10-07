//! Counters, the public metrics snapshot, and the session-loss hook.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use thadam_core::{ConnectionEpoch, EpochGate, Timestamp};

use crate::state::Shared;

/// Live counters. Updated with relaxed atomics from the poll task, the ack
/// task, and the source.
#[derive(Debug, Default)]
pub(crate) struct MqttStats {
    pub connected: AtomicBool,
    pub connects: AtomicU64,
    pub disconnects: AtomicU64,
    pub messages_received: AtomicU64,
    pub bytes_received: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub duplicates: AtomicU64,
    pub dropped_queue_full: AtomicU64,
    pub dropped_oversize: AtomicU64,
    pub oversize_disconnects: AtomicU64,
    pub session_lost: AtomicU64,
    pub purged_on_resume: AtomicU64,
    pub stale_ledger_entries: AtomicU64,
    pub abandoned: AtomicU64,
    pub acks_requested: AtomicU64,
    pub acks_sent: AtomicU64,
    pub acks_dropped: AtomicU64,
    pub acks_written: AtomicU64,
    pub unacked_max: AtomicU64,
    pub guard_trips: AtomicU64,
}

/// A point-in-time copy of the connector's counters. The names map to the
/// metrics ARCHITECTURE.md section 15 lists (`thadam_source_messages_total`,
/// `thadam_source_dropped_total{reason}`, `thadam_source_connected`,
/// `thadam_source_unacked_max`, `thadam_source_stale_acks_total`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MqttStatsSnapshot {
    /// Whether a broker connection is currently up.
    pub connected: bool,
    /// The current connection epoch.
    pub epoch: u64,
    /// Successful connections, including the first.
    pub connects: u64,
    /// Connections that dropped after being established.
    pub disconnects: u64,
    /// PUBLISH packets received from the broker.
    pub messages_received: u64,
    /// Payload bytes received.
    pub bytes_received: u64,
    /// Messages handed to the runtime.
    pub messages_delivered: u64,
    /// Received messages flagged DUP: broker redelivery after a reconnect.
    pub duplicates: u64,
    /// QoS 0 messages dropped because the queue budget was full.
    pub dropped_queue_full: u64,
    /// Messages dropped because the payload exceeded `max_message_bytes`.
    pub dropped_oversize: u64,
    /// Connections the codec failed on an over-limit packet.
    pub oversize_disconnects: u64,
    /// Times the broker lost a session that was expected to exist.
    pub session_lost: u64,
    /// Queued QoS 1 and 2 messages discarded on resume (the broker redelivers them).
    pub purged_on_resume: u64,
    /// Handles dropped because they belong to an older connection epoch.
    pub stale_acks: u64,
    /// Ledger entries of a dead connection dropped without acknowledging.
    pub stale_ledger_entries: u64,
    /// Handles dropped without completion (lost tokens).
    pub lost_acks: u64,
    /// Handles issued and not yet completed.
    pub outstanding_acks: u64,
    /// Entries abandoned by the runtime (shutdown or fatal error).
    pub abandoned: u64,
    /// Acknowledgements requested after commit (or on receive).
    pub acks_requested: u64,
    /// Acknowledgements the ack task handed to the client.
    pub acks_sent: u64,
    /// Acknowledgements the ack task dropped because their epoch ended or
    /// the client closed.
    pub acks_dropped: u64,
    /// Acknowledgements the client wrote to the socket.
    pub acks_written: u64,
    /// The most unacknowledged messages ever held: equals the broker's window.
    pub unacked_max: u64,
    /// Unacknowledged messages right now.
    pub unacked: u64,
    /// Times the unacknowledged-message guard disconnected.
    pub guard_trips: u64,
    /// Messages in the incoming queue.
    pub queued_messages: u64,
    /// Payload bytes in the incoming queue.
    pub queued_bytes: u64,
}

impl MqttStatsSnapshot {
    /// Acknowledgements requested and neither written to the socket nor
    /// dropped. Zero means the acknowledgement pipeline is drained.
    pub fn acks_pending(&self) -> u64 {
        self.acks_requested
            .saturating_sub(self.acks_written + self.acks_dropped)
    }
}

/// Reported when a broker that should have kept the session did not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionLost {
    /// When the connection that revealed the loss was established.
    pub at: Timestamp,
    /// The new connection epoch.
    pub epoch: ConnectionEpoch,
    /// The client identifier whose session is gone.
    pub client_id: String,
}

/// Called from the poll task on session loss. It must return quickly and must
/// not block: the poll task is the connection's heartbeat.
pub type SessionLostHook = Arc<dyn Fn(&SessionLost) + Send + Sync>;

/// A cloneable handle to a source's metrics and epoch gate, usable after the
/// source has been boxed into a `dyn Source`.
#[derive(Clone)]
pub struct MqttHandle {
    shared: Arc<Shared>,
}

impl fmt::Debug for MqttHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttHandle").finish_non_exhaustive()
    }
}

impl MqttHandle {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        MqttHandle { shared }
    }

    /// The current counters.
    pub fn stats(&self) -> MqttStatsSnapshot {
        let s = &self.shared.stats;
        let sizes = self.shared.snapshot_sizes();
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        MqttStatsSnapshot {
            connected: s.connected.load(Ordering::Relaxed),
            epoch: self.shared.gate.current().get(),
            connects: load(&s.connects),
            disconnects: load(&s.disconnects),
            messages_received: load(&s.messages_received),
            bytes_received: load(&s.bytes_received),
            messages_delivered: load(&s.messages_delivered),
            duplicates: load(&s.duplicates),
            dropped_queue_full: load(&s.dropped_queue_full),
            dropped_oversize: load(&s.dropped_oversize),
            oversize_disconnects: load(&s.oversize_disconnects),
            session_lost: load(&s.session_lost),
            purged_on_resume: load(&s.purged_on_resume),
            stale_acks: self.shared.gate.stale_acks(),
            stale_ledger_entries: load(&s.stale_ledger_entries),
            lost_acks: self.shared.gate.lost_acks(),
            outstanding_acks: self.shared.gate.outstanding(),
            abandoned: load(&s.abandoned),
            acks_requested: load(&s.acks_requested),
            acks_sent: load(&s.acks_sent),
            acks_dropped: load(&s.acks_dropped),
            acks_written: load(&s.acks_written),
            unacked_max: load(&s.unacked_max),
            unacked: sizes.unacked as u64,
            guard_trips: load(&s.guard_trips),
            queued_messages: sizes.queued_messages as u64,
            queued_bytes: sizes.queued_bytes as u64,
        }
    }

    /// The epoch gate every ack handle is issued through. Its counters back
    /// `thadam_source_stale_acks_total` and the lost-token watchdog.
    pub fn epoch_gate(&self) -> EpochGate {
        self.shared.gate.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_acks_saturate_and_count_drops_as_settled() {
        let snap = MqttStatsSnapshot {
            acks_requested: 9,
            acks_written: 5,
            acks_dropped: 1,
            ..MqttStatsSnapshot::default()
        };
        assert_eq!(snap.acks_pending(), 3);
        let settled = MqttStatsSnapshot {
            acks_requested: 2,
            acks_written: 5,
            ..MqttStatsSnapshot::default()
        };
        assert_eq!(settled.acks_pending(), 0);
    }
}
