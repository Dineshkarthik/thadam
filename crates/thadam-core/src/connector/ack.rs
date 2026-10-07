//! Acknowledgement handles: payload-free, scoped to a connection epoch.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A generation of a connection or session. A connector bumps it whenever the
/// session is lost, which invalidates every handle issued before.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ConnectionEpoch(u64);

impl ConnectionEpoch {
    /// The epoch every connection starts in.
    pub const FIRST: ConnectionEpoch = ConnectionEpoch(1);

    /// The epoch number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The protocol's acknowledgement identity for one message, and nothing else.
///
/// Two words are enough for the supported protocols (for MQTT, the packet
/// identifier and QoS packed into one). A token cannot hold a payload, so a
/// pending acknowledgement never pins message memory.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct AckToken {
    a: u64,
    b: u64,
}

impl AckToken {
    /// A token from two connector-defined words.
    pub const fn new(a: u64, b: u64) -> Self {
        AckToken { a, b }
    }

    /// The first word.
    pub const fn a(self) -> u64 {
        self.a
    }

    /// The second word.
    pub const fn b(self) -> u64 {
        self.b
    }
}

/// What the runtime tells the connector about a message.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum AckOutcome {
    /// The batch is durably committed: acknowledge to the protocol.
    Committed,
    /// The batch will not be committed in this run (shutdown or a fatal
    /// error). Do not acknowledge, so the peer redelivers.
    Abandoned,
}

/// What happened to a completed handle.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum AckStatus {
    /// The outcome was forwarded to the connector's [`AckTarget`].
    Delivered,
    /// The handle belongs to an older epoch. Nothing was forwarded; the drop
    /// was counted on the [`EpochGate`].
    Stale,
}

/// The connector side of an acknowledgement: turns a token into a protocol
/// message (for MQTT, a PUBACK).
///
/// A target is typically bound to one session. [`AckHandle::complete`] has
/// already screened out stale epochs, but a session can end between that check
/// and the call, so a target that serializes against session changes should
/// compare `epoch` again under its own lock.
pub trait AckTarget: Send + Sync + 'static {
    /// Acknowledges the message identified by `token`.
    fn ack(&self, epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome);
}

#[derive(Debug)]
struct GateInner {
    epoch: AtomicU64,
    stale: AtomicU64,
    lost: AtomicU64,
    outstanding: AtomicU64,
}

/// The connector's view of the current epoch and the counters behind the
/// `thadam_source_stale_acks_total` and lost-token metrics.
///
/// Clones share state. A connector creates one gate, advances it on every
/// session loss, and issues every [`AckHandle`] through it.
#[derive(Clone, Debug)]
pub struct EpochGate(Arc<GateInner>);

impl Default for EpochGate {
    fn default() -> Self {
        Self::new()
    }
}

impl EpochGate {
    /// A gate in [`ConnectionEpoch::FIRST`].
    pub fn new() -> Self {
        EpochGate(Arc::new(GateInner {
            epoch: AtomicU64::new(ConnectionEpoch::FIRST.0),
            stale: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            outstanding: AtomicU64::new(0),
        }))
    }

    /// The current epoch.
    pub fn current(&self) -> ConnectionEpoch {
        ConnectionEpoch(self.0.epoch.load(Ordering::Acquire))
    }

    /// Starts a new epoch, invalidating all outstanding handles, and returns it.
    pub fn advance(&self) -> ConnectionEpoch {
        ConnectionEpoch(self.0.epoch.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// Issues a handle for the message identified by `token`, valid in the
    /// current epoch.
    pub fn issue(&self, token: AckToken, target: Arc<dyn AckTarget>) -> AckHandle {
        self.0.outstanding.fetch_add(1, Ordering::Relaxed);
        AckHandle {
            gate: self.clone(),
            epoch: self.current(),
            token,
            target,
            done: false,
        }
    }

    /// Handles completed after their epoch ended, which were dropped.
    pub fn stale_acks(&self) -> u64 {
        self.0.stale.load(Ordering::Relaxed)
    }

    /// Handles dropped without being completed: lost tokens that would stall
    /// an ordered ack window.
    pub fn lost_acks(&self) -> u64 {
        self.0.lost.load(Ordering::Relaxed)
    }

    /// Handles issued and neither completed nor dropped yet.
    pub fn outstanding(&self) -> u64 {
        self.0.outstanding.load(Ordering::Relaxed)
    }
}

struct NoopTarget;

impl AckTarget for NoopTarget {
    fn ack(&self, _: ConnectionEpoch, _: AckToken, _: AckOutcome) {}
}

/// Permission to acknowledge one batch to the protocol.
///
/// Completing it forwards the outcome to the connector, unless the connection
/// epoch has moved on, in which case the completion is dropped and counted. A
/// handle holds only an [`AckToken`], never payload bytes. Dropping a handle
/// without completing it is a lost token and is counted.
#[must_use = "an AckHandle that is dropped without being completed stalls the source"]
pub struct AckHandle {
    gate: EpochGate,
    epoch: ConnectionEpoch,
    token: AckToken,
    target: Arc<dyn AckTarget>,
    done: bool,
}

impl AckHandle {
    /// A handle that acknowledges nothing, for sources with
    /// [`AckMode::None`](crate::AckMode::None).
    pub fn noop() -> AckHandle {
        EpochGate::new().issue(AckToken::default(), Arc::new(NoopTarget))
    }

    /// The epoch the handle was issued in.
    pub fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// The protocol identity of the message.
    pub fn token(&self) -> AckToken {
        self.token
    }

    /// Completes the handle. A stale handle is dropped and counted, never
    /// forwarded.
    pub fn complete(mut self, outcome: AckOutcome) -> AckStatus {
        self.done = true;
        self.gate.0.outstanding.fetch_sub(1, Ordering::Relaxed);
        if self.gate.current() != self.epoch {
            self.gate.0.stale.fetch_add(1, Ordering::Relaxed);
            return AckStatus::Stale;
        }
        self.target.ack(self.epoch, self.token, outcome);
        AckStatus::Delivered
    }
}

impl Drop for AckHandle {
    fn drop(&mut self) {
        if !self.done {
            self.gate.0.lost.fetch_add(1, Ordering::Relaxed);
            self.gate.0.outstanding.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl fmt::Debug for AckHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AckHandle")
            .field("epoch", &self.epoch)
            .field("token", &self.token)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<(u64, AckToken, AckOutcome)>>);

    impl AckTarget for Recorder {
        fn ack(&self, epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome) {
            self.0.lock().unwrap().push((epoch.get(), token, outcome));
        }
    }

    #[test]
    fn current_epoch_handles_are_forwarded() {
        let gate = EpochGate::new();
        let rec = Arc::new(Recorder::default());
        let h = gate.issue(AckToken::new(5, 1), rec.clone());
        assert_eq!(gate.outstanding(), 1);
        assert_eq!(h.complete(AckOutcome::Committed), AckStatus::Delivered);
        assert_eq!(gate.outstanding(), 0);
        assert_eq!(
            *rec.0.lock().unwrap(),
            vec![(1, AckToken::new(5, 1), AckOutcome::Committed)]
        );
    }

    #[test]
    fn stale_handles_are_counted_and_never_forwarded() {
        let gate = EpochGate::new();
        let rec = Arc::new(Recorder::default());
        let old = gate.issue(AckToken::new(1, 0), rec.clone());
        assert_eq!(gate.advance(), ConnectionEpoch(2));
        let new = gate.issue(AckToken::new(1, 0), rec.clone());

        assert_eq!(old.complete(AckOutcome::Committed), AckStatus::Stale);
        assert_eq!(gate.stale_acks(), 1);
        assert!(rec.0.lock().unwrap().is_empty());

        assert_eq!(new.complete(AckOutcome::Committed), AckStatus::Delivered);
        assert_eq!(rec.0.lock().unwrap().len(), 1);
        assert_eq!(gate.outstanding(), 0);
    }

    #[test]
    fn dropped_handles_are_lost_tokens() {
        let gate = EpochGate::new();
        let rec = Arc::new(Recorder::default());
        let h = gate.issue(AckToken::new(1, 0), rec.clone());
        drop(h);
        assert_eq!(gate.lost_acks(), 1);
        assert_eq!(gate.outstanding(), 0);
        assert!(rec.0.lock().unwrap().is_empty());
    }

    #[test]
    fn noop_handles_complete_quietly() {
        assert_eq!(
            AckHandle::noop().complete(AckOutcome::Committed),
            AckStatus::Delivered
        );
    }

    #[test]
    fn handle_holds_no_payload() {
        assert!(size_of::<AckHandle>() <= 64, "{}", size_of::<AckHandle>());
        let h = AckHandle::noop();
        assert!(format!("{h:?}").contains("token"));
        let _ = h.complete(AckOutcome::Abandoned);
    }
}
