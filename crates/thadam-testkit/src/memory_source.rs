//! An in-memory [`Source`] with a simulated broker behind it.
//!
//! [`MemorySource::new`] returns the source and a [`MemoryUpstream`], the
//! "broker" side tests publish into and inspect. The broker keeps every message
//! until it is acknowledged, redelivers unacknowledged messages after a session
//! loss, and checks that the source's protocol task keeps pinging.
//!
//! The source is a reference implementation of the contract (cancel-safe
//! `next_batch`, an independent protocol task, epoch-scoped payload-free
//! handles). Two knobs deliberately break the contract so the conformance
//! suites can be shown to catch violations: [`ProtocolTask::DrivenByConsumer`]
//! and [`NextBatchMode::DequeueThenWait`].

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thadam_core::{
    AckHandle, AckMode, AckOutcome, AckTarget, AckToken, Attributes, BatchLimits, Clock,
    ConnectionEpoch, EpochGate, ErrorDomain, RawBatch, RawMessage, Shutdown, Source, SourceBatch,
    SourceCapabilities, SourceContext, SourceId, ThadamError, codes,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::fixtures::id_payload;
use crate::util::lock;

/// Who drives protocol liveness (the keepalive pings).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolTask {
    /// An independent task pings on a timer, whatever the consumer does.
    Independent,
    /// Pings happen only inside `next_batch`: a stalled consumer stops
    /// liveness. This violates the contract and exists to test the suite.
    DrivenByConsumer,
}

/// How `next_batch` takes messages from the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextBatchMode {
    /// Messages leave the queue only in the final synchronous step.
    CancelSafe,
    /// Messages leave the queue and then the future waits, so dropping it
    /// loses them. This violates the contract and exists to test the suite.
    DequeueThenWait,
}

/// Behaviour of a [`MemorySource`].
#[derive(Clone, Debug)]
pub struct MemorySourceConfig {
    /// The protocol keepalive. The broker counts a gap of more than 1.5 times
    /// this between pings as a violation.
    pub keepalive: Duration,
    /// How the source acknowledges.
    pub ack_mode: AckMode,
    /// How long `open` takes to connect.
    pub open_delay: Duration,
    /// Errors returned by successive `open` calls before one succeeds.
    pub open_failures: Vec<ThadamError>,
    /// Who drives liveness.
    pub protocol_task: ProtocolTask,
    /// How `next_batch` dequeues.
    pub next_batch_mode: NextBatchMode,
}

impl Default for MemorySourceConfig {
    fn default() -> Self {
        MemorySourceConfig {
            keepalive: Duration::from_secs(5),
            ack_mode: AckMode::OnCommit,
            open_delay: Duration::ZERO,
            open_failures: Vec::new(),
            protocol_task: ProtocolTask::Independent,
            next_batch_mode: NextBatchMode::CancelSafe,
        }
    }
}

struct Queued {
    seq: u64,
    payload: Bytes,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Queued>,
    unacked: BTreeMap<u64, Bytes>,
    batches: BTreeMap<u64, Vec<u64>>,
    acked: Vec<(u64, Bytes)>,
    next_seq: u64,
    next_batch_no: u64,
    last_acked_batch: Option<u64>,
    finished: bool,
    closed: bool,
    last_ping: Option<Instant>,
    pings: u64,
    keepalive_violations: u64,
    session_losses: u64,
    published: u64,
    delivered: u64,
    out_of_order_acks: u64,
    open_failures: VecDeque<ThadamError>,
    next_batch_errors: VecDeque<ThadamError>,
}

struct Shared {
    cfg: MemorySourceConfig,
    state: Mutex<State>,
    notify: Notify,
    gate: EpochGate,
}

impl Shared {
    fn violation_limit(&self) -> Duration {
        self.cfg.keepalive.mul_f64(1.5)
    }

    fn record_ping(&self) {
        let now = Instant::now();
        let limit = self.violation_limit();
        let mut st = lock(&self.state);
        if let Some(prev) = st.last_ping
            && now.duration_since(prev) > limit
        {
            st.keepalive_violations += 1;
        }
        st.last_ping = Some(now);
        st.pings += 1;
    }

    fn queue_is_full(&self, limits: BatchLimits) -> bool {
        let st = lock(&self.state);
        let mut bytes = 0usize;
        for (n, q) in st.queue.iter().enumerate() {
            bytes += q.payload.len();
            if n + 1 >= limits.max_messages || bytes >= limits.max_bytes {
                return true;
            }
        }
        false
    }

    fn pop_up_to(st: &mut State, limits: BatchLimits) -> Vec<Queued> {
        let mut taken = Vec::new();
        let mut bytes = 0usize;
        while let Some(front) = st.queue.front() {
            let next_bytes = bytes + front.payload.len();
            if !taken.is_empty()
                && (taken.len() >= limits.max_messages || next_bytes > limits.max_bytes)
            {
                break;
            }
            bytes = next_bytes;
            if let Some(q) = st.queue.pop_front() {
                taken.push(q);
            }
        }
        taken
    }

    fn make_batch(
        self: &Arc<Self>,
        st: &mut State,
        taken: Vec<Queued>,
        source: &SourceId,
        clock: &dyn Clock,
    ) -> SourceBatch {
        let now = clock.now();
        let seqs: Vec<u64> = taken.iter().map(|q| q.seq).collect();
        let messages = taken
            .into_iter()
            .map(|q| {
                let mut attrs = Attributes::new();
                attrs.insert("seq", q.seq.to_string());
                RawMessage {
                    ingest_time: now,
                    attrs,
                    payload: q.payload,
                }
            })
            .collect::<Vec<_>>();
        st.delivered += messages.len() as u64;

        let ack = match self.cfg.ack_mode {
            AckMode::None => AckHandle::noop(),
            mode => {
                if mode == AckMode::OnReceive {
                    for seq in &seqs {
                        if let Some(payload) = st.unacked.remove(seq) {
                            st.acked.push((*seq, payload));
                        }
                    }
                }
                let batch_no = st.next_batch_no;
                st.next_batch_no += 1;
                let token = AckToken::new(batch_no, seqs.len() as u64);
                st.batches.insert(batch_no, seqs);
                self.gate
                    .issue(token, Arc::new(MemoryAckTarget(Arc::clone(self))))
            }
        };
        SourceBatch {
            batch: RawBatch::new(source.clone(), messages, clock.synced()),
            ack,
        }
    }
}

struct MemoryAckTarget(Arc<Shared>);

impl AckTarget for MemoryAckTarget {
    fn ack(&self, epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome) {
        let mut guard = lock(&self.0.state);
        let st = &mut *guard;
        if epoch != self.0.gate.current() {
            return;
        }
        let Some(seqs) = st.batches.remove(&token.a()) else {
            return;
        };
        if outcome != AckOutcome::Committed {
            return;
        }
        if st.last_acked_batch.is_some_and(|last| token.a() < last) {
            st.out_of_order_acks += 1;
        }
        st.last_acked_batch = Some(token.a());
        for seq in seqs {
            if let Some(payload) = st.unacked.remove(&seq) {
                st.acked.push((seq, payload));
            }
        }
    }
}

struct Opened {
    source_id: SourceId,
    shutdown: Shutdown,
    clock: Arc<dyn Clock>,
}

/// The source half. See the module docs.
pub struct MemorySource {
    shared: Arc<Shared>,
    opened: Option<Opened>,
    protocol: Option<(Shutdown, JoinHandle<()>)>,
}

impl MemorySource {
    /// A source and the upstream broker feeding it.
    pub fn new(cfg: MemorySourceConfig) -> (MemorySource, MemoryUpstream) {
        let state = State {
            open_failures: cfg.open_failures.iter().cloned().collect(),
            next_seq: 1,
            next_batch_no: 1,
            ..State::default()
        };
        let shared = Arc::new(Shared {
            cfg,
            state: Mutex::new(state),
            notify: Notify::new(),
            gate: EpochGate::new(),
        });
        (
            MemorySource {
                shared: Arc::clone(&shared),
                opened: None,
                protocol: None,
            },
            MemoryUpstream { shared },
        )
    }

    /// A source over a fixed script of payloads, already published and
    /// finished: `next_batch` returns `Ok(None)` after they are drained.
    pub fn scripted(
        payloads: impl IntoIterator<Item = Bytes>,
        cfg: MemorySourceConfig,
    ) -> (MemorySource, MemoryUpstream) {
        let (source, upstream) = Self::new(cfg);
        for p in payloads {
            upstream.publish(p);
        }
        upstream.finish();
        (source, upstream)
    }

    /// A source over `count` messages whose payloads are [`id_payload`]s of
    /// `0..count`, already published and finished.
    pub fn counting(count: u64, cfg: MemorySourceConfig) -> (MemorySource, MemoryUpstream) {
        Self::scripted((0..count).map(id_payload), cfg)
    }
}

fn cancelled(what: &str) -> ThadamError {
    ThadamError::transient(
        ErrorDomain::Connector,
        codes::CONNECTOR_CANCELLED,
        format!("{what} cancelled by shutdown"),
    )
}

#[async_trait]
impl Source for MemorySource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        let shared = Arc::clone(&self.shared);
        if !shared.cfg.open_delay.is_zero() {
            tokio::select! {
                () = tokio::time::sleep(shared.cfg.open_delay) => {}
                () = ctx.shutdown.triggered() => return Err(cancelled("connect")),
            }
        }
        if let Some(err) = lock(&shared.state).open_failures.pop_front() {
            return Err(err);
        }
        shared.record_ping();
        self.opened = Some(Opened {
            source_id: ctx.source_id.clone(),
            shutdown: ctx.shutdown.clone(),
            clock: Arc::clone(&ctx.clock),
        });
        if shared.cfg.protocol_task == ProtocolTask::Independent && self.protocol.is_none() {
            let stop = ctx.shutdown.child();
            let task_stop = stop.clone();
            let task_shared = Arc::clone(&shared);
            let handle = tokio::spawn(async move {
                let period = task_shared.cfg.keepalive / 3;
                loop {
                    tokio::select! {
                        () = task_stop.triggered() => break,
                        () = tokio::time::sleep(period) => task_shared.record_ping(),
                    }
                }
            });
            self.protocol = Some((stop, handle));
        }
        Ok(())
    }

    async fn next_batch(
        &mut self,
        limits: BatchLimits,
    ) -> Result<Option<SourceBatch>, ThadamError> {
        let shared = Arc::clone(&self.shared);
        let Some(opened) = &self.opened else {
            return Err(ThadamError::permanent(
                ErrorDomain::Connector,
                codes::CONNECTOR_PROTOCOL,
                "next_batch called before open",
            ));
        };
        if shared.cfg.protocol_task == ProtocolTask::DrivenByConsumer {
            shared.record_ping();
        }
        if let Some(err) = lock(&shared.state).next_batch_errors.pop_front() {
            return Err(err);
        }

        loop {
            let notified = shared.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let st = lock(&shared.state);
                if !st.queue.is_empty() {
                    break;
                }
                if st.finished || opened.shutdown.is_triggered() {
                    return Ok(None);
                }
            }
            tokio::select! {
                () = &mut notified => {}
                () = opened.shutdown.triggered() => {}
            }
        }

        if shared.cfg.next_batch_mode == NextBatchMode::DequeueThenWait {
            let taken = Shared::pop_up_to(&mut lock(&shared.state), limits);
            tokio::time::sleep(Duration::from_millis(1)).await;
            let mut guard = lock(&shared.state);
            let batch =
                shared.make_batch(&mut guard, taken, &opened.source_id, opened.clock.as_ref());
            return Ok(Some(batch));
        }

        if !limits.linger.is_zero() {
            let deadline = Instant::now() + limits.linger;
            loop {
                let notified = shared.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if shared.queue_is_full(limits) {
                    break;
                }
                tokio::select! {
                    () = &mut notified => {}
                    () = tokio::time::sleep_until(deadline) => break,
                    () = opened.shutdown.triggered() => break,
                }
            }
        }

        let mut guard = lock(&shared.state);
        let taken = Shared::pop_up_to(&mut guard, limits);
        if taken.is_empty() {
            return Ok(None);
        }
        let batch = shared.make_batch(&mut guard, taken, &opened.source_id, opened.clock.as_ref());
        Ok(Some(batch))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        if let Some((stop, handle)) = self.protocol.take() {
            stop.trigger();
            let _ = handle.await;
        }
        lock(&self.shared.state).closed = true;
        Ok(())
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::new(self.shared.cfg.ack_mode)
    }
}

/// Counters from the broker's point of view.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpstreamStats {
    /// Messages published.
    pub published: u64,
    /// Messages handed to the consumer, redeliveries included.
    pub delivered: u64,
    /// Keepalive pings the source sent.
    pub pings: u64,
    /// Gaps between pings longer than 1.5 times the keepalive, including a
    /// gap still open now. A real broker would have dropped the connection.
    pub keepalive_violations: u64,
    /// Times the session was lost.
    pub session_losses: u64,
    /// Messages acknowledged.
    pub acked: u64,
    /// Batch acknowledgements that arrived after a later batch's.
    pub out_of_order_acks: u64,
    /// Acknowledgements dropped because their epoch had ended.
    pub stale_acks: u64,
    /// Handles dropped without completion.
    pub lost_acks: u64,
    /// Handles issued and not yet completed or dropped.
    pub outstanding_handles: u64,
}

/// The broker side of a [`MemorySource`].
#[derive(Clone)]
pub struct MemoryUpstream {
    shared: Arc<Shared>,
}

impl MemoryUpstream {
    /// Publishes a message, returning the broker's sequence number for it.
    pub fn publish(&self, payload: Bytes) -> u64 {
        let seq = {
            let mut st = lock(&self.shared.state);
            let seq = st.next_seq;
            st.next_seq += 1;
            st.published += 1;
            if self.shared.cfg.ack_mode != AckMode::None {
                st.unacked.insert(seq, payload.clone());
            }
            st.queue.push_back(Queued { seq, payload });
            seq
        };
        self.shared.notify.notify_waiters();
        seq
    }

    /// Ends the stream: once the queue drains, `next_batch` returns `Ok(None)`.
    pub fn finish(&self) {
        lock(&self.shared.state).finished = true;
        self.shared.notify.notify_waiters();
    }

    /// Simulates a session loss: the epoch advances, everything queued in the
    /// old session is dropped, and the broker redelivers every unacknowledged
    /// message in order under the new epoch.
    pub fn lose_session(&self) {
        {
            let mut guard = lock(&self.shared.state);
            let st = &mut *guard;
            self.shared.gate.advance();
            st.session_losses += 1;
            st.queue.clear();
            st.batches.clear();
            for (seq, payload) in &st.unacked {
                st.queue.push_back(Queued {
                    seq: *seq,
                    payload: payload.clone(),
                });
            }
        }
        self.shared.notify.notify_waiters();
    }

    /// Makes the next `next_batch` call fail with `error`.
    pub fn fail_next_batch(&self, error: ThadamError) {
        lock(&self.shared.state).next_batch_errors.push_back(error);
    }

    /// Broker sequence numbers acknowledged, in the order acknowledgements
    /// arrived.
    pub fn acked_seqs(&self) -> Vec<u64> {
        lock(&self.shared.state)
            .acked
            .iter()
            .map(|(seq, _)| *seq)
            .collect()
    }

    /// Payloads acknowledged, in acknowledgement order.
    pub fn acked_payloads(&self) -> Vec<Bytes> {
        lock(&self.shared.state)
            .acked
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect()
    }

    /// Messages published and not yet acknowledged.
    pub fn unacked_count(&self) -> usize {
        lock(&self.shared.state).unacked.len()
    }

    /// The source's epoch gate, for stale and lost-token counters.
    pub fn gate(&self) -> EpochGate {
        self.shared.gate.clone()
    }

    /// The configured keepalive.
    pub fn keepalive(&self) -> Duration {
        self.shared.cfg.keepalive
    }

    /// A snapshot of the broker's counters.
    pub fn stats(&self) -> UpstreamStats {
        let st = lock(&self.shared.state);
        let open_gap_violation = !st.closed
            && st
                .last_ping
                .is_some_and(|p| Instant::now().duration_since(p) > self.shared.violation_limit());
        UpstreamStats {
            published: st.published,
            delivered: st.delivered,
            pings: st.pings,
            keepalive_violations: st.keepalive_violations + u64::from(open_gap_violation),
            session_losses: st.session_losses,
            acked: st.acked.len() as u64,
            out_of_order_acks: st.out_of_order_acks,
            stale_acks: self.shared.gate.stale_acks(),
            lost_acks: self.shared.gate.lost_acks(),
            outstanding_handles: self.shared.gate.outstanding(),
        }
    }
}
