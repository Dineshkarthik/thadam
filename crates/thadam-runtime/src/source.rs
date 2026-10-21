//! The source driver: pull, submit to the WAL, acknowledge in order.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use thadam_core::{
    AckHandle, AckOutcome, BatchLimits, Clock, RawBatch, Shutdown, Source, SourceBatch,
    SourceContext, SourceId, ThadamError, WalRange,
};
use thadam_wal::{CommitTicket, WalError, WalWriter};

use crate::backoff::Backoff;
use crate::chain::wal_error;
use crate::metrics::MetricEvent;
use crate::options::BackoffPolicy;
use crate::segment::pause;
use crate::status::{Component, add};

/// Why a submission failed.
pub(crate) enum SubmitError {
    /// The batch encodes to more than one frame may hold; split it.
    TooLarge(ThadamError),
    /// The WAL cannot take more data in this run.
    Wal(ThadamError),
}

/// Where a source driver submits batches: the WAL writer in production.
pub(crate) trait Submit: Send + Sync + 'static {
    type Ticket: Future<Output = Result<WalRange, ThadamError>> + Send + Unpin + 'static;

    /// Queues a batch; cancel-safe (dropping the future queues nothing).
    fn submit(
        &self,
        batch: RawBatch,
    ) -> impl Future<Output = Result<Self::Ticket, SubmitError>> + Send;
}

pub(crate) struct WalTicket(CommitTicket);

impl Future for WalTicket {
    type Output = Result<WalRange, ThadamError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.map_err(|e| wal_error(&e)))
    }
}

impl Submit for WalWriter {
    type Ticket = WalTicket;

    async fn submit(&self, batch: RawBatch) -> Result<WalTicket, SubmitError> {
        match WalWriter::submit(self, batch).await {
            Ok(t) => Ok(WalTicket(t)),
            Err(e @ WalError::FrameTooLarge { .. }) => Err(SubmitError::TooLarge(wal_error(&e))),
            Err(e) => Err(SubmitError::Wal(wal_error(&e))),
        }
    }
}

/// Batches above this many payload bytes are cloned before submission so a
/// frame-size refusal can split them; smaller ones always fit a frame at the
/// default sizes, and cloning them would cost more than it saves.
const SPLITTABLE_BYTES: usize = 64 << 10;

struct Pending<T> {
    ticket: Option<T>,
    ack: Option<AckHandle>,
    at: Instant,
}

enum End {
    Eof,
    Shutdown,
    Source(ThadamError),
    Wal(ThadamError),
}

pub(crate) struct SourceDriver<S: Submit> {
    pub id: String,
    pub component: Arc<Component>,
    pub source: Box<dyn Source>,
    pub submit: S,
    /// Stops intake: no more `next_batch` beyond draining the queue.
    pub intake: Shutdown,
    pub clock: Arc<dyn Clock>,
    pub limits: BatchLimits,
    pub max_uncommitted: usize,
    pub restart: BackoffPolicy,
    pub drain_timeout: Duration,
    /// Triggered when the WAL can no longer commit, so the run stops.
    pub wal_failed: Shutdown,
}

impl<S: Submit> SourceDriver<S> {
    fn gauges(&self, fifo: &VecDeque<Pending<S::Ticket>>) {
        self.component
            .counters
            .uncommitted_batches
            .store(fifo.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let oldest = fifo.front().map(|p| p.at);
        self.component.set_oldest_pending(oldest);
        self.component.metrics().record(MetricEvent::AckOldestAge {
            source: &self.id,
            age: oldest.map_or(Duration::ZERO, |t| t.elapsed()),
        });
        self.component
            .metrics()
            .record(MetricEvent::SourceUnackedMax {
                source: &self.id,
                unacked: fifo.len(),
            });
    }

    fn ack_head(&self, fifo: &mut VecDeque<Pending<S::Ticket>>) {
        if let Some(p) = fifo.pop_front()
            && let Some(ack) = p.ack
        {
            if ack.complete(AckOutcome::Committed) == thadam_core::AckStatus::Stale {
                self.component.metrics().record(MetricEvent::Connector(
                    thadam_core::ConnectorMetric::SourceStaleAcks {
                        source: &self.id,
                        count: 1,
                    },
                ));
            }
            self.component.metrics().record(MetricEvent::SourceAcked {
                source: &self.id,
                latency: p.at.elapsed(),
            });
        }
    }

    /// Acknowledges every leading entry that needs no commit (empty batches,
    /// dropped oversized messages), so they keep their place in order.
    fn release_ready(&self, fifo: &mut VecDeque<Pending<S::Ticket>>) {
        while fifo.front().is_some_and(|p| p.ticket.is_none()) {
            self.ack_head(fifo);
        }
    }

    fn abandon(fifo: &mut VecDeque<Pending<S::Ticket>>) {
        for p in fifo.drain(..) {
            if let Some(ack) = p.ack {
                let _ = ack.complete(AckOutcome::Abandoned);
            }
        }
    }

    /// Waits for every pending ticket in order and acknowledges each.
    async fn settle(&mut self, fifo: &mut VecDeque<Pending<S::Ticket>>) -> Result<(), ThadamError> {
        loop {
            self.release_ready(fifo);
            self.gauges(fifo);
            let Some(head) = fifo.front_mut() else {
                return Ok(());
            };
            let Some(t) = head.ticket.as_mut() else {
                continue;
            };
            if let Err(e) = t.await {
                Self::abandon(fifo);
                return Err(e);
            }
            self.ack_head(fifo);
        }
    }

    /// Submits `batch`, splitting it if it does not fit one frame. `Ok(false)`
    /// if intake stopped while waiting for room (when `cancellable`).
    async fn push(
        &mut self,
        sb: SourceBatch,
        fifo: &mut VecDeque<Pending<S::Ticket>>,
        cancellable: bool,
    ) -> Result<bool, ThadamError> {
        let SourceBatch { batch, ack } = sb;
        let at = Instant::now();
        let c = &self.component.counters;
        add(&c.messages, batch.len() as u64);
        add(&c.bytes, batch.payload_bytes() as u64);
        add(&c.batches, 1);
        self.component.metrics().record(MetricEvent::SourceBatch {
            source: &self.id,
            messages: batch.len(),
            bytes: batch.payload_bytes(),
        });
        let mut parts = VecDeque::from([batch]);
        let mut tickets = Vec::new();
        while let Some(part) = parts.pop_front() {
            if part.is_empty() {
                continue;
            }
            let spare =
                (part.payload_bytes() >= SPLITTABLE_BYTES && part.len() > 1).then(|| part.clone());
            let submit_started = Instant::now();
            let result = if cancellable {
                tokio::select! {
                    biased;
                    r = self.submit.submit(part) => r,
                    () = self.intake.triggered() => {
                        let waited = submit_started.elapsed();
                        if !waited.is_zero() {
                            self.component.metrics().record(MetricEvent::SourceBackpressure {
                                source: &self.id,
                                duration: waited,
                            });
                        }
                        for t in tickets {
                            fifo.push_back(Pending { ticket: Some(t), ack: None, at });
                        }
                        let _ = ack.complete(AckOutcome::Abandoned);
                        return Ok(false);
                    }
                }
            } else {
                self.submit.submit(part).await
            };
            let waited = submit_started.elapsed();
            if !waited.is_zero() {
                self.component
                    .metrics()
                    .record(MetricEvent::SourceBackpressure {
                        source: &self.id,
                        duration: waited,
                    });
            }
            match result {
                Ok(t) => tickets.push(t),
                Err(SubmitError::Wal(e)) => {
                    for t in tickets {
                        fifo.push_back(Pending {
                            ticket: Some(t),
                            ack: None,
                            at,
                        });
                    }
                    let _ = ack.complete(AckOutcome::Abandoned);
                    return Err(e);
                }
                Err(SubmitError::TooLarge(e)) => match spare {
                    Some(mut whole) => {
                        let tail = whole.messages.split_off(whole.messages.len() / 2);
                        let second = RawBatch::new(whole.source.clone(), tail, whole.clock_synced);
                        parts.push_front(second);
                        parts.push_front(whole);
                    }
                    None => {
                        add(&c.rejects, 1);
                        self.component.metrics().record(MetricEvent::SourceDropped {
                            source: &self.id,
                            reason: "too_large",
                            count: 1,
                        });
                        tracing::error!(source = %self.id, error = %e, "message too large for a wal frame; dropped");
                    }
                },
            }
        }
        let last = tickets.pop();
        for t in tickets {
            fifo.push_back(Pending {
                ticket: Some(t),
                ack: None,
                at,
            });
        }
        fifo.push_back(Pending {
            ticket: last,
            ack: Some(ack),
            at,
        });
        Ok(true)
    }

    async fn pump(
        &mut self,
        fifo: &mut VecDeque<Pending<S::Ticket>>,
        backoff: &mut Backoff,
    ) -> End {
        loop {
            self.release_ready(fifo);
            self.gauges(fifo);
            let can_pull = fifo.len() < self.max_uncommitted.max(1);
            let bp_started = (!can_pull).then(Instant::now);
            let has_head = !fifo.is_empty();
            let head =
                std::future::poll_fn(
                    |cx| match fifo.front_mut().and_then(|p| p.ticket.as_mut()) {
                        Some(t) => Pin::new(t).poll(cx),
                        None => Poll::Pending,
                    },
                );
            tokio::select! {
                biased;
                r = head, if has_head => {
                    if let Some(t0) = bp_started {
                        self.component.metrics().record(MetricEvent::SourceBackpressure {
                            source: &self.id,
                            duration: t0.elapsed(),
                        });
                    }
                    match r {
                        Ok(_) => self.ack_head(fifo),
                        Err(e) => return End::Wal(e),
                    }
                },
                () = self.intake.triggered() => {
                    if let Some(t0) = bp_started {
                        self.component.metrics().record(MetricEvent::SourceBackpressure {
                            source: &self.id,
                            duration: t0.elapsed(),
                        });
                    }
                    return End::Shutdown;
                },
                r = self.source.next_batch(self.limits), if can_pull => match r {
                    Ok(Some(sb)) => {
                        backoff.reset();
                        match self.push(sb, fifo, true).await {
                            Ok(true) => {}
                            Ok(false) => return End::Shutdown,
                            Err(e) => return End::Wal(e),
                        }
                    }
                    Ok(None) => return End::Eof,
                    Err(e) => return End::Source(e),
                },
            }
        }
    }

    /// After intake stopped: drain what the connector already holds into the
    /// WAL (bounded by the drain timeout), then settle every ticket.
    async fn drain(&mut self, fifo: &mut VecDeque<Pending<S::Ticket>>) -> Result<(), ThadamError> {
        let deadline = tokio::time::Instant::now() + self.drain_timeout;
        loop {
            self.release_ready(fifo);
            if fifo.len() >= self.max_uncommitted.max(1) {
                let bp_started = Instant::now();
                let res = match fifo.front_mut().and_then(|p| p.ticket.as_mut()) {
                    Some(t) => t.await.err(),
                    None => None,
                };
                self.component
                    .metrics()
                    .record(MetricEvent::SourceBackpressure {
                        source: &self.id,
                        duration: bp_started.elapsed(),
                    });
                if let Some(e) = res {
                    Self::abandon(fifo);
                    return Err(e);
                }
                self.ack_head(fifo);
                continue;
            }
            let next = tokio::select! {
                r = self.source.next_batch(self.limits) => r,
                () = tokio::time::sleep_until(deadline) => break,
            };
            match next {
                Ok(Some(sb)) => {
                    if let Err(e) = self.push(sb, fifo, false).await {
                        Self::abandon(fifo);
                        return Err(e);
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(source = %self.id, error = %e, "next_batch failed while draining");
                    break;
                }
            }
        }
        self.settle(fifo).await
    }

    async fn close(&mut self) {
        if let Err(e) = self.source.close().await {
            tracing::warn!(source = %self.id, error = %e, "source close failed");
        }
    }

    async fn finish_wal(&mut self, e: ThadamError) -> Result<(), ThadamError> {
        self.close().await;
        self.wal_failed.trigger();
        Err(e)
    }

    pub async fn run(mut self) -> Result<(), ThadamError> {
        let ctx = SourceContext::new(
            SourceId::new(self.id.as_str()),
            self.intake.clone(),
            Arc::clone(&self.clock),
        )
        .with_metrics(crate::metrics::connector_metrics(
            self.component.metrics_arc(),
        ));
        let mut backoff = Backoff::new(self.restart);
        let mut fifo: VecDeque<Pending<S::Ticket>> = VecDeque::new();
        loop {
            loop {
                match self.source.open(&ctx).await {
                    Ok(()) => {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: true,
                            });
                        break;
                    }
                    Err(_) if self.intake.is_triggered() => {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: false,
                            });
                        self.component.stopped();
                        return Ok(());
                    }
                    Err(e) if e.is_transient() => {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: false,
                            });
                        self.component.degraded(&e);
                        add(&self.component.counters.restarts, 1);
                        if !pause(&mut backoff, &self.intake).await {
                            self.component.stopped();
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: false,
                            });
                        return Err(e);
                    }
                }
            }
            self.component.running();
            match self.pump(&mut fifo, &mut backoff).await {
                End::Shutdown => {
                    self.component.draining();
                    if let Err(e) = self.drain(&mut fifo).await {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: false,
                            });
                        return self.finish_wal(e).await;
                    }
                    self.close().await;
                    self.component
                        .metrics()
                        .record(MetricEvent::SourceConnected {
                            source: &self.id,
                            connected: false,
                        });
                    self.component.stopped();
                    return Ok(());
                }
                End::Eof => {
                    if let Err(e) = self.settle(&mut fifo).await {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: false,
                            });
                        return self.finish_wal(e).await;
                    }
                    self.close().await;
                    self.component
                        .metrics()
                        .record(MetricEvent::SourceConnected {
                            source: &self.id,
                            connected: false,
                        });
                    self.component.stopped();
                    return Ok(());
                }
                End::Wal(e) => {
                    Self::abandon(&mut fifo);
                    self.component
                        .metrics()
                        .record(MetricEvent::SourceConnected {
                            source: &self.id,
                            connected: false,
                        });
                    return self.finish_wal(e).await;
                }
                End::Source(e) => {
                    if let Err(we) = self.settle(&mut fifo).await {
                        self.component
                            .metrics()
                            .record(MetricEvent::SourceConnected {
                                source: &self.id,
                                connected: false,
                            });
                        return self.finish_wal(we).await;
                    }
                    self.close().await;
                    self.component
                        .metrics()
                        .record(MetricEvent::SourceConnected {
                            source: &self.id,
                            connected: false,
                        });
                    if self.intake.is_triggered() {
                        self.component.stopped();
                        return Ok(());
                    }
                    if !e.is_transient() {
                        return Err(e);
                    }
                    tracing::warn!(source = %self.id, error = %e, "source failed; reopening");
                    self.component.degraded(&e);
                    add(&self.component.counters.restarts, 1);
                    self.component
                        .metrics()
                        .record(MetricEvent::SourceRestart { source: &self.id });
                    if !pause(&mut backoff, &self.intake).await {
                        self.component.stopped();
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use thadam_core::{ErrorDomain, SystemClock, WalEpoch, WalPos, codes};
    use thadam_testkit::{MemorySource, MemorySourceConfig, MemoryUpstream, SplitMix64};
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::metrics::NoopMetrics;
    use crate::status::ComponentKind;

    type Reply = oneshot::Sender<Result<WalRange, ThadamError>>;

    /// Tickets resolve whenever the test's committer decides, in any order.
    struct FakeWal {
        tx: mpsc::UnboundedSender<(u64, Reply)>,
        next: AtomicU64,
        refuse_from: Option<u64>,
    }

    struct FakeTicket(oneshot::Receiver<Result<WalRange, ThadamError>>);

    impl Future for FakeTicket {
        type Output = Result<WalRange, ThadamError>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.0)
                .poll(cx)
                .map(|r| r.unwrap_or_else(|_| Err(wal_failure("committer dropped the ticket"))))
        }
    }

    fn wal_failure(msg: &str) -> ThadamError {
        ThadamError::fatal(ErrorDomain::Buffer, codes::BUFFER_FSYNC_FAILED, msg)
    }

    impl Submit for FakeWal {
        type Ticket = FakeTicket;
        async fn submit(&self, _batch: RawBatch) -> Result<FakeTicket, SubmitError> {
            let seq = self.next.fetch_add(1, Ordering::SeqCst) + 1;
            if self.refuse_from.is_some_and(|r| seq >= r) {
                return Err(SubmitError::Wal(wal_failure("closed")));
            }
            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send((seq, tx));
            Ok(FakeTicket(rx))
        }
    }

    fn range(seq: u64) -> WalRange {
        WalRange::single(WalEpoch::new(1), WalPos::new(seq))
    }

    fn driver(
        source: MemorySource,
        refuse_from: Option<u64>,
        max_uncommitted: usize,
        max_messages: usize,
    ) -> (SourceDriver<FakeWal>, mpsc::UnboundedReceiver<(u64, Reply)>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock::new());
        let d = SourceDriver {
            id: "src".into(),
            component: Component::new(
                "source:src".into(),
                ComponentKind::Source,
                Arc::new(NoopMetrics),
                Arc::clone(&clock),
            ),
            source: Box::new(source),
            submit: FakeWal {
                tx,
                next: AtomicU64::new(0),
                refuse_from,
            },
            intake: Shutdown::new(),
            clock,
            limits: BatchLimits {
                max_messages,
                max_bytes: 1 << 20,
                linger: Duration::ZERO,
            },
            max_uncommitted,
            restart: BackoffPolicy::new(Duration::from_millis(1), Duration::from_millis(5)),
            drain_timeout: Duration::from_secs(5),
            wal_failed: Shutdown::new(),
        };
        (d, rx)
    }

    /// Resolves pending tickets in a random order, a few at a time.
    async fn random_committer(mut rx: mpsc::UnboundedReceiver<(u64, Reply)>, seed: u64) {
        let mut rng = SplitMix64::new(seed);
        let mut pending: Vec<(u64, Reply)> = Vec::new();
        loop {
            while let Ok(p) = rx.try_recv() {
                pending.push(p);
            }
            if pending.is_empty() {
                match rx.recv().await {
                    Some(p) => pending.push(p),
                    None => return,
                }
                continue;
            }
            let i = rng.below(pending.len() as u64) as usize;
            let (seq, reply) = pending.swap_remove(i);
            let _ = reply.send(Ok(range(seq)));
            if rng.chance(30) {
                tokio::time::sleep(Duration::from_micros(rng.between(10, 300))).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    fn assert_in_order(up: &MemoryUpstream, n: u64) {
        assert_eq!(up.acked_seqs(), (1..=n).collect::<Vec<_>>());
        let s = up.stats();
        assert_eq!(
            (s.out_of_order_acks, s.lost_acks, s.outstanding_handles),
            (0, 0, 0),
            "{s:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acks_stay_in_receive_order_under_random_commit_completion() {
        for seed in 0..8u64 {
            let n = 400;
            let (source, up) = MemorySource::counting(n, MemorySourceConfig::default());
            let mut rng = SplitMix64::new(seed);
            let window = rng.between(1, 16) as usize;
            let per_batch = rng.between(1, 5) as usize;
            let (d, rx) = driver(source, None, window, per_batch);
            let committer = tokio::spawn(random_committer(rx, seed));
            d.run().await.unwrap();
            assert_in_order(&up, n);
            committer.abort();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncommitted_batches_are_bounded() {
        let (source, up) = MemorySource::counting(1000, MemorySourceConfig::default());
        let (d, mut rx) = driver(source, None, 8, 1);
        let component = Arc::clone(&d.component);
        let task = tokio::spawn(d.run());
        let mut held = Vec::new();
        while held.len() < 8 {
            held.push(rx.recv().await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "a ninth batch was submitted");
        assert_eq!(up.stats().delivered, 8);
        assert_eq!(
            component
                .counters
                .uncommitted_batches
                .load(Ordering::Relaxed),
            8
        );
        assert!(up.acked_seqs().is_empty());
        let committer = tokio::spawn(random_committer(rx, 7));
        for (seq, reply) in held.into_iter().rev() {
            let _ = reply.send(Ok(range(seq)));
        }
        task.await.unwrap().unwrap();
        assert_in_order(&up, 1000);
        committer.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wal_failure_abandons_pending_acks_and_stops_the_run() {
        let (source, up) = MemorySource::counting(100, MemorySourceConfig::default());
        let (d, mut rx) = driver(source, None, 16, 1);
        let wal_failed = d.wal_failed.clone();
        let task = tokio::spawn(d.run());
        let mut got = Vec::new();
        while got.len() < 6 {
            got.push(rx.recv().await.unwrap());
        }
        let mut it = got.into_iter();
        for _ in 0..3 {
            let (seq, reply) = it.next().unwrap();
            let _ = reply.send(Ok(range(seq)));
        }
        let (_, failing) = it.next().unwrap();
        let _ = failing.send(Err(wal_failure("fsync failed")));
        let err = task.await.unwrap().unwrap_err();
        assert_eq!(err.code, codes::BUFFER_FSYNC_FAILED);
        assert!(wal_failed.is_triggered());
        assert_eq!(up.acked_seqs(), vec![1, 2, 3]);
        let s = up.stats();
        assert_eq!(
            (s.lost_acks, s.outstanding_handles, s.out_of_order_acks),
            (0, 0, 0),
            "{s:?}"
        );
        drop(it);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_transient_source_error_reopens_with_backoff() {
        let (source, up) = MemorySource::new(MemorySourceConfig::default());
        for i in 0..50 {
            up.publish(thadam_testkit::id_payload(i));
        }
        up.fail_next_batch(ThadamError::transient(
            ErrorDomain::Connector,
            codes::CONNECTOR_DISCONNECTED,
            "link down",
        ));
        let (d, rx) = driver(source, None, 4, 3);
        let component = Arc::clone(&d.component);
        let intake = d.intake.clone();
        let committer = tokio::spawn(random_committer(rx, 3));
        let task = tokio::spawn(d.run());
        while up.acked_seqs().len() < 50 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        intake.trigger();
        task.await.unwrap().unwrap();
        assert_in_order(&up, 50);
        assert_eq!(component.counters.restarts.load(Ordering::Relaxed), 1);
        committer.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_the_connector_queue_before_closing() {
        let (source, up) = MemorySource::new(MemorySourceConfig::default());
        let (d, rx) = driver(source, None, 4, 2);
        let intake = d.intake.clone();
        let committer = tokio::spawn(random_committer(rx, 11));
        let task = tokio::spawn(d.run());
        tokio::time::sleep(Duration::from_millis(10)).await;
        for i in 0..30 {
            up.publish(thadam_testkit::id_payload(i));
        }
        intake.trigger();
        task.await.unwrap().unwrap();
        assert_in_order(&up, 30);
        committer.abort();
    }
}
