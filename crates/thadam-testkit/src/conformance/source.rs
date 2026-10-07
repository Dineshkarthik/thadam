//! Conformance suite for [`Source`] implementations (ADR-0002).
//!
//! A connector plugs in by implementing [`SourceFixture`] (how to build a
//! source and its upstream) and [`Upstream`] (how to feed and observe the
//! broker). Each function checks one clause of the contract and panics with a
//! description of the violation. They sleep on virtual time where the clause
//! needs it (liveness stalls for three keepalives), so run them under
//! `#[tokio::test(start_paused = true)]`.

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use thadam_core::{
    AckOutcome, AckStatus, BatchLimits, EpochGate, RawBatch, Shutdown, Source, SourceBatch,
    SourceContext, SourceId,
};
use tokio::time::Instant;

use super::ack_fifo::AckFifo;
use crate::FakeClock;
use crate::fixtures::{id_payload, payload_id};
use crate::memory_source::{MemorySource, MemorySourceConfig, MemoryUpstream};
use crate::util::{SplitMix64, poll_once, poll_up_to};

/// The broker side of a source under test.
pub trait Upstream: Send + Sync {
    /// Delivers a message to the source, as the broker would.
    fn publish(&self, payload: Bytes);
    /// Ends the stream.
    fn finish(&self);
    /// Drops the session. The broker redelivers unacknowledged messages and
    /// the connector must start a new connection epoch.
    fn lose_session(&self);
    /// Payloads the broker has seen acknowledged, in the order acknowledgements
    /// arrived.
    fn acked_payloads(&self) -> Vec<Bytes>;
    /// The protocol keepalive interval.
    fn keepalive(&self) -> Duration;
    /// Times the broker would have dropped the connection for missing pings.
    fn keepalive_violations(&self) -> u64;
    /// The connector's epoch gate, if it exposes one, for counters.
    fn epoch_gate(&self) -> Option<EpochGate>;
}

/// A source and its upstream.
pub struct Case {
    /// The source under test.
    pub source: Box<dyn Source>,
    /// The broker feeding it.
    pub upstream: Box<dyn Upstream>,
}

/// Builds fresh sources for the suite.
pub trait SourceFixture: Send + Sync {
    /// A fresh source and upstream.
    fn create(&self) -> Case;

    /// Like [`SourceFixture::create`], but `open` takes `delay` to connect.
    /// Fixtures that cannot simulate a slow connect return `None` and the
    /// connect-cancellation check is skipped.
    fn create_slow_connect(&self, _delay: Duration) -> Option<Case> {
        None
    }
}

impl Upstream for MemoryUpstream {
    fn publish(&self, payload: Bytes) {
        MemoryUpstream::publish(self, payload);
    }
    fn finish(&self) {
        MemoryUpstream::finish(self);
    }
    fn lose_session(&self) {
        MemoryUpstream::lose_session(self);
    }
    fn acked_payloads(&self) -> Vec<Bytes> {
        MemoryUpstream::acked_payloads(self)
    }
    fn keepalive(&self) -> Duration {
        MemoryUpstream::keepalive(self)
    }
    fn keepalive_violations(&self) -> u64 {
        self.stats().keepalive_violations
    }
    fn epoch_gate(&self) -> Option<EpochGate> {
        Some(self.gate())
    }
}

/// Runs the suite against [`MemorySource`] variants.
#[derive(Clone, Debug, Default)]
pub struct MemorySourceFixture {
    /// The behaviour of the sources it builds.
    pub config: MemorySourceConfig,
}

impl SourceFixture for MemorySourceFixture {
    fn create(&self) -> Case {
        let (source, upstream) = MemorySource::new(self.config.clone());
        Case {
            source: Box::new(source),
            upstream: Box::new(upstream),
        }
    }

    fn create_slow_connect(&self, delay: Duration) -> Option<Case> {
        let config = MemorySourceConfig {
            open_delay: delay,
            ..self.config.clone()
        };
        let (source, upstream) = MemorySource::new(config);
        Some(Case {
            source: Box::new(source),
            upstream: Box::new(upstream),
        })
    }
}

fn context(shutdown: &Shutdown) -> SourceContext {
    SourceContext::new(
        SourceId::new("conformance"),
        shutdown.clone(),
        FakeClock::default().shared(),
    )
}

async fn open(source: &mut dyn Source, shutdown: &Shutdown) {
    if let Err(e) = source.open(&context(shutdown)).await {
        panic!("open failed: {e}");
    }
}

fn ids(batch: &RawBatch) -> Vec<u64> {
    batch
        .messages
        .iter()
        .map(|m| payload_id(&m.payload).unwrap_or_else(|| panic!("message without a test id")))
        .collect()
}

fn acked_ids(upstream: &dyn Upstream) -> Vec<u64> {
    upstream
        .acked_payloads()
        .iter()
        .map(|p| payload_id(p).unwrap_or_else(|| panic!("acked a payload without a test id")))
        .collect()
}

fn publish_ids(upstream: &dyn Upstream, range: std::ops::Range<u64>) {
    for id in range {
        upstream.publish(id_payload(id));
    }
}

async fn pull(source: &mut dyn Source, limits: BatchLimits) -> Option<SourceBatch> {
    match source.next_batch(limits).await {
        Ok(batch) => batch,
        Err(e) => panic!("next_batch failed: {e}"),
    }
}

fn no_linger(max_messages: usize) -> BatchLimits {
    BatchLimits {
        max_messages,
        linger: Duration::ZERO,
        ..BatchLimits::default()
    }
}

/// **Ordered acknowledgement.** Batches are committed in a random order but
/// acknowledged strictly in receive order: the broker sees every message
/// acknowledged exactly once, in sequence, with no loss and no stale or lost
/// handles.
pub async fn ordered_ack(fixture: &dyn SourceFixture, seed: u64) {
    const TOTAL: u64 = 240;
    let Case {
        mut source,
        upstream,
    } = fixture.create();
    let shutdown = Shutdown::new();
    open(source.as_mut(), &shutdown).await;
    publish_ids(upstream.as_ref(), 0..TOTAL);
    upstream.finish();

    let mut rng = SplitMix64::new(seed);
    let mut fifo = AckFifo::new();
    let mut uncommitted = Vec::new();
    let mut pulled = Vec::new();
    while let Some(batch) = pull(source.as_mut(), no_linger(rng.between(1, 8) as usize)).await {
        pulled.extend(ids(&batch.batch));
        uncommitted.push(fifo.push(batch.ack));
        while !uncommitted.is_empty() && rng.chance(50) {
            let pick = rng.below(uncommitted.len() as u64) as usize;
            fifo.commit(uncommitted.swap_remove(pick));
        }
        for status in fifo.release_ready() {
            assert_eq!(
                status,
                AckStatus::Delivered,
                "a live-epoch ack was not delivered"
            );
        }
    }
    while !uncommitted.is_empty() {
        let pick = rng.below(uncommitted.len() as u64) as usize;
        fifo.commit(uncommitted.swap_remove(pick));
    }
    fifo.release_ready();

    let expected: Vec<u64> = (0..TOTAL).collect();
    assert_eq!(
        pulled, expected,
        "messages were not delivered once each, in order"
    );
    assert_eq!(
        acked_ids(upstream.as_ref()),
        expected,
        "acknowledgements did not reach the broker once each, in receive order"
    );
    assert!(fifo.is_empty());
    if let Some(gate) = upstream.epoch_gate() {
        assert_eq!(gate.outstanding(), 0, "ack handles left outstanding");
        assert_eq!(gate.lost_acks(), 0, "ack handles were lost");
        assert_eq!(gate.stale_acks(), 0, "a live-epoch ack counted as stale");
    }
    assert_eq!(upstream.keepalive_violations(), 0);
}

/// **Cancel-safe `next_batch`.** The future is dropped at random points (after
/// one poll, after a few polls, on a random timeout) while messages keep
/// arriving; every message must still be delivered exactly once, in order.
pub async fn cancel_safety(fixture: &dyn SourceFixture, seed: u64) {
    const TOTAL: u64 = 300;
    let Case {
        mut source,
        upstream,
    } = fixture.create();
    let shutdown = Shutdown::new();
    open(source.as_mut(), &shutdown).await;

    let mut rng = SplitMix64::new(seed);
    let mut published = 0u64;
    let mut got: Vec<u64> = Vec::new();
    let mut dropped_futures = 0u32;
    loop {
        if published < TOTAL && rng.chance(70) {
            let n = rng.between(1, 12).min(TOTAL - published);
            publish_ids(upstream.as_ref(), published..published + n);
            published += n;
            if published == TOTAL {
                upstream.finish();
            }
        }
        let limits = BatchLimits {
            max_messages: rng.between(1, 16) as usize,
            linger: Duration::from_micros(rng.between(0, 800)),
            ..BatchLimits::default()
        };
        let outcome = match rng.below(3) {
            0 => poll_once(source.next_batch(limits)).await,
            1 => {
                let budget = Duration::from_micros(rng.between(1, 500));
                tokio::time::timeout(budget, source.next_batch(limits))
                    .await
                    .ok()
            }
            _ => poll_up_to(source.next_batch(limits), rng.between(1, 6) as u32).await,
        };
        match outcome {
            None => dropped_futures += 1,
            Some(Ok(Some(batch))) => {
                got.extend(ids(&batch.batch));
                batch.ack.complete(AckOutcome::Committed);
            }
            Some(Ok(None)) => break,
            Some(Err(e)) => panic!("next_batch failed: {e}"),
        }
    }

    let expected: Vec<u64> = (0..TOTAL).collect();
    assert!(
        dropped_futures >= 10,
        "the probe only dropped {dropped_futures} futures, too few to mean anything"
    );
    assert_eq!(
        got, expected,
        "messages were lost or duplicated after dropping next_batch futures ({dropped_futures} drops)"
    );
    assert_eq!(acked_ids(upstream.as_ref()), expected);
}

/// **Liveness.** With the consumer stalled for three keepalive intervals, the
/// connection stays up (no missed pings) and no message is lost once the
/// consumer resumes.
pub async fn liveness(fixture: &dyn SourceFixture) {
    const TOTAL: u64 = 20;
    let Case {
        mut source,
        upstream,
    } = fixture.create();
    let shutdown = Shutdown::new();
    open(source.as_mut(), &shutdown).await;
    publish_ids(upstream.as_ref(), 0..TOTAL);

    tokio::time::sleep(upstream.keepalive() * 3).await;
    assert_eq!(
        upstream.keepalive_violations(),
        0,
        "the connection missed keepalive pings while the consumer was stalled"
    );

    let mut got = Vec::new();
    while (got.len() as u64) < TOTAL {
        let limits = no_linger(8);
        let pulled =
            tokio::time::timeout(upstream.keepalive(), pull(source.as_mut(), limits)).await;
        let Ok(Some(batch)) = pulled else {
            panic!(
                "messages published during the stall never arrived ({} of {TOTAL})",
                got.len()
            );
        };
        got.extend(ids(&batch.batch));
        batch.ack.complete(AckOutcome::Committed);
    }
    assert_eq!(got, (0..TOTAL).collect::<Vec<_>>());
    assert_eq!(acked_ids(upstream.as_ref()), got);
    assert_eq!(upstream.keepalive_violations(), 0);
}

/// **Epoch-scoped handles** (the S2 E1 scenario). Handles issued before a
/// session loss must be inert afterwards: completing them acknowledges
/// nothing, is reported as stale, and no message is lost.
pub async fn epoch_scoped_handles(fixture: &dyn SourceFixture) {
    const TOTAL: u64 = 30;
    let Case {
        mut source,
        upstream,
    } = fixture.create();
    let shutdown = Shutdown::new();
    open(source.as_mut(), &shutdown).await;
    publish_ids(upstream.as_ref(), 0..TOTAL);

    let mut old_handles = Vec::new();
    for _ in 0..3 {
        let batch = pull(source.as_mut(), no_linger(5)).await;
        old_handles.push(
            batch
                .unwrap_or_else(|| panic!("no batch before the session loss"))
                .ack,
        );
    }
    upstream.lose_session();

    let stale = old_handles.len() as u64;
    for handle in old_handles {
        assert_eq!(
            handle.complete(AckOutcome::Committed),
            AckStatus::Stale,
            "a handle from before the session loss was treated as live"
        );
    }
    assert!(
        upstream.acked_payloads().is_empty(),
        "a stale acknowledgement reached the broker and acknowledged a message"
    );
    if let Some(gate) = upstream.epoch_gate() {
        assert_eq!(
            gate.stale_acks(),
            stale,
            "stale acknowledgements were not counted"
        );
    }

    upstream.finish();
    while let Some(batch) = pull(source.as_mut(), no_linger(7)).await {
        batch.ack.complete(AckOutcome::Committed);
    }
    let acked: HashSet<u64> = acked_ids(upstream.as_ref()).into_iter().collect();
    let expected: HashSet<u64> = (0..TOTAL).collect();
    assert_eq!(
        acked, expected,
        "messages were lost across the session loss"
    );
}

/// **Cooperative cancellation.** Shutdown during connect, during receive, and
/// during ack ends each operation promptly and leaves the source closable.
pub async fn cooperative_cancellation(fixture: &dyn SourceFixture) {
    let connect_delay = Duration::from_secs(30);
    if let Some(Case { mut source, .. }) = fixture.create_slow_connect(connect_delay) {
        let shutdown = Shutdown::new();
        let ctx = context(&shutdown);
        let started = Instant::now();
        let trigger = shutdown.clone();
        let canceller = async move {
            tokio::time::sleep(connect_delay / 100).await;
            trigger.trigger();
        };
        let (opened, ()) = tokio::join!(source.open(&ctx), canceller);
        assert!(
            started.elapsed() < connect_delay / 2,
            "open ignored shutdown and kept connecting"
        );
        assert!(
            opened.is_err(),
            "an open cancelled by shutdown must not claim success"
        );
        source
            .close()
            .await
            .unwrap_or_else(|e| panic!("close after a cancelled open failed: {e}"));
    }

    {
        let Case { mut source, .. } = fixture.create();
        let shutdown = Shutdown::new();
        open(source.as_mut(), &shutdown).await;
        let started = Instant::now();
        let trigger = shutdown.clone();
        let canceller = async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            trigger.trigger();
        };
        let limits = BatchLimits {
            linger: Duration::from_secs(60),
            ..BatchLimits::default()
        };
        let (received, ()) = tokio::join!(source.next_batch(limits), canceller);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "next_batch ignored shutdown while idle"
        );
        match received {
            Ok(None) | Err(_) => {}
            Ok(Some(_)) => panic!("an empty source returned a batch"),
        }
        source
            .close()
            .await
            .unwrap_or_else(|e| panic!("close after shutdown failed: {e}"));
    }

    {
        let Case {
            mut source,
            upstream,
        } = fixture.create();
        let shutdown = Shutdown::new();
        open(source.as_mut(), &shutdown).await;
        publish_ids(upstream.as_ref(), 0..3);
        let batch = pull(source.as_mut(), no_linger(8)).await;
        let batch = batch.unwrap_or_else(|| panic!("no batch to acknowledge"));
        shutdown.trigger();
        let status = batch.ack.complete(AckOutcome::Committed);
        assert!(matches!(status, AckStatus::Delivered | AckStatus::Stale));
        source
            .close()
            .await
            .unwrap_or_else(|e| panic!("close after shutdown failed: {e}"));
    }
}

/// **Lost-token detection.** A handle dropped without completion is counted,
/// and its message stays unacknowledged at the broker so it is redelivered.
pub async fn lost_token_detection(fixture: &dyn SourceFixture) {
    let Case {
        mut source,
        upstream,
    } = fixture.create();
    let shutdown = Shutdown::new();
    open(source.as_mut(), &shutdown).await;
    publish_ids(upstream.as_ref(), 0..2);

    let batch = pull(source.as_mut(), no_linger(8)).await;
    let batch = batch.unwrap_or_else(|| panic!("no batch"));
    drop(batch.ack);

    assert!(
        upstream.acked_payloads().is_empty(),
        "a dropped handle acknowledged its messages"
    );
    if let Some(gate) = upstream.epoch_gate() {
        assert_eq!(gate.lost_acks(), 1, "the lost token was not counted");
        assert_eq!(gate.outstanding(), 0);
    }
}

/// Runs every clause against `fixture`.
pub async fn run_all(fixture: &dyn SourceFixture, seed: u64) {
    ordered_ack(fixture, seed).await;
    cancel_safety(fixture, seed).await;
    liveness(fixture).await;
    epoch_scoped_handles(fixture).await;
    cooperative_cancellation(fixture).await;
    lost_token_detection(fixture).await;
}
