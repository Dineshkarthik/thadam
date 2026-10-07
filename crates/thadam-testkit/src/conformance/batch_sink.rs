//! Conformance suite for [`BatchSink`] implementations (ADR-0002).
//!
//! The runtime retries the same [`SinkBatch`] after a transient error, so a
//! sink has to tolerate seeing a batch id twice, report its effective delivery
//! class honestly, and classify failures so the runtime retries only what is
//! worth retrying.

use std::path::PathBuf;
use std::sync::Arc;

use thadam_core::{
    BatchId, BatchSink, DeliveryClass, DiskBudget, ErrorDomain, Retry, ThadamError, Shutdown,
    SinkBatch, SinkContext, SinkId,
};

use crate::FakeClock;
use crate::fixtures::test_batch;
use crate::memory_batch_sink::{
    Fault, MemoryBatchSink, MemoryBatchSinkConfig, MemoryBatchSinkHandle,
};

/// A failure the suite asks the sink's test double to produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InjectedFault {
    /// The next write fails with a transient error and applies nothing.
    Transient,
    /// The next write fails with a permanent error.
    Permanent,
    /// The next write fails with a fatal error.
    Fatal,
    /// The next write applies the batch but reports a transient error, as if
    /// the response was lost.
    UnknownOutcome,
}

/// Observes the target a sink under test writes to.
pub trait BatchSinkProbe: Send + Sync {
    /// Events visible at the target, duplicates included.
    fn applied_events(&self) -> usize;
    /// The id of every write attempt, in order.
    fn attempt_ids(&self) -> Vec<BatchId>;
    /// Arms a failure for an upcoming write. Returns `false` if the target
    /// cannot produce it, in which case the checks that need it are skipped.
    fn inject(&self, fault: InjectedFault) -> bool;
}

/// A sink and its probe.
pub struct BatchSinkCase {
    /// The sink under test.
    pub sink: Box<dyn BatchSink>,
    /// How to observe and steer its target.
    pub probe: Box<dyn BatchSinkProbe>,
}

/// Builds fresh sinks for the suite.
pub trait BatchSinkFixture: Send + Sync {
    /// A fresh sink and probe.
    fn create(&self) -> BatchSinkCase;
}

impl BatchSinkProbe for MemoryBatchSinkHandle {
    fn applied_events(&self) -> usize {
        MemoryBatchSinkHandle::applied_events(self)
    }
    fn attempt_ids(&self) -> Vec<BatchId> {
        MemoryBatchSinkHandle::attempt_ids(self)
    }
    fn inject(&self, fault: InjectedFault) -> bool {
        MemoryBatchSinkHandle::inject(
            self,
            match fault {
                InjectedFault::Transient => Fault::Fail(Retry::Transient),
                InjectedFault::Permanent => Fault::Fail(Retry::Permanent),
                InjectedFault::Fatal => Fault::Fail(Retry::Fatal),
                InjectedFault::UnknownOutcome => Fault::AppliedThenTransient,
            },
        );
        true
    }
}

/// Runs the suite against [`MemoryBatchSink`] variants.
#[derive(Clone, Debug, Default)]
pub struct MemoryBatchSinkFixture {
    /// The behaviour of the sinks it builds.
    pub config: MemoryBatchSinkConfig,
}

impl BatchSinkFixture for MemoryBatchSinkFixture {
    fn create(&self) -> BatchSinkCase {
        let (sink, handle) = MemoryBatchSink::new(self.config.clone());
        BatchSinkCase {
            sink: Box::new(sink),
            probe: Box::new(handle),
        }
    }
}

fn context() -> SinkContext {
    SinkContext::new(
        SinkId::new("conformance"),
        Shutdown::new(),
        FakeClock::default().shared(),
        PathBuf::new(),
        Arc::new(DiskBudget::new()),
    )
}

async fn opened(fixture: &dyn BatchSinkFixture) -> (BatchSinkCase, DeliveryClass) {
    let mut case = fixture.create();
    let caps = case
        .sink
        .open(&context())
        .await
        .unwrap_or_else(|e| panic!("open failed: {e}"));
    (case, caps.delivery)
}

fn failure<T>(result: Result<T, ThadamError>, what: &str) -> ThadamError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("{what}"),
    }
}

fn batch(first: u64, last: u64) -> SinkBatch {
    SinkBatch::new(test_batch(1, first, last))
}

fn assert_duplicates_allowed(delivery: DeliveryClass, applied: usize, once: usize, why: &str) {
    match delivery {
        DeliveryClass::IdempotentBatch => {
            assert_eq!(
                applied, once,
                "an idempotent sink applied a batch twice: {why}"
            );
        }
        _ => assert!(
            applied == once || applied == 2 * once,
            "an at-least-once sink must apply a batch once or twice, not {applied} of {once}: {why}"
        ),
    }
}

/// `open` succeeds and the sink can be closed again.
pub async fn open_and_close(fixture: &dyn BatchSinkFixture) {
    let (mut case, _) = opened(fixture).await;
    case.sink
        .close()
        .await
        .unwrap_or_else(|e| panic!("close failed: {e}"));
}

/// Writing the same batch id twice succeeds both times, and the reported
/// delivery class tells how many copies reach the target.
pub async fn repeated_batch_id_is_tolerated(fixture: &dyn BatchSinkFixture) {
    let (mut case, delivery) = opened(fixture).await;
    let b = batch(1, 4);
    for attempt in 1..=2 {
        case.sink
            .write(&b)
            .await
            .unwrap_or_else(|e| panic!("write attempt {attempt} of the same batch failed: {e}"));
    }
    assert_duplicates_allowed(delivery, case.probe.applied_events(), 4, "plain repeat");
    assert_eq!(case.probe.attempt_ids(), vec![b.id(), b.id()]);
}

/// A transient failure is classified as such, and retrying the same
/// [`SinkBatch`] succeeds under the same id.
pub async fn transient_failure_retries_the_same_batch_id(fixture: &dyn BatchSinkFixture) {
    let (mut case, delivery) = opened(fixture).await;
    if !case.probe.inject(InjectedFault::Transient) {
        return;
    }
    let b = batch(5, 6);
    let err = failure(
        case.sink.write(&b).await,
        "an injected transient failure must surface as an error",
    );
    assert_eq!(
        err.retry,
        Retry::Transient,
        "transient failure misclassified: {err}"
    );
    assert_eq!(err.domain, ErrorDomain::Sink);
    case.sink
        .write(&b)
        .await
        .unwrap_or_else(|e| panic!("the retry failed: {e}"));
    assert_eq!(
        case.probe.attempt_ids(),
        vec![b.id(), b.id()],
        "the retry changed the batch id"
    );
    assert_duplicates_allowed(
        delivery,
        case.probe.applied_events(),
        2,
        "retry after transient",
    );
}

/// A write whose response was lost (applied, then a transient error) is safe
/// to retry: an idempotent sink must not end up with two copies.
pub async fn unknown_outcome_retry_is_safe(fixture: &dyn BatchSinkFixture) {
    let (mut case, delivery) = opened(fixture).await;
    if !case.probe.inject(InjectedFault::UnknownOutcome) {
        return;
    }
    let b = batch(7, 9);
    let err = failure(
        case.sink.write(&b).await,
        "a lost response must surface as an error",
    );
    assert_eq!(
        err.retry,
        Retry::Transient,
        "an unknown outcome must be retryable: {err}"
    );
    case.sink
        .write(&b)
        .await
        .unwrap_or_else(|e| panic!("the retry failed: {e}"));
    assert_duplicates_allowed(
        delivery,
        case.probe.applied_events(),
        3,
        "retry after unknown outcome",
    );
}

/// Permanent and fatal failures keep their retry class.
pub async fn error_classes_are_reported(fixture: &dyn BatchSinkFixture) {
    for (fault, expected) in [
        (InjectedFault::Permanent, Retry::Permanent),
        (InjectedFault::Fatal, Retry::Fatal),
    ] {
        let (mut case, _) = opened(fixture).await;
        if !case.probe.inject(fault) {
            continue;
        }
        let err = failure(
            case.sink.write(&batch(1, 1)).await,
            "an injected failure must surface as an error",
        );
        assert_eq!(err.retry, expected, "{fault:?} misclassified: {err}");
    }
}

/// Batches with different ids are independent: all of them are applied.
pub async fn distinct_batches_accumulate(fixture: &dyn BatchSinkFixture) {
    let (mut case, _) = opened(fixture).await;
    let (a, b) = (batch(1, 2), batch(3, 6));
    assert_ne!(a.id(), b.id());
    for batch in [&a, &b] {
        case.sink
            .write(batch)
            .await
            .unwrap_or_else(|e| panic!("write failed: {e}"));
    }
    assert_eq!(case.probe.applied_events(), 6);
}

/// Runs every check against `fixture`.
pub async fn run_all(fixture: &dyn BatchSinkFixture) {
    open_and_close(fixture).await;
    repeated_batch_id_is_tolerated(fixture).await;
    transient_failure_retries_the_same_batch_id(fixture).await;
    unknown_outcome_retry_is_safe(fixture).await;
    error_classes_are_reported(fixture).await;
    distinct_batches_accumulate(fixture).await;
}
