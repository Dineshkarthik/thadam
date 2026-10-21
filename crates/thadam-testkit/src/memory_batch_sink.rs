//! An in-memory [`BatchSink`] with fault injection.
//!
//! The sink records every attempt and every batch it applied, models the two
//! delivery classes (an idempotent sink applies a repeated [`BatchId`] once; an
//! at-least-once sink applies it again), and fails on demand with transient,
//! permanent, or fatal errors or with an unknown outcome.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    BatchId, BatchSink, DeliveryClass, ErrorDomain, Event, Reject, Retry, SinkBatch,
    SinkCapabilities, SinkContext, ThadamError, WriteOutcome, codes,
};

use crate::util::lock;

/// A failure to inject into an upcoming `write` call. Faults are consumed one
/// per call, in the order they were injected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Fail without applying the batch, with an error of this retry class.
    Fail(Retry),
    /// Apply the batch and then report a transient error, like an HTTP request
    /// whose response was lost: the outcome is unknown to the caller.
    AppliedThenTransient,
    /// Wait, then succeed normally.
    Delay(Duration),
}

type RejectFn = dyn Fn(&Event) -> Option<ThadamError> + Send + Sync;

/// Behaviour of a [`MemoryBatchSink`].
#[derive(Clone, Debug)]
pub struct MemoryBatchSinkConfig {
    /// The delivery class `open` reports and the sink honours.
    pub delivery: DeliveryClass,
    /// Time every `write` takes.
    pub latency: Duration,
    /// Fails `open` with this error.
    pub open_error: Option<ThadamError>,
}

impl Default for MemoryBatchSinkConfig {
    fn default() -> Self {
        MemoryBatchSinkConfig {
            delivery: DeliveryClass::IdempotentBatch,
            latency: Duration::ZERO,
            open_error: None,
        }
    }
}

#[derive(Default)]
struct State {
    opened: bool,
    closed: bool,
    applied: Vec<(BatchId, usize)>,
    attempts: Vec<BatchId>,
    faults: VecDeque<Fault>,
    reject: Option<Arc<RejectFn>>,
}

struct Shared {
    cfg: MemoryBatchSinkConfig,
    state: Mutex<State>,
}

/// The sink half.
pub struct MemoryBatchSink {
    shared: Arc<Shared>,
}

/// Inspects and steers a [`MemoryBatchSink`] from the test.
#[derive(Clone)]
pub struct MemoryBatchSinkHandle {
    shared: Arc<Shared>,
}

impl MemoryBatchSink {
    /// A sink and its inspection handle.
    pub fn new(cfg: MemoryBatchSinkConfig) -> (MemoryBatchSink, MemoryBatchSinkHandle) {
        let shared = Arc::new(Shared {
            cfg,
            state: Mutex::new(State::default()),
        });
        (
            MemoryBatchSink {
                shared: Arc::clone(&shared),
            },
            MemoryBatchSinkHandle { shared },
        )
    }
}

impl MemoryBatchSinkHandle {
    /// Fails an upcoming write as described by `fault`.
    pub fn inject(&self, fault: Fault) {
        lock(&self.shared.state).faults.push_back(fault);
    }

    /// Makes the sink reject, per event, those for which `f` returns an error.
    pub fn reject_events_where<F>(&self, f: F)
    where
        F: Fn(&Event) -> Option<ThadamError> + Send + Sync + 'static,
    {
        lock(&self.shared.state).reject = Some(Arc::new(f));
    }

    /// Events visible at the target, duplicates from repeated applications
    /// included, rejected events excluded.
    pub fn applied_events(&self) -> usize {
        lock(&self.shared.state)
            .applied
            .iter()
            .map(|(_, n)| n)
            .sum()
    }

    /// Each application as (batch id, events stored).
    pub fn applied_batches(&self) -> Vec<(BatchId, usize)> {
        lock(&self.shared.state).applied.clone()
    }

    /// The id of every `write` call, in order, including failed ones.
    pub fn attempt_ids(&self) -> Vec<BatchId> {
        lock(&self.shared.state).attempts.clone()
    }

    /// Whether `open` succeeded.
    pub fn is_open(&self) -> bool {
        lock(&self.shared.state).opened
    }

    /// Whether `close` was called.
    pub fn is_closed(&self) -> bool {
        lock(&self.shared.state).closed
    }
}

fn fault_error(retry: Retry) -> ThadamError {
    match retry {
        Retry::Transient => ThadamError::transient(
            ErrorDomain::Sink,
            codes::SINK_UNAVAILABLE,
            "injected transient failure",
        ),
        Retry::Fatal => ThadamError::fatal(
            ErrorDomain::Sink,
            codes::SINK_WRITE_FAILED,
            "injected fatal failure",
        ),
        _ => ThadamError::permanent(
            ErrorDomain::Sink,
            codes::SINK_WRITE_FAILED,
            "injected permanent failure",
        ),
    }
}

impl MemoryBatchSink {
    fn apply(&self, batch: &SinkBatch) -> Vec<Reject> {
        let mut st = lock(&self.shared.state);
        let rejects: Vec<Reject> = match &st.reject {
            Some(f) => batch
                .events()
                .iter()
                .enumerate()
                .filter_map(|(i, e)| f(e).map(|err| Reject::new(i as u32, err)))
                .collect(),
            None => Vec::new(),
        };
        let already = st.applied.iter().any(|(id, _)| *id == batch.id());
        if !(already && self.shared.cfg.delivery == DeliveryClass::IdempotentBatch) {
            st.applied
                .push((batch.id(), batch.events().len() - rejects.len()));
        }
        rejects
    }
}

#[async_trait]
impl BatchSink for MemoryBatchSink {
    async fn open(&mut self, _ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        if let Some(err) = &self.shared.cfg.open_error {
            return Err(err.clone());
        }
        lock(&self.shared.state).opened = true;
        Ok(SinkCapabilities::new(self.shared.cfg.delivery))
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        let fault = {
            let mut st = lock(&self.shared.state);
            if !st.opened {
                return Err(ThadamError::permanent(
                    ErrorDomain::Sink,
                    codes::SINK_WRITE_FAILED,
                    "write called before open",
                ));
            }
            st.attempts.push(batch.id());
            st.faults.pop_front()
        };
        if !self.shared.cfg.latency.is_zero() {
            tokio::time::sleep(self.shared.cfg.latency).await;
        }
        match fault {
            Some(Fault::Fail(retry)) => return Err(fault_error(retry)),
            Some(Fault::AppliedThenTransient) => {
                self.apply(batch);
                return Err(ThadamError::transient(
                    ErrorDomain::Sink,
                    codes::SINK_TIMEOUT,
                    "response lost after the batch was applied",
                ));
            }
            Some(Fault::Delay(d)) => tokio::time::sleep(d).await,
            None => {}
        }
        Ok(WriteOutcome::with_rejects(self.apply(batch)))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        lock(&self.shared.state).closed = true;
        Ok(())
    }
}
