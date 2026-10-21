//! `OpcUaSource`: the pull side of the session task.
//!
//! The session task pushes publish-response messages into a bounded
//! channel. `next_batch` moves messages from that channel into `pending`, a
//! buffer the source owns, so dropping a `next_batch` future never loses a
//! message.
//!
//! When the session task dies on a fatal error, `next_batch` delivers what
//! is queued, then returns the error. Transient connection failures are
//! handled inside the task (it reconnects), so they never surface here.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    AckHandle, AckMode, BatchLimits, Clock, RawBatch, RawMessage, Shutdown, Source, SourceBatch,
    SourceCapabilities, SourceContext, SourceId, ThadamError,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::OpcUaConfig;
use crate::error::OpcUaError;
use crate::session::{self, CHANNEL_CAPACITY};

/// How long `close` waits for the session task to notice the stop signal.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Open,
    Closed,
}

/// A source that subscribes to OPC UA nodes on one server.
///
/// Each publish response produces one JSON message. Build with
/// [`OpcUaSource::new`] (or through the factory), then drive it with the
/// [`Source`] methods.
pub struct OpcUaSource {
    config: OpcUaConfig,
    phase: Phase,
    source_id: Option<SourceId>,
    clock: Option<Arc<dyn Clock>>,
    shutdown: Option<Shutdown>,
    stop: Option<Shutdown>,
    rx: Option<mpsc::Receiver<RawMessage>>,
    task: Option<JoinHandle<Result<(), OpcUaError>>>,
    pending: Vec<RawMessage>,
}

impl OpcUaSource {
    /// A source for `config`. Nothing is connected until `open`.
    pub fn new(config: OpcUaConfig) -> Self {
        OpcUaSource {
            config,
            phase: Phase::Created,
            source_id: None,
            clock: None,
            shutdown: None,
            stop: None,
            rx: None,
            task: None,
            pending: Vec::new(),
        }
    }

    fn state_error(message: &'static str) -> ThadamError {
        OpcUaError::State(message).into()
    }

    /// The queue is empty and the task ended: shutdown, or a fatal failure
    /// the runtime should see.
    async fn task_ended(&mut self) -> Result<Option<SourceBatch>, ThadamError> {
        if self.shutdown.as_ref().is_some_and(Shutdown::is_triggered) {
            return Ok(None);
        }
        let Some(task) = self.task.as_mut() else {
            return Ok(None);
        };
        let result = task.await;
        self.task = None;
        match result {
            Ok(Ok(())) => Ok(None),
            Ok(Err(error)) => Err(error.into()),
            Err(join) => Err(OpcUaError::Transport(format!("session task failed: {join}")).into()),
        }
    }
}

#[async_trait]
impl Source for OpcUaSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        if self.phase == Phase::Open {
            return Err(Self::state_error("the OPC UA source is already open"));
        }
        if self.config.sends_credentials_unsecured() {
            tracing::warn!(
                endpoint = %self.config.endpoint,
                "OPC UA username/password authentication is configured with security_policy: none; the password is not encrypted"
            );
        }
        if self.config.accepts_unknown_server_cert() {
            tracing::warn!(
                endpoint = %self.config.endpoint,
                "accept_unknown_server_cert: true disables OPC UA server authentication"
            );
        }
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let stop = ctx.shutdown.child();
        let task = tokio::spawn(session::run(
            self.config.clone(),
            Arc::clone(&ctx.clock),
            tx,
            stop.clone(),
        ));

        self.pending.clear();
        self.source_id = Some(ctx.source_id.clone());
        self.clock = Some(Arc::clone(&ctx.clock));
        self.shutdown = Some(ctx.shutdown.clone());
        self.stop = Some(stop);
        self.rx = Some(rx);
        self.task = Some(task);
        self.phase = Phase::Open;
        Ok(())
    }

    async fn next_batch(
        &mut self,
        limits: BatchLimits,
    ) -> Result<Option<SourceBatch>, ThadamError> {
        match self.phase {
            Phase::Created => return Err(Self::state_error("next_batch called before open")),
            Phase::Closed => return Ok(None),
            Phase::Open => {}
        }
        let (Some(source_id), Some(clock), Some(shutdown), Some(rx)) = (
            self.source_id.clone(),
            self.clock.clone(),
            self.shutdown.clone(),
            self.rx.as_mut(),
        ) else {
            return Err(Self::state_error("next_batch called before open"));
        };

        let max_messages = limits.max_messages.max(1);
        if self.pending.is_empty() {
            let received = tokio::select! {
                n = rx.recv_many(&mut self.pending, max_messages) => n,
                () = shutdown.triggered() => 0,
            };
            if received == 0 {
                return self.task_ended().await;
            }
        }

        let deadline = tokio::time::Instant::now() + limits.linger;
        while self.pending.len() < max_messages && pending_bytes(&self.pending) < limits.max_bytes {
            let room = max_messages - self.pending.len();
            tokio::select! {
                n = rx.recv_many(&mut self.pending, room) => {
                    if n == 0 {
                        break;
                    }
                }
                () = tokio::time::sleep_until(deadline) => break,
                () = shutdown.triggered() => break,
            }
        }

        let take = batch_len(&self.pending, limits);
        let messages: Vec<RawMessage> = self.pending.drain(..take).collect();
        Ok(Some(SourceBatch {
            batch: RawBatch::new(source_id, messages, clock.synced()),
            ack: AckHandle::noop(),
        }))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        if self.phase == Phase::Closed {
            return Ok(());
        }
        self.phase = Phase::Closed;
        if let Some(stop) = &self.stop {
            stop.trigger();
        }
        self.rx = None;
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(CLOSE_GRACE, &mut task).await.is_err()
        {
            task.abort();
        }
        self.pending.clear();
        Ok(())
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::new(AckMode::None)
    }
}

fn pending_bytes(messages: &[RawMessage]) -> usize {
    messages.iter().map(|m| m.payload.len()).sum()
}

/// How many leading messages fit the limits. Always at least one, so an
/// oversized message is still delivered on its own.
fn batch_len(messages: &[RawMessage], limits: BatchLimits) -> usize {
    let mut bytes = 0usize;
    let mut count = 0usize;
    for message in messages.iter().take(limits.max_messages.max(1)) {
        let next = bytes.saturating_add(message.payload.len());
        if count > 0 && next > limits.max_bytes {
            break;
        }
        bytes = next;
        count += 1;
    }
    count
}
