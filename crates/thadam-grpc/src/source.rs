//! `GrpcSource`: a tonic ingest server whose messages become batches.
//!
//! RPC handlers push into a bounded queue without waiting; `next_batch` drains
//! it. Only the synchronous drain step mutates the queue, so dropping a
//! `next_batch` future never loses a message.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    AckMode, BatchLimits, Clock, RawBatch, ThadamError, Shutdown, Source, SourceBatch,
    SourceCapabilities, SourceContext, SourceId,
};
use tokio::task::JoinSet;

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::queue::Shared;
use crate::server;

/// How long `close` waits for committed batches to release their replies
/// before abandoning them.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Open,
    Closed,
}

/// A source fed by the gRPC ingest service.
///
/// Build with [`GrpcSource::new`] (or through the factory), then drive it with
/// the [`Source`] methods. The source reads its identity and clock from the
/// [`SourceContext`] that `open` receives.
pub struct GrpcSource {
    config: GrpcConfig,
    queue: Arc<Shared>,
    phase: Phase,
    source_id: Option<SourceId>,
    clock: Option<Arc<dyn Clock>>,
    shutdown: Option<Shutdown>,
    stop: Option<Shutdown>,
    tasks: JoinSet<()>,
    local_addr: Option<SocketAddr>,
}

impl GrpcSource {
    /// A source for `config`. Nothing is bound until `open`.
    pub fn new(config: GrpcConfig) -> Self {
        let queue = Shared::new(config.queue_capacity);
        GrpcSource {
            config,
            queue,
            phase: Phase::Created,
            source_id: None,
            clock: None,
            shutdown: None,
            stop: None,
            tasks: JoinSet::new(),
            local_addr: None,
        }
    }

    /// The address the server is bound to, once open. Useful when the
    /// configured port is zero.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.queue)
    }
}

async fn wait_until_released(queue: &Shared) {
    loop {
        let released = queue.released().notified();
        tokio::pin!(released);
        released.as_mut().enable();
        if queue.pending() == 0 {
            return;
        }
        released.await;
    }
}

#[async_trait]
impl Source for GrpcSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        if self.phase != Phase::Created {
            return Err(GrpcError::State("the gRPC source is already open or closed").into());
        }
        self.source_id = Some(ctx.source_id.clone());
        self.clock = Some(Arc::clone(&ctx.clock));
        self.shutdown = Some(ctx.shutdown.clone());
        let stop = ctx.shutdown.child();

        let max_bytes = usize::try_from(self.config.max_message_bytes).unwrap_or(usize::MAX);
        let addr = server::start(
            &self.config,
            max_bytes,
            Arc::clone(&self.queue),
            stop.clone(),
            &mut self.tasks,
        )
        .await?;
        self.local_addr = Some(addr);

        self.stop = Some(stop);
        self.phase = Phase::Open;
        Ok(())
    }

    async fn next_batch(&mut self, limits: BatchLimits) -> Result<Option<SourceBatch>, ThadamError> {
        match self.phase {
            Phase::Created => {
                return Err(GrpcError::State("next_batch called before open").into());
            }
            Phase::Closed => return Ok(None),
            Phase::Open => {}
        }
        let (Some(source_id), Some(clock), Some(shutdown)) = (
            self.source_id.clone(),
            self.clock.clone(),
            self.shutdown.clone(),
        ) else {
            return Err(GrpcError::State("next_batch called before open").into());
        };

        loop {
            let (queued, _, finished) = self.queue.snapshot();
            if queued > 0 {
                break;
            }
            if finished || shutdown.is_triggered() {
                return Ok(None);
            }
            tokio::select! {
                () = self.queue.arrived() => {}
                () = shutdown.triggered() => {}
            }
        }

        let deadline = tokio::time::Instant::now() + limits.linger;
        loop {
            let (queued, bytes, _) = self.queue.snapshot();
            if queued >= limits.max_messages || bytes >= limits.max_bytes {
                break;
            }
            tokio::select! {
                () = self.queue.arrived() => {}
                () = tokio::time::sleep_until(deadline) => break,
                () = shutdown.triggered() => break,
            }
        }

        match self.queue.take(limits, clock.now()) {
            Some((ack, messages)) => Ok(Some(SourceBatch {
                batch: RawBatch::new(source_id, messages, clock.synced()),
                ack,
            })),
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        if self.phase == Phase::Closed {
            return Ok(());
        }
        self.phase = Phase::Closed;
        if let Some(stop) = &self.stop {
            stop.trigger();
        }

        self.queue.close();
        let queue = Arc::clone(&self.queue);
        let _ = tokio::time::timeout(CLOSE_GRACE, async move {
            wait_until_released(&queue).await;
        })
        .await;
        self.queue.abandon_inflight();

        self.tasks.shutdown().await;
        Ok(())
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::new(AckMode::OnCommit)
    }
}
