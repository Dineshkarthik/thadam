//! `CanSource`: the pull side of the receive task.
//!
//! The receive task pushes messages into a bounded channel, dropping and
//! counting frames when it is full. `next_batch` moves messages from that
//! channel into `pending`, a buffer the source owns, so dropping a
//! `next_batch` future never loses a message.
//!
//! When the receive task dies on a transport error, `next_batch` delivers
//! what is queued, then returns the error. The runtime closes the source and
//! opens it again with backoff, so a source can be reopened after `close`.

use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    AckHandle, AckMode, BatchLimits, Clock, RawBatch, RawMessage, Shutdown, Source, SourceBatch,
    SourceCapabilities, SourceContext, SourceId, ThadamError,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::{CanConfig, Emitter};
use crate::dbc::Decoder;
use crate::error::CanError;
use crate::frame::FrameSource;
use crate::recv;

/// Messages the receive task may run ahead of `next_batch`. Frames beyond
/// this queue are dropped and counted as `can.dropped`.
const CHANNEL_CAPACITY: usize = 4096;

/// How long `close` waits for the receive task to notice the stop signal.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

/// The platform error non-Linux builds report when a real socket is needed.
#[cfg(not(target_os = "linux"))]
pub(crate) const NOT_LINUX: &str =
    "SocketCAN requires Linux; this build does not include the can transport";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Open,
    Closed,
}

/// A source that receives CAN frames from a SocketCAN interface.
pub struct CanSource {
    config: CanConfig,
    decoder: Option<Arc<Decoder>>,
    frames: Option<Box<dyn FrameSource>>,
    phase: Phase,
    source_id: Option<SourceId>,
    clock: Option<Arc<dyn Clock>>,
    shutdown: Option<Shutdown>,
    stop: Option<Shutdown>,
    rx: Option<mpsc::Receiver<RawMessage>>,
    task: Option<JoinHandle<Result<(), CanError>>>,
    pending: Vec<RawMessage>,
    dropped: Arc<AtomicU64>,
}

impl CanSource {
    /// A source for `config`, reading its DBC when `emit: decoded`. Nothing
    /// is connected until `open`.
    pub fn new(config: CanConfig) -> Result<Self, CanError> {
        let decoder = load_decoder(&config)?;
        Ok(Self::composed(config, decoder, None))
    }

    /// A source fed by `frames` instead of the SocketCAN transport. This is
    /// the testing seam: with an in-memory [`FrameSource`] the whole decode
    /// and delivery path runs on any OS.
    #[cfg(test)]
    pub(crate) fn with_frame_source(
        config: CanConfig,
        frames: Box<dyn FrameSource>,
    ) -> Result<Self, CanError> {
        let decoder = load_decoder(&config)?;
        Ok(Self::composed(config, decoder, Some(frames)))
    }

    fn composed(
        config: CanConfig,
        decoder: Option<Arc<Decoder>>,
        frames: Option<Box<dyn FrameSource>>,
    ) -> Self {
        CanSource {
            config,
            decoder,
            frames,
            phase: Phase::Created,
            source_id: None,
            clock: None,
            shutdown: None,
            stop: None,
            rx: None,
            task: None,
            pending: Vec::new(),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    fn state_error(message: &'static str) -> ThadamError {
        CanError::State(message).into()
    }

    /// The queue is empty and closed: shutdown, a clean end of the frame
    /// source, or a transport failure the runtime should see.
    async fn receiver_ended(&mut self) -> Result<Option<SourceBatch>, ThadamError> {
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
            Err(join) => Err(CanError::Transport(format!("receive task failed: {join}")).into()),
        }
    }
}

/// Reads and compiles the DBC for `emit: decoded`.
fn load_decoder(config: &CanConfig) -> Result<Option<Arc<Decoder>>, CanError> {
    let (Emitter::Decoded, Some(path)) = (&config.emit, &config.dbc) else {
        return Ok(None);
    };
    let text = fs::read_to_string(path).map_err(|e| CanError::Dbc {
        path: path.clone(),
        detail: e.to_string(),
    })?;
    let decoder = Decoder::from_text(&text).map_err(|detail| CanError::Dbc {
        path: path.clone(),
        detail,
    })?;
    Ok(Some(Arc::new(decoder)))
}

#[cfg(target_os = "linux")]
fn open_transport(config: &CanConfig) -> Result<Box<dyn FrameSource>, CanError> {
    Ok(Box::new(crate::transport::SocketCanSource::open(config)?))
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::unnecessary_wraps)] // matches the Linux signature
fn open_transport(_config: &CanConfig) -> Result<Box<dyn FrameSource>, CanError> {
    Err(CanError::Config(NOT_LINUX.into()))
}

#[async_trait]
impl Source for CanSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        if self.phase == Phase::Open {
            return Err(Self::state_error("the CAN source is already open"));
        }
        let frames = match self.frames.take() {
            Some(frames) => frames,
            None => open_transport(&self.config).map_err(ThadamError::from)?,
        };

        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let stop = ctx.shutdown.child();
        let task = tokio::spawn(recv::run(
            self.config.clone(),
            self.decoder.clone(),
            frames,
            Arc::clone(&self.dropped),
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
                return self.receiver_ended().await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::ChannelFrames;
    use thadam_core::SourceId;
    use thadam_core::SystemClock;

    fn raw_config() -> CanConfig {
        CanConfig::from_value(&serde_json::json!({"interface": "vcan0"})).expect("valid")
    }

    fn ctx(shutdown: &Shutdown) -> SourceContext {
        SourceContext::new(
            SourceId::new("can-test"),
            shutdown.clone(),
            Arc::new(SystemClock::new()),
        )
    }

    #[tokio::test(start_paused = false)]
    async fn source_drains_in_memory_frames_in_batches() {
        let (frame_tx, frame_rx) = mpsc::channel(16);
        let shutdown = Shutdown::new();
        let mut source =
            CanSource::with_frame_source(raw_config(), Box::new(ChannelFrames { rx: frame_rx }))
                .expect("build");
        source.open(&ctx(&shutdown)).await.expect("open");

        for _ in 0..3 {
            frame_tx
                .send(crate::frame::CanFrameData::classic(0x55, &[0xAB]))
                .await
                .expect("send");
        }

        let batch = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let limits = BatchLimits {
                    max_messages: 8,
                    linger: Duration::from_millis(200),
                    ..BatchLimits::default()
                };
                if let Some(batch) = source.next_batch(limits).await.expect("batch") {
                    break batch;
                }
            }
        })
        .await
        .expect("batch within timeout");

        assert_eq!(batch.batch.messages.len(), 3);
        assert_eq!(batch.batch.messages[0].payload.as_ref(), &[0xAB]);
        drop(frame_tx);
        source.close().await.expect("close");
    }

    struct FailAfter {
        frames: Vec<crate::frame::CanFrameData>,
    }

    #[async_trait]
    impl FrameSource for FailAfter {
        async fn next_frame(&mut self) -> Result<Option<crate::frame::CanFrameData>, CanError> {
            match self.frames.pop() {
                Some(frame) => Ok(Some(frame)),
                None => Err(CanError::Transport("network is down".into())),
            }
        }
    }

    #[tokio::test]
    async fn a_transport_failure_surfaces_after_queued_messages() {
        let shutdown = Shutdown::new();
        let frames = vec![
            crate::frame::CanFrameData::classic(2, &[2]),
            crate::frame::CanFrameData::classic(1, &[1]),
        ];
        let mut source = CanSource::with_frame_source(raw_config(), Box::new(FailAfter { frames }))
            .expect("build");
        source.open(&ctx(&shutdown)).await.expect("open");

        let limits = BatchLimits {
            max_messages: 8,
            linger: Duration::from_millis(100),
            ..BatchLimits::default()
        };
        let mut payloads = Vec::new();
        let error = loop {
            match source.next_batch(limits).await {
                Ok(Some(batch)) => {
                    payloads.extend(batch.batch.messages.iter().map(|m| m.payload.to_vec()));
                }
                Ok(None) => panic!("a failure must not look like end of stream"),
                Err(error) => break error,
            }
        };
        assert_eq!(payloads, vec![vec![1], vec![2]]);
        assert!(error.is_transient(), "{error}");
        assert_eq!(error.code, thadam_core::codes::CONNECTOR_DISCONNECTED);

        // The runtime closes after a source error; the source stays closed
        // (not an error) until it is opened again.
        source.close().await.expect("close");
        assert!(source.next_batch(limits).await.expect("closed").is_none());
    }

    #[tokio::test]
    async fn decoded_source_reads_its_dbc_and_emits_signals() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("engine.dbc");
        std::fs::write(
            &path,
            "VERSION \"\"\n\nNS_ :\n\nBS_:\n\nBU_: E\n\n\
             BO_ 291 EngineData: 8 E\n SG_ Rpm : 0|16@1+ (0.25,0) [0|16383.75] \"rpm\" E\n",
        )
        .expect("write dbc");
        let config = CanConfig::from_value(&serde_json::json!({
            "interface": "vcan0", "emit": "decoded", "dbc": path,
        }))
        .expect("valid");

        let (frame_tx, frame_rx) = mpsc::channel(4);
        let shutdown = Shutdown::new();
        let mut source =
            CanSource::with_frame_source(config, Box::new(ChannelFrames { rx: frame_rx }))
                .expect("build");
        source.open(&ctx(&shutdown)).await.expect("open");
        frame_tx
            .send(crate::frame::CanFrameData::classic(
                291,
                &[0xE0, 0x2E, 0, 0, 0, 0, 0, 0],
            ))
            .await
            .expect("send");

        let batch = source
            .next_batch(BatchLimits::default())
            .await
            .expect("batch")
            .expect("a batch");
        let message = &batch.batch.messages[0];
        let payload: serde_json::Value = serde_json::from_slice(&message.payload).expect("json");
        assert_eq!(payload["Rpm"], serde_json::json!(3000.0));
        assert!(payload["ts_us"].is_i64());
        assert_eq!(message.attrs.get("topic"), Some("can/vcan0/EngineData"));
        source.close().await.expect("close");
    }

    #[test]
    fn a_missing_dbc_is_a_config_error() {
        let config = CanConfig::from_value(&serde_json::json!({
            "interface": "vcan0", "emit": "decoded", "dbc": "/nonexistent/engine.dbc",
        }))
        .expect("valid");
        let error: ThadamError = CanSource::new(config).err().expect("fails").into();
        assert_eq!(error.domain, thadam_core::ErrorDomain::Config);
        assert!(error.message.contains("engine.dbc"), "{error}");
    }

    #[test]
    fn the_source_does_not_ack() {
        let source = CanSource::new(raw_config()).expect("build");
        assert_eq!(source.capabilities().ack_mode, AckMode::None);
    }

    #[tokio::test]
    async fn next_batch_ends_when_shutdown_with_nothing_queued() {
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let shutdown = Shutdown::new();
        let mut source =
            CanSource::with_frame_source(raw_config(), Box::new(ChannelFrames { rx: frame_rx }))
                .expect("build");
        source.open(&ctx(&shutdown)).await.expect("open");
        shutdown.trigger();
        let ended = source
            .next_batch(BatchLimits::default())
            .await
            .expect("no error");
        assert!(ended.is_none());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn open_on_non_linux_reports_the_platform_error() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut source = CanSource::new(raw_config()).expect("build");
            let error = source
                .open(&ctx(&Shutdown::new()))
                .await
                .expect_err("linux-only");
            assert!(
                error.message.contains("SocketCAN requires Linux"),
                "{error}"
            );
            assert_eq!(error.domain, thadam_core::ErrorDomain::Config);
        });
    }
}
