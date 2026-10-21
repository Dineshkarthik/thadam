//! `MqttSource`: the pull `Source` over the poll task and the ack task.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    AckTarget, AckToken, BatchLimits, BuildContext, Clock, RawBatch, Shutdown, Source, SourceBatch,
    SourceCapabilities, SourceContext, SourceId, ThadamError,
};
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::ack_task;
use crate::config::{AckModeConfig, MqttConfig, Protocol};
use crate::error::MqttError;
use crate::link::{self, ClientHandle, Endpoint, LinkParams};
use crate::poll_task::{PollSettings, PollTask};
use crate::state::{AckCmd, Limits, Readiness, Shared, Status};
use crate::stats::{MqttHandle, SessionLostHook};
use crate::tls::{self, TlsRelay};

/// File in the component's data directory that records that a session exists
/// at the broker, so a restart can tell a lost session from a first run.
const SESSION_MARKER: &str = "mqtt-session";

/// How long `close` waits for acknowledgements to reach the socket, and then
/// for the DISCONNECT to be written, before it stops the tasks.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

/// The default MQTT 5 session expiry when none is configured: a session that
/// ends with the connection would defeat ack-after-commit.
const DEFAULT_V5_SESSION_EXPIRY_SECS: u32 = 24 * 60 * 60;

struct Running {
    shutdown: Shutdown,
    source_id: SourceId,
    clock: Arc<dyn Clock>,
    stop: CancellationToken,
    closing: Arc<AtomicBool>,
    client: ClientHandle,
    poll_done: watch::Receiver<bool>,
    poll_abort: AbortHandle,
    ack_abort: AbortHandle,
}

/// An MQTT source implementing the pull [`Source`] contract.
///
/// Build it through [`MqttSourceFactory`](crate::MqttSourceFactory) or
/// directly with [`MqttSource::new`].
pub struct MqttSource {
    config: MqttConfig,
    data_dir: PathBuf,
    hook: Option<SessionLostHook>,
    shared: Arc<Shared>,
    ack_rx: Option<mpsc::UnboundedReceiver<AckCmd>>,
    running: Option<Running>,
    closed: bool,
}

impl MqttSource {
    /// A source for `config`. Nothing connects until [`Source::open`].
    ///
    /// `config` is assumed validated ([`MqttConfig::from_value`]).
    pub fn new(config: MqttConfig, ctx: &BuildContext) -> Self {
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let limits = Limits {
            qos0_queue_bytes: usize::try_from(config.qos0_queue_bytes).unwrap_or(usize::MAX),
            guard_limit: config.unacked_guard_limit(),
            on_receive: config.ack_mode == AckModeConfig::OnReceive,
        };
        let (shared, _status) = Shared::new(limits, ack_tx);
        MqttSource {
            config,
            data_dir: ctx.data_dir.clone(),
            hook: None,
            shared: Arc::new(shared),
            ack_rx: Some(ack_rx),
            running: None,
            closed: false,
        }
    }

    /// Registers a function called from the poll task when the broker lost a
    /// session that should have existed. It must not block.
    #[must_use]
    pub fn with_session_lost_hook(mut self, hook: SessionLostHook) -> Self {
        self.hook = Some(hook);
        self
    }

    /// A handle to the source's metrics and epoch gate that outlives boxing
    /// the source into a `dyn Source`.
    pub fn handle(&self) -> MqttHandle {
        MqttHandle::new(self.shared.clone())
    }

    fn marker_path(&self) -> Option<PathBuf> {
        (!self.data_dir.as_os_str().is_empty()).then(|| self.data_dir.join(SESSION_MARKER))
    }

    async fn session_marker_present(&self) -> bool {
        let Some(path) = self.marker_path() else {
            return false;
        };
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => text.trim() == self.config.client_id,
            Err(_) => false,
        }
    }

    async fn write_session_marker(&self) {
        let Some(path) = self.marker_path() else {
            return;
        };
        let result = async {
            tokio::fs::create_dir_all(&self.data_dir).await?;
            tokio::fs::write(&path, self.config.client_id.as_bytes()).await
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(path = %path.display(), error = %e, "could not record the session marker");
        }
    }

    /// Chooses how the client reaches the broker: directly over TCP, or for
    /// `mqtts://` through a TLS relay that this source owns.
    async fn endpoint(&self) -> Result<(Endpoint, Option<TlsRelay>), MqttError> {
        let broker = self.config.broker().map_err(MqttError::Config)?;
        if !broker.tls {
            let endpoint = Endpoint::Tcp {
                host: broker.host,
                port: broker.port,
            };
            return Ok((endpoint, None));
        }
        let tls_config = self.config.tls.clone().unwrap_or_default();
        let client_config = tls::build_client_config(&tls_config).await?;
        let relay = TlsRelay::start(
            client_config,
            &broker.host,
            broker.port,
            self.config.connect_timeout.0,
        )?;
        Ok((Endpoint::Unix(relay.socket().to_owned()), Some(relay)))
    }

    async fn link_params(&self, endpoint: Endpoint) -> Result<LinkParams, MqttError> {
        let credentials = match (&self.config.username, &self.config.password_file) {
            (Some(user), Some(path)) => {
                let raw =
                    tokio::fs::read_to_string(path)
                        .await
                        .map_err(|e| MqttError::Credentials {
                            what: "password_file",
                            detail: format!("{}: {e}", path.display()),
                        })?;
                Some((user.clone(), raw.trim_end_matches(['\n', '\r']).to_owned()))
            }
            (Some(user), None) => Some((user.clone(), String::new())),
            _ => None,
        };
        let session_expiry_secs = match self.config.protocol {
            Protocol::V311 => None,
            Protocol::V5 => Some(
                self.config
                    .session_expiry
                    .map_or(DEFAULT_V5_SESSION_EXPIRY_SECS, |d| {
                        u32::try_from(d.0.as_secs()).unwrap_or(u32::MAX)
                    }),
            ),
        };
        Ok(LinkParams {
            protocol: self.config.protocol,
            client_id: self.config.client_id.clone(),
            endpoint,
            keep_alive: self.config.keep_alive.0,
            connect_timeout_secs: self.config.connect_timeout.0.as_secs(),
            clean_start: self.config.clean_start,
            max_incoming_packet_size: self.config.max_incoming_packet_size(),
            credentials,
            request_channel_capacity: self.config.request_channel_capacity,
            session_expiry_secs,
            receive_maximum: self.config.receive_maximum,
        })
    }

    fn start(
        &mut self,
        ctx: &SourceContext,
        params: &LinkParams,
        relay: Option<TlsRelay>,
        session_expected: bool,
    ) -> Result<(), MqttError> {
        let ack_rx = self.ack_rx.take().ok_or(MqttError::State(
            "this source was already opened; build a new one",
        ))?;
        self.shared
            .set_metrics(ctx.source_id.as_str().into(), ctx.metrics.clone());
        let (link, client) = link::open(params);
        let stop = CancellationToken::new();
        let closing = Arc::new(AtomicBool::new(false));
        let topics: Vec<(String, u8)> = self
            .config
            .topics
            .iter()
            .map(|t| (t.filter.clone(), t.qos))
            .collect();

        let poll = PollTask {
            link,
            client: client.clone(),
            shared: self.shared.clone(),
            stop: stop.clone(),
            closing: closing.clone(),
            clock: ctx.clock.clone(),
            hook: self.hook.clone(),
            settings: PollSettings {
                client_id: self.config.client_id.clone(),
                max_message_bytes: usize::try_from(self.config.max_message_bytes)
                    .unwrap_or(usize::MAX),
                max_incoming_packet_size: self.config.max_incoming_packet_size(),
                session_expected,
                persistent: !self.config.clean_start,
                guard_limit: self.config.unacked_guard_limit(),
            },
            relay,
        };
        let poll_handle = tokio::spawn(poll.run());
        let poll_abort = poll_handle.abort_handle();
        let (done_tx, poll_done) = watch::channel(false);
        let shared = self.shared.clone();
        tokio::spawn(async move {
            if let Err(e) = poll_handle.await
                && e.is_panic()
            {
                tracing::error!(error = %e, "MQTT source poll task panicked");
                shared.fail(MqttError::Panic.into());
            }
            done_tx.send_replace(true);
        });
        let ack_handle = tokio::spawn(ack_task::run(
            ack_rx,
            client.clone(),
            self.shared.clone(),
            topics,
        ));

        self.running = Some(Running {
            shutdown: ctx.shutdown.clone(),
            source_id: ctx.source_id.clone(),
            clock: ctx.clock.clone(),
            stop,
            closing,
            client,
            poll_done,
            poll_abort,
            ack_abort: ack_handle.abort_handle(),
        });
        Ok(())
    }

    fn abort_tasks(&mut self) {
        if let Some(run) = self.running.take() {
            run.stop.cancel();
            run.poll_abort.abort();
            run.ack_abort.abort();
        }
    }

    async fn wait_ready(&self, shutdown: &Shutdown) -> Result<(), ThadamError> {
        let deadline = Instant::now() + self.config.open_timeout.0;
        let mut status = self.shared.subscribe_status();
        loop {
            let current = status.borrow_and_update().clone();
            match current {
                Status::Ready => return Ok(()),
                Status::Failed(e) => return Err(e),
                Status::Connecting => {}
            }
            tokio::select! {
                changed = status.changed() => {
                    if changed.is_err() {
                        return Err(MqttError::Panic.into());
                    }
                }
                () = shutdown.triggered() => return Err(MqttError::Cancelled.into()),
                () = tokio::time::sleep_until(deadline) => {
                    return Err(MqttError::OpenTimeout(self.config.open_timeout.0).into());
                }
            }
        }
    }

    /// Waits for the acknowledgements the runtime completed to reach the
    /// socket.
    async fn drain_acks(&self) {
        let handle = self.handle();
        let deadline = Instant::now() + CLOSE_GRACE;
        while handle.stats().acks_pending() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

#[async_trait]
impl Source for MqttSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        if self.running.is_some() || self.closed {
            return Err(MqttError::State("open called twice").into());
        }
        let (endpoint, relay) = self.endpoint().await?;
        let params = self.link_params(endpoint).await?;
        let session_expected = !self.config.clean_start && self.session_marker_present().await;
        self.start(ctx, &params, relay, session_expected)?;

        if let Err(e) = self.wait_ready(&ctx.shutdown).await {
            self.abort_tasks();
            return Err(e);
        }
        if !self.config.clean_start {
            self.write_session_marker().await;
        }
        Ok(())
    }

    async fn next_batch(
        &mut self,
        limits: BatchLimits,
    ) -> Result<Option<SourceBatch>, ThadamError> {
        if self.closed {
            return Ok(None);
        }
        let Some(run) = &self.running else {
            return Err(MqttError::State("next_batch before open").into());
        };
        let shutdown = run.shutdown.clone();
        loop {
            // Wait for the first message. Nothing is removed from the queue
            // until `drain` below, so a dropped future loses nothing.
            loop {
                let notified = self.shared.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.shared.readiness(&limits) != Readiness::Empty {
                    break;
                }
                if let Some(e) = self.shared.fatal() {
                    return Err(e);
                }
                if shutdown.is_triggered() {
                    return Ok(None);
                }
                tokio::select! {
                    () = &mut notified => {}
                    () = shutdown.triggered() => {}
                }
            }

            if !limits.linger.is_zero() {
                let deadline = Instant::now() + limits.linger;
                loop {
                    let notified = self.shared.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if self.shared.readiness(&limits) == Readiness::Full
                        || shutdown.is_triggered()
                        || Instant::now() >= deadline
                    {
                        break;
                    }
                    tokio::select! {
                        () = &mut notified => {}
                        () = tokio::time::sleep_until(deadline) => break,
                        () = shutdown.triggered() => break,
                    }
                }
            }

            let Some(drained) = self.shared.drain(&limits) else {
                continue;
            };
            let Some(run) = &self.running else {
                return Ok(None);
            };
            let target: Arc<dyn AckTarget> = self.shared.clone();
            let ack = self
                .shared
                .gate
                .issue(AckToken::new(drained.first_seq, drained.last_seq), target);
            let batch = RawBatch::new(run.source_id.clone(), drained.messages, run.clock.synced());
            return Ok(Some(SourceBatch { batch, ack }));
        }
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        self.closed = true;
        let Some(run) = &self.running else {
            return Ok(());
        };
        let connected = self.shared.stats.connected.load(Ordering::Relaxed);
        if connected {
            // Acks flush only while the poll task runs, so keep it running
            // until the last one is written, then send DISCONNECT so the
            // persistent session keeps whatever is still unacknowledged.
            self.drain_acks().await;
            let Some(run) = &self.running else {
                return Ok(());
            };
            run.closing.store(true, Ordering::Release);
            let sent = tokio::time::timeout(CLOSE_GRACE, run.client.disconnect()).await;
            if matches!(sent, Ok(Ok(()))) {
                let mut done = run.poll_done.clone();
                let _ = tokio::time::timeout(CLOSE_GRACE, done.wait_for(|d| *d)).await;
            }
        } else {
            run.stop.cancel();
        }
        self.abort_tasks();
        Ok(())
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::new(self.config.ack_mode.into())
    }
}

impl Drop for MqttSource {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}
