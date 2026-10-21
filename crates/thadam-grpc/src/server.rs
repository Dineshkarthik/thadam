//! The tonic `Ingest` service. A request's messages are queued without
//! waiting; its response is sent only once every message is committed.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use sha2::{Digest, Sha256};
use thadam_core::{AckOutcome, Attributes, Shutdown};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, Sleep};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::transport::server::{Connected, TcpConnectInfo};
use tonic::{Request, Response, Status, Streaming};

use crate::config::GrpcConfig;
use crate::error::GrpcError;
use crate::proto::ingest_server::{Ingest, IngestServer};
use crate::proto::{PublishRequest, PublishResponse, PublishStreamResponse};
use crate::queue::{Message, PushError, Shared};
use crate::tls;

/// Headroom above `max_message_bytes` for the envelope (stream name, content
/// type, attributes). Payloads over the limit are still answered with
/// `INVALID_ARGUMENT`; only envelopes beyond this allowance are refused by the
/// transport.
const ENVELOPE_ALLOWANCE: usize = 1 << 20;

/// Messages of one client-streaming call that may be queued before the first
/// commit is awaited.
const STREAM_WINDOW: usize = 64;

/// Maximum concurrent client connections before new accepts wait for a slot.
const MAX_CONNECTIONS: usize = 1024;
/// Maximum concurrent HTTP/2 streams on a single connection.
const MAX_CONCURRENT_STREAMS: u32 = 256;
/// Maximum time to receive the initial HTTP/2 connection preface and settings.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Interval between server-initiated HTTP/2 keep-alive PING frames.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
/// Maximum time to wait for an HTTP/2 keep-alive PING acknowledgement.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time a connection may go without any inbound bytes on the socket.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Maximum time to receive a single gRPC request message body.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause after a transient TCP accept error before retrying.
const ACCEPT_RETRY: Duration = Duration::from_millis(50);
/// Accepted connections buffered between the accept loop and tonic.
const PENDING_CONNECTIONS: usize = 64;
/// Length in bytes of the mandatory HTTP/2 client connection preface.
const HTTP2_PREFACE_LEN: usize = 24;

/// Connection and per-call bounds for the gRPC server.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) max_connections: usize,
    pub(crate) header_read_timeout: Duration,
    pub(crate) keepalive_interval: Duration,
    pub(crate) keepalive_timeout: Duration,
    pub(crate) idle_timeout: Duration,
    pub(crate) body_read_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_connections: MAX_CONNECTIONS,
            header_read_timeout: HEADER_READ_TIMEOUT,
            keepalive_interval: KEEPALIVE_INTERVAL,
            keepalive_timeout: KEEPALIVE_TIMEOUT,
            idle_timeout: IDLE_TIMEOUT,
            body_read_timeout: BODY_READ_TIMEOUT,
        }
    }
}

/// Wraps an accepted TCP socket with a concurrency permit plus preface/header
/// and idle read timeouts.
struct TimedStream {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
    idle_timeout: Duration,
    bytes_read: usize,
    sleep: Pin<Box<Sleep>>,
}

impl TimedStream {
    fn new(inner: TcpStream, permit: OwnedSemaphorePermit, limits: Limits) -> Self {
        TimedStream {
            inner,
            _permit: permit,
            idle_timeout: limits.idle_timeout,
            bytes_read: 0,
            sleep: Box::pin(tokio::time::sleep(limits.header_read_timeout)),
        }
    }
}

impl Connected for TimedStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

impl AsyncRead for TimedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len().saturating_sub(before);
                if read > 0 {
                    self.bytes_read = self.bytes_read.saturating_add(read);
                    if self.bytes_read >= HTTP2_PREFACE_LEN {
                        let deadline = Instant::now() + self.idle_timeout;
                        self.sleep.as_mut().reset(deadline);
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                if self.sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "gRPC connection read timed out",
                    )));
                }
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for TimedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(crate) struct IngestService {
    queue: Arc<Shared>,
    token: Option<String>,
    max_bytes: usize,
    body_read_timeout: Duration,
}

impl IngestService {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if presented.is_some_and(|p| constant_time_eq(p.as_bytes(), token.as_bytes())) {
            Ok(())
        } else {
            Err(Status::unauthenticated("missing or invalid bearer token"))
        }
    }

    /// Queues one request's payload and returns the receiver that resolves
    /// when its batch is released.
    fn enqueue(
        &self,
        request: PublishRequest,
        method: &'static str,
        peer: Option<SocketAddr>,
    ) -> Result<oneshot::Receiver<AckOutcome>, Status> {
        if request.payload.len() > self.max_bytes {
            return Err(Status::invalid_argument(format!(
                "payload is {} bytes; max_message_bytes is {}",
                request.payload.len(),
                self.max_bytes
            )));
        }
        let PublishRequest {
            stream,
            payload,
            content_type,
            attributes,
        } = request;
        let attrs = attributes_for(&stream, &content_type, attributes, method, peer);
        let (reply, outcome) = oneshot::channel();
        let message = Message {
            payload: Bytes::from(payload),
            attrs,
            reply: Some(reply),
        };
        self.queue.try_push(message).map_err(|e| match e {
            PushError::Full => Status::resource_exhausted("source queue is full; retry later"),
            PushError::Closed => Status::unavailable("source is shutting down"),
        })?;
        Ok(outcome)
    }
}

async fn committed(outcome: oneshot::Receiver<AckOutcome>) -> Result<(), Status> {
    match outcome.await {
        Ok(AckOutcome::Committed) => Ok(()),
        _ => Err(Status::unavailable("message was not committed; retry")),
    }
}

#[tonic::async_trait]
impl Ingest for IngestService {
    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        self.authorize(&request)?;
        let peer = request.remote_addr();
        let outcome = self.enqueue(request.into_inner(), "Publish", peer)?;
        committed(outcome).await?;
        Ok(Response::new(PublishResponse {}))
    }

    async fn publish_stream(
        &self,
        request: Request<Streaming<PublishRequest>>,
    ) -> Result<Response<PublishStreamResponse>, Status> {
        self.authorize(&request)?;
        let peer = request.remote_addr();
        let mut stream = request.into_inner();

        let mut waiting: VecDeque<oneshot::Receiver<AckOutcome>> = VecDeque::new();
        let mut accepted = 0u64;
        while let Some(message) = tokio::time::timeout(self.body_read_timeout, stream.message())
            .await
            .map_err(|_| Status::deadline_exceeded("message body read timed out"))??
        {
            waiting.push_back(self.enqueue(message, "PublishStream", peer)?);
            accepted += 1;
            if waiting.len() > STREAM_WINDOW
                && let Some(outcome) = waiting.pop_front()
            {
                committed(outcome).await?;
            }
        }
        while let Some(outcome) = waiting.pop_front() {
            committed(outcome).await?;
        }
        Ok(Response::new(PublishStreamResponse { accepted }))
    }
}

fn attributes_for(
    stream: &str,
    content_type: &str,
    extra: HashMap<String, String>,
    method: &'static str,
    peer: Option<SocketAddr>,
) -> Attributes {
    let mut attrs = Attributes::new();
    attrs.insert("grpc.method", method);
    attrs.insert("grpc.stream", stream.to_owned());
    if !content_type.is_empty() {
        attrs.insert("grpc.content_type", content_type.to_owned());
    }
    if let Some(peer) = peer {
        attrs.insert("grpc.peer", peer.to_string());
    }
    for (key, value) in extra {
        attrs.insert(format!("grpc.attr.{key}"), value);
    }
    attrs
}

/// Binds the server and spawns it onto `tasks`. Returns the bound address,
/// which differs from the configured one when the port is zero.
pub(crate) async fn start(
    config: &GrpcConfig,
    max_bytes: usize,
    queue: Arc<Shared>,
    stop: Shutdown,
    tasks: &mut JoinSet<()>,
) -> Result<SocketAddr, GrpcError> {
    start_with_limits(config, max_bytes, Limits::default(), queue, stop, tasks).await
}

pub(crate) async fn start_with_limits(
    config: &GrpcConfig,
    max_bytes: usize,
    limits: Limits,
    queue: Arc<Shared>,
    stop: Shutdown,
    tasks: &mut JoinSet<()>,
) -> Result<SocketAddr, GrpcError> {
    let mut builder = Server::builder()
        .max_concurrent_streams(Some(MAX_CONCURRENT_STREAMS))
        .http2_keepalive_interval(Some(limits.keepalive_interval))
        .http2_keepalive_timeout(Some(limits.keepalive_timeout));
    if let Some(tls_files) = &config.tls {
        builder = builder
            .tls_config(tls::server_tls(tls_files)?)
            .map_err(|e| GrpcError::Tls(e.to_string()))?;
    }

    let listener = TcpListener::bind(config.bind)
        .await
        .map_err(|e| GrpcError::Bind {
            addr: config.bind.to_string(),
            detail: e.to_string(),
        })?;
    let local = listener.local_addr().map_err(|e| GrpcError::Bind {
        addr: config.bind.to_string(),
        detail: e.to_string(),
    })?;

    let (tx, rx) = mpsc::channel(PENDING_CONNECTIONS);
    tasks.spawn(accept_loop(listener, limits, tx, stop.clone()));

    let service = IngestServer::new(IngestService {
        queue,
        token: config.bearer_token.clone(),
        max_bytes,
        body_read_timeout: limits.body_read_timeout,
    })
    .max_decoding_message_size(max_bytes.saturating_add(ENVELOPE_ALLOWANCE));

    tasks.spawn(async move {
        let served = builder
            .add_service(service)
            .serve_with_incoming_shutdown(ReceiverStream::new(rx), async move {
                stop.triggered().await;
            })
            .await;
        if let Err(e) = served {
            tracing::error!(error = %e, "gRPC server stopped");
        }
    });
    Ok(local)
}

async fn accept_loop(
    listener: TcpListener,
    limits: Limits,
    tx: mpsc::Sender<io::Result<TimedStream>>,
    stop: Shutdown,
) {
    let semaphore = Arc::new(Semaphore::new(limits.max_connections));
    loop {
        let permit = tokio::select! {
            () = stop.triggered() => break,
            permit = Arc::clone(&semaphore).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let accepted = tokio::select! {
            () = stop.triggered() => break,
            accepted = listener.accept() => accepted,
        };
        match accepted {
            Ok((tcp, _peer)) => {
                let _ = tcp.set_nodelay(true);
                let stream = TimedStream::new(tcp, permit, limits);
                if tx.send(Ok(stream)).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "gRPC listener accept failed");
                drop(permit);
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let da = Sha256::digest(a);
    let db = Sha256::digest(b);
    da.iter()
        .zip(db.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::ingest_client::IngestClient;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tonic::Code;

    #[test]
    fn constant_time_eq_compares_digests_without_length_short_circuit() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"secret", b"secrets"));
        assert!(!constant_time_eq(b"", b"secret"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_or_idle_client_is_cut_off() {
        let config = GrpcConfig {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..GrpcConfig::default()
        };
        let limits = Limits {
            max_connections: 4,
            header_read_timeout: Duration::from_millis(80),
            keepalive_interval: Duration::from_millis(80),
            keepalive_timeout: Duration::from_millis(80),
            idle_timeout: Duration::from_millis(250),
            body_read_timeout: Duration::from_millis(80),
        };
        let queue = Shared::new(16);
        let stop = Shutdown::new();
        let mut tasks = JoinSet::new();
        let addr = start_with_limits(&config, 1024, limits, queue, stop.clone(), &mut tasks)
            .await
            .expect("bind");

        // 1. A TCP client that never completes the HTTP/2 preface is cut off.
        let mut slow_preface = TcpStream::connect(addr).await.expect("connect");
        slow_preface.write_all(b"PRI * ").await.expect("partial");
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), slow_preface.read_to_end(&mut buf))
            .await
            .expect("preface timeout must close the socket")
            .expect("read until server EOF");

        // 2. A client stream that stalls without sending the next message body is cut off.
        let mut client = IngestClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let (_tx, rx) = mpsc::channel::<PublishRequest>(1);
        let status = tokio::time::timeout(
            Duration::from_secs(2),
            client.publish_stream(ReceiverStream::new(rx)),
        )
        .await
        .expect("stream body timeout must fire")
        .expect_err("stalled stream must fail");
        assert_eq!(status.code(), Code::DeadlineExceeded);

        stop.trigger();
        tasks.shutdown().await;
    }
}
