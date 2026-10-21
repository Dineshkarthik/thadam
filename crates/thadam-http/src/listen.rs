//! `mode: listen`: an HTTP endpoint whose `POST` bodies become messages.
//!
//! A response is sent only after the message's batch is committed, so a
//! `204` means the message is durable. Anything that prevents that (a full
//! queue, a shutdown, an abandoned batch) is answered with a `503` the client
//! can retry.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{ConnectInfo, State};
use axum::http::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER, WWW_AUTHENTICATE,
};
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::serve::{IncomingStream, Listener};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use thadam_core::{AckOutcome, Attributes, Shutdown};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, Sleep};
use tokio_rustls::TlsAcceptor;

use crate::config::ListenConfig;
use crate::error::HttpError;
use crate::queue::{Message, Shared};
use crate::tls;

/// Maximum time to complete a TLS handshake before dropping the connection.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Pause after a transient TCP accept error before retrying.
const ACCEPT_RETRY: Duration = Duration::from_millis(50);
/// Accepted connections buffered between the handshake task and axum.
const PENDING_CONNECTIONS: usize = 64;
/// Maximum concurrent connections before new accepts wait for a free slot.
const MAX_CONNECTIONS: usize = 1024;
/// Maximum time to receive a complete HTTP/1 request header block.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time a keep-alive connection may sit idle between requests.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Maximum time to receive a request body after its headers arrive.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Connection and per-request bounds for the HTTP listener.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) max_connections: usize,
    pub(crate) header_read_timeout: Duration,
    pub(crate) idle_timeout: Duration,
    pub(crate) body_read_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_connections: MAX_CONNECTIONS,
            header_read_timeout: HEADER_READ_TIMEOUT,
            idle_timeout: IDLE_TIMEOUT,
            body_read_timeout: BODY_READ_TIMEOUT,
        }
    }
}

/// A connection stream, plain or TLS.
pub(crate) trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

type Conn = Box<dyn Duplex>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadMode {
    ReadingHead,
    Idle,
}

/// Wraps a connection stream with a concurrency permit plus HTTP/1 header-read
/// and keep-alive idle timeouts.
struct TimedConn<S> {
    inner: S,
    _permit: OwnedSemaphorePermit,
    in_handler: Arc<AtomicBool>,
    header_read_timeout: Duration,
    idle_timeout: Duration,
    mode: ReadMode,
    sleep: Pin<Box<Sleep>>,
}

impl<S> TimedConn<S> {
    fn new(
        inner: S,
        permit: OwnedSemaphorePermit,
        in_handler: Arc<AtomicBool>,
        limits: Limits,
    ) -> Self {
        TimedConn {
            inner,
            _permit: permit,
            in_handler,
            header_read_timeout: limits.header_read_timeout,
            idle_timeout: limits.idle_timeout,
            mode: ReadMode::ReadingHead,
            sleep: Box::pin(tokio::time::sleep(limits.header_read_timeout)),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for TimedConn<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len().saturating_sub(before);
                if read > 0
                    && !self.in_handler.load(Ordering::Acquire)
                    && self.mode == ReadMode::Idle
                {
                    self.mode = ReadMode::ReadingHead;
                    let deadline = Instant::now() + self.header_read_timeout;
                    self.sleep.as_mut().reset(deadline);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                if self.in_handler.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
                if self.sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "HTTP connection read timed out",
                    )));
                }
                Poll::Pending
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TimedConn<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let res = Pin::new(&mut self.inner).poll_flush(cx);
        if res.is_ready() && !self.in_handler.load(Ordering::Acquire) {
            self.mode = ReadMode::Idle;
            let deadline = Instant::now() + self.idle_timeout;
            self.sleep.as_mut().reset(deadline);
            let _ = self.sleep.as_mut().poll(cx);
        }
        res
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct ActiveGuard<'a>(&'a AtomicBool);

impl<'a> ActiveGuard<'a> {
    fn enter(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Release);
        ActiveGuard(flag)
    }
}

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// The peer address and handler-activity flag handlers see through `ConnectInfo`.
#[derive(Clone, Debug)]
pub(crate) struct Peer {
    addr: SocketAddr,
    in_handler: Arc<AtomicBool>,
}

impl axum::extract::connect_info::Connected<IncomingStream<'_, Acceptor>> for Peer {
    fn connect_info(stream: IncomingStream<'_, Acceptor>) -> Self {
        stream.remote_addr().clone()
    }
}

/// Feeds axum connections that were accepted and, when TLS is on, already
/// handshaken by their own tasks, so a slow client cannot stall accepts.
pub(crate) struct Acceptor {
    rx: mpsc::Receiver<(Conn, Peer)>,
    local: SocketAddr,
}

impl Listener for Acceptor {
    type Io = Conn;
    type Addr = Peer;

    async fn accept(&mut self) -> (Conn, Peer) {
        match self.rx.recv().await {
            Some(connection) => connection,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Peer> {
        Ok(Peer {
            addr: self.local,
            in_handler: Arc::new(AtomicBool::new(false)),
        })
    }
}

struct ListenState {
    queue: Arc<Shared>,
    path: String,
    token: Option<String>,
    max_bytes: usize,
    body_read_timeout: Duration,
    extract_headers: Vec<(HeaderName, String)>,
}

/// Binds the endpoint and spawns its accept loop and server onto `tasks`.
/// Returns the bound address, which differs from the configured one when the
/// port is zero.
pub(crate) async fn start(
    listen: &ListenConfig,
    max_bytes: usize,
    queue: Arc<Shared>,
    stop: Shutdown,
    tasks: &mut JoinSet<()>,
) -> Result<SocketAddr, HttpError> {
    start_with_limits(listen, max_bytes, Limits::default(), queue, stop, tasks).await
}

pub(crate) async fn start_with_limits(
    listen: &ListenConfig,
    max_bytes: usize,
    limits: Limits,
    queue: Arc<Shared>,
    stop: Shutdown,
    tasks: &mut JoinSet<()>,
) -> Result<SocketAddr, HttpError> {
    let acceptor = listen.tls.as_ref().map(tls::server_acceptor).transpose()?;

    let listener = TcpListener::bind(listen.bind)
        .await
        .map_err(|e| HttpError::Bind {
            addr: listen.bind.to_string(),
            detail: e.to_string(),
        })?;
    let local = listener.local_addr().map_err(|e| HttpError::Bind {
        addr: listen.bind.to_string(),
        detail: e.to_string(),
    })?;

    let (tx, rx) = mpsc::channel(PENDING_CONNECTIONS);
    tasks.spawn(accept_loop(listener, acceptor, limits, tx, stop.clone()));

    let state = Arc::new(ListenState {
        queue,
        path: listen.path.clone(),
        token: listen.bearer_token.clone(),
        max_bytes,
        body_read_timeout: limits.body_read_timeout,
        extract_headers: listen
            .extract_headers
            .iter()
            .filter_map(|name| {
                let header = HeaderName::from_bytes(name.as_bytes()).ok()?;
                Some((header, format!("http.header.{}", name.to_ascii_lowercase())))
            })
            .collect(),
    });
    let router = Router::new()
        .route(&listen.path, post(ingest))
        .with_state(state);
    tasks.spawn(serve(Acceptor { rx, local }, router, stop));

    Ok(local)
}

async fn accept_loop(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    limits: Limits,
    tx: mpsc::Sender<(Conn, Peer)>,
    stop: Shutdown,
) {
    let semaphore = Arc::new(Semaphore::new(limits.max_connections));
    let mut handshakes = JoinSet::new();
    loop {
        while handshakes.try_join_next().is_some() {}
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
        let (tcp, peer) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "HTTP listener accept failed");
                drop(permit);
                tokio::time::sleep(ACCEPT_RETRY).await;
                continue;
            }
        };
        handshakes.spawn(hand_over(
            tcp,
            peer,
            acceptor.clone(),
            limits,
            permit,
            tx.clone(),
        ));
    }
    handshakes.shutdown().await;
}

async fn hand_over(
    tcp: TcpStream,
    peer: SocketAddr,
    acceptor: Option<TlsAcceptor>,
    limits: Limits,
    permit: OwnedSemaphorePermit,
    tx: mpsc::Sender<(Conn, Peer)>,
) {
    let in_handler = Arc::new(AtomicBool::new(false));
    let conn: Conn = match acceptor {
        None => Box::new(TimedConn::new(tcp, permit, Arc::clone(&in_handler), limits)),
        Some(acceptor) => {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => Box::new(TimedConn::new(
                    stream,
                    permit,
                    Arc::clone(&in_handler),
                    limits,
                )),
                Ok(Err(e)) => {
                    tracing::debug!(%peer, error = %e, "TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "TLS handshake timed out");
                    return;
                }
            }
        }
    };
    let _ = tx
        .send((
            conn,
            Peer {
                addr: peer,
                in_handler,
            },
        ))
        .await;
}

async fn serve(acceptor: Acceptor, router: Router, stop: Shutdown) {
    let served = axum::serve(
        acceptor,
        router.into_make_service_with_connect_info::<Peer>(),
    )
    .with_graceful_shutdown(async move { stop.triggered().await })
    .await;
    if let Err(e) = served {
        tracing::error!(error = %e, "HTTP server stopped");
    }
}

async fn ingest(
    State(state): State<Arc<ListenState>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let _active = ActiveGuard::enter(&peer.in_handler);
    if let Some(token) = &state.token
        && !authorized(&headers, token)
    {
        return (StatusCode::UNAUTHORIZED, [(WWW_AUTHENTICATE, "Bearer")]).into_response();
    }

    let declared = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    if declared.is_some_and(|len| len > state.max_bytes) {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    }
    let payload: Bytes = match tokio::time::timeout(
        state.body_read_timeout,
        to_bytes(body, state.max_bytes),
    )
    .await
    {
        Ok(Ok(payload)) => payload,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };

    let (reply, outcome) = oneshot::channel();
    let message = Message {
        payload,
        attrs: attributes(&state, &headers, peer.addr),
        reply: Some(reply),
    };
    if state.queue.try_push(message).is_err() {
        return busy();
    }

    match outcome.await {
        Ok(AckOutcome::Committed) => StatusCode::NO_CONTENT.into_response(),
        _ => busy(),
    }
}

fn attributes(state: &ListenState, headers: &HeaderMap, peer: SocketAddr) -> Attributes {
    let mut attrs = Attributes::new();
    attrs.insert("http.path", state.path.clone());
    attrs.insert("http.remote_addr", peer.to_string());
    if let Some(content_type) = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) {
        attrs.insert("http.content_type", content_type.to_owned());
    }
    for (name, key) in &state.extract_headers {
        if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok()) {
            attrs.insert(key.clone(), value.to_owned());
        }
    }
    attrs
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|presented| constant_time_eq(presented.as_bytes(), token.as_bytes()))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let da = Sha256::digest(a);
    let db = Sha256::digest(b);
    da.iter()
        .zip(db.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

fn busy() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, [(RETRY_AFTER, "1")]).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn constant_time_eq_compares_digests_without_length_short_circuit() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"secret", b"secrets"));
        assert!(!constant_time_eq(b"", b"secret"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_or_idle_client_is_cut_off() {
        let listen = ListenConfig {
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..ListenConfig::default()
        };
        let limits = Limits {
            max_connections: 4,
            header_read_timeout: Duration::from_millis(80),
            idle_timeout: Duration::from_millis(80),
            body_read_timeout: Duration::from_millis(80),
        };
        let queue = Shared::new(16);
        let stop = Shutdown::new();
        let mut tasks = JoinSet::new();
        let addr = start_with_limits(&listen, 1024, limits, queue, stop.clone(), &mut tasks)
            .await
            .expect("bind");

        // 1. A client that sends partial headers and stalls is cut off.
        let mut slow_headers = TcpStream::connect(addr).await.expect("connect");
        slow_headers
            .write_all(b"POST /ingest HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("write partial headers");
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), slow_headers.read(&mut buf))
            .await
            .expect("header timeout must close the socket")
            .unwrap_or(0);
        assert_eq!(n, 0, "slow header client must be disconnected");

        // 2. A client that sends headers with Content-Length but stalls on the body gets 408.
        let mut slow_body = TcpStream::connect(addr).await.expect("connect");
        slow_body
            .write_all(b"POST /ingest HTTP/1.1\r\nHost: localhost\r\nContent-Length: 32\r\n\r\nab")
            .await
            .expect("write partial body");
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), slow_body.read_to_end(&mut reply))
            .await
            .expect("body timeout must answer and close")
            .expect("read response");
        let text = String::from_utf8_lossy(&reply);
        assert!(
            text.starts_with("HTTP/1.1 408"),
            "expected 408, got: {text}"
        );

        stop.trigger();
        tasks.shutdown().await;
    }
}
