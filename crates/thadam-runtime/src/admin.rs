//! Admin HTTP API server.
//!
//! Provides health, readiness, metrics, and status endpoints for monitoring
//! and operational purposes.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::serve::{IncomingStream, Listener};
use axum::{Json, Router};
use thadam_core::{ThadamError, codes};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

use crate::observability::PrometheusMetrics;
use crate::status::PipelineHandle;

/// Maximum concurrent admin connections before new accepts wait for a slot.
const MAX_CONNECTIONS: usize = 1024;
/// Maximum time to receive a complete HTTP/1 request header block.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time a keep-alive connection may sit idle between requests.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Maximum time to receive a request body after its headers arrive.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum request body bytes accepted on admin endpoints (all routes are GET).
const MAX_BODY_BYTES: usize = 64 * 1024;
/// Pause after a transient TCP accept error before retrying.
const ACCEPT_RETRY: Duration = Duration::from_millis(50);

/// Connection and per-request bounds for the admin HTTP server.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadMode {
    ReadingHead,
    Idle,
}

struct TimedConn {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
    in_handler: Arc<AtomicBool>,
    header_read_timeout: Duration,
    idle_timeout: Duration,
    mode: ReadMode,
    sleep: Pin<Box<Sleep>>,
}

impl TimedConn {
    fn new(
        inner: TcpStream,
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

impl AsyncRead for TimedConn {
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
                        "admin connection read timed out",
                    )));
                }
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for TimedConn {
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

#[derive(Clone, Debug)]
struct Peer {
    in_handler: Arc<AtomicBool>,
}

impl axum::extract::connect_info::Connected<IncomingStream<'_, AdminListener>> for Peer {
    fn connect_info(stream: IncomingStream<'_, AdminListener>) -> Self {
        stream.remote_addr().clone()
    }
}

struct AdminListener {
    listener: TcpListener,
    semaphore: Arc<Semaphore>,
    limits: Limits,
}

impl AdminListener {
    fn new(listener: TcpListener, limits: Limits) -> Self {
        AdminListener {
            listener,
            semaphore: Arc::new(Semaphore::new(limits.max_connections)),
            limits,
        }
    }
}

impl Listener for AdminListener {
    type Io = TimedConn;
    type Addr = Peer;

    async fn accept(&mut self) -> (TimedConn, Peer) {
        let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => std::future::pending().await,
        };
        loop {
            match self.listener.accept().await {
                Ok((tcp, _addr)) => {
                    let in_handler = Arc::new(AtomicBool::new(false));
                    let conn = TimedConn::new(tcp, permit, Arc::clone(&in_handler), self.limits);
                    return (conn, Peer { in_handler });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "admin listener accept failed");
                    tokio::time::sleep(ACCEPT_RETRY).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Peer> {
        let _ = self.listener.local_addr()?;
        Ok(Peer {
            in_handler: Arc::new(AtomicBool::new(false)),
        })
    }
}

/// Configuration for the admin HTTP server.
///
/// Specifies the bind address and optional authentication token.
/// When binding to a non-loopback address, a token is required for security.
#[derive(Clone, Debug)]
pub struct AdminConfig {
    /// The socket address to bind to.
    pub bind: SocketAddr,
    /// Optional bearer token for authentication.
    /// Required when binding to a non-loopback address.
    pub token: Option<String>,
}

/// Validates the admin server configuration.
///
/// Returns an error if binding to a non-loopback address without a token,
/// as this would expose the admin API to unauthorized access.
///
/// # Errors
///
/// Returns a [`ThadamError`] with domain [`ErrorDomain::Config`](thadam_core::ErrorDomain::Config)
/// and code [`CONFIG_INCOMPATIBLE`](thadam_core::codes::CONFIG_INCOMPATIBLE)
/// if `bind` is not loopback and `token` is `None`.
pub fn validate_admin_config(cfg: &AdminConfig) -> Result<(), ThadamError> {
    if !cfg.bind.ip().is_loopback() && cfg.token.is_none() {
        return Err(ThadamError::permanent(
            thadam_core::ErrorDomain::Config,
            codes::CONFIG_INCOMPATIBLE,
            "binding to non-loopback address requires an authentication token",
        ));
    }
    Ok(())
}

/// State shared by all admin routes.
struct AdminState {
    handle: PipelineHandle,
    metrics: Arc<PrometheusMetrics>,
    token: Option<String>,
}

/// Creates the admin router with all configured routes.
///
/// # Routes
///
/// - `GET /healthz` - Always returns 200 "ok"
/// - `GET /readyz` - Returns 200 if pipeline is ready, 503 otherwise
/// - `GET /metrics` - Returns Prometheus metrics in OpenMetrics format
/// - `GET /status` - Returns JSON status snapshot of the pipeline
///
/// # Authentication
///
/// When `token` is `Some`, all routes except `/healthz` require the header
/// `Authorization: Bearer <token>`. Missing or invalid tokens return 401.
pub fn router(
    handle: PipelineHandle,
    metrics: Arc<PrometheusMetrics>,
    token: Option<String>,
) -> Router {
    let state = Arc::new(AdminState {
        handle,
        metrics,
        token,
    });

    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route("/status", get(status_handler))
        .with_state(state)
}

/// Health check endpoint - always returns 200 "ok".
///
/// This endpoint does not require authentication.
async fn healthz() -> impl IntoResponse {
    "ok"
}

/// Readiness check endpoint.
///
/// Returns 200 if the pipeline is ready, 503 otherwise.
/// Requires authentication if a token is configured.
async fn readyz(State(state): State<Arc<AdminState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(response) = check_auth(&state.token, &headers) {
        return response.into_response();
    }

    if state.handle.is_ready() {
        (StatusCode::OK, "ok").into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

/// Metrics endpoint returning OpenMetrics format.
///
/// Returns Prometheus metrics with content type
/// `application/openmetrics-text; version=1.0.0; charset=utf-8`.
/// Requires authentication if a token is configured.
async fn metrics_handler(
    State(state): State<Arc<AdminState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(response) = check_auth(&state.token, &headers) {
        return response.into_response();
    }

    let metrics = state.metrics.encode();
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        metrics,
    )
        .into_response()
}

/// Status endpoint returning JSON snapshot of the pipeline.
///
/// Returns a comprehensive status snapshot including components,
/// cursors, WAL status, and budget information.
/// Requires authentication if a token is configured.
async fn status_handler(
    State(state): State<Arc<AdminState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(response) = check_auth(&state.token, &headers) {
        return response.into_response();
    }

    let status = state.handle.status();
    Json(status).into_response()
}

/// Checks authentication if a token is configured.
///
/// Returns Ok(()) if no token is required or if the Authorization header
/// matches the configured token. Returns 401 Unauthorized otherwise.
fn check_auth(token: &Option<String>, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if let Some(expected) = token {
        let auth_header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());

        match auth_header {
            Some(auth) if auth.starts_with("Bearer ") => {
                let provided = &auth[7..];
                if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
                    Ok(())
                } else {
                    Err(Box::new(StatusCode::UNAUTHORIZED.into_response()))
                }
            }
            _ => Err(Box::new(StatusCode::UNAUTHORIZED.into_response())),
        }
    } else {
        Ok(())
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

/// Starts the admin HTTP server.
///
/// Binds to the configured address and serves requests until the shutdown
/// signal is triggered. Validates the configuration before starting.
///
/// # Errors
///
/// Returns a [`ThadamError`] if:
/// - Configuration validation fails
/// - Binding to the address fails
/// - The server encounters an error during operation
pub async fn serve(
    cfg: AdminConfig,
    handle: PipelineHandle,
    metrics: Arc<PrometheusMetrics>,
    shutdown: thadam_core::Shutdown,
) -> Result<(), ThadamError> {
    // Validate configuration
    validate_admin_config(&cfg)?;

    // Create the router
    let app = router(handle, metrics, cfg.token);

    // Bind to the address
    let listener = TcpListener::bind(cfg.bind).await.map_err(|e| {
        ThadamError::permanent(
            thadam_core::ErrorDomain::Config,
            codes::CONFIG_INVALID,
            format!("failed to bind admin server to {}: {}", cfg.bind, e),
        )
    })?;

    serve_listener_with_limits(listener, app, Limits::default(), shutdown).await
}

pub(crate) async fn serve_listener_with_limits(
    listener: TcpListener,
    app: Router,
    limits: Limits,
    shutdown: thadam_core::Shutdown,
) -> Result<(), ThadamError> {
    let body_read_timeout = limits.body_read_timeout;
    let app = app.layer(middleware::from_fn(
        move |req: Request, next: Next| async move {
            let in_handler = req
                .extensions()
                .get::<ConnectInfo<Peer>>()
                .map(|ConnectInfo(peer)| Arc::clone(&peer.in_handler));
            let _active = in_handler.as_ref().map(|flag| ActiveGuard::enter(flag));
            let (parts, body) = req.into_parts();
            match tokio::time::timeout(body_read_timeout, to_bytes(body, MAX_BODY_BYTES)).await {
                Ok(Ok(bytes)) => {
                    let req = Request::from_parts(parts, Body::from(bytes));
                    next.run(req).await
                }
                Ok(Err(_)) => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
                Err(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
            }
        },
    ));

    axum::serve(
        AdminListener::new(listener, limits),
        app.into_make_service_with_connect_info::<Peer>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.triggered().await;
    })
    .await
    .map_err(|e| {
        ThadamError::permanent(
            thadam_core::ErrorDomain::Sink,
            codes::SINK_IO,
            format!("admin server error: {}", e),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn validate_loopback_without_token() {
        let cfg = AdminConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9464),
            token: None,
        };
        assert!(validate_admin_config(&cfg).is_ok());
    }

    #[test]
    fn validate_non_loopback_without_token_rejected() {
        let cfg = AdminConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9464),
            token: None,
        };
        let err = validate_admin_config(&cfg).unwrap_err();
        assert_eq!(err.domain, thadam_core::ErrorDomain::Config);
        assert_eq!(err.code, codes::CONFIG_INCOMPATIBLE);
    }

    #[test]
    fn validate_non_loopback_with_token() {
        let cfg = AdminConfig {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 9464),
            token: Some("secret".to_string()),
        };
        assert!(validate_admin_config(&cfg).is_ok());
    }

    fn with_auth(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::AUTHORIZATION, value.parse().unwrap());
        headers
    }

    #[test]
    fn check_auth_requires_the_exact_bearer_token() {
        let token = Some("secret".to_string());
        assert!(check_auth(&token, &with_auth("Bearer secret")).is_ok());
        assert!(check_auth(&token, &with_auth("Bearer secre")).is_err());
        assert!(check_auth(&token, &with_auth("Bearer secrets")).is_err());
        assert!(check_auth(&token, &with_auth("secret")).is_err());
        assert!(check_auth(&token, &HeaderMap::new()).is_err());
        assert!(check_auth(&None, &HeaderMap::new()).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_or_idle_client_is_cut_off() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let limits = Limits {
            max_connections: 4,
            header_read_timeout: Duration::from_millis(80),
            idle_timeout: Duration::from_millis(80),
            body_read_timeout: Duration::from_millis(80),
        };
        let shutdown = thadam_core::Shutdown::new();
        let stop = shutdown.clone();
        let app = Router::new().route("/healthz", get(healthz));
        let server =
            tokio::spawn(
                async move { serve_listener_with_limits(listener, app, limits, stop).await },
            );

        // 1. A client that sends partial headers and stalls is cut off.
        let mut slow_headers = TcpStream::connect(addr).await.unwrap();
        slow_headers
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), slow_headers.read(&mut buf))
            .await
            .expect("header timeout must close the socket")
            .unwrap_or(0);
        assert_eq!(n, 0, "slow header client must be disconnected");

        // 2. A client that sends headers with Content-Length and stalls on body gets 408.
        let mut slow_body = TcpStream::connect(addr).await.unwrap();
        slow_body
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nContent-Length: 32\r\n\r\nab")
            .await
            .unwrap();
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), slow_body.read_to_end(&mut reply))
            .await
            .expect("body timeout must answer and close")
            .unwrap();
        let text = String::from_utf8_lossy(&reply);
        assert!(
            text.starts_with("HTTP/1.1 408"),
            "expected 408, got: {text}"
        );

        shutdown.trigger();
        server.await.unwrap().unwrap();
    }
}
