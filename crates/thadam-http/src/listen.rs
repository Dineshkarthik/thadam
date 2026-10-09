//! `mode: listen`: an HTTP endpoint whose `POST` bodies become messages.
//!
//! A response is sent only after the message's batch is committed, so a
//! `204` means the message is durable. Anything that prevents that (a full
//! queue, a shutdown, an abandoned batch) is answered with a `503` the client
//! can retry.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
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
use thadam_core::{AckOutcome, Attributes, Shutdown};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use crate::config::ListenConfig;
use crate::error::HttpError;
use crate::queue::{Message, Shared};
use crate::tls;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_RETRY: Duration = Duration::from_millis(50);
const PENDING_CONNECTIONS: usize = 64;

/// A connection stream, plain or TLS.
pub(crate) trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

type Conn = Box<dyn Duplex>;

/// The peer address handlers see through `ConnectInfo`.
#[derive(Clone, Copy, Debug)]
struct Peer(SocketAddr);

impl axum::extract::connect_info::Connected<IncomingStream<'_, Acceptor>> for Peer {
    fn connect_info(stream: IncomingStream<'_, Acceptor>) -> Self {
        Peer(*stream.remote_addr())
    }
}

/// Feeds axum connections that were accepted and, when TLS is on, already
/// handshaken by their own tasks, so a slow client cannot stall accepts.
pub(crate) struct Acceptor {
    rx: mpsc::Receiver<(Conn, SocketAddr)>,
    local: SocketAddr,
}

impl Listener for Acceptor {
    type Io = Conn;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Conn, SocketAddr) {
        match self.rx.recv().await {
            Some(connection) => connection,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
}

struct ListenState {
    queue: Arc<Shared>,
    path: String,
    token: Option<String>,
    max_bytes: usize,
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
    tasks.spawn(accept_loop(listener, acceptor, tx, stop.clone()));

    let state = Arc::new(ListenState {
        queue,
        path: listen.path.clone(),
        token: listen.bearer_token.clone(),
        max_bytes,
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
    tx: mpsc::Sender<(Conn, SocketAddr)>,
    stop: Shutdown,
) {
    loop {
        let accepted = tokio::select! {
            () = stop.triggered() => break,
            accepted = listener.accept() => accepted,
        };
        let (tcp, peer) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, "HTTP listener accept failed");
                tokio::time::sleep(ACCEPT_RETRY).await;
                continue;
            }
        };
        tokio::spawn(hand_over(tcp, peer, acceptor.clone(), tx.clone()));
    }
}

async fn hand_over(
    tcp: TcpStream,
    peer: SocketAddr,
    acceptor: Option<TlsAcceptor>,
    tx: mpsc::Sender<(Conn, SocketAddr)>,
) {
    let conn: Conn = match acceptor {
        None => Box::new(tcp),
        Some(acceptor) => {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => Box::new(stream),
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
    let _ = tx.send((conn, peer)).await;
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
    let payload: Bytes = match to_bytes(body, state.max_bytes).await {
        Ok(payload) => payload,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };

    let (reply, outcome) = oneshot::channel();
    let message = Message {
        payload,
        attrs: attributes(&state, &headers, peer),
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

fn attributes(state: &ListenState, headers: &HeaderMap, peer: Peer) -> Attributes {
    let mut attrs = Attributes::new();
    attrs.insert("http.path", state.path.clone());
    attrs.insert("http.remote_addr", peer.0.to_string());
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
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn busy() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, [(RETRY_AFTER, "1")]).into_response()
}
