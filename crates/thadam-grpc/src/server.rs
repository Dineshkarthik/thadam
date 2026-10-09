//! The tonic `Ingest` service. A request's messages are queued without
//! waiting; its response is sent only once every message is committed.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use thadam_core::{AckOutcome, Attributes, Shutdown};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
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

pub(crate) struct IngestService {
    queue: Arc<Shared>,
    token: Option<String>,
    max_bytes: usize,
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
        while let Some(message) = stream.message().await? {
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
    let mut builder = Server::builder();
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

    let service = IngestServer::new(IngestService {
        queue,
        token: config.bearer_token.clone(),
        max_bytes,
    })
    .max_decoding_message_size(max_bytes.saturating_add(ENVELOPE_ALLOWANCE));

    tasks.spawn(async move {
        let served = builder
            .add_service(service)
            .serve_with_incoming_shutdown(TcpIncoming::from(listener), async move {
                stop.triggered().await;
            })
            .await;
        if let Err(e) = served {
            tracing::error!(error = %e, "gRPC server stopped");
        }
    });
    Ok(local)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
