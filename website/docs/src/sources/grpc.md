# gRPC source

**Status: Available.** The `grpc` source captures messages sent by gRPC clients to a Thadam ingest service. Each `Publish` call, or each message of a `PublishStream` call, becomes one Thadam message. The response is sent only after the message is durably committed, so an `OK` means the message is safe.

Built as the `thadam-grpc` crate with `tonic` and `prost` (server only), `rustls` with the `ring` provider for TLS, and `protox` for compiling the proto at build time. No `protoc` binary is needed, and no `aws-lc-sys` or OpenSSL.

```yaml
sources:
  - id: telemetry-ingest
    kind: grpc
    config:
      bind: 0.0.0.0:50443
      bearer_token: ${env:THADAM_INGEST_TOKEN}
      tls: { cert: /etc/thadam/server.pem, key: /etc/thadam/server.key }
      max_message_bytes: 1048576
      queue_capacity: 1024
```

## Configuration

Unknown keys are rejected at every level. Limits are enforced by `thadam validate`.

| Key | Type | Default | Description |
|---|---|---|---|
| `bind` | socket address | `127.0.0.1:50051` | Address to bind. Non-loopback addresses require `bearer_token`. |
| `bearer_token` | string | none | Calls must send `authorization: Bearer <token>` metadata. Non-empty, no control characters. Redacted in logs and debug output. |
| `tls` | mapping | none | `{ cert, key }` PEM file paths (leaf-first chain, private key). Enables TLS. Both keys are required. |
| `max_message_bytes` | integer (bytes) | `1048576` (1 MiB) | Largest accepted `payload`. Larger messages get `INVALID_ARGUMENT`. Minimum 1. |
| `queue_capacity` | integer | `1024` | Messages accepted but not yet committed. Above this, calls answer `RESOURCE_EXHAUSTED`. Minimum 16. |

The JSON Schema for this block is closed (`additionalProperties: false`) and is exported with the other connector schemas.

## Message attributes

| Attribute | Value |
|---|---|
| `grpc.method` | `Publish` or `PublishStream`. |
| `grpc.stream` | The `stream` field of the request. |
| `grpc.content_type` | The `content_type` field, when non-empty. |
| `grpc.peer` | The peer socket address. |
| `grpc.attr.<key>` | One per entry in the request's `attributes` map. |

## Delivery semantics

**At-least-once from the client's point of view.** `OK` is sent only after the WAL commit. A client that gets `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, a deadline error, or a dropped connection must retry. A crash after the commit but before the response reaches the client causes a retry, so the same payload can be captured twice. Thadam does not deduplicate; handle duplicates downstream if they matter.

- `Publish` returns after its single message is committed.
- `PublishStream` acknowledgements are released in request order. The call returns `accepted` (the number received, which equals the number committed) after every message of the stream is committed. If the call fails midway, messages already committed stay committed, and the client does not know which ones; it should retry the whole stream only if its messages are deduplicable.
- Ack mode is `on_commit`. Ordering is preserved within one stream; across concurrent calls there is no ordering guarantee.

## Responses

| Status | When | Notes |
|---|---|---|
| `OK` | The message (or every message of a stream) is committed. | Sent only after the WAL commit. |
| `UNAUTHENTICATED` | `bearer_token` is set and the metadata is missing or wrong. | Compared in constant time over SHA-256 digests. |
| `INVALID_ARGUMENT` | `payload` is larger than `max_message_bytes`. | Rejected before the message is queued. |
| `DEADLINE_EXCEEDED` | A client stream stalled for more than the 30 s message-body read timeout. | Ends the stream. |
| `RESOURCE_EXHAUSTED` | The source queue is full. | The accept path never blocks on a full queue. |
| `UNAVAILABLE` | The message was abandoned by the runtime, or the source is shutting down. | Retry later. |

## Security

- **Bind address.** The default is loopback. Binding to a non-loopback address without `bearer_token` is rejected at validation time (TLS alone does not authenticate callers). Configuring `bearer_token` on a non-loopback address without `tls` logs a warning because the token travels in cleartext.
- **Bearer tokens.** Use `${env:}` or `${file:}` so the token stays out of the config file. Tokens are compared in constant time over SHA-256 digests (without leaking token length) and never logged.
- **TLS.** When `tls` is set, the server speaks TLS only, using rustls with the `ring` provider. No client-certificate authentication is offered yet. A client that does not trust the server certificate fails the handshake.
- **Size limits.** `max_message_bytes` bounds memory per message. The gRPC decoder is also limited to that size plus a small envelope allowance.
- **Connection handling.** Concurrent connections are capped at 1024 (at most 256 concurrent HTTP/2 streams per connection). The initial HTTP/2 preface must arrive within 10 s, HTTP/2 keep-alive pings run every 20 s with a 10 s timeout (60 s socket idle cap), and streaming message body reads time out after 30 s (`DEADLINE_EXCEEDED`).

## Shutdown

On shutdown the source stops accepting calls. Calls whose messages are still queued and not yet handed to the runtime are answered `UNAVAILABLE`. Messages in a batch the runtime is committing get their answer when that commit resolves.

## Proto contract

The service is defined in `crates/thadam-grpc/proto/thadam/ingest/v1/ingest.proto`:

```protobuf
syntax = "proto3";

package thadam.ingest.v1;

service Ingest {
  // Publishes one message. Returns OK only after the message is committed.
  rpc Publish(PublishRequest) returns (PublishResponse);

  // Publishes messages in request order. Returns after every message of the
  // stream is committed, with the number accepted.
  rpc PublishStream(stream PublishRequest) returns (PublishStreamResponse);
}

message PublishRequest {
  // Logical stream name, recorded as the `grpc.stream` attribute.
  string stream = 1;
  // The message body, one Thadam message.
  bytes payload = 2;
  // Recorded as the `grpc.content_type` attribute when non-empty.
  string content_type = 3;
  // Recorded as `grpc.attr.<key>` attributes.
  map<string, string> attributes = 4;
}

message PublishResponse {}

message PublishStreamResponse {
  // Messages of the stream that were committed. Equals the number received.
  uint64 accepted = 1;
}
```

## Product note

The product design documents (D-B2-4 and ARCHITECTURE.md line 49) place gRPC after the MVP. This source was added on explicit request and is documented here as a shipped connector.
