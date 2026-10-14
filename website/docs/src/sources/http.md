# HTTP source

**Status: Available.** The `http` source captures one message per HTTP exchange. It runs in one of two modes:

- **`listen`** (default): serves `POST {path}`; each request body is one message. The response is sent only after the message is durably committed, so a `204` means the message is safe.
- **`poll`**: periodically fetches one or more URLs; each successful response body is one message.

Built as the `thadam-http` crate with `axum` (server), `reqwest` (client, `default-features = false`, rustls with `ring` only), and `tokio-rustls` for TLS. No OpenSSL, no `aws-lc-sys`.

```yaml
sources:
  - id: robot-ingest
    kind: http
    config:
      mode: listen
      listen:
        bind: 0.0.0.0:8443
        path: /ingest
        bearer_token: ${env:THADAM_INGEST_TOKEN}
        tls: { cert: /etc/thadam/server.pem, key: /etc/thadam/server.key }
        extract_headers: [X-Robot-Id]
      max_message_bytes: 1048576
      queue_capacity: 1024
```

```yaml
sources:
  - id: vendor-status
    kind: http
    config:
      mode: poll
      poll:
        urls:
          - https://vendor.example/api/status
        headers:
          Authorization: Bearer ${env:VENDOR_TOKEN}
        interval: 30s
        timeout: 10s
        backoff_max: 5m
```

## Configuration

Unknown keys are rejected at every level. Durations are whole seconds (`30`) or a unit string (`500ms`, `10s`, `5m`, `1h`). Limits are enforced by `thadam validate`.

| Key | Type | Default | Description |
|---|---|---|---|
| `mode` | `listen` or `poll` | `listen` | Selects the mode. |
| `listen` | mapping | defaults | Used in `listen` mode. See below. |
| `poll` | mapping | — | **Required** in `poll` mode; must be absent in `listen` mode. |
| `max_message_bytes` | integer | `1048576` | Largest accepted request body (`413` above it) or response body (poll drops it as too large), in bytes (1 MiB). |
| `queue_capacity` | integer | `1024` | Messages accepted but not yet committed. Above this, `listen` answers `503`. Minimum 16. |

### `listen`

| Key | Type | Default | Description |
|---|---|---|---|
| `bind` | socket address | `127.0.0.1:8080` | Address to bind. Non-loopback addresses require `bearer_token` or `tls`. |
| `path` | string | `/ingest` | Absolute path that accepts `POST`. Must not contain whitespace, `?`, `#`, `{`, `}`, or `*`. |
| `bearer_token` | string | none | Requests must send `Authorization: Bearer <token>`. Redacted in logs and debug output. |
| `tls` | mapping | none | `{ cert, key }` PEM file paths (leaf-first chain, private key). Enables HTTPS. |
| `extract_headers` | list of strings | `[]` | Request headers copied into `http.header.<name>` attributes (names are lowercased). |

### `poll`

| Key | Type | Default | Description |
|---|---|---|---|
| `urls` | list | — | **Required, non-empty.** Absolute `http://` or `https://` URLs, each polled on its own schedule. Credentials, fragments, and query strings are never echoed in logs or errors. |
| `method` | `get` or `post` | `get` | Request method. |
| `body` | string | none | Request body; requires `method: post`. |
| `headers` | mapping | `{}` | Headers sent with every request. Values may be secrets (use `${env:}`). |
| `interval` | duration | `10s` | Time between successful polls of one URL. Must be > 0. |
| `timeout` | duration | `30s` | Per-request deadline covering connect, headers, and body. Must be > 0. |
| `backoff_max` | duration | `5m` | Longest wait after consecutive failures. Must be ≥ `interval`. |

Redirects are not followed, and proxy environment variables are ignored, so a poll only talks to the URL you configured.

## Message attributes

| Attribute | Mode | Value |
|---|---|---|
| `http.path` | listen | The request path. |
| `http.remote_addr` | listen | The peer socket address. |
| `http.content_type` | both | The `Content-Type` header, when present. |
| `http.header.<name>` | listen | One per name in `extract_headers`, when present. |
| `http.url` | poll | The polled URL with credentials and query removed. |
| `http.status` | poll | The HTTP status code of the successful response (`200`–`299`). |

## Delivery semantics {#delivery-semantics}

| Mode | Ack mode | Guarantee |
|---|---|---|
| `listen` | `on_commit` | **At-least-once from the client's point of view.** The `204` is sent only after the WAL commit. A client that gets a `503`, a timeout, or a dropped connection must retry. A crash after the commit but before the `204` reaches the client causes a retry, so the same payload can be captured twice. Thadam does not deduplicate; handle duplicates downstream if they matter. Acknowledgements are released in request order per batch. |
| `poll` | `on_receive` | **At-most-once.** A response is accepted into the queue as it arrives; there is no upstream acknowledgement. A crash between receive and commit loses that response. Polling is safe only when the upstream resource is idempotent and you accept that a lost response is picked up at the next poll (repeated polls return the same state). For data that must not be lost, use `listen` or a queue-backed source. |

Within one connection, `listen` responses are sent in request order. Across connections there is no ordering guarantee.

### Listen responses

| Status | When | Notes |
|---|---|---|
| `204 No Content` | The message is committed. | Only after the WAL commit. |
| `401 Unauthorized` | `bearer_token` is set and the header is missing or wrong. | Includes `WWW-Authenticate: Bearer`. The comparison is constant-time. |
| `413 Payload Too Large` | Body exceeds `max_message_bytes`. | Rejected before the body is buffered in full when `Content-Length` says so. |
| `503 Service Unavailable` | The queue is full or the message was abandoned by the runtime. | Includes `Retry-After: 1`. The accept loop never blocks on a full queue. |

A request that matches no route gets the usual `404`/`405` from the server and never reaches the queue.

## Security

- **Bind address.** The default is loopback. Binding to a non-loopback address without `bearer_token` or `tls` is rejected at validation time.
- **Bearer tokens.** Use `${env:}` or `${file:}` so the token stays out of the config file. Tokens are compared in constant time and never logged.
- **TLS.** When `tls` is set, the server speaks HTTPS only, using rustls with the `ring` provider. No client-certificate authentication is offered yet.
- **Secrets in poll.** Header values and bodies are never written to logs, attributes, or error messages. URLs are redacted to scheme, host, port, and path.
- **Size limits.** `max_message_bytes` bounds memory per request and per response. Chunked uploads and responses without `Content-Length` are counted as they arrive.
- **Connection handling.** TLS handshakes run in per-connection tasks with a timeout, so a slow client cannot stall accepting new connections.

## Shutdown

On shutdown the source stops accepting requests. Requests still queued and not yet handed to the runtime are answered `503`. Requests in a batch the runtime is committing get their answer when that commit resolves.
