# OPC UA source

**Status: Available (behind the opt-in `opcua` Cargo feature).** The `opcua` source connects to one OPC UA server and reads a list of node ids. Built as the `thadam-opcua` crate on [`async-opcua`](https://crates.io/crates/async-opcua) 0.19 (client only, pure Rust, MPL-2.0; never `open62541`).

Two modes, chosen with `mode`:

- **`subscribe`** (default): one subscription over the nodes. The server pushes changes, and each publish response becomes one JSON message grouping that response's changes.
- **`poll`**: every `interval`, one Read service call reads all nodes, and each poll becomes one JSON message carrying every node.

**The source is off by default.** Build `thadam-cli` with the `opcua` Cargo feature:

```console
$ cargo run -p thadam-cli --features opcua -- run pipeline.yaml
```

`thadam validate` rejects `kind: opcua` without the feature.

```yaml
sources:
  - id: lineside-plc
    kind: opcua
    config:
      endpoint: opc.tcp://192.0.2.10:4840
      security_policy: aes256_sha256_rsapss
      security_mode: sign_and_encrypt
      auth:
        kind: username
        username: opcua_ro
        password: "${file:/run/secrets/opcua_ro_password}"
      cert:
        cert: own/cert.der
        key: private/key.pem
        pki_dir: /etc/thadam/opcua-pki
        trust_server_certs:
          - plc/server-cert.der
      subscribe:
        publishing_interval: 1s
        sampling_interval: 250ms
        queue_size: 10
      nodes:
        - node_id: "ns=2;s=Lineside/Temp"
          name: temp
        - node_id: "ns=2;i=1003"
          name: cycles
      reconnect:
        min_backoff: 1s
        max_backoff: 30s
        max_handshakes_per_minute: 12
```

## Configuration

Unknown keys are rejected at every level. Durations are whole seconds (`30`) or a unit string (`500ms`, `10s`, `5m`, `1h`). Limits and files are checked by `thadam validate`.

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoint` | URL | — | **Required.** The server endpoint, `opc.tcp://host:port`. The port must be explicit. |
| `security_policy` | see below | — | **Required, no default.** `none`, `basic256sha256`, `aes128_sha256_rsaoaep`, or `aes256_sha256_rsapss`. Picking `none` must be explicit, like WAL `on_full`. |
| `security_mode` | `none`, `sign`, `sign_and_encrypt` | — | **Required, no default.** Validated against the policy: `none` policy requires `none` mode (there is no channel crypto to sign with); a secured policy requires `sign` or `sign_and_encrypt`. |
| `auth` | object | `{kind: anonymous}` | `kind: anonymous`; `kind: username` with `username` and `password`; or `kind: certificate` with `cert` and `key` (see [Authentication](#authentication)). |
| `cert` | object | none | Client application certificate, **required when `security_policy` is not `none`**, rejected otherwise. See [Certificates and trust](#certificates-and-trust). |
| `mode` | `subscribe`, `poll` | `subscribe` | How values are read. See [Subscribe and poll](#subscribe-and-poll). |
| `interval` | duration | — | **Required with `mode: poll`** and rejected otherwise. How often every node is read. Must be > 0. |
| `subscribe` | object | see below | **Only with `mode: subscribe`**; rejected with `mode: poll`. |
| `reconnect` | object | see below | Reconnect behavior, both modes. |
| `nodes` | list | — | **Required, non-empty.** Nodes to read. |

### `subscribe`

| Key | Type | Default | Description |
|---|---|---|---|
| `publishing_interval` | duration | `1s` | How often the server publishes batches of data changes. Must be > 0. |
| `sampling_interval` | duration | `250ms` | How often the server samples each node. `0` means "as fast as the server supports". |
| `queue_size` | integer >= 1 | `10` | Per-node server-side queue per monitored item, `discard_oldest` on overflow. |

### `reconnect`

| Key | Type | Default | Description |
|---|---|---|---|
| `min_backoff` | duration | `1s` | First retry delay; doubles (jittered) with failed handshakes, up to `max_backoff`. |
| `max_backoff` | duration | `30s` | Longest retry delay. Must be >= `min_backoff`. |
| `max_handshakes_per_minute` | integer >= 1 | `12` | **Hard rate limit on connection handshakes**, evenly spaced with no bursts. This is a security mitigation, not just politeness: each handshake runs RSA operations, and the rate limit bounds the timing samples an observer can collect (see [Security notes](#security-notes)). |

### `nodes`

| Key | Type | Default | Description |
|---|---|---|---|
| `node_id` | string | — | **Required.** OPC UA node id: `ns=2;s=Temp`, `ns=2;i=1003`, `i=85`, `ns=2;g=<guid>`, or `ns=2;b=<base64>`. Unparsable ids are config errors. |
| `name` | string | — | **Required.** The JSON key for this node's value. Unique within the source. `ts_us` is reserved. |

A node the server refuses at subscription time (for example `BadNodeIdUnknown`) logs a warning naming it and simply never reports; everything else keeps flowing. In poll mode a node the server cannot read reports as a Bad status on every poll (see [Bad and uncertain statuses](#bad-and-uncertain-statuses)).

## Authentication

| `auth.kind` | Keys | Requirements |
|---|---|---|
| `anonymous` | none | None. The default. |
| `username` | `username`, `password` | The password is a string, so `${env:}` and `${file:}` placeholders work. It is redacted in debug output. Avoid over `security_policy: none`, where the password crosses the wire unencrypted. |
| `certificate` | `cert`, `key` | A secured `security_policy` (the user token is signed on the secure channel). `cert` is the user's X.509 certificate (DER or PEM) and `key` its private key (PEM, a file path only: inline key material is not accepted). Relative paths resolve against `cert.pki_dir`. Validated at config time: both files must exist and parse, and the key must match the certificate. The server must be configured to accept this certificate. |

```yaml
auth:
  kind: certificate
  cert: user/cert.der   # relative to cert.pki_dir
  key: user/key.pem
```

The user key is read from disk on each connection attempt and never logged. Errors name the file, not its contents.

A server that rejects the identity token or the user certificate is a fatal `auth.failed` error (see [Errors and retries](#errors-and-retries)).

## Certificates and trust

A secured `security_policy` needs the `cert` block. It holds the **client application certificate** the source presents on the secure channel, and it controls how the client decides whether to trust the **server**.

| `cert` key | Type | Default | Description |
|---|---|---|---|
| `cert` | path | — | **Required.** The client's application certificate (DER or PEM). Relative paths resolve against `pki_dir`. |
| `key` | path | — | **Required.** The client's private key (PEM). Relative paths resolve against `pki_dir`. |
| `pki_dir` | path | — | **Required.** The PKI store directory (layout below). |
| `trust_server_certs` | list of paths | `[]` | Server certificate files to **pin** (DER or PEM). Relative paths resolve against `pki_dir`. Each is copied into `<pki_dir>/trusted/` before connecting. Pinning is the recommended way to trust a server. |
| `accept_unknown_server_cert` | boolean | `false` | **INSECURE.** Trust any server certificate the first time it is seen and store it in `<pki_dir>/trusted/`. Any server that presents a certificate is accepted, including an impostor. Hostname and validity-period checks still run. Use only in development. Cannot be combined with `trust_server_certs`. |

### PKI layout

```text
<pki_dir>/
  client.der, client-key.pem   # cert and key from the cert block, if kept here
  trusted/                     # server certificates the client accepts
  rejected/                    # server certificates refused on first contact
```

- **Default: unknown server certificates are rejected.** A server whose certificate is not in `trusted/` (and not listed in `trust_server_certs`) is refused before any session is created. The refused certificate is written to `rejected/` so an operator can review it. Moving a file from `rejected/` to `trusted/` is the manual alternative to `trust_server_certs`.
- **Explicit entries win.** A certificate listed in `trust_server_certs` is copied into `trusted/` and removed from `rejected/` if it was there.
- **Refusing a server is fatal.** An untrusted server certificate ends the source with `auth.failed`; it is not retried, so a misconfigured trust store cannot turn into a stream of handshakes.
- **Hostname and validity are always checked.** Only the trust decision is relaxed, by `accept_unknown_server_cert`.

The config is checked at `thadam validate` time: the client certificate and key must exist, parse, and match; every `trust_server_certs` file must parse; the two trust options cannot be combined; and `pki_dir` must not be a regular file.

## Subscribe and poll

### Subscribe (`mode: subscribe`, default)

Monitored items on one subscription. The server reports changes, so a value that does not change produces no message. One publish response becomes one message.

### Poll (`mode: poll`)

Every `interval`, the source sends **one Read service call covering all nodes** and emits **one message carrying every node**, whether or not the values changed.

```yaml
sources:
  - id: tank-level
    kind: opcua
    config:
      endpoint: opc.tcp://192.0.2.20:4840
      security_policy: none
      security_mode: none
      mode: poll
      interval: 2s
      nodes:
        - node_id: "ns=2;s=Tank/Level"
          name: level
        - node_id: "ns=2;i=1003"
          name: cycles
```

Poll mode has no subscription, so the message attributes differ (see [Message attributes](#message-attributes)). A poll that the pipeline cannot take waits for it, instead of dropping: the interval already paces the source. A read that fails because the connection dropped ends the session, and the source reconnects under the same backoff and handshake limits as subscribe mode.

Poll mode suits slow-changing values and servers that cannot take subscriptions. Use subscribe mode for fast-changing values, where missing a change between polls would matter.

## Payload format

One message groups the values it carries:

```json
{"temp": 71.5, "cycles": 1042, "ts_us": 1791628800123456}
```

- Each carried node's `name` maps to its value, or `null` (see [Bad and uncertain statuses](#bad-and-uncertain-statuses) and the type mapping below). A subscribe-mode publish response carries only *changed* nodes; a poll carries every node.
- `ts_us` is the timestamp of the newest value in the message, in microseconds since the Unix epoch: the newest source timestamp when any value carries one, else the newest server timestamp, else the time the message was received (or, in poll mode, the poll time).
- Because the payload is ordinary JSON, dataset fields and the JSON decode step in the transform chain read it like any other JSON source.

### Type mapping

| OPC UA type | JSON |
|---|---|
| Empty variant, null String, null ByteString | `null` |
| Boolean | boolean |
| SByte, Byte, Int16..Int64, UInt16..UInt64 | number (i64/u64 exact) |
| Float, Double | number (f64). NaN and infinities have no JSON form and are *unsupported* (see below). |
| String | string |
| DateTime | RFC 3339 string, e.g. `2026-10-12T08:31:04.123456Z` |
| ByteString | base64 string (the OPC UA JSON encoding, Part 6) |
| Array of Boolean or of a mapped numeric type | array of the mapped scalars. Multi-dimensional arrays are flattened in wire order. An array with a NaN or infinite element is unsupported as a whole. |
| Guid, StatusCode, XmlElement, QualifiedName, LocalizedText, NodeId, ExpandedNodeId, ExtensionObject, nested Variant, DataValue, DiagnosticInfo, arrays of String/DateTime/ByteString | unsupported |

An unsupported value is never silently dropped: it becomes `null`, and the message's `opcua.unsupported` attribute lists it as `name=<Type>` (e.g. `pose=QualifiedName`).

## Message attributes

| Attribute | Value |
|---|---|
| `opcua.endpoint` | The configured endpoint URL. |
| `opcua.subscription_id` | Subscribe mode only. The server-assigned subscription id; changes after every reconnect. |
| `topic` | Subscribe mode: `opcua/<endpoint host>/<subscription_id>`. Poll mode: `opcua/<endpoint host>/poll`. Dataset `match.topic` filters can select on it. |
| `opcua.bad_status` | Comma-separated `name=StatusCode` entries for values with a non-Good status. Present only when there is at least one. |
| `opcua.unsupported` | Comma-separated `name=Type` entries for values outside the mapping. Present only when there is at least one. |
| `opcua.dropped` | Subscribe mode only. Running total of publish responses dropped client-side because the pipeline fell more than the channel's 64-response buffer behind. Present once nonzero. |

### Bad and uncertain statuses

A value whose OPC UA status code is not Good (Bad *or* Uncertain severity) is never silently lost:

- its value is `null` in the payload, and
- the message's `opcua.bad_status` attribute names it, e.g. `temp=BadSensorFailure`.

The message is still emitted, so consumers can tell a bad reading (`null` plus the attribute) from a node that simply did not change.

## Delivery semantics

| Mode | Guarantee |
|---|---|
| Subscribe | **At-most-once per publish response** (`ack_mode: none`). OPC UA has no application-level acknowledgement for published data, so there is nothing to redeliver or suppress. |
| Poll | **At-most-once per poll.** A poll that completed and was committed is recorded once; a poll that was never emitted is simply gone, and the next poll reads the current values. |

| Event | Result |
|---|---|
| Message received and committed | Recorded once. |
| Crash after a message, before it is committed | That message's values are lost. Subscribe mode: the server keeps publishing and the next response brings the current values. Poll mode: the next poll does. |
| Subscribe response arrives while the pipeline is backlogged beyond the client buffer | The response is dropped and counted in `opcua.dropped`; the server's per-node `queue_size` queues cover shorter stalls on their own. |
| Poll waits on a full pipeline | No drop: the poll is delayed until there is room. |
| Connection lost | Transient error internally: the source reconnects and re-creates the subscription, or resumes polling (see below). |

Because every message is re-derived from live server state, losing one shows up as a gap in `ts_us` on the far side, not a hole in a sequence.

## Errors and retries

| Failure | Classification | Behavior |
|---|---|---|
| Connection refused, unreachable, discovery or handshake failure | Transient | Reconnect with jittered backoff, pacing by `max_handshakes_per_minute`. |
| Established connection drops (network, server restart) | Transient | The same supervisor reconnects and re-creates the subscription, or resumes polling. |
| Rejected anonymous/username token or user certificate | **Fatal** (`auth.failed`) | Surfaced to the runtime after queued messages drain. No retry storm: wrong credentials only get more timing samples for the attacker. |
| Untrusted server certificate or client certificate refused by the server | **Fatal** (`auth.failed`) | As above. The refused server certificate is kept in `rejected/`. |
| Monitored item refused by the server (bad node id at runtime) | Warning only | The item never reports; the rest of the subscription flows. |
| Invalid configuration, unreadable or mismatched certificate or key | Fatal at build | Reported by `thadam validate`. |

Backoff starts at `min_backoff` and doubles (jittered in the upper half of the window) after each failed handshake, capped at `max_backoff`; a session that established resets it. Independently of backoff, **every** connection attempt — initial or retry — spends a token from the `max_handshakes_per_minute` limiter, so total handshake cadence is always bounded.

## Tested policies

The crate's end-to-end tests run against an in-process `async-opcua` server, not a PLC. They cover:

| Policy | Modes exercised | Auth exercised |
|---|---|---|
| `aes256_sha256_rsapss` (preferred) | `sign_and_encrypt` | anonymous, certificate, pinned and accepted server certificates |
| `aes128_sha256_rsaoaep` (preferred) | `sign_and_encrypt`, `sign` | anonymous |
| `basic256sha256` | `sign_and_encrypt` | username |
| `none` | `none` | anonymous; a wrong username is refused and fatal |

Also tested: an unknown server certificate is refused by default; a rejected user certificate and an untrusted client certificate are fatal and not retried; poll mode emits one message per poll with every node, reports a Bad value, and recovers after a server restart.

## Security notes

Security policies `basic256sha256`, `aes128_sha256_rsaoaep`, and `aes256_sha256_rsapss` are offered and `sign`/`sign_and_encrypt` are enforced client-side against the server's discovery list. Prefer the two AES policies — they are the OPC Foundation's currently-sanctioned suites and exercise less RSA than Basic256Sha256.

**RSA advisory RUSTSEC-2023-0071 (Marvin timing attack).** `async-opcua-crypto` pulls `rsa` 0.9, whose RSA key-exchange operations are not constant-time. A remote observer who can precisely time many of our handshake decryptions could, in principle, work toward private key material. `deny.toml` carries a time-boxed exception for this advisory with a review date of **2027-04-01**, and the intended fix is migrating to `rsa` 0.10 once `async-opcua` adopts it. The practical posture in the meantime:

- RSA is used only in secure-channel setup and in certificate user tokens, never per message.
- `reconnect.max_handshakes_per_minute` caps the collection rate of timing samples — an attacker cannot make us handshake faster than that.
- Authentication failures (tokens, user certificates, and channel certificates) are fatal, so a misconfiguration does not turn into an unattended sampling oracle.
- Server certificates are refused by default, so the client does not handshake with an unknown server's key unless an operator pinned it (or opted into `accept_unknown_server_cert`).

Everything crypto in this tree is pure Rust (RustCrypto AES/CBC/HMAC/SHA + `rsa`, `x509-cert`); there is no OpenSSL or `aws-lc-rs` in the dependency graph.

## Shutdown

On shutdown the session task is stopped and the connection closes. A clean `DeleteSubscriptions` is not attempted; the server reclaims the subscription when its lifetime expires. Poll mode has no subscription to clean up.
