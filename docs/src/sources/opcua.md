# OPC UA source

The `opcua` source connects to one OPC UA server, subscribes to a list of node ids, and emits one JSON message per publish response, grouping that response's data changes. Built as the `thadam-opcua` crate on [`async-opcua`](https://crates.io/crates/async-opcua) 0.19 (client only, pure Rust, MPL-2.0 — an owner decision; never `open62541`).

**The source is off by default.** Build the CLI with `--features opcua`:

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

Unknown keys are rejected at every level. Durations are whole seconds (`30`) or a unit string (`500ms`, `10s`, `5m`, `1h`). Limits are enforced by `thadam validate`.

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoint` | URL | — | **Required.** The server endpoint, `opc.tcp://host:port`. The port must be explicit. |
| `security_policy` | see below | — | **Required, no default.** `none`, `basic256sha256`, `aes128_sha256_rsaoaep`, or `aes256_sha256_rsapss`. Picking `none` must be explicit, like WAL `on_full`. |
| `security_mode` | `none`, `sign`, `sign_and_encrypt` | — | **Required, no default.** Validated against the policy: `none` policy requires `none` mode (there is no channel crypto to sign with); a secured policy requires `sign` or `sign_and_encrypt`. |
| `auth` | object | `{kind: anonymous}` | `kind: anonymous`, or `kind: username` with `username` and `password`. The password is a string, so `${env:}`/`${file:}` placeholders work; it is redacted in debug output. Certificate (`x509`) token authentication is step 2. |
| `cert` | object | none | Client certificate material, **required when `security_policy` is not `none`**, rejected otherwise. Keys: `cert` (DER or PEM certificate), `key` (PEM private key), `pki_dir` (PKI store: trusted server certificates go under `<pki_dir>/trusted`, first-seen rejections under `<pki_dir>/rejected`), and `trust_server` (boolean, default `false`). **`trust_server: true` trusts and skips verifying any server certificate — development only.** Relative `cert`/`key` paths resolve inside `pki_dir`. |
| `mode` | `subscribe` | `subscribe` | Monitored items on one subscription. The only mode; polling may follow. |
| `subscribe` | object | see below | Subscription timing. |
| `reconnect` | object | see below | Reconnect behavior. |
| `nodes` | list | — | **Required, non-empty.** Nodes to monitor. |

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

A node the server refuses at subscription time (for example `BadNodeIdUnknown`) logs a warning naming it and simply never reports; everything else keeps flowing.

## Payload format

One publish response becomes one message, grouping that response's data changes:

```json
{"temp": 71.5, "cycles": 1042, "ts_us": 1791628800123456}
```

- Each changed node's `name` maps to its value, or `null` (see [Bad and uncertain statuses](#bad-and-uncertain-statuses) and the type mapping below). A publish response carries only *changed* nodes.
- `ts_us` is the timestamp of the newest change in the response, in microseconds since the Unix epoch: the newest source timestamp when any change carries one, else the newest server timestamp, else the time the response was received.
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
| `opcua.subscription_id` | The server-assigned subscription id; changes after every reconnect. |
| `topic` | `opcua/<endpoint host>/<subscription_id>`. Dataset `match.topic` filters can select on it. |
| `opcua.bad_status` | Comma-separated `name=StatusCode` entries for changes with a non-Good status. Present only when there is at least one. |
| `opcua.unsupported` | Comma-separated `name=Type` entries for values outside the mapping. Present only when there is at least one. |
| `opcua.dropped` | Running total of publish responses dropped client-side because the pipeline fell more than the channel's 64-response buffer behind. Present once nonzero. |

### Bad and uncertain statuses

A data change whose OPC UA status code is not Good (Bad *or* Uncertain severity) is never silently lost:

- its value is `null` in the payload, and
- the message's `opcua.bad_status` attribute names it, e.g. `temp=BadSensorFailure`.

The message is still emitted, so consumers can tell a bad reading (`null` plus the attribute) from a node that simply did not change.

## Delivery semantics

Subscriptions are **at-most-once per publish response** (`ack_mode: none`). OPC UA has no application-level acknowledgement for published data, so there is nothing to redeliver or suppress.

| Event | Result |
|---|---|
| Publish response received and committed | Recorded once. |
| Crash after a publish response, before it is committed | That response's changes are lost. The server keeps publishing; the next response brings the current values. |
| Response arrives while the pipeline is backlogged beyond the client buffer | The response is dropped and counted in `opcua.dropped`; the server's per-node `queue_size` queues cover shorter stalls on their own. |
| Connection lost | Transient error internally: the source reconnects and re-creates the subscription (see below). |

Because every response is re-derived from live server state, losing one shows up as a gap in `ts_us` on the far side, not a hole in a sequence.

## Errors and retries

| Failure | Classification | Behavior |
|---|---|---|
| Connection refused, unreachable, discovery or handshake failure | Transient | Reconnect with jittered backoff, pacing by `max_handshakes_per_minute`. |
| Established connection drops (network, server restart) | Transient | The same supervisor reconnects and re-creates subscription and monitored items. |
| Rejected anonymous/username token, bad channel certificate | **Fatal** (`auth.failed`) | Surfaced to the runtime after queued messages drain. No retry storm: wrong credentials only get more timing samples for the attacker. |
| Monitored item refused by the server (bad node id at runtime) | Warning only | The item never reports; the rest of the subscription flows. |
| Invalid configuration | Fatal at build | Reported by `thadam validate`. |

Backoff starts at `min_backoff` and doubles (jittered in the upper half of the window) after each failed handshake, capped at `max_backoff`; a session that established resets it. Independently of backoff, **every** connection attempt — initial or retry — spends a token from the `max_handshakes_per_minute` limiter, so total handshake cadence is always bounded.

## Security notes

Security policies `basic256sha256`, `aes128_sha256_rsaoaep`, and `aes256_sha256_rsapss` are offered and `sign`/`sign_and_encrypt` are enforced client-side against the server's discovery list. Prefer the two AES policies — they are the OPC Foundation's currently-sanctioned suites and exercise less RSA than Basic256Sha256.

**RSA advisory RUSTSEC-2023-0071 (Marvin timing attack).** `async-opcua-crypto` pulls `rsa` 0.9, whose RSA key-exchange operations are not constant-time. A remote observer who can precisely time many of our handshake decryptions could, in principle, work toward private key material. `deny.toml` carries a time-boxed exception for this advisory with a review date of **2027-04-01**, and the intended fix is migrating to `rsa` 0.10 once `async-opcua` adopts it. The practical posture in the meantime:

- RSA is used only in secure-channel setup, never per message.
- `reconnect.max_handshakes_per_minute` caps the collection rate of timing samples — an attacker cannot make us handshake faster than that.
- Authentication failures (tokens and channel certificates) are fatal, so a misconfiguration does not turn into an unattended sampling oracle.
- Username tokens still encrypt the password with the server's certificate per the profile rules; avoid `kind: username` over `security_policy: none`, where the password crosses the wire unencrypted.

Everything crypto in this tree is pure Rust (RustCrypto AES/CBC/HMAC/SHA + `rsa`, `x509-cert`); there is no OpenSSL or `aws-lc-rs` in the dependency graph.

## Shutdown

On shutdown the session task is stopped and the connection closes. A clean `DeleteSubscriptions` is not attempted; the server reclaims the subscription when its lifetime expires.
