# NATS JetStream sink

**Status: Implemented.** The `nats` kind is a batch sink that publishes one dataset's events to a NATS JetStream stream — one acknowledged message per event, as JSON, deduplicated by the event id. It complements the file-oriented sinks: use it to fan captured events back out to low-latency subscribers (dashboards, alerting, downstream services) directly from the replay edge.

## How it works

1. The runtime forms batches on WAL frame boundaries and hands each batch to the sink with a stable batch id.
2. The sink renders each event's publish subject from the `subject` template, encodes the event as one JSON document, and publishes it with a `Nats-Msg-Id` header set to the event id (hex). At most `max_in_flight` publishes wait for their acknowledgement at any moment; `write` returns when every event is acknowledged or individually rejected.
3. A bounded stream with a positive `duplicate_window` deduplicates `Nats-Msg-Id` repeats server-side, which is exactly what the runtime's retries produce. `open` probes the stream and reports [`idempotent_batch`](../concepts/delivery-guarantees.md) only when the window is positive — the deployed stream, not the configuration, decides what `/status` and metrics report. A window of zero reports `at_least_once`.
4. The stream must already exist; a missing stream at `open` is a configuration error unless `create_stream: true`. Thadam deliberately never provisions infrastructure behind your back.

## Configuration

```yaml
sinks:
  - id: nats-out
    kind: nats
    config:
      url: nats://nats.internal:4222     # or tls:// for TLS
      subject: thadam.{dataset}.{robot}   # template; {dataset} + identity level names
      stream: THADAM                      # must exist unless create_stream: true
      create_stream: false               # default; create the stream at open when missing
      duplicate_window: 2m               # window of a created stream (an existing stream keeps its own)
      token: "${file:/run/secrets/nats_token}"        # one credential: token, ...
      # user: robot                      # ... user + password (both required), ...
      # creds_file: /etc/thadam/nats.creds # ... or a NATS credentials file
      tls:                               # optional; setting it requires TLS
        ca_file: /etc/thadam/ca.pem       #   replaces the platform trust roots
        client_cert: /etc/thadam/cert.pem #   client certificate (with client_key)
        client_key: /etc/thadam/key.pem
      payload: json                      # the only payload
      request_timeout: 10s               # connect and per-request timeout (default)
      max_in_flight: 256                 # publishes awaiting their ack (default)
      datasets: [imu]                    # one dataset per sink entry
```

Notes:

- Secrets are plain strings **already resolved** by `${env:}`/`${file:}` interpolation ([secrets](../configuration/index.md#secrets)); the sink wraps them in redacting types and never logs them.
- `subject` templates interpolate at whole-token granularity: `thadam.{dataset}.{robot}` is valid, `pre-{robot}` is refused at validation time (it could never match a stream's wildcard subject). Non-`dataset` placeholders must be identity levels of the selected dataset — a typo fails at `validate`/`open`, not per event. Rendered values must be usable subject tokens (no `.`, `*`, `>`, or whitespace); an event that fails rendering is rejected individually to quarantine.
- A `kind: nats` sink publishes exactly one dataset; add one sink entry per dataset. The restriction lifts when the batch contract groups events per dataset (ADR-0002 open work).
- TLS uses rustls with the `ring` provider only (ADR-0020): `ca_file` replaces the platform roots and, with `client_cert`/`client_key`, enables mutual TLS. Setting `tls:` (or a `tls://` URL) requires TLS.

## The JSON envelope

Each event publishes as one JSON object merging three regions; later regions never overwrite an earlier key, so a record field can never shadow lineage:

```json
{
  "_event_id": "0123456789abcdef0011223344556677",
  "_ingest_time": "2026-10-10T06:00:00.000000001Z",
  "_event_time": "2026-10-10T05:59:59.500000000Z",
  "_quality_status": "good",
  "robot": "r7",
  "temp": 43.25
}
```

- `_event_id` — the event id, lowercase hex; also the `Nats-Msg-Id` header.
- `_ingest_time` — edge receipt time, RFC 3339 with nanosecond precision, always present. (Integer epoch nanoseconds would silently lose precision through a JSON number beyond 2^53, so times are text.)
- `_event_time` — the corrected time if a correction was applied, else the source time when its domain is UTC, else `null` (the same rule the Parquet layout uses).
- `_quality_status` — `good` | `uncertain` | `bad`.
- Identity levels appear as `{"level": "value"}` columns; the dataset's record fields follow. Field values map naturally: integers stay numbers, floats are numbers (`null` when NaN or infinite — JSON has neither), strings stay strings, `bytes` values are lowercase hex, timestamps are RFC 3339 text, lists and records nest.

`payload: json` expects decoded record events; a raw-payload or object-reference event is rejected individually.

## Delivery guarantees

- **Reported class.** `open` probes `STREAM.INFO`: `duplicate_window > 0` reports `idempotent_batch`, otherwise `at_least_once`. Operators control the class through the stream definition; what you deployed is what you see.
- **Retries.** Publish timeouts, no-responders, and dropped connections are transient: the runtime retries the same batch with the same batch id, and the `Nats-Msg-Id` duplicate window makes those resends idempotent server-side. Failed credentials and permission violations are fatal (the component is marked `Failed`). A message exceeding the server's `max_payload` or the stream's `max_msg_size` is a permanent per-event rejection — the event goes to quarantine, the batch proceeds.
- **Permission violations.** A denied publish is never acknowledged: the server only announces `Permissions Violation` on the connection. The sink records that announcement, so the acknowledgement timeout that follows is reported as fatal (`auth/auth.forbidden`) instead of being retried forever. Credentials the server refuses at connect time are fatal too.
- **Restarts.** The client keeps publishing across a broker restart; an ack that never arrives during an outage counts as unknown-outcome (transient), exactly the case the duplicate window exists for.
- **Window limit.** Deduplication only covers the stream's `duplicate_window` (2 minutes by default). A retry that arrives after the window can store an event twice; keep the window longer than your longest expected outage.

## Integration tests

Docker-gated tests run the sink against a real broker (`nats:2.12.2` pinned by tag, run with `-js`; public Docker Hub only, never a mirror; re-pin with `THADAM_TEST_NATS_IMAGE`):

```text
cargo nextest run -p thadam-nats --run-ignored only -E 'binary(docker_nats)'
```

They cover publish-and-consume-back (subjects, envelope, headers), broker stop/start recovery with exactly-once accounting, retried batches deduplicating by `Nats-Msg-Id`, a missing stream failing `open` with a configuration error, delivery-class probing against an operator-zeroed window, `create_stream`, per-event rejection (unrenderable subject, payload over the server's `max_payload`, message over the stream's `max_msg_size`), no responders classified transient, token and user/password credentials with wrong credentials and denied publishes classified fatal, and mutual TLS with a CA and client certificate. Set `RUST_LOG=debug` for sink logs during the run.

Each test starts its own container (labelled `thadam-nats-test`) and removes it when the test ends, including on panic. Containers that needed a config file (auth, TLS) get it through `docker cp` before they start; no host path is mounted. To clean up after a killed run: `docker rm -f $(docker ps -aq --filter label=thadam-nats-test)`.
