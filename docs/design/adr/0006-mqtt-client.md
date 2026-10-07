# ADR-0006: MQTT client

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (spike S2 evidence and recommendations folded in; accepted by the owner with the EMQX 5.8.x pin)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"): keep `rumqttc` 0.25.1, pinned, behind `thadam-mqtt`, with the wrapper rules below. Spike S2 reported (`docs/design/spikes/S2-mqtt.md`) against Mosquitto 2.1.2 and EMQX 5.8.6. In the same decision the owner settled the EMQX licensing question: CI, tests, and documentation use the last open-source EMQX build, the 5.8.x line, and newer Enterprise-licensed images are not used (see "Broker versions for CI, tests, and documentation"). The QoS 2 scope is accepted as written in this ADR (claims limited to QoS 0 and 1; QoS 2 and MQTT 5 experimental). The S2b follow-up (a run of the S2 harness against the community fork `rumqttc-next`, at most one day, recommended before the pinned-upstream path is relied on long term), the remaining unticked items in "Evidence and validation required", and the PR12 conformance suite are implementation-time verification, not blockers. The decision to keep `rumqttc` stands only while the PR12 conformance suite passes in CI against both brokers.

## Context

MQTT is the only live source in the MVP (milestone M1). The `thadam-mqtt` crate implements the pull `Source` contract from ADR-0002 on top of an MQTT client library. Three properties of the source decide most of the design:

- **Ack after commit.** A message is acknowledged to the broker (PUBACK for QoS 1) only after its WAL commit (ADR-0004), in receive order, because MQTT requires PUBACKs in receive order. This is decision D7.
- **Liveness under backpressure.** If the client stops polling its network loop, it stops sending PINGREQ (the keepalive ping), and the broker disconnects it after about 1.5 times the keepalive interval. Backpressure therefore has to work by withholding PUBACKs while the client keeps polling.
- **Throughput is bounded by the broker's window.** With `ack_mode: on_commit`, one connection carries roughly the broker's inflight window divided by commit latency. Mosquitto's `max_inflight_messages` defaults to 20 and EMQX's `max_inflight` to 32 (both verified in S2), so a 100 ms commit caps one connection near 200 to 320 messages per second.

The library choice also has a maintenance dimension. The ecosystem maturity review (as checked 2026-10-07) found that `rumqttc`, which fits the design exactly, shows declining activity.

Candidates, as checked 2026-10-07 (ecosystem maturity review section 4, refined by S2 section 7):

- **`rumqttc` 0.25.1.** Released 2025-11-21 (0.24 in 2024-02, 0.25 in 2025-09). Apache-2.0, MSRV 1.64, about 2.1 million recent downloads (8.8 million total). Manual acknowledgement is confirmed in source and in S2: `MqttOptions::set_manual_acks` and `AsyncClient::ack(&Publish)`. MQTT 5 lives in a separate `v5` module. S2 recomputed the maintenance numbers from a clone at upstream HEAD (2026-05-01): 3 non-merge commits since 2025-10-07 (against 16 in the previous 12 months, 3 distinct authors), and 101 open issues plus 82 open pull requests (GitHub's "183 open issues" counts both). The 3.1.1 path is about 5,800 lines, the `v5` module about 8,300, with no `unsafe` code. Test coverage is thin: 40 unit tests on the 3.1.1 side and 16 integration tests against an in-process mock broker, with nothing for cancel-safety, keepalive under backpressure, ack ordering, or session resume against a real broker; the last published coverage figure (36.1%, April 2025) is stale. Maintenance risk raised to high-medium.
- **`rumqttc-next` (community fork, `thehouseisonfire/rumqtt`).** Published as `rumqttc-next`, `rumqttc-v4-next`, and `rumqttc-v5-next`, all 0.34.0, updated 2026-08-13, created February to March 2026, about 145,000 recent downloads, MSRV 1.88, 27 stars and 4 open issues or pull requests. Its README says it started "in response to a period of upstream inactivity", with separate 3.1.1 and 5 clients, tracked operation outcomes, and manual acknowledgements; its changelog moves fast (0.27.0 on 2026-02-19 to 0.34.0 on 2026-08-13). **The S2 harness was not run against it**; this is metadata only.
- **`paho-mqtt` 0.14.0** (2026-03). Crate license EPL-2.0, wrapping the Paho C library (EPL-2.0 and EDL-1.0). Default features build the C library and need CMake, a C compiler, and OpenSSL (`vendored-ssl` is optional). Strong MQTT 5 support, Eclipse governance, moderate release cadence.
- **`ntex-mqtt` 9.1.1** (2026-10). Very active, MIT or Apache-2.0, client and server, MQTT 3.1.1 and 5. Tied to the ntex framework (Tokio compatibility through `ntex-rt` is unconfirmed **[V]**). MSRV 1.97.
- **`mqtt-endpoint-tokio` 0.6.6.** MIT, small adoption.

### Evidence from spike S2

All numbers come from recorded runs in `spikes/s2-mqtt/results/` and are summarized in the report. The environment was an Apple Silicon (arm64) laptop with the brokers in a Docker Desktop virtual machine behind a port forwarder; commit latency was simulated, not real fsync; each throughput cell ran once. The ratios are transferable; absolute rates are machine-specific.

1. **The ack-window ceiling is real, and the model predicts it.** With manual acks delayed by commit latency `L`, one connection sustains about `window / (L + RTT)` messages per second. Measured against the nominal `window / L`: 84 to 87% at `L` = 10 ms and 95 to 98% at 50 to 100 ms, on both brokers; with the measured ack delay and a 0.16 ms RTT the model lands within 1 to 6% on Mosquitto. A Mosquitto default window of 20 gives about 1.7 thousand messages per second at 10 ms, about 384 at 50 ms, and about 195 at 100 ms. At `L` = 0 the limit is the client and broker, roughly 62 to 90 thousand messages per second per connection on that machine.
2. **The 3.1.1 window is the broker's setting, not the client's.** `MqttOptions::inflight` is the *outgoing* publish window; it neither bounds nor informs what the broker sends. The observed maximum unacknowledged count equalled the broker setting exactly (20, 100, 1000). The earlier design statement "queue capacity equals the configured inflight value (3.1.1)" is wrong. MQTT 5 `receive_maximum` works as a client-set window (the broker stopped at the declared 10 against its own 1000), and can only lower the window.
3. **Backpressure by withholding acks is correct and safe.** While `poll()` kept running and acks were withheld for 3 times the keepalive (15 s at 5 s), the connection stayed up (0 reconnects), PINGREQ and PINGRESP flowed (3 each), and the broker stopped sending at exactly its inflight limit. After release everything was delivered with 0 loss and 0 duplicates, on both brokers.
4. **Stopping the poll loop is fatal, and so is blocking it.** With no polling, Mosquitto closed the connection 7.3 s after the last client packet (keepalive 5 s) and EMQX after 10.0 s (2.0 times, although its `keepalive_multiplier` is 1.5). A bounded queue smaller than the broker window, written to with `send().await` from the poll task, caused the same disconnect plus a **deadlock**: the poll task blocked on the full queue, the acknowledgement task blocked on `client.ack().await` against a full request channel that only the blocked poll task could drain, and 91 of 12,750 messages were delivered. A queue equal to the window behaved correctly, but 3.1.1 gives the client no way to know the window.
5. **`EventLoop::poll` is not cancel-safe.** Random drops on a healthy socket (about 1.2 to 1.3 million dropped polls per broker, 540 thousand messages) lost nothing, but that method cannot reach the write-side await points. With the socket write side backed up, a poll-once-then-drop style emitted 300 `PubAck` events and **0 reached the wire** (no error, healthy-looking connection), and a plain 50 ms `timeout` lost 9 (Mosquitto) and 12 (EMQX) of 300. This matches upstream issue #930. Outbound requests including acks also flush only while `poll` runs.
6. **Payload-stripped acks work** (QoS 1 and 2, both brokers, MQTT 3.1.1 and 5). Holding 1000 unacknowledged 64 KiB messages cost 1.9 MiB of resident memory stripped against 79.7 MiB retained. The ack path reads only `qos` and `pkid`.
7. **Ack order on the wire equals issue order.** In-order issue gave 0 descents in 3000 PUBACKs. Deliberately shuffled acks went out shuffled and both brokers tolerated it, so ordering is entirely the application's job (upstream issue #846).
8. **Persistent sessions behave as designed, with sharp edges.** After `kill -9`, exactly the 20 unacknowledged in-flight messages were redelivered with DUP and none of the acknowledged ones. `session_present` was reported accurately in all five observed cases. The edges: (a) `rumqttc` does not resubscribe after session loss (0 deliveries until `subscribe` is called again); (b) **acks issued with pre-loss handles acknowledge the wrong messages after session loss**, silently losing all 10 new messages in the E1 test; (c) with the default 10 KiB `max_incoming_packet_size`, a single oversized message makes the codec fail the connection, and with a persistent session the broker redelivers it after every reconnect, giving 116 reconnects in 12 s (Mosquitto) and 114 (EMQX) with the message behind it never delivered; (d) when a broker queue limit is exceeded, the loss is silent to the publisher (Mosquitto drops the newest, EMQX the oldest); and (e) EMQX in its default configuration loses persistent sessions on a broker restart (`session_present` was false), while Mosquitto with persistence on kept them.
9. **TLS dependency check.** Default `rumqttc` features resolve `aws-lc-rs`. With the feature set below, `cargo tree -i` for `aws-lc-sys`, `aws-lc-rs`, `openssl-sys`, and `native-tls` all report no match, and `ring` 0.17.14 is present, which satisfies the ADR-0020 and `deny.toml` bans. A real TLS or mTLS handshake was not exercised.

## Decision

### Library and feature set

**Keep `rumqttc` 0.25.1 for the MVP MQTT source, pinned (`=0.25.1`), behind `thadam-mqtt`, with no `rumqttc` type in any public API.** The pinned feature set, from the S2 spike:

```toml
rumqttc = { version = "=0.25.1", default-features = false, features = ["use-rustls-no-provider"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["ring", "tls12"] }
```

`use-rustls-no-provider` was added in 0.25.1 for exactly this purpose. Do not fork now. The library does what the design needs (ordered manual acks, `Bytes` payloads, stripped acks, accurate `session_present`, working keepalive when polled), the ceiling is predictable, and every failure mode found has a wrapper-level mitigation except oversize skipping. Maintenance risk is real, but the 3.1.1 surface (about 5,800 lines, no `unsafe`, a small dependency set) is small enough to own. A fork or switch is gated on the explicit triggers below.

**The community fork `rumqttc-next` is a noted alternative, not adopted.** It is young (created February to March 2026) and the S2 harness was not run against it. S2b (a run of the harness against `rumqttc-v4-next` 0.34, at most one day, with adapter code because the API differs) should happen before committing to the pinned-upstream path long term, and it feeds exit trigger 5.

### Protocol version

**MQTT 3.1.1 (`protocol: v311`) is the supported default for the MVP. `protocol: v5` is accepted but marked experimental.** Evidence for MQTT 5: `receive_maximum` works (the broker stopped at the client-declared 10), stripped acks and persistent sessions with DUP redelivery work, keepalive works. Evidence against: the `v5` module is about 8,300 lines with 37 unit tests and no integration tests, the Q1 to Q6 matrix was not repeated on it, `set_keep_alive` panics below 5 s (the 3.1.1 module only below 1 s), and the same cancel-safety and oversize hazards apply because the code paths are shared. It becomes supported only after the PR12 conformance suite runs on it. Its main benefit, a client-controlled window, is real and is a good post-MVP step.

### Acknowledgement and ordering (D7)

- `ack_mode` defaults to `on_commit` (the owner's B3 decision of 2026-10-07 leaves this default in place and does not assume producer batching; S2 confirms it is the only mode that gives at-least-once across a Thadam crash). The SourceDriver completes each `AckHandle` strictly in receive order after the corresponding WAL commit. Filtering happens after acknowledgement (D1), so it never affects acknowledgement.
- `on_receive` is opt-in and is documented as at-most-once across a Thadam crash. `none` exists for sources that cannot acknowledge.
- The `AckHandle` holds a payload-stripped descriptor (`pkid`, `qos`) tagged with the connection epoch (rule 4) and hands it to an ack sender that calls `client.ack()` in order. Confirmed by S2 for QoS 1 and 2. Acks flush only while `poll` runs, so the shutdown sequence keeps the poll task running until the last ack has been written and then sends DISCONNECT.

### Wrapper rules (required)

S2's headline lists five wrapper rules; its section 8.2 lists eight items. Rules 1 to 5 are the headline rules, and items 6 to 8 are configuration requirements.

1. **The poll task only ever awaits `EventLoop::poll()`.** It is never raced (`select!`, `timeout`, `now_or_never`) against anything. Shutdown is a token check between polls. A test or lint in PR12 fails on any `select!`, `timeout`, or `now_or_never` around `poll`. (Evidence 5.)
2. **The poll task never blocks on downstream, and the incoming queue is never bounded below the broker's window.** The incoming queue for QoS 1 and 2 is unbounded in count but byte-accounted; the broker's window bounds it. There is no `send().await` into a bounded queue from the poll task. A hard guard *disconnects* (and does not block) if unacknowledged messages exceed a configured multiple of the observed window. (Evidence 4. This corrects the earlier design, which sized the queue to the configured inflight value.)
3. **The 3.1.1 window is the broker's, not `MqttOptions::inflight`.** The configuration key is named for what it is (for example `expected_broker_window`), is used only for the memory estimate and `thadam validate`, and the **observed** maximum unacknowledged count is exposed as a metric, because it equalled the broker window exactly in every run. For v5, `receive_maximum` is client-settable and honored.
4. **Connection-epoch-tagged ack handles.** The epoch is bumped on every `ConnAck`. Acks from older epochs are dropped and counted. The cost is extra DUP redelivery after a reconnect with `session_present = true`, which at-least-once already tolerates. (Evidence 8b: stale acks after session loss silently lost data.)
5. **On every `ConnAck`: if `session_present = false` while a session was expected, emit `SESSION_LOST` (a warning, a metric, and a gap marker), bump the epoch, and re-send SUBSCRIBE.** `rumqttc` will not resubscribe. (Evidence 8a, 8e.)
6. **Set `max_incoming_packet_size` from `max_message_bytes` and align the broker limit** (Mosquitto `message_size_limit` is unlimited by default; EMQX `mqtt.max_packet_size` defaults to 1MB). The default of 10 KiB turns one large message into an infinite reconnect loop. An in-client counted drop of an oversized packet is **not possible without patching the codec**, so the earlier statement that oversized messages are "rejected at the source with a counted drop" does not hold for MQTT. (Evidence 8c.)
7. **Acks hold only `(pkid, qos)`**, and the ordered ack tracker stays (Evidence 6 and 7).
8. **Set `NetworkOptions::connection_timeout` explicitly:** it also serves as the 5 s flush timeout, and a `FlushTimeout` on a slow link triggers disconnects. `keep_alive` must be at least 1 s for v4 and 5 s for v5; recommend 30 to 60 s. The broker's grace differs: Mosquitto about 1.5 times (7.3 s observed at 5 s), EMQX 2.0 times observed.

### Queueing and QoS 0

- **QoS 1 and 2 with `on_commit`.** Backpressure is withholding acks while polling continues (rules 1 and 2). Source-side memory is bounded by the broker's window times `max_message_bytes`, plus the guard in rule 2. In 3.1.1 that window is the broker's setting; in v5 it is `receive_maximum`. `thadam validate` uses `expected_broker_window` for the memory estimate and warns if the observed maximum later exceeds it.
- **QoS 0.** There is no flow control. The queue is bounded by bytes (`qos0_queue_bytes`) and the poll task still never blocks: it uses a non-blocking insert, and when the queue is full the newest message is dropped, counted in `thadam_source_dropped_total{reason="queue_full"}`, and marked with a `Gap`. QoS 0 overflow accounting (at twice capacity, sent equals received plus dropped) was not run in S2 and is a PR12 test.
- **QoS 2.** S2 verified the happy path only (a stripped PUBREC, completion with PUBCOMP, in order, on both brokers). Resume after reconnect (upstream issue #664) is untested. The MVP claims QoS 0 and QoS 1 only. QoS 2 is accepted in configuration as experimental and is not part of the delivery claims in ADR-0011 until the non-happy paths are tested.

### Liveness

A dedicated liveness task is the only owner of the `EventLoop`. It polls continuously, stamps `ingest_time` on each incoming publish, and checks the shutdown token only between polls (rule 1; S2 established that `poll` is not cancel-safe, so the earlier "assumed, validate in S2" caveat is resolved). `next_batch` is cancel-safe because it only receives from the internal queue.

### Sessions

`clean_start` is false, `client_id` is required and stable, and a session expiry is set. DUP-flagged redelivery after a reconnect is expected and counted. Rule 5 covers `SESSION_LOST`. A clean shutdown sends DISCONNECT so the persistent session keeps unacknowledged messages. Operators need to know that EMQX in its default configuration does not keep sessions across a broker restart, while Mosquitto needs `persistence true`.

### Connections and scale-out (D23)

One MQTT connection in the MVP. Shared subscriptions (`$share`) are rejected for the MVP because they lose cross-connection ordering; they remain a documented future option. Ordering promised to users is receive order per connection only.

### Throughput ceiling and mitigations

The ceiling is documented as a limit, not hidden. The formula to document and validate is:

`max messages per second per connection ~ window / (commit_latency + RTT)`

where `commit_latency` is receive-to-durable-ack including any group-commit wait (ADR-0004 supplies it; its provisional default `interval(10ms)` was chosen with this formula in mind). Measured worked examples (Mosquitto, window 20): 10 ms about 1.7 thousand messages per second, 50 ms about 384, 100 ms about 195. Window 100 or 1000 scales linearly until the client or broker limit. A group-commit interval of at most 10 ms, or a window sized for the interval, is the WAL guidance that follows. The owner decided on 2026-10-07 (B3, D-B3-4) that producer batching is not assumed and that `ack_mode` defaults to `on_commit`, so user documentation states the ceiling for unbatched producers, and `thadam validate` warns when rate times commit latency exceeds the observed window. Mitigations, in the order Thadam recommends them:

1. MQTT 5 Receive Maximum (it can only lower the broker's window, not raise it).
2. Operator-raised broker limits (`max_inflight_messages`, sized to at least rate times commit latency times 1.2).
3. Producer-side batching (several samples per message). Batching is NOT assumed, so this is an operator option and not a premise of the design.
4. `ack_mode: on_receive`, accepting at-most-once across a Thadam crash.

### Required broker settings (deployment documentation)

| Setting | Mosquitto 2.x | EMQX 5.8 |
|---|---|---|
| Inflight window | `max_inflight_messages` (default 20). Size to at least rate times commit latency times 1.2 (for example 1000 for 5k messages per second at 100 ms) | `mqtt.max_inflight` (default 32). **If at or above about 1000, also raise `force_shutdown.max_mailbox_size`** (default 1000): the default killed the session in S2 |
| Queue for an offline or slow client | `max_queued_messages` (default 1000), `max_queued_bytes`; size for outage times rate. **Drops the newest, silently** (one log line) | `mqtt.max_mqueue_len` (default 1000). **Drops the oldest** (throttled warning). The queue lives in process memory, so `force_shutdown.max_heap_size` (default 32MB) can kill the session |
| Session survival across a broker restart | `persistence true` (verified: `session_present = true` after a restart); client expiry unset or long | **The default configuration does not survive a restart** (sessions in memory); `mqtt.session_expiry_interval` defaults to 2 h |
| Message size | `message_size_limit` at or below Thadam's `max_message_bytes` | `mqtt.max_packet_size` (default 1MB) |
| Keepalive grace | about 1.5 times (7.3 s at 5 s) | 2.0 times observed at 5 s (multiplier setting 1.5) |
| Operator alert | log `Outgoing messages are being dropped` | log `dropped_msg_due_to_mqueue_is_full`, `check_oom_shutdown`, `maximum heap size reached` |

Under `on_commit` and the `block` policy, loss moves to the broker and happens invisibly there if its persistent-session queue overflows (confirmed in S2 for both brokers; ADR-0004, ADR-0011). The demo plan's Mosquitto configuration (persistence on, `max_inflight_messages` 100, `max_queued_messages` 200000, no retained messages) is a demonstration setting, not a validated recommendation **[V]**. In S2, Mosquitto's observed delivery counts under a withholding subscriber fit `max_queued_messages + 2 times max_inflight_messages`, but the mechanism was not investigated and must not be used for calculation.

### Broker versions for CI, tests, and documentation

Owner decision, 2026-10-07: Thadam pins the last open-source EMQX build, the **5.8.x** line, for CI, integration tests, the PR12 compatibility matrix, demos, and documentation. S2 ran `emqx/emqx:5.8.6`, an Apache-2.0 build, so every EMQX result in this ADR and in ADR-0011 was measured on the pinned line. Newer Enterprise-licensed images are not used for any of those purposes. S2 observed that `emqx/emqx:5.10.5` identifies itself as EMQX Enterprise with a non-commercial-use licence banner; that is an observation, not a legal opinion, and it is the reason for the pin.

- Images are referenced by an exact 5.8.x tag (currently `5.8.6`, the tested build), never `latest`, and never a tag from a newer line. Moving to another 5.8.x patch release requires re-running the PR12 EMQX job.
- The documented EMQX settings (table above) are the 5.8 settings. Documentation does not recommend or claim support for newer EMQX releases.
- Mosquitto 2.x remains the second CI broker and is unaffected.
- If the 5.8.x line stops receiving security fixes, or the licence terms of the pinned image change, reopen this pin (see "Revisit when").

### Security

TLS uses rustls with the `ring` provider (ADR-0020), with the feature set above. A real TLS and mTLS handshake against both brokers is still to be run (evidence below). No retained messages in the demo, because retained messages replay on every resubscribe and create duplicates.

### Exit criteria and fallback order

**The decision to keep `rumqttc` stands while the PR12 conformance suite passes in CI against both Mosquitto and EMQX 5.8.x:** liveness (acks withheld for at least 3 times the keepalive, 0 reconnects), ack ordering, `kill -9` redelivery with DUP, session-lost detection and resubscribe, the epoch-gated stale-ack test (the S2 E1 scenario must lose 0 messages), the oversize configuration test, and no `select!` around `poll`.

**Re-open the decision and fork or switch if any of these happens:**

1. Thadam needs to skip or quarantine an oversized packet instead of failing the connection (this requires a codec patch).
2. A reproducible ack or loss correctness defect appears that the wrapper cannot work around.
3. A security advisory in `rumqttc`'s dependency closure stays unfixed upstream for more than 30 days.
4. A required dependency bump breaks the conformance suite and upstream does not respond.
5. No upstream release for 12 months **and** S2b shows a maintained alternative passes the harness. The latest release is 0.25.1 from 2025-11-21 (confirmed in S2), so the 12-month point is 2026-11-21. (The earlier draft treated 12 months without a release as a trigger on its own; S2 changes that to require a passing alternative, so the project does not move on a calendar date alone.)

Fallback order:

1. A maintained fork: adopt `rumqttc-v4-next` if S2b passes, or vendor and maintain a 3.1.1-only copy (about 5,800 lines, to be re-estimated at the time).
2. `ntex-mqtt`, if it runs acceptably on Tokio within Thadam's MSRV. Adopting it would raise the declared MSRV to 1.97 (ADR-0020).
3. `paho-mqtt`, only after legal review of EPL-2.0 and with an explicit exception to the ADR-0020 ban on `openssl-sys` (its default build needs OpenSSL, and a vendored build still brings the crate).

`mqtt-endpoint-tokio` is not pursued.

### Superseded by this ADR (ARCHITECTURE.md section 19)

- The original S2 scope (`rumqttc` versus `paho-mqtt` conformance), replaced by the broader S2.
- The throughput hypothesis of 50k events per second on 2 cores with a 100 ms fsync interval, withdrawn as a target.
- Push-style `Source::run` with an `Emitter` (replaced in ADR-0002).
- Queue capacity equal to the configured inflight value for MQTT 3.1.1, and a blocking bounded queue in the poll task (replaced by wrapper rules 2 and 3).
- An in-client counted drop for oversized MQTT messages (replaced by rule 6).

## Alternatives considered

- **`rumqttc` 0.25.1, pinned (chosen).** Tokio-native, `Bytes` payloads, ordered manual acknowledgements, rustls. Risk: maintenance has slowed, which is why the exit criteria and the fork-readiness review exist.
- **`rumqttc-next` (community fork, noted alternative).** Actively maintained and MSRV 1.88, but young, run by a different small group, and not yet run against the S2 harness. S2b decides whether it becomes the first fallback or a drop-in replacement. Not adopted today.
- **Own maintained fork of `rumqttc`.** Keeps the fit and removes the maintainer risk, at the cost of Thadam owning about 5,800 lines of 3.1.1 code. The first concrete trigger is the oversized-packet codec patch.
- **`ntex-mqtt` (second fallback).** Actively developed and covers 3.1.1 and 5, but it is tied to the ntex runtime and its MSRV is 1.97, above Thadam's floor. Using it from Tokio code depends on `ntex-rt` compatibility **[V]**.
- **`paho-mqtt` (third fallback).** Strong MQTT 5 and Eclipse governance. Costs: EPL-2.0 weak copyleft needs legal review before distributing binaries **[V]**; C, CMake, and OpenSSL add cross-compile cost on ARM; it conflicts with the `openssl-sys` ban unless reviewed; OpenSSL patch management is inherited.
- **`mqtt-endpoint-tokio` 0.6.6.** Small adoption; not pursued.
- **Stop polling under backpressure.** Rejected, and measured: the broker disconnects the client (7.3 s on Mosquitto and 10.0 s on EMQX at a 5 s keepalive), causing reconnect storms and 100 DUP redeliveries per stall.
- **Block the poll task on a bounded queue.** Rejected, and measured: a queue below the broker window caused a disconnect and a deadlock.
- **Racing `poll` against a shutdown token or timeout.** Rejected, and measured: it loses acknowledgements silently when the socket is backed up.
- **Shared subscriptions or multiple connections.** Rejected for the MVP (D23): they lose cross-connection ordering and add scope before the single-connection ceiling is measured on target hardware.
- **`on_receive` as the default.** Rejected: at-most-once across a crash contradicts the capture-first promise. It remains opt-in.
- **MQTT 5 as the default.** Rejected for the MVP (see Protocol version).
- **Retained messages.** Not used in the demo.

## Consequences

**Easier**

- The runtime controls batching, fairness, metrics, and cancellation through the pull contract, while the connector keeps its own liveness.
- Backpressure is bounded and visible for QoS 1 and 2: source-side memory is bounded by the broker's window, and the observed window is a free, accurate metric.
- The throughput ceiling has a validated formula (within 6% of measurements), so documentation and `thadam validate` can state it precisely.
- The fallback order and exit triggers are decided in advance, so a maintenance failure is a planned event.

**Harder**

- Throughput under `on_commit` depends on broker configuration Thadam does not control. Operators need a runbook with the settings above, and users who publish one sample per message will hit the ceiling.
- The poll task must be written with unusual discipline (no awaits other than `poll`, no racing, byte-accounted unbounded queue with a disconnecting guard). A violation reintroduces silent ack loss or a deadlock.
- Epoch-tagged handles and resubscription are Thadam's responsibility, not the library's.
- Oversized messages cannot be skipped; they must be prevented by aligning the client and broker size limits.
- QoS 0 sources cannot be slowed; the only options are a counted drop or an overflow, and the contract chooses the counted drop.
- Thadam may have to maintain a fork of `rumqttc`.
- The conformance suite must stall the consumer for three times the keepalive interval, drop `next_batch` futures at random points, and run the stale-ack scenario.

**Fixed**

- `ack_mode` semantics and their delivery claims, which feed ADR-0011.
- `rumqttc` types are never exposed outside `thadam-mqtt`, so replacing the library does not change the contract.
- The supported protocol default (3.1.1) and the claim set (QoS 0 and 1).

## Evidence and validation required

### Decided at acceptance, and implementation-time verification

- [x] **EMQX licensing question.** Decided by the owner 2026-10-07: pin the last open-source build (5.8.x) for CI, tests, and documentation; newer Enterprise-licensed images are not used (see "Broker versions for CI, tests, and documentation"). S2 already ran `emqx/emqx:5.8.6`.
- [ ] **S2b (at most one day):** run the S2 harness against `rumqttc-v4-next` 0.34 and compare against the exit criteria. This is recommended before the pinned-upstream path is relied on long term and is not required for the decision to pin `rumqttc` 0.25.1 for PR12. Not a blocker.
- [x] **QoS 2 scope:** accepted as written (MQTT 5 and QoS 2 experimental, claims limited to QoS 0 and 1) when the owner accepted this ADR on 2026-10-07. [ ] Tests for the non-happy QoS 2 paths (issue #664) remain optional work, needed only if QoS 2 is later added to the claims.

### Evidence checklist

Against Mosquitto 2.x and EMQX 5.x.

- [x] **Ack-window throughput curve.** Inflight window in {20, 100, 1000} crossed with simulated commit latency in {0, 10, 50, 100} ms on Mosquitto 2.1.2 and EMQX 5.8.6; the model predicts the measurements within 1 to 6%. Commit latency inputs come from S1 (provisional default interval 10 ms). [ ] Repeat on target ARM hardware with realistic commit latencies and add repeated runs for variance (single sample per cell so far).
- [x] **Keepalive under backpressure.** Acks withheld for 3 times the keepalive: 0 reconnects, 0 loss, unacknowledged count never above the window. Tested at one keepalive (5 s) only.
- [x] **`EventLoop::poll` cancel-safety** ruled out on the write path (evidence 5).
- [x] **Acknowledgement with payload-stripped publishes** accepted for QoS 1 and 2 on both brokers and on MQTT 5.
- [ ] **QoS 0 overflow accounting.** At twice capacity, sent equals received plus dropped (not run in S2; PR12).
- [x] **Broker-side session behavior** observed (`session_present` accurate; EMQX default loses sessions on restart). [ ] **Thadam's session-lost handling:** the `SESSION_LOST` warning, metric, gap marker, epoch bump, and resubscribe, tested by restarting the broker without persistence (PR12).
- [x] **Crash safety, harness level.** `kill -9` with 20 unacknowledged QoS 1 messages: exactly those 20 redelivered with DUP, 0 missing, on both brokers. [ ] PR12 acceptance on the real connector: no loss within broker queue limits after restart, duplicates counted.
- [x] **TLS dependency check** (no `aws-lc-*`, `openssl-sys`, or `native-tls`; `ring` present). [ ] **TLS and mTLS handshake** with rustls and the `ring` provider against both brokers.
- [x] **MQTT 5 module maturity** assessed: experimental in the MVP (see Protocol version).
- [x] **3.1.1 window mismatch** tested: the broker window governed, and a queue below it disconnected and deadlocked the client (rules 2 and 3).
- [x] **Fork-readiness review** done (S2 section 7): 5.8k lines, no `unsafe`, thin tests, the defects found are avoidable in a wrapper except oversize skipping.
- [x] **Broker queue depth during a stall,** Mosquitto versus EMQX, observed (Mosquitto drops the newest, EMQX the oldest; both silent to the producer).
- [ ] **PR12 conformance suite** passing in CI against both brokers (see Exit criteria), including the epoch-gated stale-ack test and the no-`select!`-around-`poll` check.
- [ ] **`ntex-mqtt` on Tokio,** checked only if an exit criterion triggers **[V]**.

## Revisit when

- An exit criterion triggers (oversize skipping needed, an unworkaroundable correctness defect, an unfixed advisory for more than 30 days, a dependency break without upstream response, or no release in 12 months together with a passing maintained alternative).
- S2b or the PR12 suite finds a correctness bug in the chosen library or in the ack-after-commit design.
- Demand for MQTT 5 features appears, or the v5 module gains integration tests and the conformance suite passes on it.
- A single connection cannot reach the target throughput with realistic producer batching.
- The broker landscape changes default inflight windows, or the EMQX 5.8.x line stops receiving security fixes or its licence terms change (the owner pinned 5.8.x on 2026-10-07).

## References

- ARCHITECTURE.md sections 3 (D7, D23), 5, 10 (bounded queues, backpressure), 11, 12, 19, 21 (S2, PR12).
- ADR-0002 (connector contract: pull source, liveness clause, ordered ack, epoch-scoped handles), ADR-0004 (durable buffer, commit latency and fsync default), ADR-0011 (delivery guarantees, claims register, broker-side loss), ADR-0020 (TLS provider, license allow-list, MSRV policy).
- `docs/design/owner-decisions.md` (ADR acceptances and the EMQX 5.8.x pin, 2026-10-07).
- `docs/design/spikes/S2-mqtt.md` (2026-10-07; section 8 is the recommendation, section 5 the results, section 7 the fork-readiness review, section 9 the limitations).
- Design history: architecture review sections 2, 4, 6; Rust implementation plan sections 4.4, 4.5, 5.9; ecosystem maturity review sections 4 and 15 (item 4), as checked 2026-10-07; edge-capture demo plan, Mosquitto configuration.
- MQTT keepalive and PUBACK ordering requirements in the MQTT specification (MQTT-4.6.0-2).
- Upstream `rumqtt` issues #930 (potential auto ack lost), #846 (ack ordering), #664 (PubRel on reconnect), #250 (no automatic resubscribe).
