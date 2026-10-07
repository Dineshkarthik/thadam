# Spike S2: MQTT source (rumqttc) — ack window, keepalive, cancel-safety, sessions, fork readiness

| | |
|---|---|
| Status | Complete (evidence for ADR-006; the ADR itself is not edited here) |
| Blocker | B5 in the MVP readiness gate; gates PR12 (`thadam-mqtt`) |
| Date | 2026-10-07 |
| Code | `spikes/s2-mqtt/` (standalone Cargo project, own `[workspace]`) |
| Raw results | `spikes/s2-mqtt/results/{mosquitto,emqx}/` (JSONL + `SUMMARY.txt`) |
| Subject | `rumqttc` 0.25.1 (MQTT 3.1.1 `v4` module, plus a v5 smoke test) |

Every number below comes from a recorded run in `results/`. Where something was not measured, it says so.

---

## 1. Headline findings

1. **The ack-window ceiling is real, and the model predicts it.** With manual acks delayed by a commit latency `L`, one connection sustains `window / (L + RTT)` messages/s. Measured/nominal (`window / L`) was 84–87% at `L` = 10 ms and 95–98% at `L` = 50–100 ms on both brokers. Adding the measured ack delay and a 0.16 ms RTT, the model lands within 1–6% on Mosquitto (§5.1). A Mosquitto default window (20) with a 10 ms commit gives about 1.7k msg/s; with 100 ms it gives about 195 msg/s.
2. **The window is the broker's setting, not the client's.** rumqttc's `MqttOptions::inflight` is the *outgoing* publish window; it does not bound or inform what the broker sends. The observed maximum unacked count equalled the broker setting exactly (20, 100, 1000). Plan §5.9/§4.5 ("queue capacity equals the configured inflight value (3.1.1)") is wrong for MQTT 3.1.1. MQTT 5 `receive_maximum` does work as a client-set window.
3. **Backpressure by withholding acks is correct and safe.** While `poll()` kept running and acks were withheld for 3× keepalive, the connection stayed up (0 reconnects), PINGREQ/PINGRESP flowed (3 each in 15 s), and the broker stopped sending at exactly its inflight limit. After release, everything was delivered with 0 loss and 0 duplicates, on both brokers.
4. **Stopping the poll loop is fatal; so is blocking it.** Not polling for 3× keepalive: Mosquitto closed the connection after 7.3 s (keepalive 5 s), EMQX after 10.0 s. A bounded queue smaller than the broker window (`send().await` in the poll task) caused the same disconnect plus a **deadlock** (liveness task blocked sending, ack task blocked on `client.ack().await`; 91 of 12,750 delivered).
5. **`EventLoop::poll` is NOT cancel-safe.** Random drops on a healthy socket (≈1.2–1.3M dropped polls per broker, 540k messages) lost nothing. But when the socket write side is backed up, a dropped `poll` silently loses requests: with poll-once-then-drop, rumqttc emitted 300 `PubAck` events and **0 reached the wire** (no error, connection looks healthy); with a plain 50 ms `timeout`, 9 (Mosquitto) and 12 (EMQX) of 300 acks were lost. This matches open upstream issue #930. The plan rule "never race `poll`" is required, not just prudent.
6. **Payload-stripped acks work** (QoS 1 and QoS 2, both brokers, MQTT 3.1.1 and 5). Holding 1000 unacked 64 KiB messages cost +1.9 MiB RSS stripped vs +79.7 MiB retained.
7. **Ack order on the wire equals issue order.** In-order issue gave in-order PUBACKs (0 descents in 3000). Deliberately shuffled acks went out shuffled and both brokers tolerated it, so ordering is entirely the application's job.
8. **Persistent sessions behave as designed, with sharp edges.** After `kill -9`, exactly the 20 unacked in-flight messages were redelivered with DUP, and `session_present` was reported correctly in all cases. Edges: (a) rumqttc does not resubscribe after session loss; (b) **acks issued with pre-loss handles ack the wrong messages after session loss** (10 of 10 new messages silently lost in the E1 test); (c) a single oversized message with the default 10 KiB packet limit causes an infinite reconnect loop (116 reconnects in 12 s) and blocks everything behind it; (d) when the broker queue limit is exceeded, loss is silent to the publisher (Mosquitto drops newest, EMQX drops oldest).
9. **Recommendation: keep rumqttc 0.25.1 behind `thadam-mqtt`, pinned, with five wrapper rules (§8.2) and explicit fork triggers (§8.5). `ack_mode` default `on_commit`. MQTT 3.1.1 for the MVP; MQTT 5 experimental.**

---

## 2. Environment

| Item | Value |
|---|---|
| Host | Apple Silicon (arm64), 18 cores, 48 GiB, macOS (arm64) |
| Container runtime | Docker Desktop (Linux VM linuxkit, 18 vCPU, 7.7 GiB); brokers reach the host through its port forwarder |
| Toolchain | rustc/cargo 1.95.0; `tokio` 1; `rumqttc` 0.25.1 |
| Mosquitto | `eclipse-mosquitto:2` = Mosquitto 2.1.2, `sha256:38c0da4f2ef84284d47b3b3eeea1cb3bdeabe81ee10caf0cd5c5ff61ee3ea408` |
| EMQX | `emqx/emqx:5.8.6` (Apache-2.0 build), `sha256:a1e3d10fa1dcc8c94325b815a93b5b5974253a97cb36470b44e47d5d3ef9cd13` |
| Round trip (QoS 1 publish to PUBACK, sequential) | Mosquitto p50 160 µs / p99 330 µs (measured while the machine was loaded; 95 µs in an idle smoke run); EMQX p50 112 µs / p99 194 µs |
| Broker defaults (verified here) | Mosquitto: `max_inflight_messages` 20, `max_queued_messages` 1000 (§5.5). EMQX 5.8.6: `max_inflight` 32, `max_mqueue_len` 1000, `keepalive_multiplier` 1.5, `max_awaiting_rel` 100, `max_packet_size` 1MB, `session_expiry_interval` 2h, `force_shutdown` {`max_heap_size` 32MB, `max_mailbox_size` 1000} |

Notes on tag choice: `emqx/emqx:5` does not exist on Docker Hub; only full tags do. The newer `emqx/emqx:5.10.5` identifies itself as **EMQX Enterprise** with a non-commercial-use licence banner, so the matrix uses 5.8.6. (This is an observation, not a legal opinion; see §8.4.)

**Docker was available**, so the `rumqttd` embedded fallback was not needed. All containers and volumes created by the spike are removed (`scripts/broker.sh stop`).

---

## 3. Method

### 3.1 Harness

One binary, `s2-mqtt` (≈2.6k lines), with a subcommand per question. Source layout:

| File | Purpose |
|---|---|
| `src/throughput.rs` | Q1 ack-window curve; baseline RTT |
| `src/keepalive.rs` | Q2 withhold-acks / stop-polling / bounded-queue modes |
| `src/cancel.rs`, `src/stall.rs` | Q3 random-drop and write-blocked cancel tests |
| `src/strip.rs` | Q4 stripped vs retained acks (RSS), QoS 1 and 2 |
| `src/session.rs` | Q5 helper processes (`sess-sub`, `sess-pub`, `sess-report`), E1 stale acks |
| `src/order.rs` | Q6 ack order |
| `src/oversize.rs` | E2 oversized message |
| `src/v5.rs` | MQTT 5 smoke test |
| `src/wire.rs` | **MQTT-aware TCP proxy used as an independent oracle** |

### 3.2 Wire oracle

For Q2, Q4, Q6 (and the write-blocked Q3 test) the client connects through an in-process proxy that forwards bytes untouched and parses MQTT 3.1.1 packets in both directions with timestamps. It records PINGREQ times, PUBACK/PUBREC/PUBCOMP order and counts, DUP flags, and which side closed the socket first. Conclusions about "what reached the wire" therefore do not rely on rumqttc's own events. It can also stall the client-to-broker direction (Q3 write-blocked test). The proxy is **not** in the path for Q1 throughput.

### 3.3 Q1 setup

* One subscriber connection (QoS 1, `set_manual_acks(true)`, persistent=false), 256-byte payloads.
* The liveness loop polls `EventLoop` and forwards a *stripped* publish plus its receive instant to an acker. The acker sleeps until `receive + L` and calls `client.ack()` in receive order (simulated commit latency `L` ∈ {0, 10, 50, 100} ms, per message, completion in order).
* 4 saturating QoS 1 publisher connections, throttled only by a backlog cap (`published − received ≤ 20,000` for Mosquitto, 3,000 for EMQX). Each cell records `min_backlog_in_window` and flags `supply_limited` if the broker could ever have run dry; **no cell was supply-limited**.
* 3 s warm-up, 10 s measured window, **one run per cell** (no repetitions; see §9).
* Reported rate = `Incoming::Publish` count in the window; "wire ack/s" counts `Outgoing::PubAck` events. They matched (except for a few in-flight at the edges).
* Mosquitto: `max_inflight_messages` ∈ {20, 100, 1000}, `max_queued_messages` 100000, persistence off. EMQX: `EMQX_MQTT__MAX_INFLIGHT` ∈ {20, 100, 1000}, `max_mqueue_len` 10000 (see §5.1 for why).

### 3.4 Evidence rule

Nothing is extrapolated. Where a mechanism was not investigated (for example Mosquitto's `max_queued_messages` accounting), the report states the observed pattern only.

### 3.5 Run history (for honesty)

* The first Q5/E1 run was **discarded**: my script killed a subshell instead of the subscriber, so the "killed" client kept running and fought the restarted one for the same client ID. Fixed in `scripts/run_all.sh` (direct PID, kill verification); Q2 to Q5, E1 and v5 were re-run. The Mosquitto Q1 and Q6 results are from the first run (unaffected).
* The Mosquitto Q3 random-drop runs used a publisher backlog cap of 10,000; the committed script now uses 3,000 for both brokers (needed for EMQX). The difference is not material to the result.

---

## 4. Brokers and settings used

```
scripts/broker.sh start mosquitto <max_inflight> [max_queued=100000] [persist=false] [log=quiet|debug]
scripts/broker.sh start emqx      <max_inflight> [max_mqueue_len]     # EMQX_EXTRA_ARGS='-e K=V' adds env
```

Mosquitto is configured with `listener 1883`, `allow_anonymous true`, `max_inflight_messages`, `max_queued_messages`, `persistence`. EMQX is configured through `EMQX_MQTT__MAX_INFLIGHT` / `EMQX_MQTT__MAX_MQUEUE_LEN`.

---

## 5. Results

### 5.1 Q1: ack-window throughput

Receive rate (msg/s) of **one connection**, `on_commit`-style acks with commit latency `L`. "Nominal" is the plan's `inflight / L`.

**Mosquitto 2.1.2**

| Window | `L` = 0 | `L` = 10 ms | `L` = 50 ms | `L` = 100 ms | Auto-ack (`manual_acks=false`), `L` = 0 |
|---:|---:|---:|---:|---:|---:|
| 20 | 75,952 (max unacked 20) | 1,725 (nominal 2,000; 86.2%) | 384 (400; 96.1%) | 195 (200; 97.6%) | 87,066 |
| 100 | 70,438 (max unacked 99) | 8,529 (10,000; 85.3%) | 1,933 (2,000; 96.7%) | 978 (1,000; 97.8%) | 76,862 |
| 1000 | 70,310 (max unacked 564) | 86,840 (100,000; 86.8%) | 19,342 (20,000; 96.7%) | 9,774 (10,000; 97.7%) | 62,496 |

**EMQX 5.8.6**

| Window | `L` = 0 | `L` = 10 ms | `L` = 50 ms | `L` = 100 ms | Auto-ack, `L` = 0 |
|---:|---:|---:|---:|---:|---:|
| 20 | 34,078 | 1,676 (nominal 2,000; 83.8%) | 383 (400; 95.8%) | 195 (200; 97.5%) | 39,731 |
| 100 | 53,702 | 8,359 (10,000; 83.6%) | 1,912 (2,000; 95.6%) | 973 (1,000; 97.3%) | 51,544 |
| 1000 | 85,612 (max unacked 497) | **0: session killed by the broker** (see below); with a raised limit, 52.7k | 19,064 (20,000; 95.3%) | 9,631 (10,000; 96.3%) | 90,751 |

**Reading the curve**

* **The ceiling model holds.** At `L` ≥ 50 ms the window is the only constraint and the result is 95–98% of nominal on both brokers. The gap at 10 ms is timer and RTT overhead: the measured mean ack delay was 10.7–11.0 ms (not 10.0 ms; Tokio timer granularity) and RTT p50 was 0.16 ms. Using `window / (mean measured delay + 0.16 ms)` (Mosquitto), the achieved fraction is 94.3–95.9% at 10 ms, 98.2–98.6% at 50 ms, 98.5–98.8% at 100 ms.
* **max unacked = broker window** in every cell where the window binds (20, 100, 1000). The observed value is a direct, free measurement of the effective window.
* **At `L` = 0 the limit is the client/broker, not the window**: roughly 62k–90k msg/s per connection on this machine; max unacked stayed far below the window for 1000 (564 and 497).
* **Manual vs auto ack at `L` = 0**: 70k–76k vs 62k–87k on Mosquitto, 34k–86k vs 40k–91k on EMQX; no consistent difference across the three windows (single samples), so manual acks add no large fixed cost.
* **EMQX window 1000 / `L` = 10 ms: not window-bound.** After the fix in E3 the recorded run gave 52.7k msg/s with a mean of 543 unacked (not 1000), against a 100k nominal ceiling; two earlier manual runs (not in `results/`) gave 44.8k and 42.5k (mean 441 unacked). EMQX's per-session delivery rate was the binding limit there; not investigated further.

**EMQX caveat (E3).** With `max_inflight=1000` and the default `force_shutdown.max_mailbox_size=1000`, EMQX killed the subscriber session during the flood (`check_oom_shutdown ... reason => mailbox_overflow, value => 1157`), so the client reconnected and measured 0 msg/s. With `EMQX_FORCE_SHUTDOWN__MAX_MAILBOX_SIZE=10000` the run completed (`results/emqx/e3.jsonl`, `e3-broker.txt`). A deeper broker backlog also hit the heap limit (`maximum heap size reached`, 6,291,456-word limit): in manual runs (not in `results/`) at an 8,000-message backlog on 5.8.6, and with `max_mqueue_len` 100000 on 5.10.5 (`results/emqx/emqx-5.10.5-observation.txt`). EMQX keeps session queues in process memory, so queue depth and `max_inflight` interact with `force_shutdown`. This is why the EMQX matrix uses `max_mqueue_len` 10000 and a backlog cap of 3000.

### 5.2 Q2: keepalive under backpressure

Keepalive 5 s, hold = 3 × keepalive = 15 s. The consumer stand-in for the stalled WAL neither drains the queue nor acks during the hold. Publisher: configured 1000 msg/s for Mosquitto (≈850 achieved) and 400 msg/s for EMQX (≈340 achieved), so EMQX's queue limit is not hit.

**(a) Keep polling, withhold acks** (`results/*/q2.jsonl`)

| Broker | Window | TCP connections | Client reconnects | PINGREQ / PINGRESP in 15 s | Max unacked | Received at hold end | Published | Unique delivered | Dups | Missing |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| Mosquitto | 20 | 1 | 0 | 3 / 3 | 20 | 20 | 12,730 | 12,730 | 0 | 0 |
| Mosquitto | 100 | 1 | 0 | 3 / 3 | 100 | 100 | 12,760 | 12,760 | 0 | 0 |
| Mosquitto | 1000 | 1 | 0 | 3 / 3 | 1000 | 1000 | 12,740 | 12,740 | 0 | 0 |
| EMQX | 20 | 1 | 0 | 3 / 3 | 20 | 20 | 5,080 | 5,080 | 0 | 0 |
| EMQX | 100 | 1 | 0 | 3 / 3 | 100 | 100 | 5,080 | 5,080 | 0 | 0 |
| EMQX | 1000 | 1 | 0 | 3 / 3 | 1000 | 1000 | 5,084 | 5,084 | 0 | 0 |

The per-second timeline is flat at the window from second 1 to second 14 (for example Mosquitto, window 100: `100/100` every second), then jumps when acks are released at second 15. **The broker stops sending at exactly its inflight limit, and the connection stays up.** Per-message in-order draining after release delivered everything.

**(b) Anti-pattern: stop polling for 3 × keepalive** (window 100)

| Broker | Broker closed the socket | Evidence | After resuming `poll` |
|---|---|---|---|
| Mosquitto | **7.30 s** after the last client packet (keepalive × 1.5 = 7.5 s; Mosquitto uses whole-second timestamps) | broker log `Client ... disconnected: exceeded timeout.` | `poll` returned `Connection closed by peer abruptly`; reconnect with `session_present=true`; **100 DUP redeliveries**; 0 missing (persistent session) |
| EMQX | **10.0 s** after the last client packet (2.0 × keepalive although `keepalive_multiplier` is 1.5) | proxy saw the broker close the socket | same: error, reconnect, 100 DUP redeliveries, 0 missing |

With a *clean* session the 100 in-flight messages would have been lost. Mosquitto: 0 PINGREQs during the hold (no polling means no pings). EMQX: 1, sent at the instant polling resumed (most likely counted at the window boundary).

**(c) Bounded queue smaller than the broker window** (queue cap 20, window 100; `send().await` in the poll task)

| Broker | Broker closed the socket | PINGREQ during hold | Delivered at end | State at end |
|---|---|---|---|---|
| Mosquitto | 7.33 s after last client packet | 1 | 91 of 12,750 | liveness task blocked in `send().await`: **true**; consumer blocked in `client.ack().await`: **true** |
| EMQX | 10.01 s | 0 | 91 of 5,096 | both blocked: **true** |

This is the plan §5.9 design with an undersized queue. The poll task blocks on the full queue, stops pinging, the broker disconnects, and then the system **deadlocks**: the consumer awaits `client.ack()` on a full request channel (cap 64) that only the blocked poll task can drain. A queue equal to the window (cap 100, window 100) behaved like case (a): no disconnect, 0 missing. Since 3.1.1 gives the client no way to know the window, the safe design is that the poll task never awaits anything but `poll()` (§8.2).

### 5.3 Q3: cancel-safety of `EventLoop::poll`

**Source reading (rumqttc 0.25.1, `eventloop.rs`, `framed.rs`, `state.rs`)**

`poll()` runs `select!` over three branches: network read (`readb`), the next request (pending list, else `flume::recv_async`), and the keepalive timer. Each winning branch body then awaits again:

* **Request branch:** `request` is already consumed from the channel; `handle_outgoing_packet` has already mutated state and pushed an `Outgoing::*` event; only then `network.write(outgoing).await?` runs, then `flush()` under a timeout. If the future is dropped while `write` (that is, `Framed::feed` → `poll_ready` → flush) is pending, **the packet is lost but the event is already queued**. This applies to `PubAck`.
* **Read branch:** `Network::readb` calls `state.handle_incoming_packet` (which pushes the incoming event into `state.events`, so no delivery is lost), but for auto-ack and for QoS 2 PUBREL → PUBCOMP it then awaits `self.write(outgoing)`; dropping there loses that reply (upstream issue #930, open since 2024-12).
* **Safe parts:** `next_request` pops the pending list *after* its sleep (commented as cancel-safe); buffered events are returned first on the next poll; the connect phase keeps its state in locals (a dropped connect restarts cleanly, at the price of connect churn).

**Empirical (`results/*/q3.jsonl`, `q3-stall.jsonl`)**

*(i) Random drops on a healthy socket.* QoS 1, manual acks, persistent session, keepalive 2 s, 10 runs per broker (540k messages with cancellation, 100k control): styles `now_or_never` (poll once, drop if pending), `timeout` and `select!` with random deadlines, `mixed`, from-start (cancelling the connect phase too), and an idle 30 s run (keepalive under cancellation).

| | Mosquitto | EMQX |
|---|---|---|
| Dropped (cancelled) poll futures | 1,203,051 (largest run 1,133,251) | 1,290,152 (largest run 1,174,242) |
| Messages published / unique delivered | 640,000 / 640,000 | 640,000 / 640,000 |
| Missing | 0 | 0 |
| Duplicates without DUP flag | 0 | 0 |
| Unexpected reconnects / poll errors | 0 / 0 | 0 / 0 |
| Redelivery on fresh persistent session after run (acks all arrived) | 0 | 0 |

Result: **no loss, duplication or broken state** under random drops. But this method can only drop at pending points reachable on a healthy socket, mostly "waiting for network read". It does not reach the write-side await points above. Absence of failures here is therefore not proof of safety.

*(ii) Targeted: drop `poll` while the write side is blocked.* The proxy stops reading from the client; 40 × 256 KiB QoS 0 publishes fill the client's send buffer; 300 QoS 1 messages are delivered and acked during the stall (3 s). Compare `Outgoing::PubAck` events rumqttc emitted with PUBACK packets that reached the wire and with redelivery to a fresh session:

| Broker | Polling style during stall | Cancelled polls | PubAck events | PUBACKs on wire | Acks lost | Redelivered to fresh session | Poll errors |
|---|---|---:|---:|---:|---:|---:|---:|
| Mosquitto | none (plain `poll().await`; control) | 59 | 300 | 300 | **0** | 0 | 0 |
| Mosquitto | `timeout` 50 ms | 117 | 300 | 291 | **9** | 9 | 0 |
| Mosquitto | `now_or_never` | 25,173,531 | 300 | **0** | **300** | 300 | 0 |
| EMQX | none (control) | 59 | 300 | 300 | **0** | 0 | 0 |
| EMQX | `timeout` 50 ms | 117 | 300 | 288 | **12** | 12 | 0 |
| EMQX | `now_or_never` | 22,518,573 | 300 | **0** | **300** | 300 | 0 |

QoS 0 filler publishes were also lost (35 of 40 with `now_or_never`). **No error is raised and the connection stays up.** The lost acks leave the broker window permanently short (the broker waits for acks that will never come), a silent stall that keepalive does not detect, until the next reconnect triggers redelivery. (The first control attempt used a 100 ms timeout and also lost fillers; the clean control above uses a plain poll.)

**Conclusion.** `EventLoop::poll` is cancel-safe only at its read await points. The plan's rule (§4.4) is confirmed and must stay: the poll task owns the `EventLoop`, calls `poll()` to completion, and checks the shutdown token only between polls. `select!`/`timeout` around `poll` loses acks silently whenever the socket is backed up (slow link, broker stall). Outbound requests (including acks) also never flush unless `poll` is running (a pending-ack artifact found in Q6: acks issued after the last poll sit in the channel).

### 5.4 Q4: ack with a payload-stripped Publish

`strip(&p)` keeps `{qos, pkid}` and clears `topic`, `payload`, `dup`, `retain`. Source: `get_ack_req` reads only `publish.qos` and `publish.pkid` (QoS 1 → `PubAck(pkid)`, QoS 2 → `PubRec(pkid)`). Empirically (publisher in a separate process so RSS is isolated):

| Broker | Mode | QoS | Payload | Held unacked at peak | RSS delta at hold | Payload bytes held | Unique / dups | PUBACK (or PUBREC/PUBCOMP) on wire | Order | Redelivered on session resume |
|---|---|---:|---:|---:|---:|---:|---|---|---|---:|
| Mosquitto | stripped | 1 | 64 KiB | 1000 | **+1.9 MiB** | n/a | 3000 / 0 | 3000 PUBACK | in order | 0 |
| Mosquitto | retained | 1 | 64 KiB | 1000 | **+79.7 MiB** | 62.5 MiB | 3000 / 0 | 3000 PUBACK | in order | 0 |
| Mosquitto | stripped | 2 | 4 KiB | 1000 | +1.2 MiB | n/a | 3000 / 0 | 3000 PUBREC, 3000 PUBCOMP | in order | 0 |
| EMQX | stripped | 1 | 32 KiB | 1000 | +1.9 MiB | n/a | 1500 / 0 | 1500 PUBACK | in order | 0 |
| EMQX | retained | 1 | 32 KiB | 1000 | +48.4 MiB | 31.25 MiB | 1500 / 0 | 1500 PUBACK | in order | 0 |
| EMQX | stripped | 2 | 4 KiB | 100 | +0.9 MiB | n/a | 100 / 0 | 100 PUBREC, 100 PUBCOMP | in order | 0 |

* **Works correctly**: the broker accepted every stripped ack (zero redelivery on a fresh persistent session), the window kept advancing, and QoS 2 completed (rumqttc answers PUBREL with PUBCOMP internally).
* **Payload memory need not be retained until commit.** Retaining the full `Publish` cost about 1.27× the payload bytes (likely because the payload `Bytes` pin their read-buffer allocations; not investigated); stripping costs almost nothing. This also supports the plan's rule about not pinning read chunks.
* The EMQX QoS 2 run delivered only 100 of 150 because EMQX's default `max_awaiting_rel` (100) throttles the *publisher's* QoS 2 flow in my harness; it is not a subscriber defect (an earlier run with count 3000 delivered 202). The messages that did flow were acked and completed correctly.

### 5.5 Q5: persistent session, kill -9, queue limits

**(a) `kill -9` with unacked QoS 1** (window 20, 50 published; subscriber acks the first 5 only)

| Broker | First connect `session_present` | Delivered before kill | After restart: `session_present` | Redelivered with DUP | Fresh (non-DUP) | Total unique | Missing |
|---|---|---:|---|---|---|---:|---:|
| Mosquitto (persistence on) | false | 25 (20 in flight + 5 after acking 5) | true | 20 (seq 5–24 = exactly the unacked ones) | 25 (seq 25–49) | 50 | 0 |
| EMQX | false | 25 | true | 20 (seq 5–24) | 25 | 50 | 0 |

**Redelivery works and is marked DUP; the 5 acked messages were not redelivered.** At-least-once is preserved; Thadam must treat DUP-flagged messages as possible duplicates (the plan already counts them).

**(b) `session_present` across broker restarts** (client reconnected to the same client ID)

| Broker | First connect | After broker restart | After broker state wipe |
|---|---|---|---|
| Mosquitto (persistence on) | false | **true** | false |
| EMQX (default: sessions in memory) | false | **false** | false |

rumqttc reports `ConnAck.session_present` accurately in all five observed cases. **EMQX in its default configuration loses persistent sessions on broker restart**, so Thadam must treat `session_present=false` after an expected session as `SESSION_LOST` (plan §5.9 already does) and cannot rely on broker persistence being on.

**(c) Broker queue limit exceeded** (`max_inflight` 20, queue limit 100; 1000 QoS 1 messages published; the publisher received **all 1000 PUBACKs** in every case)

| Broker | Case | Messages the subscriber eventually received | Which ones survived | Signal to the operator |
|---|---|---|---|---|
| Mosquitto | subscriber offline | 100 | **first** 100 (seq 0–99): newest dropped | broker log `Outgoing messages are being dropped for client ...` (once) |
| Mosquitto | subscriber connected but withholding acks | 140 | first 140 (seq 0–139) | same log line |
| EMQX | subscriber offline | 100 | **last** 100 (seq 900–999): oldest dropped | warning `dropped_msg_due_to_mqueue_is_full` (log-throttled: one line per run in our logs) |
| EMQX | subscriber connected but withholding acks | 120 | first 20 (in flight) + last 100 (seq 900–999) | same warning |

**Silent to the producer in all four cases.** This confirms the architecture review's claim that `on_full: block` moves loss to the broker invisibly. Also observed: with Mosquitto's *default* config (`max_queued_messages` 1000, window 20) and a withholding subscriber, 1040 of 5070 were delivered; both Mosquitto "withholding" observations (100/20 → 140, 1000/20 → 1040) fit `max_queued_messages + 2 × max_inflight_messages`, while the offline case fits `max_queued_messages`. The mechanism was not investigated; do not compute with it.

### 5.6 Q6: PUBACK ordering

Wire oracle, completion-ordered release (each message "commits" after a random 0–20 ms; acks released strictly in receive order), 3000 messages:

| Broker | Mode | PUBACKs on wire | Out-of-order (descents) | Unmatched | Broker closed connection |
|---|---|---:|---:|---:|---:|
| Mosquitto | in order | 3000 | **0** | 0 | 0 |
| Mosquitto | shuffled (control: reversed within batches of 10) | 1000 | 900 | 0 | 0 |
| EMQX | in order | 3000 | **0** | 0 | 0 |
| EMQX | shuffled | 1000 | 900 | 0 | 0 |

When acks are issued in receive order, PUBACKs reach the wire in receive order (rumqttc forwards the request channel in order). When issued out of order they reach the wire out of order, and both brokers accepted that without complaint, so **ordering is entirely the application's responsibility** (upstream issue #846 asks for exactly this). Keep the ordered ack tracker; MQTT 3.1.1 requires PUBACKs in receive order [MQTT-4.6.0-2]. (The first version of this test missed the last 44 acks because the harness stopped polling: acks only go out while `poll` runs.)

### 5.7 Extras

**E1: acks with handles from a lost session (Mosquitto; `results/mosquitto/e1.jsonl`)**

Scenario: hold 10 unacked messages (pkids 1–10); wipe the broker (session loss); rumqttc reconnects (`session_present=false`); resubscribe; 10 new messages arrive, again with pkids 1–10 and **not acked**; then the application "finishes committing" the old messages and acks them with the old handles. Then drop the connection and resume the persistent session.

| Variant | New messages redelivered on resume (expected 10) | New messages silently lost |
|---|---:|---:|
| Control: no stale acks | 10 | 0 |
| Stale acks sent | **0** | **10** |

Stale acks acknowledged the wrong (new, uncommitted) messages: **data loss under at-least-once**. Also: after session loss, rumqttc does **not** resubscribe (0 deliveries until `subscribe` is called again). EMQX logs show the same pattern organically (`puback_packetId_not_found` for ids from the previous connection). Not run on EMQX (the test uses the Mosquitto-specific wipe script).

**E2: oversized message vs the default 10 KiB incoming limit (both brokers; `results/*/e2.jsonl`)**

rumqttc defaults to `max_incoming_packet_size` 10 KiB. A 64 KiB QoS 1 message published between two small ones: the codec returns `payload size limit exceeded: 65560` and **fails the connection** instead of skipping the packet. With a persistent session the broker redelivers it after every reconnect: **116 reconnects in 12 s (Mosquitto), 114 (EMQX)**, and the small message queued behind it (seq 2) was never delivered. One bad message stalls the stream permanently. The packet limit must be set from `max_message_bytes`, aligned with the broker limit, and an in-client "counted drop" for oversized packets (plan §4.2) is **not possible without patching the codec**.

**E3 / E4** are the EMQX notes in §5.1 and §5.4.

**MQTT 5 smoke test (Mosquitto; `results/mosquitto/v5.jsonl`)**

| Check | Result |
|---|---|
| `receive_maximum` = 10 against a broker window of 1000 | broker stopped sending at **10 unacked**; after release the remaining 290 arrived |
| Stripped v5 PUBACK + persistent session (`clean_start=false`, session expiry 300 s) | first connect `session_present=false`; after abrupt drop and reconnect `true`; 20 redelivered, all DUP-flagged; **0** redelivered again after stripped acks |
| Keepalive 5 s, 16 s run | 3 PINGRESPs, 0 errors |
| API quirks | v5 `set_keep_alive` **panics** below 5 s ("Keep alives should be >= 5 secs"); v4 only below 1 s; both default to a 10 KiB incoming limit |

Only the happy-path checks above were run on v5; the Q1–Q6 matrix was **not** repeated for v5.

---

## 6. TLS / dependency check

Default `rumqttc` features resolve `aws-lc-rs` (`aws-lc-rs 1.18.1` downloaded in a default-features check). The spike's Cargo.toml uses

```toml
rumqttc = { version = "0.25", default-features = false, features = ["use-rustls-no-provider"] }
tokio-rustls = { version = "0.26", default-features = false, features = ["ring", "tls12"] }
```

`cargo tree -i aws-lc-sys`, `aws-lc-rs`, `openssl-sys`, `native-tls` all report "did not match any packages"; `ring 0.17.14` is present. `use-rustls-no-provider` was added in 0.25.1 for exactly this. **This satisfies the `deny.toml` ban on `aws-lc-sys`/`openssl-sys`.** A real TLS/mTLS handshake was not exercised (plain TCP to local brokers).

---

## 7. Q7: fork readiness

All numbers recomputed from a shallow clone (depth 200, 209 commits, oldest 2023-02-06) at `bytebeamio/rumqtt` HEAD `e886a788935d1b78a666b45124b9036cd6d14002` (2026-05-01), in a temp dir outside the repo. GitHub API use: about 15 unauthenticated calls.

**Maintenance**

| Metric | Value |
|---|---|
| Latest release | rumqttc 0.25.1 (2025-11-21), Apache-2.0, declared MSRV 1.64 |
| Non-merge commits since 2025-10-07 | **3** (vs 16 in the previous 12 months); last: 2026-05-01 (feature, outgoing-bind). 3 distinct authors |
| Open issues / PRs | **101 issues / 82 PRs** (GitHub's "183 open issues" counts both); PR #1019 ("clear collision state on reconnection with clean session") open since 2026-01-08 |
| Issues with the `bug` label | 2 open (labels are unused, so this says little) |
| Downloads (crates.io) | 8.8M total, 2.1M recent |

**Size (rumqttc/src at HEAD, including in-file unit tests)**

| Part | Files | Lines |
|---|---:|---:|
| `rumqttc/src` total | 47 | 14,069 |
| `v5` module (incl. `v5/mqttbytes`, 4,998) | 22 | 8,285 |
| MQTT 3.1.1 path (src minus v5; includes shared options, TLS, proxy, websocket) | 25 | ≈5,784 |
| `rumqttc/tests` | 2 | 896 |

No `unsafe` in `rumqttc/src`. For comparison, `rumqttd/src` is 14,078 lines (not relevant to us).

**Test coverage**

* `#[test]` counts: 40 in the 3.1.1 side of `src`, 37 in `src/v5`, 16 in `tests/reliability.rs` (in-process mock broker, MQTT 3.1.1 only; no v5 integration tests).
* Relevant tests for Thadam: one unit test per version for manual acks (`incoming_publish_should_not_be_acked_with_manual_acks`); nothing for cancel-safety, keepalive under backpressure, ack ordering, or session resume with a real broker. CI has no real-broker, fuzz or loom job.
* CI collects coverage (`cargo llvm-cov` → Coveralls) for `rumqttc` + `rumqttd` together; the last Coveralls figure is **36.1%** (build dated 2025-04-08), so treat it as a weak, stale signal.

**Open issues relevant to a QoS 1 manual-ack subscriber** (read, not just title-matched)

| Issue | Relevance | This spike |
|---|---|---|
| #930 "potential auto ack lost" (open since 2024-12-27, 1 comment) | `write(outgoing).await` in `readb` loses the reply if cancelled | Reproduced for the request-branch ack path (§5.3) |
| #846 "Keep the ACK ordering when using manual ACK" | no mechanism; app's job | Confirmed (§5.6) |
| #1056 / #1014 / PR #1019 (collision livelock after session loss) | publish-side `collision` state; **not reachable in a subscribe-only client**; matters if Thadam ever publishes | Not exercised |
| #664 "Handling PubRel when reconnecting to existing session" | QoS 2 resume | QoS 2 only happy path tested |
| #250 "Automatic reconnect and subscribed topics?" | no auto-resubscribe | Confirmed (§5.7 E1) |
| #926, #814 | keepalive/throttling reports | Not reproduced |

**Community continuation.** crates.io shows an actively maintained fork, `thehouseisonfire/rumqtt` (published as `rumqttc-next`, `rumqttc-v4-next` and `rumqttc-v5-next`, all 0.34.0, updated 2026-08-13, created Feb–Mar 2026, about 145k recent downloads; MSRV 1.88; repo pushed 2026-10-07; 27 stars, 4 open issues/PRs). Its README describes it as a fork started "in response to a period of upstream inactivity", with separate 3.1.1 and 5 clients, tracked operation outcomes and manual acknowledgements. Its changelog is 1,265 lines and moving fast (0.27.0 on 2026-02-19 to 0.34.0 on 2026-08-13). **I did not run the S2 harness against it**; this is metadata only.

**Fork readiness assessment**

* A *v3.1.1-only* fork carries about 5.8k lines (no `unsafe`, Apache-2.0, small dependency set: `bytes`, `flume`, `tokio-util`, `fixedbitset`, rustls bits). That is a tractable surface for a small team.
* The defects we hit are all avoidable in a wrapper, except one: **skipping an oversized packet (E2) needs a codec patch.** That is the first concrete fork trigger.
* The v5 module is larger (8.3k lines) and thinner on tests (37 unit tests, no integration tests); do not take on v5 maintenance for the MVP.

---

## 8. Recommendation for ADR-006

### 8.1 Decision

**Keep `rumqttc` 0.25.1 for the MVP MQTT source**, pinned (`=0.25.1`), `default-features = false, features = ["use-rustls-no-provider"]` plus `tokio-rustls` with `ring`, behind `thadam-mqtt`, with no rumqttc types in the public API (as the plan already states). Do not fork now. Vendoring or forking is gated on explicit triggers (§8.5). Evaluate `rumqttc-v4-next` in a short follow-up (S2b, §8.6) before committing to the "pinned upstream" path long term.

Rationale: the library does what the design needs (ordered manual acks, `Bytes` payloads, stripped acks, accurate `session_present`, working keepalive when polled), the ceiling is predictable, and every failure mode found has a wrapper-level mitigation except oversize skipping. Maintenance risk is real (3 non-merge commits in 12 months) but the 3.1.1 surface is small enough to own.

### 8.2 Required wrapper rules (amend plan §4.4, §4.5, §5.9)

1. **The poll task only ever awaits `EventLoop::poll()`.** It is never raced (`select!`, `timeout`, `now_or_never`) against anything; shutdown is a token check *between* polls. (Evidence: §5.3. Keep plan §4.4.)
2. **The poll task never blocks on downstream.** Do not `send().await` into a bounded queue. Use a queue unbounded in count but byte-accounted; the broker bounds it by its window. Add a hard guard that *disconnects* (rather than blocks) if unacked exceeds a configured multiple of the observed window. (Evidence: §5.2(c): disconnect plus deadlock.)
3. **The 3.1.1 window is the broker's, not `MqttOptions::inflight`.** Rename the config key (for example `expected_broker_window`, used only for memory estimates and `thadam validate`), and expose the **observed** maximum unacked as a metric: it equalled the broker window exactly in every run. For v5, `receive_maximum` is client-settable and honoured (it can only lower the window, not raise it above the broker's).
4. **Connection-epoch-tagged ack handles.** Bump the epoch on every `ConnAck`; drop (and count) acks from older epochs. Stale acks after session loss silently lose data (§5.7 E1). The cost is extra DUP redelivery after a reconnect with `session_present=true`, which at-least-once already tolerates.
5. **On every `ConnAck`: if `session_present=false` (and a session was expected), emit `SESSION_LOST`, bump the epoch and re-send SUBSCRIBE**; rumqttc will not (E1: 0 deliveries otherwise).
6. **Set `max_incoming_packet_size` from `max_message_bytes` and align the broker limit** (Mosquitto `message_size_limit` default is unlimited; EMQX `mqtt.max_packet_size` default 1MB). The default 10 KiB turns one large message into an infinite reconnect loop (E2).
7. Acks hold only `(pkid, qos)` (stripped publish). Keep the ordered ack tracker; acks flush only while `poll` runs.
8. Set `NetworkOptions::connection_timeout` explicitly: it also serves as the 5 s flush timeout (`FlushTimeout` on a slow link triggers disconnects). `keep_alive` ≥ 1 s for v4 (5 s for v5); recommend 30–60 s; the broker's grace differs (Mosquitto ≈1.5×, observed 7.3 s at 5 s; EMQX observed 2.0×, 10.0 s).

### 8.3 `ack_mode` default and throughput guidance

* **Default `ack_mode: on_commit`.** It is the only mode that gives at-least-once across a Thadam crash. `on_receive` is an explicit opt-in documented as at-most-once across a crash (the architecture review's wording).
* **Throughput rule to document and validate:** `max msg/s per connection ≈ window / (commit_latency + RTT)`, where `commit_latency` is receive-to-durable-ack including any group-commit wait. Measured with the harness; reproduced to within 6% (§5.1). Worked examples (Mosquitto, window 20): 10 ms → ≈1.7k msg/s; 50 ms → ≈384 msg/s; 100 ms → ≈195 msg/s. Window 100 or 1000 scales linearly until the client/broker limit (≈70–90k msg/s here).
* WAL guidance that follows: group-commit interval ≤ 10 ms (or a window sized for the interval), and a startup `thadam validate` warning computed from the observed window; if a producer does not batch and the product of rate × commit latency exceeds the window, `on_commit` throughput is capped and the cap must be a documented limit (this resolves open decision B3's "producer batching" input for MQTT).

### 8.4 Required broker settings (deployment documentation)

| Setting | Mosquitto | EMQX 5.8 |
|---|---|---|
| Inflight window | `max_inflight_messages` (default 20). Size ≥ `rate × commit_latency × 1.2` (for example 1000 for 5k msg/s at 100 ms) | `mqtt.max_inflight` (default 32). **If ≥ ~1000, also raise `force_shutdown.max_mailbox_size`** (default 1000): the default killed the session in our test |
| Queue for offline/slow client | `max_queued_messages` (default 1000), `max_queued_bytes`; size for outage × rate. **Drops newest, silently** (log line only) | `mqtt.max_mqueue_len` (default 1000). **Drops oldest** (throttled warning). Queue lives in process memory: `force_shutdown.max_heap_size` (default 32MB) kills the session |
| Session survival across broker restart | `persistence true` (verified `session_present=true` after restart); client expiry unset or long | **Default config does not survive restart** (sessions in memory); `mqtt.session_expiry_interval` default 2h |
| Message size | `message_size_limit` ≤ Thadam `max_message_bytes` | `mqtt.max_packet_size` (default 1MB) |
| Keepalive grace | ≈1.5× (7.3 s at 5 s) | 2.0× observed at 5 s (multiplier setting 1.5) |
| Operator alert | log `Outgoing messages are being dropped` | log `dropped_msg_due_to_mqueue_is_full`, `check_oom_shutdown`, `maximum heap size reached` |

Licensing note for the compatibility matrix: `emqx/emqx:5.10.5` prints "EMQX Enterprise" and a non-commercial-use licence banner; this spike used 5.8.6. Someone should confirm which EMQX builds Thadam may test against and recommend in docs.

### 8.5 Exit and fork criteria

Accept ADR-006 (keep rumqttc) when the PR12 conformance suite passes, in CI, against both Mosquitto and EMQX: liveness (withhold ≥ 3× keepalive, 0 reconnects), ack ordering, `kill -9` redelivery with DUP, session-lost detection and resubscribe, **epoch-gated stale-ack test (E1 scenario must lose 0 messages)**, oversize configuration test, and no `select!` around `poll` (a lint or test that fails on it).

Re-open the decision and fork (or switch) if **any** of these happens:

1. We need to skip or quarantine an oversized packet instead of failing the connection (requires a codec patch).
2. A reproducible ack/loss correctness defect appears that the wrapper cannot work around.
3. A security advisory in rumqttc's dependency closure stays unfixed upstream for more than 30 days.
4. A required dependency bump breaks the conformance suite and upstream does not respond.
5. No upstream release for 12 months **and** S2b shows a maintained alternative passes this harness.

A v3.1.1-only vendored copy is about 5.8k lines; the estimate should be refreshed at the time of the decision.

### 8.6 Follow-ups

* **S2b (≤ 1 day):** run this harness against `rumqttc-v4-next` 0.34 (API differs; expect adapter code in `src/`), and compare against the exit criteria above.
* Run TLS and mTLS against both brokers with the `ring` provider.
* Repeat the Q1 matrix on target ARM hardware with realistic commit latencies; add repeated runs for variance.
* QoS 2 behaviour under `on_commit` beyond the happy path (#664).

### 8.7 Is MQTT 5 usable for the MVP?

**Not by default.** Evidence for: `receive_maximum` works (broker stopped at the client-declared 10), stripped acks and persistent sessions with DUP redelivery work, keepalive works. Evidence against: the v5 module is 8.3k lines with 37 unit tests and no integration tests, the full matrix was not run on it, `set_keep_alive` panics below 5 s, and the same oversize and cancel hazards apply (shared code paths). Recommendation: the MVP ships `protocol: v311` as the supported default; `protocol: v5` is accepted but marked experimental, enabled only after the conformance suite runs on it. Its main benefit (client-controlled window) is real; it is a good post-MVP step.

### 8.8 Plan statements to correct

| Plan reference | Statement | Finding |
|---|---|---|
| §5.9, §4.5 item 1 | queue capacity = configured inflight (3.1.1) | wrong: client has no window setting in 3.1.1; the broker sets it; a blocking bounded queue deadlocks (§5.2c) |
| §4.4 | `poll` cancel-safety unconfirmed [V S2] | resolved: **not** cancel-safe on the write path; keep the rule |
| §4.1 item 2 | stripped ack [V S2] | confirmed (QoS 1 and 2) |
| Review §0.2 | Mosquitto default window 20, EMQX 32 [V] | confirmed (Mosquitto 2.1.2: 20; EMQX 5.8.6: 32) |
| Review §4 | broker queue drops under `block` are invisible | confirmed; Mosquitto drops newest, EMQX drops oldest |
| §5.9 | rumqttc reconnects on next poll | true, but it does not resubscribe and stale acks must be epoch-gated |
| §4.2 | oversized message "rejected at the source with a counted drop" | not possible with rumqttc's codec limit; it fails the connection |

---

## 9. Limitations

* **Environment is not the target.** macOS arm64 laptop with brokers in a Docker Desktop VM behind a port forwarder; no real edge hardware, no real fsync (commit latency is simulated), no cellular-class RTT. Absolute msg/s at `L` = 0 are machine-specific; the *ratios* to the ceiling are the transferable result.
* **Single sample per Q1 cell**; no variance estimate. The machine was shared with other agents' work during runs (RTT p50 varied 95–160 µs).
* **Q1 supply**: publishers share the CPU with the subscriber. No cell was supply-limited by my own check, but at `L` = 0 the client/broker limit may include contention.
* **Two broker versions only** (Mosquitto 2.1.2, EMQX 5.8.6), single node, anonymous, no TLS, no retained messages, no shared subscriptions.
* **Q3 cannot reach every await point.** The random-drop runs and the write-blocked runs together demonstrate the hazard but do not prove which other points are safe. The write-blocked test used `AtMostOnce` fillers to fill the send buffer.
* **EMQX results are bounded by its heap/mailbox limits** (§5.1); the 5.10.x series was only smoke-tested (observation in `results/emqx/emqx-5.10.5-observation.txt`).
* **Mosquitto queue accounting (140 / 1040 / 100)** is an observed pattern with unknown mechanism.
* **MQTT 5** was smoke-tested on Mosquitto only; **QoS 2** only on the happy path; **keepalive stall** only at one keepalive (5 s).
* **Fork readiness** is metadata and source reading; `rumqttc-next` was not run. Git history is a depth-200 clone (12-month statistics are complete; older history is truncated). GitHub data is a snapshot of 2026-10-07.
* The Q1/Q6 Mosquitto data and the other Mosquitto sections come from two runs of the script (§3.5).

---

## 10. Reproduction

Prerequisites: Docker, Rust ≥ 1.88 (tested 1.95), `jq`, ~2 GiB free disk for images. The spike is standalone (own `[workspace]`); it does not touch the Thadam workspace.

```bash
cd spikes/s2-mqtt

docker pull eclipse-mosquitto:2
docker pull emqx/emqx:5.8.6          # note: there is no emqx/emqx:5 tag

# Build (optional; the scripts build on demand)
export CARGO_TARGET_DIR=$PWD/target
cargo build --release

# Full matrix, ~35 min for Mosquitto, ~16 min for EMQX (containers are removed at the end)
scripts/run_all.sh mosquitto                     # q1,q2,q3,q4,q5,q6,e1,v5
scripts/run_all.sh emqx q1,q2,q3,q4,q5,q6        # EMQX-specific limits are applied by the script
scripts/run_extras.sh                            # E2 oversize (both), E3 EMQX mailbox, E4 EMQX QoS 2

# Tables from raw JSONL
scripts/summarize.sh mosquitto > results/mosquitto/SUMMARY.txt
scripts/summarize.sh emqx      > results/emqx/SUMMARY.txt
```

Single experiments (broker started with `scripts/broker.sh start ...`; Mosquitto listens on 18831, EMQX on 18832; add `--port` accordingly):

```bash
BIN=target/release/s2-mqtt
scripts/broker.sh start mosquitto 100                                   # window 100
$BIN rtt --n 2000
$BIN throughput --broker mosquitto --broker-inflight 100 --latency-ms 10          # Q1 cell
$BIN throughput --broker mosquitto --broker-inflight 100 --latency-ms 0 --auto-ack

scripts/broker.sh start mosquitto 100 100000 false debug                # Q2
$BIN keepalive --mode withhold --broker mosquitto --broker-inflight 100
$BIN keepalive --mode nopoll   --broker mosquitto --broker-inflight 100 --pub-rate 100
$BIN keepalive --mode withhold --queue-cap 20 --broker mosquitto --broker-inflight 100

scripts/broker.sh start mosquitto 1000                                  # Q3, Q4
$BIN cancel --style now_or_never --count 100000 --pub-rate 5000 --broker mosquitto --broker-inflight 1000
$BIN cancel-stall --style now_or_never --broker mosquitto --broker-inflight 1000
$BIN strip --mode stripped --qos 1 --payload-kib 64 --count 3000 --broker mosquitto --broker-inflight 1000
$BIN strip --mode retained --qos 1 --payload-kib 64 --count 3000 --broker mosquitto --broker-inflight 1000

scripts/broker.sh start mosquitto 100 100000 false debug                # Q6
$BIN order --mode in_order --count 3000 --broker mosquitto --broker-inflight 100

scripts/run_all.sh mosquitto q5,e1                                      # Q5 (kill -9, session_present, queue limits) and E1 are scripted
scripts/broker.sh start mosquitto 1000
$BIN v5 --broker mosquitto --broker-inflight 1000 --receive-max 10

scripts/broker.sh stop
```

Fork-readiness commands (temp dir, outside the repo):

```bash
git clone --depth 200 https://github.com/bytebeamio/rumqtt.git /tmp/rumqtt-survey/rumqtt
cd /tmp/rumqtt-survey/rumqtt
git log -1 --format='%H %cd' --date=short
git log --since='2025-10-07' --no-merges --format='%ad %an %s' --date=short
find rumqttc/src -name '*.rs' -exec cat {} + </dev/null | wc -l
find rumqttc/src/v5 -name '*.rs' -exec cat {} + </dev/null | wc -l
rg -c '#\[(tokio::)?test\]' rumqttc/src rumqttc/tests
# unauthenticated GitHub API (60 requests/hour): issues #930, #846, #1056, #926 and the repo summary
curl -s https://api.github.com/repos/bytebeamio/rumqtt/issues/930 | jq -r '.title, .body'
curl -s -A thadam-spike https://crates.io/api/v1/crates/rumqttc-v4-next | jq .crate
```

TLS-provider check:

```bash
cd spikes/s2-mqtt
for c in aws-lc-sys aws-lc-rs openssl-sys native-tls ring; do cargo tree -i $c 2>&1 | head -1; done
```

Raw outputs (`spikes/s2-mqtt/results/`): `mosquitto/{q1,q2,q3,q3-stall,q4,q5,q6,e1,e2,v5}.jsonl`, `emqx/{q1,q2,q3,q3-stall,q4,q5,q6,e2,e3,e4}.jsonl`, `*/SUMMARY.txt`, `*/q5-logs/*.log` (per-process subscriber logs), `*/q2*-broker.txt`, `*/q5c-*-broker.txt` (filtered broker log lines), `emqx/e3-broker.txt`, `emqx/emqx-5.10.5-observation.txt`, and the driver logs `run-*.log`.
