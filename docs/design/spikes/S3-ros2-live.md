# Spike S3: live ROS 2 transport — step 1: Zenoh-native vs Jazzy + rmw_zenoh

| | |
|---|---|
| Status | Step 1 complete (evidence for ADR-0007 stage 3; the ADR itself is not edited here) |
| Date | 2026-10-12 |
| Code | `spikes/s3-ros2-live/` (standalone Cargo project, own `[workspace]`; root workspace excludes `spikes/`) |
| Raw results | `spikes/s3-ros2-live/results/` (capture summaries, sniff/probe JSONL; gitignored) |
| ROS subject | ROS 2 Jazzy, `ros:jazzy-ros-base` + `ros-jazzy-rmw-zenoh-cpp` 0.2.11-1noble.20260915.184907 |
| Rust subject | `zenoh` **=1.10.1** exact pin, `default-features = false` + `transport_tcp` (no TLS, no QUIC, no ws, no unstable feature) |

Every number below comes from a recorded run in `results/`. Where something was not measured, it says so. ARM (Pi 5) results are **NOT MEASURED** — the owner measures ARM on reference hardware later; this Mac is a reference run only.

---

## 1. Headline findings

1. **The topology works, and the fix was connectivity, not the client.** A raw Rust `zenoh` client on the macOS host captures Jazzy `rmw_zenoh` traffic at full rate through `rmw_zenohd` in a Docker container (Docker Desktop port publishing). The previous run's failure (`zenoh::open: Unable to connect to any of [tcp/127.0.0.1:7447]`, multicast-scouted `[::1]` peers) was a stale manually-started container that did not publish 7447, plus client-side multicast scouting left on. The fixes are in §4.
2. **Client mode, not peer mode.** Measured: client-mode host session → full-rate data (6786 samples/6 s sniff; zero-loss 60 s baselines). Peer-mode host session → **zero samples in 8 s**, gossip noise about unreachable `[::1]` locators from the container sessions. rmw_zenoh's own design expects the router to broker traffic for off-bridge consumers ("All communications with other hosts are routed by the Zenoh router", rmw_zenoh `docs/design.md`).
3. **All four required QoS behaviors verified on Jazzy + rmw_zenoh (60 s runs).** Zero unaccounted drops and zero decode errors on `/imu` (best_effort ~998 Hz actual), `/joint_states` (reliable 100 Hz), `/tf` (reliable 30 Hz); transient_local `/tf_static` received by a late joiner exactly once via the cache query (§6.1). Under artificial subscriber overload, best_effort drops are **counted** via attachment sequence gaps (§6.4).
4. **Transient_local late join took real protocol surgery — this is the spike's main mapping result.** The naive "query the data key expression" approach gets zero replies, for four independent, measured reasons (§5.4): the cache queryable lives under an `@adv` chunk that wildcards never match; the querier must set `_anyke` or both zenoh ends reject the disjoint reply keys; monotonic consolidation would drop replayed samples; and `QueryTarget::AllComplete` excludes a cache that declares no completeness flag. All four are now encoded in `src/zenoh_impl.rs` and proven by the `--no-late-join` control run.
5. **Two dependency-policy findings block the Zenoh feature as-is under ADR-0020/root `deny.toml`: `serde_yaml` 0.9.34 is a *mandatory* dependency of `zenoh-config` 1.10.1 (root bans `serde_yaml`), and `option-ext` 0.2.0 (MPL-2.0) is pulled in via `zenoh-util → shellexpand → dirs` (root allow-list has no MPL).** Both need an owner decision before M2-C (§7.3). The `EPL-2.0 OR Apache-2.0` election itself checks out: every zenoh-family crate satisfies the Apache-2.0 allow.
6. **Recommendation for step 2: proceed with `zenoh-bridge-ros2dds` + Fast DDS on Jazzy, reusing the harness, trait, and metrics, but expect to re-derive the key-expression and late-join mapping** (`@ros2_lv` tokens and the `@adv` cache are rmw_zenoh session internals; the bridge uses zenoh-plugin-ros2dds' own key scheme and publication cache layout — re-run the same sniff/probe procedure, §9).

## 2. Environment

| Item | Value |
|---|---|
| Host | Apple Silicon (arm64), arm64, macOS |
| Container runtime | Docker Desktop |
| Toolchain | rustc/cargo 1.99.0 (b940084d7 2026-09-28), rustfmt 1.10.0, clippy (same toolchain) |
| Base image | `ros:jazzy-ros-base` (Docker Hub, pinned by tag in `docker/Dockerfile`; built image `s3-ros2-zenoh:jazzy-1` = sha256:b0ff655972a6, 303 MB) |
| ROS packages | `ros-jazzy-rmw-zenoh-cpp` 0.2.11-1noble.20260915.184907 (recorded in the image at build time, `docker run ... /usr/local/lib/s3/versions.txt`), `ros-jazzy-tf2-msgs` |
| Spike crate | `zenoh =1.10.1` (default-features off + `transport_tcp`), `thadam-ros2-msg` / `thadam-core` (path deps), tokio 1.53.2, clap, serde_json, hdrhistogram 7.5, tracing(-subscriber) — full lock: `spikes/s3-ros2-live/Cargo.lock` |

## 3. Method

### 3.1 Harness

`spikes/s3-ros2-live/docker/` builds one image used for both containers. `docker/compose.yaml` is the canonical definition (router service `rmw_zenohd` with `127.0.0.1:7447:7447` published; `sensor-pub` on the same bridge). Runs here used `docker/run.sh`, a plain-docker replica of the compose file, because this dev sandbox admits `docker run/build` but not `docker compose up`. The two files are kept in sync (identical env: `RMW_IMPLEMENTATION=rmw_zenoh_cpp`, `ROS_DOMAIN_ID=0`, `ZENOH_CONFIG_OVERRIDE='connect/endpoints=["tcp/<router>:7447"];scouting/multicast/enabled=false'`).

The publisher (`docker/sensor_pub.py`) publishes from one rclpy node, logging cumulative counts every 5 s as ground truth:

| topic | type | rate (target) | QoS |
|---|---|---|---|
| `/imu` | `sensor_msgs/Imu` | 1 kHz (actual ~998 Hz, rclpy timer) | best_effort, volatile, depth 5 |
| `/joint_states` | `sensor_msgs/JointState` | 100 Hz | reliable, volatile, depth 10 |
| `/tf` | `tf2_msgs/TFMessage` | 30 Hz | reliable, volatile, depth 100 |
| `/tf_static` | `tf2_msgs/TFMessage` | once, ~1 s after node start | reliable, **transient_local**, depth 1 |

The Rust binary (`spikes/s3-ros2-live`) runs natively on the host with `RUST_LOG=warn` (a previous debug-level run produced >10 MiB of truncated zenoh logs; all measurements below are at warn). Subcommands: `sniff` (raw sample/liveliness dump), `discover` (liveliness-token topic listing), `capture` (subscribe matrix, decode, measure), `probe` (query-selector reachability matrix, built during the late-join investigation).

### 3.2 The Thadam trait and decoding

`src/transport.rs` defines the spike's candidate-neutral trait: `subscribe(topic, type?, qos) -> stream` of `{topic, type_name, type_hash, payload_cdr, source_timestamp, recv_time, seq, gid, late_join}` plus `discover(settle)`. `src/zenoh_impl.rs` implements it over raw `zenoh`. Payloads are decoded schema-first with `thadam-ros2-msg` using vendored Jazzy `.msg` files (`msgs/`), compiled once per type, one decode per sample.

### 3.3 Clocks and CPU/RSS

Publisher-side stamps (rmw_zenoh attachment `source_timestamp`, ROS `header.stamp`) are on the **container** clock; receive stamps on the **host** clock. `scripts/clock_offset.py` estimates container−host offset from a persistent `docker exec` shell, keeping the minimum-RTT sample (offset error bound = rtt_min/2, recorded per run: −0.76 to −0.91 ms offsets, rtt_min 0.42–0.62 ms). `scripts/run-capture.sh <prefix> -- <capture args>` measures the offset, snapshots publisher counts before/after, runs the capture, and samples the capture process `%cpu`/RSS via `ps` every 2 s. Method note: macOS `ps %cpu` is a decaying average; RSS is exact at sample time.

### 3.4 Evidence rule and run history (for honesty)

All criteria are backed by `results/capture-*.{json,log,ps,publog,offset}` from the final code. Earlier artifacts that only recorded failed connections (the previous run's `results/sniff-*` series) were deleted per the task instructions; their content is summarized in §4. One operational incident is relevant: at 09:19:50 the publisher container exited (code 1) immediately after a graph-wide `**/@adv/**` probe query produced a burst of AdvancedCache rejected-reply warnings on its RX thread — see §5.4/§8. The stack was restarted and all reported measurements were re-run afterwards.

## 4. Connectivity: host client → rmw_zenohd in Docker

What was broken before, what works now, measured:

| Setting | Previous run | This run |
|---|---|---|
| Router listen/port | stale `s3-zenoh` container; **7447 not published on the host** (`nc` connection refused) | compose/run.sh publishes `127.0.0.1:7447:7447`; `nc -vz 127.0.0.1 7447` OK before any Rust debugging |
| Host session mode | client-mode attempts never got data (diagnosed while port was down) | **client** mode: works at full rate. **peer** mode also re-measured: 0 samples/8 s, "Scouting delay elapsed before start conditions are met", gossip warnings dialing container-advertised `[::1]` locators |
| Multicast scouting | on (found phantom LAN/container peers) | off on host session (`scouting/multicast/enabled=false`) and on ROS nodes via `ZENOH_CONFIG_OVERRIDE` (rmw_zenoh's default config already disables it; set explicitly for determinism) |
| Connect endpoint | — | explicit `connect/endpoints=["tcp/127.0.0.1:7447"]` (host) / `["tcp/router:7447"]` (nodes, by compose service name) |

Two notes for later stages: (a) `rmw_zenohd`'s default router config listens `tcp/[::]:7447` inside the container — dual-stack, publishes fine through Docker Desktop's forwarder; no override was needed. (b) In client mode the host never advertises a listener, so container peers never try (and fail) to dial it through the desktop VM's NAT — the clean topology for Docker-for-Mac targets.

## 5. Mapping (verified live by `sniff`/`discover`/`probe`)

### 5.1 Data key expressions

`<domain>/<topic_fqn_without_leading_slash>/<dds_registered_type>/<RIHS01_type_hash>`; a subscriber declares on `<domain>/<fqn>/**` and learns type+hash from each concrete key. Examples captured 2026-10-12 (`results/sniff-final.jsonl`, 6786 samples/6 s):

```
0/imu/sensor_msgs::msg::dds_::Imu_/RIHS01_7d9a00ff131080897a5ec7e26e315954b8eae3353c3f995c55faf71574000b5b
0/joint_states/sensor_msgs::msg::dds_::JointState_/RIHS01_a13ee3a330e346c9d87b5aa18d24e11690752bd33a0350f11c5882bc9179260e
0/tf/tf2_msgs::msg::dds_::TFMessage_/RIHS01_e369d0f05a23ae52508854b66f6aa0437f3449d652e8cbf22d5abe85d020f087
0/tf_static/tf2_msgs::msg::dds_::TFMessage_/RIHS01_e369d0f05a23ae52508854b66f6aa0437f3449d652e8cbf22d5abe85d020f087
```

DDS registered type name → ROS type: `pkg::msg::dds_::Type_` → `pkg/msg/Type` (`src/zenoh_impl.rs::dds_to_ros_type`). Payload is CDR **with the 4-byte encapsulation header** (`00 01 00 00` = XCDR1 little-endian here; decoder covers both endians).

### 5.2 Attachment (per-sample metadata)

33 bytes, verified well-formed on every sampled topic: `int64-LE sequence number | int64-LE source_timestamp ns | u8 gid_len (=16) | 16-byte publisher GID`. The sequence number is per-publisher and starts at 1 — this is the drop-accounting basis; the type hash in the key is the ADR-0007 decoder key complement. One sender GID per topic was observed for the whole run.

### 5.3 Discovery (liveliness)

rmw_zenoh liveliness tokens at `@ros2_lv/<domain>/<session_zid>/<node_id>/<entity_id>/<kind>/...`; `@`-prefixed chunks are hermetic to `*`/`**`, so data subscribers never see them — discovery is a separate `session.liveliness().declare_subscriber("@ros2_lv/<domain>/**").history(true)` stream. Entity kinds seen: `NN` (node), `MP` (publisher, carries topic/type/hash/QoS), `MS` (subscription service clients), `SS` (service servers, e.g. `get_type_description`). `/` is mangled to `%` inside token names (`%joint_states` = `/joint_states`). `MP` QoS key expressions observed (opaque compact encoding, recorded verbatim):

| topic | qos keyexpr |
|---|---|
| `/imu` | `2::,5:,:,:,,` |
| `/joint_states` | `::,10:,:,:,,` |
| `/tf` | `::,100:,:,:,,` |
| `/tf_static` | `:1:,1:,:,:,,` |
| `/rosout` | `:1:,1000:,:10,0:,,` |

### 5.4 transient_local late join (the hard part)

rmw_zenoh implements `TRANSIENT_LOCAL` with a zenoh-ext `AdvancedPublisher` holding an `AdvancedCache`, answered by a **non-complete** queryable at `<data_ke>/@adv/pub/<publisher_zid>/<entity_id>` (trailing `uhlc` chunk for timestamp-sequenced publishers, trailing empty chunk — a routing workaround — for sequence-numbered ones). Late-join working query (mirrors zenoh-ext `AdvancedSubscriber`):

```
Session::get("<domain>/<fqn>/**/@adv/**")
    .accept_replies(ReplyKeyExpr::Any)        // selector param `_anyke`
    .consolidation(ConsolidationMode::None)
    .target(QueryTarget::All)
    .timeout(2s)
```

Each stricture was a measured failure before it was added:

1. **Literal `@adv` required**: zenoh wildcards never match `@`-leading chunks, so `<domain>/<fqn>/**` never routes a query to the cache (0 replies, 0 ms elapsed).
2. **`_anyke` required**: the cache replies with the *original data key*, which does not intersect the `@adv` query — zenoh rejects the reply on the queryable side (`Attempted to reply on '0/tf_static/...' which does not intersect with query '...'`, seen in the publisher's logs) and drops it again on the querier side.
3. **Consolidation None**: monotonic consolidation treats a replay as a duplicate of a same-key live sample.
4. **`All`, not `AllComplete`**: `AdvancedCacheBuilder` sets no completeness flag.
5. **Scope the selector to one topic.** A graph-wide `**/@adv/**` query fans out to every cached publisher and costs each one a rejected-reply callback per cached sample; the burst above preceded (and correlated with) the publisher container exiting (§8, item 3).

Replies arrive as clones of the original samples (original key, payload, attachment, source info), so late-join samples go through the same decode/metrics path, flagged `late_join`.

## 6. Results

### 6.1 Baseline, 60 s, client mode (`results/capture-baseline-60s.*`)

Per-topic (received vs expected from attachment sequence accounting; decode errors; latency p50/p99 host-recv minus publisher stamp, offset-corrected; subscriber CPU/RSS from `ps` samples):

| topic | recv | decode err | drops (seq gaps) | latency p50 | p99 | publisher Δ (ground truth, ±5 s window) |
|---|---|---|---|---|---|---|
| `/imu` (best_effort ~998 Hz) | 59 817 / 59 817 | 0 | **0 (0 gaps)** | 2.08 ms | 2.48 ms | 84 897 → 154 617 (≈59.7 k) |
| `/joint_states` (reliable 100 Hz) | 5 999 / 5 999 | 0 | **0** | 2.05 ms | 2.66 ms | 8 500 → 14 997 (6 497 over >60 s) |
| `/tf` (reliable 30 Hz) | 1 800 / 1 800 | 0 | **0** | 2.06 ms | 2.54 ms | 2 550 → 4 500 (1 950 over >60 s) |
| `/tf_static` (transient_local, published before connect) | **1 (late_join=1)** | 0 | 0 | n/a | n/a | 1 |

- Subscriber process: **avg 6.9 % of one core (max 8.9 %), RSS 10 MiB**, decoding all messages with `thadam-ros2-msg` (incl. 59.8 k Imu messages), 8 tokio worker threads.
- Max latency across all topics: 18.4 ms (one `/imu` outlier); attachment-stamp and header-stamp latencies agree within 0.1 ms, cross-checking the offset method.
- First decoded records are in the summary JSON (`first_decoded`), e.g. `/tf_static` = `world → mount_link`, matching the publisher script.

### 6.2 Control: no late join (`results/capture-no-latejoin-15s.*`, `--no-late-join`, 15 s)

`/tf_static` **0 received** while all other topics stream (14 962 imu, 1 498 joint_states, 450 tf, 0 drops). Proves the transient_local delivery in 6.1 comes from the cache query, not live delivery.

### 6.3 Control: late join under overload

In the overload run (§6.4) `/tf_static` again arrives exactly once via the late-join path.

### 6.4 Overload, 60 s (`results/capture-overload-60s.*`, collector sleeps 2 ms per message → drain ≈ 500 msg/s < 1 130 msg/s offered)

Backpressure chain: bounded fan-in (32) → per-subscription zenoh FifoChannel (256) → zenoh rx thread blocks → the router's egress queue for the client-side session drops per zenoh congestion control.

| topic | recv | expected (in observed seq window) | **counted drops (gaps)** | notes |
|---|---|---|---|---|
| `/imu` (best_effort) | 14 131 | 14 679 | **548 (2 gap events)** | drop criterion met |
| `/joint_states` (reliable) | 1 852 | 2 537 | **685 (5 gaps)** | "reliable" is retry-within-buffers, not lossless, under persistent overload |
| `/tf` (reliable) | 556 | 761 | **205 (5 gaps)** | same |
| `/tf_static` | 1 (late_join=1) | 1 | 0 | cache query unaffected |

Latency under backlog: p50 6.3 s `/imu`, 12.1 s `/joint_states` `/tf` (queue drain delay — expected, mechanically measured). RSS 21 MiB (bounded by channel capacities; no growth trend), CPU 2.1 % (mostly sleeping). Publisher held rate throughout (its logs: 69 847 imu over ~70 s).

### 6.5 Owner criteria scoreboard (2026-10-10)

| Criterion | Result | Evidence |
|---|---|---|
| Jazzy + rmw_zenoh capture (required) | **PASS** | §6.1 |
| reliable topics behave as configured | **PASS** (0 drops unloaded; drops under sustained overload are counted, not silent) | §6.1, §6.4 |
| best_effort sensor topic behaves as configured, drops counted under overload | **PASS** (548 counted, 2 gap events) | §6.4 |
| transient_local `/tf_static` behaves as configured (late join) | **PASS** (exactly-once via cache query; 0 without the query) | §6.1–6.3 |
| Jazzy via zenoh-bridge-ros2dds + Fast DDS (required) | step 2 — NOT MEASURED here | §9 |
| Lyrical + rmw_zenoh / Lyrical bridge / Humble bridge / Cyclone variants | NOT MEASURED (step 1 scope = Jazzy) | — |
| ARM Pi 5: 1 kHz IMU + 100 Hz joint_states + 30 Hz /tf at <25 % of one core, no unaccounted drops | **NOT MEASURED** (owner reference hardware; this run is the Mac reference: no unaccounted drops, 6.9 % of one Apple Silicon core) | §6.1 |
| `Gap` marker / counted-drop plumbing | **PASS** at measurement level (missing counted per (gid, seq); gap events surfaced in summary JSON) | §6.4 |

## 7. Dependency weight

### 7.1 Tree, build, binary

| Measure | Value |
|---|---|
| Resolved crates (this target, normal edges) | 301 |
| Zenoh-family crates | 20, all `zenoh* =1.10.1`, all `EPL-2.0 OR Apache-2.0` |
| Cold release build (`cargo clean` → `--release`) | **39.27 s** (incremental ≈ 3.5 s) |
| Binary `s3-ros2-live` | 16 222 528 B (15.5 MiB, line-tables-only debug) / **10 569 120 B (10.1 MiB) stripped** |
| `cargo tree -i aws-lc-sys` | empty (no match) |
| `cargo tree -i openssl-sys` | empty (no match) |

### 7.2 Licenses (from `cargo metadata`; full table in results notes)

311 resolve entries: 175 MIT OR Apache-2.0, 41 MIT, 26 EPL-2.0 OR Apache-2.0, 15 Apache-2.0 OR MIT, 12 MIT/Apache-2.0, 8 Unlicense OR MIT, 8 Zlib, 5 Apache-2.0 WITH LLVM-exception OR …, 3 EPL-2.0 OR Apache-2.0 (with leading blank), 2 Apache-2.0, 2 BSD-2-Clause, 2 ISC, 2 BSD-2-Clause OR Apache-2.0 OR MIT, and one each of 0BSD OR MIT OR Apache-2.0, Apache-2.0/MIT, MIT OR Zlib OR Apache-2.0, **MPL-2.0**, MIT OR Apache-2.0 OR LGPL-2.1-or-later, Apache-2.0 OR BSL-1.0, (NONE = the spike crate itself), BSD-3-Clause, Zlib OR Apache-2.0 OR MIT, (MIT OR Apache-2.0) AND Unicode-3.0.

### 7.3 `cargo deny` against a spike-local mirror of root `deny.toml` (`spikes/s3-ros2-live/deny.toml`)

`cargo deny check licenses bans` output (real):

- **`serde_yaml = 0.9.34+deprecated` is explicitly banned — reachable, mandatory**: `serde_yaml ← zenoh-config v1.10.1 ← zenoh v1.10.1`. It is a *mandatory* dependency (checked: no feature gate in `zenoh-config-1.10.1/Cargo.toml`). **Decision needed**: either an ADR-0020 exception for `serde_yaml` (dead config-parsing code path — the spike never uses YAML config — but it is unmaintained code), an upstream change to make it optional/remove it, or a zenoh version that already did (check at M2-C).
- **option-ext 0.2.0 MPL-2.0 not allowed**: `option-ext ← dirs-sys ← dirs ← shellexpand ← zenoh-util`. MPL-2.0 is file-scoped weak copyleft; linking is fine legally (larger work stays Apache-2.0), but root policy doesn't list it. **Decision needed**: add MPL-2.0 to the allow-list or exception for `option-ext`.
- `error[unlicensed]: s3-ros2-live` — the spike crate itself has no `license` field; cosmetic (publish=false).
- Every zenoh crate (`EPL-2.0 OR Apache-2.0`) **passes** via the Apache-2.0 allow — the ADR-0020 election works as written.
- Warnings: `block-buffer` duplicated (multiple-versions = warn by policy), `stabby-abi` via `zenoh-plugin-trait`; `target-lexicon` root exception unused in this tree.
- Duplicate versions of `proc-macro2`/`quote`/`syn`... none beyond warn-level duplicates observed.

## 8. Problems encountered (honest record)

1. **File-edit racing in the dev environment silenced several code edits** (detected via the compiler); all source was rewritten whole and re-verified with `cargo build`/`clippy`/`fmt` — final state is authoritative in git status.
2. **clap negative argument**: `--clock-offset-ns -801416` parses as a flag; fixed by `--flag=value` form in `run-capture.sh`.
3. **Publisher exit at 09:19:50**: `s3-sensor-pub` exited code 1 immediately after the graph-wide `**/@adv/**` probe triggered a burst of AdvancedCache rejected-reply warnings. Correlation is certain (timestamps match the probe run to the second); mechanism is not (no traceback in the retained log tail; container was replaced before full logs were pulled). Operational lesson already encoded: never issue broad `@adv` wildcard queries; scope late-join queries to one topic and set `_anyke` so replies are accepted on first contact.
4. My own `docker kill <image-filter>` at 09:39 took the router down mid-investigation; stack was restarted and all reported runs are afterward.
5. Previous run's conclusion "client mode receives nothing" was wrong — it was measured while the port was unreachable; re-measured here both ways (§4).

## 9. Recommendation for step 2 (zenoh-bridge-ros2dds + Fast DDS, Jazzy)

**Proceed.** What carries over unchanged: the `LiveRos2Transport` trait and its 16-field record, the metrics/drop-accounting/latency machinery, the vendored `.msg` registry + Jazzy publisher harness, the clock-offset + run-capture scripts, the Docker networking pattern (client mode + published 7447 + scouting off), and the sniff/discover/probe tooling.

What must be re-derived (expected mapping deltas, to be verified with the same procedure — **do not assume reuse without re-measurement**):

- **Key expressions.** zenoh-bridge-ros2dds publishes DDS topics on its own key scheme (`<scope>/<fqn>/<type>/<...>` variants across versions; `@ros2_lv` liveliness tokens are rmw_zenoh session internals, the bridge's discovery uses its own admin/topic space). Re-run `sniff`/`discover` against the bridge and re-pin `parse_data_keyexpr` and discovery.
- **Attachments.** The 33-byte rmw_zenoh attachment is an *rmw* implementation detail. Whatever the bridge attaches for sequence/timestamp/GID (if anything) must be re-sniffed; drop accounting needs a per-publisher sequence from somewhere — if the bridge doesn't provide one, drops can only be counted via header-stamp deltas, and that's a QoS-fidelity gap to record against the owner criteria.
- **Late join.** The bridge serves transient_local differently (its own relay cache on the DDS side and/or zenoh storage); the `@adv` + `_anyke` query is zenoh-ext `AdvancedPublisher`-specific. Re-probe.
- **QoS**: Fast DDS reliable/best_effort interplay with the bridge's congestion settings needs the same baseline/overload runs; Cyclone variant feeds the same matrix.

Suggested step-2 starting point: reuse `docker/` to add a `bridge` service (`eclipse/zenoh-bridge-ros2dds:<pinned-tag>`, Fast DDS loopback-enabled participants in one container), keep the host capture binary untouched, and first run `sniff`+`probe` to bank the new mapping before the 60 s matrix. The two license findings (§7.3) do not block step 2 (no new crates), but must be resolved before the M2-C feature flag can exist in the workspace.

## 10. What was NOT done

- **roslibrust + hiroz (optional, 30 min time-box): not attempted.** The time-box was consumed by root-causing the transient_local query protocol (§5.4) and the connectivity forensics above; ADR-0007 marks hiroz "experimental, no production claim", and the raw-zenoh path now works, so the marginal evidence was the least valuable item on the list.
- Lyrical/Humble/Cyclone and the zenoh-bridge-ros2dds runs: step 2 scope.
- ARM measurements: owner reference hardware, later (see §6.5).
- Golden CDR fixtures across distributions, fuzz-in-CI: already tracked in ADR-0007's evidence list (unaffected).
