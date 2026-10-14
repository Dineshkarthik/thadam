# ROS 2 live source

**Status: In progress (experimental).** ADR-0007 stage 4. The `ros2_live` source subscribes to ROS 2 topics through a [`rmw_zenohd`](https://github.com/ros2/rmw_zenoh) router and captures raw CDR payloads; datasets decode them with [`decode: ros2`](ros2.md#the-schema-store-and-decode-ros2-on-a-dataset). The mapping is the one spike S3 verified on Jazzy **and** Lyrical (byte-identical between them): data key expressions, the 33-byte publisher attachment (sequence, source timestamp, GID), and the `transient_local` late-join query.

**Only the `rmw_zenoh` transport exists in this step.** Your ROS 2 nodes must use `RMW_IMPLEMENTATION=rmw_zenoh_cpp` (Jazzy/Lyrical package `ros-<distro>-rmw-zenoh-cpp`) and reach the same router. A `zenoh-bridge-ros2dds` sidecar mode — for nodes on their stock DDS — is a later step, and only if it turns out to be needed (spike S3 §12.6 has the trade-offs).

**The source is off by default.** Build the CLI with `--features ros2-live`:

```console
$ cargo run -p thadam-cli --features ros2-live -- run pipeline.yaml
```

`thadam validate` rejects `kind: ros2_live` without the feature. The feature pulls in Zenoh 1.10.1 (pinned exactly, matching the zenoh-c 1.10.x that Jazzy and Lyrical rmw_zenoh vendor); its `zenoh-config` crate has a mandatory `serde_yaml` dependency, exceptioned in `deny.toml` for that crate only, and `option-ext` (MPL-2.0) has a one-crate license exception. `zenoh-transport` also pins `lz4_flex` ^0.10, which carries RUSTSEC-2026-0041: accepted in `deny.toml` with a review-by date (no fixed 0.10.x release exists, thadam enables no zenoh compression features, and the exposure is the ROS LAN trust boundary) — re-pin when a zenoh release accepts `lz4_flex` ≥ 0.11.6.

## How it works

```text
ROS 2 node (rmw_zenoh_cpp) ──▶ rmw_zenohd router ──zenoh client──▶ thadam ros2_live source
                                                                      │  raw CDR + schema store
                                                                      ▼
                                                             dataset decode: ros2
```

The source opens a Zenoh **client** session to the router and subscribes to each configured topic with `<domain>/<topic>/**`, learning the concrete type name and RIHS01 hash from each sample's key. Client mode with multicast scouting off is the spike's verified recipe: peer mode receives nothing in a Docker/Desktop topology, and multicast must not leak onto the LAN. Set the same on the ROS side via `ZENOH_CONFIG_OVERRIDE='connect/endpoints=["tcp/<router-host>:7447"];scouting/multicast/enabled=false'`.

Because a live stream has no recording to read schemas from, the source assembles each topic type's `ros2msg` schema text from the `.msg` files under `msg_dirs` (the ROS package layout `<dir>/<pkg>/msg/<Type>.msg`), compile-checks it at startup, and writes it to the pipeline's [schema store](../operations/data-directory.md#the-schema-store-schemas) **before the first message of that type can be committed to the WAL** — the same durability contract as the MCAP source. Datasets then use `decode: ros2` exactly as with MCAP files.

## Configuration

Unknown keys are rejected at every level. Durations are whole seconds (`30`) or a unit string (`500ms`, `10s`, `5m`, `1h`).

```yaml
sources:
  - id: robot
    kind: ros2_live
    config:
      endpoint: tcp/127.0.0.1:7447
      domain: 0
      msg_dirs: [/opt/ros/jazzy/share]
      topics:
        - { name: /imu, type: sensor_msgs/msg/Imu }
        - { name: /joint_states, type: sensor_msgs/msg/JointState }
        - { name: /tf_static, type: tf2_msgs/msg/TFMessage, transient_local: true }
      reconnect: { min_backoff: 1s, max_backoff: 30s }

datasets:
  - id: imu
    match: { source: robot, topic: /imu }
    decode: ros2
    time: { field: /header.stamp, unit: ns, domain: utc }
    fields:
      - { id: 1000, name: header.stamp, type: ts, unit: ns, required: true }
      - { id: 1001, name: header.frame_id, type: str, required: true }
      - { id: 1002, name: angular_velocity.z, type: f64 }
```

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | — | **Required.** Zenoh locator of the `rmw_zenohd` router, for example `tcp/127.0.0.1:7447`. The session is a Zenoh client with multicast scouting off. |
| `domain` | integer | `0` | ROS domain id; must match the publishers' `ROS_DOMAIN_ID`. |
| `msg_dirs` | list | — | **Required, non-empty.** Directories holding `.msg` definitions in the ROS layout (`<dir>/<pkg>/msg/<Type>.msg`), for example `/opt/ros/jazzy/share`. Later entries are fallback search paths. A missing or unparseable definition fails the build. |
| `topics` | list | — | **Required, non-empty.** The topics to subscribe — an explicit list; liveliness discovery is not implemented (see [limits](#limits)). |
| `topics[].name` | string | — | The ROS topic with a leading slash, for example `/imu`. The subscription is `<domain>/<name>/**`, so nested sub-topics match too; samples from a sub-topic you did not configure are skipped with a warning. |
| `topics[].type` | string | — | The ROS type the publisher sends, `pkg/msg/Type` (or `pkg/Type`), for example `sensor_msgs/msg/Imu`. Its definition must resolve under `msg_dirs`. A sample whose wire type differs is skipped with a warning (the schema would not decode it anyway). |
| `topics[].transient_local` | boolean | `false` | Run the `transient_local` late-join query for this topic — needed for `/tf_static`, so a late-joining pipeline receives what was published before it subscribed. The query is spike S3 §5.4's four-stricture cache query, scoped to exactly one topic. |
| `reconnect.min_backoff` | duration | `1s` | First retry delay after a lost session; doubles (jittered) with consecutive failures. |
| `reconnect.max_backoff` | duration | `30s` | The most a retry delay grows to. |

## Messages, attributes, and delivery class

Each sample becomes one raw message (the CDR payload, including the 4-byte encapsulation header) with these attributes:

| Attribute | Value |
|---|---|
| `topic` | The ROS topic with leading slash (the normalized attribute read by `match.topic` filters and `{topic.N}` identity templates). |
| `ros2.type` | The wire type from the key expression, for example `sensor_msgs/msg/Imu`. |
| `ros2.type_hash` | The RIHS01 type hash from the key expression (`RIHS01_<64 hex>`). Informational: schema keying is by type name — `thadam-ros2-msg` does not compute REP-2016 hashes, so it cannot verify the hash against the local `.msg` text. |
| `ros2.seq` | The publisher's per-publisher sequence number, decimal. Absent when the attachment is not well-formed. |
| `ros2.gid` | The 16-byte publisher GID as lowercase hex. Same presence rule. |
| `ros2.source_time` | Publisher-side timestamp, nanoseconds since the epoch, decimal. Same presence rule; usable as `time: { attr: ros2.source_time, unit: ns, domain: utc }`. |
| `ros2.late_join` | `"true"` when the sample came from the transient_local late-join query rather than live delivery. |
| `mcap.schema_hash` | The hex SHA-256 key of the type's assembled schema in the [schema store](../operations/data-directory.md#the-schema-store-schemas) (the shared contract attribute); the chain's `decode: ros2` resolves schema text with it. |
| `ros2.dropped` | Running total of counted losses, stamped only when nonzero. |

Delivery is **at-most-once** (`ack_mode: none`) — there is no acknowledgement back to the publisher, and no re-read after a crash; capture restarts live. Loss is never silent:

- The rmw_zenoh attachment's per-publisher sequence numbers are tracked per publisher GID. A jump means the router or network dropped messages between the publisher and thadam; the skipped count is logged and added to `ros2.dropped` (spike S3 §6.4 verified the accounting under sustained overload).
- The receive task never blocks the Zenoh callback: a full 4,096-message queue drops new messages at the client, counts them in the same `ros2.dropped` total, and logs every 10,000th drop. The two counters are disjoint (overflow drops follow an observed sequence number, so they never count as gaps).

A lost session (router restart, network partitioning) is re-established with jittered exponential backoff between `reconnect.min_backoff` and `max_backoff`; everything published while disconnected is gone (at-most-once), and sequence gaps on reconnect are counted like any other loss.

`ingest_time` is the receive time on the thadam host. For event time use the message clock (`time: { field: /header.stamp, unit: ns }`) or the attachment's publisher stamp (`time: { attr: ros2.source_time, unit: ns }`) — the spike measured both agreeing within 0.1 ms.

## Limits

- **rmw_zenoh only.** Fast DDS / CycloneDDS nodes need the (future) bridge sidecar or must switch their RMW. Humble's rmw_zenoh is zenoh 0.11 and does not interop with the 1.10 client (ADR-0007); Jazzy and Lyrical are the verified distros, including cross-distro both ways.
- **Explicit topics.** There is no `@ros2_lv` liveliness discovery in this step; every topic is declared with its type.
- **Zenoh version lockstep.** The client is pinned to zenoh 1.10.1 to match the zenoh-c 1.10.x Jazzy/Lyrical rmw_zenoh vendors; re-verify when a distro ships zenoh ≥ 1.11.
- **Type hash not verified.** Schema keying is by type name (see `ros2.type_hash` above), so the `.msg` texts under `msg_dirs` must be the same definitions the publishers were built with, or decoding silently mismatches. Point `msg_dirs` at the ROS installation the nodes use.
- **No ARM numbers yet.** The spike's reference runs are an arm64 laptop (≈6–7 % of one core, zero unaccounted drops, 1 kHz IMU + 100 Hz joint_states + 30 Hz /tf including decode); ARM reference hardware numbers are the owner's to measure.
- **Zenoh client on a reachable router.** The verified topology is the router in Docker with `127.0.0.1:7447` published (or any TCP-reachable host); client mode means the router never dials back into thadam.

## Trying it with Docker

The crate's end-to-end test spins up `rmw_zenohd` and the sensor-mix publisher (public `ros:jazzy-ros-base` image, pinned) from `crates/thadam-ros2-live/tests/assets/docker` and runs the real pipeline for ~10 s:

```console
$ cargo nextest run -p thadam-cli --features ros2-live --run-ignored only -E 'binary(ros2_live_docker)'
```

It is `#[ignore]`d by default; unit tests run without Docker.
