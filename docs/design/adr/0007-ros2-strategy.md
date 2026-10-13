# ADR-0007: ROS 2 strategy

- **Status:** Draft
- **Date:** 2026-10-07
- **Deciders:** Project owner

## Context

Thadam captures messages from robotic and IoT sources. product.md section 7 lists "ROS 2 / DDS-compatible ingestion" as a P0 protocol and section 31 makes it one of two MVP sources. ARCHITECTURE.md section 1 places ROS 2 in milestone M2, after the MQTT-to-Parquet core (M1), and says that if capacity tightens the cut order is ClickHouse first, then live ROS 2; M1 is never cut. These two statements conflicted. The owner resolved the conflict on 2026-10-07 (decision B2, D-B2-2 in `docs/design/owner-decisions.md`): ROS 2 is NOT in v0.1, and ROS 2 (MCAP first) is M2. The product.md amendment (`docs/design/product-amendments.md` Part A) was approved on 2026-10-07 and applied to product.md on 2026-10-13.

**This ADR follows the owner's decision: ROS 2 starts at M2.** Stages 1 and 2 below (the CDR decoder and MCAP file source) are the first M2 deliverables. Stage 3 (the live-transport spike) follows its own evidence.

### Forces

- **Robots run several ROS 2 distributions at once.** The lifecycle facts below were checked on 2026-10-07 against REP-2000 and the ROS documentation (ecosystem maturity review, section 1).

  | Distribution | Status |
  |---|---|
  | Humble | LTS, supported until May 2027 |
  | Jazzy | LTS, supported until May 2029 |
  | Kilted | Non-LTS, end of life November 2026 |
  | Lyrical Luth | LTS, released May 2026, supported until May 2031 |

  REP-2000 on master does not yet list Lyrical's platform or middleware tiers **[V]**.
- **rmw_zenoh** (the ROS 2 middleware layer, RMW, that uses Zenoh) is Tier 1 in Kilted. It is not tiered for Jazzy or Humble, although binary packages exist per distribution branch. Its README warns that it is incompatible between Humble and newer distributions, with messages silently dropped.
- **Robots on DDS** (the Data Distribution Service, the default ROS 2 transport) use Fast DDS (the Jazzy default) or Cyclone DDS. Fast DDS has no maintained Rust binding.
- **Rust bindings that need a ROS install** (rclrs, r2r) tie the build, and for r2r the run, to a sourced ROS environment. Thadam ships binaries (ARCHITECTURE.md section 16), so a build-time ROS dependency would enter CI and release engineering.
- **A data plane is generic.** Thadam captures whatever topics the user configures. Compile-time message code generation does not fit this, so Thadam needs a schema-driven decoder regardless of transport.
- **Best-effort ROS 2 sensor topics have no flow control.** Under overload the only options are a counted drop or an overflow; the drop is explicit, with a metric and a `Gap` marker (ARCHITECTURE.md section 10, "Backpressure").
- **Replay.** MCAP is an export and import format only, not a recording tap (D24). `thadam export --mcap` ships with the MCAP work in M2 (ARCHITECTURE.md section 20). The recording format itself is ADR-0015 (Reserved).

## Decision

Per ADR-0000, this ADR is Draft: options and evidence requirements are listed and nothing below is binding. The text states the **current lean, which is provisional**. ROS 2 support is staged so that each stage is useful and reversible without the next.

### Stage 1: `thadam-ros2-msg` (transport-independent)

- A `.msg` and IDL (interface definition language) parser plus a schema-driven CDR (Common Data Representation) decoder that produces `Record`s conforming to the dataset shape (ADR-0001). There is no `Ros2Msg` variant in the core model.
- Decoders are keyed by **type name plus type hash** (RIHS01 from Iron onward **[V]**), not by name alone, because message definitions change across distributions.
- Every live option below needs this crate. `roslibrust`'s compile-time code generation does not serve a generic data plane, and `cdr-encoding` 0.11.0 is typed serde only, not schema-driven.
- Untrusted-input parser: a `cargo fuzz` target is required (ADR-0020). Golden fixtures come from real bags. The `msg_parse` and `cdr_decode` fuzz targets exist (`fc37847`).

### Stage 2: MCAP and rosbag2 file source

- Source built on `mcap` 0.25.0 (MIT, Foxglove, pure Rust, self-describing: message definitions are embedded in the file). It carries no live-middleware risk.
- `thadam export --mcap` (D24) and `examples/ros2-mcap-to-parquet` ship with this stage.
- **Status: complete.** The `mcap` source (`d60552b`), the exporter and `thadam export --mcap` (`edcdcc1`, `8c7ad90`), and the `examples/ros2-mcap-to-parquet` example (`e9e94f7`) have landed. The export format is recorded in ADR-0015 (Draft).

### Stage 3: spike S3, live-transport evaluation in this order

The order is the maturity review's reordering of the original plan (section 15, item 1):

1. **Zenoh-native.** `roslibrust` with its `hiroz` backend, or raw `zenoh` against `rmw_zenoh`, for Lyrical (LTS to 2031) and Kilted.
2. **`zenoh-bridge-ros2dds` sidecar** for DDS robots (Humble and Jazzy on Fast DDS or Cyclone). Thadam stays a Zenoh client but needs a second key-expression mapping **[V]**.
3. **`r2r`** as the in-process rcl fallback when a sidecar is unacceptable.
4. **RustDDS with `ros2-client`** deferred.

### Stage 4: live ROS 2 source

Built per this ADR after S3 reports (ARCHITECTURE.md section 21, M2-C).

### Rules that apply in every stage

- The transport sits behind **Thadam's own trait**, and versions are pinned. Candidate crates change quickly (see evidence below).
- **No production claim on `hiroz`**, which describes itself as experimental software.
- **Zenoh is a transport only.** It is never a replacement for the local WAL (capture-first, D1).
- **DDS embedding in Thadam is not justified for the MVP.**
- Zenoh sits behind a Cargo feature flag because of its large dependency tree. Thadam elects the Apache-2.0 option of Zenoh's `EPL-2.0 OR Apache-2.0` license (ADR-0020).
- Only the stable `zenoh` and `zenoh-ext` APIs are used. Zenoh's internal crates can change at any time.
- ROS 2 crates stay excluded from the workspace until M2 starts (ARCHITECTURE.md section 9).
- SROS2 and DDS-Security support in the pure-Rust options is not demonstrated **[V]**. No security claim is made for ROS 2 transports until it is.

### Evidence base per option (as checked 2026-10-07)

Source: ecosystem maturity review, sections 1 to 3. These are the facts the lean rests on; items marked [V] are unconfirmed.

- **rclrs** 0.8.0 (2026-09), Apache-2.0, 1.5k stars, last push 2026-10-03, roughly quarterly releases. Needs a ROS install, colcon plugins, and `libclang`.
- **r2r** 0.9.7 (2026-09) after a 17-month gap since 0.9.5. License `MIT AND Apache-2.0` on crates.io; GitHub reports NOASSERTION **[V license file]**. Needs a sourced ROS environment to build and run. The original plan notes `subscribe_raw` and `subscribe_untyped` allow topic capture without compiled message types **[V]**.
- **ros2-client** 0.11.0 and **RustDDS** 0.14.3 (Atostek). Roughly monthly releases in 2026; Apache-2.0 on crates.io but GitHub shows no license file **[V]**; MSRV 1.88. RustDDS does not implement Presentation, Deadline, Latency budget, or Time-based filter QoS, and exclusive ownership is partial. Interoperability with Fast DDS (XCDR2, type lookup, security) is unproven **[V]**.
- **roslibrust** 0.26.0, MIT, pure Rust, no ROS install. Backends: hiroz (native rmw_zenoh, described as Kilted-or-newer), zenoh-ros1, rosbridge, and a mock. **Five minor releases in September 2026: high churn.**
- **hiroz** (ZettaScaleLabs, formerly `ros-z`) 0.2.0 (2026-06), Apache-2.0, 281 stars. README: "experimental software", tested with Jazzy, Humble, Kilted, and Lyrical. This sits uneasily with rmw_zenoh's Humble incompatibility warning, so interoperability with Jazzy and Humble is **[V]**.
- **dust_dds** 0.16.0, Apache-2.0, 165 stars, roughly six-monthly releases, MSRV 1.87.
- **cyclors** 0.4.0 (Cyclone DDS bindings, maintained by ZettaScale), Apache-2.0, needs a C build (cmake). Used by `zenoh-bridge-ros2dds`. Optional `dds_security` feature.
- **cyclonedds-rs**: non-standard license; rejected.
- **zenoh** 1.10.1 (2026-09), `EPL-2.0 OR Apache-2.0`, releases every one to two months, 325 open issues, MSRV 1.75 (some dependencies need newer). Eclipse Foundation governance with ZettaScale backing.
- **mcap** 0.25.0, MIT, Foxglove, about 2M recent downloads.

## Alternatives considered

- **rclrs.** Aligned with the official ROS 2 organization, full rcl semantics. Rejected as the lead option: needs a ROS install, colcon, and `libclang` at build time, is tied to a distribution, and makes CI and cross-compilation heavy.
- **r2r.** Tokio-friendly and covers every distribution, but still needs a sourced ROS environment and adds bindgen fragility. Kept as the in-process fallback (stage 3, option 3).
- **RustDDS and `ros2-client`.** Only credible pure-Rust DDS, no ROS install, easy ARM cross-compilation. Deferred because of the QoS gaps and unproven Fast DDS interoperability.
- **dust_dds.** Lower maturity than RustDDS in the review; not pursued.
- **cyclors embedded in Thadam.** Brings DDS-Security and Cyclone's maturity, but adds a C toolchain to the build. DDS embedding is not justified for the MVP. It remains available indirectly through the bridge.
- **cyclonedds-rs.** Rejected (license).
- **Fast DDS binding.** None maintained.
- **Zenoh-native with `roslibrust` and `hiroz`.** Lead option for Kilted and Lyrical. Pure Rust, no ROS install. Cons: experimental, fast-moving, and Kilted ends in November 2026, so the long-term targets are Lyrical, Jazzy, and Humble.
- **Raw `zenoh` against `rmw_zenoh`.** Avoids the roslibrust and hiroz churn, but Thadam then owns the key-expression and attachment handling, which are rmw implementation details that may change between distributions **[V]**.
- **`zenoh-bridge-ros2dds` sidecar.** Reaches DDS robots. Cons: an extra deployed process and a second key mapping.
- **Offline-only ROS 2 (MCAP and rosbag2, no live source).** Lowest risk, and stages 1 and 2 already deliver it. Rejected as the end state only because product.md lists live ROS 2 ingestion as P0; it is the fallback if S3 fails.
- **Bridge processes.** rosbridge (websocket JSON) is decoupled but adds JSON overhead. A companion node forwarding to MQTT or Zenoh keeps Thadam on supported connectors but adds a component to every robot. Neither is the lead option.

## Consequences

- Stages 1 and 2 deliver ROS 2 data into the canonical model and Parquet with no middleware risk, and give `thadam export --mcap` a home.
- `thadam-ros2-msg` is the one piece that is permanent regardless of S3. The transport can change without touching it.
- Pinned versions and a Thadam-owned transport trait make a switch of live transport a local change.
- Supporting both robots on rmw_zenoh and robots on DDS may mean two key mappings and a deployed sidecar. That is an operations cost the S3 spike must quantify.
- Live ROS 2 stays unclaimed for production use until S3 completes and no production claim rests on `hiroz`.
- Because ROS 2 is outside v0.1 (owner decision B2), the M1 scope and its reliability gate carry no ROS 2 work. Moving ROS 2 into v0.1 later would grow M1 and change the cut order in ARCHITECTURE.md section 1.
- Earlier plans are updated as follows. The original plan's spike compared a Zenoh subscriber (with `zenoh-plugin-ros2dds`) against `r2r` and kept `rustdds` as a fallback considered only if a pure-DDS need appeared. The maturity review reorders it into the four-step order above, and `rustdds` and `ros2-client` are now explicitly deferred. The original plan's "rmw_zenoh reportedly Tier 1 since Kilted [V]" is now confirmed from REP-2000 for Kilted only. Original PR numbering (PR18 to PR20 for ROS 2) is replaced by ARCHITECTURE.md section 21 labels M2-A, M2-B, S3, M2-C, because PR19 to PR21 now belong to ClickHouse.

## Evidence and validation required

Spike S3 and the stage work must produce the following before this ADR can move to Proposed. Where the sources give no numeric threshold, the criterion is open.

- [ ] **Builds without a ROS install** for each live candidate on x86_64 and aarch64, in CI. Pass or fail per candidate.
- [ ] **Distribution coverage:** a working capture from Humble, Jazzy, and Kilted or Lyrical robots or containers, on each candidate. [OPEN: which distribution and middleware combinations are required rather than best-effort, before the spike runs]
- [ ] **QoS fidelity:** for each candidate, which reliability, durability, history, and liveliness settings behave as configured against a reference publisher. [OPEN: criterion to be set before the evaluation runs]
- [ ] **Throughput on ARM** (Raspberry Pi 5 with NVMe and/or Jetson Orin Nano, per the benchmark plan reference hardware **[A: availability]**): messages per second and CPU for a representative sensor topic mix. [OPEN: criterion to be set before the evaluation runs]
- [ ] **Operations burden:** deployed components, configuration steps, and failure surface per candidate, compared on one table.
- [ ] **Zenoh mappings:** document the key-expression and attachment conventions of `rmw_zenoh` and of `zenoh-bridge-ros2dds`, and demonstrate whether one Thadam mapping layer can serve both **[V]**.
- [ ] **hiroz interoperability** with Jazzy and Humble, given the rmw_zenoh incompatibility warning **[V]**.
- [ ] **License files** for `ros2-client` and `r2r` **[V]**.
- [ ] **REP-2000 Lyrical tiers** once published **[V]**.
- [ ] **DDS-Security and SROS2** support in each pure-Rust option, and Zenoh TLS, mTLS, and access-control configuration specifics **[V]**.
- [x] **Fuzz target exists:** `cargo fuzz` targets `msg_parse` (`.msg` parser) and `cdr_decode` (CDR decoder) in `crates/thadam-ros2-msg/fuzz` (`fc37847`).
- [ ] **Fuzz target running in CI** (ADR-0020). The CI job is being added separately.
- [ ] **CDR decoder:** golden fixtures from real bags on each target distribution; type-hash keying confirmed for distributions from Iron onward **[V]**; which CDR encoding variants must be supported **[OPEN]**.
- [ ] **Drop accounting:** a best-effort topic under overload shows counted drops and a `Gap` marker, with no unbounded growth.
- [ ] **Dependency weight:** `cargo tree` and build-time cost of the Zenoh feature on aarch64, behind the feature flag; `cargo deny` passes with the Apache-2.0 election.
- [x] **Owner decision B2** recorded for ROS 2 placement: M2, not v0.1 (2026-10-07).

## Revisit when

- REP-2000 publishes Lyrical platform and middleware tiers.
- `hiroz` or `roslibrust` stabilize (hiroz leaves "experimental", roslibrust release cadence settles).
- The release cadence of `rclrs` or `r2r` changes materially.
- Kilted reaches end of life (November 2026) and the target distribution set narrows.
- The owner revisits the 2026-10-07 decision (B2) and moves ROS 2 into v0.1.
- S3 shows no candidate meets the criteria, in which case offline-only is the shipped scope.

## References

- ARCHITECTURE.md sections 1 (milestones), 3 (D1, D24), 9 (crate rules), 10 (backpressure), 16 (toolchain and licenses), 18, 20 (`thadam export --mcap`), 21 (M2), 22 (open items).
- ADR-0001 (canonical event model), ADR-0002 (connector contract), ADR-0003 (execution model), ADR-0004 (durable buffer), ADR-0015 (recording and replay format, Reserved), ADR-0020 (dependency security and toolchain policy).
- product.md sections 7, 31, 37.
- `docs/design/owner-decisions.md` (B2) and `docs/design/product-amendments.md`.
- Ecosystem maturity review (2026-10-07), sections 1, 2, 3, 15, 16, 17. Original MVP plan, section F (ROS 2 options) and section H.
- Upstream: REP-2000, `rmw_zenoh` README, Zenoh documentation, the crate repositories named above.
