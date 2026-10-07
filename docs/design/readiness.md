# Readiness gate: re-run of 2026-10-07 (after the owner decisions)

Status: **CONDITIONALLY READY.** PR3 and PR4 (`thadam-core` model and traits, `thadam-testkit`) and PR5 (`thadam-config`) can start as soon as PR1 (the workspace scaffold) and PR2 (the docs baseline) are committed to the repository. The WAL work (PR6 and PR7) stays gated on the ADR-0004 hardware re-run. The owner still needs to apply `docs/design/product-amendments.md` Part A to `product.md`.

Date: 2026-10-07. This document re-runs the checklist in section 1 of the readiness gate plan (`thadam_mvp_readiness_gate`) against the current documents and evidence, after the owner's decisions of the same day (ADR acceptances, B7, and the EMQX pin; see `docs/design/owner-decisions.md`). It supersedes the plan's "NOT READY" paragraph for tracking purposes and is updated whenever a blocker moves. The sources of truth it summarizes are `ARCHITECTURE.md`, `docs/design/owner-decisions.md`, `docs/design/adr/`, and the spike reports in `docs/design/spikes/` (S1 WAL, S2 MQTT, D8 record representation, ADR-0012 YAML, ADR-0018 filter).

Tags: **[A]** assumption, **[V]** validate before relying on it.

## 1. Verdict

**CONDITIONALLY READY.** By the plan's flip rule (section 2, "Flip to READY when"), the design conditions are met except for ADR-0004 (which gates only the WAL work). What is missing is repository state and one owner action:

| Flip condition (readiness gate plan) | State | Detail |
|---|---|---|
| B1 merged, with ADRs 0001, 0002, 0003, 0011, 0013, 0020 accepted | **Half met** | All six are **Accepted** (owner, 2026-10-07). Nothing is committed yet: the whole repository, including PR1, is untracked. The acceptances take effect in the repository when PR2 is committed. |
| B2 and B3 signed by the owner | **Met** | Decided 2026-10-07 (`owner-decisions.md`). |
| S1 and S2 reports in, with ADR-0004 and ADR-0006 accepted | **Half met** | Both reports are in. ADR-0006 is **Accepted** (with `rumqttc` pinned and EMQX pinned to 5.8.x). ADR-0004 stays **Draft**: the S1 re-run on x86 NVMe and ARM eMMC is required before the on-disk format is frozen. This gates only PR6 and PR7. |
| B6 and B7 decisions recorded | **Met** | B6 (D5, D6, D8, D9, D10) and B7 (ADR-0012 `serde-saphyr`, ADR-0018 structured predicates) are decided and their ADRs are Accepted. |
| B8 folded in | **Met** | Done in `ARCHITECTURE.md` sections 15, 20, and 21; updated for the 2026-10-07 decisions. |
| Owner applies the approved Part A amendments to `product.md` | **Not met (owner action)** | `product.md` is read-only to coding agents. It still contradicts the frozen scope (ClickHouse demos, HTTP and gRPC as P0, ROS 2 as an MVP source, aggregation as P0, the `expression:` filter example). Part A now has eleven entries (A.1 to A.11, all Approved 2026-10-07). |

### Readiness per PR group

The overall verdict is a conjunction. The practical question is which work can start, so each gate below shows what it still needs.

| Work | Needs | State |
|---|---|---|
| PR1 (workspace scaffold and CI guardrails) | Commit | **Done, uncommitted.** Re-verified in this pass (section 6), with the workspace MSRV raised to 1.89. |
| PR2 (docs baseline) | Owner go-ahead to commit | **Ready to commit.** Not committed (this pass was told not to). |
| PR3, PR4 (`thadam-core` model and traits, `thadam-testkit`) | PR1 and PR2 committed | **Ready to start on commit.** ADR-0001 and ADR-0002 are Accepted; B6 is signed; no engineering evidence is outstanding. |
| PR5 (`thadam-config`) | PR3; PR1 and PR2 committed | **Ready after PR3.** ADR-0012 is Accepted; the details the spike left open are listed in ADR-0012 as PR5 implementation details. |
| PR6, PR7 (WAL writer, readers, cursors, retention) | ADR-0004 accepted, which needs the S1 re-run on reference hardware | **Gated.** The format becomes permanent when released. |
| PR8 (transform part 1) | PR3 | Ready after PR3. |
| PR9 (transform part 2) | PR3 and PR8 | Ready after PR8. ADR-0018 is Accepted; the details the spike left open are listed in ADR-0018 as PR9 implementation details. |
| PR10, PR11 (runtime, observability) | PR3 to PR8 | Not started; D5 and D6 are signed, so no decision blocks them. |
| PR12 (`thadam-mqtt`) | PR4 and the runtime sequence | ADR gate closed: ADR-0006 is Accepted, and EMQX is pinned to 5.8.x for CI. |
| PR13 onward | Earlier PRs | Not started. ADR-0013 is Accepted. |

## 2. Checklist re-run (readiness gate plan, section 1)

Each row compares the plan's status of 2026-10-07 (morning) with the state now.

| Checklist item | Plan said | Now | Evidence and what remains |
|---|---|---|---|
| Canonical event model | PARTIAL | **ACCEPTED** | D8 (representation B behind an opaque `Record`), D9 (extended values), and D10 (one event per message plus `explode`) signed in B6; quality granularity signed in B3. ADR-0001 is Accepted. Implementation-time: the 24-byte `Value` layout measurement (PR3), open item O10 (where processing time is recorded; additive, decide before PR13), and the PR3 and PR8 tests. |
| Connector API | PARTIAL | **ACCEPTED** | D5 and D6 signed. S2 confirmed liveness, cancel-safety, ordered ack, and payload-stripped handles, and added three clauses. ADR-0002 is Accepted. Implementation-time: `SinkBatch` (O8, PR3 and PR4), conformance suites, sink prototypes. |
| Pipeline execution semantics | DEFINED, not accepted | **ACCEPTED** | ADR-0003 is Accepted, including D1 (capture-first WAL). S1 supports D1 on the WAL side (about 2% framing overhead; commit-to-ack is one group commit). Implementation-time benchmarks: chain CPU with one to three sinks, WAL growth during a sink outage, and the hypothetical post-transform WAL (Option A). |
| Buffer semantics | DEFINED; engine and format not selected | **Engine selected; format not frozen (ADR-0004 Draft)** | S1: custom segmented log, SQLite as the fallback, redb not for the bulk queue. The frame header is 24 bytes with a reserved word (O9 closed). Provisional fsync default `interval(10ms)`. ADR-0004 stays Draft: the S1 re-run on reference hardware, validation of the prost payload, an ENOSPC loop-device test, and a directory-metadata loss test are outstanding. `on_full` has no default (B3). |
| Retry semantics | DEFINED | **ACCEPTED** | ADR-0011 is Accepted. One `[OPEN]` wording item (the power-loss exposure window of the configured fsync policy) can be derived from S1 but is not yet written; no power-loss claim is published until the hardware rig runs. |
| Configuration model | PARTIAL | **DECIDED** | ADR-0012 is Accepted (`serde-saphyr`, `strict_booleans: true`, MSRV 1.89, `serde_norway` fallback) and ADR-0018 is Accepted (structured predicates, CEL reserved). The demo `[IMPL]` options are folded into `ARCHITECTURE.md` section 15. Implementation-time: key spelling, hash scope, interpolation edge cases, parse budget (PR5); filter key, namespace, null semantics, cost limits (PR9). |
| Error model | DEFINED | **DEFINED** | Unchanged. |
| Observability model | DEFINED, minor drift | **DEFINED** | Metric names consolidated (section 14). Two names were added from S2, `thadam_source_unacked_max` and `thadam_source_stale_acks_total`, with final names fixed in PR11 and PR12 **[A]**. |
| Test strategy | DEFINED | **DEFINED, strengthened** | S1 contributes the kill -9 loop, LazyFS, torn-tail, and fsync-fault harness designs; S2 contributes the wire oracle, the cancel-safety and stale-ack scenarios, and the no-`select!`-around-`poll` rule. |
| Benchmark strategy | DEFINED; hardware assumed | **DEFINED; hardware still assumed [A]** | All spikes ran on one Apple Silicon (arm64) laptop (S1 and S2 also used a Linux VM in Docker Desktop). Re-runs on reference ARM and x86 hardware are required for S1 (before the format is frozen) and recommended for S2, D8, and ADR-0018's evaluator numbers (before any published claim). |
| MQTT implementation | PROVISIONAL | **ACCEPTED, pinned** | `rumqttc` 0.25.1 behind `thadam-mqtt`, with the five wrapper rules and three configuration requirements, `on_commit` default, `v311` default and `v5` experimental. ADR-0006 is Accepted. EMQX is pinned to the last open-source line, 5.8.x, for CI, tests, demos, and documentation; newer Enterprise-licensed images are not used. Open, not blocking: S2b against `rumqttc-next`, the PR12 conformance suite, and non-happy QoS 2 paths (QoS 2 stays experimental). |
| Local Parquet capture and recovery | SELECTED | **ACCEPTED** | ADR-0013 is Accepted. Implementation-time `[OPEN]` items: the unsigned-integer default in PR13, the W4 and B6 benchmark criteria. |
| Object-store implementation | SELECTED | **SELECTED** | Unchanged. |
| ClickHouse | DEFERRAL DOCUMENTED | **DECIDED (P1 after the M1 gate), not yet in `product.md`** | Owner decision D-B2-3. ADR-0008 stays Draft. `product.md` sections 14, 40, and 46 still describe ClickHouse demos until the owner applies Part A. |
| ROS 2 strategy | DOCUMENTED; scope unresolved | **DECIDED: M2, not in v0.1** | Owner decision D-B2-2 (MCAP and rosbag2 offline first). ADR-0007 stays Draft; the live-transport spike S3 is M2 work and does not block M1. |
| Major unknowns isolated behind interfaces | MOSTLY | **YES, with one caveat** | The record representation is behind the opaque `Record`; the WAL engine is behind `thadam-wal`; `rumqttc` types never leave `thadam-mqtt`; the ROS 2 transport and ClickHouse format are behind their traits; the YAML crate is behind `serde_json::Value`. The filter syntax is user-facing but is now decided (structured predicates), with `expr:` reserved for a future CEL. Caveat: the WAL on-disk format stays unfrozen until ADR-0004 is accepted. |
| MVP scope frozen | NO | **Decided, not yet in the source of truth** | B2 and B3 are signed (MVP v0.1 = M1; ROS 2 = M2; ClickHouse = P1; HTTP, gRPC, and aggregation after the MVP; explicit `on_full`; wide Parquet; event-level quality; batching not assumed; industrial SSD or eMMC). The approved Part A amendments have not been applied to `product.md`, so the product document still contradicts the scope. |

## 3. Remaining blockers, with owners

Ordered by how much work each unblocks. "Owner" is the project owner unless stated.

1. **Commit PR1 and the docs baseline (PR2).** Owner go-ahead, then a maintainer commits. Nothing has been committed, staged, or branched in this pass; the whole repository, including the PR1 workspace, is untracked. Until PR2 is committed the ADR acceptances live only in the working tree. Unblocks PR3, PR4, and PR5.
2. **Apply the approved Part A amendments (A.1 to A.11) of `docs/design/product-amendments.md` to `product.md`.** Owner (`product.md` is read-only to coding agents); decide Parts B and C separately. Without this, `product.md` contradicts the frozen scope, and README and release wording cannot follow it. A.11 replaces the `expression: "quality == 'good'"` example with the structured-predicate form.
3. **S1 re-run on x86 NVMe and ARM eMMC, then accept ADR-0004.** Engineering, with the owner providing reference hardware **[A]**. Scope (from ADR-0004): flush latency and `fdatasync` behavior on the real devices; commit-to-ack latency by fsync mode; device-level write amplification from `/proc/diskstats`; eMMC recovery against the 50 ms criterion; the fsync default (`interval(10ms)` is provisional); Linux preallocation; segment size; validation of the prost `RawFrame` payload (CPU, framing cost, frozen field numbers); an ENOSPC test on a loop device with lazy allocation applied; and a directory-metadata loss test (`dm-flakey`) or the power-cut rig. **Gates PR6 and PR7 only.** The hardware power-cut rig (500 cycles) is separate and is needed only before a power-loss claim (ADR-0011).
4. **Reference hardware access.** Owner. A Raspberry Pi 5 with NVMe or a Jetson (ARM), an industrial eMMC board, and an x86 NVMe machine. Required by blocker 3 and by the D8, S2, and ADR-0018 re-runs on ARM and x86.
5. **ADR-0000 (the process itself) is still Proposed.** Owner. It was not in the 2026-10-07 acceptance list; the lifecycle it defines is already followed, so this is a formality.

Closed in this pass: acceptance of ADRs 0001, 0002, 0003, 0006, 0011, 0012, 0013, 0018, and 0020; the ADR-0012 and ADR-0018 decisions; the EMQX licensing question (pinned to 5.8.x); the Rust floor (1.89).

### Tracked, not blocking

- Implementation-time details: the ADR-0012 "PR5 implementation details" (key spelling, O2, hash scope, interpolation edge cases, parse budget, anchors and merge keys, path-to-span index) and the ADR-0018 "PR9 implementation details" (filter key, namespace, nested access, literal typing, null semantics, cost limits, replay-equivalence test).
- ADR-0006: S2b (the S2 harness against `rumqttc-next`, at most one day), the TLS and mTLS handshake test (PR12), QoS 0 overflow accounting (PR12), and the QoS 2 non-happy paths (QoS 2 stays experimental). EMQX image tags are pinned to an exact 5.8.x when PR12 and PR15 add them.
- ADR-0020: the MSRV CI job (`.github/workflows/ci.yml` has no MSRV job and no hard-coded MSRV, so nothing needed updating there), the `cargo deny` self-demonstration on scratch branches, and `ring` on native aarch64.
- ADR-0016 (name clearance): mitigated by `publish = false` and the provisional binary name.
- CI platform (github.com versus GitHub Enterprise) and ARM64 runner availability.
- The `[V]` items from the maturity review.
- ADR-0007 (ROS 2) and ADR-0008 (ClickHouse): Draft by design; M2 and P1 work.
- ADR-0011's `[OPEN]` wording for the power-loss exposure window, which can now be derived from S1.
- The 24-byte `Value` layout (PR3), the D8 re-measurement on reference hardware and the real decoder (PR8).

### What flips the project to fully READY

Blockers 1 and 2 closed, and, for the WAL work only, blocker 3 (with blocker 4 as its dependency). Work can start incrementally: PR3, PR4, and PR5 need only blocker 1. The owner action in blocker 2 does not gate code, but it gates README, demo, and release wording.

## 4. Caveats on the evidence

- **All spikes ran on one shared Apple Silicon (arm64) laptop.** S1 and S2 added a Linux VM in Docker Desktop whose flush semantics are unknown (Linux fsync took about 1.4 ms per group, against about 8 ms for `F_FULLFYNC` on the host). Ratios and macro differences (2 times or more) transfer; absolute numbers do not, and flush-bound macOS rows carry about plus or minus 40% run-to-run noise.
- **S1 did not exercise:** the prost frame payload, real device errors, directory-metadata loss, concurrent readers racing the writer or an unlink, multiple consumers, budget enforcement, backward clock steps, and cold-cache replay. kill -9 never tears a write and LazyFS does not drop metadata operations.
- **S2 simulated commit latency** (no real fsync), ran each throughput cell once, tested keepalive at one value (5 s), smoke-tested MQTT 5 on Mosquitto only, and covered QoS 2 on the happy path only. `rumqttc-next` was not run. EMQX results are from the 5.8.6 build, which is now the pinned line.
- **D8 measured records, not a pipeline.** Strings were copied rather than sliced from WAL bytes, records had 3 to 7 fields, and the real decoder will shift the shares of cost.
- **The ADR-0012 and ADR-0018 spikes** measured on macOS arm64 only. ADR-0012 covered the demo shape of configuration only (no large or deeply anchored files). ADR-0018's ARM criterion for CEL was not met and is no longer gating, because CEL is reserved.
- This pass changed documents and the workspace `rust-version` (1.88 to 1.89). Nothing under `spikes/` (other than two dated update notes in the ADR-0012 and ADR-0018 spike reports), `crates/`, `apps/`, `product.md`, or `thadam-assets/` was changed.

## 5. Corrections adopted from the spikes

For traceability, these design corrections are now recorded in the ADRs and `ARCHITECTURE.md`.

| Correction | Source | Recorded in |
|---|---|---|
| Frame header is 24 bytes with a reserved word that must be zero; CRC covers header bytes `[0, 20)` plus payload; no header CRC; no resynchronization in sealed segments; all-zero header is the end-of-data marker | S1 | ADR-0004, `ARCHITECTURE.md` section 6 |
| Open and recovery must not allocate space (lazy segment creation, delete before allocate) | S1 | ADR-0004, sections 6 and 11 |
| Re-verification after a failed fsync must not trust the page cache | S1 | ADR-0004, section 6 |
| No segment preallocation on macOS | S1 | ADR-0004, section 6 |
| Never reuse a sequence number at or below a durable cursor; readers and cursors bounded by the durable end; a bad segment header fails the open | S1 | ADR-0004, sections 6 and 11 |
| Default fsync `interval(10ms)` (provisional) instead of 50 ms | S1, S2 | ADR-0004, section 3 (D3), section 6 |
| In MQTT 3.1.1 the receive window is the broker's; the incoming queue is never bounded below it and never blocks the poll task | S2 | ADR-0006, ADR-0002, section 10 |
| Poll task only awaits `poll()`; never race it | S2 | ADR-0006, ADR-0002, section 10 |
| Connection-epoch-tagged ack handles; resubscribe on session loss | S2 | ADR-0006, ADR-0002, section 5 |
| `max_incoming_packet_size` from `max_message_bytes`; counted drop of oversized messages is not possible for MQTT | S2 | ADR-0006, ADR-0004, sections 6 and 10 |
| `rumqttc` feature set `use-rustls-no-provider` plus `tokio-rustls` with `ring` | S2 | ADR-0006, section 16 |
| D8: representation B behind an opaque `Record` with `ShapeKey` and positional accessors | D8 report, owner B6 | ADR-0001, section 4 |
| D9: extended values; typed-array ownership rule; 24-byte `Value` layout to evaluate in PR3 | D8 report, owner B6 | ADR-0001, section 4 |
| `serde-saphyr` with `strict_booleans: true`, duplicate keys an error, `reject_unsupported_tags: true`, tightened budget; MSRV 1.89 | ADR-0012 spike, owner B7 | ADR-0012, ADR-0020, section 16 |
| Structured predicates for filters, each with an exact CEL equivalent; `expr:` reserved; explicit integer literal types and `f32` rule | ADR-0018 spike, owner B7 | ADR-0018, section 15, `product-amendments.md` A.11 |
| EMQX pinned to the last open-source line (5.8.x); newer Enterprise-licensed images not used | S2, owner decision | ADR-0006, sections 10, 21, and 22 (O13) |

## 6. Verification performed in this pass

Run from the repository root on 2026-10-07, after raising `rust-version` in the root `Cargo.toml` from 1.88 to 1.89 (toolchain pinned by `rust-toolchain.toml`: rustc 1.99.0).

| Check | Result |
|---|---|
| `cargo fmt --check` | Pass |
| `cargo build --workspace` | Pass (11 crates) |
| `cargo clippy --workspace --all-targets -- -D warnings` | Pass, no issues |
| `cargo nextest run --workspace --no-tests=pass` | Pass (0 tests across 11 binaries, as expected for the empty workspace) |
| `cargo deny check` | Pass (advisories ok, bans ok, licenses ok, sources ok). It ran outside the sandbox because it updates the advisory database under `~/.cargo`. |
| `scripts/check-deps.sh` | Pass (`check-deps: ok`) |
| `scripts/check-deps.sh --self-test` | Pass (14 violations detected in the fixture) |
| `cargo metadata --no-deps` | All 11 workspace members report `rust_version` 1.89 |
| `.github/workflows/ci.yml` MSRV check | No hard-coded MSRV and no MSRV job; the toolchain comes from `rust-toolchain.toml`. Nothing to update. The MSRV job remains an ADR-0020 evidence item. |
| Spikes excluded from the root workspace | Unchanged from the earlier pass: `exclude = ["spikes"]`, each spike manifest declares its own `[workspace]`. |

The `clippy.toml` settings (`allow-unwrap-in-tests`, `allow-expect-in-tests`) take effect without a warning, but no test exists yet to exercise them; the first test in PR3 does.

## 7. Next pull requests

The repository has no commits of this work: every file is untracked. Suggested commit order, all for the owner to approve and a maintainer to run (this pass did not stage, commit, or branch).

### PR2: docs baseline (the next step; owner go-ahead needed)

Content, all already in the working tree:

- `ARCHITECTURE.md`; `docs/design/adr/0000` to `0020` as present, with the statuses now recorded (nine Accepted, ADR-0004, 0007, and 0008 Draft, ADR-0000 Proposed); `docs/design/owner-decisions.md`, `product-amendments.md` (Part A with A.11), `version-inventory.md`, and this file; the five spike reports in `docs/design/spikes/`.
- The spike projects under `spikes/` (source, scripts, and `results/`), because the reports cite them. They are standalone and excluded from the workspace. **[A]** The throwaway-ness of the spikes means the owner may prefer to keep only the reports; the report paths would then need a note.
- PR1 files if they are not committed first: the Cargo workspace (`Cargo.toml` with `rust-version = "1.89"`, `Cargo.lock`, `crates/`, `apps/`), `rust-toolchain.toml`, `rustfmt.toml`, `clippy.toml`, `deny.toml`, `.config/nextest.toml`, `.github/workflows/ci.yml`, `.gitignore`, and `scripts/`. A cleaner history commits PR1 first, then PR2.
- `product.md` under version control, with the owner applying Part A (A.1 to A.11) first or in the same PR. The owner decides whether `thadam-assets/`, `coding-agent-plan-prompts.md`, and `product.md` belong in this commit.

The pull request must record the owner's acceptance of each ADR it changes to Accepted (ADR-0000): cite `docs/design/owner-decisions.md`, section "ADR acceptances".

Acceptance: documents render, no stale "pending" statements remain for the spikes or the accepted ADRs, every owner decision in the register has a date, and the verification in section 6 still passes on the committed tree.

### PR3: `thadam-core` model and errors (first code PR; starts once PR1 and PR2 are committed)

ADR-0001 is Accepted. Scope (the dependency rules in `scripts/check-deps.sh` apply: no Tokio as a direct dependency, no Arrow, `prost`, protocol, or YAML crates in `thadam-core`):

- **Time, quality, identity, positions.** `Timestamp(i64)`, `TimeDomain`, `Times` (source set once, ingest never null, corrected optional with a mandatory quality flag), `Quality` with the flag bitset, `AssetPath`, `WalPos` and `WalRange`.
- **Values and shapes.** The extended `Value` set including typed arrays and nested records; `Shape` with fingerprint; per-chain shape registry with size caps (`max_shapes`) and counted rejects.
- **Opaque `Record`** with `shape_key`, `get_at`, `set_at`, and slow-path `get` and `iter`; no public fields; documented clone semantics; the typed-array ownership rule.
- **`Event`, `EventBatch`, `RawMessage`, `RawBatch`** with private fields and a builder that enforces the invariants. Size budget: 128 bytes excluding heap.
- **Errors.** `ThadamError`, `Classify`, stable error codes, the retry classes.
- **`DiskBudget`** with per-category atomics.
- **Declared-field types** (open item O1) placed in `thadam-core` so `thadam-arrow` and `thadam-transform` do not depend on `thadam-config`.
- The **24-byte `Value` layout evaluation**, recorded in ADR-0001.

Tests: builder invariants (`source_time` immutable, `correct()` always sets a flag, no event without ingest time), the `Event` size budget, proptest (value round trip, widening never narrows, `explode`), error-code snapshot, `loom` on `DiskBudget`, shape cache behavior at 10,000 distinct shapes, time-domain and clock-step cases with a fake clock, and a test that fails if `Record` exposes a public field or an `Arc<Shape>` comparison. These are the first tests in the workspace, so the `clippy.toml` settings are exercised here.

Size: likely above the 600-line review target. If so, split along the line between "time, quality, identity, positions, errors, `DiskBudget`" (3a) and "values, shapes, `Record`, `Event`" (3b), so the D8 work gets its own review.

### PR4: core traits and `thadam-testkit` (ADR-0002 Accepted; follows PR3)

- **Traits and contexts.** `Source` (cancel-safe `next_batch`), `BatchSink`, `SegmentSink`, factories (`SourceFactory`, `BatchSinkFactory`, `SegmentSinkFactory`, `ServiceFactory`) taking `serde_json::Value`, `SourceContext`, `SinkContext`, `AckHandle`, `AckMode`, `SinkBatch` (closes O8, including how `BatchId` is formed and persisted), `Shutdown` (a `tokio-util` cancellation token is allowed in the closure but not as a direct `tokio` dependency), a clock abstraction.
- **`thadam-testkit`.** `MemorySource`, `MemoryBatchSink`, `MemorySegmentSink`, `FakeClock`, and the conformance suites: liveness (stall for three times the keepalive, no lost message), cancel-safety (random drops of `next_batch` and cancellation during connect, receive, and ack), ack ordering under randomized commit completion, epoch-scoped handles (the S2 E1 stale-ack scenario loses zero messages), the protocol task never awaiting the runtime, the lost-token watchdog, and panic isolation. The memory connectors must pass their own suites first.
- **Benchmarks.** Per-batch overhead of `async_trait` boxing and ack bookkeeping, and ack-tracker throughput at 100,000 acks per second.

### PR5: `thadam-config` (ADR-0012 Accepted; follows PR3)

`serde-saphyr` with `strict_booleans: true` behind `thadam-config` (MSRV 1.89), `thadam validate`, `thadam version`, the demo option list in `ARCHITECTURE.md` section 15, the explicit-`on_full` rule, the multi-error collection pass with the path-to-span index, and the ADR-0012 "PR5 implementation details". `serde_norway` is the documented fallback.

### After PR3 to PR5

PR6 and PR7 are gated on ADR-0004 (the S1 re-run); PR8 follows PR3; PR9 follows PR8 (ADR-0018 Accepted); PR12 follows PR4 and the runtime (ADR-0006 Accepted, EMQX 5.8.x). The full sequence is `ARCHITECTURE.md` section 21.
