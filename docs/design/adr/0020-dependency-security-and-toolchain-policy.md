# ADR-0020: Dependency security and toolchain policy

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (accepted; workspace MSRV raised from 1.88 to 1.89 with the ADR-0012 decision)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"). The same day the owner chose `serde-saphyr` for ADR-0012, which declares Rust 1.89, so the workspace `rust-version` is 1.89 from the start and this ADR states 1.89 as the floor. The unticked items in "Evidence and validation required" are implementation-time verification (CI on native runners, the cargo-deny self-demonstration, the MSRV CI job, and similar), not blockers.

## Context

Thadam is a long-running process on edge devices that handles untrusted network input (MQTT payloads now; ROS 2 CDR, CAN DBC files, and OPC UA later) and cloud credentials. It depends on a fast-moving crate ecosystem: Arrow and Parquet ship a breaking major about once a quarter, `object_store` has had breaking minors several times a year, and several candidate crates are pre-1.0. Two decisions are expensive to retrofit: which TLS provider the whole dependency tree uses, and which Rust version floor Thadam commits to.

The ecosystem maturity review checked crate versions, release dates, advisories, and licenses on 2026-10-07. Every version, date, advisory, and license in this ADR comes from that review ("as checked 2026-10-07 in the ecosystem maturity review", sections 14 and 15 item 5, plus the per-area sections cited below). Nothing was tested hands-on, and the review notes that third-party production reports were unavailable.

Thadam ships binaries. Edge distributions never compile it, so the toolchain policy constrains Thadam's developers and CI, not its users.

## Decision

### Rust version (MSRV) and toolchain

As checked 2026-10-07:

- `arrow`, `parquet`, and `ros2-client` need **1.88 or later**.
- `serde-saphyr` (the YAML crate chosen in ADR-0012) needs **1.89 or later**. This sets the floor.
- The P1 crates `clickhouse` and `socketcan` also need **1.89 or later**, so they do not raise it further.
- Iceberg and Delta (P2) need **1.94 or later**.

Policy: track stable minus two, and never go below the highest MSRV among the dependencies actually in the build. The declared `rust-version` is **1.89**, set by `serde-saphyr`. It rises to 1.94 only if Iceberg or Delta lands. Adopting `ntex-mqtt` (MSRV 1.97, ADR-0006 fallback) would also raise it.

A pinned stable toolchain builds releases and runs CI. The pin lives in `rust-toolchain.toml` only, which names `rustfmt` and `clippy` components and the `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` targets.

### Minimum secure versions

Enforced by blocking `cargo deny` advisory checks. A version below its minimum matches an advisory and fails CI.

| Crate | Minimum | Advisory |
|---|---|---|
| `rustls` | 0.23.45 | RUSTSEC-2026-0285 (TLS 1.3 handshake boundary) |
| `bytes` | 1.11.1 | RUSTSEC-2026-0007 |
| `tokio` | 1.44.2 | RUSTSEC-2025-0023 |
| `lz4_flex` | 0.11.6, or 0.12.1 | RUSTSEC-2026-0041 (uninitialized memory leak on invalid input; reachable through the Parquet LZ4 codec path) |
| `pyo3` | 0.29 | RUSTSEC-2026-0013 (fixed in 0.28.2), RUSTSEC-2026-0176 and RUSTSEC-2026-0177 (fixed in 0.29.0); applies when the post-MVP Python SDK lands |
| `object_store` | 0.10.2 | RUSTSEC-2024-0358 (WebIdentityToken written to logs) |

Whether `cargo deny` bans can express these minimums directly, or the policy relies on advisories alone, is to be confirmed **[V]**. If a patched version needs a higher MSRV than the declared floor, the build must fail visibly rather than fall back to the vulnerable version; the interaction with the MSRV-aware resolver (workspace `resolver = "3"`) is to be checked **[V]**.

### TLS provider

rustls with the `ring` crypto provider for the MVP.

- `aws-lc-sys` had five advisories in 2026, all patched in 0.39 or later. That shows an active response, and also a large C surface and a cmake or clang cross-compile cost for ARM.
- `ring`'s "unmaintained" advisory was withdrawn; the 0.17 line is maintained.
- This resolves the architecture review's open question (`ring` versus `aws-lc-rs`).
- Revisit if FIPS is required.

### Bans, features, and sources

- **Banned crates:** `aws-lc-sys`, `openssl-sys`, and `serde_yaml` (deprecated). The replacement YAML crate is chosen in ADR-0012.
- **CI guard:** a check that `aws-lc-sys` never appears in `cargo tree`, across all features and both target triples **[A]**.
- **Explicit features:** `reqwest`, `rumqttc`, and `object_store` are built with `default-features = false` and an explicit feature list, so no default feature silently pulls in another TLS stack.
- **Sources:** crates.io only; unknown registries and git sources are denied.
- **Duplicate versions** warn. **Yanked** crates and **vulnerabilities** are denied. **Unmaintained** advisories are intended not to block on transitive dependencies; because `cargo deny` has no warn level for them, the configuration fails only for direct workspace dependencies (`unmaintained = "workspace"`) **[V: confirm against the cargo-deny version in use; the readiness gate wording is "warn on unmaintained"]**.

### License allow-list

Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0, Zlib. Thadam itself is Apache-2.0.

- **MPL-2.0** is added when OPC UA lands (`async-opcua` and `open62541` are MPL-2.0). It is file-level copyleft and acceptable as an unmodified dependency.
- **Zenoh** is `EPL-2.0 OR Apache-2.0`; Thadam elects Apache-2.0.
- **EPL-2.0** (the `paho-mqtt` crate and the Paho C library, ADR-0006) is allowed only after legal review.
- The `object_store` crate metadata string is `MIT/Apache-2.0` while its repository says Apache-2.0; both are on the allow-list **[V]**.
- License files for `ros2-client`, `r2r` (crate license `MIT AND Apache-2.0`, GitHub reports NOASSERTION), and the `open62541` bindings are to be confirmed **[V]**.

### Fuzzing

Parsers of untrusted input need `cargo fuzz` targets: the MQTT payload and JSON decoders (PR8), and later the CDR decoder and `.msg` and IDL parser (M2-A) and the DBC parser (P1). The WAL frame decoder is a recommended target too **[A]**, because recovery parses bytes that a crash may have corrupted. Corpus size and run duration in CI are **[OPEN]**. Fuzz tooling commonly needs a nightly toolchain, so fuzz targets live outside the pinned-stable build **[V]**.

### Credentials

Secrets are supplied through `${env:}` or `${file:}` and wrapped in `Secret<T>`, which redacts `Debug` and `Display`. RUSTSEC-2024-0358 is the cautionary case: a credential token written to logs by a dependency. Payloads and credentials are never logged.

### Workspace lints and profile

- `unsafe_code = "forbid"`; filesystem syscalls go through `rustix`'s safe API.
- `clippy::unwrap_used` and `clippy::expect_used` denied outside tests; `missing_docs` warn on public items.
- `#[non_exhaustive]` on public enums and configuration structs.
- Release profile: `lto = "thin"`, `codegen-units = 1`, `debug = "line-tables-only"`, and **`panic = "unwind"`**, which task-boundary panic isolation requires. The implementation plan also asks for stripped split debuginfo; whether PR1's `Cargo.toml` sets it is to be confirmed **[V]**.

### OPC UA note

RUSTSEC-2023-0071 (the Marvin attack on `rsa`) has no patched version, and `async-opcua` uses `rsa`. Any claim of OPC UA security-profile support needs a security review first.

### How PR1 enforces this

PR1 (the workspace scaffold, written in parallel with this ADR) is the first enforcement point. It must provide:

- `rust-toolchain.toml` pinning the toolchain, components, and targets, and `Cargo.toml` setting `rust-version = "1.89"`, the workspace lints, and the release profile.
- `deny.toml` with blocking advisories, the license allow-list, the three crate bans, `multiple-versions = "warn"`, and crates.io as the only source.
- `scripts/check-deps.sh` enforcing the workspace dependency-direction rules, with a `--self-test` mode that must fail on a violating fixture.
- A CI workflow running format, clippy, build, and nextest on `ubuntu-24.04` and `ubuntu-24.04-arm`, plus `cargo deny` and the dependency guard with its self-test.
- **PR1 acceptance (ARCHITECTURE.md section 21):** CI is green on x86_64 and aarch64, the guard self-test fails on the fixture, and a scratch-branch check shows that adding `aws-lc-sys` or `serde_yaml` makes `cargo deny` fail.

Points that the PR1 review must check against this ADR **[V]**, because the plans require them and a scaffold can easily omit them:

- A separate `cargo tree` check that `aws-lc-sys` never appears.
- Whether `deny.toml` covers optional-feature dependencies (cargo-deny's feature coverage setting), since the bans must also catch dependencies that only appear under non-default features.
- A CI job that builds with the declared MSRV, so `rust-version = "1.89"` is verified rather than assumed.
- Whether the release profile carries the stripped split debuginfo setting that the implementation plan asks for.

### Superseded by this ADR (ARCHITECTURE.md section 19)

Nothing in section 19 is replaced. This ADR closes the implementation plan's open question on `ring` versus `aws-lc-rs`, and it is the owner of the dependency and toolchain rules summarized in ARCHITECTURE.md section 16.

## Alternatives considered

- **`aws-lc-rs` as the TLS provider.** Not chosen: five 2026 advisories (all patched), a large C surface, and a cmake or clang toolchain for ARM cross-compilation. It is the revisit path if FIPS is required.
- **`openssl` or `native-tls`.** Rejected with the `openssl-sys` ban: system OpenSSL patching and cross-compile cost.
- **Allowing MPL-2.0 and EPL-2.0 now.** Rejected: no current dependency needs them. MPL-2.0 is added with OPC UA; EPL-2.0 waits for legal review.
- **Advisory warnings instead of blocking.** Rejected for vulnerabilities, because warnings are ignored. Unmaintained advisories are the exception (see above). Cost accepted: a new advisory can turn CI red without a code change.
- **MSRV pinned to the latest stable only.** Rejected: it forces needless upgrades and gives no signal when a dependency raises its floor. Stable minus two with a dependency-driven floor is the policy.
- **Vendoring all dependencies.** Not adopted for the MVP: it does not fix vulnerable versions, and `Cargo.lock` plus `--locked` already makes builds reproducible. It can be revisited for air-gapped builds.
- **Banning `ring` as unmaintained.** Moot: that advisory was withdrawn.

## Consequences

**Easier**

- One TLS stack and one set of build requirements across x86_64 and aarch64; no cmake or clang on the cross-compile path.
- Known-bad versions cannot enter the build unnoticed.
- License policy is stated in advance for the candidates on the roadmap. Several license files are still unconfirmed **[V]** (see "License allow-list"), so the policy is settled only where the evidence exists.

**Harder**

- Quarterly Arrow major upgrades and frequent `object_store` and `pyo3` breaking minors need a scheduled upgrade routine.
- Advisories can fail CI at any time. An `ignore` entry needs a written reason and an owner **[A]**.
- If FIPS or a library that requires `aws-lc-rs` is needed later, the TLS decision reopens.
- Fuzz targets are extra upkeep.

**Fixed**

- No `openssl-sys`, `aws-lc-sys`, or `serde_yaml` in the dependency tree without a new ADR.
- The MSRV floor never drops below 1.89.
- Dependencies are built with explicit features.

## Evidence and validation required

- [ ] PR1 CI green on native x86_64 and aarch64 runners.
- [ ] The cargo-deny self-demonstration: a scratch branch adding `aws-lc-sys` or `serde_yaml` fails `cargo deny`, and a scratch branch with an older `rustls` fails the advisory check.
- [ ] `scripts/check-deps.sh --self-test` fails on the violating fixture.
- [ ] Whether `cargo deny` bans can express the minimum-version rules directly, or the policy relies on advisories **[V]**.
- [ ] `ring` builds cleanly for aarch64, natively and cross-compiled.
- [ ] The `cargo tree` guard and the `all-features` coverage question are resolved.
- [ ] A job builds with Rust 1.89 to verify `rust-version`. The current `.github/workflows/ci.yml` has no MSRV job and no hard-coded MSRV; the job is added in a later PR.
- [ ] License-file checks for `ros2-client`, `r2r`, and the `open62541` bindings **[V]**, and the `object_store` license metadata **[V]**.
- [ ] Legal review of EPL-2.0 before any `paho-mqtt` adoption (ADR-0006).
- [ ] First fuzz targets (PR8 decoder) exist before PR8 merges.

## Revisit when

- FIPS compliance is required.
- A dependency raises its MSRV beyond the policy.
- Advisories change for `ring` or `rustls`, or `aws-lc-sys` advisories stop.
- OPC UA lands (add MPL-2.0 and review the Marvin advisory).
- Iceberg or Delta lands (MSRV 1.94).
- A new advisory affects a pinned minimum.

## References

- ARCHITECTURE.md sections 9 (dependency rules), 16, 19, 21 (PR1), 22.
- ADR-0006 (MQTT client: paho, ntex-mqtt MSRV, TLS), ADR-0012 (YAML crate, `serde-saphyr`, MSRV 1.89), ADR-0013 (Parquet and LZ4), ADR-0008 (ClickHouse, MSRV 1.89), ADR-0007 (ROS 2, fuzzing the CDR decoder).
- Files: `deny.toml`, `rust-toolchain.toml`, `Cargo.toml`, `scripts/check-deps.sh`, `scripts/fixtures/violating-metadata.json`, `.github/workflows/ci.yml`.
- `product.md` section 25.
- Design history: ecosystem maturity review sections 4, 5, 7, 10, 11, 13, 14, 15 (item 5), as checked 2026-10-07; Rust implementation plan section 1 (workspace policy); architecture review section 10.
- RustSec advisory database (https://github.com/rustsec/advisory-db).
