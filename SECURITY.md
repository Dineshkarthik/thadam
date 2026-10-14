# Security Policy

## Reporting a vulnerability

**Please do not open a public GitHub issue for a security report.**

Report privately through [GitHub's private vulnerability reporting](https://github.com/Dineshkarthik/thadam/security/advisories/new) ("Report a vulnerability" on the repository's Security tab). Include enough detail to reproduce the issue: affected commit, configuration, environment, and impact.

You can expect an acknowledgement within 7 days and a status update at least every 14 days until resolution. We will coordinate a fix and disclosure timeline with you and credit reporters (unless you prefer otherwise) in the release notes.

Out of scope for the private channel: missing features, performance questions, and design feedback — those belong in the public [issue tracker](https://github.com/Dineshkarthik/thadam/issues). Also note the documented, deliberate limits below before reporting.

## Supported versions

Thadam is **pre-release** software (pre-alpha; no versioned releases yet). There is no stable branch and no back-porting program: security fixes land on `main` and everyone is expected to follow it. Once numbered releases begin, this policy will name the supported lines.

| Version | Status |
|---|---|
| `main` (pre-release) | Supported — fixes land here |
| any older commit | Not supported — upgrade |

## Scope notes and documented limits

These are deliberate design decisions, not vulnerabilities; please read before reporting (they also appear in the [claims register](docs/design/adr/0011-delivery-guarantees-and-claims-register.md)):

- **The admin API is intended to be loopback-only or bearer-token protected.** Binding it to a non-loopback address without a token is a configuration error the runtime refuses. Exposing it further is your responsibility.
- **CRC32C in the WAL and catalog is an integrity check against corruption, not tamper protection.** Thadam does not sign or encrypt local files.
- **Encryption at rest is out of scope for the MVP**: use LUKS, filesystem-level encryption, and OS file permissions on the data directory.
- **`on_full: drop_oldest` discards the oldest captured data** when the WAL budget fills. With `block`, data loss can instead happen silently in the broker's queue when it overflows. Both are documented, explicit policies, not bugs.
- **MQTT QoS 0 and best-effort sources drop visibly under overload** (counted, by design); QoS 2 is experimental.
- **Power-loss durability requires flush-honoring storage** and is not claimed until hardware-rig evidence exists; SD cards are not a supported durability target.
- **`--repair` deliberately accepts** pipeline-name and WAL-epoch mismatches after operator inspection. It is guarded by requiring an explicit flag, but it is a sharp tool by design.

## Dependency policy

Dependency and toolchain security follows [ADR-0020](docs/design/adr/0020-dependency-security-and-toolchain-policy.md):

- **`cargo deny check` is blocking in CI**: RustSec advisories (minimum-secure versions are pinned, e.g. `rustls` ≥ 0.23.45, `bytes` ≥ 1.11.1, `tokio` ≥ 1.44.2, `object_store` ≥ 0.10.2), a license allow-list (with scoped `deny.toml` MPL-2.0 exceptions for `async-opcua*` and `option-ext` behind the default-off `opcua` and `ros2-live` features), bans on `aws-lc-sys`/`openssl-sys`/`serde_yaml` (`serde_yaml` is exceptioned in `deny.toml` solely for `async-opcua-core` and `zenoh-config` behind those two default-off features), time-boxed advisory exceptions `RUSTSEC-2023-0071` (`rsa` via `async-opcua-crypto`, mitigated by a handshake rate limiter) and `RUSTSEC-2026-0041` (`lz4_flex` via `zenoh-transport`, compression disabled) reviewed by `2027-04-01`, and crates.io-only sources with a committed `Cargo.lock`.
- **TLS is rustls with the `ring` provider** everywhere. Where an upstream dependency could not be used safely (the MQTT client's TLS features pin a vulnerable `rustls-webpki`), TLS is terminated inside Thadam instead of linking the vulnerable path (ADR-0006 amendment).
- **Unsafe code is forbidden** across the workspace (`#![forbid(unsafe_code)]`); platform syscalls go through `rustix`'s safe API.
- **Untrusted-input parsers get fuzz targets**: six `cargo fuzz` targets run in CI (`fuzz-smoke`) across `thadam-transform` (`chain_process`, `spec_compile`, `text_parsers`), `thadam-ros2-msg` (`msg_parse`, `cdr_decode`), and `thadam-sparkplug` (`decoder_handle`); WAL frame decoding is a recommended future target.
- Credentials are handled through `Secret<T>` and are redacted from logs and diagnostics.

Dependency vulnerabilities are tracked through `cargo deny` in CI rather than this page; if you find a fixable advisory in the tree, a public issue (or private report, for exploitable paths) is welcome.
