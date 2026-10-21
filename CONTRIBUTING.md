# Contributing to Thadam

Thanks for your interest. Thadam is a young, beta-stage project with a deliberately high correctness bar — the README explains where it stands — and it is record-first: design decisions are accepted ADRs before major code lands. This guide covers tooling, checks, and process.

## Development environment

- **Rust:** the toolchain is pinned by [`rust-toolchain.toml`](rust-toolchain.toml) (channel `1.99.0`, minimal profile, with `rustfmt` and `clippy`; the Linux `x86_64` and `aarch64` `unknown-linux-gnu` targets). With rustup installed, `cargo build` in the repo downloads it automatically. The workspace MSRV is **Rust 1.89** (`rust-version` at the workspace root), except `crates/thadam-iceberg` (the opt-in `iceberg` feature), which declares **Rust 1.94** for upstream `iceberg` 0.10.1; CI checks `cargo +1.89 check --workspace --exclude thadam-python --exclude thadam-iceberg --locked`, and the pinned toolchain is used for full builds and releases.
- **Docker** (or a compatible runtime) for broker-dependent integration tests (`thadam-mqtt`'s `docker_e2e` and conformance suites are `#[ignore]`d by default).
- Recommended: `cargo nextest` (0.9.x), `cargo deny` (0.20.x). CI installs both.
- `mdbook` 0.5.x only if you work on the site (`cargo install mdbook --locked`); build the whole site with `website/scripts/build.sh`. Notes for automated contributors: [AGENTS.md](AGENTS.md).

## Workspace layout

```text
crates/thadam-core          model, errors, positions, connector traits, clock, shutdown, disk budget, schema/birth stores
crates/thadam-config        YAML → typed PipelineSpec, interpolation, diagnostics, JSON Schema
crates/thadam-wal           capture WAL: writer thread, readers, cursors, retention, recovery
crates/thadam-transform     chains: router, decoders, shapes, steps, chain compiler (sync, pure)
crates/thadam-runtime       drivers, supervisor, shutdown, replay, observability, admin API
crates/thadam-arrow         Event → Arrow, dataset schema mapping, Parquet field IDs
crates/thadam-segments      Parquet segment sink, catalog, quarantine writer
crates/thadam-object-store  S3-compatible shipper over the segment catalog
crates/thadam-iceberg       Apache Iceberg REST table registrar service (Cargo feature `iceberg`, Rust 1.94)
crates/thadam-mqtt          MQTT source and `mqtt_publish` batch sink
crates/thadam-http          HTTP `listen` and `poll` source
crates/thadam-grpc          gRPC `Publish`/`PublishStream` source
crates/thadam-mcap          MCAP / rosbag2 file source and `McapExporter`
crates/thadam-ros2-msg      ROS 2 `.msg` parser and CDR decoder
crates/thadam-ros2-live     live ROS 2 source over `rmw_zenoh` (Cargo feature `ros2-live`, experimental)
crates/thadam-modbus        Modbus TCP polling source
crates/thadam-can           Linux SocketCAN source (`raw` and DBC `decoded`)
crates/thadam-opcua         OPC UA `subscribe` and `poll` source (Cargo feature `opcua`)
crates/thadam-sparkplug     Sparkplug B `spBv1.0` protobuf decoder
crates/thadam-nats          NATS JetStream batch sink
crates/thadam-kafka         Apache Kafka batch sink
crates/thadam-clickhouse    ClickHouse batch sink (default-on Cargo feature `clickhouse`)
crates/thadam-testkit       memory connectors, conformance suites, fake clock, crash harness
apps/thadam-cli             the `thadam` binary; the only composition root
tools/thadam-simulator      synthetic fleet to MQTT, with a send-oracle manifest and verifier
tools/thadam-bench          scenario runner and result collection
sdk/python                 `thadam-python` PyO3 extension (excluded from `default-members`)
website/                   the site: public/ (homepage, served at /), docs/ (mdBook user docs, served at /docs/), scripts/ (build.sh, preview.sh, check.py)
docs/design/               engineering records: ADRs, readiness, owner decisions, spikes
examples/edge-capture/     the demo pipeline
spikes/                    throwaway spike code (excluded from the workspace)
```

Dependency direction is enforced (`thadam-core` has no Tokio/Arrow/protocol/YAML; `thadam-runtime` knows connectors only via traits; connectors never depend on the runtime) — see ARCHITECTURE.md section 9 and `scripts/check-deps.sh`.

## Before you push: the checks

These mirror CI exactly and should pass on Linux x86_64 (and aarch64 when available):

```bash
cargo fmt --all --check
cargo clippy --workspace --exclude thadam-python --all-targets --locked -- -D warnings
cargo clippy -p thadam --all-features --all-targets --locked -- -D warnings
cargo clippy -p thadam-mqtt --features v5 --all-targets --locked -- -D warnings
cargo build --workspace --exclude thadam-python --locked
cargo +1.89 check --workspace --exclude thadam-python --exclude thadam-iceberg --locked
cargo nextest run --workspace --exclude thadam-python --locked
cargo nextest run -p thadam --all-features --locked
scripts/check-deps.sh                      # dependency-direction guard
scripts/check-deps.sh --self-test          # the guard fails on its violating fixture
cargo deny --locked check                  # advisories, bans, licenses, sources
shellcheck --severity=warning scripts/*.sh deploy/*.sh website/scripts/*.sh
```

Notes:

- Tests run under nextest; the WAL crash-loop test group is serialized (`.config/nextest.toml`).
- `clippy` runs with `-D warnings`; the workspace denies `unwrap_used`/`expect_used` outside tests (`clippy.toml` allows them in tests only).
- `#![forbid(unsafe_code)]` everywhere; filesystem syscalls go through `rustix`.
- Docker-gated tests: `cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(conformance) | binary(docker_e2e)'`.
- Building the site: `website/scripts/build.sh` writes one artifact to `website/dist/` (gitignored), then `python3 website/scripts/check.py` verifies it. Preview on 127.0.0.1 with `website/scripts/preview.sh`. Deployment is configured separately from this repository. See [website/README.md](website/README.md).

## Dependency and toolchain policy (ADR-0020)

- License allow-list: Apache-2.0, MIT, BSD-2/3-Clause, ISC, Unicode-3.0, Zlib, plus scoped `deny.toml` MPL-2.0 exceptions for `async-opcua*` and `option-ext` (pulled only by the default-off `opcua` and `ros2-live` features).
- Bans and advisories: `aws-lc-sys`, `openssl-sys`, `serde_yaml` (with narrow `deny.toml` exceptions for `async-opcua-core` and `zenoh-config` behind the default-off `opcua` and `ros2-live` features), and time-boxed advisory exceptions `RUSTSEC-2023-0071` (`rsa` via `async-opcua-crypto`, bounded by a handshake rate limiter) and `RUSTSEC-2026-0041` (`lz4_flex` via `zenoh-transport`, compression disabled), both reviewed by `2027-04-01`. Sources: crates.io only, with a `Cargo.lock` (`--locked` builds).
- Adversarial-input parsers have six `cargo fuzz` targets exercised in the `fuzz-smoke` CI job: `thadam-transform` (`chain_process`, `spec_compile`, `text_parsers`), `thadam-ros2-msg` (`msg_parse`, `cdr_decode`), and `thadam-sparkplug` (`decoder_handle`). WAL frames remain a recommended future target.
- TLS is rustls with `ring`, always.

## Python SDK (`sdk/python`)

`thadam-python` (PyO3 + maturin) is a workspace member but is excluded from default builds: `cargo build`, `cargo test`, and `cargo nextest` at the root do not need a Python toolchain. Pass `-p thadam-python` to build or test it directly, and `--exclude thadam-python` to skip it when running workspace-wide commands.

To build and test the SDK (Python 3.12 is what CI uses):

```bash
pip install maturin pytest pyarrow
maturin build -m sdk/python/Cargo.toml --out dist
pip install dist/*.whl
pytest sdk/python/tests
cargo clippy -p thadam-python --all-targets --locked -- -D warnings
```

## Making changes

1. **Small, reviewable PRs.** Aim for focus; the WAL format, connector contracts, and the config syntax are versioned surfaces — changes there need extra care and evidence.
2. **Claims discipline (ADR-0011).** No benchmark, README, or doc may state a guarantee that is not in the [claims register](docs/design/adr/0011-delivery-guarantees-and-claims-register.md), and each claim needs its evidence. Absolute performance numbers belong in spike reports and benchmark results, cited where used.
3. **Tests travel with code.** Unit tests for parsing/validation; integration tests with real brokers/stores (Docker) where practical; failure tests (crash, OOM, disk-full, duplicate input) for connectors.
4. **Commit style.** Imperative, single-purpose commits (recent history: "Add thadam status and validate connector kinds via the registry", "Gate WAL crash-loop kills on child readiness"). Reference ADRs and issues in the body when the change implements or alters a decision.
5. **Documentation.** Update user docs (this book) when user-visible behavior changes; update ADRs' "evidence" checklists when you produce evidence — do not accept an ADR yourself.

## Architecture decision process

Significant decisions are ADRs in `docs/design/adr/` (process and template: [ADR-0000](docs/design/adr/0000-adr-process.md)).

- Anyone may draft; **only the maintainer accepts** (Draft → Proposed → Accepted → Superseded). See [GOVERNANCE.md](GOVERNANCE.md).
- An accepted ADR is never edited; supersede it with a new one.
- Decisions that depend on measurements list the required spike/benchmark under "Evidence and validation required" and stay Draft/Proposed until it exists.

## Releasing (maintainer)

What gets published ([ADR-0016](docs/design/adr/0016-naming-and-publishing.md)): the `thadam` CLI and the 22 library crates under `crates/` except `thadam-testkit` go to crates.io, and the `thadam` Python package (`sdk/python`) goes to PyPI. They all share one version. `cargo publish --workspace --dry-run --locked` and `maturin sdist` plus `twine check` run in CI (`package` and `python-package` jobs) and upload nothing.

1. Set the new version in the root `Cargo.toml` twice: in `[workspace.package]`, and in every internal `version = "..."` under `[workspace.dependencies]`. The Python package reads its version from Cargo. Move the `[Unreleased]` entries into the version's section in `CHANGELOG.md` and give it a date (`## [X.Y.Z] - YYYY-MM-DD`; betas are `0.1.0-beta.N`, which PyPI shows as `0.1.0bN`).
2. Optional dry run: start the `Release` workflow manually (`workflow_dispatch`). It builds and smoke-tests the Docker image and binaries on `x86_64` and `aarch64`, and the Python wheels (Linux `x86_64` and `aarch64`, macOS `arm64`) and sdist. It uploads them as workflow artifacts and publishes nothing.
3. Push a tag `vX.Y.Z` that matches the version. `.github/workflows/release.yml` builds again and fails if the tag and version differ. It then opens a **draft** GitHub Release with `thadam-X.Y.Z-<target>.tar.gz` and a `.sha256` per architecture, using that version's CHANGELOG section as the notes. The `crates-io` and `pypi` jobs wait for approval in their GitHub environments, then publish.
4. Review the draft GitHub Release and publish it. No container image is pushed to a registry.

One-time setup before the first tag:

- **GitHub:** create the environments `crates-io` and `pypi` (Settings → Environments), each with the maintainer as a required reviewer and limited to `v*` tags.
- **PyPI:** add a pending trusted publisher for the project `thadam`: owner `Dineshkarthik`, repository `thadam`, workflow `release.yml`, environment `pypi`. The first upload from the workflow creates the project, so no token is needed.
- **crates.io:** trusted publishing is configured on crates that already exist. Unless crates.io lets you configure it for a new crate, do the first publish by hand: create an API token with the `publish-new` scope, run `cargo publish --workspace --locked` from the tagged commit with `CARGO_REGISTRY_TOKEN` set, then add a trusted publisher (repository `Dineshkarthik/thadam`, workflow `release.yml`, environment `crates-io`) to each of the 23 crates and revoke the token. Later releases go through the workflow. If the first publish is done by hand, reject the `crates-io` job's approval for that tag.

## Reporting bugs and requesting features

Use the [issue tracker](https://github.com/Dineshkarthik/thadam/issues) — the templates ask for the information that makes reports actionable. For **security issues, do not open a public issue**; see [SECURITY.md](SECURITY.md). Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Developer Certificate of Origin

Every commit must carry a sign-off that certifies the [Developer Certificate of Origin 1.1](https://developercertificate.org/). Create it with `git commit -s`, which adds a `Signed-off-by:` line from your git identity:

```bash
git commit -s -m "Describe the change"
```

The sign-off certifies that you have the right to submit the work under the project's [Apache-2.0 license](LICENSE). Commits without the sign-off line are not accepted; if you forgot, amend the commit with `git commit --amend -s`.

## License

By contributing you agree that your contributions are licensed under the project's [Apache-2.0 license](LICENSE).
