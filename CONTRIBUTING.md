# Contributing to Thadam

Thanks for your interest. Thadam is a young, pre-alpha project with a deliberately high correctness bar — the README explains where it stands — and it is record-first: design decisions are accepted ADRs before major code lands. This guide covers tooling, checks, and process.

## Development environment

- **Rust:** the toolchain is pinned by [`rust-toolchain.toml`](rust-toolchain.toml) (channel `1.99.0`, minimal profile, with `rustfmt` and `clippy`; the Linux `x86_64` and `aarch64` `unknown-linux-gnu` targets). With rustup installed, `cargo build` in the repo downloads it automatically. The workspace MSRV is **Rust 1.89** (`rust-version` at the workspace root); only the pinned toolchain is used for CI and releases.
- **Docker** (or a compatible runtime) for broker-dependent integration tests (`thadam-mqtt`'s `docker_e2e` and conformance suites are `#[ignore]`d by default).
- Recommended: `cargo nextest` (0.9.x), `cargo deny` (0.20.x). CI installs both.
- `mdbook` only if you work on the documentation site (`cargo install mdbook --locked`; build with `mdbook build docs`).

## Workspace layout

```text
crates/thadam-core          model, errors, positions, connector traits, clock, shutdown, disk budget
crates/thadam-config        YAML → typed PipelineSpec, interpolation, diagnostics, JSON Schema
crates/thadam-wal           capture WAL: writer thread, readers, cursors, retention, recovery
crates/thadam-transform     chains: router, decoders, shapes, steps, chain compiler (sync, pure)
crates/thadam-runtime       drivers, supervisor, shutdown, replay, observability, admin API
crates/thadam-arrow         Event → Arrow, dataset schema mapping, Parquet field IDs
crates/thadam-segments      Parquet/NDJSON segment sink, catalog, quarantine (in progress)
crates/thadam-object-store  S3-compatible shipper over the segment catalog (in progress)
crates/thadam-mqtt          MQTT source
crates/thadam-testkit       memory connectors, conformance suites, fake clock, crash harness
apps/thadam-cli             the `thadam` binary; the only composition root
docs/                      this book (docs/src) plus design docs and ADRs (docs/design)
examples/edge-capture/     the demo pipeline
spikes/                    throwaway spike code (excluded from the workspace)
```

Dependency direction is enforced (`thadam-core` has no Tokio/Arrow/protocol/YAML; `thadam-runtime` knows connectors only via traits; connectors never depend on the runtime) — see ARCHITECTURE.md section 9 and `scripts/check-deps.sh`.

## Before you push: the checks

These mirror CI exactly and should pass on Linux x86_64 (and aarch64 when available):

```bash
cargo fmt --all --check
cargo clippy --workspace --exclude thadam-python --all-targets --locked -- -D warnings
cargo build --workspace --exclude thadam-python --locked
cargo nextest run --workspace --exclude thadam-python --locked
scripts/check-deps.sh                      # dependency-direction guard
scripts/check-deps.sh --self-test          # the guard fails on its violating fixture
cargo deny --locked check                  # advisories, bans, licenses, sources
```

Notes:

- Tests run under nextest; the WAL crash-loop test group is serialized (`.config/nextest.toml`).
- `clippy` runs with `-D warnings`; the workspace denies `unwrap_used`/`expect_used` outside tests (`clippy.toml` allows them in tests only).
- `#![forbid(unsafe_code)]` everywhere; filesystem syscalls go through `rustix`.
- Docker-gated tests: `cargo nextest run -p thadam-mqtt --run-ignored only -E 'binary(conformance) | binary(docker_e2e)'`.
- Building the docs: `mdbook build docs` (output in `docs/book/`, gitignored).

## Dependency and toolchain policy (ADR-0020)

- License allow-list: Apache-2.0, MIT, BSD-2/3-Clause, ISC, Unicode-3.0, Zlib.
- Bans: `aws-lc-sys`, `openssl-sys`, `serde_yaml`. Sources: crates.io only, with a `Cargo.lock` (`--locked` builds).
- Adversarial-input parsers need `cargo fuzz` targets (the JSON decoder has one; WAL frames are a recommended target).
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

- Anyone may draft; **only the project owner accepts** (Draft → Proposed → Accepted → Superseded).
- An accepted ADR is never edited; supersede it with a new one.
- Decisions that depend on measurements list the required spike/benchmark under "Evidence and validation required" and stay Draft/Proposed until it exists.

## Reporting bugs and requesting features

Use the [issue tracker](https://github.com/Dineshkarthik/thadam/issues) — the templates ask for the information that makes reports actionable. For **security issues, do not open a public issue**; see [SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contributions are licensed under the project's [Apache-2.0 license](LICENSE).
