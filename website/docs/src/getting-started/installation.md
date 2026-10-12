# Installation

There are no prebuilt binaries or published crates yet (nothing is published before the naming ADR, [ADR-0016](../design/index.md)); the supported way to run Thadam today is to build from source.

## Supported platforms

- **Linux x86_64** and **Linux ARM64 (aarch64)** are the only durability targets. Production deployments run here.
- **macOS** builds work for development only. Durability claims do not cover macOS: fsync semantics differ and the WAL format is validated on Linux reference hardware.
- The supported storage baseline is **industrial SSD or eMMC that honors flush**. SD cards are not a supported target for durability claims (owner decision, 2026-10-07).
- Target resource envelope: 1–4 CPU cores, 256 MB–2 GB RAM depending on workload, local SSD.

The minimum supported Rust version (MSRV) is **1.89**; the repository pins a newer stable toolchain for reproducible builds (see below).

## Build from source

### 1. Install Rust

The repository pins the exact toolchain in `rust-toolchain.toml` (channel `1.99.0`, with `rustfmt` and `clippy` and the two Linux cross targets). With [rustup](https://rustup.rs) installed, cloning is enough — rustup downloads the pinned toolchain the first time you build:

```bash
git clone https://github.com/Dineshkarthik/thadam.git
cd thadam
rustup show        # installs the pinned toolchain if needed
```

### 2. Build the CLI

```bash
cargo build --release -p thadam-cli
```

The binary is `target/release/thadam`. Every crate builds with `#![forbid(unsafe_code)]`; filesystem syscalls go through `rustix`'s safe API.

### 3. Checking your build {#checking-your-build}

```text
$ thadam version
thadam 0.0.0
config version: 1
config hash version: 1
```

The version line reports the protocol versions the binary understands: the configuration format version (`version: 1` in every config file) and the config-hash algorithm version. Upgrade incompatibilities are refused with exit code 3 rather than guessed; see [Crash recovery](../operations/crash-recovery.md).

### Optional: lint and test

The contributor checks mirror CI:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
scripts/check-deps.sh
cargo deny check
```

## Container runtime (for demos and tests)

The [first pipeline](first-pipeline.md) and the integration tests use Docker (or a compatible runtime) for an MQTT broker. Any OCI runtime works; the examples use `eclipse-mosquitto:2` images.

## Next

Continue to [Your first pipeline](first-pipeline.md).
