# Installation

Thadam `0.1.0-beta.2` is published to crates.io (the `thadam` CLI and all 22 library crates) and PyPI (`thadam` `0.1.0b2`; see [ADR-0016](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0016-naming-and-publishing.md)). There are no prebuilt CLI release binaries or published container images yet (see [Deployment](../operations/deployment.md) to build the Docker image locally).

## Supported platforms

- **Linux x86_64** and **Linux ARM64 (aarch64)** are the only durability targets. Production deployments run here.
- **macOS** builds work for development only. Durability claims do not cover macOS: fsync semantics differ and the WAL format is validated on Linux reference hardware.
- The supported storage baseline is **industrial SSD or eMMC that honors flush**. SD cards are not a supported target for durability claims.
- Target resource envelope: 1–4 CPU cores, 256 MB–2 GB RAM depending on workload, local SSD.

The minimum supported Rust version (MSRV) is **1.89** (`thadam-iceberg` requires **1.94**); the repository pins a newer stable toolchain for reproducible builds from a checkout (see below).

## Install the CLI from crates.io

With a Rust toolchain of version 1.89 or newer installed (for example via [rustup](https://rustup.rs)), install the `thadam` CLI from crates.io. Because only pre-releases are published today, a bare `cargo install thadam` fails; pass `--version 0.1.0-beta.2` explicitly:

```bash
cargo install thadam --version 0.1.0-beta.2 --locked
```

The `clickhouse` sink is enabled by default (`--no-default-features` omits it). Enable opt-in connectors with `--features`:

- `opcua` — the [OPC UA source](../sources/opcua.md) (`kind: opcua`);
- `ros2-live` — the experimental [live ROS 2 source](../sources/ros2-live.md) (`kind: ros2_live`);
- `iceberg` — the experimental [Apache Iceberg registrar](../sinks/iceberg.md) (`kind: iceberg`, requires Rust **1.94** or newer).

```bash
cargo install thadam --version 0.1.0-beta.2 --locked --features opcua,ros2-live,iceberg
```

## Install the Python SDK from PyPI

```bash
pip install thadam
```

To pin the current pre-release explicitly, run `pip install thadam==0.1.0b2` (`pip` installs `0.1.0b2` without `--pre` because no final release exists yet). Requires Python 3.9 or newer and installs `pyarrow >= 14` as a dependency.

Prebuilt `abi3` wheels (CPython 3.9+) are published for Linux glibc (`manylinux_2_28`) and musl (`musllinux_1_2`) on `x86_64` and `aarch64`, and for macOS `arm64` (11.0+) and `x86_64`. Other platforms (such as Windows) build from the source distribution and require a Rust 1.89+ toolchain.

The `thadam` PyPI package is the [Python SDK](../sdk/python.md) (`validate`, `config_hash`, `read_segments`, `Pipeline`, `Diagnostic`, `ThadamError`). It does not install a `thadam` console command; install the CLI with `cargo install` above.

## Embed the Rust library crates

All 22 Thadam library crates are published on crates.io as a supported embedding path. Pin the exact pre-release version with `=` because Cargo's default caret requirement (`^0.1.0-beta.2`) also accepts later `0.1.0-beta.x` releases, and public APIs can change between pre-releases while the compatibility policy is open:

```bash
cargo add thadam-runtime@=0.1.0-beta.2 thadam-config@=0.1.0-beta.2
```

See [Rust crates](../sdk/rust.md) for the crate list and an in-process pipeline example.

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
cargo build --release -p thadam
```

The binary is `target/release/thadam`. Every crate builds with `#![forbid(unsafe_code)]`; filesystem syscalls go through `rustix`'s safe API.

### 3. Checking your build {#checking-your-build}

```text
$ thadam version
thadam 0.1.0-beta.2
config version: 1
config hash version: 1
```

The version line reports the protocol versions the binary understands: the configuration format version (`version: 1` in every config file) and the config-hash algorithm version. Upgrade incompatibilities are refused with exit code 3 rather than guessed; see [Crash recovery](../operations/crash-recovery.md).

### Optional: lint and test

The contributor checks mirror CI:

```bash
cargo fmt --check
cargo clippy --workspace --exclude thadam-python --all-targets -- -D warnings
cargo nextest run --workspace --exclude thadam-python
scripts/check-deps.sh
cargo deny check
```

## Container runtime (for demos and tests)

The [first pipeline](first-pipeline.md) and the integration tests use Docker (or a compatible runtime) for an MQTT broker. Any OCI runtime works; the examples use `eclipse-mosquitto:2` images.

## Next

Continue to [Your first pipeline](first-pipeline.md).
