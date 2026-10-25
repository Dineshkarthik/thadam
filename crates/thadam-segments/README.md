# thadam-segments

Parquet segment sink, CRC-checked segment catalog, and quarantine writer for Thadam. The `parquet` sink keeps per-dataset segments and an append-only catalog of finalized files, and writes rejected rows through the quarantine sink.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-segments@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core` and `thadam-arrow`. `thadam-object-store`, `thadam-iceberg`, and the `thadam` CLI depend on it. `thadam-runtime` uses it from tests.

## Links

- API docs: https://docs.rs/thadam-segments
- User docs: https://thadam.dineshkarthik.me/docs/sinks/parquet
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-segments
- License: Apache-2.0
