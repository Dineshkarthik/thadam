# thadam-object-store

Shipper of finalized Parquet segments to S3-compatible object storage for Thadam. As `kind: object_store` it runs as a service over a segment sink's catalog, uploading finalized files oldest first and checking size and checksum when an object already exists.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-object-store@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core` and `thadam-segments`. The `thadam` CLI depends on it. `thadam-runtime` and `thadam-iceberg` use it from their tests.

## Links

- API docs: https://docs.rs/thadam-object-store
- User docs: https://thadam.dineshkarthik.me/docs/sinks/object-store
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-object-store
- License: Apache-2.0
