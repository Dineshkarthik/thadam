# thadam-arrow

Encoding of Thadam dataset events into Apache Arrow record batches with stable field IDs. Events of one dataset are encoded from a declared schema, with envelope and lineage columns and a stable Parquet field ID on every column, nested columns included.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-arrow@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. `thadam-segments` and `thadam-clickhouse` depend on it. `thadam-config` and `thadam-iceberg` use it from their tests.

## Links

- API docs: https://docs.rs/thadam-arrow
- User docs: https://thadam.dineshkarthik.me/docs/
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-arrow
- License: Apache-2.0
