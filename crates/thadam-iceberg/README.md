# thadam-iceberg

Experimental Apache Iceberg table registrar for uploaded Parquet segments in Thadam. Configured as `kind: iceberg`, it runs as a service and registers already-uploaded Parquet files on a dataset's table with `fast_append`, without rewriting those files. Schema changes follow the Parquet field IDs already stored in the segments.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`), experimental. Public API may change between pre-releases until the compatibility policy is written; pin the exact version. It needs Rust 1.94; the other Thadam crates have a minimum supported Rust version of 1.89. It is an opt-in feature of the `thadam` CLI (`iceberg`).

## Install

```bash
cargo add thadam-iceberg@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core` and `thadam-segments`. The `thadam` CLI depends on it through the optional `iceberg` feature. Its tests also use `thadam-arrow` and `thadam-object-store`.

## Links

- API docs: https://docs.rs/thadam-iceberg
- User docs: https://thadam.dineshkarthik.me/docs/sinks/iceberg
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-iceberg
- License: Apache-2.0
