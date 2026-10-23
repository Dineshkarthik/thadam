# thadam-runtime

Pipeline assembly, driver tasks, supervision, recovery, and replay runtime for Thadam. It validates and builds a pipeline from a connector registry, runs source, chain, segment-sink, batch-sink, and service tasks under a supervisor, and performs startup recovery and WAL-range replay.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-runtime@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-config`, `thadam-core`, `thadam-transform`, and `thadam-wal`. The `thadam` CLI depends on it. Its tests also use `thadam-segments` and `thadam-object-store`.

## Links

- API docs: https://docs.rs/thadam-runtime
- User docs: https://thadam.dineshkarthik.me/docs/concepts/architecture-overview
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-runtime
- License: Apache-2.0
