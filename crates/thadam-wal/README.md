# thadam-wal

Capture-first write-ahead log with segmented files, durable cursors, and retention for Thadam. A writer thread group-commits raw messages; async readers, durable cursors, retention, and crash recovery sit beside it. The log stores raw messages only and does not decode events.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-wal@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. `thadam-runtime` and the `thadam` CLI depend on it.

## Links

- API docs: https://docs.rs/thadam-wal
- User docs: https://thadam.dineshkarthik.me/docs/concepts/wal-and-replay
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-wal
- License: Apache-2.0
