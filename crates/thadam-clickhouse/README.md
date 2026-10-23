# thadam-clickhouse

ClickHouse native-block batch sink connector with schema validation for Thadam. It encodes one dataset's events with `thadam-arrow`, builds a ClickHouse native block, and inserts that block into an existing table.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version. It is on by default in the `thadam` CLI (`clickhouse` feature).

## Install

```bash
cargo add thadam-clickhouse@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core` and `thadam-arrow`. The `thadam` CLI depends on it through the default `clickhouse` feature.

## Links

- API docs: https://docs.rs/thadam-clickhouse
- User docs: https://thadam.dineshkarthik.me/docs/sinks/clickhouse
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-clickhouse
- License: Apache-2.0
