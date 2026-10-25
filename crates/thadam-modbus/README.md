# thadam-modbus

Modbus TCP register and coil polling source connector for Thadam. It polls configured ranges on one connection and emits one JSON message per poll, at-most-once. A Modbus exception on a single read becomes a null value and does not drop the session.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-modbus@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-modbus
- User docs: https://thadam.dineshkarthik.me/docs/sources/modbus
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-modbus
- License: Apache-2.0
