# thadam-opcua

Pure-Rust OPC UA subscription and polling client source connector for Thadam. It connects to one server and emits one message per subscription publish (`subscribe`) or one message per timed read (`poll`). There is no application-layer acknowledgement, so delivery is at-most-once per response.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version. It is an opt-in feature of the `thadam` CLI (`opcua`).

## Install

```bash
cargo add thadam-opcua@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it through the optional `opcua` feature.

## Links

- API docs: https://docs.rs/thadam-opcua
- User docs: https://thadam.dineshkarthik.me/docs/sources/opcua
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-opcua
- License: Apache-2.0
