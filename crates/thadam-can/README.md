# thadam-can

Linux SocketCAN frame capture and DBC signal decoding source connector for Thadam. It emits one message per CAN or CAN FD frame, either raw bytes or JSON signals decoded from a DBC. Delivery is at-most-once: frames that do not fit the queue while the runtime is applying backpressure are dropped and counted.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version. Linux only (SocketCAN).

## Install

```bash
cargo add thadam-can@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-can
- User docs: https://thadam.dineshkarthik.me/docs/sources/can
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-can
- License: Apache-2.0
