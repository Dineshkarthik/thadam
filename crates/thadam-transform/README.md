# thadam-transform

Synchronous transform chain from raw WAL frames to typed event batches for Thadam. A chain matches on source and an MQTT-style topic filter, decodes to declared shapes, and runs the configured steps, including dedup, explode, validate, filter, and window aggregates. Per-event failures become rejects that keep the raw bytes.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-transform@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core`, `thadam-ros2-msg`, and `thadam-sparkplug`. `thadam-runtime` depends on it.

## Links

- API docs: https://docs.rs/thadam-transform
- User docs: https://thadam.dineshkarthik.me/docs/transforms/index
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-transform
- License: Apache-2.0
