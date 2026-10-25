# thadam-nats

NATS JetStream batch sink connector with message deduplication headers for Thadam. Each event is one JetStream message whose `Nats-Msg-Id` header is the event id, so a stream with a positive duplicate window deduplicates retries of the same batch. The sink reports idempotent-batch delivery only when that window is positive, and at-least-once otherwise.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-nats@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-nats
- User docs: https://thadam.dineshkarthik.me/docs/sinks/nats
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-nats
- License: Apache-2.0
