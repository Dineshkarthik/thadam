# thadam-kafka

Pure-Rust Apache Kafka batch sink connector with TLS and SASL for Thadam. Each event is one record: a rendered topic, a key, a JSON envelope, and an `x-thadam-event-id` header. Delivery is at-least-once; the client has no idempotent producer, so a retried batch can be written twice.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-kafka@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-kafka
- User docs: https://thadam.dineshkarthik.me/docs/sinks/kafka
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-kafka
- License: Apache-2.0
