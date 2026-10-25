# thadam-core

Canonical in-memory event model, value types, and connector traits for Thadam. Shared types cover events, values, shapes, times, quality, and identity, plus errors, WAL positions, connector traits, and clock, shutdown, and disk-budget primitives. WAL encoding, Arrow types, protocol types, YAML types, and the Tokio runtime stay out of this crate.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-core@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on no other Thadam crate. Every other crate under `crates/` and the `thadam` CLI depend on it.

## Links

- API docs: https://docs.rs/thadam-core
- User docs: https://thadam.dineshkarthik.me/docs/concepts/event-model
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-core
- License: Apache-2.0
