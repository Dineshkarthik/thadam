# thadam-sparkplug

Sparkplug B wire schema (prost), topic namespace parser, and the stateful BIRTH/alias/seq decoder behind dataset `decode: sparkplug_b`. It parses the `spBv1.0` topic namespace and tracks BIRTH registration, alias-only DATA, sequence gaps, and DEATH staleness across restarts.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-sparkplug@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core`. `thadam-transform` depends on it. The `thadam` CLI uses it only from tests, not as a CLI feature.

## Links

- API docs: https://docs.rs/thadam-sparkplug
- User docs: https://thadam.dineshkarthik.me/docs/sources/sparkplug-b
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-sparkplug
- License: Apache-2.0
