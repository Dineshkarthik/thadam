# thadam-config

YAML pipeline configuration loading, interpolation, validation, and JSON Schema for Thadam. It loads YAML into a typed, validated pipeline spec, interpolates `${env:NAME}` and `${file:/path}` strings, reports diagnostics with paths and line numbers, and can emit JSON Schema for editors.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-config@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core`. `thadam-runtime` and the `thadam` CLI depend on it. Its tests also use `thadam-arrow`.

## Links

- API docs: https://docs.rs/thadam-config
- User docs: https://thadam.dineshkarthik.me/docs/configuration/index
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-config
- License: Apache-2.0
