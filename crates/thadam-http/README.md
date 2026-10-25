# thadam-http

HTTP ingest server and polling source connector with TLS for Thadam. In `listen` mode a POST body becomes one message and the response is sent after commit. In `poll` mode each successful fetch becomes one message, at-most-once per response.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-http@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-http
- User docs: https://thadam.dineshkarthik.me/docs/sources/http
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-http
- License: Apache-2.0
