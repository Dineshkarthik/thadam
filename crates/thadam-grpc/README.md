# thadam-grpc

gRPC ingest server source connector with TLS and ack-after-commit for Thadam. A server implements `thadam.ingest.v1.Ingest`. Each request payload is one message, and the response is sent only after that message is committed.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-grpc@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-grpc
- User docs: https://thadam.dineshkarthik.me/docs/sources/grpc
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-grpc
- License: Apache-2.0
