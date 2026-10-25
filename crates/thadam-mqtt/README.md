# thadam-mqtt

MQTT source connector with persistent sessions, TLS, and ack-after-commit for Thadam. The source pulls messages and acknowledges only after commit. The same crate provides the `mqtt_publish` batch sink, which publishes one JSON message per event.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.2`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-mqtt@=0.1.0-beta.2
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.2 --locked` instead.

## Where it fits

It depends on `thadam-core`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-mqtt
- User docs: https://thadam.dineshkarthik.me/docs/sources/mqtt and https://thadam.dineshkarthik.me/docs/sinks/mqtt-publish
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-mqtt
- License: Apache-2.0
