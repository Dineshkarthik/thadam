# thadam-ros2-live

Experimental live ROS 2 topic source connector over rmw_zenoh for Thadam. It subscribes through an `rmw_zenohd` router and emits raw CDR payloads. Decoding stays on the transform chain's `decode: ros2`. Delivery is at-most-once: under backpressure the receive queue drops and counts instead of blocking the callback.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`), experimental. Public API may change between pre-releases until the compatibility policy is written; pin the exact version. It is an opt-in feature of the `thadam` CLI (`ros2-live`).

## Install

```bash
cargo add thadam-ros2-live@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core` and `thadam-ros2-msg`. The `thadam` CLI depends on it through the optional `ros2-live` feature.

## Links

- API docs: https://docs.rs/thadam-ros2-live
- User docs: https://thadam.dineshkarthik.me/docs/sources/ros2-live
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-ros2-live
- License: Apache-2.0
