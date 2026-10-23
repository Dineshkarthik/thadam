# thadam-ros2-msg

Transport-independent ROS 2 .msg schema parser and XCDR1 payload decoder for Thadam. It parses `.msg` text, including the concatenated multi-definition documents MCAP uses, resolves nested types into shapes, and decodes little- and big-endian XCDR1 payloads. XCDR2 is rejected.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-ros2-msg@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core`. `thadam-mcap`, `thadam-ros2-live`, and `thadam-transform` depend on it.

## Links

- API docs: https://docs.rs/thadam-ros2-msg
- User docs: https://thadam.dineshkarthik.me/docs/sources/ros2
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-ros2-msg
- License: Apache-2.0
