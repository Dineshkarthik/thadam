# thadam-mcap

Resumable MCAP and rosbag2 file replay source connector for Thadam. It replays one or more MCAP files, including rosbag2's MCAP storage, as one finite stream merged in log-time order. A restart continues each file after its last committed message; a crash between that commit and the resume write can repeat that window. `McapExporter` writes captured messages out as one indexed MCAP file.

Part of [Thadam](https://thadam.dineshkarthik.me), the open data plane for robotics.

## Status

Beta (`0.1.0-beta.1`). Public API may change between pre-releases until the compatibility policy is written; pin the exact version.

## Install

```bash
cargo add thadam-mcap@=0.1.0-beta.1
```

For the command-line tool, run `cargo install thadam --version 0.1.0-beta.1 --locked` instead.

## Where it fits

It depends on `thadam-core` and `thadam-ros2-msg`. The `thadam` CLI depends on it.

## Links

- API docs: https://docs.rs/thadam-mcap
- User docs: https://thadam.dineshkarthik.me/docs/sources/ros2
- Source: https://github.com/Dineshkarthik/thadam/tree/main/crates/thadam-mcap
- License: Apache-2.0
