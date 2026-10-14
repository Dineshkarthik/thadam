# ROS 2 MCAP to Parquet

A ROS 2 recording in MCAP form lands in local Parquet with its messages
decoded into typed columns and validated, then the same recording round-trips
back out as MCAP with `thadam export --mcap`.

It shows:

- an `mcap` source replaying a finite file (`pace: max`, `decode: ros2`)
- two datasets decoded from CDR (`sensor_msgs/JointState`,
  `geometry_msgs/PoseStamped`), with flattened dotted columns such as
  `header.stamp`, `pose.position.x`, `pose.orientation.w`
- `time` taken from the message's `header.stamp`
- the `joint_state` and `pose` validation steps, which flag bad messages
  without dropping them
- a local Parquet sink, and `thadam export --mcap` reading the WAL back

The sample recording is generated, not committed. It holds 200 `/joint_states`
messages (six joints, 100 Hz) and 50 `/robot_pose` messages (frame `map`,
10 Hz), with two deliberately bad messages:

- `/joint_states` message 120: `position` has 5 values for 6 names
  (`joint_state_length_mismatch`)
- `/robot_pose` message 30: quaternion norm is 1.2 (`quaternion_not_unit`)

## Prerequisites

- Rust toolchain (the workspace `rust-toolchain.toml` pins it)
- From the repository root, build the CLI:

```sh
cargo build -p thadam-cli
```

Optional, for reading the output outside Thadam: Python with `pyarrow`
(`pip install pyarrow`), or DuckDB. [Foxglove](https://foxglove.dev) opens the
MCAP file directly.

## Run it

Paths in `pipeline.yaml` are relative to this directory, so every command runs
from `examples/ros2-mcap-to-parquet`.

```sh
cd examples/ros2-mcap-to-parquet
THADAM=../../target/debug/thadam

# 1. Generate the sample recording (writes data/sample.mcap, about 18 KB).
cargo run -p thadam-mcap --example make_ros2_sample -- data/sample.mcap

# 2. Check the configuration.
$THADAM validate pipeline.yaml

# 3. Run to completion. --exit-when-sources-finish makes thadam exit once the
#    finite MCAP source is drained, after the Parquet files are finalized.
$THADAM run pipeline.yaml --exit-when-sources-finish

# 4. Inspect the Parquet segments the sink produced.
$THADAM inspect segments data/thadam --sink local

# 5. Optional: query the Parquet files directly.
python3 - <<'EOF'
import glob
import pyarrow.parquet as pq

for ds in ["joint_states", "poses"]:
    path = glob.glob(f"data/thadam/sinks/local/ready/{ds}/**/*.parquet", recursive=True)[0]
    rows = pq.read_table(path).to_pylist()
    flagged = [r for r in rows if r["_quality_status"] != "good"]
    print(ds, len(rows), "rows;", len(flagged), "flagged")
    for r in flagged:
        print("  ", r["_quality_flags"], r["_quality_reason"])
EOF

# 6. Export the recording back to MCAP from the WAL.
$THADAM export pipeline.yaml --mcap data/roundtrip.mcap --from 0
```

Open `data/roundtrip.mcap` in Foxglove to see `/joint_states` and
`/robot_pose` with their `ros2msg` schemas. `--from 0` starts the export at
the beginning of the WAL; this run's WAL is a single segment, so the output
reports `positions: 1..=1`.

## What to expect

Step 3 exits with status 0 once the Parquet files are finalized. Step 4 reports
the two sink files, one per dataset:

```text
sink: local
state         files            bytes
finalized         2            35606
...
  finalized  joint_states     wal 1..=1 rows 200 bytes 23718 ready/joint_states/unsynced/local_....parquet
  finalized  poses            wal 1..=1 rows 50 bytes 11888 ready/poses/unsynced/local_....parquet
```

Row counts: `joint_states` 200, `poses` 50. The Parquet files sit under
`data/thadam/sinks/local/ready/<dataset>/unsynced/`.

Running step 3 again on the same `data/thadam` adds no rows: the MCAP source keeps
its replay position in `data/thadam/sources/bag/resume.json` and skips what was
already committed. Replaying the recording on purpose means a fresh data directory;
deleting `resume.json` alone would append the rows again.

Step 5 prints one flagged row per dataset:

```text
joint_states 200 rows; 1 flagged
   256 joint_state_length_mismatch
poses 50 rows; 1 flagged
   1 quaternion_not_unit
```

Every row carries `_quality_status` (`good` or `bad`), `_quality_flags` (a
bitset), and `_quality_reason`. The bad rows are kept, not dropped:

| Dataset | Message | Timestamp (UTC) | `_quality_status` | `_quality_flags` | `_quality_reason` |
|---|---|---|---|---|---|
| `joint_states` | 120 | 2025-09-16 05:20:01.200 | `bad` | 256 (`INVALID`) | `joint_state_length_mismatch` |
| `poses` | 30 | 2025-09-16 05:20:03.000 | `bad` | 1 (`OUT_OF_RANGE`) | `quaternion_not_unit` |

Step 6 writes the recording back out:

```text
output: data/roundtrip.mcap
positions: 1..=1
messages: 250, channels: 2, gaps: 0, unresolved schemas: 0
```

250 messages (200 + 50) on two channels, each with its original `ros2msg`
schema.

## Files

- `pipeline.yaml`: the configuration
- `README.md`: this file
- `data/`: generated output (sample, WAL, Parquet, round-trip MCAP), git-ignored

The same flow runs as a CI smoke test in
`apps/thadam-cli/tests/ros2_example.rs`, which generates the sample into a temp
directory and asserts the row counts, columns, flags, and round trip above.

See [ROS 2 support](../../website/docs/src/sources/ros2.md) for the source and decoder
reference, and [robotics conventions](../../website/docs/src/concepts/robotics-conventions.md)
for the `pose` and `joint_state` checks.
