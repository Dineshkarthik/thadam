# Robotics conventions

Thadam datasets hold whatever fields you declare, but robot data converges on a small set of shared shapes. This page is the house convention for them: which flattened field names mean what, how frames hang together, and which validation steps watch them. Nothing here is enforced by the transport — these are conventions, and the [`pose`](../configuration/transforms.md#pose) and [`joint_state`](../configuration/transforms.md#joint_state) transform steps check the ones machines can check.

## Frames

Every spatial measurement is meaningless without the frame it is expressed in. ROS names frames with strings, and Thadam keeps them as ordinary `str` fields:

| Field | Meaning |
|---|---|
| `header.frame_id` | The frame a message's data is expressed in. Empty means "no frame was set", which is almost always a recorder bug. |
| `child_frame_id` | On a transform, the frame whose pose is expressed in `header.frame_id`. |

Frames form a tree of rigid transforms, conventionally rooted at `map` (the world), through `odom` (locally smooth, drifting), to `base_link` (the robot body), and out to sensor frames (`camera_link`, `lidar`, …). A lookup from leaf to root composes the chain `T(map ← odom) · T(odom ← base_link) · T(base_link ← sensor)`.

**TF is data, not infrastructure.** The transform tree is captured as ordinary datasets — the flattened `tf2_msgs/TFMessage` and `tf2_msgs/TFMessage` (static) streams, one event per `geometry_msgs/TransformStamped` element — so the tree state at any moment is replayable from the WAL exactly like any sensor stream. There is no special TF sidecar; a tree query is a replay plus composition in the consumer.

## How ROS 2 messages flatten

The `thadam-ros2-msg` compiler builds one Thadam `Shape` per message type. Nested scalar messages flatten to **dotted field names**, so `geometry_msgs/PoseStamped` becomes `header.stamp` (a `Ts`), `header.frame_id` (`str`), `pose.position.x/y/z` (`f64`), and `pose.orientation.x/y/z/w` (`f64`); `sensor_msgs/JointState` becomes `header.stamp`, `header.frame_id`, `name` (`list<str>`), and `position`/`velocity`/`effort` (`list<f64>` typed arrays). The full type mapping — scalar widening, array bound forms, the `builtin_interfaces/Time` triple — is documented in the [type-mapping section of `crates/thadam-ros2-msg/src/lib.rs`](https://docs.rs/thadam-ros2-msg) and mirrored on the [ROS 2 source page](../sources/ros2.md#type-mapping).

### Declaring one as a dataset {#declaring-one-as-a-dataset}

Dotted names are first-class declared fields: each is **one literal flat column**, with every dot-separated segment following the usual name rules (at most 63 characters, starts with a letter, then letters, digits, and `_` — so no leading, trailing, or doubled dots). When a field's `from` is omitted it defaults to `/<name>` as a single JSON Pointer token, which reads exactly the flattened key the decoder writes — so a complete `PoseStamped` dataset needs no explicit pointers at all:

```yaml
datasets:
  - id: poses
    match: { source: bag }
    decode: ros2
    time: { field: /header.stamp, unit: ns, domain: utc, max_skew: 5s }
    fields:
      - { id: 1000, name: header.stamp, type: ts, unit: ns, required: true }
      - { id: 1001, name: header.frame_id, type: str, required: true }
      - { id: 1002, name: pose.position.x, type: f64 }
      - { id: 1003, name: pose.position.y, type: f64 }
      - { id: 1004, name: pose.position.z, type: f64 }
      - { id: 1005, name: pose.orientation.x, type: f64 }
      - { id: 1006, name: pose.orientation.y, type: f64 }
      - { id: 1007, name: pose.orientation.z, type: f64 }
      - { id: 1008, name: pose.orientation.w, type: f64 }

transforms:
  - { kind: pose, dataset: poses }

sinks:
  - id: local
    kind: parquet
    config: { datasets: [poses] }
```

Payload keys the dataset does not declare (here `header.stamp.sec` and `header.stamp.nanosec`, the raw integers beside the `Ts`) are simply not projected. Every downstream consumer treats the names as opaque: Parquet columns carry them verbatim with their field IDs, and the [ClickHouse sink](../sinks/clickhouse.md) backticks them in SQL. `thadam validate` resolves the `pose` step's fields against this declaration — a missing `pose.orientation.w` is a `config.invalid` at `transforms[0].prefix`, without waiting for a chain compile at startup.

## Poses

A pose is a position and a unit quaternion in a named frame:

| Field | Type | Convention |
|---|---|---|
| `<prefix>.position.{x,y,z}` | `f32` or `f64` | Translation in `frame_id`. |
| `<prefix>.orientation.{x,y,z,w}` | `f32` or `f64` | Rotation as a quaternion with norm 1 (recorders drift; `w` last, per `geometry_msgs/Quaternion`). |
| `header.frame_id` | `str` | Non-empty. |

The default `<prefix>` is `pose`, matching a flattened `geometry_msgs/PoseStamped`. The [`pose`](../configuration/transforms.md#pose) step flags records whose components are missing (`pose_missing_field`), not finite (`pose_not_finite`), whose quaternion norm drifts past a tolerance (`quaternion_not_unit`, `OUT_OF_RANGE`), or whose frame id is empty (`missing_frame_id`). Events are **flagged, not dropped** — quality is data ([event model](event-model.md#quality-data-not-logs)).

## Transforms

A transform is a pose with a child frame: the flattened `geometry_msgs/TransformStamped` keeps `header.frame_id` (parent), `child_frame_id` (`str`, non-empty), the translation as `transform.translation.{x,y,z}` (`f64`), and the rotation as `transform.rotation.{x,y,z,w}` (`f64`). That `translation`/`rotation` spelling is deliberately different from the `pose` step's `position`/`orientation` layout, so a transform dataset does not pass `kind: pose` directly: frame and child-frame presence is checked with the frame rules (`frame_field` on the parent, a `validate`-style predicate on the child), and numeric sanity — a near-unit rotation quaternion, a bounded z — with ordinary [`validate`](../configuration/transforms.md#validate) rules. TF datasets (`/tf`, `/tf_static`) are the transform tree as it actually flowed, gaps and all.

## Joint states

`sensor_msgs/JointState` semantics, per the ROS definition and the [`joint_state`](../configuration/transforms.md#joint_state) step that enforces them:

- `name` (`list<str>`) is the joint roster; names must not repeat (`duplicate_joint_name`).
- `position`, `velocity`, `effort` (`list<f32>` or `list<f64>`) are each **empty or exactly as long as `name`** (`joint_state_length_mismatch`). An empty array means the recorder does not publish that quantity; a partial one is corrupt.
- Every published value is finite (`joint_state_not_finite`).

## Calibration

Sensor calibration binds frames to reality and drifts between runs, so capture it as a dataset too — one row per sensor per calibration version, never overwritten in place:

| Field | Type | Meaning |
|---|---|---|
| `sensor_id` | `str` | The sensor the row describes; pairs with the identity levels. |
| `calibration_version` | `str` | An opaque, monotonically assigned tag; consumers take the newest version with `valid_from` at or before the event time. |
| `intrinsics` | `f64` scalars or `list<f64>` | Camera matrix elements, wheel radii, imu biases — whatever the sensor model needs. |
| `extrinsics` | pose fields (`extrinsics.position.*`, `extrinsics.orientation.*`) | The sensor's pose in its reference frame (usually `base_link`); a `pose` step with `prefix: extrinsics` validates the bundle. |
| `valid_from` | `ts` | When the calibration took effect. |

Because calibration rows are events like everything else, a replay pins the calibration that was live during capture — the alternative, "latest wins", silently reprocesses old data under new geometry.
