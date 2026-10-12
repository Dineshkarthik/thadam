#!/usr/bin/env python3
"""Spike S3 sensor-mix publisher (Jazzy: rmw_zenoh_cpp or rmw_fastrtps_cpp).

Publishes, from a single node:
  /imu          sensor_msgs/Imu        1 kHz   best_effort, volatile, depth 5
  /joint_states sensor_msgs/JointState 100 Hz  reliable,    volatile, depth 10
  /tf           tf2_msgs/TFMessage      30 Hz  reliable,    volatile, depth 100
  /tf_static    tf2_msgs/TFMessage     1 shot  reliable, transient_local, depth 1

header.stamp of every published message is set to the node's clock at publish
time. The publisher logs per-topic publish counts every 5 s as ground truth.

Every streaming topic also embeds an EXACT per-topic monotone sequence counter
in a harmless payload field (imu: angular_velocity.x, joint_states: effort[-1],
tf: transforms[*].transform.rotation.x). zenoh-bridge-ros2dds (step 2) forwards
no per-publisher sequence number, so drop accounting through the bridge uses
these payload counters; float64 holds exact integers past 2^53. rmw_zenoh
(step 1) ignores them (its attachment carries the authoritative sequence) and
they serve there as an independent cross-check.
"""

import math

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy

from builtin_interfaces.msg import Time
from geometry_msgs.msg import Transform, TransformStamped
from sensor_msgs.msg import Imu, JointState
from tf2_msgs.msg import TFMessage

IMU_PERIOD = 0.001
JS_PERIOD = 0.01
TF_PERIOD = 1.0 / 30.0
STATIC_DELAY = 1.0
LOG_PERIOD = 5.0


class SensorPub(Node):
    def __init__(self):
        super().__init__("s3_sensor_pub")

        qos_sensor = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=5,
            reliability=ReliabilityPolicy.BEST_EFFORT,
            durability=DurabilityPolicy.VOLATILE,
        )
        qos_reliable = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
        )
        qos_tf = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=100,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
        )
        qos_static = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=1,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
        )

        self.imu_pub = self.create_publisher(Imu, "/imu", qos_sensor)
        self.js_pub = self.create_publisher(JointState, "/joint_states", qos_reliable)
        self.tf_pub = self.create_publisher(TFMessage, "/tf", qos_tf)
        self.tf_static_pub = self.create_publisher(TFMessage, "/tf_static", qos_static)

        self.counts = {"imu": 0, "joint_states": 0, "tf": 0, "tf_static": 0}
        # Exact per-topic sequence counters embedded in the payload (module doc).
        self.send_seq = {"imu": 0, "joint_states": 0, "tf": 0}
        self._static_done = False

        self.create_timer(IMU_PERIOD, self.publish_imu)
        self.create_timer(JS_PERIOD, self.publish_joint_states)
        self.create_timer(TF_PERIOD, self.publish_tf)
        self.create_timer(STATIC_DELAY, self.publish_tf_static_once)
        self.create_timer(LOG_PERIOD, self.log_counts)

        self.get_logger().info("s3_sensor_pub started")

    def now(self) -> Time:
        return self.get_clock().now().to_msg()

    def publish_imu(self):
        msg = Imu()
        msg.header.stamp = self.now()
        msg.header.frame_id = "imu_link"
        msg.orientation.w = 1.0
        # Exact payload sequence counter (module doc: bridge drop accounting).
        msg.angular_velocity.x = float(self.send_seq["imu"])
        msg.angular_velocity.z = 0.1
        msg.linear_acceleration.z = 9.81
        self.imu_pub.publish(msg)
        self.send_seq["imu"] += 1
        self.counts["imu"] += 1

    def publish_joint_states(self):
        t = self.get_clock().now().nanoseconds * 1e-9
        msg = JointState()
        msg.header.stamp = self.now()
        msg.name = [f"joint_{i}" for i in range(1, 7)]
        msg.position = [math.sin(t + i) for i in range(6)]
        msg.velocity = [math.cos(t + i) for i in range(6)]
        # effort[-1] carries the exact payload sequence counter.
        msg.effort = [0.0] * 5 + [float(self.send_seq["joint_states"])]
        self.js_pub.publish(msg)
        self.send_seq["joint_states"] += 1
        self.counts["joint_states"] += 1

    def make_transform(self, child: str, stamp: Time) -> TransformStamped:
        ts = TransformStamped()
        ts.header.stamp = stamp
        ts.header.frame_id = "base_link"
        ts.child_frame_id = child
        ts.transform = Transform()
        ts.transform.translation.x = 0.1
        ts.transform.translation.y = 0.2
        ts.transform.translation.z = 0.3
        ts.transform.rotation.w = 1.0
        return ts

    def publish_tf(self):
        stamp = self.now()
        msg = TFMessage()
        msg.transforms = [
            self.make_transform("laser_link", stamp),
            self.make_transform("camera_link", stamp),
        ]
        # rotation.x carries the exact payload sequence counter (unused
        # quaternion field for these synthetic transforms).
        for tf in msg.transforms:
            tf.transform.rotation.x = float(self.send_seq["tf"])
        self.tf_pub.publish(msg)
        self.send_seq["tf"] += 1
        self.counts["tf"] += 1

    def publish_tf_static_once(self):
        if self._static_done:
            return
        self._static_done = True
        stamp = self.now()
        msg = TFMessage()
        static_ts = self.make_transform("mount_link", stamp)
        static_ts.header.frame_id = "world"
        msg.transforms = [static_ts]
        self.tf_static_pub.publish(msg)
        self.counts["tf_static"] += 1
        self.get_logger().info("published /tf_static once (transient_local)")

    def log_counts(self):
        c = self.counts
        self.get_logger().info(
            f"published so far: imu={c['imu']} joint_states={c['joint_states']} "
            f"tf={c['tf']} tf_static={c['tf_static']}"
        )


def main():
    rclpy.init()
    node = SensorPub()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.shutdown()


if __name__ == "__main__":
    main()
