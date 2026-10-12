# This is a ROS message definition, vendored from ros2/common_interfaces (jazzy branch)
# Index: jazzy
# Source: https://github.com/ros2/common_interfaces/blob/jazzy/geometry_msgs/msg/Vector3.msg
#
# This represents a vector in free space.

# This is semantically different than a point.
# A vector is always anchored at the origin.
# When a transform is applied to a vector, only the rotational component is applied.

float64 x
float64 y
float64 z
