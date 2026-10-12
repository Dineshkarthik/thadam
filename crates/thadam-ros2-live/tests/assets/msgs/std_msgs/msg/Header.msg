# This is a ROS message definition, vendored from ros2/common_interfaces (jazzy branch)
# Index: jazzy
# Source: https://github.com/ros2/common_interfaces/blob/jazzy/std_msgs/msg/Header.msg
#
# Standard metadata for higher-level stamped data types.
# This is generally used to communicate timestamped data
# in a particular coordinate frame.

# Two-integer timestamp that is expressed as seconds and nanoseconds.
builtin_interfaces/Time stamp

# Frame this data is associated with.
string frame_id
