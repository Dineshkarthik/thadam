# This is a ROS message definition, vendored from ros2/common_interfaces (jazzy branch)
# Index: jazzy
# Source: https://github.com/ros2/common_interfaces/blob/jazzy/geometry_msgs/msg/TransformStamped.msg
#
# This expresses a transform from coordinate frame "header.frame_id" to the coordinate frame "child_frame_id"
#
# This message is mostly used by the
# <a href="https://docs.ros.org/en/latest/p/tf2/">tf2</a> package.
# See its documentation for more information.
#
# The header gives metadata about the transform.
#
# The child_frame_id gives the frame id for which the transform is specified.
# The transform is expressed wrt the frame id given in header.frame_id.
std_msgs/Header header
string child_frame_id
Transform transform
