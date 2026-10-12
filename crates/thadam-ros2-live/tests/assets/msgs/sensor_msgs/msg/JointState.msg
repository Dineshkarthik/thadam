# This is a ROS message definition, vendored from ros2/common_interfaces (jazzy branch)
# Index: jazzy
# Source: https://github.com/ros2/common_interfaces/blob/jazzy/sensor_msgs/msg/JointState.msg
#
# This is a message that holds data to describe the state of a set of torque controlled joints.
#
# The state of each joint (revolute or prismatic) is defined by:
#  * the position of the joint (rad or m),
#  * the velocity of the joint (rad/s or m/s) and
#  * the effort that is applied in the joint (Nm or N).
#
# Each joint is identified by its joint name in the "name" field.
# The number of joint names must be equal to the number of elements in the
# "position", "velocity" and "effort" arrays.
#
# Times are listed in the header of the message.
std_msgs/Header header

string[] name
float64[] position
float64[] velocity
float64[] effort
