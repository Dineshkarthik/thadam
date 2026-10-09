# This is a message that holds data to describe the state of a set of torque controlled joints.
#
# The state of each joint (revolute or prismatic) is defined by:
#  * the position of the joint (rad or m),
#  * the velocity of the joint (rad/s or m/s) and
#  * the effort that is applied in the joint (Nm or N).
#
# Each joint is identified by its name and all arrays must be the same size.
# The joint arrays must follow the same order as in the URDF.

std_msgs/Header header

string[] name
float64[] position
float64[] velocity
float64[] effort
