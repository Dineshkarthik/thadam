#!/usr/bin/env python3
"""Print the resolved default RMW implementation identifier.

Used by the spike Docker images at build time (versions.txt) because RUN
uses dash and bashisms for inlining get messy; requires the ROS setup to
have been sourced by the caller.
"""

import sys

from rclpy.utilities import get_rmw_implementation_identifier

sys.stdout.write(get_rmw_implementation_identifier() + "\n")
