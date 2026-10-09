# Standard metadata for higher-level stamped data types.
# This is generally used to communicate timestamped data
# in a particular coordinate frame.

# Two-integer timestamp that is expressed as:
# * stamp.sec: seconds (stamp_seconds) since epoch (in Python the variable is called 'secs')
# * stamp.nanosec: nanoseconds since stamp_seconds (in Python the variable is called 'nsecs')
# time-handling sugar is provided by the client library
builtin_interfaces/Time stamp

# Transform frame with which this data is associated.
string frame_id
