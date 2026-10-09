# Navigation Satellite fix status for any Global Navigation Satellite System.
#
# Specified using the WGS 84 reference ellipsoid

# Whether to output an augmented fix is determined by both the fix
# type and the last time differential corrections were received.  A
# value of STATUS_NO_FIX will be reported if the fix type is not good
# enough and the differential corrections have not been received for
# more than the timeout period.
int8 STATUS_NO_FIX =  -1        # unable to fix position
int8 STATUS_FIX =      0        # unaugmented fix
int8 STATUS_SBAS_FIX = 1        # with satellite-based augmentation
int8 STATUS_GBAS_FIX = 2        # with ground-based augmentation

# The Global Navigation Satellite System service(s) used for the fix.
uint16 SERVICE_GPS =     1
uint16 SERVICE_GLONASS = 2
uint16 SERVICE_COMPASS = 4      # includes BeiDou
uint16 SERVICE_GALILEO = 8

# service field is a bitfield of the SERVICE_* constants above
int8 status

# service field is a bitfield of the SERVICE_* constants above
uint16 service
