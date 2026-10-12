# thadam-ros2-live Docker test: ROS 2 Jazzy with rmw_zenoh + test publishers.
#
#   docker build -t thadam-ros2-live-test:jazzy crates/thadam-ros2-live/tests/assets/docker
#
# Same recipe as spike S3 (docs/design/spikes/S3-ros2-live.md §3.1): the
# router container runs rmw_zenohd, the publisher runs sensor_pub.py.
# Images: public Docker Hub only, pinned by tag.
FROM ros:jazzy-ros-base

# Opt-in corporate-proxy hook (same pattern as tools/thadam-simulator/Dockerfile):
# on TLS-intercepting networks drop the host CA bundle at secrets/ca.pem next
# to this file (gitignored). It is installed into the image trust store at
# build time only so apt can reach the ROS apt repo through the proxy.
COPY secrets/ /tmp/thadam-ros2-live-secrets/
RUN set -e; \
    if [ -f /tmp/thadam-ros2-live-secrets/ca.pem ]; then \
        cp /tmp/thadam-ros2-live-secrets/ca.pem /usr/local/share/ca-certificates/thadam-ros2-live-ca.crt; \
        update-ca-certificates; \
    fi

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ros-jazzy-rmw-zenoh-cpp \
        ros-jazzy-tf2-msgs \
        python3 \
    && rm -rf /var/lib/apt/lists/*

COPY sensor_pub.py /usr/local/lib/thadam-ros2-live/sensor_pub.py

# Record the resolved RMW versions in the image for failure forensics.
RUN dpkg-query -W -f='${Package} ${Version}\n' \
        ros-jazzy-rmw-zenoh-cpp ros-jazzy-rmw-fastrtps-cpp ros-jazzy-fastrtps \
        > /usr/local/lib/thadam-ros2-live/versions.txt || true
