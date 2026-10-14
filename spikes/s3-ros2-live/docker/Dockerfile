# Spike S3 (ADR-0007 step 1): ROS 2 Jazzy with rmw_zenoh + test publishers.
#
#   docker build -f docker/Dockerfile -t s3-ros2-zenoh:jazzy-1 .
#   (compose in docker/compose.yaml sets this context itself)
#
# Images: public Docker Hub only, pinned by tag.
FROM ros:jazzy-ros-base

# Opt-in TLS-intercepting-proxy hook (same pattern as tools/thadam-simulator/Dockerfile):
# on TLS-intercepting networks run `make secrets/ca.pem` (copies the host CA
# bundle, gitignored). It is installed into the image trust store at build time
# only so apt can reach the ROS apt repo through the proxy.
COPY docker/secrets/ /tmp/s3-secrets/
RUN set -e; \
    if [ -f /tmp/s3-secrets/ca.pem ]; then \
        cp /tmp/s3-secrets/ca.pem /usr/local/share/ca-certificates/s3-ca.crt; \
        update-ca-certificates; \
    fi

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ros-jazzy-rmw-zenoh-cpp \
        ros-jazzy-tf2-msgs \
        python3 \
    && rm -rf /var/lib/apt/lists/*

COPY docker/sensor_pub.py /usr/local/lib/s3/sensor_pub.py

# Record the resolved RMW versions in the image for the spike report.
RUN dpkg-query -W -f='${Package} ${Version}\n' \
        ros-jazzy-rmw-zenoh-cpp ros-jazzy-rmw-fastrtps-cpp ros-jazzy-fastrtps \
        > /usr/local/lib/s3/versions.txt || true
