#!/usr/bin/env bash
# Plain-docker equivalent of `docker compose -f compose.yaml up/down`, because
# the sandbox in this dev environment permits `docker run`/`docker build`
# but not `docker compose up`. Runs the identical images/args as compose.yaml.
#
#   run.sh <setup> up | down | logs <name> | ps
#
# Setups:
#   jazzy-zenoh           step 1: Jazzy + rmw_zenoh (s3-router, s3-sensor-pub)
#   jazzy-fastdds-bridge  step 2: Jazzy + Fast DDS via zenoh-bridge-ros2dds
#                         (s3-bridge, s3-sensor-pub-dds)
#   lyrical-zenoh         step 3: Lyrical + rmw_zenoh (s3-router,
#                         s3-sensor-pub)
#   lyrical-dds-bridge    step 3: Lyrical + its default DDS (Fast DDS) via
#                         zenoh-bridge-ros2dds (s3-bridge, s3-sensor-pub-dds)
#   lyrical-rmw-jazzy-router  step 3 cross-distro: Jazzy rmw_zenohd router +
#                         Lyrical rmw_zenoh publishers (s3-router,
#                         s3-sensor-pub)
#   jazzy-rmw-lyrical-router  step 3 cross-distro reverse: Lyrical rmw_zenohd
#                         router + Jazzy rmw_zenoh publishers (s3-router,
#                         s3-sensor-pub)
#   jazzy-cyclone-bridge  step 3 best effort: Jazzy + Cyclone DDS via
#                         zenoh-bridge-ros2dds (s3-bridge, s3-sensor-pub-dds)
#   humble-fastdds-bridge step 3 best effort: Humble + Fast DDS via
#                         zenoh-bridge-ros2dds (s3-bridge, s3-sensor-pub-dds)
#
# All setups share one bridge network (`s3ros2`): the bridge container and
# the DDS publishers must co-locate because DDS discovery is multicast.
# The host capture binary connects to tcp/127.0.0.1:7447 in every setup.
# All setups reuse the same four container names, so `run.sh <any> down`
# cleans everything.
set -euo pipefail

IMAGE_JAZZY=s3-ros2-zenoh:jazzy-1
IMAGE_LYRICAL=s3-ros2-zenoh:lyrical-1
IMAGE_CYCLONE=s3-ros2-zenoh:jazzy-cyclone-1
IMAGE_HUMBLE=s3-ros2-zenoh:humble-1
BRIDGE_IMAGE=eclipse/zenoh-bridge-ros2dds:1.10.1
BRIDGE_ZID=57a9ed
NET=s3ros2
setup="${1:-}"
cmd="${2:-up}"

usage() {
  echo "usage: $0 jazzy-zenoh|jazzy-fastdds-bridge|lyrical-zenoh|lyrical-dds-bridge|lyrical-rmw-jazzy-router|jazzy-rmw-lyrical-router|jazzy-cyclone-bridge|humble-fastdds-bridge up|down|logs <name>|ps" >&2
  exit 2
}

net_up() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
}

wait_log() { # container grep timeout-secs
  local c="$1" pat="$2" n="${3:-60}"
  for _ in $(seq 1 "$n"); do
    if docker logs "$c" 2>&1 | grep -q "$pat"; then return 0; fi
    sleep 0.5
  done
  echo "warn: $c did not log '$pat' in time" >&2
}

# rmw_zenoh router + rmw_zenoh publisher pair.
#   rmw_stack <router-image> <router-distro> <pub-image> <pub-distro>
rmw_stack() {
  local router_image="$1" router_distro="$2" pub_image="$3" pub_distro="$4"
  docker rm -f s3-router s3-sensor-pub >/dev/null 2>&1 || true
  net_up
  docker run -d --name s3-router --network "$NET" \
    -p 127.0.0.1:7447:7447 \
    "$router_image" \
    bash -c "source /opt/ros/$router_distro/setup.bash && exec ros2 run rmw_zenoh_cpp rmw_zenohd" >/dev/null
  wait_log s3-router "7447"
  docker run -d --name s3-sensor-pub --network "$NET" \
    -e RMW_IMPLEMENTATION=rmw_zenoh_cpp \
    -e ROS_DOMAIN_ID=0 \
    -e 'ZENOH_CONFIG_OVERRIDE=connect/endpoints=["tcp/s3-router:7447"];scouting/multicast/enabled=false' \
    "$pub_image" \
    bash -c "source /opt/ros/$pub_distro/setup.bash && exec python3 /usr/local/lib/s3/sensor_pub.py" >/dev/null
  wait_log s3-sensor-pub "s3_sensor_pub started"
}

# zenoh-bridge-ros2dds + DDS publisher pair.
#   dds_stack <pub-image> <pub-distro> <rmw-implementation>
dds_stack() {
  local pub_image="$1" pub_distro="$2" rmw="$3"
  docker rm -f s3-bridge s3-sensor-pub-dds >/dev/null 2>&1 || true
  net_up
  # Bridge: router mode, pinned zenoh id (admin space @/$BRIDGE_ZID/ros2),
  # zenoh-side multicast scouting OFF (nothing may leak onto the LAN);
  # DDS-side multicast is REQUIRED for DDS discovery inside $NET.
  # ROS_DISTRO silences the iron-assumption warning (bridge parses
  # ros_discovery_info).
  docker run -d --name s3-bridge --network "$NET" \
    -p 127.0.0.1:7447:7447 \
    -e ROS_DOMAIN_ID=0 \
    -e "ROS_DISTRO=$pub_distro" \
    -e RUST_LOG=info \
    "$BRIDGE_IMAGE" \
    -i "$BRIDGE_ZID" -d 0 -l tcp/0.0.0.0:7447 --no-multicast-scouting >/dev/null
  wait_log s3-bridge "Zenoh can be reached at"
  docker run -d --name s3-sensor-pub-dds --network "$NET" \
    -e "RMW_IMPLEMENTATION=$rmw" \
    -e ROS_DOMAIN_ID=0 \
    "$pub_image" \
    bash -c "source /opt/ros/$pub_distro/setup.bash && exec python3 /usr/local/lib/s3/sensor_pub.py" >/dev/null
  wait_log s3-sensor-pub-dds "s3_sensor_pub started"
  wait_log s3-bridge "Route Publisher (ROS:/imu" 30
}

case "$setup:$cmd" in
  jazzy-zenoh:up)
    rmw_stack "$IMAGE_JAZZY" jazzy "$IMAGE_JAZZY" jazzy
    echo "up: jazzy-zenoh = s3-router ($NET, host tcp/127.0.0.1:7447), s3-sensor-pub"
    ;;

  jazzy-fastdds-bridge:up)
    dds_stack "$IMAGE_JAZZY" jazzy rmw_fastrtps_cpp
    echo "up: jazzy-fastdds-bridge = s3-bridge ($NET, host tcp/127.0.0.1:7447, zid $BRIDGE_ZID), s3-sensor-pub-dds"
    ;;

  lyrical-zenoh:up)
    rmw_stack "$IMAGE_LYRICAL" lyrical "$IMAGE_LYRICAL" lyrical
    echo "up: lyrical-zenoh = s3-router ($NET, host tcp/127.0.0.1:7447), s3-sensor-pub"
    ;;

  lyrical-dds-bridge:up)
    dds_stack "$IMAGE_LYRICAL" lyrical rmw_fastrtps_cpp
    echo "up: lyrical-dds-bridge = s3-bridge ($NET, host tcp/127.0.0.1:7447, zid $BRIDGE_ZID), s3-sensor-pub-dds"
    ;;

  lyrical-rmw-jazzy-router:up)
    # Cross-distro: Jazzy rmw_zenohd router, Lyrical rmw_zenoh publishers.
    rmw_stack "$IMAGE_JAZZY" jazzy "$IMAGE_LYRICAL" lyrical
    echo "up: lyrical-rmw-jazzy-router = s3-router JAZZY ($NET, host tcp/127.0.0.1:7447), s3-sensor-pub LYRICAL"
    ;;

  jazzy-rmw-lyrical-router:up)
    # Cross-distro reverse: Lyrical rmw_zenohd router, Jazzy rmw_zenoh
    # publishers.
    rmw_stack "$IMAGE_LYRICAL" lyrical "$IMAGE_JAZZY" jazzy
    echo "up: jazzy-rmw-lyrical-router = s3-router LYRICAL ($NET, host tcp/127.0.0.1:7447), s3-sensor-pub JAZZY"
    ;;

  jazzy-cyclone-bridge:up)
    dds_stack "$IMAGE_CYCLONE" jazzy rmw_cyclonedds_cpp
    echo "up: jazzy-cyclone-bridge = s3-bridge ($NET, host tcp/127.0.0.1:7447, zid $BRIDGE_ZID), s3-sensor-pub-dds (rmw_cyclonedds_cpp)"
    ;;

  humble-fastdds-bridge:up)
    dds_stack "$IMAGE_HUMBLE" humble rmw_fastrtps_cpp
    echo "up: humble-fastdds-bridge = s3-bridge ($NET, host tcp/127.0.0.1:7447, zid $BRIDGE_ZID), s3-sensor-pub-dds (humble)"
    ;;

  *:down)
    docker rm -f s3-sensor-pub s3-router s3-sensor-pub-dds s3-bridge >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    echo "down"
    ;;

  *:logs)
    docker logs -f --tail=100 "${3:?name required (s3-router|s3-sensor-pub|s3-bridge|s3-sensor-pub-dds)}"
    ;;

  *:ps)
    docker ps -a --filter "name=s3-"
    ;;

  *)
    usage
    ;;
esac
