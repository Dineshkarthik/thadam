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
#
# Both setups share one bridge network (`s3ros2`): the bridge container and
# the DDS publishers must co-locate because DDS discovery is multicast.
# The host capture binary connects to tcp/127.0.0.1:7447 in both setups.
set -euo pipefail

IMAGE=s3-ros2-zenoh:jazzy-1
BRIDGE_IMAGE=eclipse/zenoh-bridge-ros2dds:1.10.1
BRIDGE_ZID=57a9ed
NET=s3ros2
setup="${1:-}"
cmd="${2:-up}"

usage() {
  echo "usage: $0 jazzy-zenoh|jazzy-fastdds-bridge up|down|logs <name>|ps" >&2
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

case "$setup:$cmd" in
  jazzy-zenoh:up)
    docker rm -f s3-router s3-sensor-pub >/dev/null 2>&1 || true
    net_up
    docker run -d --name s3-router --network "$NET" \
      -p 127.0.0.1:7447:7447 \
      "$IMAGE" \
      bash -c 'source /opt/ros/jazzy/setup.bash && exec ros2 run rmw_zenoh_cpp rmw_zenohd' >/dev/null
    wait_log s3-router "7447"
    docker run -d --name s3-sensor-pub --network "$NET" \
      -e RMW_IMPLEMENTATION=rmw_zenoh_cpp \
      -e ROS_DOMAIN_ID=0 \
      -e 'ZENOH_CONFIG_OVERRIDE=connect/endpoints=["tcp/s3-router:7447"];scouting/multicast/enabled=false' \
      "$IMAGE" \
      bash -c 'source /opt/ros/jazzy/setup.bash && exec python3 /usr/local/lib/s3/sensor_pub.py' >/dev/null
    wait_log s3-sensor-pub "s3_sensor_pub started"
    echo "up: jazzy-zenoh = s3-router ($NET, host tcp/127.0.0.1:7447), s3-sensor-pub"
    ;;

  jazzy-fastdds-bridge:up)
    docker rm -f s3-bridge s3-sensor-pub-dds >/dev/null 2>&1 || true
    net_up
    # Bridge: router mode, pinned zenoh id (admin space @/$BRIDGE_ZID/ros2),
    # zenoh-side multicast scouting OFF (nothing may leak onto the LAN);
    # DDS-side multicast is REQUIRED for Fast DDS discovery inside $NET.
    # ROS_DISTRO silences the iron-assumption warning (bridge parses
    # ros_discovery_info).
    docker run -d --name s3-bridge --network "$NET" \
      -p 127.0.0.1:7447:7447 \
      -e ROS_DOMAIN_ID=0 \
      -e ROS_DISTRO=jazzy \
      -e RUST_LOG=info \
      "$BRIDGE_IMAGE" \
      -i "$BRIDGE_ZID" -d 0 -l tcp/0.0.0.0:7447 --no-multicast-scouting >/dev/null
    wait_log s3-bridge "Zenoh can be reached at"
    # Publishers: Jazzy default RMW (Fast DDS), same sensor_pub.py matrix.
    docker run -d --name s3-sensor-pub-dds --network "$NET" \
      -e RMW_IMPLEMENTATION=rmw_fastrtps_cpp \
      -e ROS_DOMAIN_ID=0 \
      "$IMAGE" \
      bash -c 'source /opt/ros/jazzy/setup.bash && exec python3 /usr/local/lib/s3/sensor_pub.py' >/dev/null
    wait_log s3-sensor-pub-dds "s3_sensor_pub started"
    wait_log s3-bridge "Route Publisher (ROS:/imu" 30
    echo "up: jazzy-fastdds-bridge = s3-bridge ($NET, host tcp/127.0.0.1:7447, zid $BRIDGE_ZID), s3-sensor-pub-dds"
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
