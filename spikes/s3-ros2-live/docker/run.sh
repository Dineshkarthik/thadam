#!/usr/bin/env bash
# Plain-docker equivalent of `docker compose -f compose.yaml up/down`, because
# the sandbox in this dev environment permits `docker run`/`docker build`
# but not `docker compose up`. Runs the identical images/args as compose.yaml.
#
#   run.sh up | down | logs <name> | ps
set -euo pipefail

IMAGE=s3-ros2-zenoh:jazzy-1
NET=s3ros2
cmd="${1:-}"

case "$cmd" in
  up)
    docker rm -f s3-router s3-sensor-pub >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    docker network create "$NET" >/dev/null
    docker run -d --name s3-router --network "$NET" \
      -p 127.0.0.1:7447:7447 \
      "$IMAGE" \
      bash -c 'source /opt/ros/jazzy/setup.bash && exec ros2 run rmw_zenoh_cpp rmw_zenohd' >/dev/null
    # wait for the router port
    for _ in $(seq 1 60); do
      if docker logs s3-router 2>&1 | grep -q "7447"; then break; fi
      sleep 0.5
    done
    docker run -d --name s3-sensor-pub --network "$NET" \
      -e RMW_IMPLEMENTATION=rmw_zenoh_cpp \
      -e ROS_DOMAIN_ID=0 \
      -e 'ZENOH_CONFIG_OVERRIDE=connect/endpoints=["tcp/s3-router:7447"];scouting/multicast/enabled=false' \
      "$IMAGE" \
      bash -c 'source /opt/ros/jazzy/setup.bash && exec python3 /usr/local/lib/s3/sensor_pub.py' >/dev/null
    echo "up: s3-router ($NET, host tcp/127.0.0.1:7447), s3-sensor-pub"
    ;;
  down)
    docker rm -f s3-sensor-pub s3-router >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    echo "down"
    ;;
  logs)
    docker logs -f --tail=100 "${2:-s3-sensor-pub}"
    ;;
  ps)
    docker ps -a --filter "name=s3-"
    ;;
  *)
    echo "usage: $0 up|down|logs <name>|ps" >&2
    exit 2
    ;;
esac
