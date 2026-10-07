#!/usr/bin/env bash
# Start/stop the brokers used by spike S2. Containers are named s2-mosquitto / s2-emqx
# and are always removed on `stop`.
#
#   broker.sh start mosquitto <max_inflight> [max_queued=100000] [persist=false] [log=quiet|debug]
#   broker.sh start emqx      <max_inflight> [max_queued=100000]   (EMQX_EXTRA_ARGS='-e K=V ...' adds env)
#   broker.sh restart-mosquitto-fresh   # restart WITHOUT persistence volume (session loss)
#   broker.sh logs mosquitto|emqx
#   broker.sh stop
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
MOSQ_IMAGE="${MOSQ_IMAGE:-eclipse-mosquitto:2}"
EMQX_IMAGE="${EMQX_IMAGE:-emqx/emqx:5.8.6}"
MOSQ_PORT="${MOSQ_PORT:-18831}"
EMQX_PORT="${EMQX_PORT:-18832}"
CONF_DIR="$HERE/target/broker-conf"

cmd="${1:-}"; shift || true

stop_all() {
  docker rm -f s2-mosquitto s2-emqx >/dev/null 2>&1 || true
  docker volume rm -f s2-mosquitto-data >/dev/null 2>&1 || true
}

wait_port() {
  local port="$1" tries=120
  while ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
    tries=$((tries - 1)); [ "$tries" -gt 0 ] || { echo "broker on $port did not come up" >&2; exit 1; }
    sleep 0.5
  done
  sleep 1
}

case "$cmd" in
  start)
    kind="$1"; inflight="$2"; queued="${3:-100000}"; persist="${4:-false}"; log="${5:-quiet}"
    stop_all
    if [ "$kind" = mosquitto ]; then
      mkdir -p "$CONF_DIR"
      {
        echo "listener 1883"
        echo "allow_anonymous true"
        echo "max_inflight_messages $inflight"
        echo "max_queued_messages $queued"
        echo "persistence $persist"
        if [ "$persist" = true ]; then
          echo "persistence_location /mosquitto/data/"
          echo "autosave_interval 1"
        fi
        if [ "$log" = debug ]; then
          echo "log_type all"
          echo "connection_messages true"
          echo "log_timestamp true"
        else
          echo "log_type error"
          echo "log_type warning"
          echo "log_type notice"
        fi
      } > "$CONF_DIR/mosquitto.conf"
      docker volume create s2-mosquitto-data >/dev/null
      docker run -d --name s2-mosquitto -p "$MOSQ_PORT:1883" \
        -v "$CONF_DIR/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" \
        -v s2-mosquitto-data:/mosquitto/data "$MOSQ_IMAGE" >/dev/null
      wait_port "$MOSQ_PORT"
      echo "mosquitto up on $MOSQ_PORT (max_inflight=$inflight max_queued=$queued persistence=$persist log=$log)"
    elif [ "$kind" = emqx ]; then
      docker run -d --name s2-emqx -p "$EMQX_PORT:1883" \
        -e EMQX_MQTT__MAX_INFLIGHT="$inflight" \
        -e EMQX_MQTT__MAX_MQUEUE_LEN="$queued" \
        -e EMQX_LOG__CONSOLE__LEVEL=warning \
        ${EMQX_EXTRA_ARGS:-} \
        "$EMQX_IMAGE" >/dev/null
      wait_port "$EMQX_PORT"
      # the TCP listener opens before the node is fully ready
      for _ in $(seq 1 60); do
        docker exec s2-emqx emqx ctl status 2>/dev/null | grep -q "is started" && break
        sleep 1
      done
      echo "emqx up on $EMQX_PORT (max_inflight=$inflight max_mqueue_len=$queued)"
    else
      echo "unknown broker $kind" >&2; exit 2
    fi
    ;;
  restart-mosquitto-fresh)
    docker rm -f s2-mosquitto >/dev/null
    docker volume rm -f s2-mosquitto-data >/dev/null
    docker volume create s2-mosquitto-data >/dev/null
    docker run -d --name s2-mosquitto -p "$MOSQ_PORT:1883" \
      -v "$CONF_DIR/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro" \
      -v s2-mosquitto-data:/mosquitto/data "$MOSQ_IMAGE" >/dev/null
    wait_port "$MOSQ_PORT"
    ;;
  logs)
    docker logs "s2-$1" 2>&1
    ;;
  stop)
    stop_all
    ;;
  *)
    sed -n '2,12p' "$0"; exit 2
    ;;
esac
