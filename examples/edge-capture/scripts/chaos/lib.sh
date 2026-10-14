# Shared helpers for the edge-capture chaos scenarios. Sourced, never executed.
#
# Topology each scenario uses:
#   - the broker and Toxiproxy run in Docker (make up / up-s3 equivalents);
#   - thadam runs natively against the Toxiproxy MQTT/S3 ports (21883 / 29000);
#   - thadam-simulator runs natively and publishes straight to the broker's
#     debug port (127.0.0.1:1883), so injected faults never touch production
#     and the oracle stays a clean record of what was sent;
#   - the containerized simulator service is stopped for the duration, so there
#     is exactly one producer (the MQTT client id and the oracle file are owned
#     by the scenario).
#
# Each run gets its own directory under examples/edge-capture/data/chaos/ with
# a copy of the demo config whose data_dir points there. Bash 3.2 compatible
# (macOS /bin/bash): no associative arrays, mapfile, or ${var,,}.

CHAOS_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLE_DIR="$(cd "$CHAOS_LIB_DIR/../.." && pwd)"
WORKSPACE_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"

THADAM_BIN="${THADAM_BIN:-$WORKSPACE_DIR/target/debug/thadam}"
SIM_BIN="${SIM_BIN:-$WORKSPACE_DIR/target/debug/thadam-simulator}"
ADMIN_ADDR="${ADMIN_ADDR:-127.0.0.1:9464}"
TOXI_API="${TOXI_API:-http://localhost:8474}"
SIM_BROKER="${SIM_BROKER:-mqtt://127.0.0.1:1883}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:29000}"
CHAOS_OUT="${CHAOS_OUT:-$EXAMPLE_DIR/data/chaos}"

THADAM_PID=""
SIM_PID=""
SEAWEED_STOPPED=0
RUN_DIR=""
RUN_CONFIG=""
ORACLE=""

log() {
  printf '%s  %s\n' "$(date '+%H:%M:%S')" "$*"
}

die() {
  printf '%s  FAIL: %s\n' "$(date '+%H:%M:%S')" "$*" >&2
  exit 1
}

compose() {
  docker compose -f "$EXAMPLE_DIR/compose.yaml" "$@"
}

require_tools() {
  local tool
  for tool in docker curl jq; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found on PATH"
  done
  docker info >/dev/null 2>&1 || die "docker daemon not reachable (start Docker Desktop)"
}

require_uint() {
  case "$2" in
    ''|*[!0-9]*) die "$1 must be a whole number of seconds, got '$2'" ;;
  esac
}

# CHAOS_SKIP_BUILD=1 reuses the binaries already in target/debug (for runs while
# another cargo job holds the target directory lock).
ensure_binaries() {
  if [ "${CHAOS_SKIP_BUILD:-0}" != "1" ]; then
    cargo build -q --manifest-path "$WORKSPACE_DIR/Cargo.toml" -p thadam-cli -p thadam-simulator
  fi
  [ -x "$THADAM_BIN" ] || die "thadam binary missing at $THADAM_BIN"
  [ -x "$SIM_BIN" ] || die "thadam-simulator binary missing at $SIM_BIN"
}

# wait_until LIMIT_SECONDS WHAT COMMAND...
wait_until() {
  local limit="$1" what="$2" waited=0
  shift 2
  until "$@" >/dev/null 2>&1; do
    waited=$((waited + 1))
    if [ "$waited" -ge "$limit" ]; then
      die "timed out after ${limit}s waiting for $what"
    fi
    sleep 1
  done
}

# ensure_stack [s3]: broker and Toxiproxy, plus SeaweedFS when the argument is s3.
# bucket_init_done: succeeds once the one-shot seaweedfs-init container has exited.
bucket_init_done() {
  local id state
  id="$(compose --profile s3 ps -a -q seaweedfs-init 2>/dev/null | tail -n 1)"
  [ -n "$id" ] || return 1
  state="$(docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null)" || return 1
  [ "$state" = "exited" ]
}

# bucket_init_done_ok: the init container exited with status 0.
bucket_init_done_ok() {
  local id code
  id="$(compose --profile s3 ps -a -q seaweedfs-init 2>/dev/null | tail -n 1)"
  code="$(docker inspect -f '{{.State.ExitCode}}' "$id" 2>/dev/null)" || return 1
  [ "$code" = "0" ]
}

ensure_stack() {
  if [ "${1:-}" = "s3" ]; then
    make -s -C "$EXAMPLE_DIR" secrets/admin_token secrets/s3_access_key secrets/s3_secret_key
    compose --profile s3 up -d mosquitto toxiproxy seaweedfs seaweedfs-init >/dev/null
    wait_until 60 "seaweedfs-init to finish" bucket_init_done
    bucket_init_done_ok || die "bucket init failed (see: make logs S=seaweedfs-init)"
    export THADAM_DEMO_S3_AK="$(cat "$EXAMPLE_DIR/secrets/s3_access_key")"
    export THADAM_DEMO_S3_SK="$(cat "$EXAMPLE_DIR/secrets/s3_secret_key")"
    wait_until 60 "SeaweedFS S3 gateway" curl -s -o /dev/null --max-time 2 "$S3_ENDPOINT/"
  else
    compose up -d mosquitto toxiproxy >/dev/null
  fi
  compose stop simulator >/dev/null 2>&1 || true
  wait_until 30 "Toxiproxy control API" curl -sf --max-time 2 "$TOXI_API/version"
  log "stack up (broker, toxiproxy${1:+, $1})"
}

# new_run NAME: fresh run directory, oracle path, and config path.
new_run() {
  RUN_DIR="$CHAOS_OUT/$1-$(date '+%Y%m%d-%H%M%S')"
  mkdir -p "$RUN_DIR"
  RUN_CONFIG="$RUN_DIR/pipeline.yaml"
  ORACLE="$RUN_DIR/oracle.ndjson"
  cd "$RUN_DIR"
  log "run dir: $RUN_DIR"
}

# use_config FILE: the demo config under thadam/, with data_dir moved into the run dir.
use_config() {
  sed "s|^  data_dir: ./data\$|  data_dir: $RUN_DIR/data|" "$EXAMPLE_DIR/thadam/$1" >"$RUN_CONFIG"
  grep -q "$RUN_DIR/data" "$RUN_CONFIG" || die "data_dir was not rewritten in $1"
  log "config: thadam/$1 (data_dir -> run dir)"
}

# start_thadam LOGNAME: sets THADAM_PID.
start_thadam() {
  "$THADAM_BIN" run "$RUN_CONFIG" --log-format json --log-level info \
    --admin-bind "$ADMIN_ADDR" >"$RUN_DIR/$1" 2>&1 &
  THADAM_PID=$!
  log "thadam started (pid $THADAM_PID, log $1)"
}

wait_readyz() {
  local limit="${1:-60}" waited=0
  until curl -sf --max-time 2 "http://$ADMIN_ADDR/readyz" >/dev/null 2>&1; do
    waited=$((waited + 1))
    [ "$waited" -lt "$limit" ] || die "thadam not ready after ${limit}s; see $RUN_DIR"
    sleep 1
  done
  log "readyz ok"
}

# stop_thadam: SIGTERM, then thadam drains, finalizes Parquet, and must exit 0.
stop_thadam() {
  local rc=0
  kill -TERM "$THADAM_PID" 2>/dev/null || true
  wait "$THADAM_PID" || rc=$?
  THADAM_PID=""
  [ "$rc" -eq 0 ] || die "thadam exited with $rc on SIGTERM; see $RUN_DIR"
  log "thadam stopped cleanly"
}

# kill_thadam: SIGKILL, the crash the WAL must survive.
kill_thadam() {
  kill -9 "$THADAM_PID" 2>/dev/null || true
  wait "$THADAM_PID" 2>/dev/null || true
  THADAM_PID=""
  log "thadam killed (SIGKILL)"
}

# start_sim DURATION [extra simulator run flags...]: sets SIM_PID.
start_sim() {
  local duration="$1"
  shift
  "$SIM_BIN" run --broker "$SIM_BROKER" --fleet "$EXAMPLE_DIR/simulator/fleet.yaml" \
    --oracle "$ORACLE" --duration "$duration" --drain-timeout 8s \
    --client-id thadam-simulator-chaos --log-format json --log-level warn "$@" \
    >"$RUN_DIR/simulator.log" 2>&1 &
  SIM_PID=$!
  log "simulator started (pid $SIM_PID, duration $duration${*:+, $*})"
}

wait_sim() {
  local rc=0
  wait "$SIM_PID" || rc=$?
  SIM_PID=""
  [ "$rc" -eq 0 ] || die "simulator exited with $rc; see $RUN_DIR/simulator.log"
  log "simulator finished (broker acks drained)"
}

# s3_object_count: objects in the demo bucket (unsigned listing, demo store only).
s3_object_count() {
  local listing
  listing="$(curl -sf --max-time 5 "$S3_ENDPOINT/thadam-demo/?list-type=2" 2>/dev/null)" \
    || { echo "unavailable"; return 0; }
  printf '%s' "$listing" | { grep -o '<Key>' || true; } | wc -l | tr -d ' '
}

toxi_set() {
  local name="$1" enabled="$2"
  curl -sf -X POST -H 'Content-Type: application/json' \
    -d "{\"enabled\": $enabled}" "$TOXI_API/proxies/$name" >/dev/null \
    || die "toxiproxy: could not set $name enabled=$enabled"
  local now
  now="$(curl -sf "$TOXI_API/proxies/$name" | jq -r '.enabled')"
  [ "$now" = "$enabled" ] || die "toxiproxy: $name reports enabled=$now"
  log "toxiproxy: $name enabled=$enabled"
}

status_summary() {
  curl -sf --max-time 3 "http://$ADMIN_ADDR/status" 2>/dev/null \
    | jq -r '"wal=\(.wal.disk_bytes // 0)B segments=\(.wal.segments // 0) lag=[\([.cursors[].lag_frames] | join(","))] uncommitted=\([.components[].counters.uncommitted_batches] | add // 0)"' \
    2>/dev/null \
    || echo "status unavailable"
}

# observe SECONDS: print a status line every 5 s while a timed phase runs.
observe() {
  local left="$1" step
  while [ "$left" -gt 0 ]; do
    step=5
    [ "$left" -lt 5 ] && step="$left"
    sleep "$step"
    left=$((left - step))
    log "  $(status_summary)"
  done
}

# wait_drained LIMIT_SECONDS: the source side is idle. Every batch received has
# been committed to the WAL (uncommitted 0) and the WAL has stopped growing for
# 5 s. Sink cursors are NOT awaited: a segment sink commits its watermark only at
# rotation (timer-driven), while a reject releases the sink cursor once fsynced
# to the quarantine staging file (quarantine segments rotate on their own
# `quarantine.rotation.max_age`, demo uses 15 s). The SIGTERM finalize commits
# every sink, and verify on the finalized output is the authoritative delivery check.
wait_drained() {
  local limit="${1:-120}" waited=0 stable=0 last="" now=""
  while :; do
    now="$(curl -sf --max-time 3 "http://$ADMIN_ADDR/status" 2>/dev/null \
      | jq -r '"\(.wal.durable_end)/\([.components[].counters.uncommitted_batches] | add // 0)"' 2>/dev/null || true)"
    if [ -n "$now" ] && [ "$now" = "$last" ] && [ "${now##*/}" = "0" ]; then
      stable=$((stable + 1))
    else
      stable=0
    fi
    last="$now"
    if [ "$stable" -ge 5 ]; then
      break
    fi
    waited=$((waited + 1))
    if [ "$waited" -ge "$limit" ]; then
      die "source did not go idle within ${limit}s: $(status_summary)"
    fi
    sleep 1
  done
  log "source idle: $(status_summary)"
}

# run_verify: thadam-simulator verify against this run's oracle and sink output.
run_verify() {
  local quarantine="$RUN_DIR/data/quarantine/local/ready"
  local args=(--oracle "$ORACLE" --ready "$RUN_DIR/data/sinks/local")
  if [ -d "$quarantine" ]; then
    args+=(--quarantine "$quarantine")
  fi
  "$SIM_BIN" verify "${args[@]}"
}

# finish_with_verify: print the verify report and exit with its status.
finish_with_verify() {
  local rc=0
  run_verify || rc=$?
  log "verify exit status: $rc"
  exit "$rc"
}

toxi_restore() {
  local name
  for name in mqtt s3; do
    curl -sf -X POST -H 'Content-Type: application/json' -d '{"enabled": true}' \
      "$TOXI_API/proxies/$name" >/dev/null 2>&1 || true
  done
}

chaos_cleanup() {
  if [ -n "$SIM_PID" ]; then
    kill "$SIM_PID" 2>/dev/null || true
  fi
  if [ -n "$THADAM_PID" ]; then
    kill -TERM "$THADAM_PID" 2>/dev/null || true
    wait "$THADAM_PID" 2>/dev/null || true
  fi
  toxi_restore
  if [ "$SEAWEED_STOPPED" = "1" ]; then
    compose --profile s3 start seaweedfs >/dev/null 2>&1 || true
  fi
}

trap chaos_cleanup EXIT
trap 'exit 130' INT TERM
