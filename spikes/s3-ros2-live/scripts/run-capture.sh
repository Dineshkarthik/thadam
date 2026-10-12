#!/usr/bin/env bash
# Spike S3 capture orchestration.
#
#   scripts/run-capture.sh <out-prefix> [opts] [-- <extra capture args>]
#
# Options (before --):
#   --offset-container <name>  container whose clock is the publisher clock
#                              (default s3-router; use s3-sensor-pub-dds for
#                              bridge runs — all containers share the Docker
#                              VM kernel clock, so any works)
#   --pub-container <name>     publisher container for ground-truth logs
#                              (default s3-sensor-pub)
#   --docker-stats <name>      also sample `docker stats` for this container
#                              every 2 s into <out>.dockerstats (e.g. s3-bridge)
#
# Produces, under <out-prefix>.*:
#   .json     capture summary (stdout of the capture subcommand)
#   .log      capture stderr log (progress lines; RUST_LOG defaults to warn)
#   .offset   clock-offset diagnostics (container - host, ns)
#   .ps       subscriber %cpu/RSS samples taken every 2 s during the run
#   .publog   publisher cumulative counts before/after the window
#   .dockerstats  (only with --docker-stats) CPU%/mem of the named container
#
# The <extra capture args> are passed to `s3-ros2-live capture`; e.g.
#   scripts/run-capture.sh results/baseline -- --duration-secs 60
#   scripts/run-capture.sh results/bridge-baseline \
#     --offset-container s3-sensor-pub-dds --pub-container s3-sensor-pub-dds \
#     --docker-stats s3-bridge -- \
#     --transport bridge --bridge-zid 57a9ed --duration-secs 60
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SPIKE="$(dirname "$HERE")"
BIN="${CARGO_TARGET_DIR:-$SPIKE/target/s3-spike}/release/s3-ros2-live"
OUT="$1"
shift

OFFSET_CONTAINER=s3-router
PUB_CONTAINER=s3-sensor-pub
STATS_CONTAINER=""
while [ $# -gt 0 ]; do
  case "$1" in
    --offset-container) OFFSET_CONTAINER="$2"; shift 2;;
    --pub-container) PUB_CONTAINER="$2"; shift 2;;
    --docker-stats) STATS_CONTAINER="$2"; shift 2;;
    --) shift; break;;
    *) break;;
  esac
done

[ -x "$BIN" ] || { echo "missing binary: $BIN (cargo build --release first)" >&2; exit 2; }
mkdir -p "$(dirname "$OUT")"

echo "== clock offset (container - host, ns; container: $OFFSET_CONTAINER) ==" | tee "$OUT.offset"
OFFSET="$(python3 "$HERE/clock_offset.py" "$OFFSET_CONTAINER" 2>>"$OUT.offset")"
echo "offset_ns=$OFFSET" | tee -a "$OUT.offset"

{
  echo "=== before ==="
  docker logs --tail 2 "$PUB_CONTAINER" 2>&1
} > "$OUT.publog"

echo "== capture: $BIN capture --clock-offset-ns=$OFFSET $* =="
RUST_LOG="${RUST_LOG:-warn}" "$BIN" capture "--clock-offset-ns=$OFFSET" "$@" \
  > "$OUT.json" 2> "$OUT.log" &
CAP_PID=$!

: > "$OUT.ps"
: > "$OUT.dockerstats"
while kill -0 "$CAP_PID" 2>/dev/null; do
  # shellcheck disable=SC2009
  ps -o %cpu=,rss= -p "$CAP_PID" 2>/dev/null >> "$OUT.ps" || true
  if [ -n "$STATS_CONTAINER" ]; then
    docker stats --no-stream --format '{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}' \
      "$STATS_CONTAINER" 2>/dev/null >> "$OUT.dockerstats" || true
  fi
  sleep 2
done
wait "$CAP_PID"
RC=$?

{
  echo "=== after ==="
  docker logs --tail 2 "$PUB_CONTAINER" 2>&1
} >> "$OUT.publog"

echo "capture exit=$RC; summary: $OUT.json; samples: $(wc -l < "$OUT.ps" | tr -d ' ') ps rows"
exit "$RC"
