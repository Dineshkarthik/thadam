#!/usr/bin/env bash
# Spike S3 capture orchestration.
#
#   scripts/run-capture.sh <out-prefix> [-- <extra capture args>]
#
# Produces, under <out-prefix>.*:
#   .json     capture summary (stdout of the capture subcommand)
#   .log      capture stderr log (progress lines; RUST_LOG defaults to warn)
#   .offset   clock-offset diagnostics (container - host, ns)
#   .ps       subscriber %cpu/RSS samples taken every 2 s during the run
#   .publog   publisher cumulative counts before/after the window
#
# The <extra capture args> are passed to `s3-ros2-live capture`; e.g.
#   scripts/run-capture.sh results/baseline -- --duration-secs 60
#   scripts/run-capture.sh results/overload -- --duration-secs 60 --slow-ms 2
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SPIKE="$(dirname "$HERE")"
BIN="${CARGO_TARGET_DIR:-$SPIKE/target/s3-spike}/release/s3-ros2-live"
OUT="$1"
shift
[ "${1:-}" = "--" ] && shift

[ -x "$BIN" ] || { echo "missing binary: $BIN (cargo build --release first)" >&2; exit 2; }
mkdir -p "$(dirname "$OUT")"

echo "== clock offset (container - host, ns) ==" | tee "$OUT.offset"
OFFSET="$(python3 "$HERE/clock_offset.py" s3-router 2>>"$OUT.offset")"
echo "offset_ns=$OFFSET" | tee -a "$OUT.offset"

{
  echo "=== before ==="
  docker logs --tail 2 s3-sensor-pub 2>&1
} > "$OUT.publog"

echo "== capture: $BIN capture --clock-offset-ns=$OFFSET $* =="
RUST_LOG="${RUST_LOG:-warn}" "$BIN" capture "--clock-offset-ns=$OFFSET" "$@" \
  > "$OUT.json" 2> "$OUT.log" &
CAP_PID=$!

: > "$OUT.ps"
while kill -0 "$CAP_PID" 2>/dev/null; do
  # shellcheck disable=SC2009
  ps -o %cpu=,rss= -p "$CAP_PID" 2>/dev/null >> "$OUT.ps" || true
  sleep 2
done
wait "$CAP_PID"
RC=$?

{
  echo "=== after ==="
  docker logs --tail 2 s3-sensor-pub 2>&1
} >> "$OUT.publog"

echo "capture exit=$RC; summary: $OUT.json; samples: $(wc -l < "$OUT.ps" | tr -d ' ') ps rows"
exit "$RC"
