#!/usr/bin/env bash
# Run the S2 experiment matrix against one broker.
#
#   scripts/run_all.sh mosquitto|emqx [sections]
#
# sections: comma list of q1,q2,q3,q4,q5,q6,e1,v5 (default: all). Raw results are appended to
# results/<broker>/<section>.jsonl; broker log excerpts go to results/<broker>/*.txt.
set -uo pipefail

BROKER="${1:?usage: run_all.sh mosquitto|emqx [sections]}"
SECTIONS="${2:-q1,q2,q3,q4,q5,q6,e1,v5}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HERE/target}"
cargo build --release >/dev/null 2>&1 || { echo "build failed" >&2; exit 1; }
BIN="$CARGO_TARGET_DIR/release/s2-mqtt"
B="$HERE/scripts/broker.sh"
OUT="$HERE/results/$BROKER"
mkdir -p "$OUT" "$HERE/target/q5"

case "$BROKER" in
  mosquitto) PORT=18831 ;;
  emqx) PORT=18832 ;;
  *) echo "unknown broker" >&2; exit 2 ;;
esac
S2() { "$BIN" --port "$PORT" "$@"; }
# EMQX keeps a session's queue in the process heap and force-kills the session at the
# default 48 MiB heap limit, so deep queues/payloads are scaled down for it.
QDEF=100000; TP_EXTRA=""; Q2RATE=1000; Q4KIB=64; Q4COUNT=3000
if [ "$BROKER" = emqx ]; then QDEF=10000; TP_EXTRA="--backlog-cap 3000"; Q2RATE=400; Q4KIB=32; Q4COUNT=1500; fi
want() { [[ ",$SECTIONS," == *",$1,"* ]]; }
start() { # inflight queued persist log
  "$B" start "$BROKER" "$1" "${2:-$QDEF}" "${3:-false}" "${4:-quiet}" >/dev/null || exit 1
}
# Keep only broker log lines relevant to the spike (full debug logs are huge).
dump_logs() {
  "$B" logs "$BROKER" 2>&1 | grep -aiE "exceeded|timeout|being dropped|dropped|closed its|disconnecting|already connected|keepalive|takeover|discard" | head -400 > "$OUT/$1" || true
}
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT

if want q1; then
  : > "$OUT/q1.jsonl"
  for INFL in 20 100 1000; do
    start "$INFL"
    for L in 0 10 50 100; do
      echo "[q1] $BROKER inflight=$INFL latency=${L}ms" >&2
      S2 throughput --broker "$BROKER" --broker-inflight "$INFL" --latency-ms "$L" $TP_EXTRA >> "$OUT/q1.jsonl"
    done
    echo "[q1] $BROKER inflight=$INFL auto-ack baseline" >&2
    S2 throughput --broker "$BROKER" --broker-inflight "$INFL" --latency-ms 0 --auto-ack $TP_EXTRA >> "$OUT/q1.jsonl"
  done
  S2 rtt --n 2000 >> "$OUT/q1.jsonl"
fi

if want q2; then
  : > "$OUT/q2.jsonl"
  for INFL in 20 100 1000; do
    start "$INFL" "$QDEF" false debug
    echo "[q2a] withhold acks 3xKA, inflight=$INFL" >&2
    S2 keepalive --mode withhold --pub-rate "$Q2RATE" --broker "$BROKER" --broker-inflight "$INFL" >> "$OUT/q2.jsonl"
  done
  start 100 "$QDEF" false debug
  echo "[q2b] stop polling 3xKA (anti-pattern), inflight=100" >&2
  S2 keepalive --mode nopoll --broker "$BROKER" --broker-inflight 100 --pub-rate 100 >> "$OUT/q2.jsonl"
  dump_logs q2b-broker.txt
  start 100 "$QDEF" false debug
  echo "[q2c] bounded queue (cap 20) smaller than broker window (100)" >&2
  S2 keepalive --mode withhold --pub-rate "$Q2RATE" --queue-cap 20 --broker "$BROKER" --broker-inflight 100 >> "$OUT/q2.jsonl"
  dump_logs q2c-broker.txt
  start 100 "$QDEF" false debug
  echo "[q2d] bounded queue (cap 100) equal to broker window (100)" >&2
  S2 keepalive --mode withhold --pub-rate "$Q2RATE" --queue-cap 100 --broker "$BROKER" --broker-inflight 100 >> "$OUT/q2.jsonl"
fi

if want q3; then
  : > "$OUT/q3.jsonl"
  start 1000
  echo "[q3] cancel-safety" >&2
  C() { S2 cancel --broker "$BROKER" --broker-inflight 1000 "$@" >> "$OUT/q3.jsonl"; }
  C --style none --count 100000 --pub-rate 5000
  C --style now_or_never --count 100000 --pub-rate 5000
  C --style now_or_never --count 200000
  C --style timeout --max-us 8000 --count 4000 --pub-rate 200
  C --style select --max-us 8000 --count 4000 --pub-rate 200
  C --style mixed --max-us 3000 --count 60000 --pub-rate 2000
  C --style mixed --max-us 3000 --count 150000
  C --style mixed --max-us 3000 --count 0 --idle-secs 30 --keepalive 2
  C --style mixed --max-us 3000 --count 20000 --pub-rate 2000 --from-start
  C --style timeout --max-us 1500 --count 2000 --pub-rate 1000 --from-start
  echo "[q3-stall] cancel while the socket write side is blocked" >&2
  : > "$OUT/q3-stall.jsonl"
  for ST in none timeout now_or_never; do
    S2 cancel-stall --style "$ST" --broker "$BROKER" --broker-inflight 1000 >> "$OUT/q3-stall.jsonl"
  done
fi

if want q4; then
  : > "$OUT/q4.jsonl"
  start 1000
  echo "[q4] stripped vs retained acks" >&2
  for M in stripped retained; do
    S2 strip --mode "$M" --qos 1 --payload-kib "$Q4KIB" --count "$Q4COUNT" --broker "$BROKER" --broker-inflight 1000 >> "$OUT/q4.jsonl"
  done
  S2 strip --mode stripped --qos 2 --payload-kib 4 --count 3000 --broker "$BROKER" --broker-inflight 1000 >> "$OUT/q4.jsonl"
fi

if want q6; then
  : > "$OUT/q6.jsonl"
  start 100 "$QDEF" false debug
  echo "[q6] ack ordering" >&2
  S2 order --mode in_order --count 3000 --broker "$BROKER" --broker-inflight 100 >> "$OUT/q6.jsonl"
  S2 order --mode shuffled --count 1000 --broker "$BROKER" --broker-inflight 100 >> "$OUT/q6.jsonl"
  dump_logs q6-broker.txt
fi

# ---- Q5: persistent session ------------------------------------------------------------
wait_log() { # file pattern timeout_s
  local f="$1" pat="$2" t="${3:-20}"
  for _ in $(seq 1 $((t * 10))); do grep -q "$pat" "$f" 2>/dev/null && return 0; sleep 0.1; done
  echo "timeout waiting for '$pat' in $f" >&2; return 1
}
count_log() { grep -c "$2" "$1" 2>/dev/null || echo 0; }
rep() { # label from to logs...
  local label="$1" from="$2" to="$3"; shift 3
  S2 sess-report --label "$label" --expect-from "$from" --expect-to "$to" --logs "$@" >> "$OUT/q5.jsonl"
}

if want q5; then
  : > "$OUT/q5.jsonl"
  Q="$HERE/target/q5"; rm -f "$Q"/*.log
  PERSIST=true; [ "$BROKER" = emqx ] && PERSIST=false
  SUBPID=""
  sub_bg() { "$BIN" --port "$PORT" sess-sub "$@" & SUBPID=$!; }
  kill9() { kill -9 "$SUBPID" 2>/dev/null; wait "$SUBPID" 2>/dev/null; if kill -0 "$SUBPID" 2>/dev/null; then echo "FATAL: subscriber survived kill -9" >&2; exit 1; fi; }

  echo "[q5a] kill -9 with unacked QoS1 (inflight=20)" >&2
  start 20 "$QDEF" "$PERSIST" debug
  ID="s2-q5a-$$"; T="s2/q5a/$$"
  sub_bg --id "$ID" --topic "$T" --log "$Q/q5a-1.log" --ack-first 5
  wait_log "$Q/q5a-1.log" suback 20
  S2 sess-pub --topic "$T" --count 50 >> "$OUT/q5.jsonl"
  sleep 3
  echo "  deliveries before kill: $(count_log "$Q/q5a-1.log" '"ev":"pub"')" >&2
  kill9
  sub_bg --id "$ID" --topic "$T" --log "$Q/q5a-2.log" --ack-all --run-secs 8
  wait "$SUBPID"
  rep "q5a kill-9 redelivery" 0 50 "$Q/q5a-1.log" "$Q/q5a-2.log"

  echo "[q5b] session_present across broker restarts" >&2
  ID="s2-q5b-$$"; T="s2/q5b/$$"
  sub_bg --id "$ID" --topic "$T" --log "$Q/q5b-1.log" --run-secs 4; wait "$SUBPID"
  if [ "$BROKER" = mosquitto ]; then docker restart s2-mosquitto >/dev/null; sleep 3; else docker restart s2-emqx >/dev/null; sleep 25; fi
  sub_bg --id "$ID" --topic "$T" --log "$Q/q5b-2.log" --run-secs 4; wait "$SUBPID"
  if [ "$BROKER" = mosquitto ]; then "$B" restart-mosquitto-fresh >/dev/null; else start 20 "$QDEF" false debug; fi
  sub_bg --id "$ID" --topic "$T" --log "$Q/q5b-3.log" --run-secs 4; wait "$SUBPID"
  rep "q5b session_present: first connect | after broker restart (state kept if persisted) | after state wipe" 0 0 "$Q/q5b-1.log" "$Q/q5b-2.log" "$Q/q5b-3.log"

  for CASE in offline online; do
    echo "[q5c] queue limit exceeded ($CASE), inflight=20 queue=100" >&2
    start 20 100 "$PERSIST" debug
    ID="s2-q5c-$CASE-$$"; T="s2/q5c/$CASE/$$"
    sub_bg --id "$ID" --topic "$T" --log "$Q/q5c-$CASE-1.log" --ack-first 0 --run-secs 3600
    wait_log "$Q/q5c-$CASE-1.log" suback 20
    if [ "$CASE" = offline ]; then kill9; sleep 1; fi
    S2 sess-pub --topic "$T" --count 1000 >> "$OUT/q5.jsonl"
    sleep 2
    if [ "$CASE" = online ]; then kill9; fi
    sub_bg --id "$ID" --topic "$T" --log "$Q/q5c-$CASE-2.log" --ack-all --run-secs 10
    wait "$SUBPID"
    rep "q5c queue-limit $CASE (published 1000; deliveries before kill + after restart)" 0 1000 "$Q/q5c-$CASE-1.log" "$Q/q5c-$CASE-2.log"
    dump_logs "q5c-$CASE-broker.txt"
  done
  mkdir -p "$OUT/q5-logs"; cp "$Q"/*.log "$OUT/q5-logs/"
fi

if want v5 && [ "$BROKER" = mosquitto ]; then
  : > "$OUT/v5.jsonl"
  echo "[v5] MQTT 5 module smoke test" >&2
  start 1000
  S2 v5 --broker "$BROKER" --broker-inflight 1000 --receive-max 10 >> "$OUT/v5.jsonl"
fi

if want e1 && [ "$BROKER" = mosquitto ]; then
  : > "$OUT/e1.jsonl"
  for SA in false true; do
    echo "[e1] stale acks after session loss (stale_ack=$SA)" >&2
    start 20 "$QDEF" false debug
    S2 stale-ack --broker "$BROKER" --broker-script "$B" --count 10 $([ "$SA" = true ] && echo --stale-ack) >> "$OUT/e1.jsonl"
  done
fi

"$B" stop >/dev/null 2>&1 || true
echo "done: results in $OUT" >&2
