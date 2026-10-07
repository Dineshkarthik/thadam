#!/usr/bin/env bash
# Extra experiments that are not part of the per-broker matrix in run_all.sh:
#   E2  oversized message vs rumqttc's default 10 KiB incoming packet limit (both brokers)
#   E3  EMQX inflight=1000: default force_shutdown vs raised max_mailbox_size
#   E4  EMQX QoS 2 with a count below its default max_awaiting_rel
set -uo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HERE/target}"
cargo build --release >/dev/null 2>&1 || { echo "build failed" >&2; exit 1; }
BIN="$CARGO_TARGET_DIR/release/s2-mqtt"
B="$HERE/scripts/broker.sh"
mkdir -p results/mosquitto results/emqx

echo "[e2] oversize, mosquitto" >&2
"$B" start mosquitto 100 100000 false quiet >/dev/null
"$BIN" --port 18831 oversize --broker mosquitto --broker-inflight 100 > results/mosquitto/e2.jsonl

echo "[e2] oversize, emqx" >&2
"$B" start emqx 100 10000 >/dev/null
"$BIN" --port 18832 oversize --broker emqx --broker-inflight 100 > results/emqx/e2.jsonl

echo "[e4] emqx qos2 (count 150)" >&2
"$B" start emqx 1000 10000 >/dev/null
"$BIN" --port 18832 strip --mode stripped --qos 2 --payload-kib 4 --count 150 --broker emqx --broker-inflight 1000 > results/emqx/e4.jsonl

echo "[e3] emqx inflight=1000 default force_shutdown" >&2
: > results/emqx/e3.jsonl
"$B" start emqx 1000 10000 >/dev/null
"$BIN" --port 18832 throughput --broker emqx --broker-inflight 1000 --latency-ms 10 --backlog-cap 3000 >> results/emqx/e3.jsonl
docker logs s2-emqx 2>&1 | grep -aE "check_oom_shutdown|maximum heap" | cut -c1-400 | head -3 > results/emqx/e3-broker.txt

echo "[e3] emqx inflight=1000 with force_shutdown.max_mailbox_size=10000" >&2
EMQX_EXTRA_ARGS='-e EMQX_FORCE_SHUTDOWN__MAX_MAILBOX_SIZE=10000' "$B" start emqx 1000 10000 >/dev/null
for L in 10 0; do
  "$BIN" --port 18832 throughput --broker emqx --broker-inflight 1000 --latency-ms "$L" --backlog-cap 3000 >> results/emqx/e3.jsonl
done
docker logs s2-emqx 2>&1 | grep -aE "check_oom_shutdown|maximum heap" | cut -c1-400 | head -3 >> results/emqx/e3-broker.txt

"$B" stop >/dev/null 2>&1 || true
echo "done" >&2
