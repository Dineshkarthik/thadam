#!/usr/bin/env bash
# Runs the S1 measurement matrix and appends one JSON object per run to results/<label>.jsonl.
# Usage: ./run-matrix.sh <label> <data_dir> [append interval_capacity rate read churn recovery crash]
set -euo pipefail
LABEL=$1
DATA=$2
shift 2
SECTIONS=${*:-append interval_capacity rate read churn recovery crash}
B=${B:-./target/release/s1-wal}
SECS=${SECS:-10}
OUT=results/$LABEL.jsonl
mkdir -p results "$DATA"
OS=$(uname -s)

run() {
  echo "+ $*" >&2
  "$B" "$@" --dir "$DATA/run" | tee -a "$OUT"
  rm -rf "$DATA/run"
}

SMALL="--rec-size 200 --batch 64"
LARGE="--rec-size 4096 --batch 16"
if [ "$OS" = Darwin ]; then
  SEG="always:data always:full always:barrier interval:50ms:data interval:50ms:full never:data"
  SQL="always:data always:full interval:50ms:data interval:50ms:full never:data"
  PRE=false
  DROP=""
else
  SEG="always:data always:full interval:50ms:data never:data"
  SQL="always:data interval:50ms:data never:data"
  PRE=true
  DROP="--drop-caches"
fi
# On macOS F_FULLFSYNC is the only flush that reaches stable media; sqlite needs fullfsync=ON for it,
# redb always uses it. On Linux "data" = fdatasync and is the honest choice.
FULL=$([ "$OS" = Darwin ] && echo full || echo data)

for s in $SECTIONS; do
case $s in
append)
  for w in "$SMALL" "$LARGE"; do
    for m in $SEG; do
      run bench --engine seglog --fsync "${m%:*}" --sync-kind "${m##*:}" --prealloc $PRE $w --secs "$SECS"
    done
    alt=$([ $PRE = true ] && echo false || echo true)
    run bench --engine seglog --fsync always --sync-kind data --prealloc $alt $w --secs "$SECS"
    for m in $SQL; do
      run bench --engine sqlite --fsync "${m%:*}" --sync-kind "${m##*:}" $w --secs "$SECS"
    done
    for m in always interval:50ms never; do
      run bench --engine redb --fsync $m $w --secs "$SECS"
    done
    run bench --engine redb --fsync always --redb-cache-mib 32 $w --secs "$SECS"
  done
  ;;
interval_capacity)
  # Interval mode with a 4096-frame window per producer, so engine capacity rather than
  # window/interval bounds throughput.
  for w in "$SMALL" "$LARGE"; do
    run bench --engine seglog --fsync interval:50ms --sync-kind $FULL --prealloc $PRE $w --window 4096 --secs "$SECS"
    run bench --engine sqlite --fsync interval:50ms --sync-kind $FULL $w --window 4096 --secs "$SECS"
    run bench --engine redb --fsync interval:50ms $w --window 4096 --secs "$SECS"
  done
  ;;
rate)
  # Open-loop offered load; latency measured from scheduled send time.
  for m in always interval:50ms; do
    run bench --engine seglog --fsync $m --sync-kind $FULL --prealloc $PRE $SMALL --rate-mbps 10 --window 256 --secs "$SECS"
    run bench --engine sqlite --fsync $m --sync-kind $FULL $SMALL --rate-mbps 10 --window 256 --secs "$SECS"
    run bench --engine redb --fsync $m $SMALL --rate-mbps 10 --window 256 --secs "$SECS"
  done
  # No producer batching: one 200 B message per frame at 5000 msg/s.
  run bench --engine seglog --fsync always --sync-kind $FULL --prealloc $PRE --rec-size 200 --batch 1 --rate-mbps 1 --window 256 --secs "$SECS"
  run bench --engine sqlite --fsync always --sync-kind $FULL --rec-size 200 --batch 1 --rate-mbps 1 --window 256 --secs "$SECS"
  run bench --engine redb --fsync always --rec-size 200 --batch 1 --rate-mbps 1 --window 256 --secs "$SECS"
  ;;
read)
  for w in "$SMALL" "$LARGE"; do
    run read --engine seglog --fsync never --prealloc $PRE $w --mb 1024
    run read --engine sqlite --fsync never $w --mb 1024
    run read --engine redb --fsync never $w --mb 1024
  done
  ;;
churn)
  run churn --engine seglog --fsync interval:50ms --sync-kind $FULL --prealloc $PRE --segment-bytes 16777216 $SMALL --total-mb 2048 --backlog-mb 256
  run churn --engine sqlite --fsync interval:50ms --sync-kind $FULL $SMALL --total-mb 2048 --backlog-mb 256
  run churn --engine redb --fsync interval:50ms $SMALL --total-mb 2048 --backlog-mb 256
  ;;
recovery)
  for mb in 64 512 2048; do
    run recovery --engine seglog --fsync interval:50ms --prealloc $PRE $SMALL --mb $mb $DROP
    run recovery --engine sqlite --fsync interval:50ms $SMALL --mb $mb $DROP
    run recovery --engine redb --fsync interval:50ms $SMALL --mb $mb $DROP
    run recovery --engine redb --redb-quick-repair --fsync interval:50ms $SMALL --mb $mb $DROP
  done
  ;;
crash)
  for e in seglog sqlite redb; do
    run crash --engine $e --fsync always --prealloc $PRE --segment-bytes 262144 --iters "${CRASH_ITERS:-300}"
    run crash --engine $e --fsync interval:50ms --prealloc $PRE --segment-bytes 262144 --iters 200
    run crash --engine $e --fsync never --prealloc $PRE --segment-bytes 262144 --iters 100
  done
  ;;
esac
done
