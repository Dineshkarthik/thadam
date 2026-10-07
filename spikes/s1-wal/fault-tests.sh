#!/usr/bin/env bash
# fsync EIO injection (LD_PRELOAD shim) and ENOSPC (small tmpfs at /small). Linux only.
# Runs inside the container: ./fault-tests.sh <label>
set -euo pipefail
LABEL=$1
B=./target/release/s1-wal
OUT=results/$LABEL.jsonl
mkdir -p results
for e in seglog sqlite redb; do
  for mode in once sticky; do
    rm -rf /data/fault-$e
    FAILSYNC_ARM=/tmp/failsync.arm FAILSYNC_MODE=$mode LD_PRELOAD=/spike/target/failsync.so \
      "$B" fsync-fault --engine $e --dir /data/fault-$e | tee -a "$OUT"
  done
  rm -rf /small/$e
  "$B" enospc --engine $e --segment-bytes 1048576 --dir /small/$e | tee -a "$OUT"
  rm -rf /small/$e
done
