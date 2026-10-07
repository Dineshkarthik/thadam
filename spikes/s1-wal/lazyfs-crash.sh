#!/usr/bin/env bash
# kill -9 loop on a LazyFS mount. After every kill LazyFS drops all file data that was not
# fsynced (lazyfs::clear-cache), i.e. power loss at the kill point. LazyFS only models loss of
# un-fsynced *data*; metadata operations (create, rename, unlink, truncate) pass straight through.
# Runs inside the privileged container: ./lazyfs-crash.sh <label> [iters]
set -euo pipefail
LABEL=$1
ITERS=${2:-200}
B=./target/release/s1-wal
ROOT=/data/lazyfs-root
MNT=/mnt/lazyfs
FIFO=/tmp/lfs.fifo
DONE=/tmp/lfs-done.fifo
OUT=results/$LABEL.jsonl
mkdir -p "$ROOT" "$MNT" results
rm -f "$FIFO" "$DONE"
cat > /tmp/lazyfs.toml <<EOF
[faults]
fifo_path="$FIFO"
fifo_path_completed="$DONE"
[cache]
apply_eviction=false
[cache.simple]
custom_size="2gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile=""
EOF
/opt/lazyfs/lazyfs/build/lazyfs "$MNT" --config-path /tmp/lazyfs.toml -o allow_other -o modules=subdir -o subdir="$ROOT" -f > /tmp/lazyfs.log 2>&1 &
LPID=$!
trap 'fusermount3 -u "$MNT" 2>/dev/null || true; kill $LPID 2>/dev/null || true' EXIT
for _ in $(seq 50); do mountpoint -q "$MNT" && [ -p "$DONE" ] && break; sleep 0.2; done
mountpoint -q "$MNT"

# Keep the completion FIFO open for reading so LazyFS never blocks writing to it.
exec 3<>"$DONE"
CLEAR="echo lazyfs::clear-cache > $FIFO && timeout 20 head -n1 <&3 >/dev/null"

run() {
  echo "+ $*" >&2
  "$B" crash "$@" --segment-bytes 262144 --after-kill "$CLEAR" | tee -a "$OUT"
}
for e in seglog sqlite redb; do
  run --engine $e --fsync always --dir "$MNT/$e-always" --iters "$ITERS"
  run --engine $e --fsync interval:50ms --dir "$MNT/$e-interval" --iters $((ITERS / 2))
  # Positive control: acks without fsync must be lost under LazyFS.
  run --engine $e --fsync never --dir "$MNT/$e-never" --iters 30
done
