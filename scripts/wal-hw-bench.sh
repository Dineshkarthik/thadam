#!/usr/bin/env bash
# ADR-0004 S1 re-run on reference hardware. Runs flush probes, the S1 harness
# (commit latency, churn, recovery), the thadam-wal crash loop and criterion
# benches, and LazyFS when available. Writes <out>/report.md, raw results,
# logs, and <out>.tar.gz. Runbook: docs/design/wal-hardware-runbook.md.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/wal-hw-bench.sh --data-dir <path> [--out <dir>] [--quick] [--skip-lazyfs]

  --data-dir <path>  Directory on the device under test (required; created if missing).
  --out <dir>        Report directory; must not exist or be empty.
                     Default: wal-bench-<hostname>-<YYYY-MM-DD> in the current directory.
  --quick            Short durations and iteration counts (smoke run, not for ADR decisions).
  --skip-lazyfs      Do not run the LazyFS loop even if lazyfs is on PATH.
EOF
}

DATA=""
OUT=""
QUICK=0
SKIP_LAZYFS=0
while [ $# -gt 0 ]; do
  case $1 in
    --data-dir) DATA=${2:?"--data-dir needs a path"}; shift 2 ;;
    --out) OUT=${2:?"--out needs a path"}; shift 2 ;;
    --quick) QUICK=1; shift ;;
    --skip-lazyfs) SKIP_LAZYFS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; echo "error: unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$DATA" ]; then
  usage >&2
  echo "error: --data-dir is required" >&2
  exit 2
fi

OS=$(uname -s)
HOST=$(hostname -s 2>/dev/null || hostname)
START=$(date '+%Y-%m-%d %H:%M:%S %Z')
[ -n "$OUT" ] || OUT="wal-bench-$HOST-$(date +%Y-%m-%d)"
if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT")" ]; then
  echo "error: report dir $OUT exists and is not empty" >&2
  exit 2
fi
mkdir -p "$OUT/raw" "$OUT/logs" "$DATA"
OUT=$(cd "$OUT" && pwd)
DATA=$(cd "$DATA" && pwd)
REPO=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO"

TDIR=${CARGO_TARGET_DIR:-$REPO/target/wal-bench}
case $TDIR in /*) ;; *) TDIR=$REPO/$TDIR ;; esac
export CARGO_TARGET_DIR=$TDIR
WORK="$DATA/wal-hw-work"
RAW="$OUT/raw"
LOGS="$OUT/logs"
rm -rf "$WORK"
mkdir -p "$WORK/tmp-crash" "$WORK/tmp-criterion"
trap 'rm -rf "$WORK"' EXIT
for f in flush append rate churn recovery crash criterion lazyfs; do : > "$RAW/$f.jsonl"; done
: > "$OUT/status.tsv"
: > "$OUT/run.log"

if [ "$QUICK" = 1 ]; then
  SECS=2; WARM=0.5; FLUSH_SECS=5; FLUSH_OPS=200
  RECOVERY_REPS=1; CRASH_ITERS=20
  CHURN_TOTAL=128; CHURN_BACKLOG=16; NEED_KB=524288
  GROUP_MAXES="4194304"; CRIT_ARGS="--quick"
  LAZY_ALWAYS=10; LAZY_INTERVAL=5; LAZY_NEVER=10
else
  SECS=10; WARM=1; FLUSH_SECS=30; FLUSH_OPS=2000
  RECOVERY_REPS=5; CRASH_ITERS=1000
  CHURN_TOTAL=2048; CHURN_BACKLOG=256; NEED_KB=2097152
  GROUP_MAXES="65536 262144 4194304"; CRIT_ARGS=""
  LAZY_ALWAYS=300; LAZY_INTERVAL=150; LAZY_NEVER=30
fi
FSYNC_MODES="always interval:10ms never"
if [ "$OS" = Darwin ]; then
  SYNC=full; PRE=false; DROP=""
else
  SYNC=data; PRE=true; DROP="--drop-caches"
fi
if [ "$PRE" = true ]; then ALT_PRE=false; else ALT_PRE=true; fi
LAZY_STATUS="skipped"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$OUT/run.log" >&2; }
record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$OUT/status.tsv"; }
now() { perl -MTime::HiRes=time -e 'printf "%.6f\n", time'; }
label_of() { printf '%s' "$1" | tr ':' '_'; }

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: $1 not found on PATH" >&2; exit 2; }
}
need jq
need perl
need awk
need cargo
need tar
if [ "$OS" = Linux ]; then need findmnt; fi

AVAIL_KB=$(df -Pk "$DATA" | awk 'NR == 2 {print $4}')
if [ "$AVAIL_KB" -lt "$NEED_KB" ]; then
  echo "error: $DATA has ${AVAIL_KB} KiB free; need at least $NEED_KB KiB" >&2
  exit 2
fi

DEV_NAME=""
if [ "$OS" = Linux ]; then
  SRC=$(findmnt -T "$DATA" -no SOURCE 2>/dev/null || true)
  if [ -n "$SRC" ] && [ -e "$SRC" ]; then
    base=$(basename "$(readlink -f "$SRC")")
    if [ -e "/sys/class/block/$base/partition" ]; then
      DEV_NAME=$(basename "$(dirname "$(readlink -f "/sys/class/block/$base")")")
    else
      DEV_NAME=$base
    fi
  fi
fi

dev_sectors() {
  [ -n "$DEV_NAME" ] || return 0
  awk -v d="$DEV_NAME" '$3 == d {print $10; exit}' /proc/diskstats 2>/dev/null || true
}

record_environment() {
  local f="$OUT/environment.txt" s="$OUT/storage.txt"
  {
    echo "generated_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "mode: $([ "$QUICK" = 1 ] && echo quick || echo full)"
    echo "host: $HOST"
    echo "uname: $(uname -a)"
    if [ "$OS" = Darwin ]; then
      echo "os_release: macOS $(sw_vers -productVersion 2>/dev/null || echo unknown) build $(sw_vers -buildVersion 2>/dev/null || echo unknown)"
      echo "cpu_model: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
      echo "cpu_cores: $(sysctl -n hw.ncpu 2>/dev/null || echo unknown)"
      echo "ram: $(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%d MiB", $1 / 1048576}' || true)"
    else
      echo "os_release: $(. /etc/os-release 2>/dev/null; echo "${PRETTY_NAME:-unknown}") / kernel $(uname -r)"
      cpu=$(lscpu 2>/dev/null | awk -F: '/^Model name/ {sub(/^[ \t]+/, "", $2); print $2; exit}' || true)
      [ -n "$cpu" ] || cpu=$(tr -d '\0' < /proc/device-tree/model 2>/dev/null || echo unknown)
      echo "cpu_model: $cpu"
      echo "cpu_cores: $(nproc 2>/dev/null || echo unknown)"
      echo "ram: $(awk '/^MemTotal/ {printf "%d MiB", $2 / 1024}' /proc/meminfo)"
      if [ -r /proc/device-tree/model ]; then
        echo "board_model: $(tr -d '\0' < /proc/device-tree/model)"
      fi
    fi
    echo "rustc: $(rustc --version 2>/dev/null || echo missing)"
    echo "cargo: $(cargo --version 2>/dev/null || echo missing)"
    echo "active_toolchain: $(rustup show active-toolchain 2>/dev/null || echo unknown)"
    echo "jq: $(jq --version 2>/dev/null || echo missing)"
    echo "fio: $(fio --version 2>/dev/null || echo 'not installed')"
    echo "lazyfs: $(command -v lazyfs 2>/dev/null || echo 'not on PATH')"
    echo "git_commit: $(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "git_dirty_paths: $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
    echo "data_dir: $DATA"
    echo "out_dir: $OUT"
    echo "device_for_diskstats: ${DEV_NAME:-not resolved}"
  } > "$f"
  git -C "$REPO" status --porcelain > "$OUT/git-status.txt" 2>/dev/null || true
  {
    if [ "$OS" = Darwin ]; then
      df -P "$DATA"
      mp=$(df -P "$DATA" | awk 'NR == 2 {print $NF}')
      dev=$(df -P "$DATA" | awk 'NR == 2 {print $1}')
      mount | awk -v m="$mp" '$3 == m'
      diskutil info "$dev"
    else
      findmnt -T "$DATA" -o TARGET,SOURCE,FSTYPE,OPTIONS || true
      lsblk -o NAME,MODEL,ROTA,TRAN,SIZE || true
    fi
  } > "$s" 2>&1 || true
}

build_spike() {
  log "building S1 harness (release) into $TDIR"
  if ! cargo build --release --manifest-path "$REPO/spikes/s1-wal/Cargo.toml" > "$LOGS/build-s1.log" 2>&1; then
    echo "error: S1 harness build failed, see $LOGS/build-s1.log" >&2
    exit 1
  fi
  S1="$TDIR/release/s1-wal"
  [ -x "$S1" ] || { echo "error: $S1 missing" >&2; exit 1; }
}

flush_probe() {
  local f="$WORK/flush-probe.bin" out="$RAW/flush.jsonl" line rc=0 i t0 t1 ms
  rm -f "$f"
  if command -v fio >/dev/null 2>&1 \
     && fio --name=wal-flush --filename="$f" --rw=write --bs=4k --size=64m --ioengine=sync \
            --fdatasync=1 --time_based --runtime="$FLUSH_SECS" --output-format=json \
            > "$LOGS/flush-fio.json" 2> "$LOGS/flush-fio.log" \
     && line=$(jq -c '{method: "fio --ioengine=sync --fdatasync=1 --bs=4k", ops: .jobs[0].sync.total_ios,
                       p50_ms: (.jobs[0].sync.lat_ns.percentile["50.000000"] / 1e6),
                       p99_ms: (.jobs[0].sync.lat_ns.percentile["99.000000"] / 1e6),
                       mean_ms: (.jobs[0].sync.lat_ns.mean / 1e6)}' "$LOGS/flush-fio.json" 2>/dev/null); then
    echo "$line" > "$out"
  elif [ "$OS" = Linux ]; then
    t0=$(now)
    dd if=/dev/zero of="$f" bs=4k count="$FLUSH_OPS" oflag=dsync 2> "$LOGS/flush-dd.log" || rc=$?
    t1=$(now)
    ms=$(awk -v a="$t0" -v b="$t1" 'BEGIN {print (b - a) * 1000}')
    jq -nc --argjson ops "$FLUSH_OPS" --argjson ms "$ms" --argjson rc "$rc" \
      '{method: "dd oflag=dsync bs=4k (mean only; approximation, no percentiles)", ops: $ops, mean_ms: ($ms / $ops), exit: $rc}' > "$out"
  else
    : > "$WORK/flush-ms.txt"
    for ((i = 0; i < FLUSH_OPS; i++)); do
      t0=$(now)
      dd if=/dev/zero of="$f" bs=4k count=1 conv=fsync 2>/dev/null || true
      t1=$(now)
      awk -v a="$t0" -v b="$t1" 'BEGIN {printf "%.4f\n", (b - a) * 1000}' >> "$WORK/flush-ms.txt"
    done
    sort -n "$WORK/flush-ms.txt" | awk '
      { v[NR] = $1; s += $1 }
      END {
        n = NR
        printf "{\"method\":\"dd conv=fsync bs=4k per op (includes dd startup; smoke only)\",\"ops\":%d,\"p50_ms\":%s,\"p99_ms\":%s,\"mean_ms\":%s}\n",
          n, v[int((n * 50 + 99) / 100)], v[int((n * 99 + 99) / 100)], s / n
      }' > "$out"
  fi
  rm -f "$f"
  record flush 0 probe
}

spike() {
  local file=$1 name=$2 gm=$3 rc=0 before after dev=null
  shift 3
  local dir="$WORK/s1-$name" out="$LOGS/$name.json"
  log "run $name"
  before=$(dev_sectors)
  "$S1" "$@" --dir "$dir" > "$out" 2> "$LOGS/$name.stderr" || rc=$?
  after=$(dev_sectors)
  rm -rf "$dir"
  if [ -n "$before" ] && [ -n "$after" ]; then dev=$(( (after - before) * 512 )); fi
  if [ "$rc" -eq 0 ] && [ -s "$out" ]; then
    jq -c --arg name "$name" --arg gm "$gm" --argjson dev "$dev" \
      '. + {name: $name, group_max: (if $gm == "-" then null else ($gm | tonumber) end), device_bytes_written: $dev}' \
      "$out" >> "$RAW/$file.jsonl"
  else
    log "FAILED $name (exit $rc), see logs/$name.stderr"
  fi
  record "$name" "$rc" "$file"
  return 0
}

run_append() {
  local mode label gm
  for mode in $FSYNC_MODES; do
    label=$(label_of "$mode")
    for gm in $GROUP_MAXES; do
      spike append "append_small_${label}_gm${gm}" "$gm" bench --engine seglog --fsync "$mode" \
        --sync-kind "$SYNC" --prealloc "$PRE" --rec-size 200 --batch 64 \
        --secs "$SECS" --warmup "$WARM" --group-max "$gm"
    done
    if [ "$QUICK" = 0 ]; then
      spike append "append_large_${label}_gm4194304" 4194304 bench --engine seglog --fsync "$mode" \
        --sync-kind "$SYNC" --prealloc "$PRE" --rec-size 4096 --batch 16 \
        --secs "$SECS" --warmup "$WARM" --group-max 4194304
    fi
  done
  spike append "append_small_always_prealloc_${ALT_PRE}" 4194304 bench --engine seglog --fsync always \
    --sync-kind "$SYNC" --prealloc "$ALT_PRE" --rec-size 200 --batch 64 \
    --secs "$SECS" --warmup "$WARM" --group-max 4194304
}

run_rate() {
  local mode label
  for mode in always interval:10ms; do
    label=$(label_of "$mode")
    spike rate "rate_${label}" - bench --engine seglog --fsync "$mode" --sync-kind "$SYNC" \
      --prealloc "$PRE" --rec-size 200 --batch 64 --rate-mbps 10 --window 256 \
      --secs "$SECS" --warmup "$WARM"
  done
}

run_churn() {
  spike churn "churn_interval_10ms" - churn --engine seglog --fsync interval:10ms --sync-kind "$SYNC" \
    --prealloc "$PRE" --segment-bytes 16777216 --rec-size 200 --batch 64 \
    --total-mb "$CHURN_TOTAL" --backlog-mb "$CHURN_BACKLOG"
}

run_recovery() {
  local i
  for i in $(seq "$RECOVERY_REPS"); do
    spike recovery "recovery_rep${i}" - recovery --engine seglog --fsync interval:10ms --sync-kind "$SYNC" \
      --prealloc "$PRE" --rec-size 200 --batch 64 --mb 64 $DROP
  done
}

run_crash_loop() {
  local log_file="$LOGS/crash-loop.log" rc=0 summary iters failures
  log "crash loop: $CRASH_ITERS kill -9 iterations (thadam-wal crash_loop)"
  (cd "$REPO" && TMPDIR="$WORK/tmp-crash" THADAM_WAL_CRASH_ITERS="$CRASH_ITERS" \
     cargo test -p thadam-wal --test crash_loop kill_dash_nine_crash_loop -- --nocapture) \
     > "$log_file" 2>&1 || rc=$?
  summary=$(grep 'crash loop:' "$log_file" | tail -n 1 || true)
  iters=""
  failures=""
  if [ -n "$summary" ]; then
    iters=$(printf '%s\n' "$summary" | awk '{print $3 + 0}')
    failures=$(printf '%s\n' "$summary" | awk '{print $(NF - 1) + 0}')
  fi
  jq -nc --argjson it "${iters:-null}" --argjson fl "${failures:-null}" --argjson rc "$rc" --arg s "$summary" \
    '{iterations: $it, failures: $fl, cargo_exit: $rc, summary: $s}' > "$RAW/crash.jsonl"
  record crash_loop "$rc" crash
  if [ "$rc" -ne 0 ]; then log "FAILED crash loop (cargo exit $rc), see logs/crash-loop.log"; fi
}

run_criterion() {
  local rc=0 est g bytes name
  local read_frames=$(( (64 << 20) / (64 * 200) ))
  log "criterion: thadam-wal benches (append, read)"
  (cd "$REPO" && TMPDIR="$WORK/tmp-criterion" cargo bench -p thadam-wal --bench wal -- $CRIT_ARGS) \
    > "$LOGS/criterion.log" 2>&1 || rc=$?
  record criterion "$rc" criterion
  if [ "$rc" -ne 0 ]; then log "FAILED criterion (exit $rc), see logs/criterion.log"; fi
  for g in append/always append/interval_10ms append/never read/replay_1mib_chunks; do
    est="$TDIR/criterion/$g/new/estimates.json"
    [ -f "$est" ] || continue
    case $g in
      append/*) bytes=$(( 64 * 64 * 200 )) ;;
      *) bytes=$(( read_frames * 64 * 200 )) ;;
    esac
    name=$g
    jq -c --arg b "$name" --argjson bytes "$bytes" \
      '{bench: $b, median_ns: .median.point_estimate, bytes_per_iter: $bytes}' "$est" >> "$RAW/criterion.jsonl"
  done
}

lazy_one() {
  local name=$1 mode=$2 iters=$3 clear=$4 rc=0
  log "lazyfs: $name ($mode, $iters iterations)"
  "$S1" crash --engine seglog --fsync "$mode" --dir "$WORK/lazyfs-mnt/seglog-$name" \
    --segment-bytes 262144 --iters "$iters" --after-kill "$clear" \
    > "$LOGS/lazyfs-$name.json" 2> "$LOGS/lazyfs-$name.stderr" || rc=$?
  record "lazyfs_$name" "$rc" lazyfs
  if [ -s "$LOGS/lazyfs-$name.json" ]; then
    jq -c --arg n "$name" --argjson rc "$rc" \
      '{name: $n, fsync: .fsync, iterations: .iterations, failures: (.failures | length), acks_observed: .acks_observed, exit: $rc}' \
      "$LOGS/lazyfs-$name.json" >> "$RAW/lazyfs.jsonl"
  else
    jq -nc --arg n "$name" --argjson rc "$rc" '{name: $n, exit: $rc, error: "no output"}' >> "$RAW/lazyfs.jsonl"
  fi
  return 0
}

run_lazyfs() {
  local root="$DATA/lazyfs-root" mnt="$WORK/lazyfs-mnt" fifo="$WORK/lfs.fifo"
  local done_fifo="$WORK/lfs-done.fifo" cfg="$WORK/lazyfs.toml" lpid clear i
  rm -rf "$root" "$mnt"
  mkdir -p "$root" "$mnt"
  rm -f "$fifo" "$done_fifo"
  cat > "$cfg" <<EOF
[faults]
fifo_path="$fifo"
fifo_path_completed="$done_fifo"
[cache]
apply_eviction=false
[cache.simple]
custom_size="2gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile=""
EOF
  lazyfs "$mnt" --config-path "$cfg" -o allow_other -o modules=subdir -o subdir="$root" -f \
    > "$LOGS/lazyfs-mount.log" 2>&1 &
  lpid=$!
  for i in $(seq 50); do
    if mountpoint -q "$mnt" && [ -p "$done_fifo" ]; then break; fi
    sleep 0.2
  done
  if ! mountpoint -q "$mnt"; then
    kill "$lpid" 2>/dev/null || true
    LAZY_STATUS="failed: LazyFS did not mount (see logs/lazyfs-mount.log)"
    record lazyfs 1 lazyfs
    return 0
  fi
  exec 3<>"$done_fifo"
  clear="echo lazyfs::clear-cache > $fifo && timeout 20 head -n1 <&3 >/dev/null"
  lazy_one always always "$LAZY_ALWAYS" "$clear"
  lazy_one interval_10ms interval:10ms "$LAZY_INTERVAL" "$clear"
  lazy_one never never "$LAZY_NEVER" "$clear"
  exec 3>&-
  fusermount3 -u "$mnt" 2>/dev/null || fusermount -u "$mnt" 2>/dev/null || umount "$mnt" 2>/dev/null || true
  kill "$lpid" 2>/dev/null || true
  wait "$lpid" 2>/dev/null || true
  rm -rf "$root"
  LAZY_STATUS="ran (seglog: always $LAZY_ALWAYS, interval:10ms $LAZY_INTERVAL, never $LAZY_NEVER as expected-loss control)"
}

make_summary() {
  jq -n \
    --slurpfile flush "$RAW/flush.jsonl" \
    --slurpfile rate "$RAW/rate.jsonl" \
    --slurpfile churn "$RAW/churn.jsonl" \
    --slurpfile rec "$RAW/recovery.jsonl" \
    --slurpfile crash "$RAW/crash.jsonl" \
    --slurpfile lazy "$RAW/lazyfs.jsonl" \
    --arg lazy_status "$LAZY_STATUS" \
    '{
      flush: ($flush[0] // null),
      rate_always_p99_us: ([$rate[] | select(.name == "rate_always")][0].lat_p99_us // null),
      rate_interval_p99_us: ([$rate[] | select(.name == "rate_interval_10ms")][0].lat_p99_us // null),
      churn: ($churn[0] // null),
      recovery_open_ms: [$rec[] | .open_ms],
      crash: ($crash[0] // null),
      lazyfs: (if ($lazy | length) == 0 then null else $lazy end),
      lazyfs_status: $lazy_status
    }' > "$OUT/summary.json"
}

JQ_LIB='
def r2(x): if x == null then "n/a" else ((x * 100 | round) / 100 | tostring) end;
def dwa: (.payload_mib // .total_mib) as $p
  | if (.device_bytes_written == null or $p == null or $p == 0) then null
    else (.device_bytes_written / ($p * 1048576)) end;
'

rows() {  # file, jq row expression (no leading dot), empty text
  if [ -s "$1" ]; then
    jq -r "$JQ_LIB"' '"$2" "$1"
  else
    echo "_No runs completed; see logs._"
  fi
}

CRITERIA_JQ='
def pf(ok): if ok then "PASS" else "FAIL" end;
def commit(title; p99; extra):
  (.flush.p50_ms // .flush.mean_ms) as $f
  | p99 as $p
  | if ($p == null or $f == null) then "| \(title) | n/a | n/a | NOT MEASURED |"
    else (2 * ($f + extra)) as $lim | ($p / 1000) as $m
      | "| \(title) | <= \(r2($lim)) ms | \(r2($m)) ms | \(pf($m <= $lim)) |"
    end;
commit("Commit p99, always, reference load (open-loop 10 MB/s): <= 2 x fsync latency"; .rate_always_p99_us; 0),
commit("Commit p99, interval 10 ms, reference load: <= 2 x (fsync latency + 10 ms)"; .rate_interval_p99_us; 10),
(if .churn == null then "| Steady disk <= backlog + one segment (churn, interval 10 ms) | n/a | n/a | NOT MEASURED |"
 else (.churn.backlog_target_mib + .churn.segment_mib) as $lim | .churn.steady_disk_max_mib as $m
   | "| Steady disk <= backlog + one segment (churn, interval 10 ms) | <= \(r2($lim)) MiB | \(r2($m)) MiB | \(pf($m <= $lim)) |"
 end),
(if (.recovery_open_ms | length) == 0 then "| Recovery, 64 MiB backlog (eMMC criterion) | <= 50 ms | n/a | NOT MEASURED |"
 else (.recovery_open_ms | sort) as $s | ($s | max) as $mx | $s[((($s | length) - 1) / 2) | floor] as $md
   | "| Recovery, 64 MiB backlog (eMMC criterion) | <= 50 ms | median \(r2($md)) ms, max \(r2($mx)) ms | \(pf($mx <= 50)) |"
 end),
(if .crash == null then "| Kill -9 loop, >= 1000 iterations, zero failures | >= 1000 iterations, 0 failures | not run | NOT MEASURED |"
 else .crash as $c
   | if (($c.failures // 0) > 0 or $c.cargo_exit != 0) then "| Kill -9 loop, >= 1000 iterations, zero failures | >= 1000 iterations, 0 failures | \($c.iterations // "n/a") iterations, \($c.failures // "n/a") failures, cargo exit \($c.cargo_exit) | FAIL |"
     elif (($c.iterations // 0) >= 1000) then "| Kill -9 loop, >= 1000 iterations, zero failures | >= 1000 iterations, 0 failures | \($c.iterations) iterations, 0 failures | PASS |"
     else "| Kill -9 loop, >= 1000 iterations, zero failures | >= 1000 iterations, 0 failures | \($c.iterations // 0) iterations, \($c.failures // 0) failures | NOT MEASURED (criterion needs 1000) |"
     end
 end),
(if .lazyfs == null then "| LazyFS, 300 iterations, zero failures (always and interval) | >= 300 always iterations, 0 failures | \(.lazyfs_status) | NOT MEASURED |"
 else ([.lazyfs[] | select(.name == "always")][0] // {}) as $a
   | ([.lazyfs[] | select(.name == "interval_10ms")][0] // {}) as $i
   | (($a.failures // 0) + ($i.failures // 0)) as $f
   | ($a.iterations // 0) as $n
   | "| LazyFS, 300 iterations, zero failures (always and interval) | >= 300 always iterations, 0 failures | always \($n) iterations, \($a.failures // "n/a") failures; interval \($i.iterations // 0) iterations, \($i.failures // "n/a") failures | \(if $f > 0 then "FAIL" elif $n >= 300 then "PASS" else "NOT MEASURED (criterion needs 300)" end) |"
 end)
'

write_report() {
  local rep="$OUT/report.md"
  make_summary
  {
    echo "# WAL hardware bench report (ADR-0004 S1 re-run)"
    echo
    echo "- Mode: $([ "$QUICK" = 1 ] && echo 'quick (smoke run; not valid for ADR decisions)' || echo full)"
    echo "- Started: $START"
    echo "- Finished: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "- Data dir (device under test): \`$DATA\`"
    echo "- Host: $HOST"
    echo
    echo "## Pass criteria (ADR-0004, remaining item 6)"
    echo
    echo "| Criterion | Threshold | Measured | Result |"
    echo "|---|---|---|---|"
    jq -r "$JQ_LIB$CRITERIA_JQ" "$OUT/summary.json"
    echo
    echo "## Environment"
    echo
    echo '```'
    cat "$OUT/environment.txt"
    echo '```'
    echo
    echo '```'
    cat "$OUT/storage.txt"
    echo '```'
    echo
    echo "## Raw flush latency"
    echo
    echo "| Method | Ops | p50 ms | p99 ms | mean ms |"
    echo "|---|---|---|---|---|"
    if [ -s "$RAW/flush.jsonl" ]; then
      jq -r "$JQ_LIB"' "| \(.method) | \(.ops) | \(r2(.p50_ms)) | \(r2(.p99_ms)) | \(r2(.mean_ms)) |"' "$RAW/flush.jsonl"
    else
      echo "| _flush probe did not run_ | | | | |"
    fi
    echo
    echo "## Commit-to-ack latency by fsync mode and group size (S1 harness, seglog)"
    echo
    echo "Latency is submit-to-ack per frame. \`process WA\` is OS-counted bytes written by the process per payload byte. \`device WA\` is \`/proc/diskstats\` sectors written to the device during the run, divided by payload bytes (Linux only)."
    echo
    echo "| Run | fsync | Frame shape | Group max (bytes) | Frames/group | p50 us | p99 us | p99.9 us | MB/s | syncs/s | process WA | device WA |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
    rows "$RAW/append.jsonl" '"| \(.name) | \(.fsync) | \(.rec_size) B x \(.batch) | \(.group_max // "default") | \(r2(.frames_per_group)) | \(.lat_p50_us) | \(.lat_p99_us) | \(.lat_p999_us) | \(r2(.mb_per_s)) | \(.syncs_per_s) | \(r2(.os_write_amp)) | \(r2(dwa)) |"'
    echo
    echo "## Reference load (open-loop 10 MB/s, window 256)"
    echo
    echo "| Run | fsync | Frame shape | Group max (bytes) | Frames/group | p50 us | p99 us | p99.9 us | MB/s | syncs/s | process WA | device WA |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
    rows "$RAW/rate.jsonl" '"| \(.name) | \(.fsync) | \(.rec_size) B x \(.batch) | \(.group_max // "default") | \(r2(.frames_per_group)) | \(.lat_p50_us) | \(.lat_p99_us) | \(.lat_p999_us) | \(r2(.mb_per_s)) | \(.syncs_per_s) | \(r2(.os_write_amp)) | \(r2(dwa)) |"'
    echo
    echo "## Churn: steady disk and reclaim"
    echo
    echo "| Run | Segment MiB | Backlog target MiB | Steady disk max MiB | Steady disk avg MiB | Disk after full consume MiB | Reclaim ms | MB/s | process WA | device WA |"
    echo "|---|---|---|---|---|---|---|---|---|---|"
    rows "$RAW/churn.jsonl" '"| \(.name) | \(.segment_mib) | \(.backlog_target_mib) | \(r2(.steady_disk_max_mib)) | \(r2(.steady_disk_avg_mib)) | \(r2(.disk_after_full_consume_mib)) | \(r2(.full_consume_reclaim_ms)) | \(r2(.mb_per_s)) | \(r2(.os_write_amp)) | \(r2(dwa)) |"'
    echo
    echo "## Recovery (engine open after kill -9, 64 MiB backlog)"
    echo
    echo "| Run | Backlog MiB on disk | Frames | Open ms | Caches dropped |"
    echo "|---|---|---|---|---|"
    rows "$RAW/recovery.jsonl" '"| \(.name) | \(r2(.backlog_mib)) | \(.frames) | \(r2(.open_ms)) | \(.caches_dropped) |"'
    echo
    echo "## Kill -9 crash loop (thadam-wal crash_loop test)"
    echo
    echo "| Iterations | Failures | Cargo exit | Summary line |"
    echo "|---|---|---|---|"
    rows "$RAW/crash.jsonl" '"| \(.iterations // "n/a") | \(.failures // "n/a") | \(.cargo_exit) | \(.summary) |"'
    echo
    echo "## LazyFS"
    echo
    echo "Status: $LAZY_STATUS"
    echo
    echo "| Run | fsync | Iterations | Failures | Acks observed | Exit |"
    echo "|---|---|---|---|---|---|"
    rows "$RAW/lazyfs.jsonl" '"| \(.name) | \(.fsync // "n/a") | \(.iterations // "n/a") | \(.failures // "n/a") | \(.acks_observed // "n/a") | \(.exit) |"'
    echo
    echo "## Criterion benches (thadam-wal, production code)"
    echo
    echo "Per-iteration times: append = 64 frames x 64 records x 200 B; read = full replay of 64 MiB."
    echo
    echo "| Bench | Median ms per iteration | MB/s |"
    echo "|---|---|---|"
    rows "$RAW/criterion.jsonl" '"| \(.bench) | \(r2(.median_ns / 1e6)) | \(r2(.bytes_per_iter / .median_ns * 1000)) |"'
    echo
    echo "## Method notes"
    echo
    echo "- Commit latency and churn/recovery come from the S1 spike harness (\`spikes/s1-wal\`, seglog engine). They do not exercise the production \`thadam-wal\` code. The criterion table and crash loop do."
    echo "- Reference load is the S1 open-loop rate run (4 producers, 200 B x 64 records per frame, 10 MB/s, window 256). ADR-0004 does not define the reference load; this is the assumed value."
    echo "- Flush latency statistic used by the commit criterion: p50 (fio or per-op macOS loop), or mean for the dd fallback."
    echo "- Linux uses \`data\` sync (fdatasync). macOS uses \`full\` (F_FULLFSYNC). Preallocation is on for Linux and off for macOS, and the alternate setting is run once for \`always\`."
    echo "- Device WA counts every write to the whole block device backing the data dir during each run, including filesystem metadata. Partitions map to their parent disk. LVM or LUKS devices map to the dm device only."
    echo "- Recovery is a 64 MiB backlog written by a child process and killed with SIGKILL, then the engine open is timed. The 64 MiB default segment size is used, so the last-segment size is not exactly 64 MiB. Caches are dropped only when running as root on Linux (see the Caches dropped column)."
    echo "- The crash-loop test rotates fsync modes always, interval 5 ms, and never. The 5 ms interval is hard-coded in the test and differs from the 10 ms interval in the S1 runs. Its segments are 256 KiB. It covers process kill -9 only, not power loss."
    echo "- LazyFS covers the seglog engine only. It drops un-fsynced data at each kill, so directory ordering is not exercised. The never-fsync run is an expected-loss control."
    echo "- Quick runs are smoke tests only. They do not satisfy any ADR-0004 criterion."
  } > "$rep"
}

log "start: data=$DATA out=$OUT quick=$QUICK skip_lazyfs=$SKIP_LAZYFS os=$OS"
record_environment
build_spike
log "flush probe"
flush_probe
run_append
run_rate
run_churn
run_recovery
run_crash_loop
run_criterion
if [ "$SKIP_LAZYFS" = 1 ]; then
  LAZY_STATUS="skipped (--skip-lazyfs)"
elif [ "$OS" != Linux ]; then
  LAZY_STATUS="skipped (LazyFS needs Linux FUSE)"
elif ! command -v lazyfs >/dev/null 2>&1; then
  LAZY_STATUS="skipped (lazyfs not on PATH)"
else
  run_lazyfs
fi
log "writing report"
write_report
(cd "$(dirname "$OUT")" && COPYFILE_DISABLE=1 tar -czf "$OUT.tar.gz" "$(basename "$OUT")")
log "done: $OUT/report.md and $OUT.tar.gz"
