#!/usr/bin/env bash
# F7: disk pressure. Runs Thadam with deliberately small disk budgets
# (thadam/pipeline.disk-pressure.yaml: WAL 8 MiB in 1 MiB segments, sink
# segments 16 MiB, on_full: drop_oldest) while the fleet publishes at full rate.
# The S3 path is not part of this config, so no store needs stopping here.
#
# Usage: f7-disk-pressure.sh [DURATION_SECONDS]      (default 120)
#
# Two rules, checked at the end:
#
#   1. Budget. Every WAL disk sample stays at or below wal.max_disk plus one
#      segment (wal.segment_bytes, 1 MiB). The budget is read from /status at
#      runtime; the segment size is fixed by pipeline.disk-pressure.yaml.
#
#   2. Attributed loss. Under on_full: drop_oldest, acknowledged data may be
#      lost only when Thadam evicts it, and Thadam counts every evicted byte
#      (wal.evicted_bytes in /status). If verify fails and evicted_bytes > 0,
#      the failure is the expected, attributed loss. The full verify report is
#      printed, and the script exits 0. If verify fails with evicted_bytes == 0,
#      that loss is unattributed and the script exits 1. Attribution is at the
#      counter level only: the script does not inspect per-row causes.
#
# The script also exits 1 when the budget never engaged (evicted_bytes == 0 and
# verify passed), because then nothing was tested. Raise DURATION or lower the
# budgets in the config.
#
# Exit status is 1 for a budget breach or an unattributed failure, otherwise 0.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

DURATION="${1:-120}"
SEGMENT_BYTES=1048576
require_uint "DURATION_SECONDS" "$DURATION"

PEAK=0
WAL_LIMIT=0

sample_budget() {
  local used limit
  if ! curl -sf --max-time 2 "http://$ADMIN_ADDR/status" >"$RUN_DIR/status.last.json" 2>/dev/null; then
    return 0
  fi
  used="$(jq -r '.wal.disk_bytes // 0' "$RUN_DIR/status.last.json")"
  limit="$(jq -r '[.budgets[] | select(.category == "wal") | .limit][0] // 0' "$RUN_DIR/status.last.json")"
  if [ "$used" -gt "$PEAK" ]; then
    PEAK="$used"
  fi
  if [ "$limit" -gt 0 ]; then
    WAL_LIMIT="$limit"
  fi
}

require_tools
ensure_binaries
ensure_stack
new_run f7-disk-pressure
use_config pipeline.disk-pressure.yaml
start_thadam thadam.log
wait_readyz 60

start_sim "${DURATION}s"
END=$((SECONDS + DURATION + 10))
NEXT_LOG=$((SECONDS + 10))
while [ "$SECONDS" -lt "$END" ]; do
  sleep 2
  sample_budget
  if [ "$SECONDS" -ge "$NEXT_LOG" ]; then
    log "  $(status_summary) peak_wal=${PEAK}B limit=${WAL_LIMIT}B"
    NEXT_LOG=$((SECONDS + 10))
  fi
done

wait_sim
observe 5
sample_budget
EVICTED="$(jq -r '.wal.evicted_bytes // 0' "$RUN_DIR/status.last.json")"
SEGMENTS="$(jq -r '[.budgets[] | select(.category == "segments") | "\(.used)/\(.limit // 0)"] | join(" ")' "$RUN_DIR/status.last.json")"
log "peak wal disk ${PEAK}B of ${WAL_LIMIT}B budget; evicted ${EVICTED}B; segments used/limit: ${SEGMENTS:-none}"

stop_thadam

rc=0
run_verify || rc=$?
log "verify exit status: $rc"

BUDGET_LIMIT=$((WAL_LIMIT + SEGMENT_BYTES))
if [ "$PEAK" -gt "$BUDGET_LIMIT" ]; then
  log "FAIL: wal disk peaked at ${PEAK}B, above budget plus one segment (${BUDGET_LIMIT}B)"
  exit 1
fi
log "ok: wal disk stayed within budget plus one segment (${BUDGET_LIMIT}B)"

if [ "$EVICTED" -eq 0 ]; then
  log "FAIL: on_full never engaged (no eviction); raise DURATION or lower the budgets"
  exit 1
fi

if [ "$rc" -ne 0 ]; then
  log "expected loss: ${EVICTED}B evicted under on_full=drop_oldest; the verify failures above are attributed to those evictions"
  exit 0
fi
log "verify passed with ${EVICTED}B evicted"
exit 0
