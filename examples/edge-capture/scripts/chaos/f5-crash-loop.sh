#!/usr/bin/env bash
# F5: crash loop. Sends SIGKILL to `thadam run` K times while the fleet keeps
# publishing. Each kill comes 5-20 s after the previous restart, and the script
# waits for /readyz before every kill. Uses the level-1 config
# (thadam/pipeline.local.yaml).
#
# Usage: F5_SEED=<n> f5-crash-loop.sh [KILLS]        (default KILLS=3)
#
#   F5_SEED   seeds the interval draw so a run can be replayed (default: now)
#
# Proves: nothing acknowledged is lost across repeated hard crashes. The
# broker redelivers what Thadam had not committed, the WAL recovers on each
# start, and message-level dedup absorbs the redeliveries.
#
# Expected: every restart reaches readyz, the backlog drains, the final
# SIGTERM exits 0, and verify prints PASS. Exit status is that of
# `thadam-simulator verify`.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

KILLS="${1:-3}"
require_uint "KILLS" "$KILLS"
[ "$KILLS" -ge 1 ] || die "KILLS must be at least 1"
SEED="${F5_SEED:-$(date +%s)}"
require_uint "F5_SEED" "$SEED"
TAIL=20

require_tools
ensure_binaries
ensure_stack
new_run f5-crash-loop
use_config pipeline.local.yaml
log "seed $SEED, $KILLS kills, intervals drawn from 5-20 s"

RANDOM="$SEED"
INTERVALS=""
TOTAL=0
i=0
while [ "$i" -lt "$KILLS" ]; do
  gap=$((5 + RANDOM % 16))
  INTERVALS="$INTERVALS $gap"
  TOTAL=$((TOTAL + gap))
  i=$((i + 1))
done
log "planned gaps (s):$INTERVALS"

start_thadam thadam-0.log
wait_readyz 60

# Long enough to outlast the loop (each restart may take a few seconds).
start_sim "$((TOTAL + KILLS * 10 + TAIL))s"

run=0
for gap in $INTERVALS; do
  run=$((run + 1))
  log "kill $run/$KILLS in ${gap}s"
  sleep "$gap"
  wait_readyz 60
  kill_thadam
  start_thadam "thadam-$run.log"
  wait_readyz 60
done

wait_sim
wait_drained 120
stop_thadam
finish_with_verify
