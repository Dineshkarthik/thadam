#!/usr/bin/env bash
# F8: poison burst. Runs the simulator at a high malformed-message rate, so
# truncated JSON arrives in volume, then checks that quarantine is exact.
# Uses the level-1 config (thadam/pipeline.local.yaml).
#
# Usage: f8-poison.sh [BURST_SECONDS] [MALFORMED_RATE]   (defaults 30 and 0.25)
#
# The malformed rate applies to the whole run, so the run is the burst. The
# demo's normal rate is 0.001 (fleet.yaml).
#
# Proves: poison is contained. Every malformed message the broker acknowledged
# lands in the quarantine dataset exactly once, and none of it reaches a
# delivered dataset. The rest of the fleet still arrives intact around it.
#
# Expected: quarantine row count equals the oracle's distinct acknowledged
# malformed messages, the delivered datasets match the oracle, and verify
# prints PASS. Exit status is that of `thadam-simulator verify`.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

BURST="${1:-30}"
RATE="${2:-0.25}"
require_uint "BURST_SECONDS" "$BURST"

require_tools
ensure_binaries
ensure_stack
new_run f8-poison
use_config pipeline.local.yaml
start_thadam thadam.log
wait_readyz 60

start_sim "$((BURST + 10))s" --malformed-rate "$RATE"

log "burst: malformed rate $RATE for ${BURST}s"
observe "$BURST"

wait_sim
wait_drained 120
log "oracle: $("$SIM_BIN" oracle-stats "$ORACLE" --json | jq -c '{total_samples, publish_acked, malformed: ([.by_stream_expected | to_entries[] | select(.key | endswith("/malformed")) | .value] | add // 0)}')"
stop_thadam
finish_with_verify
