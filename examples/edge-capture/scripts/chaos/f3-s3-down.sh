#!/usr/bin/env bash
# F3: S3 backend down. Stops the SeaweedFS container for N seconds
# (`docker compose stop seaweedfs`), then starts it again. Uses the level-2
# config (thadam/pipeline.s3.local.yaml).
#
# Usage: f3-s3-down.sh [DOWN_SECONDS]                (default 30)
#
# Difference from F1: F1 cuts the network path (Toxiproxy refuses the
# connection) and leaves the store running. F3 takes the store itself away, so
# the shipper also sees a dead upstream behind a live proxy, and on restart the
# store must come back with its data intact.
#
# Proves: the same as F1 with a dead upstream instead of a dead path. A store
# outage does not cost data, and the finalized output is complete after the
# store returns. As in F1, verify checks the local output; the bucket is only
# counted for the timeline.
#
# Expected: verify prints PASS. Exit status is that of `thadam-simulator verify`.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

DOWN="${1:-30}"
WARMUP=15
TAIL=20
require_uint "DOWN_SECONDS" "$DOWN"

require_tools
ensure_binaries
ensure_stack s3
new_run f3-s3-down
use_config pipeline.s3.local.yaml
start_thadam thadam.log
wait_readyz 60

start_sim "$((WARMUP + DOWN + TAIL))s"

log "phase 1: healthy for ${WARMUP}s"
observe "$WARMUP"

log "phase 2: docker compose stop seaweedfs for ${DOWN}s"
SEAWEED_STOPPED=1
compose --profile s3 stop seaweedfs >/dev/null
observe "$DOWN"

log "phase 3: docker compose start seaweedfs"
compose --profile s3 start seaweedfs >/dev/null
wait_until 60 "SeaweedFS S3 gateway after restart" curl -s -o /dev/null --max-time 2 "$S3_ENDPOINT/"
SEAWEED_STOPPED=0
observe "$TAIL"

wait_sim
wait_drained 120
stop_thadam
log "s3 objects in thadam-demo after finalize (informational): $(s3_object_count)"
finish_with_verify
