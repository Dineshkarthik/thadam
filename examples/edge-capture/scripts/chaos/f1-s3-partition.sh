#!/usr/bin/env bash
# F1: S3 partition. Disables the Toxiproxy `s3` proxy for N seconds, then
# re-enables it. Uses the level-2 config (thadam/pipeline.s3.local.yaml).
#
# Usage: f1-s3-partition.sh [PARTITION_SECONDS]      (default 30)
#
# Proves: an S3 outage that looks like a network partition does not cost data.
# Capture from the broker continues through the partition, the shipper backlog
# is held in local segments and the WAL, and after the proxy is restored the
# finalized output is complete: verify (run after a clean SIGTERM, which
# finalizes every sink) prints PASS with no unattributed loss.
#
# The S3 object count is printed for the timeline only; verify does not inspect
# the bucket, so shipping recovery is not an assertion of this script.
#
# Expected: verify prints PASS. Exit status is that of `thadam-simulator verify`.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

PARTITION="${1:-30}"
WARMUP=15
TAIL=20
require_uint "PARTITION_SECONDS" "$PARTITION"

require_tools
ensure_binaries
ensure_stack s3
new_run f1-s3-partition
use_config pipeline.s3.local.yaml
start_thadam thadam.log
wait_readyz 60

start_sim "$((WARMUP + PARTITION + TAIL))s"

log "phase 1: healthy for ${WARMUP}s"
observe "$WARMUP"

log "phase 2: partition, S3 proxy disabled for ${PARTITION}s"
toxi_set s3 false
observe "$PARTITION"

log "phase 3: heal, S3 proxy re-enabled"
toxi_set s3 true
observe "$TAIL"

wait_sim
wait_drained 120
stop_thadam
log "s3 objects in thadam-demo after finalize (informational): $(s3_object_count)"
finish_with_verify
