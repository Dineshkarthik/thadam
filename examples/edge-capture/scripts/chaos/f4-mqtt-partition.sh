#!/usr/bin/env bash
# F4: MQTT partition. Disables the Toxiproxy `mqtt` proxy for N seconds, then
# re-enables it. Uses the level-1 config (thadam/pipeline.local.yaml).
#
# Usage: f4-mqtt-partition.sh [PARTITION_SECONDS]    (default 30)
#
# The simulator keeps publishing straight to the broker, so the fleet never
# pauses. Only Thadam's consumption path is cut. The broker holds QoS 1 messages
# for Thadam's persistent session (clean_start: false) while it is away.
#
# Proves: a consumer partition loses no acknowledged data. Thadam reconnects,
# resumes its session, and consumes the messages queued during the partition.
#
# Expected: the source reconnects after heal, the backlog drains, and verify
# prints PASS. Exit status is that of `thadam-simulator verify`.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

PARTITION="${1:-30}"
WARMUP=15
TAIL=20
require_uint "PARTITION_SECONDS" "$PARTITION"

require_tools
ensure_binaries
ensure_stack
new_run f4-mqtt-partition
use_config pipeline.local.yaml
start_thadam thadam.log
wait_readyz 60

start_sim "$((WARMUP + PARTITION + TAIL))s"

log "phase 1: healthy for ${WARMUP}s"
observe "$WARMUP"

log "phase 2: partition, MQTT proxy disabled for ${PARTITION}s"
toxi_set mqtt false
observe "$PARTITION"

log "phase 3: heal, MQTT proxy re-enabled"
toxi_set mqtt true
observe "$TAIL"

wait_sim
wait_drained 120
stop_thadam
finish_with_verify
