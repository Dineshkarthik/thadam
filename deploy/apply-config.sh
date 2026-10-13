#!/bin/sh
# Applies a candidate pipeline configuration to a node that runs `thadam run` on
# the live file: validate it against the data directory, keep the live file as
# `.prev`, switch the file atomically, restart, and roll back if the node does
# not come back on the candidate's config hash. Thadam reads the file only at
# start, so the restart is the apply. Design: docs/design/fleet-config-and-versioning.md
# section 3.
#
# usage: apply-config.sh <candidate.yaml> [<live.yaml>]
set -eu

THADAM_BIN="${THADAM_BIN:-thadam}"
THADAM_RESTART_CMD="${THADAM_RESTART_CMD:-systemctl restart thadam}"
THADAM_ADMIN_URL="${THADAM_ADMIN_URL:-http://127.0.0.1:9464}"
THADAM_ADMIN_TOKEN_FILE="${THADAM_ADMIN_TOKEN_FILE:-}"
THADAM_READY_TIMEOUT_SECS="${THADAM_READY_TIMEOUT_SECS:-30}"

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <candidate.yaml> [<live.yaml>]" >&2
  exit 2
fi
candidate=$1
live=${2:-/etc/thadam/pipeline.yaml}
new="$live.new"
prev="$live.prev"

case "$THADAM_READY_TIMEOUT_SECS" in
  '' | *[!0-9]*)
    echo "apply-config: THADAM_READY_TIMEOUT_SECS must be a whole number of seconds" >&2
    exit 2
    ;;
esac

trap 'rm -f "$new"' EXIT

# Step 1: the candidate, next to the live file, must pass the same checks the
# node applies at start. The hash it prints is the one /status must report.
cp "$candidate" "$new"
status=0
out=$("$THADAM_BIN" validate --against-data-dir "$new") || status=$?
if [ "$status" -ne 0 ]; then
  echo "apply-config: candidate rejected (exit $status); $live is unchanged" >&2
  exit "$status"
fi
printf '%s\n' "$out"
expected=$(printf '%s\n' "$out" | sed -n 's/^ *config hash: \([0-9a-f]*\).*/\1/p')
if [ -z "$expected" ]; then
  echo "apply-config: validate printed no config hash; $live is unchanged" >&2
  exit 1
fi

# Step 2: keep the live file for rollback, then switch the file atomically.
# With no live file before the apply, a rollback removes the candidate again.
if [ -f "$live" ]; then
  cp "$live" "$prev"
else
  rm -f "$prev"
fi
mv -f "$new" "$live"

# shortcut: rollback does not wait for the restored node to become ready, so a node that stays down after rollback is reported only as a message; upgrade when unattended rollbacks need an alarm.
rollback() {
  if [ -f "$prev" ]; then
    mv -f "$prev" "$live"
  else
    rm -f "$live"
  fi
  if ! sh -c "$THADAM_RESTART_CMD"; then
    echo "apply-config: restart after rollback failed; check the node by hand" >&2
  fi
  echo "apply-config: rolled back $live to the previous configuration" >&2
  exit 1
}

# Step 3: restart. A failed restart is rolled back like a failed readiness check.
if ! sh -c "$THADAM_RESTART_CMD"; then
  echo "apply-config: restart command failed" >&2
  rollback
fi

# Step 4: ready on the candidate's hash. /status answers 200 while the node is
# still starting, so the phase, readiness, and hash are checked in the body.
# shortcut: status errors are hidden, so a bad token or URL looks like a slow start; upgrade by keeping the last status error for the rollback message.
status_json() {
  if [ -n "$THADAM_ADMIN_TOKEN_FILE" ]; then
    "$THADAM_BIN" status --admin-url "$THADAM_ADMIN_URL" --token-file "$THADAM_ADMIN_TOKEN_FILE" 2>/dev/null
  else
    "$THADAM_BIN" status --admin-url "$THADAM_ADMIN_URL" 2>/dev/null
  fi
}

deadline=$(( $(date +%s) + THADAM_READY_TIMEOUT_SECS ))
while :; do
  if body=$(status_json) \
    && printf '%s\n' "$body" | grep -Fq '"phase": "running"' \
    && printf '%s\n' "$body" | grep -Fq '"ready": true' \
    && printf '%s\n' "$body" | grep -Fq "\"config_hash\": \"$expected\""; then
    echo "apply-config: $live runs config $expected"
    exit 0
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "apply-config: no ready node on config $expected at $THADAM_ADMIN_URL within ${THADAM_READY_TIMEOUT_SECS}s" >&2
    rollback
  fi
  sleep 0.5
done
