#!/usr/bin/env bash
# Alias-expansion and nesting stress, one child process per case so that a stack
# overflow, OOM or timeout is observed instead of killing the harness.
# macOS: uses /usr/bin/time -l for peak RSS and perl alarm for a 30 s timeout.
set -u
cd "$(dirname "$0")/.."
BIN=${BIN:-target/release/adr012-yaml}
run() {
  local out rc
  out=$( { /usr/bin/time -l perl -e 'alarm 30; exec @ARGV' "$BIN" "$@" ; } 2>&1 )
  rc=$?
  local line rss
  line=$(printf '%s\n' "$out" | grep -E "bomb|deep" | head -1)
  rss=$(printf '%s\n' "$out" | awk '/maximum resident set size/ {printf "%.1f MiB", $1/1048576}')
  [ -z "$line" ] && line="$* -> no result line (exit $rc; $(printf '%s\n' "$out" | grep -iE 'overflow|abort|signal|alarm|killed' | head -1))"
  echo "$line | peak RSS ${rss:-?} | exit $rc"
}
for c in norway ng saphyr; do
  for n in 5 7 9; do run bomb $c $n; done
done
for c in norway ng saphyr; do
  for d in 100 1000 10000 100000; do run deep $c $d; done
done
