#!/usr/bin/env bash
# Pathological expression probes, one child process per case so a stack overflow, OOM or
# timeout is observed instead of killing the harness (perl alarm: 30 s timeout).
set -u
cd "$(dirname "$0")/.."
BIN=${BIN:-target/release/stress}
run() {
  local out rc line
  out=$(perl -e 'alarm 30; exec @ARGV' "$BIN" "$@" 2>&1)
  rc=$?
  line=$(printf '%s\n' "$out" | grep -E " n=" | head -1)
  [ -z "$line" ] && line="$1 n=$2: no result line ($(printf '%s\n' "$out" | grep -iE 'overflow|panicked|abort|alarm|killed|Formatting' | head -2 | tr '\n' ' '))"
  echo "[stack ${3:-default} KiB] $line | exit $rc"
}
for n in 50 95 100 1000 100000; do run parens $n; done
for n in 100 1000 100000; do run and $n; done
for n in 100 1000 100000; do run unary $n; done
for n in 50 100 1000; do run list $n; done
for n in 1000 100000; do run strcat $n; done
for n in 1 2 3 4; do run nested_all $n; done
for n in 50 500 5000 50000; do run pred_not $n; done
for n in 50 500 5000 50000; do run pred_all $n; done
# Where does a left-associative `+` chain overflow the stack? 8 MiB (main thread) and 2 MiB (tokio worker default)
for n in 1000 2000 3000 5000; do run strcat $n 8192; done
for n in 500 1000 2000 3000; do run strcat $n 2048; done
for n in 1000 5000 10000; do run and $n 2048; done
