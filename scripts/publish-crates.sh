#!/usr/bin/env bash
# First publish of the workspace crates to crates.io, paced for the new-crate
# rate limit (https://crates.io/docs/rate-limits: a burst of 5 new crates, then
# 1 new crate per 10 minutes per account). Later releases go through
# .github/workflows/release.yml instead.
#
# Usage, from the release tag:
#   CARGO_REGISTRY_TOKEN=<publish-new token> scripts/publish-crates.sh
# On macOS, prefix with `caffeinate -i` so the machine stays awake (about 3 hours).
#
# Publishes in dependency order, skips versions already on crates.io (safe to
# re-run), retries HTTP 429 every 2 minutes, and stops on any other failure.
set -euo pipefail
: "${CARGO_REGISTRY_TOKEN:?export a crates.io token with the publish-new scope first}"
cd "$(dirname "${BASH_SOURCE[0]}")/.."

burst=5
interval=600
ua='thadam-release (https://github.com/Dineshkarthik/thadam)'
meta=$(cargo metadata --no-deps --format-version 1 --locked)
version=$(python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "thadam"))' <<<"$meta")
order=$(python3 -c '
import json, sys
pkgs = {p["name"]: p for p in json.load(sys.stdin)["packages"] if p["publish"] is None}
deps = {n: {d["name"] for d in p["dependencies"] if d["name"] in pkgs and d["kind"] != "dev"} for n, p in pkgs.items()}
order, done = [], set()
while len(order) < len(deps):
    ready = sorted(n for n in deps if n not in done and deps[n] <= done)
    if not ready: sys.exit("dependency cycle")
    order += ready; done |= set(ready)
print(" ".join(order))' <<<"$meta")

new=0
last=0
for crate in $order; do
  if curl -fs -o /dev/null -A "$ua" "https://crates.io/api/v1/crates/$crate/$version"; then
    echo "== $crate $version is already on crates.io, skipping"
    continue
  fi
  if (( new >= burst )); then
    wait=$(( last + interval - SECONDS ))
    if (( wait > 0 )); then
      echo "== waiting ${wait}s (rate limit) before $crate"
      sleep "$wait"
    fi
  fi
  until out=$(cargo publish -p "$crate" --locked 2>&1 | tee /dev/stderr); do
    grep -q '429' <<<"$out" || { echo "== $crate failed for a reason other than the rate limit; stopping"; exit 1; }
    echo "== rate limited on $crate; retrying in 120s"
    sleep 120
  done
  new=$((new + 1))
  last=$SECONDS
  echo "== published $crate $version ($new new this run)"
done
echo "== done: all crates at $version are on crates.io"
