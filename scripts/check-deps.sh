#!/usr/bin/env bash
# Enforces the workspace dependency-direction rules (implementation plan, section 2).
#
#   scripts/check-deps.sh              check this workspace (needs cargo and jq)
#   scripts/check-deps.sh --self-test  prove the checks fire on a known-bad fixture
#   scripts/check-deps.sh --metadata F check a saved `cargo metadata` JSON file
#
# Only normal and build dependencies count; dev-dependencies are exempt.
# Extend the tables in the jq program when crates are added (tools/, tests/e2e).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fixture="$root/scripts/fixtures/violating-metadata.json"

read -r -d '' program <<'JQ' || true
def normal_dep: any(.dep_kinds[]; .kind != "dev");

def reach($adj; $start):
  {seen: [], todo: [$start]}
  | until(.todo | length == 0;
      .todo[0] as $n
      | .todo |= .[1:]
      | if (.seen | index($n)) != null then .
        else .seen += [$n] | .todo += ($adj[$n] // [])
        end)
  | .seen - [$start];

def arrow_re: "^(arrow|arrow-.+|parquet)$";

. as $meta
| ($meta.packages | map({key: .id, value: .name}) | from_entries) as $name
| ($meta.workspace_members | map($name[.])) as $members
| (reduce $meta.resolve.nodes[] as $n ({};
    .[$name[$n.id]] += [$n.deps[] | select(normal_dep) | $name[.pkg]])) as $adj

# Transitive closure must not contain these crates.
| {
    "thadam-core": "^(arrow|arrow-.+|parquet|prost|prost-.+|rumqttc|reqwest|hyper|axum|tonic|object_store|clickhouse|serde_yaml|serde_yaml_ng|serde_norway|yaml-rust|yaml-rust2|saphyr)$",
    "thadam-transform": arrow_re,
    "thadam-runtime": "^(arrow|arrow-.+|parquet|object_store|rumqttc|reqwest)$"
  } as $forbidden_transitive

# tokio-util (CancellationToken) legitimately pulls tokio into core's closure,
# so the Tokio runtime is only forbidden as a direct dependency.
| {"thadam-core": ["tokio"], "thadam-transform": ["tokio"]} as $forbidden_direct

# Crate -> the only workspace crates allowed to depend on it.
| {
    "thadam-runtime": ["thadam-cli"],
    "thadam-mqtt": ["thadam-cli"],
    "thadam-http": ["thadam-cli"],
    "thadam-grpc": ["thadam-cli"],
    "thadam-segments": ["thadam-cli", "thadam-object-store"],
    "thadam-object-store": ["thadam-cli"],
    "thadam-clickhouse": ["thadam-cli"],
    "thadam-testkit": []
  } as $allowed_dependents

| ["thadam-arrow", "thadam-segments", "thadam-clickhouse", "thadam-simulator"] as $arrow_users

| [
    ($forbidden_transitive | to_entries[] | .key as $c | .value as $re
      | reach($adj; $c)[] | select(test($re))
      | "forbidden-dep: \($c) must not depend on \(.)"),

    ($forbidden_direct | to_entries[] | .key as $c | .value[] as $d
      | select(($adj[$c] // []) | index($d) != null)
      | "forbidden-direct-dep: \($c) must not depend directly on \($d)"),

    ($members[] as $member | ($adj[$member] // [])[] as $d
      | select($allowed_dependents | has($d))
      | select(($allowed_dependents[$d] | index($member)) == null)
      | "composition-root: \($member) must not depend on \($d) (allowed dependents: \($allowed_dependents[$d] | if length == 0 then "none, dev-dependency only" else join(", ") end))"),

    ($members[] as $member
      | select($member != "thadam-cli")
      | select(reach($adj; $member) | index("thadam-runtime") != null)
      | "runtime-reach: \($member) must not depend, even transitively, on thadam-runtime"),

    ($members[] as $member | ($adj[$member] // [])[] as $d
      | select($d | test(arrow_re))
      | select(($arrow_users | index($member)) == null)
      | "arrow-confinement: \($member) must not depend directly on \($d) (only \($arrow_users | join(", ")))"),

    ($members[] as $member
      | select(($adj[$member] // []) | index("arrow") != null)
      | "arrow-umbrella: \($member) must use arrow sub-crates, not the umbrella arrow crate")
  ]
| unique | .[]
JQ

command -v jq >/dev/null || { echo "check-deps: jq is required" >&2; exit 2; }

check() {
  jq -r "$program" "$1"
}

report() {
  local violations
  violations=$(check "$1")
  if [[ -n "$violations" ]]; then
    echo "$violations" >&2
    echo "check-deps: dependency-direction violations found" >&2
    exit 1
  fi
  echo "check-deps: ok"
}

case "${1:-}" in
  "")
    command -v cargo >/dev/null || { echo "check-deps: cargo is required" >&2; exit 2; }
    metadata=$(mktemp)
    trap 'rm -f "$metadata"' EXIT
    (cd "$root" && cargo metadata --format-version 1 --locked --all-features) >"$metadata"
    report "$metadata"
    ;;
  --metadata)
    [[ -f "${2:-}" ]] || { echo "usage: $0 --metadata FILE" >&2; exit 2; }
    report "$2"
    ;;
  --self-test)
    violations=$(check "$fixture")
    if [[ -z "$violations" ]]; then
      echo "check-deps self-test: FAILED, fixture produced no violations" >&2
      exit 1
    fi
    status=0
    for expected in \
      "forbidden-dep: thadam-core must not depend on prost" \
      "forbidden-dep: thadam-core must not depend on serde_yaml" \
      "forbidden-dep: thadam-runtime must not depend on rumqttc" \
      "forbidden-direct-dep: thadam-core must not depend directly on tokio" \
      "composition-root: thadam-runtime must not depend on thadam-mqtt" \
      "composition-root: thadam-config must not depend on thadam-testkit" \
      "runtime-reach: thadam-object-store" \
      "arrow-confinement: thadam-wal must not depend directly on arrow-array" \
      "arrow-umbrella: thadam-config"; do
      if ! grep -qF -- "$expected" <<<"$violations"; then
        echo "check-deps self-test: FAILED, expected violation not reported: $expected" >&2
        status=1
      fi
    done
    if grep -qF "thadam-runtime must not depend on thadam-testkit" <<<"$violations"; then
      echo "check-deps self-test: FAILED, dev-dependency was flagged" >&2
      status=1
    fi
    [[ $status -eq 0 ]] && echo "check-deps self-test: ok ($(wc -l <<<"$violations" | tr -d ' ') violations detected in fixture)"
    exit $status
    ;;
  *)
    echo "usage: $0 [--self-test | --metadata FILE]" >&2
    exit 2
    ;;
esac
