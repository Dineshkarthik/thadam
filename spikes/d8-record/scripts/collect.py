#!/usr/bin/env python3
"""Summarise criterion output into markdown tables.

Reads target/criterion/**/new/{benchmark,estimates}.json and prints median
time per element (event, or lookup for lookup_all) in ns, with the median's
95% confidence interval half-width as a noise indicator.

Usage: python3 scripts/collect.py [target/criterion] > results/criterion_summary.md
"""
import glob
import json
import os
import sys
from collections import defaultdict

root = sys.argv[1] if len(sys.argv) > 1 else "target/criterion"

rows = {}  # (op, wl, val, fn) -> (ns_per_elem, halfwidth_ns_per_elem, elements)
parse = {}
for bj in glob.glob(os.path.join(root, "**", "new", "benchmark.json"), recursive=True):
    d = os.path.dirname(bj)
    with open(bj) as f:
        b = json.load(f)
    with open(os.path.join(d, "estimates.json")) as f:
        e = json.load(f)
    elems = b["throughput"]["Elements"]
    med = e["median"]["point_estimate"] / elems
    ci = e["median"]["confidence_interval"]
    half = (ci["upper_bound"] - ci["lower_bound"]) / 2 / elems
    parts = b["full_id"].split("/", 3)
    if parts[0] == "parse_only":
        parse[parts[1]] = (med, half)
        continue
    op, wl, val, fn = parts
    rows[(op, wl, val, fn)] = (med, half, elems)

REPRS = ["A-fresh", "A-interned", "B-shape-arc", "C-shape-id"]
WLS = ["w1", "w2"]
VALS = ["min", "ext"]


def cell(key):
    v = rows.get(key)
    if v is None:
        return "n/a"
    return f"{v[0]:.1f} ±{v[1]:.1f}"


def table(op, title, fns=None, note=""):
    print(f"### {title}\n")
    if note:
        print(note + "\n")
    cols = fns or [(r, r) for r in REPRS]
    print("| wl | values | " + " | ".join(c[0] for c in cols) + " |")
    print("|---|---|" + "---|" * len(cols))
    for wl in WLS:
        for val in VALS:
            print(f"| {wl} | {val} | " + " | ".join(cell((op, wl, val, c[1])) for c in cols) + " |")
    print()


print("## Criterion summary (median ns per element, ± half-width of 95% CI of the median)\n")
print("### Reference: serde_json::from_slice -> Value (parse only, no record construction)\n")
print("| wl | ns/event |")
print("|---|---|")
for wl in WLS:
    if wl in parse:
        print(f"| {wl} | {parse[wl][0]:.1f} ±{parse[wl][1]:.1f} |")
print()

table("construct", "Construction from parsed serde_json::Value (ns/event)")
lookup_cols = [
    ("A-fresh by-name", "A-fresh/by-name"),
    ("A-interned by-name", "A-interned/by-name"),
    ("B by-name", "B-shape-arc/by-name"),
    ("B cached-idx", "B-shape-arc/cached-idx"),
    ("C by-name", "C-shape-id/by-name"),
    ("C cached-idx", "C-shape-id/cached-idx"),
]
table("lookup_one", "Field lookup: one field (last numeric field) (ns/lookup)", lookup_cols)
table("lookup_all", "Field lookup: every field of the record (ns/lookup, W1: 7 fields, W2: 3 fields)", lookup_cols)
table(
    "transform",
    "Transform pass: rename + unit conversion + filter (ns/event, uniquely owned input)",
)
table("clone", "Clone (ns/event; includes allocation, drop is outside the timer)")
table("drop", "Drop of uniquely owned events (ns/event)")
table("arrow", "Conversion of 10,000 events to an Arrow RecordBatch (ns/event)")

print("### Derived: construct + transform + arrow + drop (ns/event)\n")
print(
    "Sum of the independent medians above. It is an indicative per-event total for the "
    "decode-to-Arrow path, not a separately measured pipeline: it ignores that the "
    "filter removes ~30% of events before Arrow conversion and that real decode "
    "starts from WAL bytes rather than a parsed serde_json::Value.\n"
)
print("| wl | values | " + " | ".join(REPRS) + " |")
print("|---|---|" + "---|" * len(REPRS))
for wl in WLS:
    for val in VALS:
        cells = []
        for r in REPRS:
            parts = [rows.get((op, wl, val, r)) for op in ("construct", "transform", "arrow", "drop")]
            cells.append("n/a" if any(p is None for p in parts) else f"{sum(p[0] for p in parts):.0f}")
        print(f"| {wl} | {val} | " + " | ".join(cells) + " |")
print()
