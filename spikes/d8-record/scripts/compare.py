#!/usr/bin/env python3
"""Run-to-run variance between two criterion output directories.

Usage: python3 scripts/compare.py RUN1_DIR RUN2_DIR
Prints the distribution of |run2/run1 - 1| over all benchmarks and the worst cells.
"""
import glob
import json
import os
import statistics
import sys


def load(root):
    out = {}
    for bj in glob.glob(os.path.join(root, "**", "new", "benchmark.json"), recursive=True):
        d = os.path.dirname(bj)
        with open(bj) as f:
            b = json.load(f)
        with open(os.path.join(d, "estimates.json")) as f:
            e = json.load(f)
        out[b["full_id"]] = e["median"]["point_estimate"] / b["throughput"]["Elements"]
    return out


a, b = load(sys.argv[1]), load(sys.argv[2])
keys = sorted(set(a) & set(b))
dev = {k: abs(b[k] / a[k] - 1) for k in keys}
vals = sorted(dev.values())
n = len(vals)
print(f"benchmarks compared: {n}")
print(f"median |delta|: {100 * statistics.median(vals):.1f}%")
print(f"p90 |delta|: {100 * vals[int(0.9 * (n - 1))]:.1f}%")
print(f"max |delta|: {100 * vals[-1]:.1f}%")
print("worst 8:")
for k in sorted(dev, key=dev.get, reverse=True)[:8]:
    print(f"  {k}: run1 {a[k]:.1f} ns, run2 {b[k]:.1f} ns ({100 * (b[k] / a[k] - 1):+.1f}%)")
