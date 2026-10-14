#!/usr/bin/env python3
"""Turn a saved criterion run log into markdown tables (ns/event with the criterion
[low, mid, high] interval). Usage: scripts/summarize.py <criterion-output.log>"""
import re
import sys
from collections import OrderedDict

N = 1024
UNITS = {"ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}

text = open(sys.argv[1]).read()
# "<bench id>  time:   [a unit b unit c unit]" (criterion wraps long ids onto the next line)
pat = re.compile(
    r"^(\S+)\s+time:\s+\[([\d.]+) (\S+) ([\d.]+) (\S+) ([\d.]+) (\S+)\]", re.M
)
rows = OrderedDict()
for m in pat.finditer(text):
    name = m.group(1)
    if name.startswith("Benchmarking"):
        continue
    lo, mid, hi = (
        float(m.group(2)) * UNITS[m.group(3)],
        float(m.group(4)) * UNITS[m.group(5)],
        float(m.group(6)) * UNITS[m.group(7)],
    )
    rows[name] = (lo, mid, hi)

groups = OrderedDict()
for name, v in rows.items():
    parts = name.split("/")
    if parts[0] == "compile":
        groups.setdefault("compile", OrderedDict())[name] = v
    else:
        g = "/".join(parts[:2])
        groups.setdefault(g, OrderedDict())["/".join(parts[2:])] = v

variants = ["native", "cel_eval_only", "cel_resolver", "cel_ctx_refs", "cel_ctx_all", "evalexpr_refs"]
print("### Per-event evaluation, ns/event (criterion median of 40 samples, [low, high] in brackets)\n")
print("| filter | " + " | ".join(variants) + " |")
print("|---|" + "---|" * len(variants))
for g, d in groups.items():
    if g == "compile":
        continue
    cells = []
    for v in variants:
        if v in d:
            lo, mid, hi = (x / N for x in d[v])
            cells.append(f"{mid:,.0f} [{lo:,.0f}, {hi:,.0f}]")
        else:
            cells.append("n/a")
    print(f"| {g} | " + " | ".join(cells) + " |")

print("\n### Ratio to native (median)\n")
print("| filter | " + " | ".join(variants[1:]) + " |")
print("|---|" + "---|" * (len(variants) - 1))
for g, d in groups.items():
    if g == "compile":
        continue
    base = d["native"][1]
    cells = [f"{d[v][1] / base:,.0f}x" if v in d else "n/a" for v in variants[1:]]
    print(f"| {g} | " + " | ".join(cells) + " |")

print("\n### Compile cost (once per config load), microseconds per compile (median)\n")
print("| filter | cel | native (yaml parse + compile) | evalexpr |")
print("|---|---|---|---|")
comp = groups.get("compile", {})
filters = OrderedDict()
for name, v in comp.items():
    _, kind, f = name.split("/")
    filters.setdefault(f, {})[kind] = v[1] / 1e3
for f, d in filters.items():
    ee = f"{d['evalexpr']:,.1f}" if "evalexpr" in d else "n/a"
    print(f"| {f} | {d['cel']:,.1f} | {d['native_yaml_parse_and_compile']:,.1f} | {ee} |")
