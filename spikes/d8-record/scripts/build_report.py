#!/usr/bin/env python3
"""Assemble docs/design/spikes/D8-record-representation.md from the template
and the raw result files, so tables are never retyped by hand.

Usage (from spikes/d8-record): python3 scripts/build_report.py
"""
import os
import platform
import re

here = os.path.dirname(os.path.abspath(__file__))
proj = os.path.dirname(here)
out_path = os.path.normpath(os.path.join(proj, "..", "..", "docs", "design", "spikes", "D8-record-representation.md"))


def read(*p):
    with open(os.path.join(proj, *p)) as f:
        return f.read()


def demote(text, from_prefix, to_prefix):
    return re.sub(rf"^{re.escape(from_prefix)} ", f"{to_prefix} ", text, flags=re.M)


alloc = demote(read("results", "alloc_report.md").strip(), "##", "####")

crit = read("results", "criterion_summary.md").strip().split("\n")
crit = "\n".join(l for l in crit if not l.startswith("## Criterion summary")).strip()
crit = "Median ns per element, with the half-width of the median's 95% CI after the plus-minus sign. Elements are events, except `lookup_all` where they are individual lookups.\n\n" + demote(crit, "###", "####")

x = read("results", "xthread.md")
x = x[x.index("## Scenario ring"):]
x = "\n".join(l for l in x.split("\n") if not re.match(r"^\d\d:\d\d\s+up ", l))
x = re.sub(r" \[[0-9.]+-[0-9.]+\]", "", x)
x = demote(x.strip(), "##", "####")

tpl = read("scripts", "report_template.md")
report = (tpl.replace("{{ALLOC}}", alloc).replace("{{CRITERION}}", crit).replace("{{XTHREAD}}", x)
          .replace("{{HOST}}", f"{platform.machine()}, {os.cpu_count()} logical CPUs")
          .replace("{{OS}}", f"{platform.system()} {platform.release()}"))

os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, "w") as f:
    f.write(report)
print("wrote", out_path)
