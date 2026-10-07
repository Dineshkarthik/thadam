#!/usr/bin/env python3
"""Renders markdown tables from results/<label>.jsonl. Usage: report_tables.py results/macos.jsonl"""
import json
import sys


def rows(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def table(header, data):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in data]
    return "\n".join(out)


def engine_label(d):
    e = d["engine"]
    if e == "seglog":
        e += f" ({d['sync_kind']}{', prealloc' if d.get('prealloc') else ''})"
    elif e == "sqlite":
        e += f" ({d['sync_kind']})"
    elif e == "redb":
        bits = []
        if d.get("redb_cache_mib"):
            bits.append(f"cache {d['redb_cache_mib']} MiB")
        if d.get("redb_quick_repair"):
            bits.append("quick_repair")
        if bits:
            e += " (" + ", ".join(bits) + ")"
    return e


def f(x, n=1):
    return "" if x is None else f"{x:.{n}f}"


def ms(us):
    return f"{us / 1000:.1f}"


def main(path):
    rs = rows(path)
    app = [d for d in rs if d["test"] == "append" and not d.get("rate_mbps_offered")]
    for rec in (200, 4096):
        for cap in (False, True):
            sel = [d for d in app if d["rec_size"] == rec and (d["window"] == 4096) == cap]
            if not sel:
                continue
            title = f"Append, {rec} B records x {sel[0]['batch']} per frame, 4 producers, window {sel[0]['window']}"
            print(f"\n#### {title}\n")
            print(table(
                ["engine", "fsync", "MB/s", "records/s", "commit p50 ms", "p99 ms", "p99.9 ms", "frames/group", "fsyncs/s", "OS write amp", "peak RSS MiB"],
                [[engine_label(d), d["fsync"], f(d["mb_per_s"]), f"{d['records_per_s']:,.0f}", ms(d["lat_p50_us"]), ms(d["lat_p99_us"]),
                  ms(d["lat_p999_us"]), f(d["frames_per_group"]), f(d["syncs_per_s"], 0), f(d["os_write_amp"], 2), f(d["maxrss_mib"], 0)] for d in sel]))
    rate = [d for d in rs if d["test"] == "append" and d.get("rate_mbps_offered")]
    if rate:
        print("\n#### Open-loop offered load (latency from scheduled send time)\n")
        print(table(
            ["engine", "fsync", "offered", "record x batch", "achieved MB/s", "commit p50 ms", "p99 ms", "p99.9 ms", "max ms", "frames/group", "OS write amp"],
            [[engine_label(d), d["fsync"], f"{d['rate_mbps_offered']:g} MB/s", f"{d['rec_size']} B x {d['batch']}", f(d["mb_per_s"], 2),
              ms(d["lat_p50_us"]), ms(d["lat_p99_us"]), ms(d["lat_p999_us"]), ms(d["lat_max_us"]), f(d["frames_per_group"]), f(d["os_write_amp"], 2)] for d in rate]))
    rd = [d for d in rs if d["test"] == "read"]
    if rd:
        print("\n#### Replay from cursor 0 (warm page cache, engine reopened first)\n")
        print(table(["engine", "record size", "backlog MiB", "MB/s", "records/s", "peak RSS MiB"],
                    [[engine_label(d), d["rec_size"], f(d["backlog_mib"], 0), f(d["mb_per_s"], 0), f"{d['records_per_s']:,.0f}", f(d["maxrss_mib"], 0)] for d in rd]))
    ch = [d for d in rs if d["test"] == "churn"]
    if ch:
        print("\n#### Churn: 2 GiB appended, consumer trails 256 MiB behind, fsync interval:50ms\n")
        print(table(["engine", "MB/s", "steady disk avg MiB", "steady max MiB", "after full consume MiB", "full-consume reclaim ms", "time in delete ms", "OS write amp", "peak RSS MiB"],
                    [[engine_label(d), f(d["mb_per_s"], 0), f(d["steady_disk_avg_mib"], 0), f(d["steady_disk_max_mib"], 0), f(d["disk_after_full_consume_mib"], 1),
                      f(d["full_consume_reclaim_ms"], 0), f(d["delete_ms_total"], 0), f(d["os_write_amp"], 2), f(d["maxrss_mib"], 0)] for d in ch]))
    rc = [d for d in rs if d["test"] == "recovery"]
    if rc:
        print("\n#### Recovery after kill -9 vs backlog\n")
        print(table(["engine", "backlog on disk MiB", "open + first query ms", "page cache dropped", "detail"],
                    [[engine_label(d), f(d["backlog_mib"], 0), f(d["open_ms"], 1), d["caches_dropped"], d["info"]] for d in rc]))
    cr = [d for d in rs if d["test"] == "crash"]
    if cr:
        print("\n#### kill -9 loops\n")
        print(table(["engine", "fsync", "after kill", "iterations", "kills < 5 ms", "ACKs observed", "max retained frames", "recovery p50 ms", "p99 ms", "failures"],
                    [[d["engine"], d["fsync"], "LazyFS clear-cache" if d.get("after_kill") else "-", d["iterations"], d["kills_under_5ms"], f"{d['acks_observed']:,}",
                      d["max_retained_frames"], f(d["recovery_open_ms_p50"]), f(d["recovery_open_ms_p99"]),
                      (f"{len(d['failures'])}: " + d["failures"][0][:90]) if d["failures"] else "0"] for d in cr]))
    ft = [d for d in rs if d["test"] == "fsync_fault"]
    if ft:
        print("\n#### fsync EIO injection (LD_PRELOAD)\n")
        print(table(["engine", "mode", "faulted commit", "next commit, same handle", "2nd commit", "after reopen: seqs 1..6 present"],
                    [[d["engine"], d["mode"], d["faulted_commit"][:70], d["next_commit_same_handle"][:70], d["second_commit_same_handle"][:40], d["after_reopen_present_1_to_6"]] for d in ft]))
    en = [d for d in rs if d["test"] == "enospc"]
    if en:
        print("\n#### ENOSPC (24 MiB tmpfs)\n")
        print(table(["engine", "acked before error", "first error", "reopen", "acked intact", "reclaim", "append after reclaim+reopen"],
                    [[d["engine"], d["acked_before_error"], d["first_error"][:60], str(d["steps"].get("reopen"))[:40], d["steps"].get("acked_intact"),
                      str(d["steps"].get("reclaim"))[:40], str(d["steps"].get("append_after_reclaim_and_reopen"))[:50]] for d in en]))


if __name__ == "__main__":
    main(sys.argv[1])
