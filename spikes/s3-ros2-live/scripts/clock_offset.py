#!/usr/bin/env python3
"""Estimate (container_clock - host_clock) in nanoseconds.

Spike S3 helper. The rmw_zenoh attachment source timestamp and the ROS
`header.stamp` are stamped with the container clock; the subscriber stamps
recv_time with the Mac host clock. To compare them, sample the container
clock over a persistent `docker exec` shell and keep the minimum-RTT sample
(the lowest round trip is the least delayed one); the offset estimate error
is at most about rtt_min / 2 and is reported so the capture summary can be
read with that tolerance.

stdout: one integer, the offset in ns (suitable for --clock-offset-ns).
stderr: diagnostics JSON.

Usage: clock_offset.py [container] [samples]
"""

import json
import subprocess
import sys
import time


def main() -> None:
    container = sys.argv[1] if len(sys.argv) > 1 else "s3-router"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 15
    proc = subprocess.Popen(
        [
            "docker",
            "exec",
            "-i",
            container,
            "sh",
            "-c",
            "while read -r _; do date +%s%N; done",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    samples = []
    try:
        assert proc.stdin is not None and proc.stdout is not None
        # First three round trips warm up the exec session.
        for i in range(n + 3):
            t0 = time.time_ns()
            proc.stdin.write("ping\n")
            proc.stdin.flush()
            line = proc.stdout.readline().strip()
            t1 = time.time_ns()
            if not line.isdigit() or i < 3:
                continue
            rtt = t1 - t0
            offset = int(line) - (t0 + t1) // 2
            samples.append((rtt, offset))
    finally:
        proc.kill()
    if not samples:
        sys.exit("clock_offset: no samples received")
    rtt_min, offset = min(samples, key=lambda s: s[0])
    print(offset)
    print(
        json.dumps(
            {
                "container": container,
                "offset_ns": offset,
                "rtt_min_ns": rtt_min,
                "offset_error_bound_ns": rtt_min // 2,
                "samples": len(samples),
            }
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
