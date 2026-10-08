---
name: Bug report
about: Something Thadam does is wrong — a crash, data issue, incorrect validation, or misleading error
title: ""
labels: bug
---

<!--
Before filing:
- Search existing issues for the same problem.
- Security-sensitive report? Do NOT file publicly — use Security advisories instead (see SECURITY.md).
- Thadam is pre-alpha: reread the documented limits in the README and docs/design (e.g. delivery guarantees in ADR-0011) to confirm the behavior is not intentional.
-->

## Summary

One sentence: what Thadam does, and what you expected instead.

## Environment

- Thadam commit or version (`thadam version` output):
- OS/arch (e.g. Linux x86_64, Linux aarch64, macOS for development):
- Storage for the data directory (industrial SSD, eMMC, SD card, network fs):
- Broker/sink versions involved (e.g. Mosquitto 2.x, EMQX 5.8.x):

## Reproduction

Minimal steps, with the smallest config that triggers it:

```bash
# commands, config file (secrets redacted), or test case
```

## Actual behavior

What happened: exit code, log excerpts (payloads/secrets redacted), `/status` or metric output, error codes.

## Expected behavior

## Data impact

- [ ] Data was lost that had been acknowledged
- [ ] Duplicates were produced
- [ ] No data impact known

If data was lost or duplicated, include as much detail as you can: WAL `on_full` policy, fsync policy, sink configuration, broker queue settings, and the sequence of crashes/restarts involved.

## Additional context

Anything else: correlation with clock sync, disk pressure, network outages, or `--repair` use.
