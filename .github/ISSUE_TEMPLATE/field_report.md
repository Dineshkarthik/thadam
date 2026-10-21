---
name: Field report
about: You ran Thadam on your own hardware or data. Tell us how it went, good or bad
title: "Field report: "
labels: field-report
---

<!--
Redact hostnames, credentials, and payloads. A bug found along the way also deserves its own bug report.
-->

## Deployment

- Thadam version or commit (`thadam version` output):
- Install method (release tarball, Docker image, built from source):
- Hardware and OS/arch (e.g. Raspberry Pi 5 with NVMe, Linux aarch64):
- Storage for the data directory (industrial SSD, eMMC, SD card, network fs):
- How long it ran, and roughly how much data (messages per second, total volume):

## Pipeline

- Sources (e.g. `mqtt`, `opcua`, `can`) and the devices or brokers behind them:
- Sinks (e.g. `parquet`, `object_store`, `kafka`):
- WAL settings that matter (`on_full`, fsync policy, disk budget):

```yaml
# Optional: your pipeline.yaml, secrets redacted
```

## What happened

- What worked:
- What did not, or surprised you:
- Restarts, crashes, power cuts, or network outages it went through, and how it recovered:

## Would you keep running it?

What would need to change before you rely on it?
