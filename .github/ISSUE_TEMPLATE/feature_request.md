---
name: Feature request
about: Propose a capability — a connector, transform, sink, or operational feature
title: ""
labels: enhancement
---

<!--
Thadam's scope is deliberately narrow: the MVP boundary and explicit non-goals are in product.md (sections 6 and 33) and ARCHITECTURE.md (M1/M2/M3 milestones).
Requests that fit the roadmap phases land better than requests for the non-goals (GUI, fleet management, a schema-registry product, video storage, ...).
-->

## The problem

What are you trying to do, and why can't you do it today? Concrete use cases (which devices, what payload shapes, what downstream system) carry the most weight.

## The proposal

What should Thadam do? If it changes user-facing configuration or behavior, sketch the config or CLI surface you have in mind.

## Alternatives you considered

How do you solve it today (other tools, custom code)? Why is that insufficient?

## Scope fit

- [ ] This fits the current milestone (MQTT → WAL → Parquet → object storage)
- [ ] This fits a later phase per product.md/ARCHITECTURE.md (which one?)
- [ ] This may expand the project boundary — let's discuss

## Evidence offered (optional)

Can you provide: production payload samples, reference hardware (ARM boards, eMMC targets), broker/database infrastructure for integration tests, or a benchmark scenario?
