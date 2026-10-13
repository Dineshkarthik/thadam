# Fleet configuration and pipeline versioning

- **Status:** Draft (design note, not an ADR; nothing here is decided or built)
- **Date:** 2026-10-13, written against `main` at `b6f8b27` (the working tree also holds uncommitted Sparkplug work, which this note does not rely on)
- **Scope:** `product.md` section 32 Phase 5 ("remote configuration, fleet management, pipeline versioning, deployment tooling, centralized observability"), under the section 33 "do not build initially" list.

## 1. Problem and non-goals

An operator with tens of robots needs three things: to know which configuration each node runs and which configuration produced each file, to change configuration on a node without losing the capture guarantees, and to see all nodes in one place.

Today Thadam reads one YAML file at `thadam run <path>` and keeps it until the process exits (`apps/thadam-cli/src/run.rs:31`). Hot reload is a stated non-goal (`ARCHITECTURE.md:61,645`; ADR-0012 line 143, which names the missing story: "cursors, chain state, and in-flight batches"). The admin API is read-only on purpose (`website/docs/src/operations/admin-api.md:89`; `crates/thadam-runtime/src/admin.rs:85-90` has four `GET` routes).

Non-goals:

- A control-plane service, a GUI, a managed cloud, a Kubernetes operator, a workflow scheduler (`product.md` section 33).
- Any outbound call from Thadam that is not a configured source or sink. This is the owner's no-phone-home rule. In `apps/thadam-cli/src` and `crates/thadam-runtime/src` the only HTTP client use I found is `thadam status` calling the admin URL the user gave it (`apps/thadam-cli/src/status.rs:41`). Connector crates were not audited.
- Hot reload, multiple pipelines per process (ADR-0003 and ADR-0012 revisit triggers).
- Accepting configuration content over HTTP (reasons in section 3).
- Config signing, staged rollouts, canaries, fleet inventory, binary self-update.

## 2. Pipeline versioning

### Identity

A running pipeline is `(pipeline.name, config_hash)`. The hash exists and is well defined: SHA-256 truncated to 16 bytes over a canonical JSON of the typed spec, before interpolation, so key order, comments and secret rotation do not move it (`crates/thadam-config/src/hash.rs:1-16,104-120`, `CONFIG_HASH_VERSION = 1`). `thadam validate` prints it (`apps/thadam-cli/src/validate.rs:58-62`), so a delivery tool can compute the desired hash from the file before it ships. `thadam version` prints only the hash algorithm version (`apps/thadam-cli/src/main.rs:148`).

A human label (git SHA, release name) is deliberately not added to the config. The hash says what; the delivery tool's git history says which commit. CI can record `thadam validate` output next to the commit.

The hash does not cover enrichment-file content or the content behind `${file:}` references. Replacing an enrichment file under an unchanged config is an invisible version change (section 5, open question 3).

### Where the hash is carried today (checked in code)

| Carrier | Today |
|---|---|
| `data_dir/thadam.json` | `config_hash`, `config_hash_version`, `previous_config_hash` (one deep). Written on every start (`crates/thadam-runtime/src/datadir.rs:80-86,265-294`, `pipeline.rs:629-635`). No history; `updated_unix` changes on every start, so it does not say when the hash changed. |
| `GET /status` | `pipeline`, `config_hash` (`crates/thadam-runtime/src/status.rs:422-441`). No node id, binary version, hash version, previous hash or start time. |
| Log | One `info` line, only when the hash changed since the last run (`pipeline.rs:636-642`). |
| Prometheus | Nothing. Labels are limited to source, sink, dataset and bounded enums (`website/docs/src/operations/metrics.md:3`); there is no info metric. |
| Segment catalog, object metadata | No hash. Object metadata carries `thadam-sha256`, `thadam-wal-range`, `thadam-dataset`, `thadam-node` (`website/docs/src/sinks/object-store.md:50`). |
| Parquet footer `thadam.config_hash` | **Written empty before 04ba7f7 (found by reading, not by running a pipeline).** The writer supports the key (`crates/thadam-segments/src/writer.rs:32,81`), but the runtime hands the sink its `config:` block unchanged (`pipeline.rs:489`), and the only source of the value is a `config_hash` key inside that block, default `""` (`crates/thadam-segments/src/config.rs:290`). Fixed in 04ba7f7: the runtime passes the config hash and the chain's enrichment hash through `BuildContext`, and `crates/thadam-runtime/tests/parquet_pipeline_e2e.rs` checks the footer against the hash `validate` prints. |

The last row matters because five documents state the footer carries the hash: `website/docs/src/configuration/index.md:88`, `sinks/parquet.md:42`, `getting-started/validating-configs.md:86`, ADR-0012 line 28, `ARCHITECTURE.md:372`. `thadam.enrichment_hash` has the same gap (`chain.rs:37` loads the hashes; nothing passes them on).

### Design

1. **Make the footer true.** Add the config hash to `thadam_core::BuildContext` (it already carries `component_id`, `data_dir` and datasets; `thadam-segments` reads it in `ParquetSegmentSinkFactory::build`, `sink.rs:955`). `thadam-runtime` cannot depend on `thadam-segments` (ADR-0003 dependency rules), so the context is the only clean path. A non-empty context value wins over the legacy `config_hash` key.
2. **What the footer hash means.** It is the configuration the *chain* ran, not the one in force when the frame was captured. Chains always apply the current configuration from their cursor (ADR-0003:51), so WAL captured under hash A and processed after a restart under hash B is stamped B. This is what replay row-set equivalence needs (same WAL range, same hash). A segment never mixes hashes, because a restart deletes `staging/*.inprogress` and re-derives them from the WAL (`ARCHITECTURE.md:569`).
3. **`/status` additions:** `node_id`, `thadam_version`, `config_hash_version`, `previous_config_hash`, `started_unix`. All are known at build time.
4. **One info metric**, `thadam_build_info{version,pipeline,node_id,config_hash} 1`. Not a label on every metric: per-process cardinality is one series, and a join (`group_left`) attaches the hash to any other series. This is a deliberate exception to the label rule in `metrics.md:3` and needs the pinned metric-name test updated (`metrics.md:77`).
5. **One startup log record every start**, with `node_id`, `config_hash`, `previous_config_hash`, `thadam_version`. This is the per-node audit line.

## 3. Remote configuration

### Options

| Option | Verdict |
|---|---|
| **A. File drop, restart to apply (recommended).** Any external tool (git-sync, Ansible, scp, a container config mount) atomically replaces `/etc/thadam/pipeline.yaml`. A short script validates, restarts, checks and rolls back. Thadam only reads the file at start. | Smallest. Matches ADR-0012. No new Thadam attack surface. |
| B. Thadam watches the path and restarts itself. | Rejected for now. It saves one `systemctl restart` but moves timing and rollback decisions into Thadam, and an unreviewed half-copied file (`scp` is not atomic) becomes a self-applying change. The safety steps (validate, keep previous, check readiness) are needed either way and are easier outside the process. |
| C. Thadam pulls from an HTTP(S) URL. | Rejected. It is an outbound channel to a server that is not a configured source or sink, which is the phone-home the owner ruled out, and it adds TLS trust, credential and offline-failure handling. `curl`/git-sync under the operator's control does the same with existing audit trails. |
| D. `POST` a config body to the admin API. | Rejected. The admin token today can only read. A config can point `${file:/…}` into any sink or source (secrets exfiltration) and change `data_dir`, so write access to config is close to code execution on the node. Keeping the admin token read-only means a token leaked into a Prometheus config cannot change behaviour. Auth also uses a plain `==` compare (`admin.rs:171`), which would need to become constant-time before any write route. |

Where option A leaves Thadam without cover, the answer is a stronger validator, not a new network surface.

### Validate before apply

`thadam validate` is weaker than `thadam run`. It checks the file and kind names only (`validate.rs:33-36`). The factories' `validate()`, the chain compile, and the data-directory checks (`datadir::check_before_wal`, pipeline name and format versions) run only inside `Pipeline::build` and `run_inner` (`pipeline.rs:333-452,592-595`). A file that passes `validate` can still exit 2 or 3 at start, and the unit does not restart on 2 or 3 (`deploy/systemd/thadam.service:24`), so the node would stay down.

Design: split the front half of `Pipeline::build` (up to the `if !errs.is_empty()` at line 450, before any connector is built) into `Pipeline::check`, and add `thadam validate --against-data-dir`. With it, validate also reads `thadam.json`, `wal/wal.json` and the directory listing under `pipeline.data_dir`, read-only (the same way `thadam inspect` already does, `apps/thadam-cli/src/inspect.rs:3`), and reports:

- refusals `run` would make: pipeline name change, `node_id`/machine binding change (`crates/thadam-wal/src/wal.rs:155`), newer format version;
- the hash change (`current -> candidate`);
- state-derived notes, which need no stored copy of the old config: a sink or source directory on disk that the candidate does not define; a sink in the candidate with no cursor (it will start from `start_from`, default `earliest`, `plan.rs:49-59`); a different `data_dir` (nothing refuses this; a new empty directory is created and the old one is orphaned).

### Apply, atomic switch, rollback

Thadam's side of the contract is unchanged: SIGTERM drains and exits 0; a second signal or 30 s exits 130; the WAL plus recovery cover anything unfinished (`website/docs/src/operations/deployment.md:72-74`). The apply script (a step below) does:

1. Write the candidate to `pipeline.yaml.new` next to the live file; run `thadam validate --against-data-dir` (on the node, with real secrets, not `--allow-unresolved`).
2. Keep the live file as `pipeline.yaml.prev`; `mv` the candidate over `pipeline.yaml` (atomic on one filesystem).
3. Restart (`systemctl restart`, `docker restart`).
4. Poll `GET /status` until `phase == running`, `ready == true` and `config_hash` equals the hash `validate` printed, within a timeout. On failure: move `.prev` back, restart, exit non-zero.

Rollback restores the *file*, not the effects of the failed run: cursors and segments written under the new hash stay (and keep their new-hash footers). That is acceptable because chains re-apply whichever config is current from each cursor, and capture is raw (ADR-0003:51, `crash-recovery.md:31`).

### Which changes are safe on restart

There is no hot path; every change is a restart. The useful distinction is what a restart does to data. Rows marked *(read)* come from reading the code and were not run.

| Change | On restart |
|---|---|
| Transforms, filters, sampling, dedup | Apply from each sink's cursor to not-yet-processed WAL; finalized segments are not rewritten. Stateful steps warm-start from the WAL tail. |
| Add a dataset field (new field id), sink tuning, log/admin settings | *(read)* Nothing in the WAL depends on them. New segments carry the new `thadam.schema_fp` (`writer.rs:80`); shipped files keep theirs. Removing or retyping a field was not checked. |
| Add a sink | New cursor starting at `start_from`; `earliest` backfills the whole retained WAL. Set `latest` to avoid that. |
| Remove a sink or shipper | Stops pinning WAL retention (retention reads the current consumers, `crates/thadam-wal/src/retention.rs:108`). *(read)* Its cursor file and `sinks/<id>/` stay on disk (I found no code that removes them), and unshipped finalized segments are not shipped by anything. Re-adding the same id resumes the stale cursor and may hit `Gap::Evicted`. Drain first (`lag_frames == 0`, nothing `eligible`). |
| Remove a source with its datasets | Frames it already put in the WAL are read by chains; with no dataset left to match them they are counted as `unmatched` and dropped (`crates/thadam-transform/src/chain.rs:81,434`; validation forces a dataset's source to exist, `semantic.rs:592`). Exported as `thadam_chain_unmatched_total{sink}` and as `counters.unmatched` on `chain:<sink>` in `/status` (step 4). Wait for the sinks' cursors to pass that source's last frame before removing it. |
| `wal.on_full`, `wal.max_disk` | Changes the one place acknowledged data can be dropped (`backpressure-and-disk-budgets.md:39`). Treat as a reviewed change, not a routine one. |
| `pipeline.name`, `node_id` | A name change is refused (exit 3) unless `--repair`. A `node_id` change makes the WAL refuse to open (`wal.rs:155`); whether `--repair` covers it was not checked. Not a remote-apply case. |
| `pipeline.data_dir` | Not refused; starts a fresh WAL. A migration, not an apply. |

Every apply has a capture gap: intake stops first, then drain (up to 10 s for chains, 30 s overall), then recovery and start (`ARCHITECTURE.md:555-560`). Sources with broker-side queues (MQTT QoS 1, persistent session) are covered by the broker's queue limits; QoS 0 and best-effort sources drop. The gap length is not measured.

### Audit trail

Three layers, none new in Thadam beyond the startup record in section 2: the delivery tool (who, what, when: git log, Ansible log); the host (`journalctl` restart record); Thadam (the startup log record plus `thadam.json`'s last and previous hash). No config-history store is added; version history is the delivery tool's git history.

## 4. Fleet management and centralized observability

Thadam provides:

- **Stable node identity:** `pipeline.node_id` already exists, is bound into the WAL (`wal.rs:155`), and appears in file names and object metadata. It is surfaced in `/status`, the info metric and the startup log (section 2). No registration, no separate fleet id.
- **A comparable version:** one config file for the whole fleet gives one hash only if `node_id` is not a literal in it. Use `node_id: ${env:THADAM_NODE_ID}` (hashed as written; interpolation applies to string scalars, `configuration/index.md:66-81`). Whether `node_id` accepts interpolation was not run; step 1's test must show it.
- **Read-only observation:** `/healthz`, `/readyz`, `/metrics`, `/status`, as today.

Left to existing tools:

| Need | Tool |
|---|---|
| Metrics collection, dashboards, alerts (including "node not on desired hash": compare `thadam_build_info` to a desired-hash recording rule) | Prometheus / Grafana. Robots behind NAT or on intermittent links: the operator runs a scraping agent with `remote_write` beside Thadam; Thadam is unchanged. |
| Log shipping | journald, a collector sidecar (logs are a lossy bounded writer, `logging.md:7`) |
| Inventory, rollout order, canary (`serial: 1`) | Ansible or the container orchestrator |
| Config delivery and signing | git-sync, Ansible, signed commits |
| Secrets distribution | `${file:}` mounts, the host's secret mechanism |
| Binary and image updates | package or image pipeline; Thadam does not self-update |

## 5. Plan, open questions, and what was not checked

### Steps (model and size left to the maintainer)

| # | Step | Touches | Model / size |
|---|---|---|---|
| 1 | **Stamp the version.** `BuildContext` carries the config hash to the Parquet footer; `/status` fields; `thadam_build_info`; startup log record. Test: a real `Pipeline` run writes a Parquet file whose footer equals `/status.config_hash`; also that `node_id: ${env:…}` validates. Fix docs or confirm they are now true. | `thadam-core`, `thadam-segments` factory, `thadam-runtime` (`status.rs`, `observability.rs`, `pipeline.rs`), `metrics.md`, `admin-api.md` | |
| 2 | **Stronger pre-apply check.** `Pipeline::check` (the pre-connector half of `build`) and `thadam validate --against-data-dir`, with exit codes 2 and 3 as `run` gives them. | `pipeline.rs`, `apps/thadam-cli/src/validate.rs`, `validating-configs.md` | |
| 3 | **Apply script and docs.** `deploy/` shell script implementing the four apply steps and rollback; an operations page with the change table; a Prometheus rule example for hash drift. Test the script against the Docker image on one machine (the deployment page's honesty standard: say what was not run, for example under systemd). | `deploy/`, `website/docs/src/operations/` (site rules in `AGENTS.md` apply) | |
| 4 | **Meter `unmatched`.** Export the chain's `unmatched` count (`thadam_chain_events_total{outcome="unmatched"}` or a separate counter) so the remove-source row can be checked. Recommended before step 3 documents that procedure. **Done:** a separate counter, `thadam_chain_unmatched_total{sink}`, and `/status` `counters.unmatched`. The `outcome` label was not used because `thadam_chain_events_total` requires a `dataset` label, and an unmatched record has no dataset. | `thadam-runtime` observability, pinned metric list | |

### Deliberately left out

Thadam-side watcher or SIGHUP; any apply or write endpoint; HTTP(S) config pull; config history store or `last-good` copy in `data_dir`; a `version:` label field in the config; a per-metric `config_hash`/`node_id` label; fleet registration, heartbeat or inventory; signed configs; a `thadam apply` subcommand; hot reload.

### Open questions for the owner

1. Is "restart-only, script-driven, no apply endpoint" acceptable, including the capture gap per apply? If container-only nodes need self-service apply, the least-bad shape is a body-less `POST` that adopts the already-staged file, behind a separate write token; it is not designed here.
2. Treat the empty `thadam.config_hash` footer (and the unwired `thadam.enrichment_hash`) as a bug to fix now, since five documents claim otherwise?
3. Should enrichment-file content enter the version? The hash scope is frozen under `CONFIG_HASH_VERSION = 1`; widening it needs version 2 (`hash.rs:16`). Alternative: wire `enrichment_hash` into the footer only.
4. Is the `thadam_build_info` exception to the label rule in `metrics.md:3` acceptable?
5. Is shared `node_id: ${env:…}` the sanctioned way to get one hash per fleet, or should the hash exclude `node_id`?
6. Does any source need a gap-free apply (QoS 0, best-effort ROS 2)? If so, that is the ADR-0012 trigger "hot reload … becomes a requirement" and this design is not enough.
7. Should the `--against-data-dir` checks become the default for `thadam validate` (changes what CI users see), or stay behind the flag?

### What this note did not check

- No code was built or run; every "today" claim is from reading code at the commit above. In particular the empty Parquet footer was not confirmed by producing and opening a file.
- Whether `node_id` accepts `${env:}` at runtime; `pipeline.node_id` validation order (`semantic.rs:371`) was seen, not exercised.
- Whether removing a field or changing its type in a dataset is refused, tolerated or breaks downstream readers (ADR-0013 field-id rules were not re-read).
- That a removed sink's cursor and directory are never cleaned up (absence of code found by search, not by a test).
- Which `wal.*` keys are immutable once a WAL exists (only `node_id`/`machine_id` binding was checked).
- Whether `Pipeline::build`'s front half is free of side effects as the split assumes; the connector `validate()` implementations were not read.
- Restart gap length. The apply script does not exist, so its behaviour under systemd or Docker is untested.
- Connector crates for outbound calls other than their configured endpoints; `ARCHITECTURE.md` sections 14 to 16 were read for observability and security but not every connector's TLS or auth path.
- ADR-0011 claim wording was not re-checked against this note (no delivery claims are made).
