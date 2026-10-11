# ADR-0015: Recording and replay format

- **Status:** Draft
- **Date:** 2026-10-10
- **Deciders:** Project owner

## Context

D24 (ADR-0003) fixed that replay is a cursor rewind inside the WAL retention window and that MCAP is an export and import format only, never a recording tap. Two things were left open: what `thadam export --mcap` reads, and what an exported file contains. ARCHITECTURE.md section 20 places the command in M2 with the MCAP work, and ADR-0007 stage 2 ships it with the MCAP source.

### Forces

- **The WAL is the only recording.** It holds the raw source bytes plus protocol attributes (capture-first, D1). Anything exported must come from it, so a replay and an export of the same range see the same messages.
- **Robotics tooling reads MCAP.** Foxglove and the ROS 2 ecosystem expect channels with a message encoding and, for `cdr`, a `ros2msg` schema.
- **Schema text is not in the WAL.** The MCAP source records each message's schema name and encoding as attributes (`mcap.schema_name`, `mcap.schema_encoding`), not the schema text, so a WAL frame stays small. The exporter must get the text from somewhere else.
- **Mixed sources.** A pipeline can capture MQTT JSON, binary CAN or Modbus frames, and ROS 2 CDR side by side. Not every payload has a schema or an MCAP-standard encoding.
- **Nothing is dropped silently** (ARCHITECTURE.md section 6, gaps). WAL gaps reach readers as items, and an export is a reader.

## Decision

Per ADR-0000 this ADR is Draft; the text records the owner's 2026-10-10 decisions and the shipped implementation (`thadam_mcap::McapExporter`, `thadam export`).

### Input

- `thadam export <CONFIG> --mcap <OUT> --from <POINT> [--to <POINT>]` reads the WAL retention window. `POINT` takes the same forms as `thadam replay` (a WAL position, a duration before now, or an RFC 3339 time) and resolves the same way.
- WAL only. Exporting from Parquet datasets is a separate, later feature.

### Channels and encodings

- One MCAP channel per (Thadam source, topic). The topic is `mcap.topic`, else the generic `topic` attribute, else the source name. Each channel carries a `thadam.source` metadata entry.
- Original encodings are kept:
  - A message with `mcap.message_encoding` (MCAP-sourced data) keeps that encoding and its schema, named by `mcap.schema_name` and `mcap.schema_encoding`. ROS 2 CDR stays `cdr` with its `ros2msg` schema.
  - Other sources use a per-source default: `json` with no schema, or `application/octet-stream` with no schema.
- Schema text comes from a resolver supplied by the caller. The CLI reads the schema records of the MCAP files the configuration's MCAP sources point at, plus any extra files given on the command line. A schema it cannot resolve is written as schema id 0 and listed in a `thadam.unresolved_schemas` metadata record.
- Messages on one channel that disagree on encoding or schema fail the export rather than opening a second channel.

### Times and sequence

- `log_time` is the ingest time. `publish_time` is `mcap.publish_time` when present, else the ingest time. `sequence` is `mcap.sequence` when present, else a per-channel counter.

### Metadata records

- `thadam.gap`, one per WAL gap: `kind` (`corrupt`, `evicted`, `missing`), `epoch`, `first`, `last`.
- `thadam.provenance`, once per file: `thadam_version`, `wal_epoch`, `first_pos`, `last_pos`, `node_id`.
- `thadam.unresolved_schemas`, only when a schema was not resolved.

### File handling

- Chunked with the MCAP library's default compression and written with a summary section, so readers can seek.
- Written to a temporary file, flushed, fsynced, then renamed. An existing output is not overwritten without `--force`.

## Alternatives considered

- **An MCAP recording tap next to the WAL.** Rejected by D24: two recording mechanisms, double the writes, and a second format to keep crash-safe.
- **Export from Parquet datasets.** Loses the original bytes and encodings (datasets are decoded, transformed, and wide), so a re-import is not the same capture. Kept as a possible later feature with its own semantics.
- **Store schema text in every WAL frame, or once per WAL segment.** Makes exports self-contained, but costs WAL bytes on every frame or adds segment-level state that the frame format does not have (ADR-0004). Rejected while the resolver covers the MCAP-sourced case; revisit for live ROS 2, where no file holds the schema.
- **Re-encode everything as JSON with a JSON Schema.** Foxglove renders every channel, but CDR to JSON is lossy for some types and doubles the size. Rejected; the original bytes are the evidence.
- **A second channel when a topic's encoding changes.** Hides a real inconsistency in the capture. Rejected in favor of a clear error.

## Consequences

- An exported file is a faithful copy of the captured bytes for its range, with gaps and provenance visible to any MCAP reader.
- MCAP-sourced ROS 2 data round-trips through `thadam-mcap` with its schemas, as long as the original files or a schema file are available at export time.
- Raw channels (`application/octet-stream`) are preserved but not rendered by Foxglove.
- A live ROS 2 source (ADR-0007 stage 4) must decide how schema text becomes available to the exporter, since no file holds it.

## Evidence and validation required

- [x] Round-trip test: exported CDR, JSON, and raw channels read back with the `mcap` crate, including gaps and provenance (`crates/thadam-mcap/tests/export.rs`).
- [x] End-to-end CLI test: a real WAL with frames from two sources, exported and read back, including source filtering, overwrite refusal, a locked WAL, and an encoding conflict (`apps/thadam-cli/tests/export_cmd.rs`).
- [ ] Foxglove opens an export of the `ros2-mcap-to-parquet` example and renders the CDR channels.
- [ ] Export throughput on a 1 GiB retained window, on reference hardware. [OPEN: criterion]

## Revisit when

- The live ROS 2 source lands and schema text has to travel with captured data.
- Users need exports from Parquet datasets or from a fleet of nodes merged into one file.
- MCAP adds a standard encoding for raw binary payloads.

## References

- ARCHITECTURE.md sections 3 (D1, D24), 6, 20, 21 (M2-B).
- ADR-0003 (execution and process model), ADR-0004 (durable buffer), ADR-0007 (ROS 2 strategy).
- `docs/design/owner-decisions.md`; owner decisions of 2026-10-10 on export format.
- MCAP specification (mcap.dev), `mcap` crate 0.25.0.
