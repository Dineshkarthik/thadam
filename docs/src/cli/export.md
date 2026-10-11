# `thadam export` {#thadam-export}

```text
Export a retained WAL range to one MCAP file

Usage: thadam export [OPTIONS] --mcap <OUT> --from <POINT> <PATH>

Arguments:
  <PATH>  The pipeline configuration file

Options:
      --mcap <OUT>                  The MCAP file to write. It must not exist unless `--force` is given
      --from <POINT>                The first point to export, inclusive: a WAL position, a duration before now (`-45m`, `-1h30m`), or an RFC 3339 time
      --to <POINT>                  The last point to export, inclusive; the durable end when absent. Takes the same forms as `--from`
      --source <ID>                 Export only messages from this source. Repeat to select several; all sources when absent
      --encoding <SOURCE=ENCODING>  The encoding of a source's messages that carry no `mcap.message_encoding`: `json`, or `raw` for `application/octet-stream`. Repeat once per source. The default is `json` when every dataset reading the source decodes JSON, and `raw` otherwise
      --schemas <FILE.mcap>         An MCAP file whose schemas supply schema text the WAL does not carry. Repeat for several files. Schemas of the configuration's `mcap` sources are read automatically
      --force                       Replace `<OUT>` if it exists
  -h, --help                        Print help
```

Exports the frames retained in the WAL between `--from` and `--to` to one indexed [MCAP](https://mcap.dev) file. The export reads the WAL only, never the Parquet segments, and never commits a WAL cursor, so production sinks and retention are untouched. It is the writer side of the MCAP round trip: a recording replayed by the [`mcap` source](../sources/ros2.md#mcap-file-source) can be captured, exported, and read back with the original topics, encodings, and schemas.

`--from` and `--to` take the same forms as [`thadam replay`](replay.md): a WAL position, a duration before now, or an RFC 3339 time. Time bounds resolve to whole frames, the first (for `--from`) or last (for `--to`) retained frame whose ingest time falls within the bound.

Like `thadam replay`, the export takes the data-directory lock and refuses to run while `thadam run` holds the directory. Use [`thadam inspect`](inspect.md) to read the WAL while a pipeline runs.

On success the command prints the output path, the exported positions, and the counts:

```text
output: /var/lib/thadam/exports/run.mcap
positions: 4..=9
messages: 6, channels: 3, gaps: 0, unresolved schemas: 0
```

## What the file contains

- **Channels.** One MCAP channel per `(source, topic)`, tagged with `thadam.source`. The topic is the message's `mcap.topic`, else its `topic`, else the source id.
- **Encodings and schemas.** A message from an `mcap` source keeps its `mcap.message_encoding`, and its schema is named by `mcap.schema_name` and `mcap.schema_encoding`. Other messages use the source's default encoding (see below) and have no schema.
- **Times.** The log time is the WAL ingest time. The publish time is `mcap.publish_time` when it parses, else the log time.
- **Gaps.** Positions the WAL no longer holds, or holds corrupt, become `thadam.gap` records with a kind of `evicted`, `corrupt`, or `missing`, clipped to the requested range.
- **Provenance.** A `thadam.provenance` record holds the Thadam version, the WAL epoch, the first and last exported positions, and the node id. The node id is the one recorded in the WAL, else the pipeline's `node_id`.

The positions in the provenance record are those of the frames that contributed messages, so a `--source` filter that matches nothing in the range gives an empty file with `positions: none exported`.

## Encodings

The default encoding of a source's messages that carry no `mcap.message_encoding` is `json` when at least one dataset reads that source and every such dataset decodes JSON, and `application/octet-stream` otherwise. `--encoding` overrides it per source:

```text
thadam export pipeline.yaml --mcap run.mcap --from -1h --encoding plc=raw --encoding bag=json
```

A topic whose messages disagree on message encoding or schema is an error, not a second channel: the export stops, removes the partial file, and names the source and topic.

## Schemas

The WAL does not hold schema text, only names and encodings. The export reads the schema records of the MCAP files that the configuration's `mcap` sources list in `paths` (directories are scanned one level deep for `*.mcap`, as the source does). A configured file that is missing or unreadable is skipped with a warning on stderr. `--schemas` adds further MCAP files; they are read first and win over a configured file that defines the same schema.

A channel whose schema text is not found is still written, with no schema, and is listed in a `thadam.unresolved_schemas` record and on stderr:

```text
warning: no schema for `sensor/Joint` (ros2msg) on source `bag` topic `/joint`; its channel is written without schema text
```

## Atomic writes

The file is written to `<OUT>.tmp`, flushed, synced, and then renamed to `<OUT>`. A failed export removes the temporary file. An export that is killed before the rename leaves `<OUT>.tmp` behind, which the next export overwrites. An existing `<OUT>` is never touched unless `--force` is given.

## Exit codes

Exit codes follow the [exit codes](../operations/exit-codes.md) table: 0 on success; 1 for a runtime failure, including a read or write error or a topic with conflicting encodings; 2 for invalid configuration or usage (a malformed `--from`, `--to`, or `--encoding`, a `--source` or `--encoding` that names no configured source, an existing `--mcap` without `--force`, or a `--schemas` file that cannot be read); and 3 when the data directory is locked by a running pipeline or cannot be read.
