# Data directory layout

Everything Thadam must keep between restarts lives under [`pipeline.data_dir`](../configuration/pipeline.md). One data directory serves exactly one pipeline on one node.

```text
data_dir/
├── LOCK                 # flock held by the runtime for the whole run
├── thadam.json           # runtime metadata (pipeline, config hashes, format versions, WAL binding)
├── wal/                 # the capture WAL (thadam-wal)
│   ├── LOCK             # flock held while the WAL is open
│   ├── wal.json         # WAL identity: kind, format versions, epoch, node/machine binding
│   ├── clean_shutdown   # present only after a clean writer shutdown
│   ├── segments/        # {base:020}.seg — 64-byte header, then frames
│   └── cursors/         # <name>.cur — one durable cursor per consumer (sink.<id>)
├── sinks/<id>/          # state of each segment or batch sink
│   ├── staging/         # *.inprogress — open segment files (deleted on startup)
│   ├── ready/           # finalized, catalog-recorded segment files
│   └── catalog.log      # append-only segment catalog (CRC-checked)
├── quarantine/<sink>/   # each sink's quarantine instance (own budget); rejects.staging holds rejects until their segment is durable
├── schemas/             # the schema store: <sha-256 hash>.schema, content-addressed (see below)
├── sparkplug/births/<sink>/  # Sparkplug birth store: latest BIRTH per node/device, one directory per sink chain (see below)
├── sources/<id>/        # source state (connectors decide; an `mcap` source keeps resume.json, its per-file resume positions)
├── services/<id>/       # service state (e.g. the shipper)
└── replays/<id>/        # replay output — never the production directories
```

## `thadam.json`

Owned by the runtime; records everything needed to decide "is this my data?":

| Field | Meaning |
|---|---|
| `kind` | Always `thadam-data-dir`. |
| `format_version` | Runtime/data-directory layout version (current: 1). Newer is always refused. |
| `cursor_layout_version` | WAL-cursor usage version (current: 1). |
| `catalog_format_version` | The segment-catalog version this binary expects (current: 1). |
| `pipeline` | The pipeline name; a different name on restart is refused. |
| `config_hash`, `config_hash_version` | The last run's config hash and its algorithm version. |
| `previous_config_hash` | The hash before, when the configuration changed. |
| `wal` | A binding reference to `wal/wal.json`'s epoch (see below). |
| `created_unix`, `updated_unix` | Creation and last-write times. |

## `wal/wal.json`

Owned by the WAL; holds the WAL's own identity: its kind, the WAL/frame/cursor format versions, the **epoch**, and the `node_id`/`machine_id` binding.

Identity lives once, here — `thadam.json` only *references* the epoch. Reason: sink state (finalized segments, batch progress) stores bare WAL positions, which are meaningless across epochs. If `wal/` were replaced by a fresh one (new epoch, positions restarting at 1) while `sinks/` survived, resuming at old positions would silently skip new data. The runtime refuses that combination (exit 3) unless [`--repair`](crash-recovery.md#--repair) is given; likewise a directory copied to another machine is refused by the machine binding. The epoch also appears in Parquet file names and object keys, so files from the two epochs can never collide.

## The schema store (`schemas/`)

Content-addressed store of message schemas, owned by `thadam-core`'s `SchemaStore` and shared pipeline-wide. Sources whose messages reference stored schema text — today the MCAP and live ROS 2 sources for `ros2msg` channels — write each schema here the first time its channel is seen, *before the first message referencing it can be committed to the WAL*, and stamp the affected messages with the schema's hash (`mcap.schema_hash`). Chain decoders (dataset `decode: ros2`) and exports resolve the schema text by hash, so the WAL never has to carry schema text on every message.

```text
schemas/
├── <sha-256 hash>.schema    # one file per schema
└── .<sha-256 hash>.tmp-*    # in-flight writes; ignored by readers
```

Each `.schema` file is a small self-describing record (`thadam/schema-store 1`, then length-prefixed `name`, `encoding`, and `text` fields). The hash in the file name is SHA-256 over the name, encoding, and text — the name is hashed in because a `ros2msg` document does not name its main type. Writes publish atomically (write `tmp`, fsync, rename, fsync the directory) and are idempotent; readers re-hash the stored content and refuse a file that does not match the requested hash, so a damaged store fails loudly instead of decoding against the wrong schema. Deleting files only degrades new decodes of affected hashes to classified rejects.

## The Sparkplug birth store (`sparkplug/births/`)

Present when a dataset uses `decode: sparkplug_b`. Each sink's chain keeps `<sha-256>.birth` records, one per Sparkplug edge node or device: the latest BIRTH bytes, the session `bdSeq`, the BIRTH's `seq`, and a dead flag. Writes publish atomically (temporary file, fsync, rename, fsync the directory) before the message's events reach the sink, and reads verify a content digest. A restarted chain seeds its decoder from the directory so alias-only DATA resolves without a new BIRTH; `thadam replay` reads it and never writes it. Details: [Sparkplug B decoding](../sources/sparkplug-b.md#birth-store-and-replay).

## Format-status caution

WAL segment headers, `wal.json`, and cursor files carry format versions, and a binary refuses any version it does not know rather than guessing. The WAL format is **versioned but not frozen** until the v0.1.0 tag, which follows the ADR-0004 reference-hardware run ([details](../concepts/wal-and-replay.md#durability-and-fsync-policy)) — expect to re-initialize data directories across some beta upgrades.

## What is not here

- Process logs: stderr ([logging](logging.md)).
- Metrics: computed in memory, served on `/metrics` ([metrics](metrics.md)).
- Segment upload queue: on disk in `catalog.log`, not in memory.
- The backups story: there is none built in — a data directory is rebuilt by re-capturing from the source (within broker session limits) plus whatever segments were shipped to object storage.
