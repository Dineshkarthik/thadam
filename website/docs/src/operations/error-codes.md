# Error code reference

Every error Thadam surfaces — in logs, `/status`, quarantine rows, diagnostics — carries one `ThadamError` shape: a **domain** and a stable snake_case **code** written as `domain.code`, a human message (never payloads or secrets), a **retry class**, a **severity**, and structured context.

- **Retry classes:** `Transient` (retry with capped, jittered backoff; component `Degraded`), `Permanent` (retrying cannot help; per-event errors become rejects), `Fatal` (component `Failed`, readiness false, capture continues within policy).
- **Severity** defaults from the retry class: Warning, Error, Critical.
- Codes are stable: never reused for a different meaning, only added. The `thadam-core` codes are snapshot-tested; the Iceberg codes are defined in `thadam-iceberg`.

## Domains and codes

### `connector` — sources and protocol connections

| Code | Meaning |
|---|---|
| `connector.connect_failed` | Opening the connection failed. |
| `connector.disconnected` | An established connection dropped. |
| `connector.protocol` | The peer violated the protocol. |
| `connector.timeout` | A protocol operation timed out. |
| `connector.session_lost` | The broker lost the session, so unacknowledged messages were redelivered. |
| `connector.stale_ack` | An acknowledgement belonged to an older connection epoch and was dropped. |
| `connector.message_too_large` | A message exceeded the configured size limit. |
| `connector.panic` | The connector task panicked. |
| `connector.cancelled` | The operation was cancelled by shutdown. |

### `decode` — payload decoding

| Code | Meaning |
|---|---|
| `decode.invalid_payload` | The payload could not be parsed. |
| `decode.invalid_utf8` | The payload is not valid UTF-8 where text is required. |
| `decode.unsupported_encoding` | The payload uses an encoding with no decoder. |
| `decode.too_large` | The payload exceeds the size limit. |
| `decode.depth_exceeded` | The payload nests deeper than the limit. |
| `decode.invalid_time` | A timestamp could not be parsed. |
| `decode.schema_ref_missing` | The message does not carry the schema reference the decoder needs. |
| `decode.unknown_schema` | The referenced schema hash is not in the schema store. |

### `schema` — declared schemas and shapes

| Code | Meaning |
|---|---|
| `schema.type_mismatch` | A value does not match its declared field type. |
| `schema.missing_field` | A required field is missing. |
| `schema.shape_limit` | The shape registry reached `max_shapes`. |
| `schema.narrowing` | A conversion would narrow a type. |
| `schema.unsupported_type` | A declared type is not supported. |
| `schema.invalid_shape` | A shape definition is invalid. |

### `transform` — transform steps

| Code | Meaning |
|---|---|
| `transform.failed` | A transform step failed on an event. |
| `transform.unknown_unit` | A unit conversion is not defined (unknown UCUM code or incompatible dimensions). |
| `transform.eval_failed` | A filter predicate could not be evaluated. |
| `transform.panic` | A transform step panicked. |

### `quality` — data-quality rules

| Code | Meaning |
|---|---|
| `quality.rejected` | The event failed a quality rule and was rejected. |
| `quality.out_of_range` | A value is outside its declared range. |

### `buffer` — the capture WAL and disk budget

| Code | Meaning |
|---|---|
| `buffer.budget_exhausted` | The disk budget is exhausted. |
| `buffer.corrupt` | A frame or segment failed its checksum. |
| `buffer.fsync_failed` | `fsync` failed; the segment can no longer be trusted. |
| `buffer.incompatible` | The on-disk format is newer or otherwise unreadable (exit code 3). |
| `buffer.io` | A filesystem operation failed. |
| `buffer.evicted` | Data was evicted under `drop_oldest`. |

### `sink` — sinks and shipping

| Code | Meaning |
|---|---|
| `sink.write_failed` | Writing a batch failed. |
| `sink.timeout` | The sink target did not answer in time. |
| `sink.unavailable` | The sink target is unavailable. |
| `sink.rejected` | The sink rejected an event. |
| `sink.recover_failed` | Recovery after a crash failed. |
| `sink.io` | A local I/O operation failed. |
| `sink.non_monotonic_append` | A segment sink was given a batch not after its previous one. |
| `sink.object_key_collision` | A create-if-absent conflict found an object with different content (epoch or node-id misconfiguration); the shipper refuses to overwrite. Fatal. |
| `sink.iceberg_unsupported_type` | A segment's footer has a type Iceberg cannot represent, such as `UInt64` under `unsigned: native`. Fatal. |
| `sink.iceberg_schema_mismatch` | The table's current schema cannot accept the segment's schema. Fatal. |
| `sink.iceberg_commit` | A commit kept conflicting with another writer past the bounded retry; retried on a later tick. Transient. |
| `sink.panic` | The sink task panicked. |

### `auth` — authentication and authorization

| Code | Meaning |
|---|---|
| `auth.failed` | Credentials were rejected. |
| `auth.expired` | Credentials expired. |
| `auth.forbidden` | The credentials lack permission. |

### `config` — configuration

| Code | Meaning |
|---|---|
| `config.invalid` | A configuration value is invalid (exit code 2). |
| `config.missing` | A required configuration value is missing. |
| `config.unknown_kind` | No connector is registered for the configured `kind`. |
| `config.incompatible` | A configuration or data-directory version is not supported (exit code 3 on the data-directory path; also raised for a non-loopback admin bind without a token). |
| `config.iceberg_table_missing` | The Iceberg table does not exist and `create_table` is false. Fatal. |

## Where codes show up

- `thadam validate` diagnostics: `error[config.invalid]: …` with path, line, and hint.
- `/status` component `last_error` objects (`{ "domain", "code", "message" }`).
- Quarantine rows (`error_domain`, `error_code`, `reason`) with the raw bytes retained.
- Reject metrics: `thadam_rejects_total{sink, domain}` aggregates per domain — per-code detail stays in logs and quarantine to keep label cardinality bounded.
