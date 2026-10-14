# Modbus source

**Status: Available.** The `modbus` source polls one Modbus TCP server on a fixed interval. Every poll reads each configured range, in order, and emits one message whose payload is a JSON object.

Built as the `thadam-modbus` crate on `tokio-modbus` (`default-features = false`, `tcp` feature only). Only Modbus TCP is supported; RTU and RTU-over-TCP are not.

```yaml
sources:
  - id: boiler-plc
    kind: modbus
    config:
      address: 192.0.2.10:502
      unit_id: 1
      interval: 1s
      timeout: 2s
      backoff_max: 1m
      reads:
        - name: supply_temp
          kind: input
          start: 0
          count: 2
          data_type: f32
        - name: setpoint
          kind: holding
          start: 100
          count: 2
          data_type: f32
          word_order: little
          scale: 0.1
        - name: pump_running
          kind: coil
          start: 0
          count: 1
          data_type: bool
```

## Configuration

Unknown keys are rejected at every level. Durations are whole seconds (`30`) or a unit string (`500ms`, `10s`, `5m`, `1h`). Limits are enforced by `thadam validate`.

| Key | Type | Default | Description |
|---|---|---|---|
| `address` | `host:port` | — | **Required.** The Modbus TCP server. The port must be non-zero. Bracketed IPv6 hosts are accepted (`[::1]:502`). |
| `unit_id` | integer 0–255 | `1` | Unit identifier sent in every request. |
| `interval` | duration | `10s` | Time between the starts of successful polls. Must be > 0. |
| `timeout` | duration | `5s` | Deadline for connecting and for each read. Must be > 0. |
| `backoff_max` | duration | `5m` | Longest wait after consecutive failures. Must be ≥ `interval`. |
| `reads` | list | — | **Required, non-empty.** Performed in order on every poll. |

### `reads`

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | string | — | **Required.** The JSON key for this read's value. Unique within the source. `ts_us` is reserved. |
| `kind` | `holding`, `input`, `coil`, or `discrete` | — | **Required.** Holding registers (function 03), input registers (04), coils (01), or discrete inputs (02). |
| `start` | integer 0–65535 | — | **Required.** First address (0-based). `start + count` must not pass 65536. |
| `count` | integer | — | **Required.** Registers or bits to read. At most 125 registers or 2000 bits per read. |
| `data_type` | see below | — | **Required.** How the registers are interpreted. |
| `word_order` | `big` or `little` | `big` | For 32-bit types: `big` puts the first register in the high 16 bits; `little` swaps them. |
| `scale` | number | none | Multiplier applied to numeric values. Makes the value a float. |
| `offset` | number | none | Added after scaling. Makes the value a float. |

| `data_type` | Registers | Kind | Value |
|---|---|---|---|
| `u16` | 1 per value | `holding`, `input` | Unsigned 16-bit integer. |
| `i16` | 1 per value | `holding`, `input` | Signed 16-bit integer. |
| `u32` | 2 per value | `holding`, `input` | Unsigned 32-bit integer. `count` must be even. |
| `i32` | 2 per value | `holding`, `input` | Signed 32-bit integer. `count` must be even. |
| `f32` | 2 per value | `holding`, `input` | IEEE 754 single precision. `count` must be even. |
| `raw` | 1 per element | `holding`, `input` | The registers as unsigned integers, one array element each. |
| `bool` | 1 per bit | `coil`, `discrete` | JSON `true` or `false`. |

`scale` and `offset` apply to `u16`, `i16`, `u32`, `i32`, and `f32`. They are rejected for `bool` and `raw`. A read with several values produces a JSON array. A read with one value produces a scalar, except `raw`, which is always an array.

## Payload format

Each poll produces one message:

```json
{"supply_temp": 71.5, "setpoint": 42.0, "pump_running": true, "ts_us": 1791628800123456}
```

- Each read's `name` maps to its value: a scalar, an array, or `null` (see [Exception handling](#exception-handling)).
- `ts_us` is the poll time in microseconds since the Unix epoch. It is written on every message.
- Non-finite floats (`NaN`, infinities) have no JSON form and become `null`.
- `f32` values use their shortest decimal form, so `3.14` stays `3.14`.

Because the payload is ordinary JSON, dataset fields and the JSON decode step in the transform chain read it like any other JSON source.

## Message attributes

| Attribute | Value |
|---|---|
| `modbus.address` | The configured `address`. |
| `modbus.unit_id` | The configured `unit_id`. |
| `topic` | `modbus/<unit_id>`. Dataset `match.topic` filters can select on it. |
| `modbus.exceptions` | Comma-separated names of reads the device refused. Present only when there is at least one. |

## Delivery semantics

Polling is **at-most-once per poll**. The source sets `ack_mode: none`: it never reports commit to the device, and nothing is redelivered.

| Event | Result |
|---|---|
| Poll succeeds and the message is committed | Recorded once. |
| Crash after a poll, before the message is committed | That poll's reading is not committed. The next poll reads the device again. |
| Poll fails | No message for that pass. The source reconnects and retries. |

Modbus has no way to replay a past register value, so a lost poll cannot be recovered. A reading that matters should be polled at a rate that tolerates losing one.

## Errors and retries

| Failure | Classification | Behavior |
|---|---|---|
| Connection refused, unreachable, or name not resolved | Transient | Drops the session and retries with jittered backoff. |
| Connect or read exceeds `timeout` | Transient | Drops the session and retries with jittered backoff. |
| Connection closed or I/O error mid-poll | Transient | Drops the session and retries with jittered backoff. |
| Malformed or mismatched Modbus response | Transient | Drops the session and retries with jittered backoff. |
| Modbus exception response for one read | Permanent for that read | Value is `null`; see [Exception handling](#exception-handling). The session stays up. |
| Invalid configuration | Fatal at build | Reported by `thadam validate`. |

Backoff starts at `interval` and doubles after each consecutive failure, capped at `backoff_max`. Each wait is jittered within the upper half of its window. A successful poll resets the backoff to `interval`.

### Exception handling

A Modbus exception response (for example `IllegalDataAddress` for an address the device does not have) refuses one read. It does not indicate a broken connection, so the source does not reconnect:

- The read's value is `null` in that poll's payload.
- The read's `name` is listed in `modbus.exceptions`.
- A warning is logged with the read name and exception code.
- Every other read in the same poll is still performed and decoded.

The message is still emitted, so consumers can tell a refused read (`null` plus the attribute) from a read that was never configured.

## Shutdown

On shutdown the poll task is stopped and the source closes its connection. A poll in progress is abandoned. The source does not wait for an in-flight read beyond the poll task's stop signal and a short grace period.
