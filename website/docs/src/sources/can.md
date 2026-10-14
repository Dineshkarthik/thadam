# CAN source

**Status: Available.** The `can` source receives CAN and CAN FD frames from one Linux SocketCAN interface and emits one message per frame. In `raw` mode the payload is the frame's data bytes. In `decoded` mode a DBC file describes the messages and the payload is a JSON object of decoded signal values.

Built as the `thadam-can` crate on `socketcan` 4 (`tokio` feature) and `can-dbc` 10 for DBC parsing.

**Linux only.** SocketCAN is a Linux kernel interface. The crate compiles on every platform, and `thadam validate` checks the configuration anywhere, but building the source on another operating system fails with a configuration error ("SocketCAN requires Linux"). Run pipelines with a `can` source on Linux, for example the edge device that is wired to the bus.

```yaml
sources:
  - id: powertrain
    kind: can
    config:
      interface: can0
      fd: false
      filters:
        - { id: "0x120", mask: "0x7F0" }
      dbc: /etc/thadam/powertrain.dbc
      emit: decoded
      unknown: raw
```

## Configuration

Unknown keys are rejected at every level.

| Key | Type | Default | Description |
|---|---|---|---|
| `interface` | string | — | **Required.** The SocketCAN interface, for example `can0` or `vcan0`. At most 15 characters of ASCII letters, digits, and `- _ . :`. It must exist and be up when the pipeline starts. |
| `fd` | bool | `false` | Open the socket for CAN FD. With `false` only classic CAN 2.0 frames are received. With `true` classic and FD frames are received; the interface itself must be set up for FD (`ip link set can0 type can bitrate ... dbitrate ... fd on`, and an MTU of 72 on `vcan`). |
| `filters` | list | `[]` | Kernel receive filters, see below. Empty accepts every frame. |
| `dbc` | path | — | DBC file with the message and signal definitions. **Required** with `emit: decoded`; rejected with `emit: raw`. Read once when the source is built; a missing or unparsable file is a configuration error. |
| `emit` | `raw` or `decoded` | `raw` | How each frame becomes a message. |
| `unknown` | `raw` or `drop` | `raw` | With `emit: decoded`, what to do with a frame whose id is not in the DBC. Ignored with `emit: raw`. |

### `filters`

A frame is accepted when `(frame_id & mask) == (id & mask)` and the frame's addressing mode matches `extended`. The kernel applies the filters, so rejected frames never reach Thadam. Several filters are OR-ed.

| Key | Type | Default | Description |
|---|---|---|---|
| `id` | integer or `"0x…"` string | — | **Required.** The identifier to match. |
| `mask` | integer or `"0x…"` string | — | **Required.** Bits of the identifier that must match. |
| `extended` | bool | `false` | `true` matches extended (29-bit) frames only, `false` matches standard (11-bit) frames only. |

`id` and `mask` must fit the addressing mode: at most `0x7FF` for standard and `0x1FFFFFFF` for extended filters. Write hex as a quoted string (`"0x12A"`) or as a plain integer (`298`).

## Raw mode

The payload is the frame's data bytes (up to 8 for classic frames, up to 64 for FD). Remote-transmission and error frames carry no data and are skipped.

| Attribute | Value |
|---|---|
| `can.id` | The identifier in uppercase hex without a prefix: 3 digits for standard ids (`12A`), 8 for extended (`00001234`). |
| `can.extended` | `true` for a 29-bit identifier, otherwise `false`. |
| `can.fd` | `true` for a CAN FD frame, otherwise `false`. |
| `can.dlc` | The number of data bytes, as a decimal. |
| `can.interface` | The configured `interface`. |
| `topic` | `can/<interface>/<id hex>`, for example `can/can0/12A`. Dataset `match.topic` filters can select on it. |
| `can.dropped` | Frames dropped so far because the pipeline was behind; see [Backpressure](#backpressure-and-delivery). Present only after the first drop. |

The message's ingest time is the kernel receive timestamp (`SO_TIMESTAMPNS`) when available, otherwise the time the frame was read.

## Decoded mode

With `emit: decoded` each frame whose id is in the DBC becomes a JSON object with one key per decoded signal plus `ts_us`:

```json
{"Rpm": 3000.0, "CoolantTemp": 60.0, "Throttle": 42, "ts_us": 1791628800123456}
```

`ts_us` is the frame's receive time in microseconds since the Unix epoch. The message carries the raw-mode attributes plus `can.message` (the DBC message name), and `topic` is `can/<interface>/<message name>`, for example `can/can0/EngineData`.

Signal decoding follows the DBC conventions:

- **Byte order.** Intel (`@1`, little-endian) signals start at their least significant bit. Motorola (`@0`, big-endian) signals start at their most significant bit, and continue toward the byte's LSB and then at the next byte's MSB.
- **Signedness.** Signed (`-`) signals are sign-extended from their bit width.
- **Scale and offset.** The value is `raw * factor + offset`. Signals with the identity factor and offset stay integers; otherwise the value is a float.
- **IEEE floats.** Signals declared with `SIG_VALTYPE_` as 32-bit or 64-bit floats are reinterpreted from their bits (their width must match). Non-finite results (`NaN`, infinities) become `null`.
- **Multiplexing.** A signal marked `m<N>` is decoded only when the multiplexer switch (`M`) of the same message has the raw value `N`; the switch itself and plain signals are always decoded. Extended multiplexing (`SG_MUL_VAL_`) is not interpreted: a signal that is both multiplexer and multiplexed (`m<N>M`) is treated as a multiplexed signal.
- **Short frames.** A signal whose bits extend past the frame's received data is omitted, so a short frame never decodes garbage. FD frames of up to 64 bytes decode signals anywhere in the payload.
- **Identifiers.** A DBC message is matched on identifier and addressing mode together: an extended DBC id (bit 31 set in the file) never matches a standard frame of the same number.

Signal names are the JSON keys. `ts_us` is reserved: a signal of that name is overwritten.

A frame whose id is not in the DBC is handled per `unknown`: `raw` emits it exactly as `emit: raw` would (payload bytes, `topic = can/<interface>/<id hex>`), `drop` discards it silently. Value descriptions (enumerations), units, and min/max are ignored.

## Backpressure and delivery

CAN has no flow control and no acknowledgement, so the source cannot slow the bus down and never waits for the pipeline. It sets `ack_mode: none` and delivers **at most once**.

A dedicated task drains the socket continuously and pushes messages into an in-memory queue of 4096 messages. When the pipeline is slow and the queue is full, further frames are **dropped and counted**. The count is the cumulative total since the source was built (it survives reopening the interface). It is exposed three ways:

- every message emitted after the first drop carries the running total in its `can.dropped` attribute;
- a warning is logged at the first drop and then every 10,000 drops;
- `unknown: drop` discards are *not* counted.

The queue holds the oldest frames, so after a stall the pipeline first sees the frames from before the drop and the `can.dropped` total shows up on the first message that fit afterwards. Liveness never depends on the pipeline: the socket is read at bus speed whatever the downstream does, so the kernel's receive buffer does not overflow because the pipeline is slow. A burst faster than the host can read the socket is still lost by the kernel, outside Thadam's view.

| Event | Result |
|---|---|
| Frame received and queued, committed before a crash | Recorded once. |
| Frame queued but not committed at a crash | Lost. CAN has no way to replay it. |
| Pipeline slower than the bus | Frames beyond the queue are dropped and counted in `can.dropped`. |

## Errors and retries

| Failure | Classification | Behavior |
|---|---|---|
| Interface missing or the socket cannot be opened or configured | Transient | The runtime retries opening with backoff. |
| Receive error while running (interface went down, bus-off) | Transient | Queued messages are delivered first, then the runtime closes and reopens the source with backoff. |
| Missing or invalid DBC, invalid filters, non-Linux build | Fatal at build | Reported by `thadam validate` (config) or when the pipeline starts (DBC file, platform). |

## Testing

Unit tests run on any operating system; decoding and the receive loop sit behind an internal `FrameSource` trait with an in-memory implementation. The end-to-end tests use a virtual CAN interface and are `#[ignore]`d because they need Linux with the `vcan` kernel module:

```sh
sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0
cargo test -p thadam-can --test vcan -- --include-ignored
```

Docker Desktop's Linux VM is built without CAN support, so these tests cannot run under it; use a Linux host or VM.
