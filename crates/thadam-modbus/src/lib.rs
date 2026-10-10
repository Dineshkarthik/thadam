//! Modbus TCP polling source (`kind: modbus`).
//!
//! The source connects to one Modbus TCP server and, every `interval`, reads
//! each configured range in order. One poll produces ONE raw message whose
//! payload is a JSON object: each read's name maps to its value (a scalar or an
//! array), and `ts_us` holds the poll time in microseconds since the epoch.
//!
//! Delivery is at-most-once per poll. The source never acknowledges, so a poll
//! that was not committed before a crash is gone; the next poll reads the
//! device again.
//!
//! A read the device refuses with a Modbus exception (for example an illegal
//! data address) does not end the session. Its value is `null`, its name is
//! listed in the message's `modbus.exceptions` attribute, and a warning is
//! logged. Connection failures, timeouts, and protocol errors drop the session
//! and reconnect with jittered exponential backoff.

mod config;
mod decode;
mod error;
mod factory;
mod poll;
mod source;

pub use config::{
    DataType, DurationSpec, ModbusConfig, ReadKind, ReadSpec, WordOrder, config_schema,
};
pub use error::ModbusError;
pub use factory::ModbusSourceFactory;
pub use source::ModbusSource;
