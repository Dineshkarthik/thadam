//! SocketCAN source (`kind: can`). **Linux only.**
//!
//! The source receives CAN and CAN FD frames from one SocketCAN interface
//! and emits one message per frame. In `raw` mode the payload is the frame's
//! data bytes; in `decoded` mode a DBC file describes each known message and
//! the payload is a JSON object with the decoded signal values plus `ts_us`.
//!
//! CAN has no flow control and no acknowledgements. Delivery is at-most-once
//! (`ack_mode: none`): while the runtime is applying backpressure the socket
//! task drops the frames that do not fit its queue, counts them, and stamps
//! every emitted message with the running total in `can.dropped`.
//!
//! The SocketCAN transport is gated behind `#[cfg(target_os = "linux")]`.
//! Frame decoding is transport-agnostic and runs behind an internal
//! `FrameSource` trait, so it is unit-testable with an in-memory source on
//! any OS. On other operating systems [`CanSourceFactory`](crate::CanSourceFactory)'s
//! `build` returns a Config error explaining that SocketCAN requires Linux.

mod config;
mod dbc;
mod error;
mod factory;
mod frame;
mod recv;
mod source;
#[cfg(target_os = "linux")]
mod transport;

pub use config::{CanConfig, Emitter, IdFilter, Unknown, config_schema};
pub use error::CanError;
pub use factory::CanSourceFactory;
pub use source::CanSource;
