//! The SocketCAN transport: open the interface, apply filters and kernel
//! timestamps, hand frames to the receive loop. Linux only.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use socketcan::tokio::{CanFdSocket, CanSocket};
use socketcan::{
    CanAnyFrame, CanDataFrame, CanFdFrame, CanFilter, CanFrame, EmbeddedFrame as _, Frame as _,
    SocketOptions,
};

use crate::config::{CanConfig, filter_words};
use crate::error::CanError;
use crate::frame::{CanFrameData, FrameSource};

/// A [`FrameSource`] backed by a SocketCAN interface.
///
/// Kernel receive timestamps (`SO_TIMESTAMPNS`) are requested on open. A
/// socket that refuses the option still receives; its frames carry no
/// timestamp and the receive loop stamps them on arrival instead. Remote and
/// error frames carry no data and are skipped.
pub(crate) struct SocketCanSource {
    socket: Socket,
    stamped: bool,
}

enum Socket {
    /// Classic CAN 2.0 (`fd: false`).
    Classic(CanSocket),
    /// CAN FD, which also receives classic frames (`fd: true`).
    Fd(CanFdSocket),
}

impl SocketCanSource {
    /// Opens `config.interface` and applies the configured filters.
    pub(crate) fn open(config: &CanConfig) -> Result<Self, CanError> {
        let filters: Vec<CanFilter> = config
            .filters
            .iter()
            .map(|&f| {
                let (id, mask) = filter_words(f);
                CanFilter::new(id, mask)
            })
            .collect();
        let connect = |detail: String| CanError::Connect {
            interface: config.interface.clone(),
            detail,
        };

        if config.fd {
            let socket =
                CanFdSocket::open(&config.interface).map_err(|e| connect(e.to_string()))?;
            let stamped = apply_options(&socket, &filters, &config.interface).map_err(connect)?;
            Ok(SocketCanSource {
                socket: Socket::Fd(socket),
                stamped,
            })
        } else {
            let socket = CanSocket::open(&config.interface).map_err(|e| connect(e.to_string()))?;
            let stamped = apply_options(&socket, &filters, &config.interface).map_err(connect)?;
            Ok(SocketCanSource {
                socket: Socket::Classic(socket),
                stamped,
            })
        }
    }

    /// Reads one frame. `None` is a remote or error frame.
    async fn read(&self) -> Result<Option<CanFrameData>, CanError> {
        let io = |e: std::io::Error| CanError::Transport(e.to_string());
        match &self.socket {
            Socket::Classic(socket) => {
                let (frame, stamp) = if self.stamped {
                    let (frame, stamp) = socket.read_frame_with_timestamp().await.map_err(io)?;
                    (frame, Some(stamp))
                } else {
                    (socket.read_frame().await.map_err(io)?, None)
                };
                Ok(match frame {
                    CanFrame::Data(frame) => Some(classic_frame(&frame, stamp)),
                    CanFrame::Remote(_) | CanFrame::Error(_) => None,
                })
            }
            Socket::Fd(socket) => {
                let (frame, stamp) = if self.stamped {
                    let (frame, stamp) = socket.read_frame_with_timestamp().await.map_err(io)?;
                    (frame, Some(stamp))
                } else {
                    (socket.read_frame().await.map_err(io)?, None)
                };
                Ok(match frame {
                    CanAnyFrame::Normal(frame) => Some(classic_frame(&frame, stamp)),
                    CanAnyFrame::Fd(frame) => Some(fd_frame(&frame, stamp)),
                    CanAnyFrame::Remote(_) | CanAnyFrame::Error(_) => None,
                })
            }
        }
    }
}

fn classic_frame(frame: &CanDataFrame, stamp: Option<SystemTime>) -> CanFrameData {
    CanFrameData {
        id: frame.raw_id(),
        extended: frame.is_extended(),
        fd: false,
        timestamp_us: stamp.map(micros_since_epoch),
        data: frame.data().to_vec(),
    }
}

fn fd_frame(frame: &CanFdFrame, stamp: Option<SystemTime>) -> CanFrameData {
    CanFrameData {
        id: frame.raw_id(),
        extended: frame.is_extended(),
        fd: true,
        timestamp_us: stamp.map(micros_since_epoch),
        data: frame.data().to_vec(),
    }
}

/// Applies the receive filters and requests kernel timestamps. Returns
/// whether timestamps are on; a refusal is only debug-level because the
/// socket still receives.
fn apply_options<S: SocketOptions>(
    socket: &S,
    filters: &[CanFilter],
    interface: &str,
) -> Result<bool, String> {
    if !filters.is_empty() {
        socket.set_filters(filters).map_err(|e| e.to_string())?;
    }
    match socket.set_recv_timestamp(true) {
        Ok(()) => Ok(true),
        Err(error) => {
            tracing::debug!(%error, interface, "kernel receive timestamps unavailable");
            Ok(false)
        }
    }
}

fn micros_since_epoch(stamp: SystemTime) -> i64 {
    stamp
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[async_trait]
impl FrameSource for SocketCanSource {
    async fn next_frame(&mut self) -> Result<Option<CanFrameData>, CanError> {
        loop {
            if let Some(frame) = self.read().await? {
                return Ok(Some(frame));
            }
        }
    }
}
