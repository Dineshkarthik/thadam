//! The transport-neutral CAN frame and the source of received frames.
//!
//! The receive loop consumes frames through [`FrameSource`], so decoding and
//! message mapping are testable on any OS with an in-memory source. The
//! Linux SocketCAN transport implements this trait behind
//! `#[cfg(target_os = "linux")]`.

use async_trait::async_trait;

use crate::error::CanError;

/// One received CAN or CAN FD data frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanFrameData {
    /// The CAN identifier (11-bit or 29-bit value).
    pub id: u32,
    /// Whether the identifier is extended (29-bit).
    pub extended: bool,
    /// Whether the frame is CAN FD.
    pub fd: bool,
    /// The kernel receive timestamp in microseconds since the epoch, when
    /// the transport provided one.
    pub timestamp_us: Option<i64>,
    /// The frame's data bytes (at most 8 classic, 64 FD).
    pub data: Vec<u8>,
}

impl CanFrameData {
    /// A classic frame with `id` and `data`, for tests.
    #[cfg(test)]
    pub(crate) fn classic(id: u32, data: &[u8]) -> Self {
        CanFrameData {
            id,
            extended: false,
            fd: false,
            timestamp_us: None,
            data: data.to_vec(),
        }
    }

    /// The padded uppercase hex form of the id used in topics and the
    /// `can.id` attribute: 3 digits for classic ids, 8 for extended.
    pub(crate) fn id_hex(&self) -> String {
        if self.extended {
            format!("{:08X}", self.id)
        } else {
            format!("{:03X}", self.id)
        }
    }
}

/// A source of received frames. `None` ends the stream: for a CAN bus that
/// never happens, but an exhausted in-memory source ends its test.
///
/// Implementations must not await anything the runtime drains: the receive
/// loop applies backpressure by dropping frames, never by stalling here.
#[async_trait]
pub(crate) trait FrameSource: Send + 'static {
    /// The next frame, an error from the transport, or `None` at end of
    /// stream.
    async fn next_frame(&mut self) -> Result<Option<CanFrameData>, CanError>;
}

/// A [`FrameSource`] fed through a channel, for tests on any OS.
#[cfg(test)]
pub(crate) struct ChannelFrames {
    pub rx: tokio::sync::mpsc::Receiver<CanFrameData>,
}

#[cfg(test)]
#[async_trait]
impl FrameSource for ChannelFrames {
    async fn next_frame(&mut self) -> Result<Option<CanFrameData>, CanError> {
        Ok(self.rx.recv().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_hex_pads_by_addressing_mode() {
        assert_eq!(CanFrameData::classic(0x7B2, &[1]).id_hex(), "7B2");
        let mut fd = CanFrameData::classic(0x1234, &[1]);
        fd.extended = true;
        assert_eq!(fd.id_hex(), "00001234");
        let mut edge = CanFrameData::classic(5, &[]);
        assert_eq!(edge.id_hex(), "005");
        edge.extended = true;
        edge.id = 0xABCD_EF00;
        assert_eq!(edge.id_hex(), "ABCDEF00");
    }
}
