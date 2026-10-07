//! Configuration of a WAL.

use std::time::Duration;

use thadam_core::WalPos;

use crate::cursor::validate_name;
use crate::error::WalError;
use crate::format::{FRAME_HDR, SEG_HDR};

/// When the writer flushes to stable storage. Tickets resolve after the flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fsync {
    /// Flush after every group commit.
    Always,
    /// Write every group immediately, flush at most once per interval.
    Interval(Duration),
    /// Never flush during normal operation; tickets resolve after the write.
    /// Survives a process crash, not a power loss.
    Never,
}

impl Fsync {
    /// The provisional default, `interval(10ms)` (ADR-0004, D3). To be
    /// re-measured on reference eMMC before the format is frozen.
    pub const DEFAULT: Fsync = Fsync::Interval(Duration::from_millis(10));
}

impl Default for Fsync {
    fn default() -> Self {
        Fsync::DEFAULT
    }
}

/// What happens when the WAL disk budget is exhausted. There is deliberately no
/// default (owner decision B3, D-B3-1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnFull {
    /// Withhold commit tickets (and so acknowledgements) until consumers free
    /// space. Moves loss risk to the upstream broker.
    Block,
    /// Evict the oldest segments, even ones required consumers have not read.
    /// Readers see [`Gap::Evicted`](crate::Gap::Evicted) and the evicted bytes
    /// are counted.
    DropOldest,
}

/// How far a required consumer may fall behind before it stops pinning
/// retention. Exceeding it is an explicit, counted loss for that consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaxLag {
    /// WAL bytes between the consumer's position and the head.
    Bytes(u64),
    /// Monotonic age of the segment holding the consumer's next frame.
    Age(Duration),
}

/// A consumer that participates in retention decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerSpec {
    /// The cursor name.
    pub name: String,
    /// Whether segments are kept until this consumer has passed them.
    pub required: bool,
    /// Optional lag limit for a required consumer.
    pub max_lag: Option<MaxLag>,
}

impl ConsumerSpec {
    /// A required consumer with no lag limit.
    pub fn required(name: impl Into<String>) -> Self {
        ConsumerSpec {
            name: name.into(),
            required: true,
            max_lag: None,
        }
    }

    /// A consumer that never pins retention.
    pub fn optional(name: impl Into<String>) -> Self {
        ConsumerSpec {
            name: name.into(),
            required: false,
            max_lag: None,
        }
    }

    /// Sets the lag limit.
    #[must_use]
    pub fn with_max_lag(mut self, max_lag: MaxLag) -> Self {
        self.max_lag = Some(max_lag);
        self
    }
}

/// Where a reader without a committed cursor starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartFrom {
    /// The oldest retained frame.
    Earliest,
    /// The first frame after the current durable end.
    Latest,
    /// A specific position.
    At(WalPos),
}

/// WAL configuration.
///
/// The WAL disk limit (`max_disk`) is the [`thadam_core::BudgetCategory::Wal`]
/// limit of the [`thadam_core::DiskBudget`] passed to [`crate::open`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WalOptions {
    /// Flush policy. Default `interval(10ms)`.
    pub fsync: Fsync,
    /// Policy when the disk budget is exhausted. No default.
    pub on_full: OnFull,
    /// Segment size. Default 64 MiB (use 16 MiB on the edge profile).
    pub segment_bytes: u64,
    /// Largest encoded frame payload. Default 4 MiB.
    pub frame_max_bytes: usize,
    /// Group commit size. Default 4 MiB.
    pub group_max_bytes: usize,
    /// Byte bound on submitted but unwritten batches. Default 8 MiB.
    pub max_pending_bytes: usize,
    /// Count bound on submitted but unwritten batches. Default 256.
    pub max_pending_batches: usize,
    /// Consumed segments are kept at least this long (monotonic time since the
    /// segment was sealed), unless the disk budget needs the space.
    pub retain_for: Duration,
    /// Consumers that take part in retention.
    pub consumers: Vec<ConsumerSpec>,
    /// Preallocate segments with `fallocate`. Linux only; ignored elsewhere
    /// (S1: preallocation on APFS doubled the bytes written).
    pub preallocate: bool,
    /// Recorded when the WAL is created; a later open with a different value is
    /// refused. Detects a data directory copied to another machine.
    pub machine_id: Option<String>,
    /// Recorded when the WAL is created; a later open with a different value is
    /// refused.
    pub node_id: Option<String>,
    /// Fault injection.
    #[cfg(any(test, feature = "failpoints"))]
    pub failpoints: crate::failpoints::Failpoints,
}

impl WalOptions {
    /// Defaults for everything except `on_full`, which has none.
    pub fn new(on_full: OnFull) -> Self {
        WalOptions {
            fsync: Fsync::DEFAULT,
            on_full,
            segment_bytes: 64 << 20,
            frame_max_bytes: 4 << 20,
            group_max_bytes: 4 << 20,
            max_pending_bytes: 8 << 20,
            max_pending_batches: 256,
            retain_for: Duration::ZERO,
            consumers: Vec::new(),
            preallocate: false,
            machine_id: None,
            node_id: None,
            #[cfg(any(test, feature = "failpoints"))]
            failpoints: crate::failpoints::Failpoints::default(),
        }
    }

    pub(crate) fn validate(&self, wal_limit: Option<u64>) -> Result<(), WalError> {
        let bad = |m: String| Err(WalError::InvalidOptions(m));
        if self.segment_bytes < 4096 {
            return bad(format!(
                "segment_bytes {} is below 4096",
                self.segment_bytes
            ));
        }
        if self.frame_max_bytes == 0 || self.frame_max_bytes > u32::MAX as usize {
            return bad("frame_max_bytes must be between 1 and u32::MAX".into());
        }
        if (self.frame_max_bytes + FRAME_HDR) as u64 + SEG_HDR > self.segment_bytes {
            return bad(format!(
                "frame_max_bytes {} plus headers does not fit a {}-byte segment",
                self.frame_max_bytes, self.segment_bytes
            ));
        }
        if self.max_pending_bytes == 0 || self.max_pending_bytes > u32::MAX as usize {
            return bad("max_pending_bytes must be between 1 and u32::MAX".into());
        }
        if self.max_pending_batches == 0 || self.max_pending_batches > u32::MAX as usize {
            return bad("max_pending_batches must be between 1 and u32::MAX".into());
        }
        if self.group_max_bytes == 0 {
            return bad("group_max_bytes must be positive".into());
        }
        if let Fsync::Interval(d) = self.fsync
            && d.is_zero()
        {
            return bad("fsync interval must be positive (use Fsync::Always)".into());
        }
        if let Some(limit) = wal_limit
            && limit < 2 * self.segment_bytes
        {
            return bad(format!(
                "the wal disk budget ({limit} bytes) must hold at least two segments of {} bytes",
                self.segment_bytes
            ));
        }
        for (i, c) in self.consumers.iter().enumerate() {
            validate_name(&c.name)?;
            if self.consumers[..i].iter().any(|o| o.name == c.name) {
                return bad(format!("consumer {} is listed twice", c.name));
            }
        }
        Ok(())
    }

    pub(crate) fn preallocate_effective(&self) -> bool {
        self.preallocate && (cfg!(target_os = "linux") || cfg!(test))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_adr_0004() {
        let o = WalOptions::new(OnFull::Block);
        assert_eq!(o.fsync, Fsync::Interval(Duration::from_millis(10)));
        assert_eq!(o.segment_bytes, 64 << 20);
        assert_eq!(o.frame_max_bytes, 4 << 20);
        assert_eq!(o.group_max_bytes, 4 << 20);
        assert!(o.validate(None).is_ok());
    }

    #[test]
    fn inconsistent_options_are_rejected() {
        let mut o = WalOptions::new(OnFull::Block);
        o.segment_bytes = 1 << 20;
        assert!(o.validate(None).is_err(), "frame_max does not fit");
        o.frame_max_bytes = 1000;
        assert!(o.validate(None).is_ok());
        assert!(
            o.validate(Some((1 << 20) + 1)).is_err(),
            "budget below two segments"
        );
        o.consumers = vec![ConsumerSpec::required("a"), ConsumerSpec::optional("a")];
        assert!(o.validate(None).is_err());
        o.consumers = vec![ConsumerSpec::required("../x")];
        assert!(o.validate(None).is_err());
        o.consumers.clear();
        o.fsync = Fsync::Interval(Duration::ZERO);
        assert!(o.validate(None).is_err());
    }
}
