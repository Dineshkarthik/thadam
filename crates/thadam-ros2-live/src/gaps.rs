//! Sequence-gap drop accounting, per publisher GID.
//!
//! rmw_zenoh attaches a per-publisher sequence number (starting at 1) to
//! every sample. A jump from `after` to `before` means `before - after - 1`
//! messages were lost between the publisher and this client — the router
//! egress drop counted under overload in spike S3 §6.4. The receive task
//! adds every reported gap into the running `ros2.dropped` total.
//!
//! Jump-only accounting is what makes upstream loss and client-side queue
//! overlap impossible: a message the client dropped itself was observed (its
//! sequence advanced the watermark), so it never produces a jump.

use std::collections::HashMap;

use crate::transport::{RawRos2Message, hex};

/// Reports missing sequence numbers across every publisher GID.
#[derive(Debug, Default)]
pub struct GapTracker {
    /// The highest sequence number observed so far, per publisher GID.
    last: HashMap<[u8; 16], i64>,
}

impl GapTracker {
    /// An empty tracker.
    pub fn new() -> Self {
        GapTracker::default()
    }

    /// Observes one message, returning a gap report when its sequence jumped
    /// past expected numbers. Samples without a well-formed envelope carry
    /// no sequence and are skipped. Duplicates and reorders (`seq <= last`)
    /// are ignored: replayed late-join samples would otherwise count as new
    /// loss.
    pub fn observe(&mut self, msg: &RawRos2Message) -> Option<GapEvent> {
        if !msg.well_formed_envelope() {
            return None;
        }
        let seq = msg.seq;
        match self.last.get(&msg.gid) {
            None => {
                // First sample from this publisher: the sequence need not
                // start at 1 mid-run.
                self.last.insert(msg.gid, seq);
                None
            }
            Some(&last) if seq > last + 1 => {
                self.last.insert(msg.gid, seq);
                Some(GapEvent {
                    topic: msg.topic.clone(),
                    gid: hex(&msg.gid),
                    after: last,
                    before: seq,
                    missing: (seq - last - 1) as u64,
                })
            }
            Some(&last) => {
                // Duplicate (`seq == last`), late-join replay, or reorder.
                if seq > last {
                    self.last.insert(msg.gid, seq);
                }
                None
            }
        }
    }
}

/// One detected sequence jump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapEvent {
    /// The topic the jump was seen on.
    pub topic: String,
    /// The publisher GID, lowercase hex.
    pub gid: String,
    /// The last sequence before the jump.
    pub after: i64,
    /// The first sequence after the jump.
    pub before: i64,
    /// How many messages the jump skipped.
    pub missing: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn msg(topic: &str, gid: u8, seq: i64) -> RawRos2Message {
        RawRos2Message {
            topic: topic.to_string(),
            type_name: "pkg/msg/T".to_string(),
            type_hash: "RIHS01_00".to_string(),
            payload: Bytes::new(),
            source_timestamp_ns: seq,
            seq,
            gid: [gid; 16],
            late_join: false,
        }
    }

    fn missing_total(tracker: &mut GapTracker, seqs: &[(u8, i64)]) -> u64 {
        seqs.iter()
            .map(|(gid, seq)| tracker.observe(&msg("/imu", *gid, *seq)))
            .map(|e| e.map_or(0, |e| e.missing))
            .sum()
    }

    #[test]
    fn dense_sequences_report_no_gaps() {
        let mut tracker = GapTracker::new();
        let seqs: Vec<(u8, i64)> = (1..=100).map(|s| (1u8, s)).collect();
        assert_eq!(missing_total(&mut tracker, &seqs), 0);
    }

    #[test]
    fn a_jump_counts_the_skipped_range() {
        let mut tracker = GapTracker::new();
        tracker.observe(&msg("/imu", 1, 10));
        let event = tracker.observe(&msg("/imu", 1, 14)).expect("a gap");
        assert_eq!(
            event,
            GapEvent {
                topic: "/imu".to_string(),
                gid: hex(&[1u8; 16]),
                after: 10,
                before: 14,
                missing: 3,
            }
        );
        assert!(tracker.observe(&msg("/imu", 1, 15)).is_none());
    }

    #[test]
    fn gaps_are_tracked_per_publisher_gid() {
        let mut tracker = GapTracker::new();
        // A second publishing GID starts its own sequence; no cross-talk.
        assert_eq!(missing_total(&mut tracker, &[(1, 5), (2, 1)]), 0);
        assert_eq!(missing_total(&mut tracker, &[(2, 4), (1, 8)]), 4);
    }

    #[test]
    fn duplicates_reorders_and_unsequenced_samples_count_nothing() {
        let mut tracker = GapTracker::new();
        tracker.observe(&msg("/imu", 1, 10));
        assert!(tracker.observe(&msg("/imu", 1, 10)).is_none(), "duplicate");
        assert!(tracker.observe(&msg("/imu", 1, 7)).is_none(), "reordered");
        assert!(
            tracker.observe(&msg("/imu", 1, -1)).is_none(),
            "no envelope"
        );
        // Late-join replays the original sequence; it must not count.
        tracker.observe(&msg("/tf_static", 2, 1));
        let mut replay = msg("/tf_static", 2, 1);
        replay.late_join = true;
        assert!(tracker.observe(&replay).is_none());
        // After a reorder, accounting resumes from the higher watermark.
        assert_eq!(
            tracker.observe(&msg("/imu", 1, 12)).expect("gap").missing,
            1
        );
    }
}
