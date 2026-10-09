//! The per-robot stream vocabulary and the MQTT topic layout.

use crate::DEFAULT_TOPIC_PREFIX;

/// One data stream a robot publishes (plan section 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Stream {
    /// 10 Hz, one sample per message: battery, temperatures, rails.
    Telemetry,
    /// 100 Hz in batches: 7-DOF position/velocity/effort.
    Joints,
    /// 200 Hz in batches: accelerometer, gyro, quaternion.
    Imu,
    /// 50 Hz in batches: map-frame pose.
    Pose,
    /// On change plus heartbeat: operating mode and cycle.
    State,
    /// Poisson: cycle markers, faults, alarms, debug chatter.
    Events,
}

impl Stream {
    /// Every stream, in topic order.
    pub const ALL: [Stream; 6] = [
        Stream::Telemetry,
        Stream::Joints,
        Stream::Imu,
        Stream::Pose,
        Stream::State,
        Stream::Events,
    ];

    /// The topic level spelling, also the Thadam dataset id for this demo.
    pub const fn as_str(self) -> &'static str {
        match self {
            Stream::Telemetry => "telemetry",
            Stream::Joints => "joints",
            Stream::Imu => "imu",
            Stream::Pose => "pose",
            Stream::State => "state",
            Stream::Events => "events",
        }
    }

    /// Parses a topic-level spelling.
    pub fn parse(text: &str) -> Option<Stream> {
        Stream::ALL.iter().copied().find(|s| s.as_str() == text)
    }
}

impl std::fmt::Display for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Builds the publish topic: `{prefix}/{site}/{robot}/{stream}`
/// (plan section 4: five levels, site at index 2, robot at index 3).
pub fn topic(prefix: &str, site: &str, robot: &str, stream: Stream) -> String {
    debug_assert!(!prefix.is_empty(), "topic prefix must not be empty");
    format!("{prefix}/{site}/{robot}/{}", stream.as_str())
}

/// Extracts `(... site, robot, stream)` from a topic built by [`topic`]
/// under `prefix`. Returns `None` when the shape does not match.
pub fn parse_topic<'a>(prefix: &str, full: &'a str) -> Option<(&'a str, &'a str, Stream)> {
    let rest = full.strip_prefix(prefix)?.strip_prefix('/')?;
    let mut levels = rest.split('/');
    let site = levels.next()?;
    let robot = levels.next()?;
    let stream = Stream::parse(levels.next()?)?;
    if levels.next().is_some() || site.is_empty() || robot.is_empty() {
        return None;
    }
    Some((site, robot, stream))
}

/// The quality-flag spellings recorded in oracle rows. These mirror the
/// pipeline's quality flags so level-3 verification can join on them
/// (plan section 5.2); expect `OUT_OF_RANGE` and friends upstream, snake
/// case here.
pub mod flag {
    /// A value exceeded its `validate` rule range (still delivered, flagged).
    pub const OUT_OF_RANGE: &str = "out_of_range";
    /// The device clock is offset beyond `max_skew` (still delivered).
    pub const CLOCK_SKEW: &str = "clock_skew";
    /// The device sent no timestamp (delivered with a null event time).
    pub const MISSING_TIME: &str = "missing_time";
    /// The publish was reordered deliberately (still delivered).
    pub const OUT_OF_ORDER: &str = "out_of_order";
}

/// Default prefix constant re-exported for callers that hardcode the demo.
pub const DEMO_PREFIX: &str = DEFAULT_TOPIC_PREFIX;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_layout_has_five_levels_with_identity_in_place() {
        let full = topic("thadam-demo/v1", "site-a", "robot-01", Stream::Telemetry);
        let levels: Vec<&str> = full.split('/').collect();
        assert_eq!(
            levels,
            ["thadam-demo", "v1", "site-a", "robot-01", "telemetry"]
        );
        let (site, robot, stream) =
            parse_topic("thadam-demo/v1", &full).unwrap_or_else(|| panic!("topic round trip"));
        assert_eq!(
            (site, robot, stream),
            ("site-a", "robot-01", Stream::Telemetry)
        );
    }

    #[test]
    fn stream_names_match_the_pipeline_datasets() {
        // `examples/edge-capture/thadam/pipeline*.yaml` defines one dataset per
        // stream with the same id; keep the spellings glued together.
        let names: Vec<&str> = Stream::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            names,
            ["telemetry", "joints", "imu", "pose", "state", "events"]
        );
        for stream in Stream::ALL {
            assert_eq!(Stream::parse(stream.as_str()), Some(stream));
        }
        assert_eq!(Stream::parse("lidar"), None);
    }

    #[test]
    fn parse_topic_rejects_wrong_shapes() {
        assert!(parse_topic("thadam-demo/v1", "other/v1/a/b/telemetry").is_none());
        assert!(parse_topic("thadam-demo/v1", "thadam-demo/v1/a/b").is_none());
        assert!(parse_topic("thadam-demo/v1", "thadam-demo/v1/a/b/c/d").is_none());
        assert!(parse_topic("thadam-demo/v1", "thadam-demo/v1//b/telemetry").is_none());
        assert!(parse_topic("thadam-demo/v1", "thadam-demo/v1/a/b/lidar").is_none());
    }
}
