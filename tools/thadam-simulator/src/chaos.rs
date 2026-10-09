//! The chaos pass: applied to each generated message before it goes on the
//! wire, and always reflected in the oracle rows.
//!
//! Decision order (fixed, so the per-robot `chaos` RNG stream is
//! deterministic): missing timestamp, out-of-range value, out-of-order
//! delay, serialize, malformed truncation, duplicate scheduling. A malformed
//! message is never duplicated: its duplicate would also fail decode and
//! land in quarantine, not in dedup (the chain decodes before it dedups).

use std::time::Duration;

use crate::config::ChaosConfig;
use crate::generate::Generated;
use crate::rng::SimRng;
use crate::types::{Stream, flag};

/// What the chaos pass did to one message.
#[derive(Clone, Debug)]
pub struct ChaosOutcome {
    /// The exact bytes to publish.
    pub bytes: Vec<u8>,
    /// True when `bytes` are truncated JSON (the message quarantines).
    pub malformed: bool,
    /// Flags the chaos rules added (still delivered when not malformed).
    pub extra_flags: Vec<&'static str>,
    /// Extra publish delay (out-of-order injection).
    pub delay: Duration,
    /// When `Some(d)`, the same bytes are re-published after `d`
    /// (application-level duplicate; the pipeline's message-level dedup
    /// drops it).
    pub duplicate_after: Option<Duration>,
}

/// Applies the configured chaos rules to one generated message.
pub fn apply(
    chaos: &ChaosConfig,
    robot: &str,
    generated: &mut Generated,
    rng: &mut SimRng,
) -> ChaosOutcome {
    let stream = generated.stream;
    let mut extra_flags = Vec::new();

    if let Some(rule) = chaos
        .missing_ts
        .as_ref()
        .filter(|r| r.robot == robot && Stream::parse(&r.stream) == Some(stream))
        && rng.bernoulli(rule.rate)
    {
        strip_timestamps(generated);
        extra_flags.push(flag::MISSING_TIME);
    }

    if stream == Stream::Telemetry && rng.bernoulli(chaos.out_of_range_rate) {
        // 400 C in deci-degrees: past the 150 C validate ceiling.
        let idx = rng.range_u64(0, 6) as usize;
        if let Some(motor) = generated
            .payload
            .get_mut("motor_temp_dc")
            .and_then(|m| m.get_mut(idx))
        {
            *motor = serde_json::json!(4000.0);
            extra_flags.push(flag::OUT_OF_RANGE);
        }
    }

    let mut delay = Duration::ZERO;
    if let Some(rule) = chaos
        .out_of_order
        .as_ref()
        .filter(|r| r.robot == robot && Stream::parse(&r.stream) == Some(stream))
        && rng.bernoulli(rule.rate)
    {
        delay = Duration::from_millis(rule.delay_ms);
        extra_flags.push(flag::OUT_OF_ORDER);
    }

    let mut bytes = serde_json::to_vec(&generated.payload)
        .unwrap_or_else(|_| Vec::from(b"{\"v\":1,\"__error\":\"serialize\"}".as_slice()));
    let malformed = rng.bernoulli(chaos.malformed_rate);
    if malformed {
        let cut = (bytes.len() as f64 * rng.range(0.4, 0.9)).round() as usize;
        bytes.truncate(cut.max(1));
    }

    let duplicate_after = if !malformed && rng.bernoulli(chaos.duplicate_publish_rate) {
        Some(Duration::from_secs_f64(rng.range(
            chaos.duplicate_delay_seconds[0],
            chaos.duplicate_delay_seconds[1],
        )))
    } else {
        None
    };

    ChaosOutcome {
        bytes,
        malformed,
        extra_flags,
        delay,
        duplicate_after,
    }
}

/// Removes `ts_us` from the payload (message level, or per sample for
/// batched streams) and blanks the oracle sample timestamps to match what
/// went on the wire.
fn strip_timestamps(generated: &mut Generated) {
    match generated.payload.get_mut("samples") {
        Some(samples) => {
            if let Some(items) = samples.as_array_mut() {
                for item in items {
                    if let Some(obj) = item.as_object_mut() {
                        obj.remove("ts_us");
                    }
                }
            }
        }
        None => {
            if let Some(obj) = generated.payload.as_object_mut() {
                obj.remove("ts_us");
            }
        }
    }
    for (_, ts) in &mut generated.samples {
        *ts = None;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config::{ChaosConfig, MissingTsRule, OutOfOrderRule};
    use crate::generate::Generated;

    fn gen_msg(stream: Stream) -> Generated {
        Generated {
            stream,
            seq: 7,
            payload: json!({
                "v": 1, "seq": 7, "ts_us": 1234,
                "soc_pct": 80.0, "voltage_mv": 48000.0, "current_ma": 100.0,
                "cpu_temp_dc": 500.0, "motor_temp_dc": [400.0, 401.0, 402.0, 403.0, 404.0, 405.0, 406.0],
            }),
            samples: vec![(0, Some(1234))],
            state_fields: None,
            event_level: None,
            flags: Vec::new(),
        }
    }

    #[test]
    fn malformed_rate_one_truncates_bytes() {
        let mut rng = SimRng::new(42, "t/chaos");
        let chaos = ChaosConfig {
            malformed_rate: 1.0,
            duplicate_publish_rate: 0.0,
            out_of_range_rate: 0.0,
            clock_skew: None,
            missing_ts: None,
            out_of_order: None,
            ..ChaosConfig::default()
        };
        for _ in 0..50 {
            let mut g = gen_msg(Stream::Telemetry);
            let out = apply(&chaos, "robot-01", &mut g, &mut rng);
            assert!(out.malformed);
            assert!(serde_json::from_slice::<serde_json::Value>(&out.bytes).is_err());
            assert!(out.delay.is_zero());
            assert!(out.duplicate_after.is_none());
        }
    }

    #[test]
    fn duplicate_rate_one_schedules_an_identical_resend() {
        let mut rng = SimRng::new(42, "t/chaos");
        let chaos = ChaosConfig {
            malformed_rate: 0.0,
            duplicate_publish_rate: 1.0,
            duplicate_delay_seconds: [0.0, 3.0],
            out_of_range_rate: 0.0,
            clock_skew: None,
            missing_ts: None,
            out_of_order: None,
        };
        let mut g = gen_msg(Stream::Joints);
        let out = apply(&chaos, "robot-01", &mut g, &mut rng);
        let delay = out
            .duplicate_after
            .unwrap_or_else(|| panic!("duplicate scheduled"));
        assert!(delay <= Duration::from_secs(3));
        assert_eq!(
            out.bytes,
            serde_json::to_vec(&g.payload).ok().unwrap_or_default()
        );
    }

    #[test]
    fn out_of_range_rate_one_flags_telemetry_only() {
        let mut rng = SimRng::new(42, "t/chaos");
        let chaos = ChaosConfig {
            out_of_range_rate: 1.0,
            malformed_rate: 0.0,
            duplicate_publish_rate: 0.0,
            ..ChaosConfig::default()
        };
        let mut g = gen_msg(Stream::Telemetry);
        let out = apply(&chaos, "robot-01", &mut g, &mut rng);
        assert!(out.extra_flags.contains(&flag::OUT_OF_RANGE));
        let value: serde_json::Value = serde_json::from_slice(&out.bytes).ok().unwrap_or_default();
        let temps = value["motor_temp_dc"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert!(temps.iter().any(|t| t.as_f64() == Some(4000.0)));

        let mut g = gen_msg(Stream::Imu);
        let out = apply(&chaos, "robot-01", &mut g, &mut rng);
        assert!(!out.extra_flags.contains(&flag::OUT_OF_RANGE));
    }

    #[test]
    fn missing_ts_and_out_of_order_rules_are_robot_scoped() {
        let mut rng = SimRng::new(42, "t/chaos");
        let chaos = ChaosConfig {
            missing_ts: Some(MissingTsRule {
                robot: "robot-05".to_owned(),
                stream: "telemetry".to_owned(),
                rate: 1.0,
            }),
            out_of_order: Some(OutOfOrderRule {
                robot: "robot-03".to_owned(),
                stream: "pose".to_owned(),
                rate: 1.0,
                delay_ms: 500,
            }),
            malformed_rate: 0.0,
            duplicate_publish_rate: 0.0,
            out_of_range_rate: 0.0,
            clock_skew: None,
            duplicate_delay_seconds: [0.0, 3.0],
        };
        let mut g = gen_msg(Stream::Telemetry);
        let out = apply(&chaos, "robot-05", &mut g, &mut rng);
        assert!(out.extra_flags.contains(&flag::MISSING_TIME));
        let value: serde_json::Value = serde_json::from_slice(&out.bytes).ok().unwrap_or_default();
        assert!(value.get("ts_us").is_none());
        assert_eq!(g.samples, vec![(0, None)]);

        let mut g = gen_msg(Stream::Telemetry);
        let out = apply(&chaos, "robot-01", &mut g, &mut rng);
        assert!(!out.extra_flags.contains(&flag::MISSING_TIME));

        let mut g = gen_msg(Stream::Pose);
        let out = apply(&chaos, "robot-03", &mut g, &mut rng);
        assert_eq!(out.delay, Duration::from_millis(500));
        assert!(out.extra_flags.contains(&flag::OUT_OF_ORDER));
    }
}
