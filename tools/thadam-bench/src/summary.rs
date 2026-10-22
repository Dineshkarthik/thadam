//! Turns sampled scrapes into the numbers in the result file. Counters and
//! histograms are differenced per `thadam` instance, so a restart resets
//! nothing: the window of each instance is summed, and downtime is excluded.

use crate::metrics::{
    Series, counter_sum, delta_buckets, histogram_buckets, histogram_exported, quantile,
};
use crate::process::PsSample;
use crate::result::{Histogram, Resources, Throughput};

/// Counter name for source messages (label `source`).
pub const SOURCE_MESSAGES: &str = "thadam_source_messages_total";
/// Counter name for source bytes (label `source`).
pub const SOURCE_BYTES: &str = "thadam_source_bytes_total";
/// Histogram base for source ack latency.
pub const SOURCE_ACK: &str = "thadam_source_ack_latency_seconds";
/// Histogram base for WAL commit latency.
pub const WAL_COMMIT: &str = "thadam_wal_commit_seconds";
/// Histogram base for sink batch writes (label `sink`).
pub const SINK_BATCH_WRITE: &str = "thadam_batch_write_seconds";

/// One successful `/metrics` read.
#[derive(Debug, Clone)]
pub struct Scrape {
    /// Seconds since the run started.
    pub at_s: f64,
    /// Every series in the exposition.
    pub series: Vec<Series>,
}

/// The scrapes from one `thadam run` process, in time order.
#[derive(Debug, Clone, Default)]
pub struct Instance {
    /// The scrapes taken while this process was up.
    pub scrapes: Vec<Scrape>,
}

impl Instance {
    fn endpoints(&self) -> Option<(&Scrape, &Scrape)> {
        let first = self.scrapes.first()?;
        let last = self.scrapes.last()?;
        (self.scrapes.len() >= 2).then_some((first, last))
    }
}

/// Ingest rates over every instance's window, or `None` when no instance has
/// two scrapes or the source counters were never exported.
#[must_use]
pub fn throughput(instances: &[Instance]) -> Option<Throughput> {
    let exported = instances
        .iter()
        .flat_map(|i| &i.scrapes)
        .any(|s| counter_sum(&s.series, SOURCE_MESSAGES).is_some());
    if !exported {
        return None;
    }
    let mut window_s = 0.0;
    let mut messages = 0.0;
    let mut bytes = 0.0;
    for instance in instances {
        let Some((first, last)) = instance.endpoints() else {
            continue;
        };
        window_s += last.at_s - first.at_s;
        messages += counter_delta(first, last, SOURCE_MESSAGES);
        bytes += counter_delta(first, last, SOURCE_BYTES);
    }
    if window_s <= 0.0 {
        return None;
    }
    Some(Throughput {
        window_s,
        messages,
        bytes,
        source_messages_per_s: messages / window_s,
        mb_per_s: bytes / window_s / 1_000_000.0,
    })
}

fn counter_delta(first: &Scrape, last: &Scrape, name: &str) -> f64 {
    let end = counter_sum(&last.series, name).unwrap_or(0.0);
    let start = counter_sum(&first.series, name).unwrap_or(0.0);
    (end - start).max(0.0)
}

/// Percentiles of histogram `base` over every instance's window, in
/// milliseconds. Buckets are differenced per instance, then merged by bound.
#[must_use]
pub fn histogram(instances: &[Instance], base: &str) -> Histogram {
    let exported = instances
        .iter()
        .flat_map(|i| &i.scrapes)
        .any(|s| histogram_exported(&s.series, base));
    if !exported {
        return Histogram {
            status: "not exported".to_owned(),
            count: None,
            p50_ms: None,
            p95_ms: None,
            p99_ms: None,
        };
    }
    let mut merged: Vec<(f64, f64)> = Vec::new();
    for instance in instances {
        let Some((first, last)) = instance.endpoints() else {
            continue;
        };
        let delta = delta_buckets(
            &histogram_buckets(&first.series, base),
            &histogram_buckets(&last.series, base),
        );
        for (bound, count) in delta {
            match merged.iter_mut().find(|(b, _)| *b == bound) {
                Some(entry) => entry.1 += count,
                None => merged.push((bound, count)),
            }
        }
    }
    merged.sort_by(|a, b| a.0.total_cmp(&b.0));
    let count = merged.last().map(|(_, c)| *c);
    let at = |q: f64| quantile(&merged, q).map(|s| s * 1000.0);
    match at(0.5) {
        Some(p50) => Histogram {
            status: "ok".to_owned(),
            count,
            p50_ms: Some(p50),
            p95_ms: at(0.95),
            p99_ms: at(0.99),
        },
        None => Histogram {
            status: "no observations".to_owned(),
            count: Some(0.0),
            p50_ms: None,
            p95_ms: None,
            p99_ms: None,
        },
    }
}

/// CPU and RSS over the `ps` samples, or `None` when there are none.
#[must_use]
pub fn resources(samples: &[PsSample]) -> Option<Resources> {
    if samples.is_empty() {
        return None;
    }
    let n = samples.len() as f64;
    let mean_cpu = samples.iter().map(|s| s.cpu_percent).sum::<f64>() / n;
    let rss_bytes: Vec<f64> = samples.iter().map(|s| s.rss_kib * 1024.0).collect();
    let peak = rss_bytes.iter().copied().fold(0.0_f64, f64::max);
    let mean_rss = rss_bytes.iter().sum::<f64>() / n;
    Some(Resources {
        samples: u32::try_from(samples.len()).unwrap_or(u32::MAX),
        mean_cpu_percent: mean_cpu,
        peak_rss_bytes: peak,
        mean_rss_bytes: mean_rss,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::metrics::parse_exposition;

    fn scrape(at_s: f64, text: &str) -> Scrape {
        Scrape {
            at_s,
            series: parse_exposition(text).expect("exposition"),
        }
    }

    fn counters(messages: f64, bytes: f64) -> String {
        format!(
            "thadam_source_messages_total{{source=\"fleet-mqtt\"}} {messages}\n\
             thadam_source_bytes_total{{source=\"fleet-mqtt\"}} {bytes}\n"
        )
    }

    #[test]
    fn throughput_is_the_counter_delta_over_the_window() {
        let instance = Instance {
            scrapes: vec![
                scrape(1.0, &counters(100.0, 1_000_000.0)),
                scrape(11.0, &counters(600.0, 6_000_000.0)),
            ],
        };
        let t = throughput(&[instance]).expect("throughput");
        assert!((t.window_s - 10.0).abs() < 1e-9);
        assert!((t.messages - 500.0).abs() < 1e-9);
        assert!((t.source_messages_per_s - 50.0).abs() < 1e-9);
        assert!((t.mb_per_s - 0.5).abs() < 1e-9);
    }

    #[test]
    fn restarts_reset_counters_without_going_negative() {
        let first = Instance {
            scrapes: vec![
                scrape(0.0, &counters(0.0, 0.0)),
                scrape(10.0, &counters(1000.0, 0.0)),
            ],
        };
        let second = Instance {
            scrapes: vec![
                scrape(12.0, &counters(0.0, 0.0)),
                scrape(22.0, &counters(400.0, 0.0)),
            ],
        };
        let t = throughput(&[first, second]).expect("throughput");
        assert!((t.messages - 1400.0).abs() < 1e-9);
        assert!((t.window_s - 20.0).abs() < 1e-9);
    }

    #[test]
    fn throughput_needs_two_scrapes_and_an_exported_counter() {
        let one = Instance {
            scrapes: vec![scrape(1.0, &counters(1.0, 1.0))],
        };
        assert!(throughput(&[one]).is_none());
        let absent = Instance {
            scrapes: vec![scrape(1.0, "other 1\n"), scrape(2.0, "other 2\n")],
        };
        assert!(throughput(&[absent]).is_none());
    }

    fn ack(at_s: f64, le_1ms: f64, le_10ms: f64, inf: f64) -> Scrape {
        scrape(
            at_s,
            &format!(
                "thadam_source_ack_latency_seconds_bucket{{source=\"a\",le=\"0.001\"}} {le_1ms}\n\
                 thadam_source_ack_latency_seconds_bucket{{source=\"a\",le=\"0.01\"}} {le_10ms}\n\
                 thadam_source_ack_latency_seconds_bucket{{source=\"a\",le=\"+Inf\"}} {inf}\n\
                 thadam_source_ack_latency_seconds_count{{source=\"a\"}} {inf}\n"
            ),
        )
    }

    #[test]
    fn histogram_percentiles_use_only_the_window_observations() {
        // Before the window: 1000 slow observations that must not count.
        let instance = Instance {
            scrapes: vec![ack(0.0, 0.0, 0.0, 1000.0), ack(10.0, 50.0, 100.0, 1100.0)],
        };
        let h = histogram(&[instance], SOURCE_ACK);
        assert_eq!(h.status, "ok");
        assert_eq!(h.count, Some(100.0));
        let p50 = h.p50_ms.expect("p50");
        assert!((0.0..=1.0).contains(&p50), "p50 {p50}");
        let p99 = h.p99_ms.expect("p99");
        assert!((1.0..=10.0).contains(&p99), "p99 {p99}");
        assert!(h.p95_ms.expect("p95") >= p50);
    }

    #[test]
    fn histogram_is_reported_not_exported_when_absent() {
        let instance = Instance {
            scrapes: vec![
                scrape(0.0, &counters(1.0, 1.0)),
                scrape(1.0, &counters(2.0, 2.0)),
            ],
        };
        let h = histogram(&[instance], WAL_COMMIT);
        assert_eq!(h.status, "not exported");
        assert_eq!(h.p50_ms, None);
    }

    #[test]
    fn histogram_with_no_window_observations_says_so() {
        let instance = Instance {
            scrapes: vec![ack(0.0, 5.0, 5.0, 5.0), ack(10.0, 5.0, 5.0, 5.0)],
        };
        let h = histogram(&[instance], SOURCE_ACK);
        assert_eq!(h.status, "no observations");
        assert_eq!(h.count, Some(0.0));
    }

    #[test]
    fn resources_report_mean_and_peak() {
        let samples = [
            PsSample {
                cpu_percent: 10.0,
                rss_kib: 1000.0,
            },
            PsSample {
                cpu_percent: 30.0,
                rss_kib: 3000.0,
            },
        ];
        let r = resources(&samples).expect("resources");
        assert_eq!(r.samples, 2);
        assert!((r.mean_cpu_percent - 20.0).abs() < 1e-9);
        assert!((r.peak_rss_bytes - 3000.0 * 1024.0).abs() < 1e-6);
        assert!((r.mean_rss_bytes - 2000.0 * 1024.0).abs() < 1e-6);
        assert!(resources(&[]).is_none());
    }
}
