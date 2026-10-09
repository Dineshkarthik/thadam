//! OpenMetrics text from `/metrics`: parsing, counter sums, histogram buckets,
//! and quantiles. Pure functions; the sampler feeds them scraped text.

use crate::error::{BenchError, Result};

/// One exposition line: a series name, its labels, and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    /// The metric name as exported, including any `_total`, `_bucket`,
    /// `_sum`, or `_count` suffix.
    pub name: String,
    /// The label pairs, in exposition order.
    pub labels: Vec<(String, String)>,
    /// The sample value.
    pub value: f64,
}

/// Parses an OpenMetrics or Prometheus text exposition. Comment and blank
/// lines are skipped; exemplars and timestamps after the value are ignored.
///
/// # Errors
///
/// Returns [`BenchError::Run`] naming the line when a sample line is malformed.
pub fn parse_exposition(text: &str) -> Result<Vec<Series>> {
    let mut out = Vec::new();
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let series = parse_line(line)
            .map_err(|e| BenchError::Run(format!("metrics line {}: {e}: `{line}`", idx + 1)))?;
        out.push(series);
    }
    Ok(out)
}

fn parse_line(line: &str) -> std::result::Result<Series, String> {
    let name_end = line
        .find(|c: char| c == '{' || c.is_whitespace())
        .ok_or_else(|| "no value".to_owned())?;
    let name = &line[..name_end];
    let rest = &line[name_end..];
    let (labels, after) = if let Some(body) = rest.strip_prefix('{') {
        let close = label_block_end(body).ok_or_else(|| "unterminated label block".to_owned())?;
        (parse_labels(&body[..close])?, &body[close + 1..])
    } else {
        (Vec::new(), rest)
    };
    let token = after
        .split_whitespace()
        .next()
        .ok_or_else(|| "missing value".to_owned())?;
    let value = token
        .parse::<f64>()
        .map_err(|_| format!("bad value `{token}`"))?;
    Ok(Series {
        name: name.to_owned(),
        labels,
        value,
    })
}

/// Index of the `}` that closes a label block, skipping quoted values.
fn label_block_end(body: &str) -> Option<usize> {
    let mut in_quote = false;
    let mut escaped = false;
    for (i, c) in body.char_indices() {
        if in_quote {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_quote = false;
            }
        } else if c == '"' {
            in_quote = true;
        } else if c == '}' {
            return Some(i);
        }
    }
    None
}

fn parse_labels(body: &str) -> std::result::Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut chars = body.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(c) if *c == ',' || c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                break;
            }
            key.push(c);
            chars.next();
        }
        if chars.next() != Some('=') {
            return Err("label without `=`".to_owned());
        }
        if chars.next() != Some('"') {
            return Err("label value is not quoted".to_owned());
        }
        let mut value = String::new();
        loop {
            match chars.next() {
                None => return Err("unterminated label value".to_owned()),
                Some('\\') => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some(c) => value.push(c),
                    None => return Err("dangling escape in label value".to_owned()),
                },
                Some('"') => break,
                Some(c) => value.push(c),
            }
        }
        out.push((key.trim().to_owned(), value));
    }
    Ok(out)
}

/// The sum of every series named exactly `name`, or `None` when the metric is
/// not exported at all.
#[must_use]
pub fn counter_sum(series: &[Series], name: &str) -> Option<f64> {
    let mut found = false;
    let mut total = 0.0;
    for s in series.iter().filter(|s| s.name == name) {
        found = true;
        total += s.value;
    }
    found.then_some(total)
}

/// Whether a histogram family is exported (any `_count` series present).
#[must_use]
pub fn histogram_exported(series: &[Series], base: &str) -> bool {
    let count_name = format!("{base}_count");
    series.iter().any(|s| s.name == count_name)
}

/// The cumulative buckets of histogram `base`, merged across label sets and
/// sorted by upper bound. The last entry is the `+Inf` bucket.
#[must_use]
pub fn histogram_buckets(series: &[Series], base: &str) -> Vec<(f64, f64)> {
    let bucket_name = format!("{base}_bucket");
    let mut merged: Vec<(f64, f64)> = Vec::new();
    for s in series.iter().filter(|s| s.name == bucket_name) {
        let Some(le) = label(s, "le").and_then(|v| v.parse::<f64>().ok()) else {
            continue;
        };
        match merged.iter_mut().find(|(bound, _)| *bound == le) {
            Some(entry) => entry.1 += s.value,
            None => merged.push((le, s.value)),
        }
    }
    merged.sort_by(|a, b| a.0.total_cmp(&b.0));
    merged
}

fn label<'a>(series: &'a Series, key: &str) -> Option<&'a str> {
    series
        .labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// Per-bucket difference of two cumulative histograms, for a measurement
/// window. Buckets missing from `first` count from zero.
#[must_use]
pub fn delta_buckets(first: &[(f64, f64)], last: &[(f64, f64)]) -> Vec<(f64, f64)> {
    last.iter()
        .map(|&(bound, count)| {
            let base = first
                .iter()
                .find(|(b, _)| *b == bound)
                .map_or(0.0, |(_, c)| *c);
            (bound, (count - base).max(0.0))
        })
        .collect()
}

/// The `q`-quantile (0..=1) of cumulative buckets, interpolated linearly
/// inside the bucket that holds the rank, as Prometheus `histogram_quantile`
/// does. Buckets must be sorted by upper bound and end with `+Inf`. A rank in
/// the `+Inf` bucket returns the largest finite bound, which is a lower bound
/// on the true value.
#[must_use]
pub fn quantile(buckets: &[(f64, f64)], q: f64) -> Option<f64> {
    let total = buckets.last()?.1;
    if total <= 0.0 {
        return None;
    }
    let rank = q.clamp(0.0, 1.0) * total;
    let mut lower = 0.0_f64;
    let mut lower_count = 0.0_f64;
    for &(upper, count) in buckets {
        if count >= rank {
            if upper.is_infinite() {
                return Some(lower);
            }
            let in_bucket = count - lower_count;
            if in_bucket <= 0.0 {
                return Some(upper);
            }
            return Some(lower + (upper - lower) * ((rank - lower_count) / in_bucket));
        }
        lower = upper;
        lower_count = count;
    }
    Some(lower)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
# TYPE thadam_source_messages counter
thadam_source_messages_total{source=\"fleet-mqtt\"} 120
thadam_source_bytes_total{source=\"fleet-mqtt\"} 4096
thadam_source_ack_latency_seconds_bucket{source=\"fleet-mqtt\",le=\"0.001\"} 3
thadam_source_ack_latency_seconds_bucket{source=\"fleet-mqtt\",le=\"0.01\"} 9
thadam_source_ack_latency_seconds_bucket{source=\"fleet-mqtt\",le=\"+Inf\"} 10
thadam_source_ack_latency_seconds_sum{source=\"fleet-mqtt\"} 0.25
thadam_source_ack_latency_seconds_count{source=\"fleet-mqtt\"} 10
thadam_chain_events_total{sink=\"local\",dataset=\"joints\",outcome=\"ok\"} 7 # {trace_id=\"x\"} 1
thadam_clock_synced 1
# EOF
";

    #[test]
    fn parses_names_labels_and_values() {
        let series = parse_exposition(SAMPLE).expect("parse");
        assert_eq!(series.len(), 9);
        let msgs = &series[0];
        assert_eq!(msgs.name, "thadam_source_messages_total");
        assert_eq!(
            msgs.labels,
            vec![("source".to_owned(), "fleet-mqtt".to_owned())]
        );
        assert!((msgs.value - 120.0).abs() < f64::EPSILON);
        let events = series
            .iter()
            .find(|s| s.name == "thadam_chain_events_total")
            .expect("events");
        assert_eq!(events.labels.len(), 3);
        assert!((events.value - 7.0).abs() < f64::EPSILON);
        let synced = series
            .iter()
            .find(|s| s.name == "thadam_clock_synced")
            .expect("gauge");
        assert!(synced.labels.is_empty());
    }

    #[test]
    fn label_values_handle_escapes_and_braces() {
        let text = "m{a=\"x\\\"y\\\\z\",b=\"}{\"} 1\n";
        let series = parse_exposition(text).expect("parse");
        assert_eq!(series[0].labels[0].1, "x\"y\\z");
        assert_eq!(series[0].labels[1].1, "}{");
    }

    #[test]
    fn special_values_parse() {
        let series = parse_exposition("a +Inf\nb NaN\n").expect("parse");
        assert!(series[0].value.is_infinite());
        assert!(series[1].value.is_nan());
    }

    #[test]
    fn malformed_lines_name_the_line() {
        let err = parse_exposition("ok 1\nbroken{a=1} 2\n").expect_err("bad label");
        assert!(err.to_string().contains("line 2"), "{err}");
        assert!(parse_exposition("nameonly\n").is_err());
        assert!(parse_exposition("m 1.2.3\n").is_err());
    }

    #[test]
    fn counter_sum_distinguishes_zero_from_absent() {
        let series = parse_exposition(SAMPLE).expect("parse");
        assert_eq!(
            counter_sum(&series, "thadam_source_messages_total"),
            Some(120.0)
        );
        assert_eq!(counter_sum(&series, "thadam_nope_total"), None);
        let two = parse_exposition("c{s=\"a\"} 2\nc{s=\"b\"} 3\n").expect("parse");
        assert_eq!(counter_sum(&two, "c"), Some(5.0));
    }

    #[test]
    fn histogram_buckets_are_sorted_and_merged() {
        let text = "\
h_bucket{s=\"a\",le=\"+Inf\"} 4
h_bucket{s=\"a\",le=\"1\"} 1
h_bucket{s=\"b\",le=\"1\"} 2
h_bucket{s=\"b\",le=\"+Inf\"} 5
";
        let series = parse_exposition(text).expect("parse");
        let buckets = histogram_buckets(&series, "h");
        assert_eq!(buckets, vec![(1.0, 3.0), (f64::INFINITY, 9.0)]);
        assert!(!histogram_exported(&series, "h"));
    }

    #[test]
    fn histogram_export_detection_uses_count_series() {
        let series = parse_exposition(SAMPLE).expect("parse");
        assert!(histogram_exported(
            &series,
            "thadam_source_ack_latency_seconds"
        ));
        assert!(!histogram_exported(&series, "thadam_batch_write_seconds"));
    }

    #[test]
    fn delta_buckets_subtract_per_bound() {
        let first = [(1.0, 5.0), (2.0, 8.0), (f64::INFINITY, 8.0)];
        let last = [(1.0, 15.0), (2.0, 28.0), (f64::INFINITY, 30.0)];
        assert_eq!(
            delta_buckets(&first, &last),
            vec![(1.0, 10.0), (2.0, 20.0), (f64::INFINITY, 22.0)]
        );
        assert_eq!(delta_buckets(&[], &last)[0], (1.0, 15.0));
    }

    fn approx(a: Option<f64>, b: f64) {
        let a = a.expect("some quantile");
        assert!((a - b).abs() < 1e-9, "{a} != {b}");
    }

    #[test]
    fn quantiles_interpolate_within_buckets() {
        let buckets = [(1.0, 10.0), (2.0, 20.0), (4.0, 40.0), (f64::INFINITY, 40.0)];
        approx(quantile(&buckets, 0.25), 1.0);
        approx(quantile(&buckets, 0.5), 2.0);
        approx(quantile(&buckets, 0.75), 3.0);
        approx(quantile(&buckets, 0.99), 3.96);
        approx(quantile(&buckets, 1.0), 4.0);
    }

    #[test]
    fn quantile_in_overflow_bucket_returns_last_finite_bound() {
        let buckets = [(1.0, 0.0), (2.0, 0.0), (f64::INFINITY, 10.0)];
        approx(quantile(&buckets, 0.5), 2.0);
    }

    #[test]
    fn quantile_of_empty_or_zero_histogram_is_none() {
        assert_eq!(quantile(&[], 0.5), None);
        assert_eq!(quantile(&[(1.0, 0.0), (f64::INFINITY, 0.0)], 0.5), None);
    }

    #[test]
    fn single_observation_quantiles_stay_inside_its_bucket() {
        let buckets = [(0.001, 0.0), (0.01, 1.0), (f64::INFINITY, 1.0)];
        let p50 = quantile(&buckets, 0.5).expect("p50");
        let p99 = quantile(&buckets, 0.99).expect("p99");
        assert!((0.001..=0.01).contains(&p50));
        assert!((0.001..=0.01).contains(&p99));
        assert!(p99 >= p50);
    }
}
