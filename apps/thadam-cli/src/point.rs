//! The `--from` and `--to` points of `thadam replay` and `thadam export`: a WAL
//! position, a duration before now, or an RFC 3339 time. Times resolve to the
//! WAL position of the first or last retained frame ingested within the bound.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thadam_core::{Timestamp, WalPos};

use crate::data_dir;

/// A parsed `--from` or `--to` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    /// A WAL position.
    Position(u64),
    /// This long before now.
    Ago(Duration),
    /// An absolute time, Unix nanoseconds.
    At(i64),
}

/// Which frame a time bound selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bound {
    /// The first frame ingested at or after the time.
    First,
    /// The last frame ingested at or before the time.
    Last,
}

/// Resolves a `--from` or `--to` value against the WAL in `data_dir`. `flag`
/// names the option in error messages.
pub fn resolve(
    text: &str,
    flag: &str,
    data_dir: &Path,
    bound: Bound,
    now: i64,
) -> Result<WalPos, String> {
    let point = parse_point(text).map_err(|why| format!("invalid {flag} `{text}`: {why}"))?;
    let at = match point {
        Point::Position(p) => return Ok(WalPos::new(p)),
        Point::Ago(d) => {
            let nanos = i64::try_from(d.as_nanos())
                .map_err(|_| format!("invalid {flag} `{text}`: the duration is too long"))?;
            now.checked_sub(nanos)
                .ok_or_else(|| format!("invalid {flag} `{text}`: the time is out of range"))?
        }
        Point::At(nanos) => nanos,
    };
    let wal_dir = data_dir::wal_dir(data_dir)?;
    let inspection = thadam_wal::inspect(&wal_dir)
        .map_err(|e| format!("cannot read the WAL in {}: {e}", wal_dir.display()))?;
    let at_time = Timestamp::from_nanos(at);
    let found = match bound {
        Bound::First => inspection.first_seq_ingested_since(at_time),
        Bound::Last => inspection.last_seq_ingested_until(at_time),
    }
    .map_err(|e| format!("cannot read the WAL in {}: {e}", wal_dir.display()))?;
    found.ok_or_else(|| match bound {
        Bound::First => format!("no retained frame was ingested at or after `{text}`"),
        Bound::Last => format!("no retained frame was ingested at or before `{text}`"),
    })
}

/// The current time, Unix nanoseconds.
pub fn now_nanos() -> i64 {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since_epoch.as_nanos()).unwrap_or(i64::MAX)
}

fn parse_point(text: &str) -> Result<Point, String> {
    if text.is_empty() {
        return Err("expected a WAL position, a duration like -45m, or an RFC 3339 time".into());
    }
    if text.bytes().all(|b| b.is_ascii_digit()) {
        return text
            .parse::<u64>()
            .map(Point::Position)
            .map_err(|_| "the WAL position is too large".into());
    }
    if let Some(rest) = text.strip_prefix('-') {
        return parse_ago(rest).map(Point::Ago);
    }
    parse_rfc3339(text).map(Point::At)
}

/// Parses the part of `-45m` after the sign: one or more `<digits><unit>`
/// groups with units `s`, `m`, `h`, `d`.
fn parse_ago(text: &str) -> Result<Duration, String> {
    let mut total: u64 = 0;
    let mut digits = String::new();
    let mut groups = 0;
    for c in text.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        let unit = match c {
            's' => 1,
            'm' => 60,
            'h' => 3_600,
            'd' => 86_400,
            _ => return Err(format!("unknown duration unit `{c}`; use s, m, h, or d")),
        };
        let n: u64 = digits
            .parse()
            .map_err(|_| format!("a duration needs a number before `{c}`"))?;
        total = n
            .checked_mul(unit)
            .and_then(|s| total.checked_add(s))
            .ok_or("the duration is too long")?;
        digits.clear();
        groups += 1;
    }
    if !digits.is_empty() {
        return Err("a duration needs a unit: s, m, h, or d".into());
    }
    if groups == 0 {
        return Err("expected a duration like -45m".into());
    }
    Ok(Duration::from_secs(total))
}

/// Parses an RFC 3339 date-time with a `Z` or `±HH:MM` offset, and optional
/// fractional seconds.
fn parse_rfc3339(text: &str) -> Result<i64, String> {
    let bad = || "expected a WAL position, a duration like -45m, or an RFC 3339 time".to_owned();
    let b = text.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't')
        || b[13] != b':'
        || b[16] != b':'
    {
        return Err(bad());
    }
    let field = |range: std::ops::Range<usize>| -> Option<i64> {
        let s = text.get(range)?;
        if s.bytes().all(|c| c.is_ascii_digit()) {
            s.parse().ok()
        } else {
            None
        }
    };
    let (Some(year), Some(month), Some(day)) = (field(0..4), field(5..7), field(8..10)) else {
        return Err(bad());
    };
    let (Some(hour), Some(minute), Some(second)) = (field(11..13), field(14..16), field(17..19))
    else {
        return Err(bad());
    };
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err("the RFC 3339 time has a field out of range".into());
    }

    let mut rest = &text[19..];
    let mut nanos: i64 = 0;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let digits = after_dot.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(bad());
        }
        let mut frac = after_dot[..digits.min(9)].to_owned();
        while frac.len() < 9 {
            frac.push('0');
        }
        nanos = frac.parse().map_err(|_| bad())?;
        rest = &after_dot[digits..];
    }
    let offset_secs = match rest {
        "Z" | "z" => 0,
        _ => {
            let zone = rest.as_bytes();
            if zone.len() != 6 || zone[3] != b':' || !matches!(zone[0], b'+' | b'-') {
                return Err(bad());
            }
            let zh = field_of(&rest[1..3]).ok_or_else(bad)?;
            let zm = field_of(&rest[4..6]).ok_or_else(bad)?;
            if zh > 23 || zm > 59 {
                return Err("the RFC 3339 offset is out of range".into());
            }
            let secs = zh * 3_600 + zm * 60;
            if zone[0] == b'+' { secs } else { -secs }
        }
    };

    let days = days_from_civil(year, month, day);
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_secs;
    secs.checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(nanos))
        .ok_or_else(|| "the RFC 3339 time is out of range".into())
}

fn field_of(s: &str) -> Option<i64> {
    if s.bytes().all(|c| c.is_ascii_digit()) {
        s.parse().ok()
    } else {
        None
    }
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        _ => 28,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_numbers_are_wal_positions() {
        assert_eq!(parse_point("42"), Ok(Point::Position(42)));
        assert_eq!(parse_point("0"), Ok(Point::Position(0)));
    }

    #[test]
    fn durations_look_back_from_now() {
        assert_eq!(
            parse_point("-45m"),
            Ok(Point::Ago(Duration::from_secs(2_700)))
        );
        assert_eq!(
            parse_point("-1h30m"),
            Ok(Point::Ago(Duration::from_secs(5_400)))
        );
        assert_eq!(
            parse_point("-2d"),
            Ok(Point::Ago(Duration::from_secs(172_800)))
        );
        assert_eq!(parse_point("-10s"), Ok(Point::Ago(Duration::from_secs(10))));
    }

    #[test]
    fn malformed_durations_are_rejected() {
        assert!(parse_point("-").is_err());
        assert!(parse_point("-45").is_err());
        assert!(parse_point("-5x").is_err());
        assert!(parse_point("-m").is_err());
    }

    #[test]
    fn rfc3339_times_resolve_to_unix_nanos() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(
            parse_rfc3339("2000-01-01T00:00:00Z"),
            Ok(946_684_800_000_000_000)
        );
        assert_eq!(
            parse_rfc3339("2000-01-01T00:00:00.5Z"),
            Ok(946_684_800_500_000_000)
        );
        assert_eq!(
            parse_rfc3339("2000-01-01T01:00:00+01:00"),
            Ok(946_684_800_000_000_000)
        );
        assert_eq!(
            parse_rfc3339("1999-12-31T19:00:00-05:00"),
            Ok(946_684_800_000_000_000)
        );
    }

    #[test]
    fn rfc3339_rejects_impossible_dates() {
        assert!(parse_rfc3339("2026-02-30T00:00:00Z").is_err());
        assert!(parse_rfc3339("2026-13-01T00:00:00Z").is_err());
        assert!(parse_rfc3339("2026-10-09T25:00:00Z").is_err());
        assert!(parse_rfc3339("2026-10-09 12:00:00Z").is_err());
        assert!(parse_rfc3339("2026-10-09T12:00:00").is_err());
    }

    #[test]
    fn leap_days_follow_the_gregorian_rule() {
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2100, 2), 28);
        assert_eq!(days_in_month(2000, 2), 29);
    }
}
