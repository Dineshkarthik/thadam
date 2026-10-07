//! Epoch-number scaling and RFC 3339 timestamp parsing.
//!
//! Everything here converts to `i64` nanoseconds since the Unix epoch (UTC).
//! Inputs are treated as untrusted: all arithmetic is checked, nothing panics,
//! and errors never contain the input text.

use thiserror::Error;

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const SECONDS_PER_DAY: i64 = 86_400;
/// Longest accepted timestamp or offset string, in bytes.
const MAX_INPUT_LEN: usize = 64;
/// Most fractional-second digits accepted; digits past the ninth are dropped.
const MAX_FRACTION_DIGITS: usize = 12;
/// 2^63 as `f64`; exactly representable, unlike `i64::MAX`.
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// Unit of an epoch-based numeric timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EpochUnit {
    /// Whole or fractional seconds since the epoch.
    Seconds,
    /// Milliseconds since the epoch.
    Millis,
    /// Microseconds since the epoch.
    Micros,
    /// Nanoseconds since the epoch.
    Nanos,
}

impl EpochUnit {
    /// Parses the short unit name: `"s"`, `"ms"`, `"us"` or `"ns"`.
    ///
    /// Matching is exact (case-sensitive, no trimming).
    pub fn parse(s: &str) -> Option<EpochUnit> {
        match s {
            "s" => Some(EpochUnit::Seconds),
            "ms" => Some(EpochUnit::Millis),
            "us" => Some(EpochUnit::Micros),
            "ns" => Some(EpochUnit::Nanos),
            _ => None,
        }
    }

    /// Returns the short unit name accepted by [`EpochUnit::parse`].
    pub const fn as_str(self) -> &'static str {
        match self {
            EpochUnit::Seconds => "s",
            EpochUnit::Millis => "ms",
            EpochUnit::Micros => "us",
            EpochUnit::Nanos => "ns",
        }
    }

    const fn nanos_per_unit(self) -> i64 {
        match self {
            EpochUnit::Seconds => 1_000_000_000,
            EpochUnit::Millis => 1_000_000,
            EpochUnit::Micros => 1_000,
            EpochUnit::Nanos => 1,
        }
    }

    /// Converts an integer count of this unit to nanoseconds since the epoch.
    ///
    /// Returns `None` if the product overflows `i64`.
    pub fn int_to_nanos(self, v: i64) -> Option<i64> {
        v.checked_mul(self.nanos_per_unit())
    }

    /// Converts a floating-point count of this unit to nanoseconds since the
    /// epoch, rounding to the nearest nanosecond with ties away from zero.
    ///
    /// Returns `None` if `v` is NaN or infinite, or if the scaled and rounded
    /// value lies outside the `i64` range (`>= 2^63` or `< -2^63`).
    ///
    /// # Precision
    ///
    /// An `f64` carries about 15.95 significant decimal digits, so the result
    /// is only as precise as the input. Epoch *seconds* (around 1.8e9) resolve
    /// to roughly 0.24 microseconds; a float in milliseconds or microseconds
    /// resolves finer because its magnitude is smaller. Use
    /// [`EpochUnit::int_to_nanos`] when the source value is an integer.
    pub fn float_to_nanos(self, v: f64) -> Option<i64> {
        if !v.is_finite() {
            return None;
        }
        let scaled = match self {
            EpochUnit::Nanos => v,
            other => v * other.nanos_per_unit() as f64,
        };
        // Scaling can overflow to infinity; `round` leaves infinities alone.
        let rounded = scaled.round();
        if rounded.is_finite() && (-TWO_POW_63..TWO_POW_63).contains(&rounded) {
            // In range, so the cast is exact.
            Some(rounded as i64)
        } else {
            None
        }
    }
}

/// A fixed offset from UTC, stored as seconds east of UTC.
///
/// The magnitude is always strictly less than 86 400 seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UtcOffset {
    seconds: i32,
}

impl UtcOffset {
    /// The zero offset.
    pub const UTC: UtcOffset = UtcOffset { seconds: 0 };

    /// Creates an offset from seconds east of UTC.
    ///
    /// Returns `None` unless `|seconds| < 86_400`.
    pub fn from_seconds(seconds: i32) -> Option<UtcOffset> {
        if i64::from(seconds.unsigned_abs()) < SECONDS_PER_DAY {
            Some(UtcOffset { seconds })
        } else {
            None
        }
    }

    /// Returns the offset in seconds east of UTC.
    pub fn seconds(self) -> i32 {
        self.seconds
    }

    /// Parses an offset: `"Z"`, `"z"`, `"UTC"`, or a numeric offset of the form
    /// `+HH:MM`, `-HH:MM`, `+HHMM` or `+HH` (either sign).
    ///
    /// Hours must be at most 23 and minutes at most 59. The whole string must
    /// be consumed: no whitespace is trimmed.
    pub fn parse(s: &str) -> Result<UtcOffset, TimeParseError> {
        if s.len() > MAX_INPUT_LEN {
            return Err(TimeParseError::TooLong);
        }
        if matches!(s, "Z" | "z" | "UTC") {
            return Ok(UtcOffset::UTC);
        }
        Cursor::new(s).numeric_offset()
    }
}

/// Error returned when a timestamp or offset string cannot be converted.
///
/// Variants carry only static text and byte positions, never input content.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum TimeParseError {
    /// The input exceeded 64 bytes.
    #[error("input is longer than {MAX_INPUT_LEN} bytes")]
    TooLong,
    /// The input does not match the expected grammar.
    #[error("syntax error at byte {pos}: {message}")]
    Syntax {
        /// Zero-based byte offset where parsing failed.
        pos: usize,
        /// Static description of what was expected.
        message: &'static str,
    },
    /// The input is well-formed but a field or the final instant is out of range.
    #[error("value out of range: {0}")]
    OutOfRange(&'static str),
    /// No UTC offset was present and no default offset was supplied.
    #[error("timestamp has no UTC offset and no default offset is configured")]
    MissingOffset,
}

/// Parses an RFC 3339 style timestamp into nanoseconds since the Unix epoch.
///
/// Accepted shape: `YYYY-MM-DD`, then `T`, `t` or a single space, then
/// `HH:MM:SS`, an optional fraction, and an optional offset.
///
/// * Fraction: `.` followed by 1 to 12 ASCII digits. Digits beyond the ninth
///   are validated but truncated, not rounded. More than 12 digits is a
///   syntax error.
/// * Offset: `Z`/`z`, or `+HH:MM`, `-HH:MM`, `+HHMM`, `+HH` (either sign).
///   With no offset, `default_offset` is used, or [`TimeParseError::MissingOffset`]
///   is returned if it is `None`.
/// * Ranges: month 1..=12, day valid for the month (Gregorian leap years),
///   hour 0..=23, minute 0..=59, second 0..=60, offset hours <= 23 and offset
///   minutes <= 59.
/// * Leap second: second `60` is accepted and added arithmetically, so
///   `23:59:60` is exactly the next day's `00:00:00`.
/// * The year must be four digits, and the resulting instant must fit `i64`
///   nanoseconds, which is 1677-09-21T00:12:43.145224192Z through
///   2262-04-11T23:47:16.854775807Z; otherwise [`TimeParseError::OutOfRange`].
///
/// Whitespace is not trimmed and no other date forms (week or ordinal dates,
/// month names) are accepted. Input longer than 64 bytes is rejected with
/// [`TimeParseError::TooLong`].
pub fn parse_rfc3339(s: &str, default_offset: Option<UtcOffset>) -> Result<i64, TimeParseError> {
    if s.len() > MAX_INPUT_LEN {
        return Err(TimeParseError::TooLong);
    }
    let mut c = Cursor::new(s);

    let year = c.digits(4)?;
    c.expect_one_of(b"-", "expected '-'")?;
    let month = c.digits(2)?;
    c.expect_one_of(b"-", "expected '-'")?;
    let day = c.digits(2)?;
    c.expect_one_of(b"Tt ", "expected 'T', 't' or space")?;
    let hour = c.digits(2)?;
    c.expect_one_of(b":", "expected ':'")?;
    let minute = c.digits(2)?;
    c.expect_one_of(b":", "expected ':'")?;
    let second = c.digits(2)?;
    let fraction_nanos = c.fraction()?;

    let offset = match c.peek() {
        None => default_offset,
        Some(b'Z' | b'z') => {
            c.advance();
            c.expect_end()?;
            Some(UtcOffset::UTC)
        }
        Some(b'+' | b'-') => Some(c.numeric_offset()?),
        Some(_) => return Err(c.syntax("expected offset")),
    };

    if !(1..=12).contains(&month) {
        return Err(TimeParseError::OutOfRange("month"));
    }
    let max_day =
        days_in_month(i64::from(year), month).ok_or(TimeParseError::OutOfRange("month"))?;
    if day < 1 || day > max_day {
        return Err(TimeParseError::OutOfRange("day"));
    }
    if hour > 23 {
        return Err(TimeParseError::OutOfRange("hour"));
    }
    if minute > 59 {
        return Err(TimeParseError::OutOfRange("minute"));
    }
    // 60 is a leap second; it simply rolls into the next minute below.
    if second > 60 {
        return Err(TimeParseError::OutOfRange("second"));
    }
    let offset = offset.ok_or(TimeParseError::MissingOffset)?;

    const OVERFLOW: TimeParseError =
        TimeParseError::OutOfRange("instant does not fit in i64 nanoseconds");
    let days = days_from_civil(i64::from(year), month, day).ok_or(OVERFLOW)?;
    let seconds = days
        .checked_mul(SECONDS_PER_DAY)
        .and_then(|v| v.checked_add(i64::from(hour).checked_mul(3_600)?))
        .and_then(|v| v.checked_add(i64::from(minute).checked_mul(60)?))
        .and_then(|v| v.checked_add(i64::from(second)))
        .and_then(|v| v.checked_sub(i64::from(offset.seconds)))
        .ok_or(OVERFLOW)?;
    // Combine in i128: a pre-1970 instant near i64::MIN has a whole-second
    // part whose nanosecond product overflows i64 even though the sum fits.
    let nanos = i128::from(seconds)
        .checked_mul(i128::from(NANOS_PER_SECOND))
        .and_then(|v| v.checked_add(i128::from(fraction_nanos)))
        .ok_or(OVERFLOW)?;
    i64::try_from(nanos).map_err(|_| OVERFLOW)
}

/// Whether `year` is a Gregorian leap year.
fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Number of days in `month` (1..=12) of `year`, or `None` for an invalid month.
fn days_in_month(year: i64, month: u32) -> Option<u32> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 if is_leap_year(year) => Some(29),
        2 => Some(28),
        _ => None,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`). The caller guarantees a valid month and day.
fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    let month = i64::from(month);
    let day = i64::from(day);
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = y.div_euclid(400);
    let year_of_era = y.rem_euclid(400);
    let shifted_month = if month > 2 {
        month.checked_sub(3)?
    } else {
        month.checked_add(9)?
    };
    let day_of_year = shifted_month
        .checked_mul(153)?
        .checked_add(2)?
        .checked_div(5)?
        .checked_add(day.checked_sub(1)?)?;
    let day_of_era = year_of_era
        .checked_mul(365)?
        .checked_add(year_of_era.checked_div(4)?)?
        .checked_sub(year_of_era.checked_div(100)?)?
        .checked_add(day_of_year)?;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
}

/// Byte cursor over the input with position-tracking error construction.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(s: &'a str) -> Self {
        Cursor {
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn advance(&mut self) {
        self.pos = self.pos.saturating_add(1);
    }

    fn syntax(&self, message: &'static str) -> TimeParseError {
        TimeParseError::Syntax {
            pos: self.pos,
            message,
        }
    }

    fn expect_end(&self) -> Result<(), TimeParseError> {
        if self.peek().is_some() {
            Err(self.syntax("unexpected trailing characters"))
        } else {
            Ok(())
        }
    }

    fn expect_one_of(
        &mut self,
        allowed: &[u8],
        message: &'static str,
    ) -> Result<(), TimeParseError> {
        match self.peek() {
            Some(b) if allowed.contains(&b) => {
                self.advance();
                Ok(())
            }
            Some(_) => Err(self.syntax(message)),
            None => Err(self.syntax("unexpected end of input")),
        }
    }

    /// Reads exactly `n` (at most 9) ASCII digits as a number.
    fn digits(&mut self, n: usize) -> Result<u32, TimeParseError> {
        let mut value: u32 = 0;
        for _ in 0..n {
            match self.peek() {
                Some(b) if b.is_ascii_digit() => {
                    value = value.saturating_mul(10).saturating_add(u32::from(b - b'0'));
                    self.advance();
                }
                Some(_) => return Err(self.syntax("expected digit")),
                None => return Err(self.syntax("unexpected end of input")),
            }
        }
        Ok(value)
    }

    /// Reads an optional `.digits` fraction and returns it as nanoseconds.
    fn fraction(&mut self) -> Result<u32, TimeParseError> {
        if self.peek() != Some(b'.') {
            return Ok(0);
        }
        self.advance();
        let mut nanos: u32 = 0;
        let mut count: usize = 0;
        while let Some(b) = self.peek().filter(u8::is_ascii_digit) {
            if count >= MAX_FRACTION_DIGITS {
                return Err(self.syntax("too many fractional digits"));
            }
            if count < 9 {
                nanos = nanos.saturating_mul(10).saturating_add(u32::from(b - b'0'));
            }
            count = count.saturating_add(1);
            self.advance();
        }
        if count == 0 {
            return Err(match self.peek() {
                Some(_) => self.syntax("expected digit"),
                None => self.syntax("unexpected end of input"),
            });
        }
        for _ in count..9 {
            nanos = nanos.saturating_mul(10);
        }
        Ok(nanos)
    }

    /// Reads `±HH`, `±HHMM` or `±HH:MM` and requires the input to end after it.
    fn numeric_offset(&mut self) -> Result<UtcOffset, TimeParseError> {
        let sign: i32 = match self.peek() {
            Some(b'+') => 1,
            Some(b'-') => -1,
            Some(_) => return Err(self.syntax("expected offset")),
            None => return Err(self.syntax("unexpected end of input")),
        };
        self.advance();
        let hours = self.digits(2)?;
        let minutes = if self.peek().is_none() {
            0
        } else {
            if self.peek() == Some(b':') {
                self.advance();
            }
            self.digits(2)?
        };
        self.expect_end()?;
        if hours > 23 {
            return Err(TimeParseError::OutOfRange("offset hours"));
        }
        if minutes > 59 {
            return Err(TimeParseError::OutOfRange("offset minutes"));
        }
        // Both operands are tiny, so the conversions and arithmetic cannot
        // fail; the checked forms keep that obvious without a panic path.
        let magnitude = i32::try_from(hours)
            .ok()
            .and_then(|h| h.checked_mul(3_600))
            .zip(i32::try_from(minutes).ok().and_then(|m| m.checked_mul(60)))
            .and_then(|(h, m)| h.checked_add(m))
            .ok_or(TimeParseError::OutOfRange("offset"))?;
        let seconds = magnitude
            .checked_mul(sign)
            .ok_or(TimeParseError::OutOfRange("offset"))?;
        UtcOffset::from_seconds(seconds).ok_or(TimeParseError::OutOfRange("offset"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn p(s: &str) -> Result<i64, TimeParseError> {
        parse_rfc3339(s, None)
    }

    /// Reference civil-from-days (Hinnant), independent of the production code.
    fn civil_from_days(z: i64) -> (i64, i64, i64) {
        let z = z + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + i64::from(m <= 2);
        (y, m, d)
    }

    /// Formats `nanos` (an instant) as local time in `offset_secs`.
    fn format_local(nanos: i64, offset_secs: i32) -> String {
        let local = i128::from(nanos) + i128::from(offset_secs) * 1_000_000_000;
        let secs = local.div_euclid(1_000_000_000);
        let frac = local.rem_euclid(1_000_000_000);
        let days = i64::try_from(secs.div_euclid(86_400)).unwrap();
        let sod = secs.rem_euclid(86_400);
        let (y, m, d) = civil_from_days(days);
        let sign = if offset_secs < 0 { '-' } else { '+' };
        let abs = offset_secs.abs();
        format!(
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:09}{sign}{:02}:{:02}",
            sod / 3_600,
            sod % 3_600 / 60,
            sod % 60,
            abs / 3_600,
            abs % 3_600 / 60,
        )
    }

    #[test]
    fn unit_parse_and_as_str_round_trip() {
        for unit in [
            EpochUnit::Seconds,
            EpochUnit::Millis,
            EpochUnit::Micros,
            EpochUnit::Nanos,
        ] {
            assert_eq!(EpochUnit::parse(unit.as_str()), Some(unit));
        }
        for bad in ["", "S", "sec", " s", "s ", "m", "μs", "NS"] {
            assert_eq!(EpochUnit::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn int_scaling() {
        assert_eq!(EpochUnit::Seconds.int_to_nanos(1), Some(1_000_000_000));
        assert_eq!(EpochUnit::Millis.int_to_nanos(1), Some(1_000_000));
        assert_eq!(EpochUnit::Micros.int_to_nanos(1), Some(1_000));
        assert_eq!(EpochUnit::Nanos.int_to_nanos(1), Some(1));
        assert_eq!(EpochUnit::Seconds.int_to_nanos(0), Some(0));
        assert_eq!(
            EpochUnit::Seconds.int_to_nanos(1_700_000_000),
            Some(1_700_000_000_000_000_000)
        );
    }

    #[test]
    fn int_scaling_negative_and_overflow() {
        assert_eq!(EpochUnit::Seconds.int_to_nanos(-1), Some(-1_000_000_000));
        assert_eq!(
            EpochUnit::Seconds.int_to_nanos(9_223_372_036),
            Some(9_223_372_036_000_000_000)
        );
        assert_eq!(EpochUnit::Seconds.int_to_nanos(9_223_372_037), None);
        assert_eq!(
            EpochUnit::Seconds.int_to_nanos(-9_223_372_036),
            Some(-9_223_372_036_000_000_000)
        );
        assert_eq!(EpochUnit::Seconds.int_to_nanos(-9_223_372_037), None);
        assert_eq!(EpochUnit::Seconds.int_to_nanos(i64::MAX), None);
        assert_eq!(EpochUnit::Seconds.int_to_nanos(i64::MIN), None);
        assert_eq!(EpochUnit::Millis.int_to_nanos(i64::MAX), None);
        assert_eq!(EpochUnit::Micros.int_to_nanos(i64::MAX), None);
        assert_eq!(EpochUnit::Nanos.int_to_nanos(i64::MAX), Some(i64::MAX));
        assert_eq!(EpochUnit::Nanos.int_to_nanos(i64::MIN), Some(i64::MIN));
    }

    #[test]
    fn float_scaling() {
        assert_eq!(EpochUnit::Seconds.float_to_nanos(1.5), Some(1_500_000_000));
        assert_eq!(
            EpochUnit::Seconds.float_to_nanos(-1.5),
            Some(-1_500_000_000)
        );
        assert_eq!(EpochUnit::Millis.float_to_nanos(1.5), Some(1_500_000));
        assert_eq!(EpochUnit::Micros.float_to_nanos(1.5), Some(1_500));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(42.0), Some(42));
        assert_eq!(EpochUnit::Seconds.float_to_nanos(0.0), Some(0));
        assert_eq!(EpochUnit::Seconds.float_to_nanos(-0.0), Some(0));
    }

    #[test]
    fn float_rounds_ties_away_from_zero() {
        assert_eq!(EpochUnit::Nanos.float_to_nanos(0.5), Some(1));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(-0.5), Some(-1));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(1.5), Some(2));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(2.5), Some(3));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(-2.5), Some(-3));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(0.49), Some(0));
        assert_eq!(EpochUnit::Nanos.float_to_nanos(-0.49), Some(0));
        assert_eq!(EpochUnit::Micros.float_to_nanos(0.0005), Some(1));
    }

    #[test]
    fn float_rejects_non_finite_and_huge() {
        for unit in [
            EpochUnit::Seconds,
            EpochUnit::Millis,
            EpochUnit::Micros,
            EpochUnit::Nanos,
        ] {
            assert_eq!(unit.float_to_nanos(f64::NAN), None);
            assert_eq!(unit.float_to_nanos(f64::INFINITY), None);
            assert_eq!(unit.float_to_nanos(f64::NEG_INFINITY), None);
            assert_eq!(unit.float_to_nanos(1e30), None);
            assert_eq!(unit.float_to_nanos(-1e30), None);
            assert_eq!(unit.float_to_nanos(f64::MAX), None);
            assert_eq!(unit.float_to_nanos(f64::MIN), None);
        }
    }

    #[test]
    fn float_i64_boundaries() {
        // `i64::MAX as f64` rounds up to exactly 2^63, which is out of range.
        assert_eq!(EpochUnit::Nanos.float_to_nanos(i64::MAX as f64), None);
        assert_eq!(
            EpochUnit::Nanos.float_to_nanos(9_223_372_036_854_775_808.0),
            None
        );
        // The largest f64 below 2^63 is 2^63 - 1024.
        assert_eq!(
            EpochUnit::Nanos.float_to_nanos(9_223_372_036_854_774_784.0),
            Some(9_223_372_036_854_774_784)
        );
        // -2^63 is representable and in range; the next f64 below it is not.
        assert_eq!(
            EpochUnit::Nanos.float_to_nanos(-9_223_372_036_854_775_808.0),
            Some(i64::MIN)
        );
        assert_eq!(
            EpochUnit::Nanos.float_to_nanos(-9_223_372_036_854_777_856.0),
            None
        );
        assert_eq!(
            EpochUnit::Seconds.float_to_nanos(9_223_372_036.0),
            Some(9_223_372_036_000_000_000)
        );
        assert_eq!(EpochUnit::Seconds.float_to_nanos(9_223_372_037.0), None);
        assert_eq!(
            EpochUnit::Seconds.float_to_nanos(-9_223_372_036.0),
            Some(-9_223_372_036_000_000_000)
        );
        assert_eq!(EpochUnit::Seconds.float_to_nanos(-9_223_372_037.0), None);
    }

    proptest! {
        #[test]
        fn float_seconds_precision(micros in 1_500_000_000_000_000_i64..=2_000_000_000_000_000_i64) {
            let secs = micros as f64 / 1e6;
            let got = EpochUnit::Seconds.float_to_nanos(secs).unwrap();
            let exact = micros * 1_000;
            prop_assert!((got - exact).abs() <= 300, "err {} ns", (got - exact).abs());
        }
    }

    #[test]
    fn offset_constructor_bounds() {
        assert_eq!(UtcOffset::from_seconds(0), Some(UtcOffset::UTC));
        assert_eq!(
            UtcOffset::from_seconds(86_399).map(UtcOffset::seconds),
            Some(86_399)
        );
        assert_eq!(
            UtcOffset::from_seconds(-86_399).map(UtcOffset::seconds),
            Some(-86_399)
        );
        assert_eq!(UtcOffset::from_seconds(86_400), None);
        assert_eq!(UtcOffset::from_seconds(-86_400), None);
        assert_eq!(UtcOffset::from_seconds(i32::MIN), None);
        assert_eq!(UtcOffset::from_seconds(i32::MAX), None);
    }

    #[test]
    fn offset_parse_accepts() {
        let secs = |s: &str| UtcOffset::parse(s).map(UtcOffset::seconds);
        assert_eq!(secs("Z"), Ok(0));
        assert_eq!(secs("z"), Ok(0));
        assert_eq!(secs("UTC"), Ok(0));
        assert_eq!(secs("+00:00"), Ok(0));
        assert_eq!(secs("-00:00"), Ok(0));
        assert_eq!(secs("+02:00"), Ok(7_200));
        assert_eq!(secs("-05:30"), Ok(-19_800));
        assert_eq!(secs("+0530"), Ok(19_800));
        assert_eq!(secs("-0530"), Ok(-19_800));
        assert_eq!(secs("+02"), Ok(7_200));
        assert_eq!(secs("-11"), Ok(-39_600));
        assert_eq!(secs("+23:59"), Ok(86_340));
    }

    #[test]
    fn offset_parse_rejects() {
        for bad in [
            "",
            "+",
            "+2",
            "+2:00",
            "+02:",
            "+02:0",
            "+02:000",
            "+02:00:00",
            "02:00",
            "utc",
            "Zulu",
            "Z ",
            " Z",
            "+02 00",
            "+0a:00",
            "++02",
            "+02:6a",
        ] {
            assert!(
                matches!(UtcOffset::parse(bad), Err(TimeParseError::Syntax { .. })),
                "{bad:?} -> {:?}",
                UtcOffset::parse(bad)
            );
        }
        assert_eq!(
            UtcOffset::parse("+24:00"),
            Err(TimeParseError::OutOfRange("offset hours"))
        );
        assert_eq!(
            UtcOffset::parse("-99"),
            Err(TimeParseError::OutOfRange("offset hours"))
        );
        assert_eq!(
            UtcOffset::parse("+02:60"),
            Err(TimeParseError::OutOfRange("offset minutes"))
        );
        assert_eq!(
            UtcOffset::parse(&"+".repeat(65)),
            Err(TimeParseError::TooLong)
        );
    }

    #[test]
    fn epoch_is_zero() {
        assert_eq!(p("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(p("1970-01-01T00:00:01Z"), Ok(1_000_000_000));
    }

    #[test]
    fn full_nanosecond_precision() {
        // 2026-10-07T12:34:56Z = 1_791_376_496 s.
        assert_eq!(
            p("2026-10-07T12:34:56.123456789Z"),
            Ok(1_791_376_496_123_456_789)
        );
        assert_eq!(p("2026-10-07T12:34:56Z"), Ok(1_791_376_496_000_000_000));
    }

    #[test]
    fn numeric_offsets_are_subtracted() {
        let utc = p("2026-10-07T12:34:56Z").unwrap();
        assert_eq!(p("2026-10-07T14:34:56+02:00"), Ok(utc));
        assert_eq!(p("2026-10-07T14:34:56+0200"), Ok(utc));
        assert_eq!(p("2026-10-07T14:34:56+02"), Ok(utc));
        assert_eq!(p("2026-10-07T07:04:56-05:30"), Ok(utc));
        assert_eq!(p("2026-10-07T07:04:56-0530"), Ok(utc));
        assert_eq!(p("2026-10-07T18:04:56+0530"), Ok(utc));
        assert_eq!(p("2026-10-07T18:04:56+05:30"), Ok(utc));
    }

    #[test]
    fn lowercase_and_space_separators() {
        let utc = p("2026-10-07T12:34:56Z").unwrap();
        assert_eq!(p("2026-10-07t12:34:56z"), Ok(utc));
        assert_eq!(p("2026-10-07 12:34:56Z"), Ok(utc));
        assert_eq!(p("2026-10-07 12:34:56z"), Ok(utc));
    }

    #[test]
    fn missing_offset_and_default() {
        assert_eq!(p("2026-10-07T12:34:56"), Err(TimeParseError::MissingOffset));
        assert_eq!(
            p("2026-10-07T12:34:56.5"),
            Err(TimeParseError::MissingOffset)
        );
        let utc = p("2026-10-07T12:34:56Z").unwrap();
        assert_eq!(
            parse_rfc3339("2026-10-07T12:34:56", Some(UtcOffset::UTC)),
            Ok(utc)
        );
        let plus2 = UtcOffset::parse("+02:00").unwrap();
        assert_eq!(parse_rfc3339("2026-10-07T14:34:56", Some(plus2)), Ok(utc));
        // An explicit offset wins over the default.
        assert_eq!(parse_rfc3339("2026-10-07T12:34:56Z", Some(plus2)), Ok(utc));
    }

    #[test]
    fn fractional_digit_counts() {
        let base = p("2026-10-07T12:34:56Z").unwrap();
        let cases: [(&str, i64); 12] = [
            ("1", 100_000_000),
            ("12", 120_000_000),
            ("123", 123_000_000),
            ("1234", 123_400_000),
            ("12345", 123_450_000),
            ("123456", 123_456_000),
            ("1234567", 123_456_700),
            ("12345678", 123_456_780),
            ("123456789", 123_456_789),
            ("1234567891", 123_456_789),
            ("12345678999", 123_456_789),
            ("999999999999", 999_999_999),
        ];
        for (digits, nanos) in cases {
            let s = format!("2026-10-07T12:34:56.{digits}Z");
            assert_eq!(p(&s), Ok(base + nanos), "{s}");
        }
        assert_eq!(
            p("2026-10-07T12:34:56.0000000000009Z"),
            Err(TimeParseError::Syntax {
                pos: 32,
                message: "too many fractional digits"
            })
        );
        assert!(matches!(
            p("2026-10-07T12:34:56.Z"),
            Err(TimeParseError::Syntax { pos: 20, .. })
        ));
        assert!(matches!(
            p("2026-10-07T12:34:56."),
            Err(TimeParseError::Syntax { pos: 20, .. })
        ));
        assert!(matches!(
            p("2026-10-07T12:34:56,5Z"),
            Err(TimeParseError::Syntax { pos: 19, .. })
        ));
    }

    #[test]
    fn leap_years() {
        assert!(p("2024-02-29T00:00:00Z").is_ok());
        assert_eq!(
            p("2023-02-29T00:00:00Z"),
            Err(TimeParseError::OutOfRange("day"))
        );
        assert_eq!(
            p("1900-02-29T00:00:00Z"),
            Err(TimeParseError::OutOfRange("day"))
        );
        assert!(p("2000-02-29T00:00:00Z").is_ok());
        assert_eq!(
            p("2100-02-29T00:00:00Z"),
            Err(TimeParseError::OutOfRange("day"))
        );
        assert_eq!(
            p("2024-03-01T00:00:00Z").unwrap() - p("2024-02-29T00:00:00Z").unwrap(),
            86_400 * 1_000_000_000
        );
    }

    #[test]
    fn day_limits_per_month() {
        for (m, max) in [
            (1, 31),
            (2, 28),
            (3, 31),
            (4, 30),
            (5, 31),
            (6, 30),
            (7, 31),
            (8, 31),
            (9, 30),
            (10, 31),
            (11, 30),
            (12, 31),
        ] {
            assert!(p(&format!("2023-{m:02}-{max:02}T00:00:00Z")).is_ok());
            assert_eq!(
                p(&format!("2023-{m:02}-{:02}T00:00:00Z", max + 1)),
                Err(TimeParseError::OutOfRange("day"))
            );
            assert_eq!(
                p(&format!("2023-{m:02}-00T00:00:00Z")),
                Err(TimeParseError::OutOfRange("day"))
            );
        }
    }

    #[test]
    fn leap_second_rolls_to_next_minute() {
        assert_eq!(p("2016-12-31T23:59:60Z"), p("2017-01-01T00:00:00Z"));
        assert_eq!(p("1999-12-31T23:59:60Z"), p("2000-01-01T00:00:00Z"));
        assert_eq!(p("2026-10-07T12:34:60Z"), p("2026-10-07T12:35:00Z"));
        assert_eq!(
            p("2016-12-31T23:59:60.5Z").unwrap(),
            p("2017-01-01T00:00:00Z").unwrap() + 500_000_000
        );
        assert_eq!(
            p("2016-12-31T23:59:61Z"),
            Err(TimeParseError::OutOfRange("second"))
        );
    }

    #[test]
    fn field_range_errors() {
        assert_eq!(
            p("2026-13-01T00:00:00Z"),
            Err(TimeParseError::OutOfRange("month"))
        );
        assert_eq!(
            p("2026-00-01T00:00:00Z"),
            Err(TimeParseError::OutOfRange("month"))
        );
        assert_eq!(
            p("2026-01-01T24:00:00Z"),
            Err(TimeParseError::OutOfRange("hour"))
        );
        assert_eq!(
            p("2026-01-01T00:60:00Z"),
            Err(TimeParseError::OutOfRange("minute"))
        );
        assert_eq!(
            p("2026-01-01T00:00:00+24:00"),
            Err(TimeParseError::OutOfRange("offset hours"))
        );
        assert_eq!(
            p("2026-01-01T00:00:00+00:60"),
            Err(TimeParseError::OutOfRange("offset minutes"))
        );
    }

    #[test]
    fn pre_epoch_dates() {
        assert_eq!(p("1969-12-31T23:59:59Z"), Ok(-1_000_000_000));
        assert_eq!(p("1969-12-31T23:59:59.5Z"), Ok(-500_000_000));
        assert_eq!(p("1969-12-31T23:59:59.999999999Z"), Ok(-1));
        assert_eq!(p("1969-12-31T00:00:00Z"), Ok(-86_400 * 1_000_000_000));
        assert_eq!(
            p("1900-01-01T00:00:00Z"),
            Ok(-2_208_988_800 * 1_000_000_000)
        );
        assert_eq!(p("1970-01-01T00:00:00+01:00"), Ok(-3_600 * 1_000_000_000));
    }

    #[test]
    fn i64_nanosecond_range_edges() {
        assert_eq!(p("1677-09-21T00:12:43.145224192Z"), Ok(i64::MIN));
        assert!(matches!(
            p("1677-09-21T00:12:43.145224191Z"),
            Err(TimeParseError::OutOfRange(_))
        ));
        assert_eq!(p("2262-04-11T23:47:16.854775807Z"), Ok(i64::MAX));
        assert!(matches!(
            p("2262-04-11T23:47:16.854775808Z"),
            Err(TimeParseError::OutOfRange(_))
        ));
        // 1678-01-01 is just inside the i64 range (the minimum is 1677-09-21).
        assert!(p("1678-01-01T00:00:00Z").unwrap() < -9_000_000_000_000_000_000);
        assert!(matches!(
            p("1677-01-01T00:00:00Z"),
            Err(TimeParseError::OutOfRange(_))
        ));
        assert!(matches!(
            p("0000-01-01T00:00:00Z"),
            Err(TimeParseError::OutOfRange(_))
        ));
        assert!(matches!(
            p("9999-12-31T23:59:59Z"),
            Err(TimeParseError::OutOfRange(_))
        ));
        assert!(matches!(
            p("2263-01-01T00:00:00Z"),
            Err(TimeParseError::OutOfRange(_))
        ));
        // The offset is applied before the range check.
        assert_eq!(p("2262-04-12T01:47:16.854775807+02:00"), Ok(i64::MAX));
    }

    #[test]
    fn garbage_is_rejected() {
        for bad in [
            "",
            "T",
            "Z",
            "2026",
            "2026-10-07",
            "2026-10-07T",
            "2026-10-07T12",
            "2026-10-07T12:34",
            "2026-10-07T12:34:5",
            "2026-1-07T12:34:56Z",
            "26-10-07T12:34:56Z",
            "20261007T123456Z",
            "2026/10/07T12:34:56Z",
            "2026-10-07X12:34:56Z",
            "2026-10-07T12-34-56Z",
            "2026-10-07T12:34:56Zjunk",
            "2026-10-07T12:34:56ZZ",
            "2026-10-07T12:34:56 ",
            " 2026-10-07T12:34:56Z",
            "2026-10-07T12:34:56Z\n",
            "2026-10-07T12:34:56+",
            "2026-10-07T12:34:56+2",
            "2026-10-07T12:34:56+02:0",
            "2026-10-07T12:34:56+02:00:00",
            "2026-10-07T12:34:56UTC",
            "+2026-10-07T12:34:56Z",
            "2026-Oct-07T12:34:56Z",
            "2026-W41-3T12:34:56Z",
            "2026-280T12:34:56Z",
            "２０２６-10-07T12:34:56Z",
            "2026-13-01T00:00:00Z",
        ] {
            assert!(p(bad).is_err(), "{bad:?} should fail");
        }
        assert!(matches!(
            p("2026-10-07T12:34:56Zjunk"),
            Err(TimeParseError::Syntax { pos: 20, .. })
        ));
        assert_eq!(
            p(""),
            Err(TimeParseError::Syntax {
                pos: 0,
                message: "unexpected end of input"
            })
        );
    }

    #[test]
    fn syntax_error_positions() {
        assert!(matches!(
            p("2026x10-07T00:00:00Z"),
            Err(TimeParseError::Syntax { pos: 4, .. })
        ));
        assert!(matches!(
            p("2026-10-07T00:00:00!"),
            Err(TimeParseError::Syntax { pos: 19, .. })
        ));
        assert!(matches!(
            p("20a6-10-07T00:00:00Z"),
            Err(TimeParseError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            p("2026-10-07T00:00"),
            Err(TimeParseError::Syntax { pos: 16, .. })
        ));
    }

    #[test]
    fn length_cap() {
        let at_cap = format!("2026-10-07T12:34:56.{}Z", "0".repeat(64 - 21));
        assert_eq!(at_cap.len(), 64);
        // Too many fractional digits, but not TooLong.
        assert!(matches!(p(&at_cap), Err(TimeParseError::Syntax { .. })));
        let over = "x".repeat(65);
        assert_eq!(p(&over), Err(TimeParseError::TooLong));
        let valid_padded = format!("2026-10-07T12:34:56Z{}", " ".repeat(45));
        assert_eq!(valid_padded.len(), 65);
        assert_eq!(p(&valid_padded), Err(TimeParseError::TooLong));
    }

    #[test]
    fn errors_do_not_echo_input() {
        let secret = "SECRET-PAYLOAD";
        let inputs = [
            format!("{secret}T00:00:00Z"),
            format!("2026-10-07T12:34:56Z{secret}"),
            format!("2026-10-07T12:34:56{secret}"),
            secret.repeat(10),
        ];
        for input in inputs {
            let err = p(&input).unwrap_err();
            assert!(!format!("{err}").contains(secret));
            assert!(!format!("{err:?}").contains(secret));
        }
        let err = UtcOffset::parse(secret).unwrap_err();
        assert!(!format!("{err}").contains(secret));
        assert!(!format!("{err:?}").contains(secret));
    }

    #[test]
    fn error_display_is_informative() {
        assert_eq!(
            TimeParseError::TooLong.to_string(),
            "input is longer than 64 bytes"
        );
        assert_eq!(
            TimeParseError::Syntax {
                pos: 3,
                message: "expected digit"
            }
            .to_string(),
            "syntax error at byte 3: expected digit"
        );
        assert_eq!(
            TimeParseError::OutOfRange("month").to_string(),
            "value out of range: month"
        );
        assert!(TimeParseError::MissingOffset.to_string().contains("offset"));
    }

    proptest! {
        #[test]
        fn printable_ascii_never_panics(s in "[ -~]{0,80}") {
            let _ = parse_rfc3339(&s, None);
            let _ = parse_rfc3339(&s, Some(UtcOffset::UTC));
            let _ = UtcOffset::parse(&s);
        }

        #[test]
        fn timestamp_alphabet_never_panics(s in "[0-9TtZz:+. -]{0,70}") {
            let _ = parse_rfc3339(&s, None);
            let _ = UtcOffset::parse(&s);
        }

        #[test]
        fn arbitrary_unicode_never_panics(s in any::<String>()) {
            let _ = parse_rfc3339(&s, None);
            let _ = UtcOffset::parse(&s);
        }

        #[test]
        fn mutated_valid_timestamp_never_panics(
            idx in 0usize..30,
            byte in 0x20u8..0x7f,
            cut in 0usize..40,
        ) {
            let mut bytes = b"2026-10-07T12:34:56.123456789+05:30".to_vec();
            if let Some(slot) = bytes.get_mut(idx) {
                *slot = byte;
            }
            bytes.truncate(cut.max(1));
            if let Ok(s) = String::from_utf8(bytes) {
                let _ = parse_rfc3339(&s, None);
            }
        }

        #[test]
        fn round_trip_against_reference_formatter(
            nanos in any::<i64>(),
            offset_secs in -86_399_i32..=86_399,
        ) {
            // Whole-minute offsets only: the formatter emits +HH:MM, and
            // offsets above 23:59 are rejected by the parser.
            let offset_secs = offset_secs / 60 * 60;
            let text = format_local(nanos, offset_secs);
            prop_assert_eq!(parse_rfc3339(&text, None), Ok(nanos), "{}", text);
        }

        #[test]
        fn round_trip_default_offset(
            nanos in (-9_000_000_000_000_000_000_i64..9_000_000_000_000_000_000_i64),
            offset_minutes in -1_439_i32..=1_439,
        ) {
            let offset_secs = offset_minutes * 60;
            let text = format_local(nanos, offset_secs);
            let without_offset = &text[..text.len() - 6];
            let off = UtcOffset::from_seconds(offset_secs);
            prop_assert_eq!(parse_rfc3339(without_offset, off), Ok(nanos), "{}", text);
        }

        #[test]
        fn round_trip_compact_and_hour_offsets(
            nanos in (-9_000_000_000_000_000_000_i64..9_000_000_000_000_000_000_i64),
            hours in -23_i32..=23,
        ) {
            let offset_secs = hours * 3_600;
            let text = format_local(nanos, offset_secs);
            let head = &text[..text.len() - 6];
            let sign = if hours < 0 { '-' } else { '+' };
            let hh = hours.abs();
            prop_assert_eq!(parse_rfc3339(&format!("{head}{sign}{hh:02}"), None), Ok(nanos));
            prop_assert_eq!(parse_rfc3339(&format!("{head}{sign}{hh:02}00"), None), Ok(nanos));
        }
    }
}
