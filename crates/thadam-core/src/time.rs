//! Timestamps, time domains, and the per-event time record (ADR-0001, D15).

use std::fmt;
use std::time::Duration;

use crate::quality::{Quality, QualityFlags};

/// Nanoseconds since the Unix epoch, UTC. The representable range is about
/// plus or minus 292 years around 1970.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp(i64);

impl Timestamp {
    /// 1970-01-01T00:00:00Z.
    pub const EPOCH: Timestamp = Timestamp(0);
    /// The smallest representable timestamp.
    pub const MIN: Timestamp = Timestamp(i64::MIN);
    /// The largest representable timestamp.
    pub const MAX: Timestamp = Timestamp(i64::MAX);

    /// Builds a timestamp from nanoseconds since the Unix epoch.
    pub const fn from_nanos(nanos: i64) -> Self {
        Timestamp(nanos)
    }

    /// Builds a timestamp from microseconds, or `None` on overflow.
    pub fn from_micros(micros: i64) -> Option<Self> {
        micros.checked_mul(1_000).map(Timestamp)
    }

    /// Builds a timestamp from milliseconds, or `None` on overflow.
    pub fn from_millis(millis: i64) -> Option<Self> {
        millis.checked_mul(1_000_000).map(Timestamp)
    }

    /// Builds a timestamp from whole seconds, or `None` on overflow.
    pub fn from_secs(secs: i64) -> Option<Self> {
        secs.checked_mul(1_000_000_000).map(Timestamp)
    }

    /// Nanoseconds since the Unix epoch.
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// Microseconds since the Unix epoch, rounded toward negative infinity.
    pub const fn as_micros(self) -> i64 {
        self.0.div_euclid(1_000)
    }

    /// Milliseconds since the Unix epoch, rounded toward negative infinity.
    pub const fn as_millis(self) -> i64 {
        self.0.div_euclid(1_000_000)
    }

    /// Whole seconds since the Unix epoch, rounded toward negative infinity.
    pub const fn as_secs(self) -> i64 {
        self.0.div_euclid(1_000_000_000)
    }

    /// Adds a duration, or `None` on overflow.
    pub fn checked_add(self, d: Duration) -> Option<Self> {
        let nanos = i64::try_from(d.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Timestamp)
    }

    /// Subtracts a duration, or `None` on overflow.
    pub fn checked_sub(self, d: Duration) -> Option<Self> {
        let nanos = i64::try_from(d.as_nanos()).ok()?;
        self.0.checked_sub(nanos).map(Timestamp)
    }

    /// The time elapsed since `earlier`, or `None` if `earlier` is later.
    pub fn checked_duration_since(self, earlier: Timestamp) -> Option<Duration> {
        let diff = i128::from(self.0) - i128::from(earlier.0);
        u64::try_from(diff).ok().map(Duration::from_nanos)
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({}ns)", self.0)
    }
}

/// The clock a source timestamp was read from.
///
/// A non-UTC source time never becomes `event_time` without an explicit
/// mapping. The set is fixed once released.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
#[repr(u8)]
pub enum TimeDomain {
    /// UTC wall clock.
    Utc = 0,
    /// International Atomic Time (UTC plus 37 s under PTP).
    Tai = 1,
    /// Monotonic time since boot or process start.
    Monotonic = 2,
    /// Simulation time, such as ROS `/clock`.
    Sim = 3,
    /// The domain is not known.
    #[default]
    Unknown = 4,
}

impl TimeDomain {
    /// Every domain, in discriminant order.
    pub const ALL: [TimeDomain; 5] = [
        TimeDomain::Utc,
        TimeDomain::Tai,
        TimeDomain::Monotonic,
        TimeDomain::Sim,
        TimeDomain::Unknown,
    ];

    /// The stable lowercase name used in configuration and columns.
    pub const fn as_str(self) -> &'static str {
        match self {
            TimeDomain::Utc => "utc",
            TimeDomain::Tai => "tai",
            TimeDomain::Monotonic => "monotonic",
            TimeDomain::Sim => "sim",
            TimeDomain::Unknown => "unknown",
        }
    }
}

impl fmt::Display for TimeDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Violations of the time invariants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TimeError {
    /// `source_time` is set once at decode and never overwritten.
    #[error("source time is already set and cannot be overwritten")]
    SourceAlreadySet,
    /// A time correction must carry at least one quality flag.
    #[error("a time correction must set at least one quality flag")]
    EmptyCorrectionFlag,
}

const HAS_SOURCE: u8 = 0b01;
const HAS_CORRECTED: u8 = 0b10;

/// The times attached to an event.
///
/// - `ingest` is the edge receipt time. It is mandatory and never changes.
/// - `source` is what the device reported. It is set at most once.
/// - `corrected` is optional and only set through [`Times::correct`], which
///   always raises a quality flag so device time is never silently replaced.
///
/// Unset times are stored as the epoch so derived equality stays meaningful.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Times {
    source: Timestamp,
    ingest: Timestamp,
    corrected: Timestamp,
    domain: TimeDomain,
    has: u8,
}

impl Times {
    /// Times for an event received at `ingest`, with source times read from
    /// `domain`. Ingest time is mandatory, so no `Times` exists without one.
    pub fn new(ingest: Timestamp, domain: TimeDomain) -> Self {
        Times {
            source: Timestamp::EPOCH,
            ingest,
            corrected: Timestamp::EPOCH,
            domain,
            has: 0,
        }
    }

    /// Records the device-reported time. Fails if one is already set.
    pub fn set_source(&mut self, source: Timestamp) -> Result<(), TimeError> {
        if self.has & HAS_SOURCE != 0 {
            return Err(TimeError::SourceAlreadySet);
        }
        self.source = source;
        self.has |= HAS_SOURCE;
        Ok(())
    }

    /// Like [`Times::set_source`], for building a value in one expression.
    pub fn with_source(mut self, source: Timestamp) -> Result<Self, TimeError> {
        self.set_source(source)?;
        Ok(self)
    }

    /// Sets the corrected time and raises `flag` on `quality`.
    ///
    /// The source time is never touched. A later correction replaces an earlier
    /// one; the flags accumulate. `flag` must not be empty.
    pub fn correct(
        &mut self,
        ts: Timestamp,
        quality: &mut Quality,
        flag: QualityFlags,
    ) -> Result<(), TimeError> {
        if flag.is_empty() {
            return Err(TimeError::EmptyCorrectionFlag);
        }
        self.corrected = ts;
        self.has |= HAS_CORRECTED;
        quality.flags |= flag;
        Ok(())
    }

    /// The device-reported time, if any.
    pub fn source(&self) -> Option<Timestamp> {
        (self.has & HAS_SOURCE != 0).then_some(self.source)
    }

    /// The edge receipt time.
    pub fn ingest(&self) -> Timestamp {
        self.ingest
    }

    /// The corrected time, if a correction was applied.
    pub fn corrected(&self) -> Option<Timestamp> {
        (self.has & HAS_CORRECTED != 0).then_some(self.corrected)
    }

    /// The domain source times are read from.
    pub fn domain(&self) -> TimeDomain {
        self.domain
    }

    /// `corrected.or(source if the domain is Utc).or(null)`, as sinks
    /// materialize it.
    pub fn event_time(&self) -> Option<Timestamp> {
        self.corrected().or_else(|| match self.domain {
            TimeDomain::Utc => self.source(),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_conversions_round_toward_negative_infinity() {
        let t = Timestamp::from_nanos(-1);
        assert_eq!(t.as_micros(), -1);
        assert_eq!(t.as_millis(), -1);
        assert_eq!(t.as_secs(), -1);
        assert_eq!(
            Timestamp::from_secs(2).map(Timestamp::as_millis),
            Some(2000)
        );
    }

    #[test]
    fn constructors_detect_overflow() {
        assert_eq!(Timestamp::from_secs(i64::MAX), None);
        assert_eq!(Timestamp::from_millis(i64::MAX), None);
        assert_eq!(Timestamp::from_micros(i64::MAX), None);
    }

    #[test]
    fn duration_arithmetic_is_checked() {
        let t = Timestamp::from_nanos(10);
        assert_eq!(
            t.checked_add(Duration::from_nanos(5)),
            Some(Timestamp::from_nanos(15))
        );
        assert_eq!(Timestamp::MAX.checked_add(Duration::from_nanos(1)), None);
        assert_eq!(Timestamp::MIN.checked_sub(Duration::from_nanos(1)), None);
        assert_eq!(t.checked_add(Duration::MAX), None);
        assert_eq!(
            Timestamp::from_nanos(15).checked_duration_since(t),
            Some(Duration::from_nanos(5))
        );
        assert_eq!(t.checked_duration_since(Timestamp::from_nanos(15)), None);
    }

    #[test]
    fn source_time_is_set_once() {
        let mut times = Times::new(Timestamp::from_nanos(100), TimeDomain::Utc);
        assert_eq!(times.source(), None);
        times.set_source(Timestamp::from_nanos(7)).unwrap();
        assert_eq!(
            times.set_source(Timestamp::from_nanos(8)),
            Err(TimeError::SourceAlreadySet)
        );
        assert_eq!(times.source(), Some(Timestamp::from_nanos(7)));
    }

    #[test]
    fn correction_never_touches_source_and_always_flags() {
        let mut times = Times::new(Timestamp::from_nanos(100), TimeDomain::Utc)
            .with_source(Timestamp::from_nanos(7))
            .unwrap();
        let mut quality = Quality::default();

        assert_eq!(
            times.correct(
                Timestamp::from_nanos(50),
                &mut quality,
                QualityFlags::empty()
            ),
            Err(TimeError::EmptyCorrectionFlag)
        );
        assert_eq!(times.corrected(), None);
        assert!(quality.flags.is_empty());

        times
            .correct(
                Timestamp::from_nanos(50),
                &mut quality,
                QualityFlags::CLOCK_SKEW,
            )
            .unwrap();
        assert_eq!(times.source(), Some(Timestamp::from_nanos(7)));
        assert_eq!(times.corrected(), Some(Timestamp::from_nanos(50)));
        assert!(quality.flags.contains(QualityFlags::CLOCK_SKEW));

        times
            .correct(
                Timestamp::from_nanos(60),
                &mut quality,
                QualityFlags::MISSING_TIME,
            )
            .unwrap();
        assert!(
            quality
                .flags
                .contains(QualityFlags::CLOCK_SKEW | QualityFlags::MISSING_TIME)
        );
    }

    #[test]
    fn event_time_follows_the_domain_rule() {
        for domain in TimeDomain::ALL {
            let times = Times::new(Timestamp::from_nanos(100), domain)
                .with_source(Timestamp::from_nanos(7))
                .unwrap();
            let expected = (domain == TimeDomain::Utc).then_some(Timestamp::from_nanos(7));
            assert_eq!(times.event_time(), expected, "{domain}");
        }
        let mut times = Times::new(Timestamp::from_nanos(100), TimeDomain::Monotonic);
        let mut quality = Quality::default();
        times
            .correct(
                Timestamp::from_nanos(42),
                &mut quality,
                QualityFlags::MISSING_TIME,
            )
            .unwrap();
        assert_eq!(times.event_time(), Some(Timestamp::from_nanos(42)));
    }

    #[test]
    fn ingest_time_is_always_present() {
        let times = Times::new(Timestamp::from_nanos(-5), TimeDomain::Unknown);
        assert_eq!(times.ingest(), Timestamp::from_nanos(-5));
    }
}
