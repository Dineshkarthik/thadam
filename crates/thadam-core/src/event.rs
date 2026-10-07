//! The canonical event and its invariant-preserving builder (ADR-0001).

use std::fmt;

use bytes::Bytes;

use crate::identity::AssetPath;
use crate::position::WalPos;
use crate::quality::{Quality, QualityFlags, QualityStatus};
use crate::record::Record;
use crate::text::{Sym, Utf8Bytes};
use crate::time::{TimeError, Times, Timestamp};

/// A 128-bit event identifier.
///
/// Stored as two words so the struct needs only 8-byte alignment. By default
/// it is `xxh3_128(epoch, frame_seq, record_index)`; a dataset may derive it
/// from content instead (ADR-0001). The derivation lives in `thadam-transform`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct EventId {
    hi: u64,
    lo: u64,
}

impl EventId {
    /// Builds an id from its 128-bit value.
    pub const fn from_u128(v: u128) -> Self {
        EventId {
            hi: (v >> 64) as u64,
            lo: v as u64,
        }
    }

    /// The 128-bit value.
    pub const fn as_u128(self) -> u128 {
        ((self.hi as u128) << 64) | self.lo as u128
    }

    /// Builds an id from 16 big-endian bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self::from_u128(u128::from_be_bytes(bytes))
    }

    /// The id as 16 big-endian bytes.
    pub const fn to_bytes(self) -> [u8; 16] {
        self.as_u128().to_be_bytes()
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EventId({self})")
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.as_u128())
    }
}

/// An opaque pointer to a large object stored outside the event stream.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ObjectRef {
    /// Where the object lives.
    pub uri: Utf8Bytes,
    /// Its size in bytes.
    pub size_bytes: u64,
    /// Its media type, if known.
    pub content_type: Option<Sym>,
}

impl ObjectRef {
    /// A reference to `size_bytes` at `uri`.
    pub fn new(uri: impl Into<Utf8Bytes>, size_bytes: u64) -> Self {
        ObjectRef {
            uri: uri.into(),
            size_bytes,
            content_type: None,
        }
    }
}

/// An undecoded payload kept as bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RawPayload {
    /// The media type.
    pub content_type: Sym,
    /// The bytes.
    pub bytes: Bytes,
}

/// What an event carries. The variants that do not fit inline are boxed so
/// `Event` stays within its 128-byte budget.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Payload {
    /// A decoded record.
    Record(Record),
    /// Undecoded bytes with a media type.
    Raw(Box<RawPayload>),
    /// A reference to a large object.
    ObjectRef(Box<ObjectRef>),
}

impl Payload {
    /// An undecoded payload.
    pub fn raw(content_type: impl Into<Sym>, bytes: Bytes) -> Payload {
        Payload::Raw(Box::new(RawPayload {
            content_type: content_type.into(),
            bytes,
        }))
    }

    /// An object reference payload.
    pub fn object_ref(object: ObjectRef) -> Payload {
        Payload::ObjectRef(Box::new(object))
    }

    /// The record, if this is one.
    pub fn as_record(&self) -> Option<&Record> {
        match self {
            Payload::Record(r) => Some(r),
            _ => None,
        }
    }

    /// The record for in-place edits, if this is one.
    pub fn as_record_mut(&mut self) -> Option<&mut Record> {
        match self {
            Payload::Record(r) => Some(r),
            _ => None,
        }
    }
}

impl From<Record> for Payload {
    fn from(r: Record) -> Self {
        Payload::Record(r)
    }
}

/// Why an event could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EventBuildError {
    /// No payload was supplied.
    #[error("an event needs a payload")]
    MissingPayload,
    /// The times carry a correction but the quality carries no flag.
    #[error("a time correction requires at least one quality flag")]
    CorrectionWithoutFlag,
}

/// A captured, decoded event.
///
/// Fields are private; accessors and setters preserve these invariants:
///
/// - the event has an id, an ingest time, a WAL position, and a payload;
/// - `source_time` is set at most once and never overwritten;
/// - a corrected time always comes with a quality flag.
///
/// The size budget is 128 bytes excluding heap allocations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    id: EventId,
    times: Times,
    quality: Quality,
    asset: AssetPath,
    payload: Payload,
    wal_pos: WalPos,
    sample_idx: Option<u32>,
}

const _: () = assert!(
    size_of::<Event>() <= 128,
    "Event exceeds its 128-byte budget"
);

impl Event {
    /// Starts building an event.
    ///
    /// `times` already carries the mandatory ingest time. `wal_pos` is the WAL
    /// frame the event came from, which the lineage columns record (the epoch
    /// is on the enclosing batch).
    pub fn builder(id: EventId, wal_pos: WalPos, times: Times) -> EventBuilder {
        EventBuilder {
            id,
            wal_pos,
            times,
            quality: Quality::default(),
            asset: AssetPath::empty(),
            payload: None,
            sample_idx: None,
        }
    }

    /// A builder pre-filled with this event's metadata and the given payload
    /// slot left empty, for steps that derive new events (such as `explode`).
    pub fn to_builder(&self) -> EventBuilder {
        EventBuilder {
            id: self.id,
            wal_pos: self.wal_pos,
            times: self.times,
            quality: self.quality.clone(),
            asset: self.asset.clone(),
            payload: None,
            sample_idx: self.sample_idx,
        }
    }

    /// The event id.
    pub fn id(&self) -> EventId {
        self.id
    }

    /// Source, ingest, and corrected times.
    pub fn times(&self) -> &Times {
        &self.times
    }

    /// The event's quality.
    pub fn quality(&self) -> &Quality {
        &self.quality
    }

    /// The identity of the asset that produced the event.
    pub fn asset(&self) -> &AssetPath {
        &self.asset
    }

    /// The payload.
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// The payload for in-place edits. The payload is never absent, so this
    /// cannot break an invariant.
    pub fn payload_mut(&mut self) -> &mut Payload {
        &mut self.payload
    }

    /// The WAL frame the event was captured in.
    pub fn wal_pos(&self) -> WalPos {
        self.wal_pos
    }

    /// The sample index for events produced by `explode`.
    pub fn sample_idx(&self) -> Option<u32> {
        self.sample_idx
    }

    /// Corrects the event time and raises `flag`. The source time is untouched.
    pub fn correct_time(&mut self, ts: Timestamp, flag: QualityFlags) -> Result<(), TimeError> {
        self.times.correct(ts, &mut self.quality, flag)
    }

    /// Raises quality flags.
    pub fn add_quality_flags(&mut self, flags: QualityFlags) {
        self.quality.flags |= flags;
    }

    /// Sets the quality status.
    pub fn set_quality_status(&mut self, status: QualityStatus) {
        self.quality.status = status;
    }

    /// Sets or clears the quality reason.
    pub fn set_quality_reason(&mut self, reason: Option<Sym>) {
        self.quality.reason = reason;
    }

    /// Replaces the asset path.
    pub fn set_asset(&mut self, asset: AssetPath) {
        self.asset = asset;
    }

    /// Sets or clears the sample index.
    pub fn set_sample_idx(&mut self, idx: Option<u32>) {
        self.sample_idx = idx;
    }

    /// Replaces the event id. Used by steps that derive a content-based id.
    pub fn set_id(&mut self, id: EventId) {
        self.id = id;
    }

    /// Consumes the event, returning its payload.
    pub fn into_payload(self) -> Payload {
        self.payload
    }
}

/// Builds an [`Event`], enforcing its invariants at [`EventBuilder::build`].
#[derive(Clone, Debug)]
pub struct EventBuilder {
    id: EventId,
    wal_pos: WalPos,
    times: Times,
    quality: Quality,
    asset: AssetPath,
    payload: Option<Payload>,
    sample_idx: Option<u32>,
}

impl EventBuilder {
    /// Sets the payload. Required.
    #[must_use]
    pub fn payload(mut self, payload: impl Into<Payload>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    /// Sets the quality.
    #[must_use]
    pub fn quality(mut self, quality: Quality) -> Self {
        self.quality = quality;
        self
    }

    /// Sets the asset path.
    #[must_use]
    pub fn asset(mut self, asset: AssetPath) -> Self {
        self.asset = asset;
        self
    }

    /// Sets the sample index.
    #[must_use]
    pub fn sample_idx(mut self, idx: u32) -> Self {
        self.sample_idx = Some(idx);
        self
    }

    /// Sets the event id.
    #[must_use]
    pub fn id(mut self, id: EventId) -> Self {
        self.id = id;
        self
    }

    /// Checks the invariants and builds the event.
    pub fn build(self) -> Result<Event, EventBuildError> {
        let payload = self.payload.ok_or(EventBuildError::MissingPayload)?;
        if self.times.corrected().is_some() && self.quality.flags.is_empty() {
            return Err(EventBuildError::CorrectionWithoutFlag);
        }
        Ok(Event {
            id: self.id,
            times: self.times,
            quality: self.quality,
            asset: self.asset,
            payload,
            wal_pos: self.wal_pos,
            sample_idx: self.sample_idx,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shape::{FieldDef, FieldType, Shape, ShapeRef};
    use crate::time::TimeDomain;
    use crate::value::Value;

    fn record() -> Record {
        let shape = ShapeRef::new(Shape::new(vec![FieldDef::new("v", FieldType::I64)]).unwrap());
        Record::new(shape, vec![Value::I64(1)]).unwrap()
    }

    fn times() -> Times {
        Times::new(Timestamp::from_nanos(1_000), TimeDomain::Utc)
    }

    fn builder() -> EventBuilder {
        Event::builder(EventId::from_u128(42), WalPos::new(9), times())
    }

    #[test]
    fn event_fits_the_size_budget() {
        assert!(
            size_of::<Event>() <= 128,
            "Event is {} bytes",
            size_of::<Event>()
        );
    }

    #[test]
    fn payload_is_required() {
        assert_eq!(
            builder().build().err(),
            Some(EventBuildError::MissingPayload)
        );
        assert!(builder().payload(record()).build().is_ok());
    }

    #[test]
    fn built_event_always_has_an_ingest_time() {
        let e = builder().payload(record()).build().unwrap();
        assert_eq!(e.times().ingest(), Timestamp::from_nanos(1_000));
        assert_eq!(e.wal_pos(), WalPos::new(9));
        assert_eq!(e.id(), EventId::from_u128(42));
    }

    #[test]
    fn correction_without_a_flag_cannot_be_built() {
        let mut t = times();
        let mut throwaway = Quality::default();
        t.correct(
            Timestamp::from_nanos(5),
            &mut throwaway,
            QualityFlags::CLOCK_SKEW,
        )
        .unwrap();
        let err = Event::builder(EventId::default(), WalPos::ZERO, t)
            .payload(record())
            .build()
            .err();
        assert_eq!(err, Some(EventBuildError::CorrectionWithoutFlag));

        let quality = Quality {
            flags: QualityFlags::CLOCK_SKEW,
            ..Quality::default()
        };
        assert!(
            Event::builder(EventId::default(), WalPos::ZERO, t)
                .payload(record())
                .quality(quality)
                .build()
                .is_ok()
        );
    }

    #[test]
    fn correct_time_flags_and_preserves_source_time() {
        let t = times().with_source(Timestamp::from_nanos(77)).unwrap();
        let mut e = Event::builder(EventId::default(), WalPos::ZERO, t)
            .payload(record())
            .build()
            .unwrap();
        assert_eq!(
            e.correct_time(Timestamp::from_nanos(5), QualityFlags::empty()),
            Err(TimeError::EmptyCorrectionFlag)
        );
        assert_eq!(e.times().corrected(), None);

        e.correct_time(Timestamp::from_nanos(5), QualityFlags::MISSING_TIME)
            .unwrap();
        assert_eq!(e.times().source(), Some(Timestamp::from_nanos(77)));
        assert_eq!(e.times().corrected(), Some(Timestamp::from_nanos(5)));
        assert!(e.quality().flags.contains(QualityFlags::MISSING_TIME));
    }

    #[test]
    fn to_builder_carries_metadata_but_not_the_payload() {
        let path = AssetPath::new([("robot", "r1")]).unwrap();
        let e = builder()
            .asset(path.clone())
            .sample_idx(3)
            .payload(record())
            .build()
            .unwrap();
        assert_eq!(
            e.to_builder().build().err(),
            Some(EventBuildError::MissingPayload)
        );
        let d = e
            .to_builder()
            .id(EventId::from_u128(43))
            .sample_idx(4)
            .payload(record())
            .build()
            .unwrap();
        assert_eq!(d.asset(), &path);
        assert_eq!(d.sample_idx(), Some(4));
        assert_eq!(d.times(), e.times());
        assert_eq!(e.sample_idx(), Some(3));
    }

    #[test]
    fn payload_variants() {
        let mut p = Payload::from(record());
        assert!(p.as_record().is_some() && p.as_record_mut().is_some());
        let raw = Payload::raw("application/octet-stream", Bytes::from_static(b"\x00\x01"));
        assert!(raw.as_record().is_none());
        let obj = Payload::object_ref(ObjectRef::new("s3://b/k", 10));
        assert!(matches!(obj, Payload::ObjectRef(o) if o.size_bytes == 10));
    }

    #[test]
    fn event_id_round_trips() {
        let id = EventId::from_u128(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        assert_eq!(id.as_u128(), 0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        assert_eq!(EventId::from_bytes(id.to_bytes()), id);
        assert_eq!(id.to_string(), "0123456789abcdef0011223344556677");
    }
}
