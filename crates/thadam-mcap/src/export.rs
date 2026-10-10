//! The MCAP writer: captured WAL messages in, one indexed MCAP file out
//! (ADR-0015, draft).
//!
//! One MCAP channel is written per `(thadam source, topic)`, tagged with a
//! `thadam.source` metadata entry. A message keeps its original encoding when
//! it carries `mcap.message_encoding`: its schema is named by
//! `mcap.schema_name` and `mcap.schema_encoding` and resolved through the
//! caller's [`SchemaResolver`], since schema text is not in the WAL. Without
//! that attribute the source's [`DefaultEncoding`] applies and the channel has
//! no schema. A schema the resolver cannot supply is written as schema id 0
//! and listed in the `thadam.unresolved_schemas` metadata record at finish.
//!
//! WAL gaps are `thadam.gap` metadata records, and `thadam.provenance` is written
//! at finish. The output is chunked with the MCAP library's default zstd
//! compression and carries a summary section, so readers can seek.

use std::collections::BTreeMap;
use std::io::{Seek, Write};

use bytes::Bytes;
use mcap::records::{MessageHeader, Metadata};
use mcap::{WriteOptions, Writer};
use thadam_core::{Attributes, RawMessage, Timestamp};

use crate::error::McapError;
use crate::source::attr;

const SOURCE_KEY: &str = "thadam.source";
const GAP_RECORD: &str = "thadam.gap";
const UNRESOLVED_RECORD: &str = "thadam.unresolved_schemas";
const PROVENANCE_RECORD: &str = "thadam.provenance";

/// The message encoding for a source's messages that carry no
/// `mcap.message_encoding` attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultEncoding {
    /// `json`, with no schema.
    Json,
    /// `application/octet-stream`, with no schema.
    OctetStream,
}

impl DefaultEncoding {
    /// The MCAP message encoding string.
    pub fn as_str(self) -> &'static str {
        match self {
            DefaultEncoding::Json => "json",
            DefaultEncoding::OctetStream => "application/octet-stream",
        }
    }
}

/// Supplies schema text for a schema named in a message's `mcap.*` attributes.
///
/// Implemented for any `Fn(&str, &str) -> Option<Bytes>`, so a closure works
/// directly. Returning `None` is not an error: the channel is written without
/// a schema and reported as unresolved.
pub trait SchemaResolver {
    /// The schema text for `name` in `encoding`, or `None` if it is unknown.
    fn resolve(&self, name: &str, encoding: &str) -> Option<Bytes>;
}

impl<F> SchemaResolver for F
where
    F: Fn(&str, &str) -> Option<Bytes>,
{
    fn resolve(&self, name: &str, encoding: &str) -> Option<Bytes> {
        self(name, encoding)
    }
}

/// Export configuration: the fallback encoding, per-source overrides, and the
/// schema resolver.
pub struct ExportOptions {
    default_encoding: DefaultEncoding,
    source_encodings: BTreeMap<String, DefaultEncoding>,
    resolver: Box<dyn SchemaResolver>,
}

impl ExportOptions {
    /// Options where messages without `mcap.message_encoding` use
    /// `default_encoding`, unless a source is overridden with
    /// [`with_source_encoding`](Self::with_source_encoding).
    pub fn new(default_encoding: DefaultEncoding, resolver: impl SchemaResolver + 'static) -> Self {
        ExportOptions {
            default_encoding,
            source_encodings: BTreeMap::new(),
            resolver: Box::new(resolver),
        }
    }

    /// Uses `encoding` for messages from `source` that carry no
    /// `mcap.message_encoding` attribute.
    #[must_use]
    pub fn with_source_encoding(
        mut self,
        source: impl Into<String>,
        encoding: DefaultEncoding,
    ) -> Self {
        self.source_encodings.insert(source.into(), encoding);
        self
    }

    fn encoding_for(&self, source: &str) -> DefaultEncoding {
        self.source_encodings
            .get(source)
            .copied()
            .unwrap_or(self.default_encoding)
    }
}

/// Why a WAL gap was recorded. The string forms are the `kind` field of a
/// `thadam.gap` record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GapKind {
    /// Frames failed their checksum or framing.
    Corrupt,
    /// Frames were removed by retention before they were exported.
    Evicted,
    /// Positions that are absent from the WAL without a recorded reason.
    Missing,
}

impl GapKind {
    /// The `kind` field value: `corrupt`, `evicted`, or `missing`.
    pub fn as_str(self) -> &'static str {
        match self {
            GapKind::Corrupt => "corrupt",
            GapKind::Evicted => "evicted",
            GapKind::Missing => "missing",
        }
    }
}

/// File-level provenance, written as the `thadam.provenance` record at finish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance {
    /// The version of Thadam that wrote the file.
    pub thadam_version: String,
    /// The WAL epoch the exported positions belong to.
    pub wal_epoch: u64,
    /// The first WAL position exported.
    pub first_pos: u64,
    /// The last WAL position exported.
    pub last_pos: u64,
    /// The identifier of the node that produced the WAL.
    pub node_id: String,
}

/// A channel whose schema text the resolver could not supply. The channel is
/// written with schema id 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnresolvedSchema {
    /// The thadam source the channel belongs to.
    pub source: String,
    /// The channel's topic.
    pub topic: String,
    /// The schema name from `mcap.schema_name`.
    pub schema_name: String,
    /// The schema encoding from `mcap.schema_encoding`.
    pub schema_encoding: String,
}

/// What an export wrote, returned by [`McapExporter::finish`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExportReport {
    /// Messages written.
    pub messages: u64,
    /// Channels written, one per `(source, topic)`.
    pub channels: usize,
    /// `thadam.gap` records written.
    pub gaps: u64,
    /// Channels written without schema text, in the order they were created.
    pub unresolved_schemas: Vec<UnresolvedSchema>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChannelSpec {
    encoding: String,
    schema_name: String,
    schema_encoding: String,
}

struct ChannelState {
    id: u16,
    spec: ChannelSpec,
    next_sequence: u32,
}

/// Writes captured messages to an MCAP file.
///
/// Create it with [`McapExporter::new`], feed it with
/// [`write_raw`](Self::write_raw) and [`add_gap`](Self::add_gap), and call
/// [`finish`](Self::finish) exactly once to write the trailing metadata and the
/// summary. Writing through a buffered sink is the caller's responsibility:
/// `finish` does not flush `W`, and dropping the exporter drops `W`, so a
/// `BufWriter` must be flushed explicitly (or passed as `&mut`) before the
/// exporter is dropped.
pub struct McapExporter<W: Write + Seek> {
    writer: Writer<W>,
    options: ExportOptions,
    channels: BTreeMap<(String, String), ChannelState>,
    unresolved: Vec<UnresolvedSchema>,
    messages: u64,
    gaps: u64,
}

impl<W: Write + Seek> McapExporter<W> {
    /// Starts an MCAP file on `writer`.
    pub fn new(writer: W, options: ExportOptions) -> Result<Self, McapError> {
        let writer = Writer::with_options(writer, WriteOptions::default()).map_err(write_error)?;
        Ok(McapExporter {
            writer,
            options,
            channels: BTreeMap::new(),
            unresolved: Vec::new(),
            messages: 0,
            gaps: 0,
        })
    }

    /// Writes one message from thadam `source`.
    ///
    /// The channel is `(source, topic)`, where topic is `mcap.topic`, else
    /// `topic`, else the source name. The log time is the ingest time. The
    /// publish time is `mcap.publish_time` when it parses, else the log time.
    /// The sequence is `mcap.sequence` when it parses, else a per-channel
    /// counter that wraps at `u32::MAX`.
    ///
    /// Messages on one `(source, topic)` must agree on message encoding and
    /// schema; a disagreement is an export error, not a second channel.
    pub fn write_raw(&mut self, source: &str, msg: &RawMessage) -> Result<(), McapError> {
        let log_time = log_time_of(source, msg.ingest_time)?;
        let publish_time = publish_time_of(&msg.attrs, log_time);
        let explicit_sequence = sequence_of(&msg.attrs);
        let topic = topic_of(source, &msg.attrs);
        let spec = channel_spec(&self.options, source, &msg.attrs);
        let channel = self.channel_mut(source, topic, spec)?;
        let sequence = explicit_sequence.unwrap_or(channel.next_sequence);
        channel.next_sequence = channel.next_sequence.wrapping_add(1);
        let header = MessageHeader {
            channel_id: channel.id,
            sequence,
            log_time,
            publish_time,
        };
        self.writer
            .write_to_known_channel(&header, &msg.payload)
            .map_err(write_error)?;
        self.messages += 1;
        Ok(())
    }

    /// Records a WAL gap as a `thadam.gap` metadata record. `first` and `last`
    /// are inclusive positions; `last` may equal `first`.
    pub fn add_gap(
        &mut self,
        kind: GapKind,
        epoch: u64,
        first: u64,
        last: u64,
    ) -> Result<(), McapError> {
        if last < first {
            return Err(McapError::Export {
                detail: format!(
                    "{} gap in epoch {epoch}: last position {last} precedes first {first}",
                    kind.as_str()
                ),
            });
        }
        let fields = BTreeMap::from([
            ("kind".to_owned(), kind.as_str().to_owned()),
            ("epoch".to_owned(), epoch.to_string()),
            ("first".to_owned(), first.to_string()),
            ("last".to_owned(), last.to_string()),
        ]);
        self.write_record(GAP_RECORD, fields)?;
        self.gaps += 1;
        Ok(())
    }

    /// Writes the trailing metadata and the summary, and reports what was
    /// written.
    ///
    /// `thadam.unresolved_schemas` is written only when some channel lacked
    /// schema text. Its keys are `channel.N.source`, `channel.N.topic`,
    /// `channel.N.schema`, and `channel.N.schema_encoding`, where `N` indexes
    /// [`ExportReport::unresolved_schemas`]. `thadam.provenance` is always
    /// written.
    pub fn finish(mut self, provenance: Provenance) -> Result<ExportReport, McapError> {
        if !self.unresolved.is_empty() {
            let mut fields = BTreeMap::new();
            for (n, u) in self.unresolved.iter().enumerate() {
                fields.insert(format!("channel.{n}.source"), u.source.clone());
                fields.insert(format!("channel.{n}.topic"), u.topic.clone());
                fields.insert(format!("channel.{n}.schema"), u.schema_name.clone());
                fields.insert(
                    format!("channel.{n}.schema_encoding"),
                    u.schema_encoding.clone(),
                );
            }
            self.write_record(UNRESOLVED_RECORD, fields)?;
        }
        let fields = BTreeMap::from([
            ("thadam_version".to_owned(), provenance.thadam_version),
            ("wal_epoch".to_owned(), provenance.wal_epoch.to_string()),
            ("first_pos".to_owned(), provenance.first_pos.to_string()),
            ("last_pos".to_owned(), provenance.last_pos.to_string()),
            ("node_id".to_owned(), provenance.node_id),
        ]);
        self.write_record(PROVENANCE_RECORD, fields)?;
        self.writer.finish().map_err(write_error)?;
        Ok(ExportReport {
            messages: self.messages,
            channels: self.channels.len(),
            gaps: self.gaps,
            unresolved_schemas: self.unresolved,
        })
    }

    fn write_record(
        &mut self,
        name: &str,
        metadata: BTreeMap<String, String>,
    ) -> Result<(), McapError> {
        self.writer
            .write_metadata(&Metadata {
                name: name.to_owned(),
                metadata,
            })
            .map_err(write_error)
    }

    fn channel_mut(
        &mut self,
        source: &str,
        topic: &str,
        spec: ChannelSpec,
    ) -> Result<&mut ChannelState, McapError> {
        let key = (source.to_owned(), topic.to_owned());
        match self.channels.get(&key).map(|state| state.spec == spec) {
            Some(true) => {}
            Some(false) => {
                return Err(McapError::Export {
                    detail: format!(
                        "source `{source}` topic `{topic}`: messages disagree on message encoding or schema"
                    ),
                });
            }
            None => {
                let schema_id = self.register_schema(source, topic, &spec)?;
                let metadata = BTreeMap::from([(SOURCE_KEY.to_owned(), source.to_owned())]);
                let id = self
                    .writer
                    .add_channel(schema_id, topic, &spec.encoding, &metadata)
                    .map_err(write_error)?;
                self.channels.insert(
                    key.clone(),
                    ChannelState {
                        id,
                        spec,
                        next_sequence: 0,
                    },
                );
            }
        }
        self.channels
            .get_mut(&key)
            .ok_or_else(|| McapError::Export {
                detail: format!("source `{source}` topic `{topic}`: channel bookkeeping lost"),
            })
    }

    fn register_schema(
        &mut self,
        source: &str,
        topic: &str,
        spec: &ChannelSpec,
    ) -> Result<u16, McapError> {
        if spec.schema_name.is_empty() {
            return Ok(0);
        }
        match self
            .options
            .resolver
            .resolve(&spec.schema_name, &spec.schema_encoding)
        {
            Some(text) => self
                .writer
                .add_schema(&spec.schema_name, &spec.schema_encoding, &text)
                .map_err(write_error),
            None => {
                self.unresolved.push(UnresolvedSchema {
                    source: source.to_owned(),
                    topic: topic.to_owned(),
                    schema_name: spec.schema_name.clone(),
                    schema_encoding: spec.schema_encoding.clone(),
                });
                Ok(0)
            }
        }
    }
}

fn write_error(error: mcap::McapError) -> McapError {
    McapError::Write {
        detail: error.to_string(),
    }
}

fn log_time_of(source: &str, ingest: Timestamp) -> Result<u64, McapError> {
    u64::try_from(ingest.as_nanos()).map_err(|_| McapError::Export {
        detail: format!("source `{source}`: ingest time precedes the Unix epoch"),
    })
}

fn publish_time_of(attrs: &Attributes, log_time: u64) -> u64 {
    attrs
        .get(attr::MCAP_PUBLISH_TIME)
        .and_then(|value| value.parse().ok())
        .unwrap_or(log_time)
}

fn sequence_of(attrs: &Attributes) -> Option<u32> {
    attrs
        .get(attr::MCAP_SEQUENCE)
        .and_then(|value| value.parse().ok())
}

fn topic_of<'a>(source: &'a str, attrs: &'a Attributes) -> &'a str {
    [attr::MCAP_TOPIC, attr::TOPIC]
        .into_iter()
        .find_map(|key| attrs.get(key).filter(|topic| !topic.is_empty()))
        .unwrap_or(source)
}

fn channel_spec(options: &ExportOptions, source: &str, attrs: &Attributes) -> ChannelSpec {
    match attrs
        .get(attr::MCAP_MESSAGE_ENCODING)
        .filter(|encoding| !encoding.is_empty())
    {
        Some(encoding) => ChannelSpec {
            encoding: encoding.to_owned(),
            schema_name: attrs
                .get(attr::MCAP_SCHEMA_NAME)
                .unwrap_or_default()
                .to_owned(),
            schema_encoding: attrs
                .get(attr::MCAP_SCHEMA_ENCODING)
                .unwrap_or_default()
                .to_owned(),
        },
        None => ChannelSpec {
            encoding: options.encoding_for(source).as_str().to_owned(),
            schema_name: String::new(),
            schema_encoding: String::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    const LOG: u64 = 1_758_000_000_000_000_042;

    fn resolver(name: &str, encoding: &str) -> Option<Bytes> {
        (name == "example/Known" && encoding == "ros2msg").then(|| Bytes::from_static(b"int32 x\n"))
    }

    fn options() -> ExportOptions {
        ExportOptions::new(DefaultEncoding::OctetStream, resolver)
            .with_source_encoding("annotations", DefaultEncoding::Json)
    }

    fn provenance() -> Provenance {
        Provenance {
            thadam_version: "0.0.0-test".into(),
            wal_epoch: 7,
            first_pos: 100,
            last_pos: 200,
            node_id: "node-a".into(),
        }
    }

    fn msg(ingest: i64, attrs: &[(&str, &str)], payload: &'static [u8]) -> RawMessage {
        RawMessage {
            ingest_time: Timestamp::from_nanos(ingest),
            attrs: attrs.iter().copied().collect(),
            payload: Bytes::from_static(payload),
        }
    }

    fn export(
        build: impl FnOnce(&mut McapExporter<&mut Cursor<Vec<u8>>>),
    ) -> (Vec<u8>, ExportReport) {
        let mut buf = Cursor::new(Vec::new());
        let report = {
            let mut exporter = McapExporter::new(&mut buf, options()).expect("new");
            build(&mut exporter);
            exporter.finish(provenance()).expect("finish")
        };
        (buf.into_inner(), report)
    }

    fn metadata_records(bytes: &[u8], name: &str) -> Vec<BTreeMap<String, String>> {
        let summary = mcap::read::Summary::read(bytes)
            .expect("summary")
            .expect("present");
        summary
            .metadata_indexes
            .iter()
            .filter(|index| index.name == name)
            .map(|index| {
                mcap::read::metadata(bytes, index)
                    .expect("metadata")
                    .metadata
            })
            .collect()
    }

    struct ChannelInfo {
        topic: String,
        schema_name: Option<String>,
        metadata: BTreeMap<String, String>,
    }

    fn channels(bytes: &[u8]) -> Vec<ChannelInfo> {
        let summary = mcap::read::Summary::read(bytes)
            .expect("summary")
            .expect("present");
        let mut out: Vec<ChannelInfo> = summary
            .channels
            .values()
            .map(|c| ChannelInfo {
                topic: c.topic.clone(),
                schema_name: c.schema.as_ref().map(|s| s.name.clone()),
                metadata: c.metadata.clone(),
            })
            .collect();
        out.sort_by_key(|c| (c.topic.clone(), c.metadata.get("thadam.source").cloned()));
        out
    }

    #[test]
    fn channels_are_keyed_by_source_and_topic() {
        let (bytes, report) = export(|e| {
            e.write_raw("a", &msg(1, &[(attr::MCAP_TOPIC, "/x")], b"1"))
                .unwrap();
            e.write_raw("a", &msg(2, &[(attr::MCAP_TOPIC, "/x")], b"2"))
                .unwrap();
            e.write_raw("a", &msg(3, &[(attr::MCAP_TOPIC, "/y")], b"3"))
                .unwrap();
            e.write_raw("b", &msg(4, &[(attr::MCAP_TOPIC, "/x")], b"4"))
                .unwrap();
        });
        assert_eq!(report.messages, 4);
        assert_eq!(report.channels, 3);
        let channels = channels(&bytes);
        let sources: Vec<(&str, &str)> = channels
            .iter()
            .map(|c| (c.topic.as_str(), c.metadata["thadam.source"].as_str()))
            .collect();
        assert_eq!(sources, [("/x", "a"), ("/x", "b"), ("/y", "a")]);
    }

    #[test]
    fn topic_prefers_mcap_topic_then_topic_then_source_name() {
        let both = msg(1, &[(attr::MCAP_TOPIC, "/m"), (attr::TOPIC, "/t")], b"");
        assert_eq!(topic_of("src", &both.attrs), "/m");
        let generic = msg(1, &[(attr::TOPIC, "/t")], b"");
        assert_eq!(topic_of("src", &generic.attrs), "/t");
        let empty = msg(1, &[(attr::MCAP_TOPIC, "")], b"");
        assert_eq!(topic_of("src", &empty.attrs), "src");
        let none = msg(1, &[], b"");
        assert_eq!(topic_of("src", &none.attrs), "src");
    }

    #[test]
    fn encoding_comes_from_the_message_attrs_or_the_source_default() {
        let explicit = msg(
            1,
            &[
                (attr::MCAP_MESSAGE_ENCODING, "cdr"),
                (attr::MCAP_SCHEMA_NAME, "example/Known"),
                (attr::MCAP_SCHEMA_ENCODING, "ros2msg"),
            ],
            b"",
        );
        let spec = channel_spec(&options(), "robot", &explicit.attrs);
        assert_eq!(spec.encoding, "cdr");
        assert_eq!(spec.schema_name, "example/Known");
        assert_eq!(spec.schema_encoding, "ros2msg");

        let plain = msg(1, &[], b"");
        let spec = channel_spec(&options(), "robot", &plain.attrs);
        assert_eq!(spec.encoding, "application/octet-stream");
        assert!(spec.schema_name.is_empty());

        let json = channel_spec(&options(), "annotations", &plain.attrs);
        assert_eq!(json.encoding, "json");

        let empty_encoding = msg(1, &[(attr::MCAP_MESSAGE_ENCODING, "")], b"");
        assert_eq!(
            channel_spec(&options(), "robot", &empty_encoding.attrs).encoding,
            "application/octet-stream"
        );
    }

    #[test]
    fn resolved_schema_is_attached_and_unresolved_schema_is_recorded() {
        let (bytes, report) = export(|e| {
            e.write_raw(
                "robot",
                &msg(
                    1,
                    &[
                        (attr::MCAP_TOPIC, "/known"),
                        (attr::MCAP_MESSAGE_ENCODING, "cdr"),
                        (attr::MCAP_SCHEMA_NAME, "example/Known"),
                        (attr::MCAP_SCHEMA_ENCODING, "ros2msg"),
                    ],
                    b"k",
                ),
            )
            .unwrap();
            e.write_raw(
                "robot",
                &msg(
                    2,
                    &[
                        (attr::MCAP_TOPIC, "/lost"),
                        (attr::MCAP_MESSAGE_ENCODING, "cdr"),
                        (attr::MCAP_SCHEMA_NAME, "example/Lost"),
                        (attr::MCAP_SCHEMA_ENCODING, "ros2msg"),
                    ],
                    b"l",
                ),
            )
            .unwrap();
        });
        assert_eq!(
            report.unresolved_schemas,
            vec![UnresolvedSchema {
                source: "robot".into(),
                topic: "/lost".into(),
                schema_name: "example/Lost".into(),
                schema_encoding: "ros2msg".into(),
            }]
        );
        let channels = channels(&bytes);
        let known = channels
            .iter()
            .find(|c| c.topic == "/known")
            .expect("known");
        assert_eq!(known.schema_name.as_deref(), Some("example/Known"));
        let lost = channels.iter().find(|c| c.topic == "/lost").expect("lost");
        assert_eq!(lost.schema_name, None);

        let records = metadata_records(&bytes, UNRESOLVED_RECORD);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["channel.0.source"], "robot");
        assert_eq!(records[0]["channel.0.topic"], "/lost");
        assert_eq!(records[0]["channel.0.schema"], "example/Lost");
        assert_eq!(records[0]["channel.0.schema_encoding"], "ros2msg");
        assert_eq!(records[0].len(), 4);
    }

    #[test]
    fn no_unresolved_record_when_every_schema_resolves() {
        let (bytes, report) = export(|e| {
            e.write_raw("annotations", &msg(1, &[(attr::TOPIC, "/a")], b"{}"))
                .unwrap();
        });
        assert!(report.unresolved_schemas.is_empty());
        assert!(metadata_records(&bytes, UNRESOLVED_RECORD).is_empty());
    }

    #[test]
    fn times_and_sequence_come_from_attrs_with_fallbacks() {
        let (bytes, _) = export(|e| {
            e.write_raw(
                "robot",
                &msg(
                    LOG as i64,
                    &[
                        (attr::MCAP_TOPIC, "/t"),
                        (attr::MCAP_PUBLISH_TIME, "1758000000000000049"),
                        (attr::MCAP_SEQUENCE, "41"),
                    ],
                    b"a",
                ),
            )
            .unwrap();
            e.write_raw(
                "robot",
                &msg(LOG as i64 + 1, &[(attr::MCAP_TOPIC, "/t")], b"b"),
            )
            .unwrap();
            e.write_raw(
                "robot",
                &msg(
                    LOG as i64 + 2,
                    &[
                        (attr::MCAP_TOPIC, "/t"),
                        (attr::MCAP_PUBLISH_TIME, "not-a-time"),
                        (attr::MCAP_SEQUENCE, "nope"),
                    ],
                    b"c",
                ),
            )
            .unwrap();
        });
        let mut seen = Vec::new();
        for m in mcap::MessageStream::new(&bytes).expect("stream") {
            let m = m.expect("message");
            seen.push((m.sequence, m.log_time, m.publish_time));
        }
        assert_eq!(
            seen,
            vec![
                (41, LOG, 1_758_000_000_000_000_049),
                (1, LOG + 1, LOG + 1),
                (2, LOG + 2, LOG + 2),
            ]
        );
    }

    #[test]
    fn negative_ingest_time_is_rejected_without_writing_payload() {
        let mut buf = Cursor::new(Vec::new());
        let mut exporter = McapExporter::new(&mut buf, options()).expect("new");
        let err = exporter
            .write_raw(
                "robot",
                &msg(-1, &[(attr::TOPIC, "/t")], b"secret-payload-bytes"),
            )
            .expect_err("pre-epoch time");
        assert!(matches!(err, McapError::Export { .. }));
        assert!(!format!("{err}").contains("secret-payload"));
    }

    #[test]
    fn same_topic_with_conflicting_encoding_is_an_error() {
        let mut buf = Cursor::new(Vec::new());
        let mut exporter = McapExporter::new(&mut buf, options()).expect("new");
        exporter
            .write_raw("robot", &msg(1, &[(attr::TOPIC, "/t")], b"a"))
            .expect("first");
        let err = exporter
            .write_raw(
                "robot",
                &msg(
                    2,
                    &[(attr::TOPIC, "/t"), (attr::MCAP_MESSAGE_ENCODING, "cdr")],
                    b"b",
                ),
            )
            .expect_err("conflict");
        assert!(format!("{err}").contains("robot"));
        assert!(format!("{err}").contains("/t"));
    }

    #[test]
    fn gaps_and_provenance_are_metadata_records() {
        let (bytes, report) = export(|e| {
            e.add_gap(GapKind::Corrupt, 7, 110, 119).unwrap();
            e.add_gap(GapKind::Evicted, 7, 150, 150).unwrap();
        });
        assert_eq!(report.gaps, 2);

        let gaps = metadata_records(&bytes, GAP_RECORD);
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0]["kind"], "corrupt");
        assert_eq!(gaps[0]["epoch"], "7");
        assert_eq!(gaps[0]["first"], "110");
        assert_eq!(gaps[0]["last"], "119");
        assert_eq!(gaps[1]["kind"], "evicted");
        assert_eq!(gaps[1]["first"], gaps[1]["last"]);

        let prov = metadata_records(&bytes, PROVENANCE_RECORD);
        assert_eq!(prov.len(), 1);
        assert_eq!(prov[0]["thadam_version"], "0.0.0-test");
        assert_eq!(prov[0]["wal_epoch"], "7");
        assert_eq!(prov[0]["first_pos"], "100");
        assert_eq!(prov[0]["last_pos"], "200");
        assert_eq!(prov[0]["node_id"], "node-a");
    }

    #[test]
    fn inverted_gap_range_is_rejected() {
        let mut buf = Cursor::new(Vec::new());
        let mut exporter = McapExporter::new(&mut buf, options()).expect("new");
        let err = exporter
            .add_gap(GapKind::Missing, 1, 10, 9)
            .expect_err("inverted");
        assert!(matches!(err, McapError::Export { .. }));
    }

    #[test]
    fn gap_kinds_have_stable_names() {
        assert_eq!(GapKind::Corrupt.as_str(), "corrupt");
        assert_eq!(GapKind::Evicted.as_str(), "evicted");
        assert_eq!(GapKind::Missing.as_str(), "missing");
        assert_eq!(DefaultEncoding::Json.as_str(), "json");
        assert_eq!(
            DefaultEncoding::OctetStream.as_str(),
            "application/octet-stream"
        );
    }

    #[test]
    fn export_errors_classify_as_sink_failures() {
        use thadam_core::{Classify, ErrorDomain, Retry};
        let err = McapError::Export { detail: "x".into() };
        assert_eq!(err.domain(), ErrorDomain::Sink);
        assert_eq!(err.retry(), Retry::Permanent);
        let err = McapError::Write { detail: "x".into() };
        assert_eq!(err.domain(), ErrorDomain::Sink);
        assert_eq!(err.code(), thadam_core::codes::SINK_IO);
    }
}
