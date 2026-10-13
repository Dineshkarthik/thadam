//! Streams one MCAP file record by record, flattening chunks as they arrive.
//!
//! The `mcap` crate's message iterators operate on a fully buffered (or
//! memory mapped) file, and memory mapping needs `unsafe`, which the
//! workspace forbids. Instead this reader frames top-level records itself
//! (1-byte opcode + 8-byte little-endian length + body — the MCAP framing is
//! fixed) and reuses the crate's [`mcap::parse_record`] and
//! [`mcap::read::ChunkReader`] for every format subtlety: decompression,
//! chunk CRC validation, and record decoding all stay upstream's.
//!
//! Only the data section is walked; the reader stops at `DataEnd` and never
//! touches the summary section. Peak memory is one chunk record plus its
//! decompressed form.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use mcap::read::ChunkReader;
use mcap::records::{self, op};
use thadam_core::ThadamError;

use crate::error::McapError;

/// The largest record body or decompressed chunk materialized at once. Chunks
/// are typically a few megabytes; this catches a corrupt length prefix or
/// compression bomb before it exhausts memory.
const MAX_RECORD_LEN: u64 = 64 << 20;

/// A schema as recorded in the file.
#[derive(Debug)]
pub(crate) struct SchemaInfo {
    /// The schema id channels reference.
    pub id: u16,
    /// For example `sensor_msgs/JointState` or `sensor_msgs/msg/JointState`.
    pub name: String,
    /// For example `ros2msg` or `jsonschema`.
    pub encoding: String,
    /// The schema bytes (`ros2msg` text, JSON Schema, ...).
    pub data: Bytes,
}

/// A channel with its schema resolved.
#[derive(Debug)]
pub(crate) struct ChannelInfo {
    /// The channel id messages reference.
    pub id: u16,
    /// The topic, for example `/joint_states`.
    pub topic: String,
    /// For example `cdr` or `json`.
    pub message_encoding: String,
    /// The channel's schema, if it has one.
    pub schema: Option<Arc<SchemaInfo>>,
}

/// One message, with its channel resolved and payload owned.
#[derive(Debug)]
pub(crate) struct Item {
    /// The channel the message was published on.
    pub channel: Arc<ChannelInfo>,
    /// The recorder's sequence number.
    pub sequence: u32,
    /// The MCAP log time, nanoseconds since the epoch.
    pub log_time: u64,
    /// The MCAP publish time, nanoseconds since the epoch.
    pub publish_time: u64,
    /// The payload bytes, as written.
    pub payload: Bytes,
}

/// The read side of one file.
pub(crate) struct FileReader {
    path: PathBuf,
    inner: BufReader<File>,
    schemas: HashMap<u16, Arc<SchemaInfo>>,
    channels: HashMap<u16, Arc<ChannelInfo>>,
    queue: VecDeque<Item>,
    done: bool,
    /// Channels accumulated since the last drain, first-seen first. The
    /// source pre-flights their schemas in `ros2` mode.
    new_channels: Vec<Arc<ChannelInfo>>,
    /// Messages returned by `next_item` so far: the file-order position
    /// the source's resume state is expressed in.
    consumed: u64,
}

impl FileReader {
    /// Opens `path` and checks the MCAP magic.
    pub(crate) fn start(path: PathBuf) -> Result<Self, ThadamError> {
        let file = File::open(&path).map_err(|e| McapError::open(&path, e))?;
        let mut inner = BufReader::new(file);
        let mut magic = [0u8; 8];
        read_to_end(&mut inner, &mut magic).map_err(|e| McapError::open(&path, e))?;
        if magic != mcap::MAGIC {
            return Err(McapError::parse(&path, mcap::McapError::BadMagic).into());
        }
        Ok(FileReader {
            path,
            inner,
            schemas: HashMap::new(),
            channels: HashMap::new(),
            queue: VecDeque::new(),
            done: false,
            new_channels: Vec::new(),
            consumed: 0,
        })
    }

    /// The file's path, for error context.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Channels seen since the last call, first-seen first.
    pub(crate) fn drain_new_channels(&mut self) -> Vec<Arc<ChannelInfo>> {
        std::mem::take(&mut self.new_channels)
    }

    /// The next message, or `None` at the end of the data section.
    pub(crate) fn next_item(&mut self) -> Result<Option<Item>, ThadamError> {
        self.fill()?;
        let item = self.queue.pop_front();
        self.consumed += u64::from(item.is_some());
        Ok(item)
    }

    /// How many messages `next_item` has returned.
    pub(crate) fn consumed(&self) -> u64 {
        self.consumed
    }

    /// Reads past the first `n` messages without handing them out. A file
    /// with fewer messages than the saved position is not the file the
    /// position was saved for.
    pub(crate) fn skip(&mut self, n: u64) -> Result<(), ThadamError> {
        while self.consumed < n {
            if self.next_item()?.is_none() {
                return Err(self.corrupt(&format!(
                    "the saved resume position ({n} messages) is past the end of the file ({}); delete the source's resume.json to replay from the start",
                    self.consumed
                )));
            }
        }
        Ok(())
    }

    /// Reads until one message is queued or the data section is over.
    /// End of file before `DataEnd` means the file was cut short: a
    /// classified error, not a silent partial replay.
    fn fill(&mut self) -> Result<(), ThadamError> {
        while self.queue.is_empty() && !self.done {
            let Some((opcode, body)) = self.read_record()? else {
                self.done = true;
                return Err(self.corrupt("the file ends before its DataEnd record (truncated?)"));
            };
            match opcode {
                op::SCHEMA => match mcap::parse_record(op::SCHEMA, &body) {
                    Ok(records::Record::Schema { header, data }) => {
                        self.add_schema(header, Bytes::from(data.into_owned()))?;
                    }
                    _ => return Err(self.corrupt("malformed schema record")),
                },
                op::CHANNEL => match mcap::parse_record(op::CHANNEL, &body) {
                    Ok(records::Record::Channel(channel)) => self.add_channel(channel)?,
                    _ => return Err(self.corrupt("malformed channel record")),
                },
                op::MESSAGE => match mcap::parse_record(op::MESSAGE, &body) {
                    Ok(records::Record::Message { header, data }) => {
                        let item = self.make_item(header, Bytes::copy_from_slice(&data))?;
                        self.queue.push_back(item);
                    }
                    _ => return Err(self.corrupt("malformed message record")),
                },
                op::CHUNK => self.read_chunk(&body)?,
                op::DATA_END | op::FOOTER => self.done = true,
                // Attachments, metadata, indexes: skipped without decoding.
                _ => {}
            }
        }
        Ok(())
    }

    /// Frames the next top-level record: opcode and an owned body.
    /// `Ok(None)` is a clean end of file; a partial frame is a classified
    /// parse error, never a panic.
    fn read_record(&mut self) -> Result<Option<(u8, Vec<u8>)>, ThadamError> {
        let mut frame = [0u8; 9];
        let mut got = 0;
        while got < frame.len() {
            match self.inner.read(&mut frame[got..]) {
                Ok(0) if got == 0 => return Ok(None),
                Ok(0) => {
                    return Err(McapError::parse(&self.path, mcap::McapError::UnexpectedEof).into());
                }
                Ok(n) => got += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(McapError::open(&self.path, e).into()),
            }
        }
        let opcode = frame[0];
        let len = u64::from_le_bytes([
            frame[1], frame[2], frame[3], frame[4], frame[5], frame[6], frame[7], frame[8],
        ]);
        if len > MAX_RECORD_LEN {
            return Err(McapError::parse(
                &self.path,
                mcap::McapError::RecordTooLarge { opcode, len },
            )
            .into());
        }
        let mut body = Vec::with_capacity((len as usize).min(64 * 1024));
        (&mut self.inner)
            .take(len)
            .read_to_end(&mut body)
            .map_err(|e| McapError::open(&self.path, e))?;
        if body.len() as u64 != len {
            return Err(McapError::parse(&self.path, mcap::McapError::UnexpectedEof).into());
        }
        Ok(Some((opcode, body)))
    }

    /// Decompresses one chunk record and queues every message in it,
    /// registering schemas and channels on the way.
    fn read_chunk(&mut self, body: &[u8]) -> Result<(), ThadamError> {
        let (header, data) = match mcap::parse_record(op::CHUNK, body) {
            Ok(records::Record::Chunk { header, data }) => (header, data.into_owned()),
            _ => return Err(self.corrupt("malformed chunk record")),
        };
        if header.uncompressed_size > MAX_RECORD_LEN {
            return Err(McapError::parse(
                &self.path,
                mcap::McapError::RecordTooLarge {
                    opcode: op::CHUNK,
                    len: header.uncompressed_size,
                },
            )
            .into());
        }
        let reader =
            ChunkReader::new(header, &data).map_err(|e| McapError::parse(&self.path, e))?;
        let mut decompressed_bytes = 0u64;
        for record in reader {
            let record = record.map_err(|e| McapError::parse(&self.path, e))?;
            let record_bytes = match &record {
                records::Record::Schema { header, data } => {
                    (header.name.len() + header.encoding.len() + data.len()) as u64
                }
                records::Record::Channel(channel) => {
                    (channel.topic.len() + channel.message_encoding.len()) as u64
                }
                records::Record::Message { data, .. } => data.len() as u64,
                _ => 0,
            };
            decompressed_bytes = decompressed_bytes.saturating_add(record_bytes);
            if decompressed_bytes > MAX_RECORD_LEN {
                return Err(McapError::parse(
                    &self.path,
                    mcap::McapError::RecordTooLarge {
                        opcode: op::CHUNK,
                        len: decompressed_bytes,
                    },
                )
                .into());
            }
            match record.into_owned() {
                records::Record::Schema { header, data } => {
                    self.add_schema(header, Bytes::from(data.into_owned()))?
                }
                records::Record::Channel(channel) => self.add_channel(channel)?,
                records::Record::Message { header, data } => {
                    let item = self.make_item(header, Bytes::from(data.into_owned()))?;
                    self.queue.push_back(item);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn make_item(
        &self,
        header: records::MessageHeader,
        payload: Bytes,
    ) -> Result<Item, ThadamError> {
        let channel = self.channels.get(&header.channel_id).ok_or_else(|| {
            McapError::parse(
                &self.path,
                mcap::McapError::UnknownChannel(header.sequence, header.channel_id),
            )
        })?;
        Ok(Item {
            channel: Arc::clone(channel),
            sequence: header.sequence,
            log_time: header.log_time,
            publish_time: header.publish_time,
            payload,
        })
    }

    fn add_schema(&mut self, header: records::SchemaHeader, data: Bytes) -> Result<(), ThadamError> {
        if header.id == 0 {
            return Err(McapError::parse(&self.path, mcap::McapError::InvalidSchemaId).into());
        }
        match self.schemas.entry(header.id) {
            Entry::Occupied(entry) => {
                let existing = entry.get();
                if existing.name == header.name
                    && existing.encoding == header.encoding
                    && existing.data == data
                {
                    return Ok(());
                }
                Err(self.corrupt(&format!(
                    "schema `{}` has multiple records that do not match",
                    header.name
                )))
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(SchemaInfo {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data,
                }));
                Ok(())
            }
        }
    }

    fn add_channel(&mut self, channel: records::Channel) -> Result<(), ThadamError> {
        let schema = if channel.schema_id == 0 {
            None
        } else {
            let schema = self.schemas.get(&channel.schema_id).ok_or_else(|| {
                McapError::parse(
                    &self.path,
                    mcap::McapError::UnknownSchema(channel.topic.clone(), channel.schema_id),
                )
            })?;
            Some(Arc::clone(schema))
        };
        let info = ChannelInfo {
            id: channel.id,
            topic: channel.topic,
            message_encoding: channel.message_encoding,
            schema,
        };
        match self.channels.entry(channel.id) {
            Entry::Occupied(entry) => {
                let existing = entry.get();
                let same_schema = match (&existing.schema, &info.schema) {
                    (None, None) => true,
                    (Some(a), Some(b)) => a.id == b.id,
                    _ => false,
                };
                if existing.topic == info.topic
                    && existing.message_encoding == info.message_encoding
                    && same_schema
                {
                    return Ok(());
                }
                Err(self.corrupt(&format!(
                    "channel `{}` has multiple records that do not match",
                    info.topic
                )))
            }
            Entry::Vacant(entry) => {
                let info = Arc::new(info);
                entry.insert(Arc::clone(&info));
                self.new_channels.push(info);
                Ok(())
            }
        }
    }

    fn corrupt(&self, detail: &str) -> ThadamError {
        McapError::Parse {
            path: self.path.clone(),
            detail: detail.to_owned(),
        }
        .into()
    }
}

/// `read_exact`, retrying interrupts.
fn read_to_end(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<()> {
    let mut got = 0;
    while got < buf.len() {
        match reader.read(&mut buf[got..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected end of file",
                ));
            }
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
