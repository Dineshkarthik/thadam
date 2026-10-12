//! Iceberg `FileIO` over the workspace's audited `object_store` S3 stack.
//!
//! `iceberg` 0.10 ships built-in storage for memory and local FS only; the
//! optional `iceberg-storage-opendal` crate is rejected because opendal 0.57
//! resolves reqwest's default rustls stacks (`hyper-rustls` +
//! `rustls-platform-verifier`) and pulls `aws-lc-sys`, which `deny.toml`
//! bans (dependency audit in docs/design/iceberg-append.md). Instead this
//! module implements iceberg's [`Storage`]/[`StorageFactory`] traits — eleven
//! small async methods — over `object_store` 0.14 with `aws-base` + reqwest
//! `rustls-no-provider` + `ring` (ADR-0020, the shipper's stack).
//!
//! shortcut: single-bucket FileIO — every `s3://` URI must name the
//! configured bucket (table metadata and data files live in the same one);
//! upgrade to a per-bucket store map when a deployment needs cross-bucket
//! tables.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result as IceResult};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::config::StoreConfig;
use crate::error::IcebergError;

/// Builds the configured S3 store. Does no network I/O: the first read or
/// write is what talks to the endpoint.
pub fn build_s3(url: &str, config: &StoreConfig) -> Result<AmazonS3, IcebergError> {
    let mut builder = AmazonS3Builder::new()
        .with_url(url)
        .with_region(&config.region)
        .with_allow_http(config.allow_http)
        .with_access_key_id(config.credentials.access_key_id())
        .with_secret_access_key(config.credentials.secret_access_key());
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    builder.build().map_err(IcebergError::Store)
}

/// An `s3://bucket` FileIO backend over `object_store`.
#[derive(Clone)]
pub struct S3Io {
    store: Arc<dyn ObjectStore>,
    bucket: Arc<str>,
    /// Built lazily by tests constructing an `S3Io` without a factory.
    self_arc: Arc<OnceLock<Arc<S3Io>>>,
}

impl fmt::Debug for S3Io {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "S3Io(s3://{})", self.bucket)
    }
}

impl S3Io {
    /// A backend over `store` answering only URIs in `bucket`.
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<Arc<str>>) -> Self {
        S3Io {
            store,
            bucket: bucket.into(),
            self_arc: Arc::new(OnceLock::new()),
        }
    }

    /// The shared handle the iceberg `InputFile`/`OutputFile` wrappers need.
    fn handle(&self) -> Arc<S3Io> {
        self.self_arc.get_or_init(|| Arc::new(self.clone())).clone()
    }

    /// Maps an absolute `s3://bucket/key` URI to a store-relative path.
    fn path(&self, uri: &str) -> IceResult<object_store::path::Path> {
        let rest = uri.strip_prefix("s3://").ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("thadam-iceberg storage supports s3:// URIs only, got `{uri}`"),
            )
        })?;
        let (bucket, key) = rest.split_once('/').ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("s3 URI `{uri}` has no object key"),
            )
        })?;
        if bucket != self.bucket.as_ref() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "thadam-iceberg storage is single-bucket (s3://{}); URI `{uri}` names another bucket",
                    self.bucket
                ),
            ));
        }
        if key.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("s3 URI `{uri}` has an empty object key"),
            ));
        }
        Ok(object_store::path::Path::from(key))
    }

    /// The absolute `s3://` URI for an uploaded object's key.
    pub fn uri_of(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }

    /// A ranged read for the footer path (one GET per segment schema check).
    pub(crate) async fn read_range(
        &self,
        uri: &str,
        range: Range<u64>,
    ) -> Result<Bytes, IcebergError> {
        let path = self.path(uri).map_err(store_invalid)?;
        Ok(self.store.get_range(&path, range).await?)
    }

    /// The object size, for footer tail reads.
    pub(crate) async fn size_of(&self, uri: &str) -> Result<u64, IcebergError> {
        let path = self.path(uri).map_err(store_invalid)?;
        Ok(self.store.head(&path).await?.size)
    }
}

/// iceberg's `Error` from a storage-layer problem (not surfaced directly;
/// the registrar works with `IcebergError`).
fn store_invalid(e: Error) -> IcebergError {
    IcebergError::Catalog(e)
}

#[async_trait::async_trait]
#[typetag::serde]
impl Storage for S3Io {
    async fn exists(&self, path: &str) -> IceResult<bool> {
        let path = self.path(path)?;
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(store_io_error(e)),
        }
    }

    async fn metadata(&self, path: &str) -> IceResult<FileMetadata> {
        let path = self.path(path)?;
        let meta = self.store.head(&path).await.map_err(store_io_error)?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> IceResult<Bytes> {
        let path = self.path(path)?;
        let result = self.store.get(&path).await.map_err(store_io_error)?;
        result.bytes().await.map_err(store_io_error)
    }

    async fn reader(&self, path: &str) -> IceResult<Box<dyn FileRead>> {
        let path = self.path(path)?;
        Ok(Box::new(S3Reader {
            store: Arc::clone(&self.store),
            path,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> IceResult<()> {
        let path = self.path(path)?;
        // A single S3 PUT appears atomically; manifest and metadata names
        // are unique per commit (uuid), so create-if-absent adds nothing.
        self.store
            .put(&path, bs.into())
            .await
            .map_err(store_io_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> IceResult<Box<dyn FileWrite>> {
        let path = self.path(path)?;
        Ok(Box::new(S3Writer {
            store: Arc::clone(&self.store),
            path,
            buf: Vec::new(),
            closed: false,
        }))
    }

    async fn delete(&self, path: &str) -> IceResult<()> {
        let path = self.path(path)?;
        self.store.delete(&path).await.map_err(store_io_error)
    }

    async fn delete_prefix(&self, path: &str) -> IceResult<()> {
        let prefix = self.path(path)?;
        let mut stream = self.store.list(Some(&prefix));
        let mut deletes = Vec::new();
        while let Some(meta) = stream.next().await {
            deletes.push(meta.map_err(store_io_error)?.location);
        }
        for path in deletes {
            self.store.delete(&path).await.map_err(store_io_error)?;
        }
        Ok(())
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> IceResult<()> {
        let this = self.clone();
        let mapped: BoxStream<'static, IceResult<object_store::path::Path>> =
            Box::pin(paths.map(move |uri| this.path(&uri)));
        let mut paths = mapped;
        while let Some(path) = paths.next().await {
            self.store.delete(&path?).await.map_err(store_io_error)?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> IceResult<InputFile> {
        Ok(InputFile::new(self.handle(), path.to_owned()))
    }

    fn new_output(&self, path: &str) -> IceResult<OutputFile> {
        Ok(OutputFile::new(self.handle(), path.to_owned()))
    }
}

struct S3Reader {
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
}

#[async_trait::async_trait]
impl FileRead for S3Reader {
    async fn read(&self, range: Range<u64>) -> IceResult<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(store_io_error)
    }
}

struct S3Writer {
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    buf: Vec<u8>,
    closed: bool,
}

#[async_trait::async_trait]
impl FileWrite for S3Writer {
    async fn write(&mut self, bs: Bytes) -> IceResult<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::Unexpected, "write after close"));
        }
        // shortcut: manifests and metadata.json are small (KiBs); buffering
        // one commit's bytes in memory is fine. Upgrade to multipart if Iceberg
        // metadata writes ever grow.
        self.buf.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> IceResult<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::Unexpected, "close called twice"));
        }
        self.closed = true;
        self.store
            .put(&self.path, std::mem::take(&mut self.buf).into())
            .await
            .map_err(store_io_error)?;
        Ok(())
    }
}

fn store_io_error(e: object_store::Error) -> Error {
    Error::new(ErrorKind::Unexpected, format!("s3 file io: {e}")).with_source(e)
}

/// Produces [`S3Io`] handles for iceberg's `FileIOBuilder` (the REST catalog
/// holds one and builds per-table FileIO from it).
#[derive(Clone)]
pub struct S3IoFactory {
    io: Arc<S3Io>,
}

impl fmt::Debug for S3IoFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "S3IoFactory({:?})", self.io)
    }
}

impl S3IoFactory {
    /// A factory handing out clones of `io`.
    pub fn new(io: Arc<S3Io>) -> Self {
        S3IoFactory { io }
    }

    /// The shared backend (also used directly for footer reads).
    pub fn io(&self) -> &Arc<S3Io> {
        &self.io
    }
}

#[typetag::serde]
impl StorageFactory for S3IoFactory {
    fn build(&self, _config: &iceberg::io::StorageConfig) -> IceResult<Arc<dyn Storage>> {
        Ok(self.io.clone())
    }
}

// `Storage`/`StorageFactory` are typetag-serde traits, so implementors must
// be serde types. A live S3 handle is process-local runtime state: there is
// nothing honest to round-trip. Fail both directions instead of sketching a
// config-shaped lie (and never serialize credentials).
impl serde::Serialize for S3Io {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "thadam-iceberg storage is runtime state and is not serializable",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for S3Io {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "thadam-iceberg storage is runtime state and cannot be deserialized",
        ))
    }
}

impl serde::Serialize for S3IoFactory {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "thadam-iceberg storage is runtime state and is not serializable",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for S3IoFactory {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "thadam-iceberg storage is runtime state and cannot be deserialized",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io() -> S3Io {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        S3Io::new(store, "warehouse")
    }

    #[test]
    fn uri_mapping_accepts_the_configured_bucket() {
        let io = io();
        let p = io
            .path("s3://warehouse/lake/thadam/v1/p/d/f.parquet")
            .expect("path");
        assert_eq!(p.as_ref(), "lake/thadam/v1/p/d/f.parquet");
    }

    #[test]
    fn uri_mapping_rejects_other_schemes_and_buckets() {
        let io = io();
        assert!(io.path("http://warehouse/a").is_err());
        assert!(io.path("s3://other/a").is_err());
        assert!(io.path("s3://warehouse/").is_err());
        assert!(io.path("s3://warehouse").is_err());
    }

    #[test]
    fn uri_of_joins_bucket_and_key() {
        assert_eq!(
            io().uri_of("lake/thadam/v1/p/d/f.parquet"),
            "s3://warehouse/lake/thadam/v1/p/d/f.parquet"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_read_range_delete_round_trip() {
        let io = io();
        let uri = io.uri_of("a/b.parquet");
        Storage::write(&io, &uri, Bytes::from_static(b"0123456789"))
            .await
            .expect("put");
        assert!(Storage::exists(&io, &uri).await.expect("head"));
        assert_eq!(io.size_of(&uri).await.expect("size"), 10);
        let b = io.read_range(&uri, 2..6).await.expect("range");
        assert_eq!(&b[..], b"2345");
        Storage::delete(&io, &uri).await.expect("delete");
        assert!(!Storage::exists(&io, &uri).await.expect("head"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writer_buffers_until_close() {
        let io = io();
        let uri = io.uri_of("m/metadata.json");
        let mut w = Storage::writer(&io, &uri).await.expect("writer");
        FileWrite::write(&mut *w, Bytes::from_static(b"{\"a\":"))
            .await
            .expect("write part");
        assert!(!Storage::exists(&io, &uri).await.expect("head"));
        FileWrite::write(&mut *w, Bytes::from_static(b"1}"))
            .await
            .expect("write part");
        FileWrite::close(&mut *w).await.expect("close");
        let got = Storage::read(&io, &uri).await.expect("read");
        assert_eq!(&got[..], b"{\"a\":1}");
    }
}
