//! One Iceberg table per dataset: resolution (existing or created),
//! idempotence bookkeeping, and the append commit with conflict handling.
//!
//! Idempotence (`docs/design/iceberg-append.md`): every registrar commit
//! writes the segment's object key into the snapshot summary
//! ([`SUMMARY_SEGMENT`]); the committed set is rebuilt from the table's
//! snapshot summaries at open and after every commit failure, so a crash
//! between the catalog's `Uploaded` record and the commit — or a commit
//! whose response was lost — converges on "the key is there or it isn't",
//! with no duplicate data file.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iceberg::io::FileIOBuilder;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind, MetadataLocation, NamespaceIdent, TableIdent};

use crate::config::IcebergConfig;
use crate::error::IcebergError;
use crate::schema::{iceberg_schema_of, schema_mismatches};
use crate::storage::{S3Io, S3IoFactory};

/// Snapshot summary property carrying the committed segment object key.
pub(crate) const SUMMARY_SEGMENT: &str = "thadam.segment";
/// Snapshot summary property carrying the dataset name.
pub(crate) const SUMMARY_DATASET: &str = "thadam.dataset";
/// Snapshot summary property carrying the covered WAL range,
/// `{epoch:016x}.{first:020}.{last:020}`.
pub(crate) const SUMMARY_WAL_RANGE: &str = "thadam.wal_range";

/// Conflict retries with a fresh table base beyond `tx.commit`'s own
/// table-property backoff (the other-writer race is expected to be rare:
/// one registrar runs per table-write path).
const CONFLICT_ATTEMPTS: u32 = 3;

/// The keys a table's snapshot summaries mark as committed by a thadam
/// registrar. Pure: testable without a catalog.
pub(crate) fn committed_from_summaries<'a>(
    summaries: impl Iterator<Item = &'a HashMap<String, String>>,
) -> HashSet<String> {
    summaries
        .filter_map(|s| s.get(SUMMARY_SEGMENT).cloned())
        .collect()
}

fn scan_committed(table: &Table) -> HashSet<String> {
    committed_from_summaries(
        table
            .metadata()
            .snapshots()
            .map(|s| &s.summary().additional_properties),
    )
}

/// Whether a commit failure means "someone committed first; reload and
/// retry" rather than a hard stop.
fn is_conflict(e: &iceberg::Error) -> bool {
    e.kind() == ErrorKind::CatalogCommitConflicts || e.retryable()
}

/// A resolved dataset table plus the keys already committed to it.
pub(crate) struct TableHandle {
    /// `namespace.dataset`, for messages.
    pub(crate) name: String,
    table: Table,
    committed: HashSet<String>,
}

impl TableHandle {
    /// Opens the dataset's table: loads it and validates the schema of the
    /// first segment's footer against it, or (with `create_table`) creates
    /// the namespace and table from that same footer. `first_uri` is any
    /// uploaded segment of the dataset; datasets are processed oldest-first,
    /// so it is the earliest one pending.
    pub(crate) async fn open(
        catalog: &Arc<dyn Catalog>,
        io: &Arc<S3Io>,
        config: &IcebergConfig,
        dataset: &str,
        first_uri: &str,
    ) -> Result<Self, IcebergError> {
        let ns = NamespaceIdent::from_vec(config.namespace.clone())
            .map_err(|e| IcebergError::Internal(format!("validated namespace is invalid: {e}")))?;
        let ident = TableIdent::new(ns.clone(), dataset.to_owned());
        let name = format!("{}.{dataset}", config.namespace.join("."));

        let table = match catalog.load_table(&ident).await {
            Ok(table) => table,
            Err(e) if e.kind() == ErrorKind::TableNotFound => {
                if !config.create_table {
                    return Err(IcebergError::TableMissing { table: name });
                }
                create(catalog, io, config, &ns, dataset, first_uri).await?
            }
            Err(e) => return Err(IcebergError::Catalog(e)),
        };

        if config.create_table {
            // The schema the table was created from moments ago; nothing to
            // re-check this process. On restart with an existing table the
            // load path above applies the check.
            let committed = scan_committed(&table);
            return Ok(TableHandle {
                name,
                table,
                committed,
            });
        }

        // Existing table: the segment footers must be readable through it.
        let file_schema = iceberg_schema_of(io, first_uri).await?;
        let mismatches =
            schema_mismatches(table.metadata().current_schema().as_ref(), &file_schema);
        if !mismatches.is_empty() {
            return Err(IcebergError::SchemaMismatch {
                table: name,
                detail: mismatches.join("; "),
            });
        }
        let committed = scan_committed(&table);
        Ok(TableHandle {
            name,
            table,
            committed,
        })
    }

    /// Appends one uploaded segment. Returns `true` when this call committed
    /// it, `false` when it was already present. Conflict handling: reload the
    /// table, re-derive the committed set, skip if now present, otherwise
    /// rebuild the append on the fresh base and retry (bounded); all lost-
    /// response and zombie-writer outcomes still leave exactly one data file.
    pub(crate) async fn append(
        &mut self,
        catalog: &Arc<dyn Catalog>,
        key: &str,
        rows: u64,
        bytes: u64,
        wal_range: &str,
    ) -> Result<bool, IcebergError> {
        let uri = format!("s3://{}/{}", self.bucket_of(), key);
        for attempt in 1..=CONFLICT_ATTEMPTS {
            if self.committed.contains(key) {
                return Ok(false);
            }
            let data_file = build_data_file(&uri, rows, bytes)?;
            let mut props = HashMap::new();
            props.insert(SUMMARY_SEGMENT.to_owned(), key.to_owned());
            props.insert(SUMMARY_DATASET.to_owned(), self.dataset_of().to_owned());
            props.insert(SUMMARY_WAL_RANGE.to_owned(), wal_range.to_owned());

            let tx = Transaction::new(&self.table);
            let tx = tx
                .fast_append()
                .add_data_files([data_file])
                .set_snapshot_properties(props)
                .apply(tx)
                .map_err(IcebergError::Catalog)?;
            match tx.commit(catalog.as_ref()).await {
                Ok(table) => {
                    self.table = table;
                    self.committed.insert(key.to_owned());
                    return Ok(true);
                }
                Err(e) => {
                    self.refresh(catalog).await?;
                    if self.committed.contains(key) {
                        // Lost response or a conflict where the other writer
                        // was this segment too: it is now committed.
                        return Ok(false);
                    }
                    if !is_conflict(&e) {
                        return Err(IcebergError::Catalog(e));
                    }
                    tracing::warn!(
                        table = %self.name,
                        segment = %key,
                        attempt,
                        error = %e,
                        "iceberg commit conflict; retrying on a fresh table"
                    );
                }
            }
        }
        Err(IcebergError::CommitFailed {
            table: self.name.clone(),
            key: key.to_owned(),
        })
    }

    /// Reloads the table metadata and rebuilds the committed set.
    async fn refresh(&mut self, catalog: &Arc<dyn Catalog>) -> Result<(), IcebergError> {
        let table = catalog
            .load_table(self.table.identifier())
            .await
            .map_err(IcebergError::Catalog)?;
        self.committed = scan_committed(&table);
        self.table = table;
        Ok(())
    }

    fn bucket_of(&self) -> &str {
        // Table location is `s3://{bucket}[/{prefix}]/thadam/v1/...`; the
        // data-file scheme and bucket mirror it.
        self.table
            .metadata()
            .location()
            .strip_prefix("s3://")
            .and_then(|rest| rest.split('/').next())
            .unwrap_or_default()
    }

    fn dataset_of(&self) -> &str {
        self.table.identifier().name()
    }
}

/// Creates the namespace (if absent) and the table from the first segment's
/// footer, at the explicit location under the configured `url`
/// (`{url}/thadam/v1/{pipeline}/{dataset}`, the directory the shipper owns).
///
/// Creates the namespace (if absent) and the table metadata JSON for the
/// footer-derived schema *client-side*, writes it through our FileIO, then
/// adopts the table with `register_table`. Iceberg assigns fresh sequential
/// IDs at `create_table` time (the Java reference server and iceberg-rust's
/// `TableMetadataBuilder` alike), which would orphan the stable Parquet
/// field IDs ADR-0013 guarantees — files would be unreadable-by-id. So the
/// initial metadata is authored here with the file's own IDs (verified:
/// `apache/iceberg-rest-fixture:1.10.1` re-numbers create-table IDs but
/// honors registered metadata).
async fn create(
    catalog: &Arc<dyn Catalog>,
    io: &Arc<S3Io>,
    config: &IcebergConfig,
    ns: &NamespaceIdent,
    dataset: &str,
    first_uri: &str,
) -> Result<Table, IcebergError> {
    let file_schema = iceberg_schema_of(io, first_uri).await?;
    if !catalog
        .namespace_exists(ns)
        .await
        .map_err(IcebergError::Catalog)?
    {
        catalog
            .create_namespace(ns, HashMap::new())
            .await
            .map_err(IcebergError::Catalog)?;
    }
    let location = format!(
        "{}/thadam/v1/{}/{}",
        config.url.trim_end_matches('/'),
        config.pipeline,
        dataset
    );
    let ident = TableIdent::new(ns.clone(), dataset.to_owned());
    let metadata_json =
        initial_metadata_json(&location, &file_schema).map_err(IcebergError::Internal)?;
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&metadata_json)
        .map_err(|e| IcebergError::Internal(format!("authored table metadata is invalid: {e}")))?;
    let metadata_location = MetadataLocation::new_with_metadata(&location, &metadata);
    let file_io = FileIOBuilder::new(Arc::new(S3IoFactory::new(Arc::clone(io)))).build();
    metadata
        .write_to(&file_io, &metadata_location)
        .await
        .map_err(IcebergError::Catalog)?;
    catalog
        .register_table(&ident, metadata_location.to_string())
        .await
        .map_err(IcebergError::Catalog)
}

/// The initial v1 `metadata.json` for a snapshot-less, unpartitioned,
/// unsorted format-v2 table at `location` with `schema` as the current
/// schema, keeping the file's own field IDs. Shapes mirror the metadata
/// `TableMetadataBuilder` produces for the same inputs (its JSON round-trip
/// is what the REST fixture accepted: no snapshot keys beyond empty refs).
fn initial_metadata_json(location: &str, schema: &iceberg::spec::Schema) -> Result<String, String> {
    let schema_json = serde_json::to_value(schema).map_err(|e| e.to_string())?;
    let last_column_id = max_field_id(schema).ok_or("schema has no fields".to_owned())?;
    let json = serde_json::json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::now_v7().to_string(),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": now_millis(),
        "last-column-id": last_column_id,
        "schemas": [schema_json],
        "current-schema-id": schema.schema_id(),
        "partition-specs": [{ "spec-id": 0, "fields": [] }],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "sort-orders": [{ "order-id": 0, "fields": [] }],
        "default-sort-order-id": 0,
        "properties": {},
        "refs": {},
    });
    Ok(json.to_string())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The largest field ID anywhere in the schema (declared IDs can reach the
/// derived-nested range 8,388,608+; `last-column-id` covers them all).
fn max_field_id(schema: &iceberg::spec::Schema) -> Option<i32> {
    fn max_of(ty: &iceberg::spec::Type, acc: i32) -> i32 {
        match ty {
            iceberg::spec::Type::Primitive(_) => acc,
            iceberg::spec::Type::Struct(st) => st
                .fields()
                .iter()
                .fold(acc, |a, f| max_of(f.field_type.as_ref(), a.max(f.id))),
            iceberg::spec::Type::List(list) => max_of(
                list.element_field.field_type.as_ref(),
                acc.max(list.element_field.id),
            ),
            iceberg::spec::Type::Map(map) => max_of(
                map.value_field.field_type.as_ref(),
                max_of(
                    map.key_field.field_type.as_ref(),
                    acc.max(map.key_field.id).max(map.value_field.id),
                ),
            ),
        }
    }
    schema
        .as_struct()
        .fields()
        .iter()
        .fold(None, |acc: Option<i32>, f| {
            Some(max_of(
                f.field_type.as_ref(),
                acc.unwrap_or(i32::MIN).max(f.id),
            ))
        })
}

fn build_data_file(
    uri: &str,
    rows: u64,
    bytes: u64,
) -> Result<iceberg::spec::DataFile, IcebergError> {
    DataFileBuilder::default()
        .partition_spec_id(0)
        .content(DataContentType::Data)
        .file_path(uri.to_owned())
        .file_format(DataFileFormat::Parquet)
        .record_count(rows)
        .file_size_in_bytes(bytes)
        .partition(Struct::empty())
        .build()
        .map_err(|e| IcebergError::Internal(format!("data file builder: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn committed_keys_come_only_from_the_segment_property() {
        let summaries = [
            summary(&[(SUMMARY_SEGMENT, "lake/a.parquet"), ("added-files", "1")]),
            summary(&[("added-files", "3")]),
            summary(&[(SUMMARY_SEGMENT, "lake/b.parquet")]),
        ];
        let committed = committed_from_summaries(summaries.iter());
        assert_eq!(committed.len(), 2);
        assert!(committed.contains("lake/a.parquet"));
        assert!(committed.contains("lake/b.parquet"));
    }

    #[test]
    fn authored_metadata_round_trips_with_the_files_own_ids() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let fields: Vec<std::sync::Arc<NestedField>> = vec![
            std::sync::Arc::new(NestedField::required(
                2,
                "_ingest_time",
                Type::Primitive(PrimitiveType::Timestamptz),
            )),
            std::sync::Arc::new(NestedField::optional(
                8_388_608,
                "elem",
                Type::Primitive(PrimitiveType::Int),
            )),
            std::sync::Arc::new(NestedField::optional(
                1000,
                "n",
                Type::Primitive(PrimitiveType::Long),
            )),
        ];
        let schema = iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()
            .expect("schema");

        let json = initial_metadata_json("s3://warehouse/lake/thadam/v1/p/d", &schema)
            .expect("metadata json");
        let metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(&json).expect("valid table metadata");
        assert_eq!(metadata.format_version(), iceberg::spec::FormatVersion::V2);
        assert_eq!(metadata.location(), "s3://warehouse/lake/thadam/v1/p/d");
        let ids: std::collections::BTreeSet<i32> = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.id)
            .collect();
        let expected: std::collections::BTreeSet<i32> = [2, 1000, 8_388_608].into_iter().collect();
        assert_eq!(ids, expected, "the file's own field IDs survive");
        assert_eq!(
            max_field_id(&schema),
            Some(8_388_608),
            "last-column-id covers the derived range"
        );
        assert_eq!(metadata.location(), "s3://warehouse/lake/thadam/v1/p/d");
        assert!(
            metadata.snapshots().next().is_none(),
            "starts snapshot-less"
        );
    }

    #[test]
    fn data_file_is_unpartitioned_with_counts() {
        let df = build_data_file("s3://warehouse/lake/f.parquet", 42, 1234).expect("data file");
        assert_eq!(df.record_count(), 42);
        assert_eq!(df.file_size_in_bytes(), 1234);
        assert_eq!(df.file_path(), "s3://warehouse/lake/f.parquet");
    }
}
