//! The REST catalog's HTTP client (optional private CA) and schema commits.
//!
//! Why schema commits do not use the crate's `update_schema` API (0.10.1):
//! `UpdateSchemaAction` assigns fresh sequential IDs to added columns from
//! `last-column-id` — the same reason step 1 rejects REST `create_table`; a
//! declared column with the configured ID 1001 would land as
//! `last-column-id` + 1 and every Parquet file carrying ID 1001 would become
//! unreadable-by-id. It also has no rename or type-promotion operation. And
//! the escape hatch is sealed: `TableCommit`'s builder is `pub(crate)` and
//! `TransactionAction` is a crate-private trait, so no caller can hand the
//! catalog a custom update. The evolution commit is therefore authored here
//! (`add-schema` with the files' own field IDs + `set-current-schema`) and
//! POSTed with the crate's own public wire types (`CommitTableRequest`,
//! `CommitTableResponse`), the way step 1 authors creation metadata.
//!
//! The same limit means a schema change and the append cannot share one
//! commit through the public API; see `docs/design/iceberg-append.md` for
//! why the order schema-first is crash-safe.

use std::collections::HashMap;

use iceberg::table::Table;
use iceberg::{Error, ErrorKind, TableRequirement, TableUpdate};
use iceberg_catalog_rest::{CommitTableRequest, CommitTableResponse};

use crate::error::IcebergError;

/// The reqwest 0.12 client the catalog (and the schema commits) use: rustls
/// with the process `ring` provider (ADR-0020). With `ca_file` the platform
/// roots are replaced by the PEM bundle's certificates, so only that CA is
/// trusted for the catalog connection.
pub(crate) fn http_client(ca_file: Option<&str>) -> Result<reqwest012::Client, IcebergError> {
    let mut builder = reqwest012::Client::builder();
    if let Some(path) = ca_file {
        let pem = std::fs::read(path).map_err(|e| IcebergError::Config {
            message: format!("catalog_ca_file {path}: {e}"),
        })?;
        let certs = reqwest012::Certificate::from_pem_bundle(&pem)
            .ok()
            .filter(|certs| !certs.is_empty())
            .ok_or_else(|| IcebergError::Config {
                message: format!("catalog_ca_file {path}: no PEM certificate found"),
            })?;
        builder = builder.tls_built_in_root_certs(false);
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder
        .build()
        .map_err(|e| IcebergError::Internal(format!("http client for the catalog: {e}")))
}

/// Iceberg REST catalog commit access for schema updates.
pub(crate) struct RestCommitter {
    client: reqwest012::Client,
    /// `{catalog_uri}/v1` or `{catalog_uri}/v1/{prefix}` when the catalog
    /// serves under a prefix (discovered from `/v1/config`).
    base: String,
}

/// The REST spec's `/v1/config` response: the catalog's `prefix`, if any.
#[derive(serde::Deserialize, Default)]
struct CatalogConfig {
    #[serde(default)]
    defaults: HashMap<String, String>,
    #[serde(default)]
    overrides: HashMap<String, String>,
}

impl RestCommitter {
    /// Resolves the catalog's URL prefix from `/v1/config` (the same GET the
    /// catalog client makes at load) over the shared `client`.
    pub(crate) async fn connect(
        client: reqwest012::Client,
        catalog_uri: &str,
    ) -> Result<RestCommitter, IcebergError> {
        let config_url = format!("{catalog_uri}/v1/config");
        let resp = client
            .get(&config_url)
            .send()
            .await
            .map_err(|e| request_error(&format!("GET {config_url}"), e))?;
        if !resp.status().is_success() {
            return Err(IcebergError::Catalog(Error::new(
                ErrorKind::Unexpected,
                format!("GET {config_url}: status {}", resp.status()),
            )));
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| request_error(&format!("GET {config_url}"), e))?;
        let config: CatalogConfig = serde_json::from_slice(&body).map_err(|e| {
            IcebergError::Catalog(Error::new(
                ErrorKind::Unexpected,
                format!("GET {config_url}: /v1/config does not parse: {e}"),
            ))
        })?;
        let prefix = config
            .overrides
            .get("prefix")
            .or_else(|| config.defaults.get("prefix"))
            .filter(|p| !p.is_empty());
        let base = match prefix {
            Some(prefix) => format!("{catalog_uri}/v1/{prefix}"),
            None => format!("{catalog_uri}/v1"),
        };
        Ok(RestCommitter { client, base })
    }

    /// Commits `updates` against `table`, guarded by `requirements`
    /// (optimistic: a concurrent change makes the catalog answer 409, which
    /// surfaces as a retryable `CatalogCommitConflicts`). Returns the table
    /// state the catalog reports.
    pub(crate) async fn commit(
        &self,
        table: &Table,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<Table, IcebergError> {
        let ident = table.identifier();
        let url = format!(
            "{}/namespaces/{}/tables/{}",
            self.base,
            ident.namespace().to_url_string(),
            ident.name()
        );
        let body = serde_json::to_vec(&CommitTableRequest {
            identifier: Some(ident.clone()),
            requirements,
            updates,
        })
        .map_err(|e| IcebergError::Internal(format!("commit request does not serialize: {e}")))?;
        let what = format!("schema commit to {}", ident.name());
        let resp = self
            .client
            .post(&url)
            .header(reqwest012::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| request_error(&what, e))?;
        let status = resp.status();
        let text = resp.bytes().await.map_err(|e| request_error(&what, e))?;
        if !status.is_success() {
            let detail: String = String::from_utf8_lossy(&text).chars().take(512).collect();
            return Err(IcebergError::Catalog(status_error(&what, status, &detail)));
        }
        let parsed: CommitTableResponse = serde_json::from_slice(&text).map_err(|e| {
            IcebergError::Catalog(Error::new(
                ErrorKind::Unexpected,
                format!("{what}: response does not parse: {e}"),
            ))
        })?;
        let runtime = iceberg::Runtime::try_current().map_err(|e| {
            IcebergError::Internal(format!("no tokio runtime for the iceberg client: {e}"))
        })?;
        Table::builder()
            .metadata(parsed.metadata)
            .metadata_location(parsed.metadata_location)
            .identifier(ident.clone())
            .file_io(table.file_io().clone())
            .runtime(runtime)
            .build()
            .map_err(IcebergError::Catalog)
    }
}

/// A transport failure, with the reqwest error kept as the `source` so the
/// classifier can see TLS causes (`invalid peer certificate: ...`) that
/// reqwest's own `Display` omits.
fn request_error(what: &str, e: reqwest012::Error) -> IcebergError {
    IcebergError::Catalog(
        Error::new(ErrorKind::Unexpected, format!("{what}: request failed")).with_source(e),
    )
}

/// An HTTP failure mapped onto the kinds [`crate::table`] classifies: 404 is
/// a vanished table (fatal), 409 a commit conflict (reload and retry), 401
/// and 403 are caught by the classifier's text scan, anything else is
/// transient.
fn status_error(what: &str, status: reqwest012::StatusCode, detail: &str) -> Error {
    let message = format!("{what} (status {status}): {detail}");
    match status {
        reqwest012::StatusCode::NOT_FOUND => Error::new(ErrorKind::TableNotFound, message),
        reqwest012::StatusCode::CONFLICT => {
            Error::new(ErrorKind::CatalogCommitConflicts, message).with_retryable(true)
        }
        _ => Error::new(ErrorKind::Unexpected, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_statuses_map_to_the_kinds_the_registrar_classifies() {
        let conflict = status_error("c", reqwest012::StatusCode::CONFLICT, "stale");
        assert_eq!(conflict.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(conflict.retryable());
        let missing = status_error("c", reqwest012::StatusCode::NOT_FOUND, "");
        assert_eq!(missing.kind(), ErrorKind::TableNotFound);
        let down = status_error("c", reqwest012::StatusCode::BAD_GATEWAY, "");
        assert_eq!(down.kind(), ErrorKind::Unexpected);
        assert!(!down.retryable());
    }

    #[test]
    fn a_missing_or_empty_ca_file_is_a_config_error() {
        let dir = tempfile::tempdir().expect("dir");
        let missing = dir.path().join("nope.pem");
        let e = http_client(missing.to_str()).expect_err("missing file");
        assert!(matches!(e, IcebergError::Config { .. }), "{e}");
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate").expect("write");
        let e = http_client(empty.to_str()).expect_err("no certificate");
        assert!(e.to_string().contains("no PEM certificate"), "{e}");
    }
}
