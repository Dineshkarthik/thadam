//! The ClickHouse batch sink: `open` probes, `write` inserts one Native block.

use std::sync::Arc;

use async_trait::async_trait;
use clickhouse::Client;
use clickhouse::error::Error as ChError;
use clickhouse::native::Block;
use thadam_arrow::{BatchEncoder, DatasetSchema};
use thadam_core::{
    BatchId, BatchSink, DeliveryClass, ErrorDomain, ThadamError, SinkBatch, SinkCapabilities,
    SinkContext, WriteOutcome, codes,
};

use crate::block::{self, EncodeError};
use crate::config::{ChConfig, Compression};
use crate::error::{classify, classify_at_open};
use crate::probe::{self, DedupFacts, Delivery};
use crate::types::{self, InsertPlan, TableCol};

/// Rows the encoder reserves room for; batches may be larger.
const ENCODER_CAPACITY_HINT: usize = 1024;

/// A `kind: clickhouse` batch sink: one dataset, one existing table, one
/// Native block per batch, `insert_deduplication_token` when the probe found
/// dedup support.
///
/// Constructing it is pure: the HTTP client, which loads the OS trust store,
/// is created by `open`, never by `build`, `validate`, or `new`.
pub struct ClickHouseSink {
    cfg: ChConfig,
    sink_id: String,
    client: Option<Client>,
    schema: Arc<DatasetSchema>,
    encoder: BatchEncoder,
    plan: Option<InsertPlan>,
    delivery: Delivery,
}

impl std::fmt::Debug for ClickHouseSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSink")
            .field("sink", &self.sink_id)
            .field("url", &self.cfg.url)
            .field(
                "table",
                &format!("{}.{}", self.cfg.database, self.cfg.table),
            )
            .field("opened", &self.plan.is_some())
            .finish_non_exhaustive()
    }
}

/// Backtick-quotes an identifier. Config validation forbids backticks,
/// backslashes and quotes in identifiers, so quoting cannot be escaped from.
fn ident(name: &str) -> String {
    debug_assert!(!name.contains(['`', '\\', '\'', '"']));
    format!("`{name}`")
}

/// Single-quotes a string literal; same safety argument as [`ident`].
fn lit(name: &str) -> String {
    debug_assert!(!name.contains(['`', '\\', '\'', '"']));
    format!("'{name}'")
}

/// The `DESCRIBE TABLE` probe statement. Backticked identifiers carry dots
/// literally: `` `poses.v2` `` is one table name, never `poses` database
/// `v2` table.
fn describe_sql(database: &str, table: &str) -> String {
    format!("DESCRIBE TABLE {}.{}", ident(database), ident(table))
}

/// The dedup token, per ADR-0008: `{epoch}-{sink}-{first}-{last}`. It is a
/// pure function of the batch id and the sink id: the driver persists the
/// in-flight range before sending and retries with the same `BatchId`, so the
/// token repeats after a crash exactly when the batch does. Bisected
/// sub-batches cover different ranges and so get different tokens, always the
/// same ones for the same split. The sink id keeps two sinks that write one
/// table from colliding on equal WAL ranges.
fn dedup_token(sink_id: &str, id: &BatchId) -> String {
    let r = id.range();
    format!(
        "{}-{sink_id}-{}-{}",
        r.epoch(),
        r.first().get(),
        r.last().get()
    )
}

fn encode_error(e: &EncodeError) -> ThadamError {
    ThadamError::permanent(
        ErrorDomain::Schema,
        codes::SCHEMA_TYPE_MISMATCH,
        e.to_string(),
    )
}

fn unavailable(what: String) -> ThadamError {
    ThadamError::transient(ErrorDomain::Sink, codes::SINK_UNAVAILABLE, what)
}

/// Builds the client. Every statement runs with `async_insert = 0`: a profile
/// that turned asynchronous inserts on would acknowledge before the data is
/// durable and, without `async_insert_deduplicate`, silently void the dedup
/// token.
///
/// Loading the OS trust store is slow (on macOS it is an IPC to `trustd`, which
/// serializes across processes), so this runs only from `open`, on a blocking
/// thread. The vendor's default client panics when the store holds no root
/// certificate (it builds its TLS connector eagerly, whatever the URL scheme);
/// that surfaces here as a Config error instead of a crash.
pub(crate) fn make_client(cfg: &ChConfig) -> Result<Client, ThadamError> {
    #[cfg(test)]
    CLIENTS_BUILT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let base = std::panic::catch_unwind(Client::default).map_err(|_| {
        ThadamError::permanent(
            ErrorDomain::Config,
            codes::CONFIG_INVALID,
            "the ClickHouse client could not initialize TLS: the system trust store has no \
             root certificates (install your platform's ca-certificates package)",
        )
    })?;
    let client = base
        .with_url(&cfg.url)
        .with_user(&cfg.user)
        .with_database(&cfg.database)
        .with_compression(match cfg.compression {
            Compression::Lz4 => clickhouse::Compression::Lz4,
            Compression::None => clickhouse::Compression::None,
        })
        .with_setting("async_insert", "0");
    Ok(match &cfg.password {
        Some(p) => client.with_password(p.reveal()),
        None => client,
    })
}

/// How many clients were built in this test process; the unit tests assert it
/// stays 0, so none of them loads the trust store or opens a socket.
#[cfg(test)]
pub(crate) static CLIENTS_BUILT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl ClickHouseSink {
    /// A sink for `cfg` over the given dataset schema; `open` builds the
    /// client and probes before the first write. Does no I/O.
    pub(crate) fn new(
        cfg: ChConfig,
        sink_id: impl Into<String>,
        schema: Arc<DatasetSchema>,
    ) -> Self {
        let encoder = BatchEncoder::new(Arc::clone(&schema), ENCODER_CAPACITY_HINT);
        ClickHouseSink {
            cfg,
            sink_id: sink_id.into(),
            client: None,
            schema,
            encoder,
            plan: None,
            delivery: Delivery::AT_LEAST_ONCE,
        }
    }

    /// Whether `open` has built the HTTP client yet.
    #[cfg(test)]
    pub(crate) fn has_client(&self) -> bool {
        self.client.is_some()
    }

    /// Runs a probe query and returns its rows as strings, one `Vec` per row
    /// with the requested columns in order. A server that stops answering is a
    /// Transient error at `open` (the runtime retries the probe until the
    /// server returns or shutdown stops it).
    async fn query_strings(
        &self,
        op: &'static str,
        sql: &str,
        columns: &[&str],
    ) -> Result<Vec<Vec<String>>, ThadamError> {
        let timeout = self.cfg.request_timeout;
        let timed_out = || {
            ThadamError::transient(
                ErrorDomain::Sink,
                codes::SINK_TIMEOUT,
                format!("the ClickHouse {op} probe timed out after {timeout:?}"),
            )
        };
        let client = self.client.as_ref().ok_or_else(|| {
            ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_WRITE_FAILED,
                "the ClickHouse client is built by `open`",
            )
        })?;
        let mut cursor = client
            .query(sql)
            .fetch_native()
            .map_err(|e| classify_at_open(&e))?;
        let mut rows = Vec::new();
        loop {
            let next = tokio::time::timeout(timeout, cursor.next())
                .await
                .map_err(|_| timed_out())?;
            let Some(block) = next.map_err(|e| classify_at_open(&e))? else {
                return Ok(rows);
            };
            let mut cols = Vec::with_capacity(columns.len());
            for name in columns {
                cols.push(string_column(&block, name, op)?);
            }
            for i in 0..block.num_rows() {
                rows.push(cols.iter().map(|c| c[i].clone()).collect());
            }
        }
    }

    async fn server_version(&self) -> Result<String, ThadamError> {
        let rows = self
            .query_strings("version", "SELECT version()", &["version()"])
            .await?;
        rows.into_iter()
            .next()
            .and_then(|r| r.into_iter().next())
            .ok_or_else(|| unavailable("the version probe returned no rows".to_owned()))
    }

    async fn describe_table(&self) -> Result<Vec<TableCol>, ThadamError> {
        let sql = describe_sql(&self.cfg.database, &self.cfg.table);
        let rows = self
            .query_strings("describe", &sql, &["name", "type", "default_type"])
            .await?;
        Ok(rows
            .into_iter()
            .map(|mut r| {
                let default_kind = r.pop().unwrap_or_default();
                let ty = r.pop().unwrap_or_default();
                let name = r.pop().unwrap_or_default();
                TableCol {
                    ty: types::parse_ch_type(&ty),
                    name,
                    default_kind,
                }
            })
            .collect())
    }

    /// Reads the engine facts the dedup decision needs. A failure here never
    /// aborts `open`: the delivery class degrades to the honest weaker claim
    /// (and `dedup: token` turns the gap into a Config error in `decide`).
    async fn engine_probe(&self) -> DedupFacts {
        let engine_sql = format!(
            "SELECT engine, engine_full FROM system.tables WHERE database = {} AND name = {}",
            lit(&self.cfg.database),
            lit(&self.cfg.table),
        );
        let (engine, engine_full) = match self
            .query_strings("engine", &engine_sql, &["engine", "engine_full"])
            .await
        {
            Ok(rows) => match rows.into_iter().next() {
                Some(mut row) => {
                    let full = row.pop().unwrap_or_default();
                    (row.pop(), full)
                }
                None => (None, String::new()),
            },
            Err(e) => {
                tracing::warn!(sink = %self.sink_id, error = %e, "dedup engine probe failed; reporting at-least-once");
                (None, String::new())
            }
        };
        let settings_sql = format!(
            "SELECT name, value FROM system.merge_tree_settings WHERE name IN ({}, {})",
            lit(probe::NON_REPLICATED_WINDOW),
            lit(probe::REPLICATED_WINDOW),
        );
        let mut facts = DedupFacts {
            engine,
            engine_full,
            ..DedupFacts::default()
        };
        match self
            .query_strings("merge_tree_settings", &settings_sql, &["name", "value"])
            .await
        {
            Ok(rows) => {
                for row in rows {
                    let [name, value] = row.as_slice() else {
                        continue;
                    };
                    let value = value.parse::<u64>().ok();
                    match name.as_str() {
                        probe::NON_REPLICATED_WINDOW => {
                            facts.default_non_replicated_window = value;
                        }
                        probe::REPLICATED_WINDOW => facts.default_replicated_window = value,
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!(sink = %self.sink_id, error = %e, "merge_tree_settings probe failed; reporting at-least-once");
            }
        }
        facts
    }
}

/// One string column of a probe block, by name.
fn string_column(block: &Block, name: &str, op: &'static str) -> Result<Vec<String>, ThadamError> {
    let col = block.column_by_name(name).ok_or_else(|| {
        unavailable(format!(
            "the ClickHouse {op} response has no `{name}` column"
        ))
    })?;
    col.iter::<String>()
        .map_err(|e: ChError| classify_at_open(&e))?
        .map(|cell| cell.map_err(|e| classify_at_open(&e)))
        .collect()
}

#[async_trait]
impl BatchSink for ClickHouseSink {
    async fn open(&mut self, _ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        if self.cfg.is_plain_http() && self.cfg.password.is_some() {
            tracing::warn!(
                sink = %self.sink_id,
                "the ClickHouse password is sent over plain http; use an https:// url outside local setups"
            );
        }
        if self.client.is_none() {
            let cfg = self.cfg.clone();
            let built = tokio::task::spawn_blocking(move || make_client(&cfg))
                .await
                .map_err(|e| {
                    ThadamError::fatal(
                        ErrorDomain::Sink,
                        codes::SINK_WRITE_FAILED,
                        format!("the ClickHouse client task failed: {e}"),
                    )
                })??;
            self.client = Some(built);
        }
        let version = self.server_version().await?;
        let describe = self.describe_table().await?;
        if describe.is_empty() {
            return Err(ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                format!(
                    "DESCRIBE TABLE {}.{} returned no columns; Thadam never creates tables",
                    ident(&self.cfg.database),
                    ident(&self.cfg.table),
                ),
            ));
        }
        let fields: Vec<_> = self
            .schema
            .arrow_schema()
            .fields()
            .iter()
            .cloned()
            .collect();
        let plan = types::build_plan(&fields, &describe, self.cfg.columns.as_ref()).map_err(
            |problems| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    format!(
                        "dataset `{}` does not fit table {}.{} ({} problem(s)): {}. Fix the table \
                         or the `columns` mapping; Thadam never creates or alters tables",
                        self.schema.name(),
                        ident(&self.cfg.database),
                        ident(&self.cfg.table),
                        problems.len(),
                        problems.join(" | "),
                    ),
                )
            },
        )?;
        if plan.cols.is_empty() {
            return Err(ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                "no dataset column maps to any table column, so there is nothing to insert",
            ));
        }

        let facts = self.engine_probe().await;
        let delivery = probe::decide(self.cfg.dedup, &facts)?;

        tracing::info!(
            sink = %self.sink_id,
            version = %version,
            table = %format!("{}.{}", ident(&self.cfg.database), ident(&self.cfg.table)),
            columns = plan.cols.len(),
            server_filled = ?plan.unmapped_table_cols,
            engine = ?facts.engine,
            dedup_token = delivery.send_token,
            delivery = delivery.class.as_str(),
            "clickhouse sink opened"
        );
        if delivery.class == DeliveryClass::AtLeastOnce {
            tracing::warn!(
                sink = %self.sink_id,
                "the target table has no deduplication window: a batch retried after a timeout or crash can be inserted twice"
            );
        }

        self.plan = Some(plan);
        self.delivery = delivery;
        Ok(SinkCapabilities::new(delivery.class))
    }

    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        let (Some(plan), Some(client)) = (&self.plan, &self.client) else {
            return Err(ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_WRITE_FAILED,
                "write before a successful open",
            ));
        };
        let rejects = self.encoder.push_batch(batch.batch());
        if self.encoder.is_empty() {
            // Every event failed schema encoding: nothing to insert.
            return Ok(WriteOutcome::with_rejects(rejects));
        }
        let arrow_batch = self.encoder.finish();
        let Some(block) = block::build_block(plan, &arrow_batch).map_err(|e| encode_error(&e))?
        else {
            return Ok(WriteOutcome::with_rejects(rejects));
        };

        let timeout = Some(self.cfg.request_timeout);
        let mut insert = client
            .insert_native(&self.cfg.table)
            .with_timeouts(timeout, timeout);
        if self.delivery.send_token {
            insert = insert.with_setting("insert_deduplicate", "1").with_setting(
                "insert_deduplication_token",
                dedup_token(&self.sink_id, &batch.id()),
            );
        }
        insert.write(&block).await.map_err(|e| classify(&e))?;
        insert.end().await.map_err(|e| classify(&e))?;
        Ok(WriteOutcome::with_rejects(rejects))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use thadam_core::{WalEpoch, WalPos, WalRange};

    use super::*;

    fn id(epoch: u64, first: u64, last: u64) -> BatchId {
        BatchId::from_range(
            WalRange::new(WalEpoch::new(epoch), WalPos::new(first), WalPos::new(last)).unwrap(),
        )
    }

    #[test]
    fn the_token_is_deterministic_and_scoped_by_sink() {
        let a = id(0xab, 3, 9);
        assert_eq!(dedup_token("ch-imu", &a), "ab-ch-imu-3-9");
        assert_eq!(
            dedup_token("ch-imu", &a),
            dedup_token("ch-imu", &id(0xab, 3, 9))
        );
        assert_ne!(dedup_token("ch-imu", &a), dedup_token("ch-gps", &a));
        // Bisected halves and a later epoch never share a token.
        assert_ne!(dedup_token("s", &a), dedup_token("s", &id(0xab, 3, 5)));
        assert_ne!(dedup_token("s", &a), dedup_token("s", &id(0xac, 3, 9)));
    }

    #[test]
    fn identifiers_and_literals_are_quoted() {
        assert_eq!(ident("imu"), "`imu`");
        assert_eq!(lit("telemetry"), "'telemetry'");
    }

    #[test]
    fn describe_sql_backticks_identifiers_with_dots() {
        // A dotted name is one quoted identifier, never nested or
        // database-qualified.
        assert_eq!(
            describe_sql("telemetry", "poses.v2"),
            "DESCRIBE TABLE `telemetry`.`poses.v2`"
        );
        assert_eq!(ident("pose.position.x"), "`pose.position.x`");
        assert_eq!(ident("a.b.c"), "`a.b.c`");
    }

    #[test]
    fn encode_errors_are_permanent_schema_errors() {
        let e = encode_error(&EncodeError::Range("c".to_owned(), "x".to_owned()));
        assert_eq!(e.retry, thadam_core::Retry::Permanent);
        assert_eq!(e.domain, ErrorDomain::Schema);
    }
}
