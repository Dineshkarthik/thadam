//! SQLite (bundled) in WAL mode as a queue table: one row per frame.
//!
//! fsync mapping:
//! - always:   synchronous=FULL, one transaction per group (COMMIT fsyncs the -wal file).
//! - interval: synchronous=FULL, groups accumulate in one open transaction; `sync` COMMITs.
//! - never:    synchronous=OFF, one transaction per group.
//!
//! With sync kind `full`, PRAGMA fullfsync/checkpoint_fullfsync make SQLite use F_FULLFSYNC on macOS.

use crate::engine::{Engine, FsyncMode, Opts, Result, SyncKind};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

pub struct Sqlite {
    conn: Connection,
    fsync: FsyncMode,
    in_txn: bool,
    next_seq: u64,
    poisoned: Option<String>,
}

impl Sqlite {
    pub fn open(dir: &Path, o: &Opts) -> Result<Self> {
        let conn = Connection::open(dir.join("queue.db"))?;
        // auto_vacuum must be set before the first table is created.
        conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL;")?;
        let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if mode != "wal" {
            return Err(format!("journal_mode is {mode}").into());
        }
        let sync = if o.fsync == FsyncMode::Never { "OFF" } else { "FULL" };
        let full = if o.sync_kind == SyncKind::Full { "ON" } else { "OFF" };
        conn.execute_batch(&format!(
            "PRAGMA synchronous={sync};
             PRAGMA fullfsync={full};
             PRAGMA checkpoint_fullfsync={full};
             PRAGMA journal_size_limit=16777216;
             CREATE TABLE IF NOT EXISTS q(seq INTEGER PRIMARY KEY, data BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS cursors(name TEXT PRIMARY KEY, seq INTEGER NOT NULL);"
        ))?;
        let max_q: Option<i64> = conn.query_row("SELECT max(seq) FROM q", [], |r| r.get(0))?;
        let max_c: Option<i64> = conn.query_row("SELECT max(seq) FROM cursors", [], |r| r.get(0))?;
        let next_seq = max_q.unwrap_or(0).max(max_c.unwrap_or(0)) as u64 + 1;
        Ok(Sqlite { conn, fsync: o.fsync, in_txn: false, next_seq, poisoned: None })
    }

    fn check(&self) -> Result<()> {
        match &self.poisoned {
            Some(w) => Err(format!("poisoned: {w}").into()),
            None => Ok(()),
        }
    }

    fn commit(&mut self) -> Result<()> {
        if self.in_txn {
            self.in_txn = false;
            if let Err(e) = self.conn.execute_batch("COMMIT") {
                self.poisoned = Some(e.to_string());
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(e.into());
            }
        }
        Ok(())
    }
}

impl Engine for Sqlite {
    fn append_group(&mut self, frames: &[&[u8]]) -> Result<u64> {
        self.check()?;
        if !self.in_txn {
            self.conn.execute_batch("BEGIN IMMEDIATE")?;
            self.in_txn = true;
        }
        let first = self.next_seq;
        let mut st = self.conn.prepare_cached("INSERT INTO q(seq, data) VALUES (?1, ?2)")?;
        for p in frames {
            if let Err(e) = st.execute(params![self.next_seq as i64, p]) {
                self.poisoned = Some(e.to_string());
                return Err(e.into());
            }
            self.next_seq += 1;
        }
        Ok(first)
    }

    fn flush(&mut self) -> Result<()> {
        self.check()?;
        if self.fsync == FsyncMode::Never {
            self.commit()?;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.check()?;
        self.commit()
    }

    fn read(&mut self, after: u64, max_bytes: usize, f: &mut dyn FnMut(u64, &[u8])) -> Result<usize> {
        let mut st = self.conn.prepare_cached("SELECT seq, data FROM q WHERE seq > ?1 ORDER BY seq")?;
        let mut rows = st.query(params![after as i64])?;
        let (mut n, mut bytes) = (0, 0);
        while bytes < max_bytes {
            let Some(row) = rows.next()? else { break };
            let seq: i64 = row.get(0)?;
            let data = row.get_ref(1)?.as_blob()?;
            f(seq as u64, data);
            bytes += data.len();
            n += 1;
        }
        Ok(n)
    }

    fn commit_cursor(&mut self, name: &str, seq: u64) -> Result<()> {
        self.sync()?;
        if self.fsync == FsyncMode::Never {
            self.conn.execute_batch("PRAGMA synchronous=FULL")?;
        }
        let r = self
            .conn
            .execute("INSERT OR REPLACE INTO cursors(name, seq) VALUES (?1, ?2)", params![name, seq as i64]);
        if self.fsync == FsyncMode::Never {
            self.conn.execute_batch("PRAGMA synchronous=OFF")?;
        }
        r?;
        Ok(())
    }

    fn load_cursor(&mut self, name: &str) -> Result<Option<u64>> {
        let v: Option<i64> = self
            .conn
            .query_row("SELECT seq FROM cursors WHERE name = ?1", params![name], |r| r.get(0))
            .optional()?;
        Ok(v.map(|v| v as u64))
    }

    fn delete_consumed(&mut self, upto: u64) -> Result<()> {
        self.sync()?;
        self.conn.execute("DELETE FROM q WHERE seq <= ?1", params![upto as i64])?;
        // incremental_vacuum frees about one page per sqlite3_step, so it must be stepped to
        // completion (execute_batch steps once).
        let mut st = self.conn.prepare_cached("PRAGMA incremental_vacuum")?;
        let mut rows = st.query([])?;
        while rows.next()?.is_some() {}
        drop(rows);
        // In WAL mode freed pages only leave the files at checkpoint time.
        self.conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(())
    }

    fn next_seq(&self) -> u64 {
        self.next_seq
    }

    fn first_seq(&mut self) -> Result<u64> {
        let v: Option<i64> = self.conn.query_row("SELECT min(seq) FROM q", [], |r| r.get(0))?;
        Ok(v.map_or(self.next_seq, |v| v as u64))
    }
}
