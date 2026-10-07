//! redb queue table: one entry per frame keyed by seq.
//!
//! fsync mapping:
//! - always:   one Durability::Immediate transaction per group.
//! - interval: groups accumulate in one open write transaction; `sync` commits it (Immediate).
//! - never:    one Durability::None transaction per group.
//!
//! redb calls File::sync_data, which Rust std maps to F_FULLFSYNC on macOS, so `sync_kind` is ignored.

use crate::engine::{Engine, FsyncMode, Opts, Result};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use std::path::Path;

const Q: TableDefinition<u64, &[u8]> = TableDefinition::new("q");
const CUR: TableDefinition<&str, u64> = TableDefinition::new("cursors");

pub struct Redb {
    db: Database,
    fsync: FsyncMode,
    quick_repair: bool,
    txn: Option<WriteTransaction>,
    next_seq: u64,
    poisoned: Option<String>,
}

impl Redb {
    pub fn open(dir: &Path, o: &Opts) -> Result<Self> {
        let path = dir.join("queue.redb");
        if !path.exists() {
            // A kill -9 inside Database::create leaves a file whose magic is still zero, which redb
            // then refuses to open, so initialise under a temporary name and rename into place.
            let tmp = dir.join("queue.redb.tmp");
            let _ = std::fs::remove_file(&tmp);
            let db = Database::create(&tmp)?;
            let t = db.begin_write()?;
            t.open_table(Q)?;
            t.open_table(CUR)?;
            t.commit()?;
            drop(db);
            std::fs::rename(&tmp, &path)?;
            crate::engine::sync_dir(dir, crate::engine::SyncKind::Full)?;
        }
        let mut b = redb::Builder::new();
        if let Some(mib) = o.redb_cache_mib {
            b.set_cache_size(mib << 20);
        }
        let db = b.create(&path)?;
        let mut me = Redb { db, fsync: o.fsync, quick_repair: o.redb_quick_repair, txn: None, next_seq: 1, poisoned: None };
        let t = me.begin(Durability::Immediate)?;
        let mut max = 0;
        {
            let q = t.open_table(Q)?;
            if let Some((k, _)) = q.last()? {
                max = k.value();
            }
            let c = t.open_table(CUR)?;
            for e in c.iter()? {
                max = max.max(e?.1.value());
            }
        }
        t.commit()?;
        me.next_seq = max + 1;
        Ok(me)
    }

    fn begin(&self, d: Durability) -> Result<WriteTransaction> {
        let mut t = self.db.begin_write()?;
        t.set_durability(d)?;
        t.set_quick_repair(self.quick_repair);
        Ok(t)
    }

    fn check(&self) -> Result<()> {
        match &self.poisoned {
            Some(w) => Err(format!("poisoned: {w}").into()),
            None => Ok(()),
        }
    }

    fn commit(&mut self) -> Result<()> {
        if let Some(t) = self.txn.take() {
            if let Err(e) = t.commit() {
                self.poisoned = Some(e.to_string());
                return Err(e.into());
            }
        }
        Ok(())
    }
}

impl Engine for Redb {
    fn append_group(&mut self, frames: &[&[u8]]) -> Result<u64> {
        self.check()?;
        if self.txn.is_none() {
            let d = if self.fsync == FsyncMode::Never { Durability::None } else { Durability::Immediate };
            self.txn = Some(self.begin(d)?);
        }
        let first = self.next_seq;
        let t = self.txn.as_ref().unwrap();
        let r: Result<()> = (|| {
            let mut q = t.open_table(Q)?;
            for (i, p) in frames.iter().enumerate() {
                q.insert(first + i as u64, *p)?;
            }
            Ok(())
        })();
        if let Err(e) = r {
            self.poisoned = Some(e.to_string());
            return Err(e);
        }
        self.next_seq += frames.len() as u64;
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
        let t = self.db.begin_read()?;
        let q = t.open_table(Q)?;
        let (mut n, mut bytes) = (0, 0);
        for e in q.range(after + 1..)? {
            let (k, v) = e?;
            f(k.value(), v.value());
            bytes += v.value().len();
            n += 1;
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(n)
    }

    fn commit_cursor(&mut self, name: &str, seq: u64) -> Result<()> {
        self.sync()?;
        let t = self.begin(Durability::Immediate)?;
        t.open_table(CUR)?.insert(name, seq)?;
        t.commit()?;
        Ok(())
    }

    fn load_cursor(&mut self, name: &str) -> Result<Option<u64>> {
        let t = self.db.begin_read()?;
        let c = t.open_table(CUR)?;
        Ok(c.get(name)?.map(|v| v.value()))
    }

    fn delete_consumed(&mut self, upto: u64) -> Result<()> {
        self.sync()?;
        let d = if self.fsync == FsyncMode::Never { Durability::None } else { Durability::Immediate };
        let t = self.begin(d)?;
        t.open_table(Q)?.retain_in(..=upto, |_, _| false)?;
        t.commit()?;
        Ok(())
    }

    fn next_seq(&self) -> u64 {
        self.next_seq
    }

    fn first_seq(&mut self) -> Result<u64> {
        let t = self.db.begin_read()?;
        let q = t.open_table(Q)?;
        Ok(q.first()?.map_or(self.next_seq, |(k, _)| k.value()))
    }
}
