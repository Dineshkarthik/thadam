//! fsync-error and ENOSPC behavior.
//!
//! `fsync_fault` is meant to run under LD_PRELOAD=failsync.so (see fault/failsync.c): creating the
//! file named by $FAILSYNC_ARM makes the next fsync/fdatasync return EIO (once, or every call while
//! the file exists with FAILSYNC_MODE=sticky). The page cache is untouched, so this tests only how
//! each engine reacts to the error, not real data loss. SQLite and redb are driven through their
//! raw APIs so the result reflects the library, not this spike's wrappers.

use crate::engine::{self, Engine, FsyncMode, Kind, Opts, Result, payload};
use serde_json::{Value, json};
use std::path::Path;

fn outcome<T, E: std::fmt::Display>(r: &std::result::Result<T, E>) -> Value {
    match r {
        Ok(_) => json!("ok"),
        Err(e) => json!(format!("error: {e}")),
    }
}

fn frame(seq: u64) -> Vec<u8> {
    let mut p = Vec::new();
    payload::build(&mut p, 0, seq, 4, 200);
    p
}

trait Raw {
    fn commit_one(&mut self, seq: u64) -> std::result::Result<(), String>;
}

struct RawSqlite(rusqlite::Connection);
impl Raw for RawSqlite {
    fn commit_one(&mut self, seq: u64) -> std::result::Result<(), String> {
        let c = &self.0;
        c.execute_batch("BEGIN IMMEDIATE").map_err(|e| e.to_string())?;
        if let Err(e) = c.execute("INSERT INTO q(seq, data) VALUES (?1, ?2)", rusqlite::params![seq as i64, frame(seq)]) {
            let _ = c.execute_batch("ROLLBACK");
            return Err(e.to_string());
        }
        c.execute_batch("COMMIT").map_err(|e| {
            let _ = c.execute_batch("ROLLBACK");
            e.to_string()
        })
    }
}

struct RawRedb(redb::Database);
const Q: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("q");
impl Raw for RawRedb {
    fn commit_one(&mut self, seq: u64) -> std::result::Result<(), String> {
        let t = self.0.begin_write().map_err(|e| e.to_string())?;
        t.open_table(Q).map_err(|e| e.to_string())?.insert(seq, frame(seq).as_slice()).map_err(|e| e.to_string())?;
        t.commit().map_err(|e| e.to_string())
    }
}

struct SpikeLog(Box<dyn Engine>);
impl Raw for SpikeLog {
    fn commit_one(&mut self, seq: u64) -> std::result::Result<(), String> {
        let p = frame(seq);
        let r: Result<()> = (|| {
            self.0.append_group(&[&p])?;
            self.0.sync()
        })();
        r.map_err(|e| e.to_string())
    }
}

fn open_raw(dir: &Path, o: &Opts) -> Result<Box<dyn Raw>> {
    std::fs::create_dir_all(dir)?;
    Ok(match o.kind {
        Kind::Seglog => Box::new(SpikeLog(engine::open(dir, o)?)),
        Kind::Sqlite => {
            let c = rusqlite::Connection::open(dir.join("raw.db"))?;
            c.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
            c.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS q(seq INTEGER PRIMARY KEY, data BLOB NOT NULL);")?;
            Box::new(RawSqlite(c))
        }
        Kind::Redb => {
            let db = redb::Database::create(dir.join("raw.redb"))?;
            let t = db.begin_write()?;
            t.open_table(Q)?;
            t.commit()?;
            Box::new(RawRedb(db))
        }
    })
}

fn present(dir: &Path, o: &Opts, seqs: &[u64]) -> Result<Vec<bool>> {
    use redb::ReadableDatabase;
    Ok(match o.kind {
        Kind::Seglog => {
            let mut e = engine::open(dir, o)?;
            let mut have = Vec::new();
            let first = e.first_seq()?;
            let mut after = first - 1;
            loop {
                let a = after;
                if e.read(a, 1 << 20, &mut |s, p| {
                    if payload::verify(p) == Some((0, s)) {
                        have.push(s);
                    }
                    after = s;
                })? == 0
                {
                    break;
                }
            }
            seqs.iter().map(|s| have.contains(s)).collect()
        }
        Kind::Sqlite => {
            let c = rusqlite::Connection::open(dir.join("raw.db"))?;
            seqs.iter()
                .map(|s| c.query_row("SELECT count(*) FROM q WHERE seq=?1", [*s as i64], |r| r.get::<_, i64>(0)).map(|n| n == 1))
                .collect::<std::result::Result<_, _>>()?
        }
        Kind::Redb => {
            let db = redb::Database::create(dir.join("raw.redb"))?;
            let t = db.begin_read()?;
            let q = t.open_table(Q)?;
            seqs.iter().map(|s| q.get(*s).map(|v| v.is_some())).collect::<std::result::Result<_, _>>()?
        }
    })
}

pub fn fsync_fault(dir: &Path, o: &Opts) -> Result<Value> {
    let arm = std::env::var("FAILSYNC_ARM").map_err(|_| "FAILSYNC_ARM not set (run under LD_PRELOAD=failsync.so)")?;
    crate::bench::fresh_dir(dir)?;
    let mut o = o.clone();
    o.fsync = FsyncMode::Always;
    let mut eng = open_raw(dir, &o)?;
    for s in 1..=3 {
        eng.commit_one(s).map_err(|e| format!("pre-fault commit failed: {e}"))?;
    }
    std::fs::write(&arm, b"1")?;
    let faulted = eng.commit_one(4);
    let armed_after = Path::new(&arm).exists();
    let _ = std::fs::remove_file(&arm);
    let next = eng.commit_one(5);
    let next2 = eng.commit_one(6);
    drop(eng);
    let reopen = present(dir, &o, &[1, 2, 3, 4, 5, 6]);
    Ok(json!({
        "test": "fsync_fault",
        "engine": format!("{:?}", o.kind).to_lowercase(),
        "mode": std::env::var("FAILSYNC_MODE").unwrap_or_else(|_| "once".into()),
        "faulted_commit": format!("{faulted:?}"),
        "fault_still_armed_after": armed_after,
        "next_commit_same_handle": format!("{next:?}"),
        "second_commit_same_handle": format!("{next2:?}"),
        "after_reopen_present_1_to_6": match reopen { Ok(v) => json!(v), Err(e) => json!(e.to_string()) },
    }))
}

/// Appends with fsync=always until an error (expected ENOSPC), then checks the engine can be
/// reopened, every acknowledged frame is intact, and space can be reclaimed to continue.
pub fn enospc(dir: &Path, o: &Opts) -> Result<Value> {
    crate::bench::fresh_dir(dir)?;
    let mut o = o.clone();
    o.fsync = FsyncMode::Always;
    let mut acked = 0u64;
    let mut buf = Vec::new();
    let first_err = {
        let mut e = engine::open(dir, &o)?;
        loop {
            let seq = e.next_seq();
            payload::build(&mut buf, 0, seq, 16, 200);
            let r: Result<()> = (|| {
                e.append_group(&[&buf])?;
                e.flush()?;
                e.sync()
            })();
            match r {
                Ok(()) => acked = seq,
                Err(err) => break err.to_string(),
            }
        }
    };
    let mut steps = serde_json::Map::new();
    let reopen = engine::open(dir, &o);
    steps.insert("reopen".into(), outcome(&reopen));
    if let Ok(mut e) = reopen {
        let mut seen = 0u64;
        let mut bad = 0u64;
        let first = e.first_seq()?;
        let mut after = first - 1;
        loop {
            let a = after;
            match e.read(a, 1 << 20, &mut |s, p| {
                if payload::verify(p) != Some((0, s)) {
                    bad += 1;
                }
                seen = s;
                after = s;
            }) {
                Ok(0) => break,
                Ok(_) => {}
                Err(err) => {
                    steps.insert("read_error".into(), json!(err.to_string()));
                    break;
                }
            }
        }
        steps.insert("acked_intact".into(), json!(seen >= acked && bad == 0));
        let r: Result<()> = (|| {
            e.commit_cursor("c", acked)?;
            e.delete_consumed(acked)?;
            Ok(())
        })();
        steps.insert("reclaim".into(), outcome(&r));
        drop(e);
        let r: Result<()> = (|| {
            let mut e = engine::open(dir, &o)?;
            for _ in 0..8 {
                let seq = e.next_seq();
                payload::build(&mut buf, 0, seq, 16, 200);
                e.append_group(&[&buf])?;
                e.sync()?;
            }
            Ok(())
        })();
        steps.insert("append_after_reclaim_and_reopen".into(), outcome(&r));
    }
    Ok(json!({
        "test": "enospc",
        "engine": format!("{:?}", o.kind).to_lowercase(),
        "acked_before_error": acked,
        "first_error": first_err,
        "steps": steps,
        "disk_bytes": engine::disk_bytes(dir),
    }))
}
