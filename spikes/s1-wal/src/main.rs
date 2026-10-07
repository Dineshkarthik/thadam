mod bench;
mod crash;
mod engine;
mod fault;
mod metrics;
mod redbq;
mod seglog;
mod sqlite;
mod torn;

use clap::{ArgAction, Args, Parser, Subcommand};
use engine::{FsyncMode, Kind, Opts, SyncKind};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Spike S1: durable buffer engine comparison")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct EArgs {
    #[arg(long)]
    engine: Kind,
    #[arg(long, default_value = "always")]
    fsync: FsyncMode,
    #[arg(long, default_value = "data")]
    sync_kind: SyncKind,
    #[arg(long, default_value_t = 64 << 20)]
    segment_bytes: u64,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    prealloc: bool,
    #[arg(long)]
    redb_quick_repair: bool,
    #[arg(long)]
    redb_cache_mib: Option<usize>,
    #[arg(long)]
    dir: PathBuf,
}

impl EArgs {
    fn opts(&self) -> Opts {
        Opts {
            kind: self.engine,
            fsync: self.fsync,
            sync_kind: self.sync_kind,
            segment_bytes: self.segment_bytes,
            prealloc: self.prealloc,
            redb_quick_repair: self.redb_quick_repair,
            redb_cache_mib: self.redb_cache_mib,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut v = vec![
            "--engine".into(),
            format!("{:?}", self.engine).to_lowercase(),
            "--fsync".into(),
            self.fsync.to_string(),
            "--sync-kind".into(),
            self.sync_kind.to_string(),
            "--segment-bytes".into(),
            self.segment_bytes.to_string(),
            "--prealloc".into(),
            self.prealloc.to_string(),
            "--dir".into(),
            self.dir.display().to_string(),
        ];
        if self.redb_quick_repair {
            v.push("--redb-quick-repair".into());
        }
        if let Some(m) = self.redb_cache_mib {
            v.extend(["--redb-cache-mib".into(), m.to_string()]);
        }
        v
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Sustained append throughput and commit latency.
    Bench {
        #[command(flatten)]
        e: EArgs,
        #[arg(long, default_value_t = 200)]
        rec_size: usize,
        #[arg(long, default_value_t = 64)]
        batch: u32,
        #[arg(long, default_value_t = 4)]
        producers: usize,
        #[arg(long, default_value_t = 16)]
        window: usize,
        #[arg(long, default_value_t = 10.0)]
        secs: f64,
        #[arg(long, default_value_t = 1.0)]
        warmup: f64,
        #[arg(long)]
        rate_mbps: Option<f64>,
        #[arg(long, default_value_t = 4 << 20)]
        group_max: usize,
    },
    /// Replay throughput from the start of a backlog.
    Read {
        #[command(flatten)]
        e: EArgs,
        #[arg(long, default_value_t = 200)]
        rec_size: usize,
        #[arg(long, default_value_t = 64)]
        batch: u32,
        #[arg(long, default_value_t = 512)]
        mb: u64,
    },
    /// Disk usage under churn and reclaim after consumers advance.
    Churn {
        #[command(flatten)]
        e: EArgs,
        #[arg(long, default_value_t = 200)]
        rec_size: usize,
        #[arg(long, default_value_t = 64)]
        batch: u32,
        #[arg(long, default_value_t = 2048)]
        total_mb: u64,
        #[arg(long, default_value_t = 128)]
        backlog_mb: u64,
    },
    /// Recovery time after kill -9 with a given backlog.
    Recovery {
        #[command(flatten)]
        e: EArgs,
        #[arg(long, default_value_t = 200)]
        rec_size: usize,
        #[arg(long, default_value_t = 64)]
        batch: u32,
        #[arg(long, default_value_t = 256)]
        mb: u64,
        #[arg(long)]
        drop_caches: bool,
    },
    #[command(hide = true)]
    Fill {
        #[command(flatten)]
        e: EArgs,
        #[arg(long)]
        rec_size: usize,
        #[arg(long)]
        batch: u32,
        #[arg(long)]
        mb: u64,
    },
    /// kill -9 loop with full verification after every crash.
    Crash {
        #[command(flatten)]
        e: EArgs,
        #[arg(long, default_value_t = 200)]
        iters: u32,
        #[arg(long, default_value_t = 250)]
        max_ms: u64,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long)]
        after_kill: Option<String>,
    },
    #[command(hide = true)]
    CrashChild {
        #[command(flatten)]
        e: EArgs,
        #[arg(long)]
        run: u32,
        #[arg(long)]
        seed: u64,
    },
    /// Exhaustive torn-tail and corruption tests for the custom log.
    Torn {
        #[arg(long)]
        dir: PathBuf,
    },
    /// fsync EIO injection (Linux, under LD_PRELOAD=failsync.so).
    FsyncFault {
        #[command(flatten)]
        e: EArgs,
    },
    /// Fill the filesystem with fsync=always and check recovery.
    Enospc {
        #[command(flatten)]
        e: EArgs,
    },
}

fn main() {
    let cli = Cli::parse();
    let r = match cli.cmd {
        Cmd::Bench { e, rec_size, batch, producers, window, secs, warmup, rate_mbps, group_max } => bench::bench(
            &e.dir,
            &e.opts(),
            &bench::BenchCfg { rec_size, batch, producers, window, secs, warmup, rate_mbps, group_max },
        ),
        Cmd::Read { e, rec_size, batch, mb } => bench::read_bench(&e.dir, &e.opts(), rec_size, batch, mb),
        Cmd::Churn { e, rec_size, batch, total_mb, backlog_mb } => {
            bench::churn(&e.dir, &e.opts(), rec_size, batch, total_mb, backlog_mb)
        }
        Cmd::Recovery { e, rec_size, batch, mb, drop_caches } => {
            bench::recovery(&e.dir, &e.opts(), &e.to_args(), rec_size, batch, mb, drop_caches)
        }
        Cmd::Fill { e, rec_size, batch, mb } => {
            bench::fill(&e.dir, &e.opts(), rec_size, batch, mb).map(|_| serde_json::Value::Null)
        }
        Cmd::Crash { e, iters, max_ms, seed, after_kill } => {
            crash::parent(&e.dir, &e.opts(), &e.to_args(), iters, max_ms, seed, after_kill.as_deref())
        }
        Cmd::CrashChild { e, run, seed } => crash::child(&e.dir, &e.opts(), run, seed).map(|_| serde_json::Value::Null),
        Cmd::Torn { dir } => torn::run(&dir),
        Cmd::FsyncFault { e } => fault::fsync_fault(&e.dir, &e.opts()),
        Cmd::Enospc { e } => fault::enospc(&e.dir, &e.opts()),
    };
    match r {
        Ok(serde_json::Value::Null) => {}
        Ok(v) => println!("{v}"),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
