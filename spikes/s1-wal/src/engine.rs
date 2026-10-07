use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::Duration;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncMode {
    Always,
    Interval(Duration),
    Never,
}

impl std::str::FromStr for FsyncMode {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            _ => {
                let ms = s
                    .strip_prefix("interval:")
                    .and_then(|v| v.strip_suffix("ms"))
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| format!("bad fsync mode {s:?} (always|never|interval:<N>ms)"))?;
                Ok(Self::Interval(Duration::from_millis(ms)))
            }
        }
    }
}

impl std::fmt::Display for FsyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "always"),
            Self::Never => write!(f, "never"),
            Self::Interval(d) => write!(f, "interval:{}ms", d.as_millis()),
        }
    }
}

/// Which flush primitive "sync" maps to.
/// - `data`: fdatasync on Linux, plain fsync on macOS (does NOT flush the drive cache on macOS).
/// - `full`: fsync on Linux, fcntl(F_FULLFSYNC) on macOS.
/// - `barrier`: fcntl(F_BARRIERFSYNC) on macOS (ordering barrier only); not available on Linux.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncKind {
    Data,
    Full,
    Barrier,
}

impl std::str::FromStr for SyncKind {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "data" => Ok(Self::Data),
            "full" => Ok(Self::Full),
            "barrier" => Ok(Self::Barrier),
            _ => Err(format!("bad sync kind {s:?} (data|full|barrier)")),
        }
    }
}

impl std::fmt::Display for SyncKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Data => "data",
            Self::Full => "full",
            Self::Barrier => "barrier",
        })
    }
}

pub fn sync_fd(file: &File, kind: SyncKind) -> io::Result<()> {
    let fd = file.as_raw_fd();
    let rc = unsafe {
        match kind {
            #[cfg(target_os = "linux")]
            SyncKind::Data => libc::fdatasync(fd),
            #[cfg(target_os = "linux")]
            SyncKind::Full => libc::fsync(fd),
            #[cfg(target_os = "linux")]
            SyncKind::Barrier => {
                return Err(io::Error::new(io::ErrorKind::Unsupported, "barrier fsync is macOS-only"));
            }
            #[cfg(target_os = "macos")]
            SyncKind::Data => libc::fsync(fd),
            #[cfg(target_os = "macos")]
            SyncKind::Full => libc::fcntl(fd, libc::F_FULLFSYNC),
            #[cfg(target_os = "macos")]
            SyncKind::Barrier => libc::fcntl(fd, libc::F_BARRIERFSYNC),
        }
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

pub fn sync_dir(dir: &Path, kind: SyncKind) -> io::Result<()> {
    let kind = if kind == SyncKind::Barrier { SyncKind::Full } else { kind };
    sync_fd(&File::open(dir)?, kind)
}

/// Allocated bytes (st_blocks * 512) of every regular file under `dir`, recursively.
pub fn disk_bytes(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if let Ok(m) = e.metadata() {
                if m.is_dir() {
                    total += disk_bytes(&e.path());
                } else {
                    total += m.blocks() * 512;
                }
            }
        }
    }
    total
}

/// The operations every candidate engine implements.
///
/// Durability contract: frames passed to `append_group` are durable once `sync` returns Ok.
/// `flush` ends a group (push to the OS / commit non-durably where the engine supports it);
/// whether it makes anything durable depends on the engine's fsync mode. After any error from
/// `flush` or `sync` the engine must refuse further writes until reopened.
pub trait Engine {
    /// Stages one frame per element; returns the sequence number assigned to the first frame.
    fn append_group(&mut self, frames: &[&[u8]]) -> Result<u64>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    /// Calls `f(seq, payload)` for frames with seq > `after` in order, until about `max_bytes`
    /// of payload were delivered or the committed end is reached. Returns frames delivered.
    fn read(&mut self, after: u64, max_bytes: usize, f: &mut dyn FnMut(u64, &[u8])) -> Result<usize>;
    /// Durable on return.
    fn commit_cursor(&mut self, name: &str, seq: u64) -> Result<()>;
    fn load_cursor(&mut self, name: &str) -> Result<Option<u64>>;
    /// Reclaims storage for frames with seq <= `upto` (may keep some, e.g. whole segments).
    fn delete_consumed(&mut self, upto: u64) -> Result<()>;
    /// Seq of the next frame to be appended.
    fn next_seq(&self) -> u64;
    /// Lowest retained seq (next_seq if empty).
    fn first_seq(&mut self) -> Result<u64>;
    /// Engine-specific recovery details from the last open.
    fn info(&self) -> String {
        String::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Seglog,
    Sqlite,
    Redb,
}

impl std::str::FromStr for Kind {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "seglog" => Ok(Self::Seglog),
            "sqlite" => Ok(Self::Sqlite),
            "redb" => Ok(Self::Redb),
            _ => Err(format!("bad engine {s:?} (seglog|sqlite|redb)")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Opts {
    pub kind: Kind,
    pub fsync: FsyncMode,
    pub sync_kind: SyncKind,
    pub segment_bytes: u64,
    pub prealloc: bool,
    pub redb_quick_repair: bool,
    pub redb_cache_mib: Option<usize>,
}

pub fn open(dir: &Path, o: &Opts) -> Result<Box<dyn Engine>> {
    std::fs::create_dir_all(dir)?;
    Ok(match o.kind {
        Kind::Seglog => Box::new(crate::seglog::SegLog::open(dir, o)?),
        Kind::Sqlite => Box::new(crate::sqlite::Sqlite::open(dir, o)?),
        Kind::Redb => Box::new(crate::redbq::Redb::open(dir, o)?),
    })
}

/// Deterministic test payloads so readers can verify every byte.
pub mod payload {
    #[inline]
    fn mix(mut x: u64) -> u64 {
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^= x >> 33;
        x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
        x ^ (x >> 33)
    }

    /// Frame payload: [run u32][seq u64][nrec u32] then nrec x ([len u32][bytes]).
    /// Record bytes are a function of (run, seq, record index).
    pub fn build(buf: &mut Vec<u8>, run: u32, seq: u64, nrec: u32, rec_size: usize) {
        buf.clear();
        buf.extend_from_slice(&run.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&nrec.to_le_bytes());
        for i in 0..nrec {
            buf.extend_from_slice(&(rec_size as u32).to_le_bytes());
            let mut s = mix(((run as u64) << 40) ^ (seq << 8) ^ i as u64);
            let start = buf.len();
            buf.resize(start + rec_size, 0);
            for chunk in buf[start..].chunks_mut(8) {
                s = mix(s);
                chunk.copy_from_slice(&s.to_le_bytes()[..chunk.len()]);
            }
        }
    }

    /// Returns (run, seq) if the payload is byte-exact for those values.
    pub fn verify(p: &[u8]) -> Option<(u32, u64)> {
        if p.len() < 16 {
            return None;
        }
        let run = u32::from_le_bytes(p[0..4].try_into().unwrap());
        let seq = u64::from_le_bytes(p[4..12].try_into().unwrap());
        let nrec = u32::from_le_bytes(p[12..16].try_into().unwrap());
        let rec_size = if nrec == 0 {
            0
        } else {
            u32::from_le_bytes(p.get(16..20)?.try_into().unwrap()) as usize
        };
        let mut expect = Vec::with_capacity(p.len());
        build(&mut expect, run, seq, nrec, rec_size);
        (expect == p).then_some((run, seq))
    }
}
