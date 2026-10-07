//! Flush, lock, and preallocation primitives (safe `rustix` wrappers).

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

/// Flushes file data to stable storage: `fdatasync` on Linux,
/// `fcntl(F_FULLFSYNC)` on Apple platforms (plain `fsync` there does not flush
/// the drive cache).
pub(crate) fn sync_file(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    rustix::fs::fcntl_fullfsync(file)?;
    #[cfg(not(target_vendor = "apple"))]
    rustix::fs::fdatasync(file)?;
    Ok(())
}

/// Makes directory entries (create, rename, unlink) durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    let d = File::open(dir)?;
    #[cfg(target_vendor = "apple")]
    rustix::fs::fcntl_fullfsync(&d)?;
    #[cfg(not(target_vendor = "apple"))]
    rustix::fs::fsync(&d)?;
    Ok(())
}

/// Takes an exclusive, non-blocking `flock`. `Ok(None)` if it is held
/// elsewhere (another process, or another open handle in this process).
pub(crate) fn try_lock(path: &Path) -> io::Result<Option<File>> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match rustix::fs::flock(&f, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(Some(f)),
        Err(e) if e == rustix::io::Errno::WOULDBLOCK => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Reserves `len` bytes for `file` so later writes cannot hit ENOSPC.
#[cfg(target_os = "linux")]
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, len)?;
    Ok(())
}

/// Unit tests on other platforms emulate preallocation (a zero-filled tail)
/// so the accounting and recovery paths are covered everywhere.
#[cfg(all(test, not(target_os = "linux")))]
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    if file.metadata()?.len() < len {
        file.set_len(len)?;
    }
    Ok(())
}

#[cfg(all(not(test), not(target_os = "linux")))]
pub(crate) fn preallocate(_file: &File, _len: u64) -> io::Result<()> {
    Ok(())
}

/// Writes `bytes` to a temporary sibling, flushes it, renames it over `path`,
/// and flushes the directory.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?
        .to_owned();
    tmp_name.push(".tmp");
    let tmp = dir.join(tmp_name);
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    f.write_all_at(bytes, 0)?;
    sync_file(&f)?;
    drop(f);
    fs::rename(&tmp, path)?;
    sync_dir(dir)
}

/// Removes a file, treating "already gone" as success.
pub(crate) fn remove_if_exists(path: &Path) -> io::Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
