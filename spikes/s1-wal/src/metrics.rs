/// Peak resident set size of this process in MiB.
pub fn maxrss_mib() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    #[cfg(target_os = "macos")]
    return ru.ru_maxrss as f64 / (1024.0 * 1024.0);
    #[cfg(target_os = "linux")]
    return ru.ru_maxrss as f64 / 1024.0;
}

/// Bytes this process caused to be written to storage, as reported by the OS
/// (macOS: proc_pid_rusage ri_diskio_byteswritten; Linux: /proc/self/io write_bytes).
pub fn os_bytes_written() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let mut ri: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::proc_pid_rusage(
                std::process::id() as i32,
                libc::RUSAGE_INFO_V2,
                &mut ri as *mut _ as *mut libc::rusage_info_t,
            )
        };
        (rc == 0).then_some(ri.ri_diskio_byteswritten)
    }
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/io").ok()?;
        s.lines().find_map(|l| l.strip_prefix("write_bytes: ")?.trim().parse().ok())
    }
}
