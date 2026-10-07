//! Counting global allocator. Installed only by the `alloc_report` binary;
//! the criterion bench and `xthread` use the plain system allocator so that
//! timings are not perturbed by the counters.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES_REQUESTED: AtomicU64 = AtomicU64::new(0);
static LIVE_REQUESTED: AtomicI64 = AtomicI64::new(0);
static LIVE_USABLE: AtomicI64 = AtomicI64::new(0);

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn malloc_size(ptr: *const std::ffi::c_void) -> usize;
}

#[inline]
fn usable(ptr: *mut u8, fallback: usize) -> usize {
    #[cfg(target_os = "macos")]
    {
        let _ = fallback;
        // SAFETY: ptr was just returned by the system allocator (malloc-backed).
        unsafe { malloc_size(ptr as *const _) }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = ptr;
        fallback
    }
}

pub struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES_REQUESTED.fetch_add(l.size() as u64, Relaxed);
            LIVE_REQUESTED.fetch_add(l.size() as i64, Relaxed);
            LIVE_USABLE.fetch_add(usable(p, l.size()) as i64, Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES_REQUESTED.fetch_add(l.size() as u64, Relaxed);
            LIVE_REQUESTED.fetch_add(l.size() as i64, Relaxed);
            LIVE_USABLE.fetch_add(usable(p, l.size()) as i64, Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE_REQUESTED.fetch_sub(l.size() as i64, Relaxed);
        LIVE_USABLE.fetch_sub(usable(p, l.size()) as i64, Relaxed);
        unsafe { System.dealloc(p, l) }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let old_usable = usable(p, l.size());
        let np = unsafe { System.realloc(p, l, new_size) };
        if !np.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES_REQUESTED.fetch_add(new_size as u64, Relaxed);
            LIVE_REQUESTED.fetch_add(new_size as i64 - l.size() as i64, Relaxed);
            LIVE_USABLE.fetch_add(usable(np, new_size) as i64 - old_usable as i64, Relaxed);
        }
        np
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    pub allocs: u64,
    pub bytes_requested: u64,
    pub live_requested: i64,
    pub live_usable: i64,
}

impl Snapshot {
    pub fn now() -> Self {
        Snapshot {
            allocs: ALLOCS.load(Relaxed),
            bytes_requested: BYTES_REQUESTED.load(Relaxed),
            live_requested: LIVE_REQUESTED.load(Relaxed),
            live_usable: LIVE_USABLE.load(Relaxed),
        }
    }

    pub fn since(self, earlier: Snapshot) -> Snapshot {
        Snapshot {
            allocs: self.allocs - earlier.allocs,
            bytes_requested: self.bytes_requested - earlier.bytes_requested,
            live_requested: self.live_requested - earlier.live_requested,
            live_usable: self.live_usable - earlier.live_usable,
        }
    }
}
