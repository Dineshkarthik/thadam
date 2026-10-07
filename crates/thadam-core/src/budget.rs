//! Lock-free disk accounting shared by the WAL, segment sinks, and quarantine.

use std::fmt;

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};

/// What a reservation is for. Each category has its own counter and limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BudgetCategory {
    /// The capture WAL.
    Wal,
    /// Staging and finalized segment files.
    Segments,
    /// The quarantine sink, budgeted separately so a schema change cannot
    /// starve normal data.
    Quarantine,
}

impl BudgetCategory {
    /// Every category.
    pub const ALL: [BudgetCategory; 3] = [
        BudgetCategory::Wal,
        BudgetCategory::Segments,
        BudgetCategory::Quarantine,
    ];

    /// The stable lowercase name used in metrics and `/status`.
    pub const fn as_str(self) -> &'static str {
        match self {
            BudgetCategory::Wal => "wal",
            BudgetCategory::Segments => "segments",
            BudgetCategory::Quarantine => "quarantine",
        }
    }

    const fn index(self) -> usize {
        match self {
            BudgetCategory::Wal => 0,
            BudgetCategory::Segments => 1,
            BudgetCategory::Quarantine => 2,
        }
    }
}

impl fmt::Display for BudgetCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A reservation was refused because it would exceed the category's limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{category} budget exhausted: {used} used, {requested} requested, limit {limit}")]
pub struct BudgetExceeded {
    /// The category that is full.
    pub category: BudgetCategory,
    /// Bytes requested.
    pub requested: u64,
    /// Bytes in use when the request was refused.
    pub used: u64,
    /// The configured limit.
    pub limit: u64,
}

/// A point-in-time reading of one category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CategoryUsage {
    /// Bytes in use.
    pub used: u64,
    /// The limit, or `None` if unlimited.
    pub limit: Option<u64>,
}

struct Cell {
    used: AtomicU64,
    limit: AtomicU64,
}

impl Cell {
    fn new() -> Self {
        Cell {
            used: AtomicU64::new(0),
            limit: AtomicU64::new(UNLIMITED),
        }
    }
}

const UNLIMITED: u64 = u64::MAX;

/// Per-category byte counters with optional limits.
///
/// All operations are lock-free atomics. The counters guard no other memory,
/// so every access is `Relaxed`. Reservations are all-or-nothing and never
/// push `used` past the limit; [`DiskBudget::charge`] is the one exception,
/// for accounting data that already exists on disk at startup.
pub struct DiskBudget {
    cells: [Cell; 3],
    accounting_errors: AtomicU64,
}

impl Default for DiskBudget {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DiskBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("DiskBudget");
        for c in BudgetCategory::ALL {
            let usage = self.usage(c);
            s.field(c.as_str(), &(usage.used, usage.limit));
        }
        s.finish()
    }
}

impl DiskBudget {
    /// A budget with no limits and nothing in use.
    pub fn new() -> Self {
        DiskBudget {
            cells: [Cell::new(), Cell::new(), Cell::new()],
            accounting_errors: AtomicU64::new(0),
        }
    }

    /// Sets a limit while building.
    #[must_use]
    pub fn with_limit(self, category: BudgetCategory, bytes: u64) -> Self {
        self.set_limit(category, Some(bytes));
        self
    }

    fn cell(&self, category: BudgetCategory) -> &Cell {
        &self.cells[category.index()]
    }

    /// Changes a limit; `None` removes it. Lowering a limit below current use
    /// does not release anything but refuses further reservations.
    pub fn set_limit(&self, category: BudgetCategory, limit: Option<u64>) {
        self.cell(category)
            .limit
            .store(limit.unwrap_or(UNLIMITED), Ordering::Relaxed);
    }

    /// The limit, or `None` if unlimited.
    pub fn limit(&self, category: BudgetCategory) -> Option<u64> {
        match self.cell(category).limit.load(Ordering::Relaxed) {
            UNLIMITED => None,
            limit => Some(limit),
        }
    }

    /// Bytes in use.
    pub fn used(&self, category: BudgetCategory) -> u64 {
        self.cell(category).used.load(Ordering::Relaxed)
    }

    /// Bytes that can still be reserved; `u64::MAX` if unlimited.
    pub fn available(&self, category: BudgetCategory) -> u64 {
        match self.limit(category) {
            Some(limit) => limit.saturating_sub(self.used(category)),
            None => u64::MAX,
        }
    }

    /// Reserves `bytes` if they fit under the limit.
    pub fn try_reserve(&self, category: BudgetCategory, bytes: u64) -> Result<(), BudgetExceeded> {
        let cell = self.cell(category);
        let limit = cell.limit.load(Ordering::Relaxed);
        let mut used = cell.used.load(Ordering::Relaxed);
        loop {
            let next = used.checked_add(bytes).filter(|next| *next <= limit);
            let Some(next) = next else {
                return Err(BudgetExceeded {
                    category,
                    requested: bytes,
                    used,
                    limit,
                });
            };
            match cell
                .used
                .compare_exchange_weak(used, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(actual) => used = actual,
            }
        }
    }

    /// Adds `bytes` to the usage regardless of the limit, saturating. Used to
    /// account for data already on disk at startup.
    pub fn charge(&self, category: BudgetCategory, bytes: u64) {
        let cell = self.cell(category);
        let mut used = cell.used.load(Ordering::Relaxed);
        loop {
            match cell.used.compare_exchange_weak(
                used,
                used.saturating_add(bytes),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => used = actual,
            }
        }
    }

    /// Returns `bytes` to the budget. Releasing more than is in use is an
    /// accounting bug elsewhere: usage saturates at zero and
    /// [`DiskBudget::accounting_errors`] counts the event.
    pub fn release(&self, category: BudgetCategory, bytes: u64) {
        let cell = self.cell(category);
        let mut used = cell.used.load(Ordering::Relaxed);
        loop {
            let next = used.checked_sub(bytes);
            match cell.used.compare_exchange_weak(
                used,
                next.unwrap_or(0),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if next.is_none() {
                        self.accounting_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    return;
                }
                Err(actual) => used = actual,
            }
        }
    }

    /// How many times a release exceeded the bytes in use.
    pub fn accounting_errors(&self) -> u64 {
        self.accounting_errors.load(Ordering::Relaxed)
    }

    /// Whether current use exceeds the limit (possible after [`DiskBudget::charge`]
    /// or after lowering the limit).
    pub fn is_over_limit(&self, category: BudgetCategory) -> bool {
        self.limit(category)
            .is_some_and(|limit| self.used(category) > limit)
    }

    /// A reading of one category. The two values are read separately, so under
    /// concurrent updates they may not be from the same instant.
    pub fn usage(&self, category: BudgetCategory) -> CategoryUsage {
        CategoryUsage {
            used: self.used(category),
            limit: self.limit(category),
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;

    use BudgetCategory::{Segments, Wal};

    #[test]
    fn reserve_respects_limit_exactly() {
        let b = DiskBudget::new().with_limit(Wal, 100);
        assert_eq!(b.try_reserve(Wal, 60), Ok(()));
        assert_eq!(b.try_reserve(Wal, 40), Ok(()));
        let err = b.try_reserve(Wal, 1).unwrap_err();
        assert_eq!(
            err,
            BudgetExceeded {
                category: Wal,
                requested: 1,
                used: 100,
                limit: 100
            }
        );
        assert_eq!(b.available(Wal), 0);
        b.release(Wal, 10);
        assert_eq!(b.try_reserve(Wal, 10), Ok(()));
    }

    #[test]
    fn categories_are_independent() {
        let b = DiskBudget::new().with_limit(Wal, 10);
        b.try_reserve(Segments, 1 << 40).unwrap();
        assert_eq!(b.used(Wal), 0);
        assert_eq!(b.limit(Segments), None);
        assert_eq!(b.available(Segments), u64::MAX);
    }

    #[test]
    fn unlimited_reservation_overflow_is_refused_not_wrapped() {
        let b = DiskBudget::new();
        b.try_reserve(Wal, u64::MAX - 1).unwrap();
        assert!(b.try_reserve(Wal, 2).is_err());
        assert_eq!(b.used(Wal), u64::MAX - 1);
    }

    #[test]
    fn over_release_saturates_and_is_counted() {
        let b = DiskBudget::new();
        b.try_reserve(Wal, 5).unwrap();
        b.release(Wal, 9);
        assert_eq!(b.used(Wal), 0);
        assert_eq!(b.accounting_errors(), 1);
    }

    #[test]
    fn charge_can_exceed_the_limit_and_blocks_reservations() {
        let b = DiskBudget::new().with_limit(Wal, 10);
        b.charge(Wal, 25);
        assert!(b.is_over_limit(Wal));
        assert!(b.try_reserve(Wal, 1).is_err());
        b.release(Wal, 20);
        assert!(!b.is_over_limit(Wal));
        assert_eq!(b.try_reserve(Wal, 5), Ok(()));
    }

    #[test]
    fn lowering_the_limit_keeps_usage() {
        let b = DiskBudget::new();
        b.try_reserve(Wal, 50).unwrap();
        b.set_limit(Wal, Some(10));
        assert_eq!(b.used(Wal), 50);
        assert!(b.try_reserve(Wal, 1).is_err());
        b.set_limit(Wal, None);
        assert_eq!(b.try_reserve(Wal, 1), Ok(()));
    }

    #[test]
    fn concurrent_reservations_never_exceed_the_limit() {
        let b = Arc::new(DiskBudget::new().with_limit(Wal, 10_000));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let b = Arc::clone(&b);
                std::thread::spawn(move || {
                    (0..5_000).filter(|_| b.try_reserve(Wal, 3).is_ok()).count() as u64
                })
            })
            .collect();
        let granted: u64 = threads.into_iter().map(|t| t.join().unwrap()).sum();
        assert_eq!(b.used(Wal), granted * 3);
        assert!(b.used(Wal) <= 10_000);
        assert!(b.used(Wal) > 10_000 - 3);
    }

    proptest! {
        #[test]
        fn used_matches_a_sequential_model(ops in proptest::collection::vec((0u8..3, 0u64..50), 0..200)) {
            let limit = 200u64;
            let b = DiskBudget::new().with_limit(Wal, limit);
            let mut model = 0u64;
            for (op, n) in ops {
                match op {
                    0 => {
                        let ok = b.try_reserve(Wal, n).is_ok();
                        prop_assert_eq!(ok, model + n <= limit);
                        if ok { model += n; }
                    }
                    1 => {
                        b.release(Wal, n);
                        model = model.saturating_sub(n);
                    }
                    _ => {
                        b.charge(Wal, n);
                        model += n;
                    }
                }
                prop_assert_eq!(b.used(Wal), model);
            }
        }
    }
}
