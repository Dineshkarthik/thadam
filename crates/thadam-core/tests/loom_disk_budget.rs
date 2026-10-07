//! Exhaustive interleaving checks for `DiskBudget`.
//!
//! Run with `RUSTFLAGS="--cfg loom" cargo test -p thadam-core --release --test loom_disk_budget`.
#![cfg(loom)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use loom::sync::Arc;
use loom::thread;
use thadam_core::{BudgetCategory::Wal, DiskBudget};

#[test]
fn concurrent_reservations_never_exceed_the_limit() {
    loom::model(|| {
        let budget = Arc::new(DiskBudget::new().with_limit(Wal, 10));
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let b = Arc::clone(&budget);
                thread::spawn(move || b.try_reserve(Wal, 4).is_ok())
            })
            .collect();
        let granted = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert_eq!(granted, 2, "10 bytes fit exactly two 4-byte reservations");
        assert_eq!(budget.used(Wal), 8);
    });
}

#[test]
fn reserve_and_release_balance_out() {
    loom::model(|| {
        let budget = Arc::new(DiskBudget::new().with_limit(Wal, 6));
        budget.try_reserve(Wal, 3).unwrap();
        let releaser = {
            let b = Arc::clone(&budget);
            thread::spawn(move || b.release(Wal, 3))
        };
        let reserver = {
            let b = Arc::clone(&budget);
            thread::spawn(move || b.try_reserve(Wal, 4).is_ok())
        };
        releaser.join().unwrap();
        let reserved = reserver.join().unwrap();
        assert_eq!(budget.used(Wal), if reserved { 4 } else { 0 });
        assert_eq!(budget.accounting_errors(), 0);
        assert!(budget.used(Wal) <= 6);
    });
}

#[test]
fn concurrent_over_release_saturates_and_counts_once_per_call() {
    loom::model(|| {
        let budget = Arc::new(DiskBudget::new());
        budget.charge(Wal, 5);
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let b = Arc::clone(&budget);
                thread::spawn(move || b.release(Wal, 4))
            })
            .collect();
        handles.into_iter().for_each(|h| h.join().unwrap());
        assert_eq!(budget.used(Wal), 0);
        assert_eq!(
            budget.accounting_errors(),
            1,
            "exactly one release found less than it asked for"
        );
    });
}
