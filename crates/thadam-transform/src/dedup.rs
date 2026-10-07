//! A bounded time-and-size window of recently seen event ids.

use std::collections::{HashMap, VecDeque};

use thadam_core::{EventId, Timestamp};

/// Remembers ids for `window` of ingest time, at most `max_keys` at once.
///
/// Time is the WAL record's ingest time, never the wall clock, so replaying
/// the same frames gives the same answers. A duplicate does not extend the
/// window: it counts from the first sighting. An id is reported as a duplicate
/// only if exactly that id is still remembered, so distinct ids are never
/// dropped; past `max_keys` the oldest id is forgotten (and counted), which can
/// only let a duplicate through.
#[derive(Debug)]
pub(crate) struct DedupWindow {
    window_ns: i128,
    max_keys: usize,
    seen: HashMap<EventId, i64>,
    order: VecDeque<(EventId, i64)>,
    evicted: u64,
}

impl DedupWindow {
    pub(crate) fn new(window: std::time::Duration, max_keys: usize) -> Self {
        DedupWindow {
            window_ns: window.as_nanos() as i128,
            max_keys: max_keys.max(1),
            seen: HashMap::new(),
            order: VecDeque::new(),
            evicted: 0,
        }
    }

    /// Whether `id` was seen within the window before `now`; records it if not.
    pub(crate) fn is_duplicate(&mut self, id: EventId, now: Timestamp) -> bool {
        let now = now.as_nanos();
        self.expire(now);
        if let Some(&first) = self.seen.get(&id)
            && i128::from(now) - i128::from(first) <= self.window_ns
        {
            return true;
        }
        self.insert(id, now);
        false
    }

    /// Ids forgotten because the window was full.
    pub(crate) fn evicted(&self) -> u64 {
        self.evicted
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.seen.len()
    }

    fn expire(&mut self, now: i64) {
        while let Some(&(id, ts)) = self.order.front() {
            if i128::from(now) - i128::from(ts) <= self.window_ns {
                break;
            }
            self.order.pop_front();
            if self.seen.get(&id) == Some(&ts) {
                self.seen.remove(&id);
            }
        }
    }

    fn insert(&mut self, id: EventId, now: i64) {
        self.seen.insert(id, now);
        self.order.push_back((id, now));
        if self.order.len() > self.max_keys.saturating_mul(2).saturating_add(16) {
            // Out-of-order times can leave stale entries behind the front; the
            // sort keeps the rebuilt order independent of hash iteration order.
            let mut live: Vec<(EventId, i64)> = self.seen.iter().map(|(k, v)| (*k, *v)).collect();
            live.sort_unstable_by_key(|&(id, ts)| (ts, id));
            self.order = live.into();
        }
        while self.seen.len() > self.max_keys {
            let Some((old, ts)) = self.order.pop_front() else {
                break;
            };
            if self.seen.get(&old) == Some(&ts) {
                self.seen.remove(&old);
                self.evicted += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn id(n: u128) -> EventId {
        EventId::from_u128(n)
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_secs(secs).unwrap()
    }

    #[test]
    fn repeats_inside_the_window_are_duplicates() {
        let mut d = DedupWindow::new(Duration::from_secs(10), 100);
        assert!(!d.is_duplicate(id(1), t(0)));
        assert!(d.is_duplicate(id(1), t(5)));
        assert!(d.is_duplicate(id(1), t(10)));
        assert!(!d.is_duplicate(id(2), t(10)));
    }

    #[test]
    fn window_counts_from_the_first_sighting() {
        let mut d = DedupWindow::new(Duration::from_secs(10), 100);
        assert!(!d.is_duplicate(id(1), t(0)));
        assert!(d.is_duplicate(id(1), t(9)));
        assert!(!d.is_duplicate(id(1), t(11)), "expired, so it is new again");
        assert!(d.is_duplicate(id(1), t(12)));
    }

    #[test]
    fn size_cap_evicts_the_oldest_and_counts_it() {
        let mut d = DedupWindow::new(Duration::from_secs(1_000), 2);
        d.is_duplicate(id(1), t(0));
        d.is_duplicate(id(2), t(1));
        d.is_duplicate(id(3), t(2));
        assert_eq!(d.len(), 2);
        assert_eq!(d.evicted(), 1);
        assert!(!d.is_duplicate(id(1), t(3)), "forgotten, so it passes");
        assert!(d.is_duplicate(id(3), t(3)));
    }

    #[test]
    fn time_going_backwards_does_not_panic_or_drop_distinct_ids() {
        let mut d = DedupWindow::new(Duration::from_secs(5), 100);
        assert!(!d.is_duplicate(id(1), t(100)));
        assert!(!d.is_duplicate(id(2), t(50)));
        assert!(!d.is_duplicate(id(3), t(i64::MIN / 2_000_000_000)));
        assert!(d.is_duplicate(id(2), t(51)));
    }

    #[test]
    fn memory_stays_bounded_over_a_long_stream() {
        let mut d = DedupWindow::new(Duration::from_secs(60), 1_000);
        for i in 0..50_000u128 {
            d.is_duplicate(id(i), t(i64::try_from(i).unwrap()));
            assert!(d.len() <= 1_000);
        }
        assert!(d.order.len() <= 1_100);
    }
}
