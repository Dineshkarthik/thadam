//! Bounded per-asset state shared by `on_change` and `sample`.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

use thadam_core::AssetPath;

/// State keyed by asset with a hard cap. At the cap the oldest asset's state
/// is dropped (first in, first out), which only ever makes a stateful step
/// keep one more event. The order depends on input order alone, so replay
/// gives the same result.
#[derive(Debug)]
pub(crate) struct AssetMap<S> {
    cap: usize,
    map: HashMap<AssetPath, S>,
    order: VecDeque<AssetPath>,
    evicted: u64,
}

impl<S> AssetMap<S> {
    pub(crate) fn new(cap: usize) -> Self {
        AssetMap {
            cap: cap.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
            evicted: 0,
        }
    }

    /// The state for `key`, creating it with `init` if absent. The flag is
    /// true when the state is new.
    pub(crate) fn get_or_insert_with(
        &mut self,
        key: &AssetPath,
        init: impl FnOnce() -> S,
    ) -> (&mut S, bool) {
        if !self.map.contains_key(key) && self.map.len() >= self.cap {
            while let Some(old) = self.order.pop_front() {
                if self.map.remove(&old).is_some() {
                    self.evicted += 1;
                    break;
                }
            }
        }
        match self.map.entry(key.clone()) {
            Entry::Occupied(e) => (e.into_mut(), false),
            Entry::Vacant(v) => {
                self.order.push_back(key.clone());
                (v.insert(init()), true)
            }
        }
    }

    pub(crate) fn evicted(&self) -> u64 {
        self.evicted
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(n: usize) -> AssetPath {
        AssetPath::new([("robot", format!("r{n}"))]).unwrap()
    }

    #[test]
    fn state_is_created_once_and_bounded() {
        let mut m = AssetMap::new(2);
        assert!(m.get_or_insert_with(&asset(1), || 10).1);
        assert!(!m.get_or_insert_with(&asset(1), || 99).1);
        assert!(m.get_or_insert_with(&asset(2), || 20).1);
        assert!(m.get_or_insert_with(&asset(3), || 30).1);
        assert_eq!(m.len(), 2);
        assert_eq!(m.evicted(), 1);
        assert!(
            m.get_or_insert_with(&asset(1), || 11).1,
            "oldest was evicted"
        );
    }
}
