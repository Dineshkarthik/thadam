//! Component identifiers, asset identity, and message attributes.

use std::fmt;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::text::{Sym, Utf8Bytes};

macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
        pub struct $name(Sym);

        impl $name {
            /// Wraps a configured identifier.
            pub fn new(id: impl Into<Sym>) -> Self {
                Self(id.into())
            }

            /// The identifier text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(id: &str) -> Self {
                Self::new(id)
            }
        }
    };
}

id_newtype!(
    /// The configured identifier of a source.
    SourceId
);
id_newtype!(
    /// The configured identifier of a sink.
    SinkId
);

/// One level of an [`AssetPath`]: a level name and its value.
pub type AssetLevel = (Sym, Utf8Bytes);

/// Most levels an asset path may have; paths come from topic templates and
/// payload fields, which are untrusted.
pub const MAX_ASSET_LEVELS: usize = 32;

/// Why an [`AssetPath`] could not be built.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AssetPathError {
    /// A level name was empty.
    #[error("asset level names must not be empty")]
    EmptyLevelName,
    /// Two levels shared a name, which would collide as columns.
    #[error("duplicate asset level `{0}`")]
    DuplicateLevel(String),
    /// More than [`MAX_ASSET_LEVELS`] levels.
    #[error("asset path has {0} levels, the maximum is {MAX_ASSET_LEVELS}")]
    TooManyLevels(usize),
}

/// An ordered hierarchy of (level, value) pairs identifying where data came
/// from. `device_id` is just a level and no level is mandatory.
///
/// Paths are immutable and shared: cloning bumps one reference count, so all
/// events of a device can share one path.
#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct AssetPath(Option<Arc<Vec<AssetLevel>>>);

impl AssetPath {
    /// The path with no levels.
    pub fn empty() -> Self {
        AssetPath(None)
    }

    /// Builds a path from levels, checking names and the level cap.
    pub fn new<I, K, V>(levels: I) -> Result<Self, AssetPathError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Sym>,
        V: Into<Utf8Bytes>,
    {
        let levels: Vec<AssetLevel> = levels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        if levels.len() > MAX_ASSET_LEVELS {
            return Err(AssetPathError::TooManyLevels(levels.len()));
        }
        for (i, (name, _)) in levels.iter().enumerate() {
            if name.is_empty() {
                return Err(AssetPathError::EmptyLevelName);
            }
            if levels[..i].iter().any(|(earlier, _)| earlier == name) {
                return Err(AssetPathError::DuplicateLevel(name.to_string()));
            }
        }
        Ok(if levels.is_empty() {
            AssetPath(None)
        } else {
            AssetPath(Some(Arc::new(levels)))
        })
    }

    /// The levels in order.
    pub fn levels(&self) -> &[AssetLevel] {
        self.0.as_deref().map_or(&[], Vec::as_slice)
    }

    /// Iterates (level, value) pairs as strings.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.levels().iter().map(|(k, v)| (&**k, v.as_str()))
    }

    /// The value of the named level.
    pub fn get(&self, level: &str) -> Option<&str> {
        self.levels()
            .iter()
            .find(|(k, _)| &**k == level)
            .map(|(_, v)| v.as_str())
    }

    /// Number of levels.
    pub fn len(&self) -> usize {
        self.levels().len()
    }

    /// Whether the path has no levels.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

/// Protocol metadata carried with a raw message (for MQTT: topic, QoS, retain,
/// user properties), as ordered string pairs with unique keys.
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct Attributes(SmallVec<[(Sym, Utf8Bytes); 4]>);

impl Attributes {
    /// An empty attribute set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets `key`, returning the previous value if it was already present.
    pub fn insert(
        &mut self,
        key: impl Into<Sym>,
        value: impl Into<Utf8Bytes>,
    ) -> Option<Utf8Bytes> {
        let key = key.into();
        let value = value.into();
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some((_, slot)) => Some(std::mem::replace(slot, value)),
            None => {
                self.0.push((key, value));
                None
            }
        }
    }

    /// The value for `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Iterates pairs in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (&**k, v.as_str()))
    }

    /// Number of attributes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no attributes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<K: Into<Sym>, V: Into<Utf8Bytes>> FromIterator<(K, V)> for Attributes {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut attrs = Attributes::new();
        for (k, v) in iter {
            attrs.insert(k, v);
        }
        attrs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_path_keeps_order_and_looks_up_levels() {
        let p = AssetPath::new([("site", "berlin"), ("robot", "r7")]).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(p.get("robot"), Some("r7"));
        assert_eq!(p.get("missing"), None);
        assert_eq!(
            p.iter().collect::<Vec<_>>(),
            vec![("site", "berlin"), ("robot", "r7")]
        );
    }

    #[test]
    fn asset_path_rejects_bad_levels() {
        assert_eq!(
            AssetPath::new([("", "x")]),
            Err(AssetPathError::EmptyLevelName)
        );
        assert_eq!(
            AssetPath::new([("a", "1"), ("a", "2")]),
            Err(AssetPathError::DuplicateLevel("a".into()))
        );
        let many: Vec<(String, &str)> = (0..=MAX_ASSET_LEVELS)
            .map(|i| (format!("l{i}"), "v"))
            .collect();
        assert_eq!(
            AssetPath::new(many),
            Err(AssetPathError::TooManyLevels(MAX_ASSET_LEVELS + 1))
        );
    }

    #[test]
    fn empty_path_has_no_allocation_and_equals_default() {
        let none: [(&str, &str); 0] = [];
        assert_eq!(AssetPath::new(none).unwrap(), AssetPath::default());
        assert!(AssetPath::empty().is_empty());
        assert_eq!(AssetPath::empty().levels().len(), 0);
    }

    #[test]
    fn attributes_replace_existing_keys() {
        let mut a = Attributes::new();
        assert_eq!(a.insert("topic", "a/b"), None);
        assert_eq!(a.insert("qos", "1"), None);
        assert_eq!(
            a.insert("topic", "c/d").map(|u| u.to_string()),
            Some("a/b".into())
        );
        assert_eq!(a.len(), 2);
        assert_eq!(a.get("topic"), Some("c/d"));
        let b: Attributes = [("topic", "c/d"), ("qos", "1")].into_iter().collect();
        assert_eq!(a.iter().collect::<Vec<_>>(), b.iter().collect::<Vec<_>>());
    }

    #[test]
    fn ids_display_their_text() {
        assert_eq!(SourceId::new("robot-mqtt").to_string(), "robot-mqtt");
        assert_eq!(SinkId::from("local").as_str(), "local");
    }
}
