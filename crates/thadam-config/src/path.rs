//! Document paths: a sequence of mapping keys and list indexes.

use std::fmt::Write as _;

/// One step of a [`PathVec`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Seg {
    /// A mapping key.
    Key(String),
    /// A list index.
    Idx(usize),
}

/// A path from the document root.
pub(crate) type PathVec = Vec<Seg>;

/// Builds child paths without mutating the parent.
pub(crate) trait PathExt {
    fn key(&self, key: &str) -> PathVec;
    fn idx(&self, index: usize) -> PathVec;
}

impl PathExt for [Seg] {
    fn key(&self, key: &str) -> PathVec {
        let mut p = self.to_vec();
        p.push(Seg::Key(key.to_owned()));
        p
    }

    fn idx(&self, index: usize) -> PathVec {
        let mut p = self.to_vec();
        p.push(Seg::Idx(index));
        p
    }
}

/// Renders a path as `datasets[0].fields[1].name`; the root is `$`.
pub(crate) fn display(path: &[Seg]) -> String {
    if path.is_empty() {
        return "$".to_owned();
    }
    let mut out = String::new();
    for seg in path {
        match seg {
            Seg::Key(k) => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(k);
            }
            Seg::Idx(i) => {
                let _ = write!(out, "[{i}]");
            }
        }
    }
    out
}

/// Renders a path as an RFC 6901 JSON Pointer.
pub(crate) fn pointer(path: &[Seg]) -> String {
    let mut out = String::new();
    for seg in path {
        out.push('/');
        match seg {
            Seg::Key(k) => out.push_str(&k.replace('~', "~0").replace('/', "~1")),
            Seg::Idx(i) => {
                let _ = write!(out, "{i}");
            }
        }
    }
    out
}

/// Converts a `serde_path_to_error` path.
pub(crate) fn from_serde(path: &serde_path_to_error::Path) -> PathVec {
    use serde_path_to_error::Segment;
    path.iter()
        .filter_map(|seg| match seg {
            Segment::Seq { index } => Some(Seg::Idx(*index)),
            Segment::Map { key } => Some(Seg::Key(key.clone())),
            Segment::Enum { variant } => Some(Seg::Key(variant.clone())),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_and_pointer() {
        let p = Vec::new().key("datasets").idx(1).key("a/b");
        assert_eq!(display(&p), "datasets[1].a/b");
        assert_eq!(pointer(&p), "/datasets/1/a~1b");
        assert_eq!(display(&[]), "$");
    }
}
