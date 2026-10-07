//! Interned-style string types shared across the model.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::Utf8Error;
use std::sync::Arc;

use bytes::Bytes;

/// A cheaply clonable immutable string for names that repeat across events
/// (field names, units, attribute keys, content types).
pub type Sym = Arc<str>;

/// A validated, immutable, cheaply clonable UTF-8 string value.
///
/// Validation happens once, at construction, so [`Utf8Bytes::as_str`] is free
/// and needs no `unsafe`. The backing store is a thin-pointer `Arc<str>`, which
/// keeps `Value` at 24 bytes (see the crate docs). Constructing from a
/// [`Bytes`] slice therefore copies; nothing pins the buffer it came from.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Utf8Bytes(Arc<str>);

impl Utf8Bytes {
    /// Validates `bytes` as UTF-8 and copies them.
    pub fn from_utf8(bytes: &Bytes) -> Result<Self, Utf8Error> {
        Self::from_utf8_slice(bytes)
    }

    /// Validates `bytes` as UTF-8 and copies them.
    pub fn from_utf8_slice(bytes: &[u8]) -> Result<Self, Utf8Error> {
        std::str::from_utf8(bytes).map(Self::from)
    }

    /// The string contents.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The contents as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the string is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Shares the contents as a [`Sym`] without copying.
    pub fn to_sym(&self) -> Sym {
        Arc::clone(&self.0)
    }
}

impl From<&str> for Utf8Bytes {
    fn from(s: &str) -> Self {
        Utf8Bytes(Arc::from(s))
    }
}

impl From<String> for Utf8Bytes {
    fn from(s: String) -> Self {
        Utf8Bytes(Arc::from(s))
    }
}

impl From<Sym> for Utf8Bytes {
    fn from(s: Sym) -> Self {
        Utf8Bytes(s)
    }
}

impl Deref for Utf8Bytes {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Utf8Bytes {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Utf8Bytes {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Utf8Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for Utf8Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_utf8() {
        assert!(Utf8Bytes::from_utf8_slice(&[0xff, 0xfe]).is_err());
        assert!(Utf8Bytes::from_utf8(&Bytes::from_static(b"\xc3\x28")).is_err());
    }

    #[test]
    fn accepts_valid_utf8_and_round_trips() {
        let s = "temperature \u{b0}C \u{1f321}";
        let u = Utf8Bytes::from_utf8(&Bytes::copy_from_slice(s.as_bytes())).unwrap();
        assert_eq!(u.as_str(), s);
        assert_eq!(u.as_bytes(), s.as_bytes());
        assert_eq!(u.len(), s.len());
        assert_eq!(&*u.to_sym(), s);
    }

    #[test]
    fn debug_and_display_show_the_string() {
        let u = Utf8Bytes::from("a\"b");
        assert_eq!(format!("{u}"), "a\"b");
        assert_eq!(format!("{u:?}"), "\"a\\\"b\"");
    }
}
