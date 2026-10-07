//! `Secret<T>`: a value that never appears in `Debug`, `Display`, or serialized output.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The text shown in place of a secret.
pub const REDACTED: &str = "<redacted>";

/// A secret value, such as a credential read from `${env:}` or `${file:}`.
///
/// `Debug`, `Display`, and `Serialize` all print [`REDACTED`]. The only way to
/// read the value is [`Secret::expose`], so each use is greppable.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wraps a value.
    pub fn new(value: T) -> Self {
        Secret(value)
    }

    /// Reveals the value.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Unwraps the value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for Secret<T> {
    fn from(value: T) -> Self {
        Secret(value)
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Secret({REDACTED})")
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> Serialize for Secret<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(REDACTED)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Secret<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Secret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_prints_the_value() {
        let s = Secret::new("hunter2".to_owned());
        assert_eq!(format!("{s:?}"), "Secret(<redacted>)");
        assert_eq!(format!("{s:#?}"), "Secret(<redacted>)");
        assert_eq!(s.to_string(), "<redacted>");
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"<redacted>\"");
        assert_eq!(s.expose(), "hunter2");
    }

    #[test]
    fn deserializes_transparently() {
        let s: Secret<String> = serde_json::from_str("\"abc\"").unwrap();
        assert_eq!(s.into_inner(), "abc");
    }
}
