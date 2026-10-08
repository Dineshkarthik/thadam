//! Crash failpoints in the upload sequence, armed from tests
//! (ARCHITECTURE.md PR16: `ship::after_put_before_mark`, `ship::mid_multipart`).
//!
//! Compiled in for this crate's own unit tests, and for other crates under
//! the `failpoints` feature.
#![cfg_attr(not(any(test, feature = "failpoints")), allow(dead_code))]

use std::sync::{Arc, Mutex};

use crate::error::ShipError;

/// A point in the upload sequence where an armed shipper crashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Failpoint {
    /// After the object exists in the store, before the `Uploaded` catalog
    /// record is appended: the recovery path that produces an
    /// AlreadyExists conflict on the next run.
    AfterPutBeforeMark,
    /// After the first multipart part upload, before `complete`: recovery
    /// must find no partial object and start over.
    MidMultipart,
}

impl Failpoint {
    /// Every failpoint.
    #[cfg(any(test, feature = "failpoints"))]
    pub const ALL: [Failpoint; 2] = [Failpoint::AfterPutBeforeMark, Failpoint::MidMultipart];

    /// The stable failpoint name.
    #[cfg(any(test, feature = "failpoints"))]
    pub const fn name(self) -> &'static str {
        match self {
            Failpoint::AfterPutBeforeMark => "ship::after_put_before_mark",
            Failpoint::MidMultipart => "ship::mid_multipart",
        }
    }

    /// Looks a failpoint up by name.
    #[cfg(any(test, feature = "failpoints"))]
    pub fn from_name(name: &str) -> Option<Failpoint> {
        Self::ALL.into_iter().find(|f| f.name() == name)
    }
}

/// The arming registry shared by a test fixture and the shippers it builds.
#[derive(Clone, Debug, Default)]
pub struct Failpoints {
    armed: Arc<Mutex<Option<Failpoint>>>,
}

impl Failpoints {
    /// Arms a failpoint: it fires once, at the next upload that reaches it.
    pub fn arm(&self, failpoint: Failpoint) {
        *self.armed.lock().unwrap_or_else(|e| e.into_inner()) = Some(failpoint);
    }

    /// Disarms any failpoint that did not fire.
    pub fn disarm(&self) {
        *self.armed.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    /// Fires the armed failpoint when it is `failpoint`, consuming the arm.
    pub(crate) fn hit(&self, failpoint: Failpoint) -> Result<(), ShipError> {
        let mut armed = self.armed.lock().unwrap_or_else(|e| e.into_inner());
        if *armed == Some(failpoint) {
            *armed = None;
            return Err(ShipError::Crashed(failpoint_stable_name(failpoint)));
        }
        Ok(())
    }
}

fn failpoint_stable_name(failpoint: Failpoint) -> &'static str {
    match failpoint {
        Failpoint::AfterPutBeforeMark => "ship::after_put_before_mark",
        Failpoint::MidMultipart => "ship::mid_multipart",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arms_fire_once_and_names_round_trip() {
        for fp in Failpoint::ALL {
            assert_eq!(Failpoint::from_name(fp.name()), Some(fp));
        }
        assert_eq!(Failpoint::from_name("ship::nope"), None);

        let fps = Failpoints::default();
        fps.arm(Failpoint::MidMultipart);
        assert!(fps.hit(Failpoint::AfterPutBeforeMark).is_ok());
        assert!(fps.hit(Failpoint::MidMultipart).is_err());
        assert!(fps.hit(Failpoint::MidMultipart).is_ok(), "fired once");
        fps.disarm();
    }
}
