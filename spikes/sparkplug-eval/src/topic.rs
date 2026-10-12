//! The Sparkplug topic namespace: `spBv1.0/<group>/<kind>/<edge>[/<device>]`.
//! STATE topics put the host application id in the edge position; a passive
//! consumer only observes them (Sparkplug spec 3.0.0, chapter 5).

/// The message kind of a topic (third token).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    NBirth,
    NDeath,
    NData,
    NCmd,
    DBirth,
    DDeath,
    DData,
    DCmd,
    State,
}

impl Kind {
    fn from_token(token: &str) -> Option<Kind> {
        Some(match token {
            "NBIRTH" => Kind::NBirth,
            "NDEATH" => Kind::NDeath,
            "NDATA" => Kind::NData,
            "NCMD" => Kind::NCmd,
            "DBIRTH" => Kind::DBirth,
            "DDEATH" => Kind::DDeath,
            "DDATA" => Kind::DData,
            "DCMD" => Kind::DCmd,
            "STATE" => Kind::State,
            _ => return None,
        })
    }

    /// The spelling found on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::NBirth => "NBIRTH",
            Kind::NDeath => "NDEATH",
            Kind::NData => "NDATA",
            Kind::NCmd => "NCMD",
            Kind::DBirth => "DBIRTH",
            Kind::DDeath => "DDEATH",
            Kind::DData => "DDATA",
            Kind::DCmd => "DCMD",
            Kind::State => "STATE",
        }
    }

    /// Device-scoped kinds must carry a device id segment (spec 5.4):
    /// device topics have 5 segments, node/host topics 4.
    pub fn device_level(self) -> bool {
        matches!(self, Kind::DBirth | Kind::DDeath | Kind::DData | Kind::DCmd)
    }

    /// Kinds that move a session's `seq` stream forward (spec 6.4.5).
    pub fn carries_seq(self) -> bool {
        !matches!(self, Kind::NCmd | Kind::DCmd | Kind::State)
    }
}

/// A parsed topic in the `spBv1.0` namespace.
#[derive(Clone, Copy, Debug)]
pub struct Topic<'a> {
    pub group: &'a str,
    pub kind: Kind,
    /// The edge node id, or the host application id on a STATE topic.
    pub node: &'a str,
    pub device: Option<&'a str>,
    /// `false` when the segment counts disagree with the kind's spec shape;
    /// the message still decodes, with a note attached by the decoder.
    pub well_formed: bool,
}

/// Parses a topic, or `None` when it is not in the Sparkplug namespace.
pub fn parse(topic: &str) -> Option<Topic<'_>> {
    let mut segments = topic.split('/');
    if segments.next()? != "spBv1.0" {
        return None;
    }
    let (group, kind, node) = (
        segments.next()?,
        Kind::from_token(segments.next()?)?,
        segments.next()?,
    );
    let device = segments.next().filter(|d| !d.is_empty());
    if segments.next().is_some() || group.is_empty() || node.is_empty() {
        return None;
    }
    let well_formed = kind.device_level() == device.is_some();
    Some(Topic {
        group,
        kind,
        node,
        device,
        well_formed,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn parses_all_shapes() {
        let t = parse("spBv1.0/plantA/NBIRTH/edge1").unwrap();
        assert_eq!(
            (t.group, t.kind, t.node, t.device, t.well_formed),
            ("plantA", Kind::NBirth, "edge1", None, true)
        );

        let t = parse("spBv1.0/g/DDATA/edge1/dev1").unwrap();
        assert_eq!(
            (t.kind, t.device, t.well_formed),
            (Kind::DData, Some("dev1"), true)
        );

        let t = parse("spBv1.0/g/STATE/host-1").unwrap();
        assert_eq!(
            (t.kind, t.node, t.well_formed),
            (Kind::State, "host-1", true)
        );

        let t = parse("spBv1.0/g/NDATA/edge1/devX").unwrap();
        assert!(!t.well_formed);

        for bad in [
            "spBv1.0/g/NBIRTH",
            "spBv1.0/g/NOPE/edge1",
            "spBv2.0/g/NBIRTH/edge1",
            "robotics/r7/telemetry",
            "spBv1.0/g/DBIRTH/edge1",
            "spBv1.0//NBIRTH/edge1",
            "spBv1.0/g/NBIRTH/edge1/d/x",
        ] {
            match parse(bad) {
                None => {}
                Some(t) => assert!(!t.well_formed, "{bad}"),
            }
        }
    }
}
