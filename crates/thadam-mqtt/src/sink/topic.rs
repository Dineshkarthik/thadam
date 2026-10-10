//! The publish topic template, such as `thadam/{dataset}/{robot}`.
//!
//! `{dataset}` renders the dataset id; any other placeholder renders the
//! asset identity level of that name. Literal text is checked when the
//! template is parsed: it may not hold the wildcards `+` and `#`, NUL, empty
//! levels, or a leading `$` (broker-reserved). Each rendered value is checked
//! per event, because only then are the identity values known: a value must
//! be non-empty and must not contain `/`, `+`, `#`, or NUL. An event that
//! lacks a level the template names cannot be routed and is rejected alone.

use thadam_core::{AssetPath, ErrorDomain, ThadamError, codes};

/// The largest topic name an MQTT packet can carry.
pub const MAX_TOPIC_BYTES: usize = 65_535;

/// Why a topic template string could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TopicTemplateError(pub String);

/// Why one event's topic could not be rendered.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TopicError {
    /// The event's identity has no value for a level the template names.
    #[error("identity has no level `{level}` named by the topic template")]
    MissingLevel {
        /// The missing level.
        level: String,
    },
    /// A level value cannot appear in a topic level.
    #[error("identity level `{level}` has a value that is unusable in a topic: {reason}")]
    UnusableValue {
        /// The level.
        level: String,
        /// What is wrong with the value.
        reason: String,
    },
    /// The rendered topic as a whole is not a valid topic name.
    #[error("the rendered topic is unusable: {reason}")]
    UnusableTopic {
        /// What is wrong with the topic.
        reason: String,
    },
}

impl TopicError {
    /// The permanent per-event reject this failure becomes.
    pub fn into_reject_error(self) -> ThadamError {
        ThadamError::permanent(ErrorDomain::Sink, codes::SINK_REJECTED, self.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Part {
    Literal(String),
    Var(String),
}

/// A parsed, validated topic template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicTemplate {
    parts: Vec<Part>,
}

fn valid_placeholder_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn value_problem(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        Some("the value is empty")
    } else if value.contains(['/', '+', '#']) {
        Some("values must not contain `/`, `+` or `#`")
    } else if value.contains('\0') {
        Some("values must not contain NUL")
    } else {
        None
    }
}

impl TopicTemplate {
    /// Parses and validates `text`.
    pub fn parse(text: &str) -> Result<Self, TopicTemplateError> {
        let err = |m: &str| TopicTemplateError(format!("{m} (in `{text}`)"));
        if text.is_empty() {
            return Err(err("the topic must not be empty"));
        }

        let mut parts: Vec<Part> = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find('{') {
            let (lit, after) = rest.split_at(open);
            if lit.contains('}') {
                return Err(err("stray `}` outside a placeholder"));
            }
            if !lit.is_empty() {
                parts.push(Part::Literal(lit.to_owned()));
            }
            let Some(close) = after.find('}') else {
                return Err(err("unclosed `{`"));
            };
            let name = &after[1..close];
            if name.contains('{') {
                return Err(err("placeholders cannot nest"));
            }
            if name.is_empty() {
                return Err(err("empty `{}` placeholder"));
            }
            if !valid_placeholder_name(name) {
                return Err(err(&format!(
                    "placeholder `{{{name}}}` may only use letters, digits, `_` and `-`"
                )));
            }
            parts.push(Part::Var(name.to_owned()));
            rest = &after[close + 1..];
        }
        if rest.contains('}') {
            return Err(err("stray `}` outside a placeholder"));
        }
        if !rest.is_empty() {
            parts.push(Part::Literal(rest.to_owned()));
        }

        for part in &parts {
            if let Part::Literal(l) = part {
                if l.contains(['+', '#']) {
                    return Err(err(
                        "wildcards `+` and `#` are not allowed in a publish topic",
                    ));
                }
                if l.contains('\0') {
                    return Err(err("the topic must not contain NUL"));
                }
            }
        }

        // Placeholders render as non-empty text here, so only literal emptiness
        // is caught; per-value emptiness is checked at render time.
        let skeleton: String = parts
            .iter()
            .map(|p| match p {
                Part::Literal(l) => l.as_str(),
                Part::Var(_) => "x",
            })
            .collect();
        if skeleton.split('/').any(str::is_empty) {
            return Err(err("topic levels must not be empty"));
        }
        if skeleton.starts_with('$') {
            return Err(err("topics starting with `$` are reserved for the broker"));
        }
        Ok(TopicTemplate { parts })
    }

    /// The placeholder names the template uses, in occurrence order.
    pub fn variables(&self) -> Vec<&str> {
        self.parts
            .iter()
            .filter_map(|p| match p {
                Part::Var(name) => Some(name.as_str()),
                Part::Literal(_) => None,
            })
            .collect()
    }

    /// Renders the topic for one event. `{dataset}` takes the dataset id;
    /// other placeholders read the event's identity levels.
    pub fn render(&self, dataset: &str, asset: &AssetPath) -> Result<String, TopicError> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                Part::Literal(l) => out.push_str(l),
                Part::Var(name) => {
                    let value = if name == "dataset" {
                        Some(dataset)
                    } else {
                        asset.get(name)
                    };
                    let Some(value) = value else {
                        return Err(TopicError::MissingLevel {
                            level: name.clone(),
                        });
                    };
                    if let Some(reason) = value_problem(value) {
                        return Err(TopicError::UnusableValue {
                            level: name.clone(),
                            reason: reason.to_owned(),
                        });
                    }
                    out.push_str(value);
                }
            }
        }
        if out.starts_with('$') {
            return Err(TopicError::UnusableTopic {
                reason: "topics starting with `$` are reserved for the broker".into(),
            });
        }
        if out.len() > MAX_TOPIC_BYTES {
            return Err(TopicError::UnusableTopic {
                reason: format!(
                    "it is {} bytes, the maximum is {MAX_TOPIC_BYTES}",
                    out.len()
                ),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(text: &str) -> TopicTemplate {
        TopicTemplate::parse(text).expect("parse")
    }

    fn robot(id: &str) -> AssetPath {
        AssetPath::new([("robot", id)]).expect("path")
    }

    #[test]
    fn parses_literals_and_placeholders() {
        let t = template("thadam/{dataset}/{robot}");
        assert_eq!(t.variables(), ["dataset", "robot"]);
        assert_eq!(template("plant/line-1/status").variables().len(), 0);
        assert_eq!(template("thadam/{dataset}-{robot}").variables().len(), 2);
    }

    #[test]
    fn parse_rejects_bad_templates() {
        for (text, want) in [
            ("", "must not be empty"),
            ("thadam/{robot", "unclosed"),
            ("thadam/robot}", "stray"),
            ("thadam/{}", "empty"),
            ("thadam/{a{b}}", "nest"),
            ("thadam/{ro bot}", "may only use"),
            ("thadam/{a.b}", "may only use"),
            ("thadam/+/x", "wildcards"),
            ("thadam/#", "wildcards"),
            ("thadam/{dataset}/#", "wildcards"),
            ("a//b", "must not be empty"),
            ("/a", "must not be empty"),
            ("a/", "must not be empty"),
            ("$SYS/x", "reserved"),
            ("a\0b", "NUL"),
        ] {
            let err = TopicTemplate::parse(text).expect_err(text);
            assert!(err.0.contains(want), "`{text}`: missing `{want}` in {err}");
        }
    }

    #[test]
    fn renders_dataset_and_identity_levels() {
        let t = template("thadam/{dataset}/{robot}");
        assert_eq!(
            t.render("imu", &robot("r7")).expect("render"),
            "thadam/imu/r7"
        );
        assert_eq!(
            template("thadam/{dataset}-{robot}")
                .render("imu", &robot("r7"))
                .expect("render"),
            "thadam/imu-r7"
        );
    }

    #[test]
    fn missing_level_is_a_per_event_error() {
        let err = template("thadam/{dataset}/{robot}")
            .render("imu", &AssetPath::empty())
            .expect_err("no robot");
        assert_eq!(
            err,
            TopicError::MissingLevel {
                level: "robot".into()
            }
        );
    }

    #[test]
    fn illegal_characters_in_values_are_rejected() {
        let t = template("thadam/{dataset}/{robot}");
        for bad in ["r/7", "r+7", "r#7", "r\u{0}7", ""] {
            let err = t.render("imu", &robot(bad)).expect_err(bad);
            assert!(
                matches!(err, TopicError::UnusableValue { ref level, .. } if level == "robot"),
                "{bad:?}: {err:?}"
            );
        }
        let err = t.render("im+u", &robot("r7")).expect_err("dataset");
        assert!(matches!(err, TopicError::UnusableValue { ref level, .. } if level == "dataset"));
    }

    #[test]
    fn rendered_topics_must_be_valid_topic_names() {
        let t = template("{robot}/x");
        let err = t.render("imu", &robot("$SYS")).expect_err("reserved");
        assert!(matches!(err, TopicError::UnusableTopic { .. }));
        let at_limit = "a".repeat(MAX_TOPIC_BYTES);
        assert!(template("{robot}").render("imu", &robot(&at_limit)).is_ok());
        let over = "a".repeat(MAX_TOPIC_BYTES + 1);
        let err = template("{robot}")
            .render("imu", &robot(&over))
            .expect_err("long");
        assert!(matches!(err, TopicError::UnusableTopic { .. }));
    }

    #[test]
    fn render_errors_become_permanent_sink_rejects() {
        let e = TopicError::MissingLevel {
            level: "robot".into(),
        }
        .into_reject_error();
        assert_eq!(e.code, codes::SINK_REJECTED);
        assert!(!e.is_transient());
    }
}
