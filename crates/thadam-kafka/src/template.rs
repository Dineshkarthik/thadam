//! The per-event `topic` and `key` templates.
//!
//! A topic template such as `telemetry.{dataset}.{robot}` follows the NATS
//! sink's whole-token rule: every `{name}` placeholder spans a whole
//! dot-separated token (`pre-{robot}` is refused at parse time). `{dataset}`
//! renders the dataset id; any other name renders the asset identity level
//! of that name. Rendering happens per event, so an event whose identity
//! lacks a level the template needs (or whose values are not usable topic
//! characters) is rejected individually.
//!
//! A key template interpolates the same placeholders anywhere in the string
//! (keys are opaque bytes, so there is no token rule) and additionally
//! accepts `{event_id}` for the event id (hex) — the default key when `key`
//! is not configured.

use thadam_core::{AssetPath, ErrorDomain, EventId, ThadamError, codes};

/// The largest topic name Kafka accepts.
const MAX_TOPIC_LEN: usize = 249;

/// Why a template string could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TemplateParseError(pub String);

/// Why one event could not be rendered.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RenderError {
    /// The event's identity has no value for a level the template names.
    #[error("identity has no level `{level}` named by the template")]
    MissingLevel {
        /// The missing level.
        level: String,
    },
    /// A level value cannot appear in a Kafka topic name.
    #[error("identity level `{level}` has a value that is unusable in a topic: {reason}")]
    UnusableValue {
        /// The level.
        level: String,
        /// What is wrong with the value.
        reason: String,
    },
    /// The rendered topic exceeds Kafka's maximum name length.
    #[error("rendered topic is {len} bytes; Kafka allows at most {MAX_TOPIC_LEN}")]
    TooLong {
        /// The rendered length.
        len: usize,
    },
}

impl RenderError {
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

/// Splits `text` into literals and `{var}` placeholders; braces cannot nest
/// or dangle. Placeholder names get the topic-rules check (they name
/// identity levels, which follow the same character discipline).
fn parse_parts(what: &str, text: &str) -> Result<Vec<Part>, TemplateParseError> {
    let err = |m: &str| TemplateParseError(format!("{m} (in `{text}`)"));
    if text.is_empty() {
        return Err(err(&format!("the {what} template must not be empty")));
    }
    let mut parts: Vec<Part> = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('{') {
        let (lit, after) = rest.split_at(open);
        if !lit.is_empty() {
            parts.push(Part::Literal(lit.to_owned()));
        }
        let Some(close) = after.find('}') else {
            return Err(err("unclosed `{`"));
        };
        let name = &after[1..close];
        if name.is_empty() {
            return Err(err("empty `{}` placeholder"));
        }
        if let Some(why) = kafka_name_problem(name) {
            return Err(err(&format!("placeholder name `{name}`: {why}")));
        }
        parts.push(Part::Var(name.to_owned()));
        rest = &after[close + 1..];
    }
    if !rest.is_empty() {
        if rest.contains('}') {
            return Err(err("stray `}` outside a placeholder"));
        }
        parts.push(Part::Literal(rest.to_owned()));
    }
    Ok(parts)
}

fn variables_of(parts: &[Part]) -> Vec<&str> {
    parts
        .iter()
        .filter_map(|p| match p {
            Part::Var(name) => Some(name.as_str()),
            Part::Literal(_) => None,
        })
        .collect()
}

fn render_parts(
    parts: &[Part],
    dataset: &str,
    event_id: Option<&EventId>,
    asset: &AssetPath,
    value_ok: bool,
) -> Result<String, RenderError> {
    let mut out = String::new();
    for part in parts {
        match part {
            Part::Literal(l) => out.push_str(l),
            Part::Var(name) => {
                let resolved = match name.as_str() {
                    "dataset" => Some(dataset.to_owned()),
                    "event_id" => event_id.map(|id| id.to_string()),
                    _ => asset.get(name).map(str::to_owned),
                };
                let Some(value) = resolved else {
                    return Err(RenderError::MissingLevel {
                        level: name.clone(),
                    });
                };
                if value_ok && let Some(why) = kafka_value_problem(&value) {
                    return Err(RenderError::UnusableValue {
                        level: name.clone(),
                        reason: why.to_owned(),
                    });
                }
                out.push_str(&value);
            }
        }
    }
    Ok(out)
}

/// Kafka topic names may only contain ASCII alphanumerics, `.`, `-`, `_`.
/// Applies to literal tokens and placeholder names.
fn kafka_name_problem(token: &str) -> Option<&'static str> {
    if token.is_empty() {
        return Some("must not be empty");
    }
    if !token
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Some("must match [a-zA-Z0-9._-] (Kafka topic characters)");
    }
    None
}

/// A placeholder value inside a topic: the same character set, but a `.` in
/// a value would smuggle in extra tokens (the template's token structure is
/// the routing contract), so values get the token-strict check.
fn kafka_value_problem(value: &str) -> Option<&'static str> {
    if value.is_empty() {
        return Some("must not be empty");
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Some("must match [a-zA-Z0-9-_] (no dots inside a value)");
    }
    None
}

/// A parsed `topic` template with whole-token placeholders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicTemplate {
    parts: Vec<Part>,
}

impl TopicTemplate {
    /// Parses and validates `text`.
    pub fn parse(text: &str) -> Result<Self, TemplateParseError> {
        let err = |m: &str| TemplateParseError(format!("{m} (in `{text}`)"));
        let parts = parse_parts("topic", text)?;

        // Literal structure: only Kafka topic characters, no empty tokens.
        let mut rendered = String::new();
        for part in &parts {
            match part {
                Part::Literal(l) => rendered.push_str(l),
                Part::Var(_) => rendered.push('x'),
            }
        }
        for token in rendered.split('.') {
            if token.is_empty() {
                return Err(err("topic tokens must not be empty"));
            }
            if let Some(why) = kafka_name_problem(token) {
                return Err(err(why));
            }
        }
        if text.len() > MAX_TOPIC_LEN {
            return Err(err(&format!(
                "topic templates must be at most {MAX_TOPIC_LEN} bytes before rendering"
            )));
        }

        // Every placeholder must span a whole token (start of template or
        // after a dot, end of template or before a dot), like the NATS sink.
        for (i, part) in parts.iter().enumerate() {
            if !matches!(part, Part::Var(_)) {
                continue;
            }
            let left_ok = i == 0
                || matches!(parts.get(i.wrapping_sub(1)), Some(Part::Literal(l)) if l.ends_with('.'));
            let right_ok = i + 1 == parts.len()
                || matches!(parts.get(i + 1), Some(Part::Literal(l)) if l.starts_with('.'));
            if !left_ok || !right_ok {
                return Err(err(
                    "a placeholder must be a whole topic token between dots",
                ));
            }
        }
        Ok(TopicTemplate { parts })
    }

    /// The placeholder names the template uses, in occurrence order.
    pub fn variables(&self) -> Vec<&str> {
        variables_of(&self.parts)
    }

    /// Renders the topic for one event.
    pub fn render(&self, dataset: &str, asset: &AssetPath) -> Result<String, RenderError> {
        let out = render_parts(&self.parts, dataset, None, asset, true)?;
        if out == "." || out == ".." {
            return Err(RenderError::UnusableValue {
                level: String::new(),
                reason: "`.` and `..` are not legal topic names".to_owned(),
            });
        }
        if out.len() > MAX_TOPIC_LEN {
            return Err(RenderError::TooLong { len: out.len() });
        }
        Ok(out)
    }
}

/// A parsed `key` template. Keys are opaque bytes: placeholders may appear
/// anywhere and accept `{dataset}`, `{event_id}`, and identity levels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyTemplate {
    parts: Vec<Part>,
}

impl KeyTemplate {
    /// Parses and validates `text`.
    pub fn parse(text: &str) -> Result<Self, TemplateParseError> {
        Ok(KeyTemplate {
            parts: parse_parts("key", text)?,
        })
    }

    /// The placeholder names the template uses, in occurrence order.
    pub fn variables(&self) -> Vec<&str> {
        variables_of(&self.parts)
    }

    /// Renders the key bytes for one event.
    pub fn render(
        &self,
        dataset: &str,
        id: &EventId,
        asset: &AssetPath,
    ) -> Result<Vec<u8>, RenderError> {
        Ok(render_parts(&self.parts, dataset, Some(id), asset, false)?.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(text: &str) -> TopicTemplate {
        TopicTemplate::parse(text).expect("parse")
    }

    #[test]
    fn topic_parses_literals_and_placeholders() {
        let t = topic("telemetry.{dataset}.{robot}");
        assert_eq!(t.variables(), ["dataset", "robot"]);
        assert_eq!(topic("plain-name_1.2").variables().len(), 0);
    }

    #[test]
    fn topic_parse_rejects_bad_templates() {
        for (text, want) in [
            ("telemetry.{dataset", "unclosed"),
            ("telemetry.}", "stray"),
            ("telemetry.{}", "empty"),
            ("pre-{robot}", "whole topic token"),
            ("{robot}-x", "whole topic token"),
            ("telemetry..b", "must not be empty"),
            (".telemetry.b", "must not be empty"),
            ("telemetry.x.", "must not be empty"),
            ("bad space.x", "topic characters"),
            ("slash/name.x", "topic characters"),
            ("", "must not be empty"),
            (
                &format!("{}.{}", "a".repeat(250), "{dataset}"),
                "at most 249",
            ),
        ] {
            let err = TopicTemplate::parse(text).expect_err(text);
            assert!(err.0.contains(want), "`{text}`: missing `{want}` in {err}");
        }
    }

    #[test]
    fn topic_render_checks_values_and_length() {
        let t = topic("telemetry.{dataset}.{robot}");
        let asset = AssetPath::new([("robot", "r7")]).expect("path");
        assert_eq!(t.render("imu", &asset).expect("render"), "telemetry.imu.r7");

        let err = t.render("imu", &AssetPath::empty()).expect_err("no robot");
        assert_eq!(
            err,
            RenderError::MissingLevel {
                level: "robot".into()
            }
        );

        let dotted = AssetPath::new([("robot", "r.7")]).expect("path");
        let err = t.render("imu", &dotted).expect_err("dotted value");
        assert!(matches!(err, RenderError::UnusableValue { .. }));

        let long = AssetPath::new([("robot", "b".repeat(20))]).expect("path");
        // Template 236 + 5 + 7 = 248 bytes (legal); rendered 236 + 5 + 20
        // = 261 bytes — over Kafka's 249.
        let t = topic(&format!("{}padx.{{robot}}", "a".repeat(236)));
        let err = t.render("imu", &long).expect_err("over 249 rendered");
        assert_eq!(err, RenderError::TooLong { len: 261 });

        let err = err.into_reject_error();
        assert_eq!(err.code, codes::SINK_REJECTED);
        assert!(!err.is_transient());
    }

    #[test]
    fn key_template_interpolates_anywhere_and_event_id() {
        let t = KeyTemplate::parse("k-{dataset}-{robot}-{event_id}").expect("parse");
        assert_eq!(t.variables(), ["dataset", "robot", "event_id"]);
        let asset = AssetPath::new([("robot", "r.7")]).expect("path");
        let id = EventId::from_u128(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        let key = t.render("imu", &id, &asset).expect("render");
        assert_eq!(
            String::from_utf8(key).expect("utf8"),
            "k-imu-r.7-0123456789abcdef0011223344556677",
            "dots are fine in key values"
        );

        let err = t
            .render("imu", &id, &AssetPath::empty())
            .expect_err("no robot");
        assert!(matches!(err, RenderError::MissingLevel { .. }));
    }

    #[test]
    fn event_id_in_a_topic_acts_like_an_unknown_level() {
        // Like the NATS sink, the topic template does not special-case
        // `event_id`; the factory refuses it at build because it is not a
        // declared identity level.
        let t = topic("telemetry.{event_id}");
        let err = t
            .render("imu", &AssetPath::empty())
            .expect_err("no such level");
        assert!(matches!(err, RenderError::MissingLevel { .. }));
        assert!(matches!(
            err.into_reject_error().retry,
            thadam_core::Retry::Permanent
        ));
    }
}
