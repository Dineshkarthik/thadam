//! Small path-like primitives shared by the transform pipeline: MQTT topic
//! filters, identity templates and JSON Pointer field paths.
//!
//! Patterns, templates and pointers come from trusted configuration; topics and
//! payloads are untrusted. Errors therefore only ever mention the text being
//! compiled, never runtime values, and matching returns a plain `bool`.

use serde_json::Value;

const MAX_FILTER_BYTES: usize = 1024;
const MAX_FILTER_LEVELS: usize = 64;
const MAX_TEMPLATE_BYTES: usize = 512;
const MAX_TEMPLATE_PARTS: usize = 16;
const MAX_TOPIC_INDEX: usize = 63;
const MAX_POINTER_BYTES: usize = 512;
const MAX_POINTER_TOKENS: usize = 32;

/// Errors produced while compiling or rendering paths, filters and templates.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PathError {
    /// The input was empty where a non-empty value is required.
    #[error("empty input")]
    Empty,
    /// The input is malformed.
    #[error("syntax error at byte {pos}: {message}")]
    Syntax {
        /// Byte offset into the input where the problem was detected.
        pos: usize,
        /// Static description of the problem.
        message: &'static str,
    },
    /// The input exceeds the maximum length in bytes.
    #[error("input too long")]
    TooLong,
    /// The input has more levels (or pointer tokens) than allowed.
    #[error("too many levels")]
    TooManyLevels,
    /// The template has more parts than allowed.
    #[error("too many template parts")]
    TooManyParts,
    /// A placeholder had no value at render time.
    #[error("unresolved placeholder `{what}`")]
    Unresolved {
        /// Name of the placeholder (never its value), e.g. `topic.7`.
        what: String,
    },
}

const fn syntax(pos: usize, message: &'static str) -> PathError {
    PathError::Syntax { pos, message }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Level {
    Literal(Box<str>),
    Plus,
    Hash,
}

/// A validated MQTT 3.1.1/5 topic filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicFilter {
    levels: Vec<Level>,
    source: String,
}

impl TopicFilter {
    /// Parses and validates a topic filter.
    ///
    /// `+` and `#` must each occupy a whole level and `#` must be the last one.
    ///
    /// # Errors
    ///
    /// [`PathError::Empty`] for an empty filter, [`PathError::TooLong`] above
    /// 1024 bytes, [`PathError::TooManyLevels`] above 64 levels and
    /// [`PathError::Syntax`] for misplaced wildcards.
    pub fn parse(filter: &str) -> Result<TopicFilter, PathError> {
        if filter.is_empty() {
            return Err(PathError::Empty);
        }
        if filter.len() > MAX_FILTER_BYTES {
            return Err(PathError::TooLong);
        }
        let count = filter.matches('/').count() + 1;
        if count > MAX_FILTER_LEVELS {
            return Err(PathError::TooManyLevels);
        }

        let mut levels = Vec::with_capacity(count);
        let mut pos = 0;
        for (i, level) in filter.split('/').enumerate() {
            let parsed = match level {
                "+" => Level::Plus,
                "#" if i + 1 == count => Level::Hash,
                "#" => return Err(syntax(pos, "'#' must be the last level")),
                _ if level.contains(['+', '#']) => {
                    return Err(syntax(pos, "wildcard must occupy an entire level"));
                }
                _ => Level::Literal(level.into()),
            };
            levels.push(parsed);
            pos += level.len() + 1;
        }
        Ok(TopicFilter {
            levels,
            source: filter.to_owned(),
        })
    }

    /// Returns whether `topic` matches this filter.
    ///
    /// The topic is not validated; wildcard characters in it are plain text.
    /// Topics starting with `$` never match a filter whose first level is a
    /// wildcard (MQTT 4.7.2).
    #[must_use]
    pub fn matches(&self, topic: &str) -> bool {
        if topic.starts_with('$') && matches!(self.levels.first(), Some(Level::Plus | Level::Hash))
        {
            return false;
        }
        let mut topic_levels = topic.split('/');
        for level in &self.levels {
            match level {
                Level::Hash => return true,
                Level::Plus => {
                    if topic_levels.next().is_none() {
                        return false;
                    }
                }
                Level::Literal(lit) => {
                    if topic_levels.next() != Some(&**lit) {
                        return false;
                    }
                }
            }
        }
        topic_levels.next().is_none()
    }

    /// Returns the original filter text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.source
    }
}

/// One element of a parsed [`Template`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TemplatePart {
    /// Literal text, with brace escapes already resolved.
    Literal(String),
    /// `{topic.N}`: the topic level at zero-based index `N`.
    TopicLevel(usize),
    /// `{topic}`: the whole topic.
    Topic,
    /// `{source}`: the source name.
    Source,
    /// `{attr.KEY}`: a message attribute.
    Attr(String),
    /// `{payload:POINTER}`: a scalar from the payload.
    Payload(FieldPath),
}

/// Supplies the values a [`Template`] is rendered against.
pub trait TemplateContext {
    /// The message topic, if the message has one.
    fn topic(&self) -> Option<&str>;
    /// The name of the source that produced the message.
    fn source(&self) -> &str;
    /// The attribute named `key`, if present.
    fn attr(&self, key: &str) -> Option<&str>;
    /// The scalar string/number/bool at a payload pointer rendered as text (None if absent, null, object or array).
    fn payload_text(&self, path: &FieldPath) -> Option<String>;
}

/// A parsed identity template such as `robot-{topic.3}`.
///
/// Placeholders are `{topic.N}`, `{topic}`, `{source}`, `{attr.KEY}` and
/// `{payload:POINTER}`; `{{` and `}}` produce literal braces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Template {
    parts: Vec<TemplatePart>,
}

impl Template {
    /// Parses a template. The empty string is a valid template rendering `""`.
    ///
    /// A placeholder body ends at the first `}`.
    ///
    /// # Errors
    ///
    /// [`PathError::TooLong`] above 512 bytes, [`PathError::TooManyParts`]
    /// above 16 parts and [`PathError::Syntax`] for unknown placeholders,
    /// unclosed `{`, stray `}` or an invalid payload pointer (including parent
    /// paths).
    pub fn parse(src: &str) -> Result<Template, PathError> {
        if src.len() > MAX_TEMPLATE_BYTES {
            return Err(PathError::TooLong);
        }
        let mut parts = Vec::new();
        let mut literal = String::new();
        let mut rest = src;
        let mut pos = 0;

        while let Some(idx) = rest.find(['{', '}']) {
            let (before, tail) = rest.split_at(idx);
            literal.push_str(before);
            pos += idx;

            let mut chars = tail.chars();
            let is_open = chars.next() == Some('{');
            let after = chars.as_str();
            let doubled = after.strip_prefix(if is_open { '{' } else { '}' });

            if let Some(next) = doubled {
                literal.push(if is_open { '{' } else { '}' });
                rest = next;
                pos += 2;
            } else if is_open {
                let Some(end) = after.find('}') else {
                    return Err(syntax(pos, "unclosed '{'"));
                };
                let (body, closing) = after.split_at(end);
                if !literal.is_empty() {
                    parts.push(TemplatePart::Literal(std::mem::take(&mut literal)));
                }
                parts.push(parse_placeholder(body, pos)?);
                rest = closing.strip_prefix('}').unwrap_or_default();
                pos += end + 2;
            } else {
                return Err(syntax(pos, "unmatched '}'"));
            }
        }
        literal.push_str(rest);
        if !literal.is_empty() {
            parts.push(TemplatePart::Literal(literal));
        }

        if parts.len() > MAX_TEMPLATE_PARTS {
            return Err(PathError::TooManyParts);
        }
        Ok(Template { parts })
    }

    /// Returns the parsed parts in order.
    #[must_use]
    pub fn parts(&self) -> &[TemplatePart] {
        &self.parts
    }

    /// Returns `true` if the template contains no placeholders.
    #[must_use]
    pub fn is_literal(&self) -> bool {
        self.parts
            .iter()
            .all(|part| matches!(part, TemplatePart::Literal(_)))
    }

    /// Renders the template against `ctx`.
    ///
    /// # Errors
    ///
    /// [`PathError::Unresolved`] naming the first placeholder that has no
    /// value, e.g. `topic.7` or `attr.region`.
    pub fn render(&self, ctx: &dyn TemplateContext) -> Result<String, PathError> {
        let mut out = String::new();
        for part in &self.parts {
            match part {
                TemplatePart::Literal(text) => out.push_str(text),
                TemplatePart::TopicLevel(n) => {
                    let level = ctx.topic().and_then(|topic| topic.split('/').nth(*n));
                    out.push_str(level.ok_or_else(|| unresolved(format!("topic.{n}")))?);
                }
                TemplatePart::Topic => {
                    out.push_str(ctx.topic().ok_or_else(|| unresolved("topic".into()))?);
                }
                TemplatePart::Source => out.push_str(ctx.source()),
                TemplatePart::Attr(key) => {
                    out.push_str(
                        ctx.attr(key)
                            .ok_or_else(|| unresolved(format!("attr.{key}")))?,
                    );
                }
                TemplatePart::Payload(path) => {
                    let text = ctx
                        .payload_text(path)
                        .ok_or_else(|| unresolved(format!("payload:{}", path.as_str())))?;
                    out.push_str(&text);
                }
            }
        }
        Ok(out)
    }

    /// Returns `true` if any placeholder reads from the payload.
    #[must_use]
    pub fn uses_payload(&self) -> bool {
        self.parts
            .iter()
            .any(|part| matches!(part, TemplatePart::Payload(_)))
    }
}

const fn unresolved(what: String) -> PathError {
    PathError::Unresolved { what }
}

/// `open` is the byte offset of the placeholder's `{`.
fn parse_placeholder(body: &str, open: usize) -> Result<TemplatePart, PathError> {
    let body_pos = open + 1;
    match body {
        "topic" => return Ok(TemplatePart::Topic),
        "source" => return Ok(TemplatePart::Source),
        _ => {}
    }
    if let Some(index) = body.strip_prefix("topic.") {
        let valid = !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit());
        return match index.parse::<usize>() {
            Ok(n) if valid && n <= MAX_TOPIC_INDEX => Ok(TemplatePart::TopicLevel(n)),
            _ => Err(syntax(open, "topic level must be a number from 0 to 63")),
        };
    }
    if let Some(key) = body.strip_prefix("attr.") {
        return if key.is_empty() {
            Err(syntax(open, "attribute name is empty"))
        } else {
            Ok(TemplatePart::Attr(key.to_owned()))
        };
    }
    if let Some(pointer) = body.strip_prefix("payload:") {
        let pointer_pos = body_pos + "payload:".len();
        let path = FieldPath::parse(pointer).map_err(|err| match err {
            PathError::Syntax { pos, message } => syntax(pointer_pos + pos, message),
            other => other,
        })?;
        if path.is_parent() {
            return Err(syntax(
                pointer_pos,
                "payload path must not be a parent path",
            ));
        }
        return Ok(TemplatePart::Payload(path));
    }
    Err(syntax(open, "unknown placeholder"))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Token {
    text: Box<str>,
    index: Option<usize>,
}

impl Token {
    fn new(text: String) -> Self {
        let canonical = text.bytes().all(|b| b.is_ascii_digit())
            && !text.is_empty()
            && (text == "0" || !text.starts_with('0'));
        let index = if canonical { text.parse().ok() } else { None };
        Token {
            text: text.into_boxed_str(),
            index,
        }
    }
}

/// A JSON Pointer (RFC 6901) that may be anchored at the parent scope.
///
/// The `^` prefix (`^`, `^/seq`) makes the pointer resolve against the root
/// value instead of the current scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldPath {
    parent: bool,
    tokens: Vec<Token>,
    source: String,
}

impl FieldPath {
    /// Parses `""` (the value itself), `"/a/b/0"`, `"^"` or `"^/seq"`.
    ///
    /// # Errors
    ///
    /// [`PathError::TooLong`] above 512 bytes, [`PathError::TooManyLevels`]
    /// above 32 tokens and [`PathError::Syntax`] for a missing leading `/`, a
    /// `^` not followed by `/` or the end, or an invalid `~` escape.
    pub fn parse(src: &str) -> Result<FieldPath, PathError> {
        if src.len() > MAX_POINTER_BYTES {
            return Err(PathError::TooLong);
        }
        let (parent, pointer, base) = match src.strip_prefix('^') {
            Some(rest) => (true, rest, 1),
            None => (false, src, 0),
        };

        let mut tokens = Vec::new();
        if !pointer.is_empty() {
            let Some(body) = pointer.strip_prefix('/') else {
                return Err(syntax(
                    base,
                    if parent {
                        "expected '/' after '^'"
                    } else {
                        "pointer must start with '/'"
                    },
                ));
            };
            if body.matches('/').count() + 1 > MAX_POINTER_TOKENS {
                return Err(PathError::TooManyLevels);
            }
            let mut pos = base + 1;
            for raw in body.split('/') {
                tokens.push(Token::new(unescape(raw, pos)?));
                pos += raw.len() + 1;
            }
        }
        Ok(FieldPath {
            parent,
            tokens,
            source: src.to_owned(),
        })
    }

    /// Returns `true` for the `^` (parent scope) form.
    #[must_use]
    pub fn is_parent(&self) -> bool {
        self.parent
    }

    /// Returns the original path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Looks the path up, starting at `root` for parent paths and at `scope`
    /// otherwise.
    ///
    /// Returns `None` if the path is absent. A JSON `null` at the path is
    /// returned as `Some(Value::Null)`; callers decide how to treat it.
    #[must_use]
    pub fn resolve<'a>(&self, scope: &'a Value, root: &'a Value) -> Option<&'a Value> {
        let mut current = if self.parent { root } else { scope };
        for token in &self.tokens {
            current = match current {
                Value::Object(map) => map.get(&*token.text)?,
                Value::Array(items) => items.get(token.index?)?,
                _ => return None,
            };
        }
        Some(current)
    }
}

/// `pos` is the byte offset of `raw` within the whole path, for error reporting.
fn unescape(raw: &str, pos: usize) -> Result<String, PathError> {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.char_indices();
    while let Some((i, c)) = chars.next() {
        if c != '~' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some((_, '0')) => out.push('~'),
            Some((_, '1')) => out.push('/'),
            _ => return Err(syntax(pos + i, "invalid '~' escape")),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::parse(s).unwrap()
    }

    fn template(s: &str) -> Template {
        Template::parse(s).unwrap()
    }

    fn path(s: &str) -> FieldPath {
        FieldPath::parse(s).unwrap()
    }

    #[test]
    fn mqtt_spec_multi_level_wildcard() {
        let f = filter("sport/tennis/player1/#");
        assert!(f.matches("sport/tennis/player1"));
        assert!(f.matches("sport/tennis/player1/ranking"));
        assert!(f.matches("sport/tennis/player1/score/wimbledon"));
        assert!(!f.matches("sport/tennis/player2"));

        let f = filter("sport/#");
        assert!(f.matches("sport"));
        assert!(f.matches("sport/"));
        assert!(f.matches("sport/tennis"));
        assert!(!f.matches("sports"));

        let f = filter("#");
        assert!(f.matches("anything/at/all"));
        assert!(f.matches(""));
        assert!(f.matches("/"));
    }

    #[test]
    fn mqtt_spec_single_level_wildcard() {
        let f = filter("sport/tennis/+");
        assert!(f.matches("sport/tennis/player1"));
        assert!(f.matches("sport/tennis/player2"));
        assert!(!f.matches("sport/tennis/player1/ranking"));

        let f = filter("sport/+");
        assert!(f.matches("sport/"));
        assert!(f.matches("sport/tennis"));
        assert!(!f.matches("sport"));
        assert!(!f.matches("sport/tennis/player1"));

        let f = filter("+/tennis/#");
        assert!(f.matches("sport/tennis"));
        assert!(f.matches("sport/tennis/player1"));
        assert!(f.matches("/tennis/x"));
        assert!(!f.matches("sport/golf"));
    }

    #[test]
    fn plus_and_empty_levels() {
        assert!(!filter("+").matches("/finance"));
        assert!(filter("+/+").matches("/finance"));
        assert!(filter("/+").matches("/finance"));
        assert!(filter("+").matches(""));
        assert!(filter("+").matches("finance"));
        assert!(!filter("+/+").matches(""));
        assert!(filter("a//b").matches("a//b"));
        assert!(!filter("a//b").matches("a/b"));
    }

    #[test]
    fn dollar_topics_skip_leading_wildcards() {
        assert!(!filter("#").matches("$SYS/broker/load"));
        assert!(!filter("+/broker/load").matches("$SYS/broker/load"));
        assert!(!filter("+").matches("$SYS"));
        assert!(filter("$SYS/#").matches("$SYS/broker/load"));
        assert!(filter("$SYS/+/load").matches("$SYS/broker/load"));
        assert!(filter("a/+").matches("a/$SYS"));
        assert!(filter("a/#").matches("a/$SYS/x"));
        assert!(filter("#").matches("a/$SYS"));
    }

    #[test]
    fn demo_filter() {
        let f = filter("thadam-demo/v1/+/+/telemetry");
        assert!(f.matches("thadam-demo/v1/site-a/robot-01/telemetry"));
        assert!(!f.matches("thadam-demo/v1/site-a/robot-01/events"));
        assert!(!f.matches("thadam-demo/v1/site-a/telemetry"));
        assert!(!f.matches("thadam-demo/v1/site-a/robot-01/telemetry/extra"));
        assert!(!f.matches("thadam-demo/v2/site-a/robot-01/telemetry"));
        assert!(!f.matches("other/v1/site-a/robot-01/telemetry"));
    }

    #[test]
    fn topic_with_wildcard_chars_is_literal() {
        assert!(filter("a/b").matches("a/b"));
        assert!(!filter("a/b").matches("a/+"));
        assert!(!filter("a/b").matches("a/#"));
        assert!(filter("a/+").matches("a/+"));
        assert!(filter("a/+").matches("a/#"));
    }

    #[test]
    fn filter_as_str_and_equality() {
        assert_eq!(filter("a/+/c").as_str(), "a/+/c");
        assert_eq!(filter("a/+/c"), filter("a/+/c"));
        assert_ne!(filter("a/+/c"), filter("a/b/c"));
    }

    #[test]
    fn invalid_filters() {
        assert_eq!(TopicFilter::parse(""), Err(PathError::Empty));
        assert!(matches!(
            TopicFilter::parse("a+"),
            Err(PathError::Syntax { pos: 0, .. })
        ));
        assert!(matches!(
            TopicFilter::parse("a/b#"),
            Err(PathError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            TopicFilter::parse("a#b"),
            Err(PathError::Syntax { .. })
        ));
        assert!(matches!(
            TopicFilter::parse("#a"),
            Err(PathError::Syntax { pos: 0, .. })
        ));
        assert!(matches!(
            TopicFilter::parse("a/#/b"),
            Err(PathError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            TopicFilter::parse("#/#"),
            Err(PathError::Syntax { pos: 0, .. })
        ));
        assert!(matches!(
            TopicFilter::parse("++"),
            Err(PathError::Syntax { .. })
        ));
    }

    #[test]
    fn filter_limits() {
        let max_levels = vec!["a"; 64].join("/");
        assert!(TopicFilter::parse(&max_levels).is_ok());
        let too_many = vec!["a"; 65].join("/");
        assert_eq!(TopicFilter::parse(&too_many), Err(PathError::TooManyLevels));
        assert!(TopicFilter::parse(&"a".repeat(1024)).is_ok());
        assert_eq!(
            TopicFilter::parse(&"a".repeat(1025)),
            Err(PathError::TooLong)
        );
    }

    struct Ctx {
        topic: Option<&'static str>,
        attrs: HashMap<&'static str, &'static str>,
        payload: Value,
    }

    impl Ctx {
        fn new(topic: Option<&'static str>) -> Self {
            Ctx {
                topic,
                attrs: HashMap::from([("region", "eu-west")]),
                payload: json!({"site": {"id": "site-a", "n": 7, "ok": true, "obj": {}}, "nil": null}),
            }
        }
    }

    impl TemplateContext for Ctx {
        fn topic(&self) -> Option<&str> {
            self.topic
        }
        fn source(&self) -> &str {
            "mqtt-main"
        }
        fn attr(&self, key: &str) -> Option<&str> {
            self.attrs.get(key).copied()
        }
        fn payload_text(&self, path: &FieldPath) -> Option<String> {
            match path.resolve(&self.payload, &self.payload)? {
                Value::String(s) => Some(s.clone()),
                v @ (Value::Number(_) | Value::Bool(_)) => Some(v.to_string()),
                _ => None,
            }
        }
    }

    const TOPIC: &str = "thadam-demo/v1/site-a/robot-01/telemetry";

    fn render(src: &str) -> Result<String, PathError> {
        template(src).render(&Ctx::new(Some(TOPIC)))
    }

    #[test]
    fn template_topic_level() {
        assert_eq!(render("{topic.2}").unwrap(), "site-a");
        assert_eq!(render("robot-{topic.3}").unwrap(), "robot-robot-01");
        assert_eq!(
            render("{topic.0}/{topic.4}").unwrap(),
            "thadam-demo/telemetry"
        );
        assert_eq!(
            template("{topic.2}").parts(),
            &[TemplatePart::TopicLevel(2)]
        );
    }

    #[test]
    fn template_whole_topic_source_attr() {
        assert_eq!(render("{topic}").unwrap(), TOPIC);
        assert_eq!(render("{source}").unwrap(), "mqtt-main");
        assert_eq!(render("{attr.region}").unwrap(), "eu-west");
        assert_eq!(
            render("{source}:{attr.region}:{topic.1}").unwrap(),
            "mqtt-main:eu-west:v1"
        );
    }

    #[test]
    fn template_payload() {
        let t = template("{payload:/site/id}");
        assert!(t.uses_payload());
        assert!(!t.is_literal());
        assert_eq!(t.render(&Ctx::new(None)).unwrap(), "site-a");
        assert_eq!(render("n={payload:/site/n}").unwrap(), "n=7");
        assert_eq!(render("{payload:/site/ok}").unwrap(), "true");
        assert!(matches!(
            template("{payload:/site/id}").parts(),
            [TemplatePart::Payload(p)] if p.as_str() == "/site/id"
        ));
    }

    #[test]
    fn template_payload_unresolved() {
        for (src, what) in [
            ("{payload:/missing}", "payload:/missing"),
            ("{payload:/nil}", "payload:/nil"),
            ("{payload:/site/obj}", "payload:/site/obj"),
            ("{payload:/site}", "payload:/site"),
        ] {
            assert_eq!(
                render(src),
                Err(PathError::Unresolved { what: what.into() })
            );
        }
    }

    #[test]
    fn template_literals_and_escapes() {
        let t = template("{{x}}");
        assert!(t.is_literal());
        assert!(!t.uses_payload());
        assert_eq!(t.parts(), &[TemplatePart::Literal("{x}".into())]);
        assert_eq!(t.render(&Ctx::new(None)).unwrap(), "{x}");
        assert_eq!(render("a{{b}}c{source}}}").unwrap(), "a{b}cmqtt-main}");
        assert_eq!(render("{{{source}}}").unwrap(), "{mqtt-main}");

        let plain = template("plain");
        assert!(plain.is_literal());
        assert_eq!(plain.render(&Ctx::new(None)).unwrap(), "plain");

        let empty = template("");
        assert!(empty.is_literal());
        assert!(empty.parts().is_empty());
        assert_eq!(empty.render(&Ctx::new(None)).unwrap(), "");
    }

    #[test]
    fn template_syntax_errors() {
        assert!(matches!(
            Template::parse("a{nope}"),
            Err(PathError::Syntax { pos: 1, .. })
        ));
        assert!(matches!(
            Template::parse("{}"),
            Err(PathError::Syntax { pos: 0, .. })
        ));
        assert!(matches!(
            Template::parse("ab{topic.1"),
            Err(PathError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            Template::parse("ab}"),
            Err(PathError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            Template::parse("{topic}}x"),
            Err(PathError::Syntax { pos: 7, .. })
        ));
        for bad in [
            "{topic.}",
            "{topic.x}",
            "{topic.-1}",
            "{topic.+1}",
            "{topic.64}",
            "{topic.99999999999999999999999}",
            "{attr.}",
            "{Topic}",
            "{topic }",
            "{payload}",
        ] {
            assert!(
                matches!(Template::parse(bad), Err(PathError::Syntax { .. })),
                "{bad}"
            );
        }
        assert!(Template::parse("{topic.63}").is_ok());
    }

    #[test]
    fn template_payload_pointer_errors() {
        assert!(matches!(
            Template::parse("{payload:^/seq}"),
            Err(PathError::Syntax { pos: 9, .. })
        ));
        assert!(matches!(
            Template::parse("{payload:^}"),
            Err(PathError::Syntax { .. })
        ));
        assert!(matches!(
            Template::parse("{payload:a/b}"),
            Err(PathError::Syntax { pos: 9, .. })
        ));
        assert!(matches!(
            Template::parse("x{payload:/a~2}"),
            Err(PathError::Syntax { pos: 12, .. })
        ));
        assert!(Template::parse("{payload:}").is_ok());
    }

    #[test]
    fn template_attr_key_any_chars() {
        let t = template("{attr.a b/c{d}");
        assert_eq!(t.parts(), &[TemplatePart::Attr("a b/c{d".into())]);
    }

    #[test]
    fn template_unresolved() {
        assert_eq!(
            Template::parse("{topic.99}"),
            Err(PathError::Syntax {
                pos: 0,
                message: "topic level must be a number from 0 to 63"
            })
        );
        assert_eq!(
            render("{topic.7}"),
            Err(PathError::Unresolved {
                what: "topic.7".into()
            })
        );
        assert_eq!(
            render("{attr.missing}"),
            Err(PathError::Unresolved {
                what: "attr.missing".into()
            })
        );
        let no_topic = Ctx::new(None);
        assert_eq!(
            template("{topic}").render(&no_topic),
            Err(PathError::Unresolved {
                what: "topic".into()
            })
        );
        assert_eq!(
            template("{topic.0}").render(&no_topic),
            Err(PathError::Unresolved {
                what: "topic.0".into()
            })
        );
    }

    #[test]
    fn template_topic_level_beyond_topic() {
        let ctx = Ctx::new(Some("a/b"));
        assert_eq!(template("{topic.1}").render(&ctx).unwrap(), "b");
        assert!(template("{topic.2}").render(&ctx).is_err());
        let ctx = Ctx::new(Some(""));
        assert_eq!(template("[{topic.0}]").render(&ctx).unwrap(), "[]");
    }

    #[test]
    fn template_unresolved_never_echoes_values() {
        let ctx = Ctx::new(Some("secret/topic"));
        let err = template("{topic.5}").render(&ctx).unwrap_err();
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn template_limits() {
        let sixteen = "{source}".repeat(16);
        assert_eq!(template(&sixteen).parts().len(), 16);
        let seventeen = "{source}".repeat(17);
        assert_eq!(Template::parse(&seventeen), Err(PathError::TooManyParts));
        assert_eq!(
            Template::parse(&"x".repeat(512)).map(|t| t.parts().len()),
            Ok(1)
        );
        assert_eq!(Template::parse(&"x".repeat(513)), Err(PathError::TooLong));
        assert_eq!(Template::parse(&"{{".repeat(257)), Err(PathError::TooLong));
    }

    fn doc() -> Value {
        json!({
            "ts_us": 1_700_000_000_000_000_u64,
            "seq": 42,
            "samples": [{"pos": [1.0, 2.0]}, {"pos": [3.0, 4.0]}],
            "a": {"0": "zero", "x~y": 1, "p/q": 2, "": "empty"},
            "arr": ["a", "b", "c"],
            "nil": null,
        })
    }

    #[test]
    fn field_path_basic() {
        let d = doc();
        assert_eq!(
            path("/ts_us").resolve(&d, &d),
            Some(&json!(1_700_000_000_000_000_u64))
        );
        assert_eq!(
            path("/samples/0/pos").resolve(&d, &d),
            Some(&json!([1.0, 2.0]))
        );
        assert_eq!(path("/samples/1/pos/1").resolve(&d, &d), Some(&json!(4.0)));
        assert_eq!(path("/missing").resolve(&d, &d), None);
        assert_eq!(path("/ts_us/deeper").resolve(&d, &d), None);
        assert_eq!(path("/nil/deeper").resolve(&d, &d), None);
        assert_eq!(path("/a/x~0y").as_str(), "/a/x~0y");
    }

    #[test]
    fn field_path_whole_value() {
        let d = doc();
        let p = path("");
        assert!(!p.is_parent());
        assert_eq!(p.as_str(), "");
        assert_eq!(p.resolve(&d, &d), Some(&d));
        let scalar = json!(5);
        assert_eq!(p.resolve(&scalar, &d), Some(&scalar));
    }

    #[test]
    fn field_path_parent() {
        let root = doc();
        let scope = json!({"seq": "scope-seq", "inner": 1});

        let p = path("^/seq");
        assert!(p.is_parent());
        assert_eq!(p.as_str(), "^/seq");
        assert_eq!(p.resolve(&scope, &root), Some(&json!(42)));
        assert_eq!(
            path("/seq").resolve(&scope, &root),
            Some(&json!("scope-seq"))
        );
        assert_eq!(path("/inner").resolve(&root, &root), None);

        let itself = path("^");
        assert!(itself.is_parent());
        assert_eq!(itself.resolve(&scope, &root), Some(&root));
    }

    #[test]
    fn field_path_escapes() {
        let d = doc();
        assert_eq!(path("/a/x~0y").resolve(&d, &d), Some(&json!(1)));
        assert_eq!(path("/a/p~1q").resolve(&d, &d), Some(&json!(2)));
        assert_eq!(path("/a/").resolve(&d, &d), Some(&json!("empty")));
        assert_eq!(
            path("/a/x~01").resolve(&json!({"a": {"x~1": 9}}), &d),
            Some(&json!(9))
        );
        assert_eq!(path("/a/p/q").resolve(&d, &d), None);
    }

    #[test]
    fn field_path_array_indexes() {
        let d = doc();
        assert_eq!(path("/arr/0").resolve(&d, &d), Some(&json!("a")));
        assert_eq!(path("/arr/2").resolve(&d, &d), Some(&json!("c")));
        assert_eq!(path("/arr/3").resolve(&d, &d), None);
        assert_eq!(path("/arr/01").resolve(&d, &d), None);
        assert_eq!(path("/arr/00").resolve(&d, &d), None);
        assert_eq!(path("/arr/-").resolve(&d, &d), None);
        assert_eq!(path("/arr/+1").resolve(&d, &d), None);
        assert_eq!(path("/arr/-1").resolve(&d, &d), None);
        assert_eq!(path("/arr/1x").resolve(&d, &d), None);
        assert_eq!(path("/arr/").resolve(&d, &d), None);
        assert_eq!(
            path("/arr/99999999999999999999999999").resolve(&d, &d),
            None
        );
        assert_eq!(path("/arr/x").resolve(&d, &d), None);
    }

    #[test]
    fn field_path_numeric_object_key() {
        let d = doc();
        assert_eq!(path("/a/0").resolve(&d, &d), Some(&json!("zero")));
        let obj = json!({"01": "lead", "-": "dash"});
        assert_eq!(path("/01").resolve(&obj, &obj), Some(&json!("lead")));
        assert_eq!(path("/-").resolve(&obj, &obj), Some(&json!("dash")));
    }

    #[test]
    fn field_path_null_is_some() {
        let d = doc();
        assert_eq!(path("/nil").resolve(&d, &d), Some(&Value::Null));
    }

    #[test]
    fn field_path_invalid() {
        assert!(matches!(
            FieldPath::parse("a/b"),
            Err(PathError::Syntax { pos: 0, .. })
        ));
        assert!(matches!(
            FieldPath::parse("/a~"),
            Err(PathError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            FieldPath::parse("/a~2"),
            Err(PathError::Syntax { pos: 2, .. })
        ));
        assert!(matches!(
            FieldPath::parse("/a/b~x"),
            Err(PathError::Syntax { pos: 4, .. })
        ));
        assert!(matches!(
            FieldPath::parse("^x"),
            Err(PathError::Syntax { pos: 1, .. })
        ));
        assert!(matches!(
            FieldPath::parse("^/a~"),
            Err(PathError::Syntax { pos: 3, .. })
        ));
        assert!(matches!(
            FieldPath::parse("^^"),
            Err(PathError::Syntax { pos: 1, .. })
        ));
        assert!(matches!(
            FieldPath::parse("/é~"),
            Err(PathError::Syntax { pos: 3, .. })
        ));
    }

    #[test]
    fn field_path_limits() {
        let max = format!("/{}", vec!["a"; 32].join("/"));
        assert!(FieldPath::parse(&max).is_ok());
        let over = format!("/{}", vec!["a"; 33].join("/"));
        assert_eq!(FieldPath::parse(&over), Err(PathError::TooManyLevels));
        assert!(FieldPath::parse(&format!("/{}", "a".repeat(511))).is_ok());
        assert_eq!(
            FieldPath::parse(&format!("/{}", "a".repeat(512))),
            Err(PathError::TooLong)
        );
    }

    #[test]
    fn field_path_hash_and_eq() {
        use std::collections::HashSet;
        let set: HashSet<FieldPath> = [path("/a"), path("/a"), path("^/a")].into_iter().collect();
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn error_display_is_informative() {
        let err = TopicFilter::parse("a+").unwrap_err();
        assert!(err.to_string().contains("byte 0"));
        assert!(
            PathError::Unresolved {
                what: "topic.7".into()
            }
            .to_string()
            .contains("topic.7")
        );
    }

    proptest! {
        #[test]
        fn matches_never_panics(f in ".*", topic in ".*") {
            if let Ok(parsed) = TopicFilter::parse(&f) {
                let _ = parsed.matches(&topic);
            }
        }

        #[test]
        fn matches_never_panics_wildcard_heavy(f in "[a+#/$]{0,40}", topic in "[a+#/$]{0,40}") {
            if let Ok(parsed) = TopicFilter::parse(&f) {
                let _ = parsed.matches(&topic);
            }
        }

        #[test]
        fn literal_filter_matches_only_itself(topic in ".*") {
            let f = filter("a/b/c");
            prop_assert_eq!(f.matches(&topic), topic == "a/b/c");
        }

        #[test]
        fn random_literal_levels_match_themselves(
            l1 in "[^/+#]{0,8}",
            l2 in "[^/+#]{0,8}",
            l3 in "[^/+#]{0,8}",
            o1 in "[^/+#]{0,8}",
            o2 in "[^/+#]{0,8}",
            o3 in "[^/+#]{0,8}",
        ) {
            let same = format!("{l1}/{l2}/{l3}");
            let shorter = format!("{l1}/{l2}");
            let longer = format!("{same}/x");
            let other = format!("{o1}/{o2}/{o3}");
            let f = TopicFilter::parse(&same).unwrap();
            prop_assert!(f.matches(&same));
            prop_assert_eq!(f.matches(&other), same == other);
            prop_assert!(!f.matches(&shorter));
            prop_assert!(!f.matches(&longer));
        }

        #[test]
        fn hash_filter_matches_everything_but_dollar(topic in ".*") {
            prop_assert_eq!(filter("#").matches(&topic), !topic.starts_with('$'));
        }

        #[test]
        fn parsers_never_panic(s in ".*") {
            let _ = Template::parse(&s);
            let _ = FieldPath::parse(&s);
        }

        #[test]
        fn resolve_never_panics(s in "[/^~01a-]{0,30}") {
            if let Ok(p) = FieldPath::parse(&s) {
                let d = doc();
                let _ = p.resolve(&d, &d);
            }
        }
    }
}
