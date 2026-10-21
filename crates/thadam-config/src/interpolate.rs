//! `${env:NAME}` and `${file:/absolute/path}` interpolation.
//!
//! Rules (ADR-0012 PR5 detail 4):
//!
//! - It runs on parsed string scalars, never on raw text, so a value cannot
//!   change the document structure. Keys are not interpolated.
//! - A result is always a string, and a substituted value is never expanded again.
//! - `$${` writes a literal `${`. Nesting (`${env:${env:X}}`) and defaults are
//!   not supported.
//! - `${file:}` needs an absolute path; one trailing newline is removed, and
//!   the file must be UTF-8 and at most [`MAX_SECRET_FILE_BYTES`].

use std::path::Path;

use serde_json::Value;
use thadam_core::codes;

use crate::ctx::{Ctx, Resolved};
use crate::path::{PathExt, PathVec};
use crate::source::{EnvSource, MAX_SECRET_FILE_BYTES};
use crate::spans::Anchor;
use crate::spec::InterpolationSite;

/// What to do with a `${env:}` or `${file:}` reference that cannot be resolved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnresolvedPolicy {
    /// Report an error. The default.
    #[default]
    Error,
    /// Report a warning and substitute `<unresolved:...>`, so a configuration
    /// can be checked on a machine that lacks the secrets.
    Placeholder,
}

/// The longest string interpolation may produce, in bytes.
const MAX_RESULT_BYTES: usize = 1 << 20;

struct Problem {
    code: &'static str,
    message: String,
    hint: Option<String>,
    warning: bool,
}

struct Expansion {
    text: String,
    values: Vec<String>,
    interpolated: bool,
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn unresolved(policy: UnresolvedPolicy, message: String, hint: &str) -> Problem {
    Problem {
        code: codes::CONFIG_MISSING,
        message,
        hint: Some(hint.to_owned()),
        warning: policy == UnresolvedPolicy::Placeholder,
    }
}

fn resolve_env(
    name: &str,
    env: &dyn EnvSource,
    policy: UnresolvedPolicy,
) -> Result<String, (String, Problem)> {
    if !valid_env_name(name) {
        return Err((
            format!("${{env:{name}}}"),
            Problem {
                code: codes::CONFIG_INVALID,
                message: format!("`{name}` is not a valid environment variable name"),
                hint: Some(
                    "names use letters, digits, and `_`, and do not start with a digit".into(),
                ),
                warning: false,
            },
        ));
    }
    env.var(name).ok_or_else(|| {
        (
            format!("<unresolved:env:{name}>"),
            unresolved(
                policy,
                format!("environment variable `{name}` is not set"),
                "set the variable in the environment thadam starts in",
            ),
        )
    })
}

fn resolve_file(
    path: &str,
    env: &dyn EnvSource,
    policy: UnresolvedPolicy,
) -> Result<String, (String, Problem)> {
    let placeholder = format!("<unresolved:file:{path}>");
    if !Path::new(path).is_absolute() {
        return Err((
            placeholder,
            Problem {
                code: codes::CONFIG_INVALID,
                message: format!("`${{file:{path}}}` needs an absolute path"),
                hint: Some("a relative path would depend on the working directory".into()),
                warning: false,
            },
        ));
    }
    let bytes = env.read_file(Path::new(path)).map_err(|e| {
        (
            placeholder.clone(),
            unresolved(
                policy,
                format!("cannot read `{path}`: {e}"),
                "check that the file exists and thadam can read it",
            ),
        )
    })?;
    if bytes.len() as u64 > MAX_SECRET_FILE_BYTES {
        return Err((
            placeholder,
            Problem {
                code: codes::CONFIG_INVALID,
                message: format!("`{path}` is larger than {MAX_SECRET_FILE_BYTES} bytes"),
                hint: Some("`${file:}` is for small values such as keys and tokens".into()),
                warning: false,
            },
        ));
    }
    let mut text = String::from_utf8(bytes).map_err(|_| {
        (
            placeholder,
            Problem {
                code: codes::CONFIG_INVALID,
                message: format!("`{path}` is not valid UTF-8"),
                hint: None,
                warning: false,
            },
        )
    })?;
    if text.ends_with('\n') {
        text.pop();
        if text.ends_with('\r') {
            text.pop();
        }
    }
    Ok(text)
}

fn expand(
    s: &str,
    env: &dyn EnvSource,
    policy: UnresolvedPolicy,
    problems: &mut Vec<Problem>,
) -> Expansion {
    let mut out = String::with_capacity(s.len());
    let mut values = Vec::new();
    let mut interpolated = false;
    let mut i = 0;
    while i < s.len() {
        let rest = &s[i..];
        if rest.starts_with("$${") {
            out.push_str("${");
            i += 3;
        } else if let Some(after) = rest.strip_prefix("${") {
            let Some(end) = after.find('}') else {
                problems.push(Problem {
                    code: codes::CONFIG_INVALID,
                    message: "unterminated `${`".into(),
                    hint: Some("write `$${` for a literal `${`".into()),
                    warning: false,
                });
                out.push_str(rest);
                break;
            };
            let body = &after[..end];
            i += 2 + end + 1;
            interpolated = true;
            if body.contains("${") {
                problems.push(Problem {
                    code: codes::CONFIG_INVALID,
                    message: "nested interpolation is not supported".into(),
                    hint: None,
                    warning: false,
                });
                continue;
            }
            let resolved = match body.split_once(':') {
                Some(("env", name)) => resolve_env(name, env, policy),
                Some(("file", path)) => resolve_file(path, env, policy),
                Some((other, _)) => Err((
                    String::new(),
                    Problem {
                        code: codes::CONFIG_INVALID,
                        message: format!("unknown interpolation source `{other}`"),
                        hint: Some("use `${env:NAME}` or `${file:/absolute/path}`".into()),
                        warning: false,
                    },
                )),
                None => Err((
                    String::new(),
                    Problem {
                        code: codes::CONFIG_INVALID,
                        message: format!("`${{{body}}}` is not an interpolation"),
                        hint: Some("use `${env:NAME}` or `${file:/absolute/path}`; write `$${` for a literal `${`".into()),
                        warning: false,
                    },
                )),
            };
            match resolved {
                Ok(value) => {
                    out.push_str(&value);
                    values.push(value);
                }
                Err((placeholder, problem)) => {
                    if problem.warning {
                        out.push_str(&placeholder);
                    }
                    problems.push(problem);
                }
            }
        } else {
            let Some(c) = rest.chars().next() else { break };
            out.push(c);
            i += c.len_utf8();
        }
    }
    if out.len() > MAX_RESULT_BYTES {
        problems.push(Problem {
            code: codes::CONFIG_INVALID,
            message: format!("the interpolated value is larger than {MAX_RESULT_BYTES} bytes"),
            hint: None,
            warning: false,
        });
    }
    Expansion {
        text: out,
        values,
        interpolated,
    }
}

/// Interpolates every string scalar under `root` in place, returning where
/// interpolation happened.
pub(crate) fn interpolate(
    root: &mut Value,
    env: &dyn EnvSource,
    policy: UnresolvedPolicy,
    ctx: &mut Ctx<'_>,
) -> Vec<InterpolationSite> {
    let mut sites = Vec::new();
    walk(root, &Vec::new(), env, policy, ctx, &mut sites);
    sites
}

fn walk(
    v: &mut Value,
    path: &PathVec,
    env: &dyn EnvSource,
    policy: UnresolvedPolicy,
    ctx: &mut Ctx<'_>,
    sites: &mut Vec<InterpolationSite>,
) {
    match v {
        Value::String(s) if s.contains("${") || s.contains("$${") => {
            let mut problems = Vec::new();
            let e = expand(s, env, policy, &mut problems);
            let template = std::mem::replace(s, e.text);
            if e.interpolated {
                ctx.add_resolved(Resolved {
                    path: path.clone(),
                    values: e.values,
                });
                sites.push(InterpolationSite {
                    path: path.clone(),
                    template,
                });
            }
            for p in problems {
                if p.warning {
                    ctx.warn(p.code, path, Anchor::Value, p.message, p.hint);
                } else {
                    ctx.error(p.code, path, Anchor::Value, p.message, p.hint);
                }
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                walk(item, &path.idx(i), env, policy, ctx, sites);
            }
        }
        Value::Object(map) => {
            for (k, item) in map.iter_mut() {
                walk(item, &path.key(k), env, policy, ctx, sites);
            }
        }
        _ => {}
    }
}
