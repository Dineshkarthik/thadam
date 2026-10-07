//! ADR-0006 rule 1: the poll task only ever awaits `EventLoop::poll()` to
//! completion. `poll` is not cancel-safe on its write path (spike S2: a dropped
//! poll lost acknowledgements silently), so it must never sit inside
//! `select!`, `timeout`, `now_or_never`, or any other combinator that can drop
//! it. This test reads the crate's sources and fails when that changes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};

fn sources() -> Vec<(PathBuf, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "rs"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).unwrap();
            (path, text)
        })
        .collect()
}

/// Lines of `text` outside `#[cfg(test)]` modules, with comments removed.
fn code_lines(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let all: Vec<&str> = text.lines().collect();
    for (i, line) in all.iter().enumerate() {
        if line.trim() == "#[cfg(test)]"
            && all
                .get(i + 1)
                .is_some_and(|next| next.trim_start().starts_with("mod tests"))
        {
            break;
        }
        let code = line.split("//").next().unwrap_or("").trim_end().to_owned();
        if !code.trim().is_empty() {
            out.push((i + 1, code));
        }
    }
    out
}

/// Whether `line` uses `token` as its own word, so `set_connection_timeout(`
/// does not count as `timeout(`.
fn uses(line: &str, token: &str) -> bool {
    line.match_indices(token).any(|(at, _)| {
        line[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
    })
}

const FORBIDDEN_WRAPPERS: &[&str] = &[
    "select!",
    "timeout(",
    "now_or_never",
    "poll_once",
    "FutureExt",
    "race(",
    "join!",
    "abort_on_drop",
];

#[test]
fn poll_is_awaited_directly_and_only_where_allowed() {
    let mut call_sites = Vec::new();
    for (path, text) in sources() {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let lines = code_lines(&text);
        for (idx, (line_no, line)) in lines.iter().enumerate() {
            if !line.contains(".poll()") {
                continue;
            }
            assert!(
                line.contains(".poll().await"),
                "{name}:{line_no}: `.poll()` must be awaited on the spot, never stored or passed on: {line}"
            );
            for wrapper in FORBIDDEN_WRAPPERS {
                assert!(
                    !uses(line, wrapper),
                    "{name}:{line_no}: `{wrapper}` wraps `poll()`: {line}"
                );
            }
            let previous = idx
                .checked_sub(1)
                .map(|p| lines[p].1.as_str())
                .unwrap_or("");
            for wrapper in FORBIDDEN_WRAPPERS {
                assert!(
                    !uses(previous, wrapper),
                    "{name}:{line_no}: `{wrapper}` on the previous line wraps `poll()`: {previous}"
                );
            }
            call_sites.push(format!("{name}: {}", line.trim()));
        }
    }
    call_sites.sort();
    assert_eq!(
        call_sites,
        [
            "link.rs: Link::V4(el) => v4::map(el.poll().await),",
            "link.rs: Link::V5(el) => v5::map(el.poll().await),",
            "poll_task.rs: let polled = self.link.poll().await;",
        ],
        "the set of places that poll the event loop changed; each one needs review against ADR-0006 rule 1"
    );
}

#[test]
fn the_link_module_has_no_combinators_that_can_drop_a_poll() {
    let (_, text) = sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("link.rs"))
        .unwrap();
    for (line_no, line) in code_lines(&text) {
        for wrapper in FORBIDDEN_WRAPPERS {
            assert!(
                !uses(&line, wrapper),
                "link.rs:{line_no}: `{wrapper}` has no place in the module that owns the event loop: {line}"
            );
        }
    }
}

#[test]
fn the_poll_task_never_awaits_the_runtime_or_a_bounded_queue() {
    let (_, text) = sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("poll_task.rs"))
        .unwrap();
    for (line_no, line) in code_lines(&text) {
        for banned in [".send(", ".lock().await", "acquire", "recv()", ".reserve("] {
            let allowed = line.contains("ack_tx") || line.contains("self.stop");
            assert!(
                allowed || !line.contains(banned),
                "poll_task.rs:{line_no}: `{banned}` could make the poll task wait on the runtime: {line}"
            );
        }
        if line.contains(".await") && !line.contains(".poll().await") {
            assert!(
                line.contains("tokio::time::sleep") || line.contains("self.stop.cancelled()"),
                "poll_task.rs:{line_no}: the poll task may only await `poll()` and its own backoff: {line}"
            );
        }
    }
}

#[test]
fn the_only_select_in_the_poll_task_waits_out_the_reconnect_backoff() {
    let (_, text) = sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("poll_task.rs"))
        .unwrap();
    let selects: Vec<_> = code_lines(&text)
        .into_iter()
        .filter(|(_, line)| line.contains("select!"))
        .collect();
    assert_eq!(selects.len(), 1, "{selects:?}");
    let body = text
        .lines()
        .skip(selects[0].0 - 1)
        .take(4)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(body.contains("tokio::time::sleep(backoff)"), "{body}");
    assert!(body.contains("self.stop.cancelled()"), "{body}");
    assert!(!body.contains("poll"), "{body}");
}
