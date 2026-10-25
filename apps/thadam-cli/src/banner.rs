//! Terminal logo banner for interactive `--help` and `version` output.

use std::fmt::Write as _;
use std::io::IsTerminal;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

const ICON_PNG: &[u8] = include_bytes!("../assets/thadam-icon-dark.png");
const ORANGE: &str = "\x1b[38;2;242;106;61m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";
const TAGLINE: &str = "The open data plane for robotics.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Blocks,
    Kitty,
    Iterm,
}

pub(crate) fn banner() -> Option<String> {
    let stdout = std::io::stdout();
    if !stdout.is_terminal() {
        return None;
    }
    let colour = anstream::AutoStream::choice(&stdout) != anstream::ColorChoice::Never;
    let kind = choose(&|key| std::env::var(key).ok(), colour)?;
    Some(match kind {
        Kind::Blocks => blocks(),
        Kind::Kitty => kitty(ICON_PNG),
        Kind::Iterm => iterm(ICON_PNG),
    })
}

fn choose(env: &dyn Fn(&str) -> Option<String>, colour: bool) -> Option<Kind> {
    match env("THADAM_BANNER").as_deref() {
        Some("off") => return None,
        Some("blocks") => return Some(Kind::Blocks),
        Some("image") => return Some(detect_image(env).unwrap_or(Kind::Kitty)),
        _ => {}
    }
    if !colour {
        return Some(Kind::Blocks);
    }
    if env("TMUX").is_some_and(|v| !v.is_empty())
        || env("TERM")
            .as_deref()
            .is_some_and(|t| t.starts_with("screen") || t.starts_with("tmux"))
    {
        return Some(Kind::Blocks);
    }
    Some(detect_image(env).unwrap_or(Kind::Blocks))
}

fn detect_image(env: &dyn Fn(&str) -> Option<String>) -> Option<Kind> {
    if env("KITTY_WINDOW_ID").is_some_and(|v| !v.is_empty())
        || matches!(
            env("TERM").as_deref(),
            Some("xterm-kitty" | "xterm-ghostty")
        )
        || matches!(env("TERM_PROGRAM").as_deref(), Some("ghostty" | "WezTerm"))
    {
        return Some(Kind::Kitty);
    }
    if env("TERM_PROGRAM").as_deref() == Some("iTerm.app") {
        return Some(Kind::Iterm);
    }
    None
}

fn blocks() -> String {
    format!(
        "████████{ORANGE}████{RESET}\n\
         ▀▀▀▀████{ORANGE}▀▀▀▀{RESET}   {BOLD}thadam{RESET}\n\
         \x20   ████       {TAGLINE}\n\
         \x20   ████\n\
         \x20   ████\n"
    )
}

fn kitty(png: &[u8]) -> String {
    let encoded = STANDARD.encode(png);
    let chunks: Vec<&[u8]> = encoded.as_bytes().chunks(4096).collect();
    let mut out = String::new();
    let last = chunks.len().saturating_sub(1);
    for (i, chunk) in chunks.iter().enumerate() {
        let more = u8::from(i < last);
        let payload = std::str::from_utf8(chunk).unwrap_or_default();
        if i == 0 {
            let _ = write!(out, "\x1b_Ga=T,f=100,c=10,r=5,m={more};{payload}\x1b\\");
        } else {
            let _ = write!(out, "\x1b_Gm={more};{payload}\x1b\\");
        }
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "{BOLD}thadam{RESET}");
    let _ = writeln!(out, "{TAGLINE}");
    out
}

fn iterm(png: &[u8]) -> String {
    let encoded = STANDARD.encode(png);
    let size = png.len();
    format!(
        "\x1b]1337;File=inline=1;size={size};width=10;height=5;preserveAspectRatio=1:{encoded}\x07\n\
         {BOLD}thadam{RESET}\n\
         {TAGLINE}\n"
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn env_from<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            vars.iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn embedded_icon_matches_canonical_brand_asset() {
        let canonical = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../thadam-assets/brand/png/thadam-icon-dark.png"
        ));
        if !canonical.exists() {
            return;
        }
        let expected = std::fs::read(canonical).expect("read canonical icon");
        assert_eq!(
            ICON_PNG,
            expected.as_slice(),
            "apps/thadam-cli/assets/thadam-icon-dark.png drifted; copy thadam-assets/brand/png/thadam-icon-dark.png again"
        );
    }

    #[test]
    fn blocks_without_ansi_matches_sketch() {
        let stripped = anstream::adapter::strip_str(&blocks()).to_string();
        let expected = concat!(
            "████████████\n",
            "▀▀▀▀████▀▀▀▀   thadam\n",
            "    ████       The open data plane for robotics.\n",
            "    ████\n",
            "    ████\n",
        );
        assert_eq!(stripped, expected);
    }

    #[test]
    fn kitty_chunks_and_round_trips_png() {
        let rendered = kitty(ICON_PNG);
        assert!(rendered.starts_with("\x1b_Ga=T,f=100,c=10,r=5,"));
        let (graphics, text) = rendered.split_once('\n').expect("newline after image");
        let frames: Vec<&str> = graphics.split("\x1b\\").filter(|s| !s.is_empty()).collect();
        assert!(!frames.is_empty());

        let mut combined = String::new();
        for (i, frame) in frames.iter().enumerate() {
            let body = frame.strip_prefix("\x1b_G").expect("APC prefix");
            let (meta, payload) = body.split_once(';').expect("meta;payload");
            assert!(payload.len() <= 4096);
            if i + 1 == frames.len() {
                assert!(meta.ends_with("m=0"), "last chunk meta: {meta}");
            } else {
                assert!(meta.ends_with("m=1"), "non-last chunk meta: {meta}");
            }
            combined.push_str(payload);
        }
        let decoded = STANDARD.decode(combined).expect("valid base64");
        assert_eq!(decoded, ICON_PNG);
        assert!(text.contains("thadam"));
        assert!(text.contains(TAGLINE));
    }

    #[test]
    fn iterm_includes_byte_size_and_text() {
        let rendered = iterm(ICON_PNG);
        assert!(rendered.contains(&format!("size={};", ICON_PNG.len())));
        assert!(rendered.contains("thadam"));
        assert!(rendered.contains(TAGLINE));
    }

    #[test]
    fn choose_respects_overrides_and_terminal_detection() {
        assert_eq!(choose(&env_from(&[("THADAM_BANNER", "off")]), true), None);
        assert_eq!(
            choose(
                &env_from(&[("THADAM_BANNER", "blocks"), ("KITTY_WINDOW_ID", "1")]),
                true
            ),
            Some(Kind::Blocks)
        );
        assert_eq!(
            choose(&env_from(&[("THADAM_BANNER", "image")]), false),
            Some(Kind::Kitty)
        );
        assert_eq!(
            choose(
                &env_from(&[("THADAM_BANNER", "image"), ("TERM_PROGRAM", "iTerm.app")]),
                false
            ),
            Some(Kind::Iterm)
        );
        assert_eq!(
            choose(
                &env_from(&[("TMUX", "/tmp/tmux-1000/default"), ("KITTY_WINDOW_ID", "1")]),
                true
            ),
            Some(Kind::Blocks)
        );
        assert_eq!(
            choose(&env_from(&[("TERM", "tmux-256color")]), true),
            Some(Kind::Blocks)
        );
        assert_eq!(
            choose(&env_from(&[("TERM", "screen-256color")]), true),
            Some(Kind::Blocks)
        );
        assert_eq!(
            choose(&env_from(&[("KITTY_WINDOW_ID", "1")]), true),
            Some(Kind::Kitty)
        );
        assert_eq!(
            choose(&env_from(&[("TERM", "xterm-kitty")]), true),
            Some(Kind::Kitty)
        );
        assert_eq!(
            choose(&env_from(&[("TERM", "xterm-ghostty")]), true),
            Some(Kind::Kitty)
        );
        assert_eq!(
            choose(&env_from(&[("TERM_PROGRAM", "ghostty")]), true),
            Some(Kind::Kitty)
        );
        assert_eq!(
            choose(&env_from(&[("TERM_PROGRAM", "WezTerm")]), true),
            Some(Kind::Kitty)
        );
        assert_eq!(
            choose(&env_from(&[("TERM_PROGRAM", "iTerm.app")]), true),
            Some(Kind::Iterm)
        );
        assert_eq!(
            choose(&env_from(&[("TERM_PROGRAM", "vscode")]), true),
            Some(Kind::Blocks)
        );
        assert_eq!(
            choose(&env_from(&[("KITTY_WINDOW_ID", "1")]), false),
            Some(Kind::Blocks)
        );
    }
}
