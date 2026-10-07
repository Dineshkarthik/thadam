//! Mangled input must produce diagnostics, never a panic, and the result must
//! stay consistent: a spec exists exactly when there are no errors.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;

use proptest::prelude::*;
use thadam_config::{CheckOptions, MapEnv, UnresolvedPolicy, check_str};

fn demo() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/demo/pipeline.yaml"),
    )
    .unwrap()
}

fn assert_consistent(yaml: &str) {
    let opts = CheckOptions::new().unresolved(UnresolvedPolicy::Placeholder);
    let checked = check_str(yaml, &MapEnv::new(), &opts);
    assert_eq!(checked.spec.is_some(), !checked.report.has_errors());
    for d in checked.report.errors.iter().chain(&checked.report.warnings) {
        assert!(d.code.starts_with("config."));
        assert!(!d.message.is_empty());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn arbitrary_text_never_panics(text in ".{0,400}") {
        assert_consistent(&text);
    }

    #[test]
    fn truncated_demo_never_panics(cut in 0usize..6000) {
        let demo = demo();
        let mut end = cut.min(demo.len());
        while !demo.is_char_boundary(end) {
            end -= 1;
        }
        assert_consistent(&demo[..end]);
    }

    #[test]
    fn demo_with_a_deleted_or_garbled_line_never_panics(line in 0usize..200, garble in "[ -~]{0,12}") {
        let demo = demo();
        let mut lines: Vec<String> = demo.lines().map(str::to_owned).collect();
        let i = line % lines.len();
        if garble.is_empty() {
            lines.remove(i);
        } else {
            lines[i].push_str(&garble);
        }
        assert_consistent(&lines.join("\n"));
    }
}

#[test]
fn the_demo_itself_is_consistent() {
    assert_consistent(&demo());
}
