//! Guards the D8 escape hatch: `Record` stays opaque so that moving from
//! representation B (shared `Arc<Shape>`) to C (per-chain shape ids) only
//! touches the name-based accessors.
//!
//! These tests read source files because the properties are about what the
//! code exposes, not how it behaves.
#![cfg(not(loom))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn strip_comments(src: &str) -> String {
    src.lines()
        .map(|l| l.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n")
}

fn braced_body(src: &str, header: &str) -> String {
    let start = src
        .find(header)
        .unwrap_or_else(|| panic!("`{header}` not found"));
    let rest = &src[start + header.len()..];
    let open = rest.find('{').expect("opening brace");
    let mut depth = 0usize;
    for (i, c) in rest[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return rest[open + 1..open + i].to_string();
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces after `{header}`");
}

#[test]
fn record_has_no_public_fields() {
    let src = strip_comments(
        &fs::read_to_string(workspace_root().join("crates/thadam-core/src/record.rs")).unwrap(),
    );
    let body = braced_body(&src, "pub struct Record");
    for line in body.lines().map(str::trim).filter(|l| !l.is_empty()) {
        assert!(
            !line.starts_with("pub"),
            "Record exposes a public field: `{line}`"
        );
    }
    assert!(
        body.contains("shape") && body.contains("values"),
        "the scan found the wrong struct"
    );
}

#[test]
fn record_signatures_never_hand_out_an_arc_or_the_value_container() {
    let src = strip_comments(
        &fs::read_to_string(workspace_root().join("crates/thadam-core/src/record.rs")).unwrap(),
    );
    let impl_start = src.find("impl Record {").expect("impl Record");
    let impl_body = &src[impl_start
        ..src[impl_start..]
            .find("\nimpl PartialEq")
            .map_or(src.len(), |e| impl_start + e)];
    for line in impl_body
        .lines()
        .filter(|l| l.trim_start().starts_with("pub fn"))
    {
        assert!(!line.contains("Arc<"), "Record API leaks an Arc: `{line}`");
        assert!(
            !line.contains("Box<[Value]>"),
            "Record API leaks its storage: `{line}`"
        );
        assert!(
            !line.contains("&Vec<Value>"),
            "Record API leaks its storage: `{line}`"
        );
    }
}

#[test]
fn shape_ref_does_not_expose_the_arc_or_pointer_equality() {
    let src = strip_comments(
        &fs::read_to_string(workspace_root().join("crates/thadam-core/src/shape.rs")).unwrap(),
    );
    let body = braced_body(&src, "pub struct ShapeRegistry");
    assert!(!body.contains("ptr_eq"));
    let shape_ref = &src[src.find("pub struct ShapeRef").expect("ShapeRef")..];
    let decl = shape_ref.lines().next().unwrap();
    assert!(
        !decl.contains("pub Arc"),
        "ShapeRef's field must be private: `{decl}`"
    );
    assert!(
        !src.contains("pub fn as_arc")
            && !src.contains("pub fn arc(")
            && !src.contains("-> &Arc<Shape>"),
        "ShapeRef must not hand out its Arc"
    );
}

#[test]
fn no_code_in_the_workspace_compares_shape_pointers() {
    let mut files = Vec::new();
    for dir in ["crates", "apps", "tools", "tests"] {
        rust_files(&workspace_root().join(dir), &mut files);
    }
    assert!(!files.is_empty());
    let this_file = Path::new(file!()).file_name().unwrap().to_owned();
    for file in files {
        if file.file_name() == Some(&this_file) {
            continue;
        }
        let src = strip_comments(&fs::read_to_string(&file).unwrap());
        for (n, line) in src.lines().enumerate() {
            let lower = line.to_ascii_lowercase();
            let compares_pointers = lower.contains("ptr_eq") && lower.contains("shape");
            assert!(
                !compares_pointers,
                "{}:{}: compares shape pointers: `{}`",
                file.display(),
                n + 1,
                line.trim()
            );
        }
    }
}
