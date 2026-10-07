//! A path-to-position index built from the low-level YAML event stream.
//!
//! Loading parses the document to a `serde_json::Value`, which has no
//! positions, so this pass walks the same text once more and records where each
//! mapping entry and list item starts. It also reports what the `Value` parse
//! would hide or accept: duplicate keys, anchors and aliases, merge keys, YAML
//! tags, and more than one document (ADR-0012, PR5 details 6 and 7).

use std::collections::{HashMap, HashSet};

use serde_saphyr::granit_parser::{Event, Parser};

use crate::diagnostics::LineCol;
use crate::path::{PathExt, PathVec, Seg};

/// Where a node starts: its key (or list item) and, for scalars, its value.
#[derive(Clone, Copy, Debug)]
struct Entry {
    key: LineCol,
    value: LineCol,
}

/// Which position of a node a diagnostic points at.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Anchor {
    /// The key, or the start of a list item. Used for unknown and missing keys.
    Key,
    /// The value, when it is a scalar; otherwise the key.
    Value,
}

#[derive(Debug, Default)]
pub(crate) struct SpanIndex {
    entries: HashMap<PathVec, Entry>,
    order: HashMap<PathVec, Vec<String>>,
    root: Option<LineCol>,
}

impl SpanIndex {
    /// The position of `path`, falling back to the nearest enclosing node.
    pub(crate) fn locate(&self, path: &[Seg], anchor: Anchor) -> Option<LineCol> {
        let mut p = path;
        loop {
            if let Some(e) = self.entries.get(p) {
                return Some(match anchor {
                    Anchor::Key => e.key,
                    Anchor::Value => e.value,
                });
            }
            match p.split_last() {
                Some((_, parent)) => p = parent,
                None => return self.root,
            }
        }
    }

    /// The keys of the mapping at `path`, in the order they were written.
    pub(crate) fn key_order(&self, path: &[Seg]) -> Option<&[String]> {
        self.order.get(path).map(Vec::as_slice)
    }
}

/// A problem the `Value` parse would not report on its own.
#[derive(Debug)]
pub(crate) struct Structural {
    pub path: PathVec,
    pub pos: LineCol,
    pub kind: StructuralKind,
}

#[derive(Debug)]
pub(crate) enum StructuralKind {
    DuplicateKey(String),
    Anchor,
    Alias,
    MergeKey,
    Tag(String),
    MultipleDocuments,
}

/// The YAML could not be tokenized or parsed.
#[derive(Debug)]
pub(crate) struct SyntaxError {
    pub pos: LineCol,
    pub message: String,
}

#[derive(Debug, Default)]
pub(crate) struct Scan {
    pub index: SpanIndex,
    pub structural: Vec<Structural>,
    pub empty: bool,
}

enum Frame {
    Map {
        path: PathVec,
        key: Option<(String, LineCol)>,
        seen: HashSet<String>,
        order: Vec<String>,
    },
    Seq {
        path: PathVec,
        next: usize,
    },
}

fn linecol(line: usize, col: usize) -> LineCol {
    LineCol {
        line: u32::try_from(line).unwrap_or(u32::MAX),
        column: u32::try_from(col + 1).unwrap_or(u32::MAX),
    }
}

const CORE_TAGS: [&str; 7] = ["bool", "float", "int", "map", "null", "seq", "str"];

fn tag_problem(tag: Option<&serde_saphyr::granit_parser::Tag>) -> Option<String> {
    let tag = tag?;
    let core = tag.handle() == "tag:yaml.org,2002:" && CORE_TAGS.contains(&tag.suffix());
    (!core).then(|| format!("{}{}", tag.handle(), tag.suffix()))
}

pub(crate) fn scan(src: &str) -> Result<Scan, SyntaxError> {
    let mut out = Scan {
        empty: true,
        ..Scan::default()
    };
    let mut stack: Vec<Frame> = Vec::new();
    let mut documents = 0usize;

    for item in Parser::new_from_str(src) {
        let (event, span) = item.map_err(|e| SyntaxError {
            pos: linecol(e.marker().line(), e.marker().col()),
            message: e.info(),
        })?;
        let pos = linecol(span.start.line(), span.start.col());
        let tag_pos = span.tag_start.map_or(pos, |m| linecol(m.line(), m.col()));

        let (is_scalar, is_container, anchor, tag) = match &event {
            Event::DocumentStart(..) => {
                documents += 1;
                if documents > 1 {
                    out.structural.push(Structural {
                        path: Vec::new(),
                        pos,
                        kind: StructuralKind::MultipleDocuments,
                    });
                    break;
                }
                continue;
            }
            Event::MappingEnd | Event::SequenceEnd => {
                if let Some(Frame::Map { path, order, .. }) = stack.pop() {
                    out.index.order.insert(path, order);
                }
                continue;
            }
            Event::Alias(_) => (false, false, 0, None),
            Event::Scalar(_, _, anchor, tag) => (true, false, *anchor, tag.as_deref()),
            Event::MappingStart(_, anchor, tag) | Event::SequenceStart(_, anchor, tag) => {
                (false, true, *anchor, tag.as_deref())
            }
            _ => continue,
        };
        out.empty = false;

        let (path, key_pos) = match stack.last_mut() {
            None => {
                out.index.root = Some(pos);
                (Vec::new(), pos)
            }
            Some(Frame::Seq { path, next }) => {
                let p = path.idx(*next);
                *next += 1;
                (p, pos)
            }
            Some(Frame::Map {
                path,
                key,
                seen,
                order,
            }) => match key.take() {
                None => {
                    let Event::Scalar(text, ..) = &event else {
                        return Err(SyntaxError {
                            pos,
                            message: "complex mapping keys are not supported".into(),
                        });
                    };
                    let text = text.to_string();
                    if !seen.insert(text.clone()) {
                        out.structural.push(Structural {
                            path: path.key(&text),
                            pos,
                            kind: StructuralKind::DuplicateKey(text.clone()),
                        });
                    } else {
                        order.push(text.clone());
                    }
                    if text == "<<" {
                        out.structural.push(Structural {
                            path: path.key(&text),
                            pos,
                            kind: StructuralKind::MergeKey,
                        });
                    }
                    if let Some(problem) = tag_problem(tag) {
                        out.structural.push(Structural {
                            path: path.key(&text),
                            pos: tag_pos,
                            kind: StructuralKind::Tag(problem),
                        });
                    }
                    *key = Some((text, pos));
                    continue;
                }
                Some((k, kp)) => (path.key(&k), kp),
            },
        };

        match &event {
            Event::Alias(_) => out.structural.push(Structural {
                path: path.clone(),
                pos,
                kind: StructuralKind::Alias,
            }),
            _ => {
                if anchor != 0 {
                    out.structural.push(Structural {
                        path: path.clone(),
                        pos,
                        kind: StructuralKind::Anchor,
                    });
                }
                if let Some(problem) = tag_problem(tag) {
                    out.structural.push(Structural {
                        path: path.clone(),
                        pos: tag_pos,
                        kind: StructuralKind::Tag(problem),
                    });
                }
            }
        }

        // A repeated key keeps its first position so diagnostics point at the original.
        out.index.entries.entry(path.clone()).or_insert(Entry {
            key: key_pos,
            value: if is_scalar { pos } else { key_pos },
        });

        if is_container {
            match &event {
                Event::MappingStart(..) => stack.push(Frame::Map {
                    path,
                    key: None,
                    seen: HashSet::new(),
                    order: Vec::new(),
                }),
                _ => stack.push(Frame::Seq { path, next: 0 }),
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(keys: &[&str]) -> PathVec {
        keys.iter().map(|k| Seg::Key((*k).to_owned())).collect()
    }

    #[test]
    fn indexes_keys_values_and_items() {
        let src = "a:\n  b: 1\n  c:\n    - x\n    - { d: 2 }\n";
        let scan = scan(src).unwrap();
        let idx = &scan.index;
        assert_eq!(
            idx.locate(&p(&["a"]), Anchor::Key),
            Some(LineCol { line: 1, column: 1 })
        );
        assert_eq!(
            idx.locate(&p(&["a", "b"]), Anchor::Key),
            Some(LineCol { line: 2, column: 3 })
        );
        assert_eq!(
            idx.locate(&p(&["a", "b"]), Anchor::Value),
            Some(LineCol { line: 2, column: 6 })
        );
        let item = p(&["a", "c"]).idx(1);
        assert_eq!(
            idx.locate(&item, Anchor::Key),
            Some(LineCol { line: 5, column: 7 })
        );
        assert_eq!(
            idx.locate(&item.key("d"), Anchor::Value),
            Some(LineCol {
                line: 5,
                column: 12
            })
        );
        assert_eq!(
            idx.locate(&p(&["a", "missing"]), Anchor::Key),
            Some(LineCol { line: 1, column: 1 })
        );
        assert_eq!(idx.key_order(&p(&["a"])).unwrap(), ["b", "c"]);
    }

    #[test]
    fn reports_structural_problems() {
        let src = "a: 1\na: 2\nb: &x 3\nc: *x\n<<: {}\nd: !custom 4\n";
        let scan = scan(src).unwrap();
        let kinds: Vec<_> = scan
            .structural
            .iter()
            .map(|s| format!("{:?}@{}", s.kind, s.pos))
            .collect();
        assert_eq!(
            kinds,
            [
                "DuplicateKey(\"a\")@2:1",
                "Anchor@3:7",
                "Alias@4:4",
                "MergeKey@5:1",
                "Tag(\"!custom\")@6:4"
            ]
        );
    }

    #[test]
    fn second_document_and_empty_input() {
        let scan1 = scan("a: 1\n---\nb: 2\n").unwrap();
        assert!(matches!(
            scan1.structural[0].kind,
            StructuralKind::MultipleDocuments
        ));
        assert!(scan("").unwrap().empty);
        assert!(scan("# only a comment\n").unwrap().empty);
    }

    #[test]
    fn syntax_errors_carry_a_position() {
        let err = scan("a: [1, 2\nb: 3\n").unwrap_err();
        assert!(err.pos.line >= 1);
    }
}
