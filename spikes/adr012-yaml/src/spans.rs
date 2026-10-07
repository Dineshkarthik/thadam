//! Proof of concept: a path -> source position index built from the low-level
//! `granit-parser` event stream, independent of whichever serde crate produces the
//! typed value. Paths use the `serde_path_to_error` display format (`a.b[3].c`).
//! For a mapping entry the position is the key; for a sequence item, the item start.

use serde_saphyr::granit_parser::{Event, Parser};
use std::collections::HashMap;

enum Frame {
    Map {
        path: String,
        key: Option<String>,
        key_pos: (usize, usize),
    },
    Seq {
        path: String,
        idx: usize,
    },
}

pub fn index(src: &str) -> Result<HashMap<String, (usize, usize)>, String> {
    let mut out = HashMap::new();
    let mut stack: Vec<Frame> = Vec::new();
    for item in Parser::new_from_str(src) {
        let (ev, span) = item.map_err(|e| e.to_string())?;
        let pos = (span.start.line(), span.start.col() + 1);
        let (is_start, is_end) = match &ev {
            Event::Scalar(..) | Event::Alias(_) => (false, false),
            Event::MappingStart(..) | Event::SequenceStart(..) => (true, false),
            Event::MappingEnd | Event::SequenceEnd => (false, true),
            _ => continue,
        };
        if is_end {
            stack.pop();
            continue;
        }
        let path = match stack.last_mut() {
            None => ".".to_string(),
            Some(Frame::Seq { path, idx }) => {
                let p = format!("{path}[{idx}]");
                *idx += 1;
                out.insert(p.clone(), pos);
                p
            }
            Some(Frame::Map { path, key, key_pos }) => match key.take() {
                None => {
                    if let Event::Scalar(s, ..) = &ev {
                        *key = Some(s.to_string());
                        *key_pos = pos;
                    } else {
                        *key = Some(String::from("<complex-key>"));
                        *key_pos = pos;
                    }
                    continue;
                }
                Some(k) => {
                    let p = if path.is_empty() || path == "." {
                        k
                    } else {
                        format!("{path}.{k}")
                    };
                    out.insert(p.clone(), *key_pos);
                    p
                }
            },
        };
        if is_start {
            let base = if path == "." { String::new() } else { path };
            match ev {
                Event::MappingStart(..) => stack.push(Frame::Map {
                    path: base,
                    key: None,
                    key_pos: (0, 0),
                }),
                _ => stack.push(Frame::Seq { path: base, idx: 0 }),
            }
        }
    }
    Ok(out)
}
