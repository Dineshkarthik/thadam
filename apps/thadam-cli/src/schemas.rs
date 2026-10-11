//! Schema text for `thadam export`. The WAL keeps each message's schema name and
//! encoding but not the schema text, so the text is read from the MCAP files
//! that the configuration's `kind: mcap` sources point at, then from any files
//! named with `--schemas`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use mcap::sans_io::summary_reader::{SummaryReadEvent, SummaryReader};
use thadam_config::PipelineSpec;
use thadam_mcap::McapSourceFactory;

/// Schema text keyed by `(schema name, schema encoding)`.
#[derive(Debug, Default)]
pub struct SchemaSet {
    schemas: BTreeMap<(String, String), Bytes>,
}

impl SchemaSet {
    /// The schema text for `name` in `encoding`, for the exporter.
    pub fn into_resolver(self) -> impl Fn(&str, &str) -> Option<Bytes> + 'static {
        move |name: &str, encoding: &str| {
            self.schemas
                .get(&(name.to_owned(), encoding.to_owned()))
                .cloned()
        }
    }

    fn add(&mut self, from: &Path, found: Vec<Schema>, err: &mut dyn Write) {
        for (name, encoding, data) in found {
            match self.schemas.get(&(name.clone(), encoding.clone())) {
                None => {
                    self.schemas.insert((name, encoding), data);
                }
                Some(existing) if *existing != data => {
                    let _ = writeln!(
                        err,
                        "warning: schema `{name}` ({encoding}) in {} differs from an earlier file; keeping the earlier one",
                        from.display()
                    );
                }
                Some(_) => {}
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.schemas.len()
    }
}

type Schema = (String, String, Bytes);

/// Loads the schemas of the configuration's `kind: mcap` sources and of
/// `extra`. The files named with `--schemas` are read first, so they win over
/// the same schema in a configured file. A configured file that is missing or
/// unreadable is skipped with a warning on `err`; an `extra` file that cannot be
/// read is an error.
pub fn load(
    spec: &PipelineSpec,
    extra: &[PathBuf],
    err: &mut dyn Write,
) -> Result<SchemaSet, String> {
    let mut set = SchemaSet::default();
    for file in extra {
        let found = read_schemas(file)?;
        set.add(file, found, err);
    }

    let mut configured = BTreeSet::new();
    for source in spec
        .sources
        .iter()
        .filter(|s| s.kind == McapSourceFactory::KIND)
    {
        for path in configured_paths(&source.config) {
            match expand(&path) {
                Ok(files) => configured.extend(files),
                Err(e) => {
                    let _ = writeln!(
                        err,
                        "warning: skipping schemas from `{}` (source `{}`): {e}",
                        path.display(),
                        source.id
                    );
                }
            }
        }
    }
    for file in configured {
        match read_schemas(&file) {
            Ok(found) => set.add(&file, found, err),
            Err(why) => {
                let _ = writeln!(err, "warning: skipping schemas: {why}");
            }
        }
    }
    Ok(set)
}

fn configured_paths(config: &serde_json::Value) -> Vec<PathBuf> {
    config
        .get("paths")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(PathBuf::from)
        .collect()
}

/// A directory is scanned one level deep for `*.mcap`, as the `mcap` source
/// does; a file is taken as named.
fn expand(path: &Path) -> io::Result<Vec<PathBuf>> {
    if !fs::metadata(path)?.is_dir() {
        return Ok(vec![path.to_path_buf()]);
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let is_mcap = entry
            .path()
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("mcap"));
        if is_mcap && entry.file_type()?.is_file() {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// The schemas in an MCAP file's summary section, read through a seeking
/// reader so large recordings are never loaded whole.
fn read_schemas(path: &Path) -> Result<Vec<Schema>, String> {
    let fail = |what: &str, e: &dyn std::fmt::Display| format!("{what} {}: {e}", path.display());
    let mut file = File::open(path).map_err(|e| fail("cannot open", &e))?;
    let mut reader = SummaryReader::new();
    while let Some(event) = reader.next_event() {
        match event.map_err(|e| fail("cannot read", &e))? {
            SummaryReadEvent::ReadRequest(need) => {
                let read = file
                    .read(reader.insert(need))
                    .map_err(|e| fail("cannot read", &e))?;
                reader.notify_read(read);
            }
            SummaryReadEvent::SeekRequest(to) => {
                let at = file.seek(to).map_err(|e| fail("cannot seek", &e))?;
                reader.notify_seeked(at);
            }
        }
    }
    let summary = reader.finish().ok_or_else(|| {
        format!(
            "{} has no summary section, so its schemas cannot be read",
            path.display()
        )
    })?;
    let mut found: Vec<Schema> = summary
        .schemas
        .values()
        .filter(|s| !s.name.is_empty())
        .map(|s| {
            (
                s.name.clone(),
                s.encoding.clone(),
                Bytes::copy_from_slice(&s.data),
            )
        })
        .collect();
    found.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::BufWriter;

    use mcap::records::MessageHeader;
    use thadam_testkit::TempDataDir;

    use super::*;

    fn write_mcap(path: &Path, schema: (&str, &str, &[u8])) {
        let file = File::create(path).unwrap();
        let mut writer = mcap::Writer::new(BufWriter::new(file)).unwrap();
        let (name, encoding, data) = schema;
        let schema_id = writer.add_schema(name, encoding, data).unwrap();
        let channel = writer
            .add_channel(schema_id, "/t", "cdr", &BTreeMap::new())
            .unwrap();
        let header = MessageHeader {
            channel_id: channel,
            sequence: 0,
            log_time: 1,
            publish_time: 1,
        };
        writer.write_to_known_channel(&header, b"x").unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn expand_scans_a_directory_one_level_for_mcap_files() {
        let dir = TempDataDir::new().unwrap();
        write_mcap(&dir.join("b.mcap"), ("x", "ros2msg", &b"a"[..]));
        write_mcap(&dir.join("a.MCAP"), ("x", "ros2msg", &b"a"[..]));
        fs::write(dir.join("notes.txt"), b"no").unwrap();
        fs::create_dir(dir.join("nested")).unwrap();
        write_mcap(
            &dir.join("nested").join("c.mcap"),
            ("x", "ros2msg", &b"a"[..]),
        );

        let found = expand(dir.path()).unwrap();
        let names: Vec<String> = found
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["a.MCAP", "b.mcap"]);
    }

    #[test]
    fn schemas_are_keyed_by_name_and_encoding() {
        let dir = TempDataDir::new().unwrap();
        let path = dir.join("bag.mcap");
        write_mcap(&path, ("sensor/Joint", "ros2msg", &b"float64 p\n"[..]));

        let found = read_schemas(&path).unwrap();
        assert_eq!(
            found,
            vec![(
                "sensor/Joint".to_owned(),
                "ros2msg".to_owned(),
                Bytes::from_static(b"float64 p\n"),
            )]
        );

        let mut set = SchemaSet::default();
        set.add(&path, found, &mut Vec::new());
        let resolve = set.into_resolver();
        assert_eq!(
            resolve("sensor/Joint", "ros2msg"),
            Some(Bytes::from_static(b"float64 p\n"))
        );
        assert_eq!(resolve("sensor/Joint", "jsonschema"), None);
        assert_eq!(resolve("sensor/Other", "ros2msg"), None);
    }

    #[test]
    fn a_missing_configured_file_is_a_warning_not_an_error() {
        let dir = TempDataDir::new().unwrap();
        let gone = dir.join("gone.mcap");
        let spec = thadam_config::load_str(
            &format!(
                r#"version: 1
pipeline: {{ name: p, node_id: n, data_dir: "{}" }}
sources:
  - {{ id: bag, kind: mcap, config: {{ paths: ["{}"] }} }}
wal: {{ max_disk: 64MiB, on_full: block }}
datasets:
  - id: d
    match: {{ source: bag }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
sinks:
  - id: pq
    kind: parquet
    config: {{ datasets: [d] }}
"#,
                dir.path().display(),
                gone.display()
            ),
            &thadam_config::MapEnv::new(),
        )
        .unwrap();
        let mut err = Vec::new();
        let set = load(&spec, &[], &mut err).unwrap();
        assert_eq!(set.len(), 0);
        let warning = String::from_utf8(err).unwrap();
        assert!(warning.contains("warning: skipping schemas"), "{warning}");
        assert!(warning.contains("gone.mcap"), "{warning}");
    }

    #[test]
    fn an_unreadable_schemas_file_is_an_error() {
        let dir = TempDataDir::new().unwrap();
        let err = read_schemas(&dir.join("missing.mcap")).unwrap_err();
        assert!(err.contains("missing.mcap"), "{err}");
    }
}
