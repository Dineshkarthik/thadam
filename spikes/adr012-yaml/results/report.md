

## A. Demo pipeline into PipelineSpec
- serde_norway   OK: 1 sources, 2 datasets, 3 transforms, 2 sinks; canonical json 3619 bytes
- serde_yaml_ng  OK: 1 sources, 2 datasets, 3 transforms, 2 sinks; canonical json 3619 bytes
- serde-saphyr   OK: 1 sources, 2 datasets, 3 transforms, 2 sinks; canonical json 3619 bytes
- all three produce identical canonical JSON: true


## B. Malformed variants of the demo (typed PipelineSpec; location = line:col, 1-based)

### B1 misspelled top-level key (`wal:` -> `wall:`)
- serde_norway   ERR at 20:1 path=`wall` :: unknown field `wall`, expected one of `version`, `pipeline`, `sources`, `wal`, `quarantine`, `datasets`, `transforms`, `sinks`, `runtime`, `observability` at line 20 column 1
- serde_yaml_ng  ERR at 20:1 path=`wall` :: unknown field `wall`, expected one of `version`, `pipeline`, `sources`, `wal`, `quarantine`, `datasets`, `transforms`, `sinks`, `runtime`, `observability` at line 20 column 1
- serde-saphyr   ERR at 20:1 path=`wall` :: unknown field `wall`, expected one of version, pipeline, sources, wal, quarantine, datasets, transforms, sinks, runtime, observability at line 20, column 1

### B2 unknown key nested in a list item (`from:` -> `fromm:` in a dataset field)
- serde_norway   ERR at 39:48 path=`datasets[0].fields[1].fromm` :: datasets[0].fields[1]: unknown field `fromm`, expected one of `name`, `type`, `from`, `required`, `unit`, `convert_to` at line 39 column 48
- serde_yaml_ng  ERR at 39:48 path=`datasets[0].fields[1].fromm` :: datasets[0].fields[1]: unknown field `fromm`, expected one of `name`, `type`, `from`, `required`, `unit`, `convert_to` at line 39 column 48
- serde-saphyr   ERR at 39:48 path=`datasets[0].fields[1].fromm` :: unknown field `fromm`, expected one of name, type, from, required, unit, convert_to at line 39, column 48

### B3 wrong type (`worker_threads: two`)
- serde_norway   ERR at 100:28 path=`runtime.worker_threads` :: runtime.worker_threads: invalid type: string "two", expected u32 at line 100 column 28
- serde_yaml_ng  ERR at 100:28 path=`runtime.worker_threads` :: runtime.worker_threads: invalid type: string "two", expected u32 at line 100 column 28
- serde-saphyr   ERR at 100:28 path=`runtime.worker_threads` :: invalid u32 at line 100, column 28

### B4 wrong type, sequence where scalar expected (`on_full: [block]`)
- serde_norway   ERR at 24:12 path=`wal.on_full` :: wal.on_full: invalid type: sequence, expected a YAML tag starting with '!' at line 24 column 12
- serde_yaml_ng  ERR at 24:12 path=`wal.on_full` :: wal.on_full: invalid type: sequence, expected a YAML tag starting with '!' at line 24 column 12
- serde-saphyr   ERR at 24:12 path=`wal.on_full` :: externally tagged enum expected scalar or mapping at line 24, column 12

### B5 missing required key (`on_full` removed)
- serde_norway   ERR at 21:3 path=`wal` :: wal: missing field `on_full` at line 21 column 3
- serde_yaml_ng  ERR at 21:3 path=`wal` :: wal: missing field `on_full` at line 21 column 3
- serde-saphyr   ERR at 24:3 path=`wal` :: missing field `on_full` at line 24, column 3

### B6 invalid enum value (`on_full: blok`)
- serde_norway   ERR at 24:12 path=`wal.on_full` :: wal.on_full: unknown variant `blok`, expected `block` or `drop_oldest` at line 24 column 12
- serde_yaml_ng  ERR at 24:12 path=`wal.on_full` :: wal.on_full: unknown variant `blok`, expected `block` or `drop_oldest` at line 24 column 12
- serde-saphyr   ERR at 24:12 path=`wal.on_full` :: unknown variant `blok`, expected one of block, drop_oldest at line 24, column 12

### B7 bad duration (`retain_for: 1hr30`)
- serde_norway   ERR at 21:3 path=`wal.retain_for` :: wal: bad duration "1hr30": time unit needed, for example 30sec or 30ms at line 21 column 3
- serde_yaml_ng  ERR at 21:3 path=`wal.retain_for` :: wal: bad duration "1hr30": time unit needed, for example 30sec or 30ms at line 21 column 3
- serde-saphyr   ERR at 25:15 path=`wal.retain_for` :: bad duration "1hr30": time unit needed, for example 30sec or 30ms at line 25, column 15

### B8 bad size (`max_disk: 2GiBB`)
- serde_norway   ERR at 21:3 path=`wal.max_disk` :: wal: bad size "2GiBB": couldn't parse "GiBB" into a known SI unit, Failed to parse unit "GiB..." at line 21 column 3
- serde_yaml_ng  ERR at 21:3 path=`wal.max_disk` :: wal: bad size "2GiBB": couldn't parse "GiBB" into a known SI unit, Failed to parse unit "GiB..." at line 21 column 3
- serde-saphyr   ERR at 21:13 path=`wal.max_disk` :: bad size "2GiBB": couldn't parse "GiBB" into a known SI unit, Failed to parse unit "GiB..." at line 21, column 13

### B9 bad count (`max_keys: 5oo`)
- serde_norway   ERR at 35:12 path=`datasets[0].dedup.max_keys` :: datasets[0].dedup: bad count "5oo": invalid digit found in string at line 35 column 12
- serde_yaml_ng  ERR at 35:12 path=`datasets[0].dedup.max_keys` :: datasets[0].dedup: bad count "5oo": invalid digit found in string at line 35 column 12
- serde-saphyr   ERR at 35:37 path=`datasets[0].dedup.max_keys` :: bad count "5oo": invalid digit found in string at line 35, column 37

### B10 duplicate key in a struct (`retain_for` twice)
- serde_norway   ERR at 21:3 path=`wal` :: wal: duplicate field `retain_for` at line 21 column 3
- serde_yaml_ng  ERR at 21:3 path=`wal` :: wal: duplicate field `retain_for` at line 21 column 3
- serde-saphyr   ERR at 26:3 path=`wal.?` :: duplicate mapping key: retain_for, set DuplicateKeyPolicy in Options if acceptable at line 26, column 3

### B11 unknown transform kind (`kind: validte`)
- serde_norway   ERR at 60:11 path=`transforms[0].kind` :: transforms[0].kind: unknown variant `validte`, expected one of `validate`, `on_change`, `filter` at line 60 column 11
- serde_yaml_ng  ERR at 60:11 path=`transforms[0].kind` :: transforms[0].kind: unknown variant `validte`, expected one of `validate`, `on_change`, `filter` at line 60 column 11
- serde-saphyr   ERR at 60:5 path=`transforms[0].kind` :: unknown variant `validte`, expected one of validate, on_change, filter at line 60, column 5

### B12 unknown key inside internally tagged variant (`expr:` -> `exprr:`)
- serde_norway   ERR at 60:3 path=`transforms[2]` :: transforms: unknown field `exprr`, expected `dataset` or `expr` at line 60 column 3
- serde_yaml_ng  ERR at 60:3 path=`transforms[2]` :: transforms: unknown field `exprr`, expected `dataset` or `expr` at line 60 column 3
- serde-saphyr   ERR at 67:5 path=`transforms[2]` :: unknown field `exprr`, expected one of dataset, expr at line 67, column 5

### B13 wrong type inside internally tagged variant (`heartbeat: [5m]`)
- serde_norway   ERR at 60:3 path=`transforms[1]` :: transforms: invalid type: sequence, expected a string at line 60 column 3
- serde_yaml_ng  ERR at 60:3 path=`transforms[1]` :: transforms: invalid type: sequence, expected a string at line 60 column 3
- serde-saphyr   ERR at 63:5 path=`transforms[1]` :: invalid type: sequence, expected a string at line 63, column 5

### B14 version as string (`version: "1"`)
- serde_norway   ERR at 1:10 path=`version` :: version: invalid type: string "1", expected u32 at line 1 column 10
- serde_yaml_ng  ERR at 1:10 path=`version` :: version: invalid type: string "1", expected u32 at line 1 column 10
- serde-saphyr   OK (3619 bytes json)

### B15 syntax error: unclosed flow mapping
- serde_norway   ERR at 103:8 path=`observability.logs.?` :: did not find expected ',' or '}' at line 103 column 8, while parsing a flow mapping at line 102 column 9
- serde_yaml_ng  ERR at 103:8 path=`observability.logs.?` :: did not find expected ',' or '}' at line 103 column 8, while parsing a flow mapping at line 102 column 9
- serde-saphyr   ERR at 103:8 path=`observability.logs.?` :: while parsing a flow mapping, did not find expected ',' or '}' at line 103, column 8

### B16 syntax error: tab indentation
- serde_norway   ERR at 3:1 path=`pipeline` :: found character that cannot start any token at line 3 column 1, while scanning for the next token
- serde_yaml_ng  ERR at 3:1 path=`pipeline` :: found character that cannot start any token at line 3 column 1, while scanning for the next token
- serde-saphyr   ERR at 3:2 path=`.` :: tabs disallowed within this context (block indentation) at line 3, column 2

### B17 syntax error: bad indentation inside sequence
- serde_norway   ERR at 8:5 path=`sources[0]` :: sources[0]: missing field `kind` at line 8 column 5
- serde_yaml_ng  ERR at 8:5 path=`sources[0]` :: sources[0]: missing field `kind` at line 8 column 5
- serde-saphyr   ERR at 8:5 path=`sources[0]` :: missing field `kind` at line 8, column 5

### B18 accidental second document (`---` before `sinks:`)
- serde_norway   ERR at 1:1 path=`.` :: missing field `sinks`
- serde_yaml_ng  ERR at 1:1 path=`.` :: missing field `sinks`
- serde-saphyr   ERR at 59:1 path=`.` :: missing field `sinks` at line 59, column 1

### B19 empty file
- serde_norway   ERR at 1:1 path=`.` :: missing field `version`
- serde_yaml_ng  ERR at 1:1 path=`.` :: missing field `version`
- serde-saphyr   ERR at 1:1 path=`.` :: unexpected end of input at line 1, column 1

### B20 unresolved interpolation placeholder is just a string (no YAML-level failure)
- serde_norway   OK (3590 bytes json)
- serde_yaml_ng  OK (3590 bytes json)
- serde-saphyr   OK (3590 bytes json)


## C. Error text with the crates' own rendering (no serde_path_to_error), case B2 and B13
- B2 serde_norway   native Display:
    datasets[0].fields[1]: unknown field `fromm`, expected one of `name`, `type`, `from`, `required`, `unit`, `convert_to` at line 39 column 48
- B2 serde_yaml_ng  native Display:
    datasets[0].fields[1]: unknown field `fromm`, expected one of `name`, `type`, `from`, `required`, `unit`, `convert_to` at line 39 column 48
- B2 serde-saphyr   native Display:
    error: line 39 column 48: unknown field `fromm`, expected one of name, type, from, required, unit, convert_to
      --> <input>:39:48
       |
    37 |     fields:
    38 |       - { name: seq,          type: u64,       from: /seq, required: true }
    39 |       - { name: soc_pct,      type: f32,       fromm: /soc_pct }
       |                                                ^ unknown field `fromm`, expected one of name, type, from, required, unit, convert_to
    40 |       - { name: voltage_v,    type: f32,       from: /voltage_mv,  unit: mV, convert_to: V }
    41 |       - { name: current_a,    type: f32,       from: /current_ma,  unit: mA, convert_to: A }
       |
- B13 serde_norway   native Display:
    transforms: invalid type: sequence, expected a string at line 60 column 3
- B13 serde_yaml_ng  native Display:
    transforms: invalid type: sequence, expected a string at line 60 column 3
- B13 serde-saphyr   native Display:
    error: line 63 column 5: invalid type: sequence, expected a string
      --> <input>:63:5
       |
    61 |     dataset: telemetry
    62 |     rules: [{ field: motor_temp_c, min: -40, max: 150, flag: OUT_OF_R…
    63 |   - kind: on_change
       |     ^ invalid type: sequence, expected a string
    64 |     dataset: state
    65 |     fields: [mode, cycle_id, program]
       |

saphyr with default options (snippet enabled), B2:
    error: line 39 column 48: unknown field `fromm`, expected one of name, type, from, required, unit, convert_to
      --> <input>:39:48
       |
    37 |     fields:
    38 |       - { name: seq,          type: u64,       from: /seq, required: true }
    39 |       - { name: soc_pct,      type: f32,       fromm: /soc_pct }
       |                                                ^ unknown field `fromm`, expected one of name, type, from, required, unit, convert_to
    40 |       - { name: voltage_v,    type: f32,       from: /voltage_mv,  unit: mV, convert_to: V }
    41 |       - { name: current_a,    type: f32,       from: /current_ma,  unit: mA, convert_to: A }
       |

saphyr with default options (snippet enabled), B7 (custom error from a Deserialize impl):
    error: line 25 column 15: bad duration "1hr30": time unit needed, for example 30sec or 30ms
      --> <input>:25:15
       |
    23 |   fsync: { interval: 50ms }
    24 |   on_full: block                          # explicit; see scenario F7 for drop_…
    25 |   retain_for: 1hr30                          # replay window, bounded by max_di…
       |               ^ bad duration "1hr30": time unit needed, for example 30sec or 30ms
    26 |
    27 | quarantine: { format: parquet, max_disk: 256MiB }
       |


## D. Multi-error collection: a fixture with three unrelated errors

### D1 three independent errors (misspelled key, bad duration, bad number)
- serde_norway   ERR at 20:1 path=`wall` :: unknown field `wall`, expected one of `version`, `pipeline`, `sources`, `wal`, `quarantine`, `datasets`, `transforms`, `sinks`, `runtime`, `observability` at line 20 column 1
- serde_yaml_ng  ERR at 20:1 path=`wall` :: unknown field `wall`, expected one of `version`, `pipeline`, `sources`, `wal`, `quarantine`, `datasets`, `transforms`, `sinks`, `runtime`, `observability` at line 20 column 1
- serde-saphyr   ERR at 20:1 path=`wall` :: unknown field `wall`, expected one of version, pipeline, sources, wal, quarantine, datasets, transforms, sinks, runtime, observability at line 20, column 1

### D2 five independent errors in five sections (single serde pass)
- serde_norway   ERR at 21:3 path=`wal.retain_for` :: wal: bad duration "1hr30": time unit needed, for example 30sec or 30ms at line 21 column 3
- serde_yaml_ng  ERR at 21:3 path=`wal.retain_for` :: wal: bad duration "1hr30": time unit needed, for example 30sec or 30ms at line 21 column 3
- serde-saphyr   ERR at 25:15 path=`wal.retain_for` :: bad duration "1hr30": time unit needed, for example 30sec or 30ms at line 25, column 15


## E. YAML 1.1 vs 1.2 scalar coercion (the Norway problem)

#### target `String (typed config field the user intends as text, e.g. client_id, site)`

| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |
|---|---|---|---|---|
| `no` | `"no"` | `"no"` | `"no"` | `"no"` |
| `No` | `"No"` | `"No"` | `"No"` | `"No"` |
| `NO` | `"NO"` | `"NO"` | `"NO"` | `"NO"` |
| `yes` | `"yes"` | `"yes"` | `"yes"` | `"yes"` |
| `on` | `"on"` | `"on"` | `"on"` | `"on"` |
| `off` | `"off"` | `"off"` | `"off"` | `"off"` |
| `y` | `"y"` | `"y"` | `"y"` | `"y"` |
| `n` | `"n"` | `"n"` | `"n"` | `"n"` |
| `true` | `"true"` | `"true"` | `"true"` | `"true"` |
| `null` | `"null"` | `"null"` | ERR: cannot deserialize null into string; use Option<String> at line 1, col... | ERR: cannot deserialize null into string; use Option<String> at line 1, col... |
| `~` | `"~"` | `"~"` | ERR: cannot deserialize null into string; use Option<String> at line 1, col... | ERR: cannot deserialize null into string; use Option<String> at line 1, col... |
| `12345` | `"12345"` | `"12345"` | `"12345"` | `"12345"` |
| `1e3` | `"1e3"` | `"1e3"` | `"1e3"` | `"1e3"` |
| `0x1F` | `"0x1F"` | `"0x1F"` | `"0x1F"` | `"0x1F"` |
| `014` | `"014"` | `"014"` | `"014"` | `"014"` |
| `0o14` | `"0o14"` | `"0o14"` | `"0o14"` | `"0o14"` |
| `1_000` | `"1_000"` | `"1_000"` | `"1_000"` | `"1_000"` |
| `12:30:45` | `"12:30:45"` | `"12:30:45"` | `"12:30:45"` | `"12:30:45"` |
| `1.10` | `"1.10"` | `"1.10"` | `"1.10"` | `"1.10"` |
| `.inf` | `".inf"` | `".inf"` | `".inf"` | `".inf"` |
| `2024-01-31` | `"2024-01-31"` | `"2024-01-31"` | `"2024-01-31"` | `"2024-01-31"` |
| `'no'` | `"no"` | `"no"` | `"no"` | `"no"` |
| `"no"` | `"no"` | `"no"` | `"no"` | `"no"` |

#### target `bool (typed config field)`

| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |
|---|---|---|---|---|
| `true` | `true` | `true` | `true` | `true` |
| `True` | `true` | `true` | `true` | `true` |
| `TRUE` | `true` | `true` | `true` | `true` |
| `false` | `false` | `false` | `false` | `false` |
| `no` | ERR: k: invalid type: string "no", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "no", expected a boolean at line 1 column 4 | `false` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `No` | ERR: k: invalid type: string "No", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "No", expected a boolean at line 1 column 4 | `false` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `yes` | ERR: k: invalid type: string "yes", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "yes", expected a boolean at line 1 column 4 | `true` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `on` | ERR: k: invalid type: string "on", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "on", expected a boolean at line 1 column 4 | `true` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `off` | ERR: k: invalid type: string "off", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "off", expected a boolean at line 1 column 4 | `false` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `y` | ERR: k: invalid type: string "y", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "y", expected a boolean at line 1 column 4 | `true` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `n` | ERR: k: invalid type: string "n", expected a boolean at line 1 column 4 | ERR: k: invalid type: string "n", expected a boolean at line 1 column 4 | `false` | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `1` | ERR: k: invalid type: integer `1`, expected a boolean at line 1 column 4 | ERR: k: invalid type: integer `1`, expected a boolean at line 1 column 4 | ERR: invalid boolean at line 1, column 4 | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |
| `0` | ERR: k: invalid type: integer `0`, expected a boolean at line 1 column 4 | ERR: k: invalid type: integer `0`, expected a boolean at line 1 column 4 | ERR: invalid boolean at line 1, column 4 | ERR: invalid boolean (strict mode expects true/false) at line 1, column 4 |

#### target `i64`

| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |
|---|---|---|---|---|
| `42` | `42` | `42` | `42` | `42` |
| `-7` | `-7` | `-7` | `-7` | `-7` |
| `0x1F` | `31` | `31` | `31` | `31` |
| `0o14` | `12` | `12` | `12` | `12` |
| `014` | ERR: k: invalid type: string "014", expected i64 at line 1 column 4 | ERR: k: invalid type: string "014", expected i64 at line 1 column 4 | ERR: invalid i64 at line 1, column 4 | ERR: invalid i64 at line 1, column 4 |
| `1_000` | ERR: k: invalid type: string "1_000", expected i64 at line 1 column 4 | ERR: k: invalid type: string "1_000", expected i64 at line 1 column 4 | `1000` | `1000` |
| `1e3` | ERR: k: invalid type: floating point `1000.0`, expected i64 at line 1 colum... | ERR: k: invalid type: floating point `1000.0`, expected i64 at line 1 colum... | ERR: invalid i64 at line 1, column 4 | ERR: invalid i64 at line 1, column 4 |
| `1.0` | ERR: k: invalid type: floating point `1.0`, expected i64 at line 1 column 4 | ERR: k: invalid type: floating point `1.0`, expected i64 at line 1 column 4 | ERR: invalid i64 at line 1, column 4 | ERR: invalid i64 at line 1, column 4 |
| `12:30:45` | ERR: k: invalid type: string "12:30:45", expected i64 at line 1 column 4 | ERR: k: invalid type: string "12:30:45", expected i64 at line 1 column 4 | ERR: invalid i64 at line 1, column 4 | ERR: invalid i64 at line 1, column 4 |
| `+5` | `5` | `5` | `5` | `5` |
| `0b101` | `5` | `5` | `5` | `5` |
| `'42'` | ERR: k: invalid type: string "42", expected i64 at line 1 column 4 | ERR: k: invalid type: string "42", expected i64 at line 1 column 4 | `42` | `42` |
| `42.0` | ERR: k: invalid type: floating point `42.0`, expected i64 at line 1 column ... | ERR: k: invalid type: floating point `42.0`, expected i64 at line 1 column ... | ERR: invalid i64 at line 1, column 4 | ERR: invalid i64 at line 1, column 4 |
| `9223372036854775808` | ERR: k: invalid type: integer `9223372036854775808`, expected i64 at line 1... | ERR: k: invalid type: integer `9223372036854775808`, expected i64 at line 1... | ERR: invalid i64 at line 1, column 4 | ERR: invalid i64 at line 1, column 4 |

#### target `f64`

| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |
|---|---|---|---|---|
| `1.5` | `1.5` | `1.5` | `1.5` | `1.5` |
| `1e3` | `1000.0` | `1000.0` | `1000.0` | `1000.0` |
| `1E3` | `1000.0` | `1000.0` | `1000.0` | `1000.0` |
| `.5` | `0.5` | `0.5` | `0.5` | `0.5` |
| `5.` | `5.0` | `5.0` | `5.0` | `5.0` |
| `.inf` | `inf` | `inf` | `inf` | `inf` |
| `-.Inf` | `-inf` | `-inf` | `-inf` | `-inf` |
| `.nan` | `NaN` | `NaN` | `NaN` | `NaN` |
| `0x1F` | ERR: k: invalid type: integer `31`, expected f64 at line 1 column 4 | ERR: k: invalid type: integer `31`, expected f64 at line 1 column 4 | ERR: invalid floating point at line 1, column 4 | ERR: invalid floating point at line 1, column 4 |
| `014` | `14.0` | `14.0` | `14.0` | `14.0` |
| `1_000` | ERR: k: invalid type: string "1_000", expected f64 at line 1 column 4 | ERR: k: invalid type: string "1_000", expected f64 at line 1 column 4 | ERR: invalid floating point at line 1, column 4 | ERR: invalid floating point at line 1, column 4 |
| `12:30:45` | ERR: k: invalid type: string "12:30:45", expected f64 at line 1 column 4 | ERR: k: invalid type: string "12:30:45", expected f64 at line 1 column 4 | ERR: invalid floating point at line 1, column 4 | ERR: invalid floating point at line 1, column 4 |
| `42` | `42.0` | `42.0` | `42.0` | `42.0` |
| `'1.5'` | ERR: k: invalid type: string "1.5", expected f64 at line 1 column 4 | ERR: k: invalid type: string "1.5", expected f64 at line 1 column 4 | `1.5` | `1.5` |

#### target `Option<String>`

| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |
|---|---|---|---|---|
| `null` | `None` | `None` | `None` | `None` |
| `Null` | `None` | `None` | `None` | `None` |
| `NULL` | `None` | `None` | `None` | `None` |
| `~` | `None` | `None` | `None` | `None` |
| `''` | `Some("")` | `Some("")` | `Some("")` | `Some("")` |
| `` | `None` | `None` | `None` | `None` |
| `nil` | `Some("nil")` | `Some("nil")` | `Some("nil")` | `Some("nil")` |
| `none` | `Some("none")` | `Some("none")` | `Some("none")` | `Some("none")` |

#### target `serde_json::Value (the D22 connector `config` boundary)`

| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |
|---|---|---|---|---|
| `no` | `String("no")` | `String("no")` | `Bool(false)` | `String("no")` |
| `yes` | `String("yes")` | `String("yes")` | `Bool(true)` | `String("yes")` |
| `on` | `String("on")` | `String("on")` | `Bool(true)` | `String("on")` |
| `off` | `String("off")` | `String("off")` | `Bool(false)` | `String("off")` |
| `y` | `String("y")` | `String("y")` | `Bool(true)` | `String("y")` |
| `n` | `String("n")` | `String("n")` | `Bool(false)` | `String("n")` |
| `true` | `Bool(true)` | `Bool(true)` | `Bool(true)` | `Bool(true)` |
| `True` | `Bool(true)` | `Bool(true)` | `Bool(true)` | `Bool(true)` |
| `null` | `Null` | `Null` | `Null` | `Null` |
| `~` | `Null` | `Null` | `Null` | `Null` |
| `12345` | `Number(12345)` | `Number(12345)` | `Number(12345)` | `Number(12345)` |
| `1e3` | `Number(1000.0)` | `Number(1000.0)` | `Number(1000.0)` | `Number(1000.0)` |
| `0x1F` | `Number(31)` | `Number(31)` | `Number(31)` | `Number(31)` |
| `014` | `String("014")` | `String("014")` | `Number(14.0)` | `Number(14.0)` |
| `0o14` | `Number(12)` | `Number(12)` | `Number(12)` | `Number(12)` |
| `1_000` | `String("1_000")` | `String("1_000")` | `Number(1000)` | `Number(1000)` |
| `12:30:45` | `String("12:30:45")` | `String("12:30:45")` | `String("12:30:45")` | `String("12:30:45")` |
| `1.10` | `Number(1.1)` | `Number(1.1)` | `Number(1.1)` | `Number(1.1)` |
| `2024-01-31` | `String("2024-01-31")` | `String("2024-01-31")` | `String("2024-01-31")` | `String("2024-01-31")` |
| `.inf` | `Null` | `Null` | ERR: non-finite float `.inf` rejected by reject_non_finite_typeless_float a... | ERR: non-finite float `.inf` rejected by reject_non_finite_typeless_float a... |
| `'no'` | `String("no")` | `String("no")` | `String("no")` | `String("no")` |


## F. Duplicate keys
- serde_norway   struct: ERR duplicate field `a` | serde_json::Value: OK Object {"a": Number(2)} | BTreeMap: OK {"a": 2}
- serde_yaml_ng  struct: ERR duplicate field `a` | serde_json::Value: OK Object {"a": Number(2)} | BTreeMap: OK {"a": 2}
- serde-saphyr   struct: ERR duplicate mapping key: a, set DuplicateKeyPolicy in Options if accepta... | serde_json::Value: ERR duplicate mapping key: a, set DuplicateKeyPolicy in Options if accepta... | BTreeMap: ERR duplicate mapping key: a, set DuplicateKeyPolicy in Options if accepta...
- serde-saphyr duplicate_keys option exists: Error (default) | FirstWins | LastWins; serde_norway/serde_yaml_ng have no option.


## G. Anchors, aliases and merge keys

### G1 merge key `<<: *base` into a struct with deny_unknown_fields
- serde_norway   ERR at 3:7 path=`fields[1].<<` :: fields[1]: unknown field `<<`, expected one of `name`, `type`, `from` at line 3 column 7
- serde_yaml_ng  ERR at 3:7 path=`fields[1].<<` :: fields[1]: unknown field `<<`, expected one of `name`, `type`, `from` at line 3 column 7
- serde-saphyr   OK (88 bytes json)
    serde-saphyr   fields[1] = Fld { name: "b", ty: "f32", from: "/b" }

### G2 plain alias to a whole mapping (`- *f`)
- serde_norway   OK (88 bytes json)
- serde_yaml_ng  OK (88 bytes json)
- serde-saphyr   OK (88 bytes json)

### G3 top-level `x-common:` anchor holder with deny_unknown_fields
- serde_norway   ERR at 1:1 path=`x-common` :: unknown field `x-common`, expected `fields`
- serde_yaml_ng  ERR at 1:1 path=`x-common` :: unknown field `x-common`, expected `fields`
- serde-saphyr   ERR at 1:1 path=`x-common` :: unknown field `x-common`, expected one of fields at line 1, column 1

### G4 merge key into serde_json::Value
- serde_norway   OK (118 bytes json)
- serde_yaml_ng  OK (118 bytes json)
- serde-saphyr   OK (88 bytes json)
    serde_norway   Value[fields][1] = {"<<":{"from":"/a","name":"a","type":"f32"},"from":"/b","name":"b"}
    serde_yaml_ng  Value[fields][1] = {"<<":{"from":"/a","name":"a","type":"f32"},"from":"/b","name":"b"}
    serde-saphyr   Value[fields][1] = {"from":"/b","name":"b","type":"f32"}

### G5 alias to an undefined anchor
- serde_norway   ERR at 2:5 path=`fields` :: unknown anchor at line 2 column 5
- serde_yaml_ng  ERR at 2:5 path=`fields` :: unknown anchor at line 2 column 5
- serde-saphyr   ERR at 2:5 path=`fields` :: alias references unknown anchor at line 2, column 5

### G6 unknown local/global tag on a mapping
- serde_norway   OK (50 bytes json)
- serde_yaml_ng  OK (50 bytes json)
- serde-saphyr   OK (50 bytes json)


## H. Path -> span index from the low-level parser (PoC, independent of the serde crate)

- B2 unknown key
    serde_norway   reported    39:48  path `datasets[0].fields[1].fromm` -> index    39:48
    serde_yaml_ng  reported    39:48  path `datasets[0].fields[1].fromm` -> index    39:48
    serde-saphyr   reported    39:48  path `datasets[0].fields[1].fromm` -> index    39:48

- B5 missing field
    serde_norway   reported     21:3  path `wal` -> index     20:1
    serde_yaml_ng  reported     21:3  path `wal` -> index     20:1
    serde-saphyr   reported     24:3  path `wal` -> index     20:1

- B7 bad duration
    serde_norway   reported     21:3  path `wal.retain_for` -> index     25:3
    serde_yaml_ng  reported     21:3  path `wal.retain_for` -> index     25:3
    serde-saphyr   reported    25:15  path `wal.retain_for` -> index     25:3

- B9 bad count
    serde_norway   reported    35:12  path `datasets[0].dedup.max_keys` -> index    35:27
    serde_yaml_ng  reported    35:12  path `datasets[0].dedup.max_keys` -> index    35:27
    serde-saphyr   reported    35:37  path `datasets[0].dedup.max_keys` -> index    35:27

- B12 unknown key in tagged variant
    serde_norway   reported     60:3  path `transforms[2]` -> index     67:5
    serde_yaml_ng  reported     60:3  path `transforms[2]` -> index     67:5
    serde-saphyr   reported     67:5  path `transforms[2]` -> index     67:5

- B13 wrong type in tagged variant
    serde_norway   reported     60:3  path `transforms[1]` -> index     63:5
    serde_yaml_ng  reported     60:3  path `transforms[1]` -> index     63:5
    serde-saphyr   reported     63:5  path `transforms[1]` -> index     63:5


## I. Multi-error collection PoC: parse to serde_json::Value, validate per section/item, spans from the index
- serde_norway   5 errors collected:
      105:1  wall2                              unknown top-level key `wall2`, expected one of version, pipeline, sour...
       25:3  wal.retain_for                     bad duration "1hr30": time unit needed, for example 30sec or 30ms
      39:48  datasets[0].fields[1].fromm        unknown field `fromm`, expected one of `name`, `type`, `from`, `requir...
       63:5  transforms[1]                      invalid type: sequence, expected a string
     100:12  runtime.worker_threads             invalid type: string "two", expected u32
- serde_yaml_ng  5 errors collected:
      105:1  wall2                              unknown top-level key `wall2`, expected one of version, pipeline, sour...
       25:3  wal.retain_for                     bad duration "1hr30": time unit needed, for example 30sec or 30ms
      39:48  datasets[0].fields[1].fromm        unknown field `fromm`, expected one of `name`, `type`, `from`, `requir...
       63:5  transforms[1]                      invalid type: sequence, expected a string
     100:12  runtime.worker_threads             invalid type: string "two", expected u32
- serde-saphyr   5 errors collected:
      105:1  wall2                              unknown top-level key `wall2`, expected one of version, pipeline, sour...
       25:3  wal.retain_for                     bad duration "1hr30": time unit needed, for example 30sec or 30ms
      39:48  datasets[0].fields[1].fromm        unknown field `fromm`, expected one of `name`, `type`, `from`, `requir...
       63:5  transforms[1]                      invalid type: sequence, expected a string
     100:12  runtime.worker_threads             invalid type: string "two", expected u32


## J. Duration, size and count scalars named in the ADR checklist
- serde_norway   OK d1=50ms d4(1d)=86400s s1=134217728 c1=500000
- serde_yaml_ng  OK d1=50ms d4(1d)=86400s s1=134217728 c1=500000
- serde-saphyr   OK d1=50ms d4(1d)=86400s s1=134217728 c1=500000


## K. Interaction with serde_path_to_error
- serde_norway   path=`transforms[1]` (expect transforms[1].heartbeat)
- serde_norway   unknown-field path=`datasets[0].fields[1].fromm`
- serde_yaml_ng  path=`transforms[1]` (expect transforms[1].heartbeat)
- serde_yaml_ng  unknown-field path=`datasets[0].fields[1].fromm`
- serde-saphyr   path=`transforms[1]` (expect transforms[1].heartbeat)
- serde-saphyr   unknown-field path=`datasets[0].fields[1].fromm`
