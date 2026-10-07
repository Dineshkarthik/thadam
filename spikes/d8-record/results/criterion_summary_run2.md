## Criterion summary (median ns per element, ± half-width of 95% CI of the median)

### Reference: serde_json::from_slice -> Value (parse only, no record construction)

| wl | ns/event |
|---|---|
| w1 | 255.5 ±3.6 |
| w2 | 708.0 ±30.7 |

### Construction from parsed serde_json::Value (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 162.2 ±3.3 | 102.1 ±1.6 | 100.1 ±1.3 | 101.5 ±2.8 |
| w1 | ext | 162.8 ±4.4 | 101.8 ±1.6 | 100.8 ±1.9 | 103.5 ±1.4 |
| w2 | min | 94.8 ±4.5 | 67.7 ±0.9 | 72.4 ±3.4 | 64.2 ±1.8 |
| w2 | ext | 88.1 ±1.5 | 65.6 ±1.1 | 62.6 ±1.7 | 62.0 ±0.9 |

### Field lookup: one field (last numeric field) (ns/lookup)

| wl | values | A-fresh by-name | A-interned by-name | B by-name | B cached-idx | C by-name | C cached-idx |
|---|---|---|---|---|---|---|---|
| w1 | min | 5.9 ±0.1 | 5.8 ±0.1 | 6.1 ±0.2 | 0.5 ±0.0 | 6.0 ±0.2 | 0.5 ±0.0 |
| w1 | ext | 5.8 ±0.1 | 5.9 ±0.1 | 5.8 ±0.1 | 0.5 ±0.0 | 6.1 ±0.1 | 0.5 ±0.0 |
| w2 | min | 2.8 ±0.1 | 2.8 ±0.1 | 2.6 ±0.0 | 0.5 ±0.0 | 2.7 ±0.1 | 0.5 ±0.0 |
| w2 | ext | 2.8 ±0.0 | 2.8 ±0.1 | 2.6 ±0.0 | 0.5 ±0.0 | 2.8 ±0.0 | 0.5 ±0.0 |

### Field lookup: every field of the record (ns/lookup, W1: 7 fields, W2: 3 fields)

| wl | values | A-fresh by-name | A-interned by-name | B by-name | B cached-idx | C by-name | C cached-idx |
|---|---|---|---|---|---|---|---|
| w1 | min | 4.2 ±0.1 | 4.2 ±0.1 | 4.0 ±0.1 | 0.3 ±0.0 | 4.7 ±0.1 | 0.3 ±0.0 |
| w1 | ext | 4.6 ±0.1 | 4.3 ±0.1 | 4.0 ±0.1 | 0.3 ±0.0 | 4.5 ±0.1 | 0.3 ±0.0 |
| w2 | min | 2.8 ±0.1 | 2.9 ±0.1 | 2.9 ±0.0 | 0.4 ±0.0 | 2.8 ±0.1 | 0.4 ±0.0 |
| w2 | ext | 2.9 ±0.1 | 3.0 ±0.0 | 2.9 ±0.1 | 0.4 ±0.0 | 2.8 ±0.1 | 0.4 ±0.0 |

### Transform pass: rename + unit conversion + filter (ns/event, uniquely owned input)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 127.5 ±0.8 | 44.9 ±0.9 | 12.0 ±0.3 | 12.2 ±0.2 |
| w1 | ext | 132.3 ±2.9 | 46.4 ±1.0 | 12.9 ±0.4 | 13.0 ±0.1 |
| w2 | min | 118.3 ±4.9 | 57.8 ±2.4 | 41.6 ±1.1 | 40.5 ±1.4 |
| w2 | ext | 94.1 ±1.6 | 40.8 ±0.7 | 22.4 ±0.5 | 21.7 ±0.5 |

### Clone (ns/event; includes allocation, drop is outside the timer)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 20.2 ±0.3 | 21.0 ±1.2 | 17.0 ±0.8 | 16.8 ±0.5 |
| w1 | ext | 23.1 ±1.3 | 21.9 ±0.7 | 16.4 ±0.4 | 16.1 ±0.5 |
| w2 | min | 59.0 ±1.0 | 64.7 ±4.4 | 61.3 ±1.6 | 59.8 ±2.6 |
| w2 | ext | 13.3 ±0.1 | 14.1 ±0.3 | 13.1 ±0.4 | 11.4 ±0.3 |

### Drop of uniquely owned events (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 78.8 ±1.2 | 22.5 ±0.4 | 19.1 ±0.6 | 19.1 ±0.3 |
| w1 | ext | 80.2 ±1.2 | 21.3 ±0.5 | 18.6 ±0.2 | 18.3 ±0.3 |
| w2 | min | 77.7 ±0.8 | 52.0 ±1.2 | 49.1 ±0.8 | 50.9 ±0.8 |
| w2 | ext | 55.9 ±2.0 | 33.6 ±1.2 | 33.4 ±0.7 | 30.5 ±0.6 |

### Conversion of 10,000 events to an Arrow RecordBatch (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 42.7 ±0.4 | 41.8 ±0.9 | 16.8 ±0.3 | 16.5 ±0.4 |
| w1 | ext | 42.1 ±1.7 | 43.3 ±0.9 | 17.4 ±0.4 | 16.4 ±0.2 |
| w2 | min | 32.9 ±0.5 | 33.1 ±1.0 | 26.8 ±0.4 | 28.6 ±1.0 |
| w2 | ext | 27.4 ±1.0 | 26.9 ±0.5 | 19.5 ±0.5 | 18.0 ±0.4 |

### Derived: construct + transform + arrow + drop (ns/event)

Sum of the independent medians above. It is an indicative per-event total for the decode-to-Arrow path, not a separately measured pipeline: it ignores that the filter removes ~30% of events before Arrow conversion and that real decode starts from WAL bytes rather than a parsed serde_json::Value.

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 411 | 211 | 148 | 149 |
| w1 | ext | 417 | 213 | 150 | 151 |
| w2 | min | 324 | 211 | 190 | 184 |
| w2 | ext | 265 | 167 | 138 | 132 |

