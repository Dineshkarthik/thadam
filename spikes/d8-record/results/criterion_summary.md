## Criterion summary (median ns per element, ± half-width of 95% CI of the median)

### Reference: serde_json::from_slice -> Value (parse only, no record construction)

| wl | ns/event |
|---|---|
| w1 | 250.3 ±2.3 |
| w2 | 723.9 ±26.3 |

### Construction from parsed serde_json::Value (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 161.6 ±1.9 | 100.6 ±2.1 | 96.9 ±1.6 | 101.0 ±2.5 |
| w1 | ext | 161.2 ±2.1 | 102.3 ±2.0 | 109.1 ±2.3 | 104.7 ±2.5 |
| w2 | min | 90.7 ±0.5 | 62.6 ±1.1 | 72.4 ±4.2 | 71.0 ±2.2 |
| w2 | ext | 88.1 ±1.3 | 57.5 ±0.4 | 57.8 ±0.5 | 57.2 ±1.9 |

### Field lookup: one field (last numeric field) (ns/lookup)

| wl | values | A-fresh by-name | A-interned by-name | B by-name | B cached-idx | C by-name | C cached-idx |
|---|---|---|---|---|---|---|---|
| w1 | min | 5.5 ±0.1 | 5.7 ±0.1 | 5.8 ±0.1 | 0.5 ±0.0 | 5.7 ±0.1 | 0.5 ±0.0 |
| w1 | ext | 5.9 ±0.1 | 5.6 ±0.1 | 5.8 ±0.1 | 0.6 ±0.0 | 5.9 ±0.1 | 0.5 ±0.0 |
| w2 | min | 2.8 ±0.0 | 2.8 ±0.0 | 2.6 ±0.0 | 0.6 ±0.0 | 2.7 ±0.0 | 0.5 ±0.0 |
| w2 | ext | 2.8 ±0.1 | 2.8 ±0.0 | 2.6 ±0.0 | 0.5 ±0.0 | 2.7 ±0.0 | 0.5 ±0.0 |

### Field lookup: every field of the record (ns/lookup, W1: 7 fields, W2: 3 fields)

| wl | values | A-fresh by-name | A-interned by-name | B by-name | B cached-idx | C by-name | C cached-idx |
|---|---|---|---|---|---|---|---|
| w1 | min | 4.3 ±0.1 | 4.3 ±0.1 | 3.7 ±0.0 | 0.3 ±0.0 | 4.2 ±0.1 | 0.3 ±0.0 |
| w1 | ext | 4.1 ±0.1 | 4.1 ±0.1 | 4.2 ±0.1 | 0.3 ±0.0 | 4.7 ±0.1 | 0.3 ±0.0 |
| w2 | min | 2.9 ±0.1 | 2.8 ±0.0 | 2.9 ±0.0 | 0.4 ±0.0 | 2.7 ±0.0 | 0.3 ±0.0 |
| w2 | ext | 2.9 ±0.0 | 2.9 ±0.0 | 2.9 ±0.1 | 0.3 ±0.0 | 2.8 ±0.0 | 0.3 ±0.0 |

### Transform pass: rename + unit conversion + filter (ns/event, uniquely owned input)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 126.1 ±1.9 | 43.8 ±0.7 | 11.2 ±0.0 | 11.6 ±0.1 |
| w1 | ext | 125.9 ±2.1 | 45.3 ±0.7 | 13.0 ±0.3 | 12.9 ±0.1 |
| w2 | min | 115.6 ±3.8 | 55.9 ±0.3 | 41.1 ±0.7 | 40.6 ±0.4 |
| w2 | ext | 93.2 ±0.8 | 38.0 ±0.4 | 21.0 ±0.2 | 20.5 ±0.1 |

### Clone (ns/event; includes allocation, drop is outside the timer)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 19.7 ±0.2 | 19.5 ±0.2 | 14.6 ±0.1 | 14.8 ±0.1 |
| w1 | ext | 19.9 ±0.2 | 19.6 ±0.3 | 15.8 ±0.2 | 15.4 ±0.3 |
| w2 | min | 60.3 ±1.2 | 60.6 ±0.8 | 63.7 ±1.4 | 69.5 ±2.3 |
| w2 | ext | 12.3 ±0.1 | 12.1 ±0.1 | 11.2 ±0.1 | 10.9 ±0.1 |

### Drop of uniquely owned events (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 77.3 ±1.3 | 20.1 ±0.1 | 18.6 ±0.1 | 18.1 ±0.4 |
| w1 | ext | 76.8 ±0.5 | 20.4 ±0.1 | 19.1 ±0.4 | 17.8 ±0.2 |
| w2 | min | 76.2 ±0.8 | 58.6 ±3.7 | 54.3 ±1.8 | 49.1 ±0.8 |
| w2 | ext | 54.5 ±0.7 | 32.1 ±0.1 | 30.1 ±0.1 | 30.4 ±0.2 |

### Conversion of 10,000 events to an Arrow RecordBatch (ns/event)

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 39.8 ±0.4 | 40.7 ±0.6 | 16.3 ±0.2 | 15.2 ±0.0 |
| w1 | ext | 40.4 ±0.6 | 46.7 ±0.9 | 17.4 ±0.5 | 17.5 ±0.4 |
| w2 | min | 31.5 ±1.1 | 38.2 ±1.1 | 27.0 ±0.6 | 33.3 ±1.2 |
| w2 | ext | 26.1 ±0.3 | 26.2 ±0.3 | 18.8 ±0.2 | 18.6 ±0.4 |

### Derived: construct + transform + arrow + drop (ns/event)

Sum of the independent medians above. It is an indicative per-event total for the decode-to-Arrow path, not a separately measured pipeline: it ignores that the filter removes ~30% of events before Arrow conversion and that real decode starts from WAL bytes rather than a parsed serde_json::Value.

| wl | values | A-fresh | A-interned | B-shape-arc | C-shape-id |
|---|---|---|---|---|---|
| w1 | min | 405 | 205 | 143 | 146 |
| w1 | ext | 404 | 215 | 159 | 153 |
| w2 | min | 314 | 215 | 195 | 194 |
| w2 | ext | 262 | 154 | 128 | 127 |

