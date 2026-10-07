
#### demo/level_ne_debug (`level != 'debug'`), 10000 events

| strategy | allocs/event | bytes requested/event |
|---|---|---|
| native | 0.00 | 0 |
| cel_eval_only | 0.00 | 0 |
| cel_resolver | 1.00 | 24 |
| cel_ctx_refs | 5.00 | 533 |
| cel_ctx_all | 71.31 | 3707 |
| evalexpr_refs | 7.00 | 514 |

#### demo/quality_good (`quality.status == 'good'`), 10000 events

| strategy | allocs/event | bytes requested/event |
|---|---|---|
| native | 0.00 | 0 |
| cel_eval_only | 2.00 | 78 |
| cel_resolver | 22.82 | 1161 |
| cel_ctx_refs | 24.82 | 1624 |
| cel_ctx_all | 73.31 | 3785 |
| evalexpr_refs | 7.00 | 519 |

#### demo/range_scalar (`cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0`), 10000 events

| strategy | allocs/event | bytes requested/event |
|---|---|---|
| native | 0.00 | 0 |
| cel_eval_only | 11.69 | 327 |
| cel_resolver | 13.64 | 343 |
| cel_ctx_refs | 14.69 | 801 |
| cel_ctx_all | 83.00 | 4034 |
| evalexpr_refs | 7.00 | 886 |

#### demo/range_list (`motor_temp_c.all(t, t >= -40.0 && t <= 150.0)`), 10000 events

| strategy | allocs/event | bytes requested/event |
|---|---|---|
| native | 0.00 | 0 |
| cel_eval_only | 70.42 | 1919 |
| cel_resolver | 82.42 | 2535 |
| cel_ctx_refs | 84.42 | 3003 |
| cel_ctx_all | 141.72 | 5626 |

#### demo/combo (`level != 'debug' && (robot != 'robot-03' || quality.status == 'good') && soc_pct >= 10.0`), 10000 events

| strategy | allocs/event | bytes requested/event |
|---|---|---|
| native | 0.00 | 0 |
| cel_eval_only | 4.57 | 130 |
| cel_resolver | 9.06 | 282 |
| cel_ctx_refs | 37.39 | 1846 |
| cel_ctx_all | 75.88 | 3837 |
| evalexpr_refs | 24.00 | 1938 |

#### wide/wide_range_scalar (`s17 >= -40.0 && s17 <= 150.0`), 10000 events

| strategy | allocs/event | bytes requested/event |
|---|---|---|
| native | 0.00 | 0 |
| cel_eval_only | 11.47 | 321 |
| cel_resolver | 13.38 | 336 |
| cel_ctx_refs | 14.47 | 788 |
| cel_ctx_all | 182.47 | 6984 |
| evalexpr_refs | 7.00 | 879 |

#### isolation: allocations of `cel_eval_only` per evaluation by expression form

| expression | allocs/event | bytes requested/event |
|---|---|---|
| `true` | 0.00 | 0 |
| `cpu_temp_c >= 0.0` | 6.00 | 168 |
| `cpu_temp_c >= -40.0` | 6.00 | 168 |
| `cpu_temp_c >= 0.0 && cpu_temp_c <= 150.0` | 10.46 | 293 |
| `cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0` | 11.69 | 327 |
| `level != 'debug'` | 0.00 | 0 |
| `level == 'info'` | 0.00 | 0 |
| `soc_pct > 10.0` | 6.00 | 167 |
