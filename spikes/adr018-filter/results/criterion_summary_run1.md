### Per-event evaluation, ns/event (criterion median of 40 samples, [low, high] in brackets)

| filter | native | cel_eval_only | cel_resolver | cel_ctx_refs | cel_ctx_all | evalexpr_refs |
|---|---|---|---|---|---|---|
| demo/level_ne_debug | 3 [3, 3] | 22 [22, 22] | 46 [46, 47] | 113 [113, 114] | 1,739 [1,730, 1,753] | 156 [156, 156] |
| demo/quality_good | 4 [4, 4] | 93 [93, 93] | 687 [683, 693] | 736 [731, 743] | 1,834 [1,822, 1,851] | 151 [147, 156] |
| demo/range_scalar | 3 [3, 3] | 232 [230, 234] | 273 [270, 278] | 288 [285, 293] | 1,957 [1,943, 1,977] | 221 [220, 224] |
| demo/range_list | 4 [4, 4] | 1,497 [1,484, 1,512] | 1,727 [1,717, 1,742] | 1,755 [1,734, 1,784] | 3,279 [3,245, 3,330] | n/a |
| demo/combo | 13 [13, 13] | 155 [154, 157] | 259 [258, 262] | 996 [991, 1,006] | 1,872 [1,859, 1,889] | 625 [622, 629] |
| wide/wide_range_scalar | 3 [3, 3] | 229 [228, 230] | 267 [265, 270] | 282 [279, 286] | 5,951 [5,929, 5,975] | 219 [217, 224] |

### Ratio to native (median)

| filter | cel_eval_only | cel_resolver | cel_ctx_refs | cel_ctx_all | evalexpr_refs |
|---|---|---|---|---|---|
| demo/level_ne_debug | 7x | 14x | 35x | 542x | 49x |
| demo/quality_good | 25x | 184x | 197x | 492x | 41x |
| demo/range_scalar | 82x | 97x | 102x | 692x | 78x |
| demo/range_list | 386x | 445x | 452x | 845x | n/a |
| demo/combo | 12x | 19x | 75x | 140x | 47x |
| wide/wide_range_scalar | 68x | 79x | 83x | 1,760x | 65x |

### Compile cost (once per config load), microseconds per compile (median)

| filter | cel | native (yaml parse + compile) | evalexpr |
|---|---|---|---|
| level_ne_debug | 22.8 | 2.4 | 0.5 |
| quality_good | 24.0 | 2.4 | 0.6 |
| range_scalar | 44.2 | 2.6 | 1.3 |
| range_list | 51.5 | 2.6 | n/a |
| combo | 39.3 | 11.3 | 2.3 |
