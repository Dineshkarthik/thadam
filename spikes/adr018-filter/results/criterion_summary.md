### Per-event evaluation, ns/event (criterion median of 40 samples, [low, high] in brackets)

| filter | native | cel_eval_only | cel_resolver | cel_ctx_refs | cel_ctx_all | evalexpr_refs |
|---|---|---|---|---|---|---|
| demo/level_ne_debug | 3 [3, 3] | 21 [21, 22] | 44 [44, 45] | 111 [110, 112] | 1,698 [1,687, 1,711] | 158 [157, 160] |
| demo/quality_good | 4 [4, 4] | 91 [91, 92] | 682 [678, 690] | 730 [726, 735] | 1,783 [1,776, 1,791] | 146 [146, 147] |
| demo/range_scalar | 3 [3, 3] | 231 [229, 234] | 272 [270, 275] | 288 [282, 297] | 1,954 [1,933, 1,977] | 228 [224, 232] |
| demo/range_list | 4 [4, 4] | 1,471 [1,456, 1,490] | 1,705 [1,692, 1,723] | 1,740 [1,729, 1,754] | 3,225 [3,203, 3,248] | n/a |
| demo/combo | 13 [13, 13] | 150 [149, 152] | 253 [252, 255] | 982 [979, 985] | 1,836 [1,829, 1,844] | 626 [621, 633] |
| wide/wide_range_scalar | 3 [3, 3] | 229 [227, 233] | 270 [267, 274] | 279 [276, 283] | 6,046 [6,005, 6,104] | 221 [220, 223] |

### Ratio to native (median)

| filter | cel_eval_only | cel_resolver | cel_ctx_refs | cel_ctx_all | evalexpr_refs |
|---|---|---|---|---|---|
| demo/level_ne_debug | 7x | 14x | 35x | 533x | 50x |
| demo/quality_good | 25x | 188x | 201x | 491x | 40x |
| demo/range_scalar | 88x | 104x | 110x | 744x | 87x |
| demo/range_list | 381x | 442x | 451x | 836x | n/a |
| demo/combo | 11x | 19x | 75x | 140x | 48x |
| wide/wide_range_scalar | 67x | 79x | 81x | 1,765x | 65x |

### Compile cost (once per config load), microseconds per compile (median)

| filter | cel | native (yaml parse + compile) | evalexpr |
|---|---|---|---|
| level_ne_debug | 22.8 | 2.5 | 0.5 |
| quality_good | 23.8 | 2.5 | 0.6 |
| range_scalar | 44.1 | 2.6 | 1.3 |
| range_list | 51.4 | 2.6 | n/a |
| combo | 38.9 | 11.7 | 2.3 |
