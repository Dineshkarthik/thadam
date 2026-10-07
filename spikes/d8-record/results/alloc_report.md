## Value sizes (bytes, inline)

| type | size_of |
|---|---|
| bytes::Bytes | 32 |
| Arc<str> | 16 |
| Arc<[f64]> | 16 |
| Vec<_> | 24 |
| min::Value (Str = Utf8Bytes(Bytes), as in plan) | 40 |
| ext::Value (Str = Utf8Bytes(Bytes), as in plan) | 40 |
| compact alt (Str = Arc<str>, List = Box<[_]>), size only | 24 |
| Record A (Vec<(Arc<str>, ext::Value)>) | 24 |
| Record B (Arc<Shape> + Vec) | 32 |
| Record C (u32 + Vec) | 32 |

## Allocation and memory per event (N = 10000, single thread)

Columns: `live B req` = requested bytes still live after construction; `live B usable` = malloc_size of those blocks (includes size-class rounding); `total B/ev` = sizeof(Rec) + live B usable; `xform allocs` = uniquely owned events, `xform(shared)` = events whose arrays are shared with a clone.

| wl | values | repr | sizeof(Rec) | build allocs/ev | build B req/ev | live B req/ev | live B usable/ev | total B/ev | clone allocs/ev | xform allocs/ev | xform(shared) allocs/ev | kept | arrow allocs/row | arrow B req/row |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| w1 | min | A-fresh | 24 | 9.00 | 568 | 568 | 688 | 712 | 1.00 | 4.00 | 4.00 | 73.0% | 0.01 | 45 |
| w1 | min | A-interned | 24 | 2.00 | 400 | 400 | 464 | 488 | 1.00 | 0.00 | 0.00 | 73.0% | 0.01 | 45 |
| w1 | min | B-shape-arc | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.0% | 0.01 | 45 |
| w1 | min | C-shape-id | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.0% | 0.01 | 45 |
| w1 | ext | A-fresh | 24 | 9.00 | 568 | 568 | 688 | 712 | 1.00 | 4.00 | 4.00 | 73.4% | 0.01 | 45 |
| w1 | ext | A-interned | 24 | 2.00 | 400 | 400 | 464 | 488 | 1.00 | 0.00 | 0.00 | 73.4% | 0.01 | 45 |
| w1 | ext | B-shape-arc | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.4% | 0.01 | 45 |
| w1 | ext | C-shape-id | 32 | 2.00 | 288 | 288 | 336 | 368 | 1.00 | 0.00 | 0.00 | 73.4% | 0.01 | 45 |
| w2 | min | A-fresh | 24 | 7.00 | 1080 | 1080 | 1248 | 1272 | 4.00 | 3.00 | 3.00 | 71.0% | 0.00 | 180 |
| w2 | min | A-interned | 24 | 4.00 | 1008 | 1008 | 1152 | 1176 | 4.00 | 0.00 | 0.00 | 71.0% | 0.00 | 180 |
| w2 | min | B-shape-arc | 32 | 4.00 | 960 | 960 | 1088 | 1120 | 4.00 | 0.00 | 0.00 | 71.0% | 0.00 | 180 |
| w2 | min | C-shape-id | 32 | 4.00 | 960 | 960 | 1088 | 1120 | 4.00 | 0.00 | 0.00 | 71.0% | 0.00 | 180 |
| w2 | ext | A-fresh | 24 | 7.00 | 456 | 456 | 528 | 552 | 1.00 | 3.00 | 5.00 | 71.0% | 0.00 | 180 |
| w2 | ext | A-interned | 24 | 4.00 | 384 | 384 | 432 | 456 | 1.00 | 0.00 | 2.00 | 71.0% | 0.00 | 180 |
| w2 | ext | B-shape-arc | 32 | 4.00 | 336 | 336 | 368 | 400 | 1.00 | 0.00 | 2.00 | 71.0% | 0.00 | 180 |
| w2 | ext | C-shape-id | 32 | 4.00 | 336 | 336 | 368 | 400 | 1.00 | 0.00 | 2.00 | 71.0% | 0.00 | 180 |
