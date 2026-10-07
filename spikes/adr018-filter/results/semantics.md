### CEL (`cel` 0.15.0): numeric, null, missing, error semantics

| expression | bound variables | result |
|---|---|---|
| `1 < 1.5` |  | Bool(true) |
| `1 == 1.0` |  | Bool(true) |
| `x >= -40` | x=Float(-39.5) | Bool(true) |
| `9223372036854775808u > 9223372036854775807` |  | Bool(true) |
| `big > 9223372036854775807` | big=UInt(18446744073709551615) | Bool(true) |
| `big == 18446744073709551615u` | big=UInt(18446744073709551615) | Bool(true) |
| `x == 0.1` | x=Float(0.10000000149011612) | Bool(false) |
| `x == 0.10000000149011612` | x=Float(0.10000000149011612) | Bool(true) |
| `9223372036854775807 + 1` |  | RUNTIME ERROR: Overflow from binary operator 'add': Int(9223372036854775807), Int(1) |
| `1 / 0` |  | RUNTIME ERROR: Division by zero of Int(1) |
| `1.0 / 0.0` |  | Float(inf) |
| `code == null` | code=Null | Bool(true) |
| `code == 'E-1'` | code=Null | Bool(false) |
| `code != 'E-1'` | code=Null | Bool(true) |
| `level != 'debug'` | level=Null | Bool(true) |
| `nope == 1` |  | RUNTIME ERROR: Undeclared reference to 'nope' |
| `q.nope == 1` | q=Map(Map { map: {String("status"): String("good")} }) | RUNTIME ERROR: No such key: nope |
| `has(q.nope)` | q=Map(Map { map: {String("status"): String("good")} }) | Bool(false) |
| `x > 'hot'` | x=Float(1.0) | RUNTIME ERROR: found no matching overload for '_>_' applied to '(double, string)' |
| `false && (1 / 0 > 0)` |  | Bool(false) |
| `(1 / 0 > 0) \|\| true` |  | Bool(true) |
| `(1 / 0 > 0) && false` |  | Bool(false) |
| `now()` |  | RUNTIME ERROR: Undeclared reference to 'now' |
| `rand()` |  | RUNTIME ERROR: Undeclared reference to 'rand' |
| `random()` |  | RUNTIME ERROR: Undeclared reference to 'random' |
| `timestamp('2026-01-01T00:00:00Z') < timestamp('2027-01-01T00:00:00Z')` |  | Bool(true) |
| `'joint 3 over temperature'.matches('^joint [0-9]+ over')` |  | Bool(true) |
| `'abc'.contains('b')` |  | Bool(true) |
| `t.all(v, v > 0.0)` | t=List([Float(1.0), Float(2.0)]) | Bool(true) |
| `t.exists(v, v > 1.5) && size(t) == 2` | t=List([Float(1.0), Float(2.0)]) | Bool(true) |

### CEL compile-time diagnostics (`Env::compile`)

| source | result |
|---|---|
| `level != ` | ERROR(s): ERROR: <input>:1:10: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER} ¶ \| level !=  ¶ \| .........^ |
| `level = 'debug'` | ERROR(s): ERROR: <input>:1:7: Syntax error: token recognition error at: '= ' ¶ \| level = 'debug' ¶ \| ......^ ¶ ERROR: <input>:1:9: Syntax error: extraneous input ''debug'' expecting <EOF> ¶ \| level = 'debug' ¶ \| ........^ |
| `level != 'debug' &&` | ERROR(s): ERROR: <input>:1:20: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER} ¶ \| level != 'debug' && ¶ \| ...................^ |
| `(level != 'debug'` | ERROR(s): ERROR: <input>:1:18: Syntax error: missing ')' at '<EOF>' ¶ \| (level != 'debug' ¶ \| .................^ |
| `level != 'debug` | ERROR(s): ERROR: <input>:1:10: Syntax error: token recognition error at: ''debug' ¶ \| level != 'debug ¶ \| .........^ ¶ ERROR: <input>:1:16: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER} ¶ \| level != 'debug ¶ \| ...............^ |
| `level != 'debug' and soc_pct > 1` | ERROR(s): ERROR: <input>:1:18: Syntax error: mismatched input 'and' expecting {<EOF>, '==', '!=', 'in', '<', '<=', '>=', '>', '&&', '\|\|', '[', '.', '-', '?', '+', '*', '/', '%'} ¶ \| level != 'debug' and soc_pct > 1 ¶ \| .................^ |
| `lvl != 'debug'` | compiles; references variables ["lvl"] |
| `unknown_fn(level)` | compiles; references variables ["level"] |
| `level.startsWith(` | ERROR(s): ERROR: <input>:1:18: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', ')', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER} ¶ \| level.startsWith( ¶ \| .................^ |
| `level != 'debug' ? 1 : 2` | compiles; references variables ["level"] |
| `1 +` | ERROR(s): ERROR: <input>:1:4: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER} ¶ \| 1 + ¶ \| ...^ |
| `[1, 2` | ERROR(s): ERROR: <input>:1:6: Syntax error: mismatched input '<EOF>' expecting {']', ','} ¶ \| [1, 2 ¶ \| .....^ |
| `a.b.c.d == 1` | compiles; references variables ["a"] |
| `level != 'debug' &&\n  robot !=` | ERROR(s): ERROR: <input>:2:11: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER} ¶ \|   robot != ¶ \| ..........^ |

Structured positions for the last case: [(2, 11)]

### Structured predicates (this spike's evaluator): the same questions

| predicate | event override | result |
|---|---|---|
| `{ field: soc_pct, eq: 0.1 }` | soc_pct = f32 nearest 0.1 | true |
| `{ field: soc_pct, eq: 0.10000000149011612 }` | soc_pct = f32 nearest 0.1 | true |
| `{ field: seq, gt: 9223372036854775807 }` | seq = u64::MAX | true |
| `{ field: seq, eq: 18446744073709551615 }` | seq = u64::MAX | true |
| `{ field: seq, eq: 18446744073709551614 }` | seq = u64::MAX | false |
| `{ field: code, eq: E-1 }` | code = null | false |
| `{ field: code, ne: E-1 }` | code = null | true |
| `{ field: cpu_temp_c, min: -40 }` | cpu_temp_c = 3 | true |
| `{ field: nope, eq: 1 }` | - | COMPILE ERROR: filter.field: unknown field `nope` |
| `{ field: cpu_temp_c, gt: hot }` | - | COMPILE ERROR: filter.gt: field `cpu_temp_c` is numeric (F32); got Str("hot") |
| `{ field: level, gt: debug }` | - | COMPILE ERROR: filter.gt: `gt` is not defined for string field `level` |
| `{ field: level, nee: debug }` | - | YAML/SCHEMA ERROR: error: line 1 column 17: unknown field `nee`, expected one of all, any, not, field, eq, ne, lt, le, gt, ge, in, min, max |
| `{ field: level }` | - | COMPILE ERROR: filter: no comparison given for field `level` |
| `{ field: level, ne: debug, eq: info }` | level = info | true |
| `{ all: [], field: level, ne: debug }` | - | COMPILE ERROR: filter: exactly one of `all`, `any`, `not`, or `field` is required |

### evalexpr 13.1.0 (AGPL-3.0-only)

| expression | variables | result |
|---|---|---|
| `1 < 1.5` |  | Boolean(true) |
| `1 == 1.0` |  | Boolean(false) |
| `x == 0.1` | x=Float(0.10000000149011612) | Boolean(false) |
| `1 / 0` |  | ERROR: Error dividing 1 / 0 |
| `9223372036854775807 + 1` |  | ERROR: Error adding 9223372036854775807 + 1 |
| `nope == 1` |  | ERROR: Variable identifier is not bound to anything by context: "nope". |
| `x > "hot"` | x=Float(1.0) | ERROR: Expected a Value::Float or Value::Int, but got String("hot"). |
| `random()` |  | ERROR: Function identifier is not bound to anything by context: "random". |
| `now()` |  | ERROR: Function identifier is not bound to anything by context: "now". |
| `level != "debug"` | level=Empty | Boolean(true) |
| `level != ` | level=String("x") | ERROR: An operator expected 2 arguments, but got 1. |
| `level = "debug"` | level=String("x") | ERROR: Cannot manipulate context |
| `(level != "debug"` | level=String("x") | ERROR: Found an unmatched opening parenthesis '('. |
