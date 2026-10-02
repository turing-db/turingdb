# LDBC SNB on the TuringDB v3 engine

The official LDBC Social Network Benchmark queries, run unchanged against the v3 (MLIR)
query engine.

`queries/interactive/` is the whole `cypher/queries` directory of
[ldbc_snb_interactive_v1_impls](https://github.com/ldbc/ldbc_snb_interactive_v1_impls):
14 complex reads, 7 short reads, 8 updates. `queries/bi/` is the whole `neo4j/queries`
directory of [ldbc_snb_bi](https://github.com/ldbc/ldbc_snb_bi): 20 business-intelligence
queries plus the graph-projection helpers queries 19 and 20 need. Both are copied
verbatim — no query is edited to suit the engine, which is the point of the exercise.

`params/interactive_*_param.txt` are the official substitution parameters that ship with
the same data set.

## Running it

```bash
./fetch_data.sh                       # clone the LDBC test data, convert it to data/ldbc.jsonl
./load_data.sh /tmp/ldbc              # load it as the graph ldbc, replacing an earlier load
./run_ldbc.py --turing-dir /tmp/ldbc --repeat 5
```

Both `load_data.sh` and `run_ldbc.py` run `build/tools/turingdb/turingdb`, not the
`turingdb` on the `PATH`. `load_data.sh` refuses a `data/ldbc.jsonl` older than
`ldbc_to_jsonl.py`. Rerun `./fetch_data.sh` after the converter changes.

## The data set

The LDBC test data set the reference implementation ships (`cypher/test-data/vanilla`),
converted by `ldbc_to_jsonl.py`: 34,735 nodes and 70,842 edges — 222 persons, 5,924 posts,
2,218 comments, 805 forums, 16,080 tags. It loads in 283 ms.

The labels and relationship types are the ones the Neo4j reference implementation builds,
so the queries match the same names: `Post:Message`, `Comment:Message`,
`Place:City`/`Country`/`Continent`, `Organisation:Company`/`University`, and `KNOWS`,
`HAS_CREATOR`, `REPLY_OF`, `IS_LOCATED_IN` and the rest.

Two properties do not survive the conversion as the benchmark writes them. `Person.email`
and `Person.speaks` are lists in LDBC, and `ldbc_to_jsonl.py` leaves both as the
';'-separated strings the CSV holds.

Every date is a `DateTime`. The CSVs hold `birthday`, `creationDate` and `joinDate` as the
epoch milliseconds the long-date-formatter serializer writes. `ldbc_to_jsonl.py` writes them
as ISO-8601 strings, and `WITH DATETIMES` loads them as `DateTime` properties. The date
parameters are `datetime('...')` literals.

The interactive queries were written for integer dates. IC 7 subtracts two dates, and IC 10
reads `birthday` as epoch milliseconds with `datetime({epochMillis: friend.birthday})`. Both
stop earlier for other reasons.

## How queries reach the engine

The v3 engine is the only one the shell runs in local mode, so `run_ldbc.py` drives one
shell process over stdin and reads the row count and the timing the shell prints. The
engine takes no query parameters, so each `$name` is replaced by a literal from
`params/turingdb.json` first. A list parameter is spelled as a list there, so `$tagIds`,
`$studyAt` and `$workAt` reach the engine as `[1524]` and `[[2435, 2004]]`. Writes need an
open change, so the update queries run after `CHANGE NEW` / `checkout change-0`.

`--repeat N` runs each query N times and reports the fastest.

## Results

Run on 2026-10-01 against main at f14911858, release build.

**34 of the 55 queries run.** Setting aside the 10 that call a Neo4j library (`gds.*` in
BI 15, 19 and 20; `apoc.*` in BI 10) rather than the Cypher language, it is 34 of 45.

| query | rows |
| --- | --- |
| interactive-short-1 | 1 |
| interactive-short-2 | 10 |
| interactive-short-3 | 48 |
| interactive-short-4 | 1 |
| interactive-short-5 | 1 |
| interactive-short-6 | 1 |
| interactive-short-7 | 14 |
| interactive-complex-2 | 20 |
| interactive-complex-3 | 0 |
| interactive-complex-4 | 9 |
| interactive-complex-5 | 20 |
| interactive-complex-6 | 10 |
| interactive-complex-8 | 20 |
| interactive-complex-9 | 20 |
| interactive-complex-11 | 2 |
| interactive-complex-12 | 2 |
| interactive-update-1 | 0 |
| interactive-update-2 | 0 |
| interactive-update-3 | 0 |
| interactive-update-4 | 0 |
| interactive-update-5 | 0 |
| interactive-update-6 | 0 |
| interactive-update-7 | 0 |
| interactive-update-8 | 0 |
| bi-1 | 6 |
| bi-3 | 6 |
| bi-5 | 20 |
| bi-6 | 20 |
| bi-7 | 39 |
| bi-8 | 0 |
| bi-9 | 44 |
| bi-11 | 1 |
| bi-12 | 11 |
| bi-18 | 0 |

It is the same 34 queries as at c651fd852, with the same row counts. Three of the queries
that stop moved underneath the count. BI 2 no longer stops at `duration`, which main
implemented at 1bd5bf0f1 and built from a map at f14911858, and stops at `abs` instead. With
`abs` taken out, the rest of BI 2 runs. BI 13 no longer stops at `.year` on a function call,
which main implemented at d4fd4c23f. BI 4's `WHERE` after `WITH` runs since 83441efe1. Both
now stop at a runtime failure, written up below.

The answers are right, not just the row counts. IS 2, 3, 6, IC 3, 4, 5, 6, 8, 9, 11, 12 and
BI 1, 3, 7, 8, 9, 11, 12, 18 were recomputed in Python from the CSVs and match row for row,
in order. BI 11 counts 25 friend triangles in India. IC 8's top 20 replies match ids, names,
dates and order. IS 3 returns 48 friends for person 4398046511333, its degree in
`person_knows_person`.

Three of them return 0 rows, and 0 is the answer on this data. IC 3's window holds
12 messages, all located in Sweden, so no friend has a message in Kazakhstan. BI 8 and BI 18
take the tag Carl_Gustaf_Emil_Mannerheim, which no person has an interest in. BI 8's 30
messages with that tag were created in September 2010, outside its June window. With the tag
William_Shakespeare, BI 8 returns 24 rows and BI 18 returns 20, both matching the CSVs.

IC 9's image posts have no content, and their text falls back to the file name,
`photo343597386103.jpg`. IC 11 finds one friend at a Swedish company, Joakim Larsson, and
returns 2 of his 3 jobs. The third starts in 2006, and the query keeps jobs started before
2006. BI 12's 11 rows sum to 222 persons, every person in the graph.

BI 1 keeps all 8,142 messages, since every one was created in 2010, before its
`datetime('2011-01-01')` cut. With the cut moved to `datetime('2010-07-01')`, 2,378 messages
fall before it and the 6 rows still match the CSVs.

IU 1 indexes an element of a nested list, and its write was read back after `COMMIT`.
Person 999999999 is Bench Mark, its `STUDY_AT` edge carries classYear 2004 to organisation
2435, its `WORKS_AT` edge carries workFrom 2010 to organisation 296, its `HAS_INTEREST`
edge reaches tag 1524 and its `IS_LOCATED_IN` edge reaches city 1073. The two years come
from `s[1]` and `w[1]`, the two organisations from `s[0]` and `w[0]`.

### What stops the other 11

| queries | blocked on |
| --- | --- |
| IC 1, 13 | `shortestPath` |
| IC 7, BI 14 | `collect` of a map literal, `collect({score: score})` |
| IC 14 | `allShortestPaths` |
| IC 10 | a datetime built from a map, `datetime({epochMillis: friend.birthday})` |
| BI 2 | `abs` |
| BI 13 | the `ELSE` of a `CASE` evaluated on the rows its `WHEN` guards, dividing by zero |
| BI 16 | property access on a map, `UNWIND [{letter: 'A'}] AS param` then `param.letter` |
| BI 17 | a relationship with both arrowheads, `(forum1)<-[:HAS_MEMBER]->(person2)` |
| BI 4 | an internal assertion on `IN` inside a `CALL { ... UNION ALL ... }` |

The last two are not missing features. Both queries reach the runtime and fail there, so
they are written up separately below.

### Two runtime failures

Each was reduced to the smallest query that reproduces it.

**Division by zero under `CASE`** (blocks BI 13). `UNWIND [0, 2] AS t RETURN CASE t WHEN 0
THEN 0 ELSE 1 / t END` fails with `Attempted to divide by zero.` The `ELSE` is evaluated on
the row where `t` is 0. Neo4j evaluates only the branch taken and returns 0 for both rows.
BI 13 writes the float form, `CASE totalLikeCount WHEN 0 THEN 0.0 ELSE zombieLikeCount /
toFloat(totalLikeCount) END`, and all 6 of its zombies have a `totalLikeCount` of 0. With the
division taken out, BI 13 returns those 6 rows. `RETURN 1.0 / 0.0` fails the same way, where
Neo4j returns `Infinity`.

**`IN` inside a `UNION ALL` branch** (blocks BI 4). `WITH range(1, 2) AS ks CALL { WITH ks
UNWIND ks AS k WITH k WHERE k IN ks RETURN k UNION ALL WITH ks UNWIND ks AS k RETURN k }
RETURN count(k)` fails with `The assertion 'lhs->size() == rhs->size()' failed at
storage/columns/BinaryPredicates.h:266`. It should return 4. With the literal `[1, 2]` in
place of `range(1, 2)` it returns 4. Each branch of BI 4's `CALL` runs alone, returning 164
and 1,369 rows.
