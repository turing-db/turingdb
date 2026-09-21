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
mkdir -p /tmp/ldbc/data && cp data/ldbc.jsonl /tmp/ldbc/data/
echo 'LOAD JSONL "ldbc.jsonl" AS ldbc' | turingdb -turing-dir /tmp/ldbc
./run_ldbc.py --turing-dir /tmp/ldbc --repeat 5
```

## The data set

The LDBC test data set the reference implementation ships (`cypher/test-data/vanilla`),
converted by `ldbc_to_jsonl.py`: 34,735 nodes and 70,842 edges — 222 persons, 5,924 posts,
2,218 comments, 805 forums, 16,080 tags. It loads in 285 ms.

The labels and relationship types are the ones the Neo4j reference implementation builds,
so the queries match the same names: `Post:Message`, `Comment:Message`,
`Place:City`/`Country`/`Continent`, `Organisation:Company`/`University`, and `KNOWS`,
`HAS_CREATOR`, `REPLY_OF`, `IS_LOCATED_IN` and the rest.

Two properties do not survive the conversion as the benchmark writes them. `Person.email`
and `Person.speaks` are lists in LDBC, and `ldbc_to_jsonl.py` leaves both as the
';'-separated strings the CSV holds. Every date is the epoch-millisecond integer the
long-date-formatter serializer produces, which is also what the queries' datetime
parameters are replaced by.

## How queries reach the engine

The v3 engine is the only one the shell runs in local mode, so `run_ldbc.py` drives one
shell process over stdin and reads the row count and the timing the shell prints. The
engine takes no query parameters, so each `$name` is replaced by a literal from
`params/turingdb.json` first. A list parameter is spelled as a list there, so `$tagIds`,
`$studyAt` and `$workAt` reach the engine as `[1524]` and `[[2435, 2004]]`. Writes need an
open change, so the update queries run after `CHANGE NEW` / `checkout change-0`.

`--repeat N` runs each query N times and reports the fastest.

## Results

Run on 2026-09-21 against main at 451e6f8a8, release build.

**18 of the 55 queries run.** Setting aside the 10 that call a Neo4j library (`gds.*` in
BI 15, 19 and 20; `apoc.*` in BI 10) rather than the Cypher language, it is 18 of 45.

| query | rows |
| --- | --- |
| interactive-short-1 | 1 |
| interactive-short-3 | 48 |
| interactive-short-4 | 1 |
| interactive-short-5 | 1 |
| interactive-short-7 | 14 |
| interactive-complex-2 | 20 |
| interactive-complex-8 | 20 |
| interactive-update-1 | 0 |
| interactive-update-2 | 0 |
| interactive-update-3 | 0 |
| interactive-update-4 | 0 |
| interactive-update-5 | 0 |
| interactive-update-6 | 0 |
| interactive-update-7 | 0 |
| interactive-update-8 | 0 |
| bi-5 | 20 |
| bi-6 | 20 |
| bi-11 | 1 |

It is the same 18 queries as at 7f2b0883f. One thing moved underneath the count: BI 16 no
longer stops at `CALL { subquery }`, which main implemented at 0de9efdcc, and stops at the
map literals it unwinds instead.

The answers are right, not just the row counts. BI 11 counts 25 friend triangles in India,
which is what counting them over the CSVs in Python gives. IC 8's top 20 replies match that
computation row for row, ids, names, dates and order. IS 3 returns 48 friends for person
4398046511333, its degree in `person_knows_person`.

IU 1 indexes an element of a nested list, and its write was read back after `COMMIT`.
Person 999999999 is Bench Mark, its `STUDY_AT` edge carries classYear 2004 to organisation
2435, its `WORKS_AT` edge carries workFrom 2010 to organisation 296, its `HAS_INTEREST`
edge reaches tag 1524 and its `IS_LOCATED_IN` edge reaches city 1073. The two years come
from `s[1]` and `w[1]`, the two organisations from `s[0]` and `w[0]`.

### What stops the other 27

| queries | blocked on |
| --- | --- |
| IC 3, 5, 6, 9, 10, 11, IS 2, 6, BI 3, 4, 9, 12, 17 | variable-length paths, `[:KNOWS*1..2]` |
| BI 1, 2, 13 | datetime: `duration`, `.year` / `.month` on a date |
| IC 1, 13 | `shortestPath` |
| IC 7, BI 14 | `collect` of a map literal, `collect({score: score})` |
| BI 7, 18 | path expressions |
| IC 14 | `allShortestPaths` |
| IC 12 | edge type alternation, `[:A\|B]` |
| BI 8 | pattern comprehensions, `size([(a)-[r]-(b) \| r])` |
| BI 16 | map literals, `UNWIND [{letter: 'A'}] AS param` then `param.letter` |
| IC 4 | chained comparison, `a <= b < c` |

The last one is not a missing feature. It is a query the engine has every piece to run and
an analyzer rule turns away, so it is written up separately below.

### One rejection that looks too broad

It was reduced to the smallest query that reproduces it.

**Chained comparison** (blocks IC 4). `1275350400000 <= p.creationDate < 1277856000000` is
rejected with `Operands are not valid or compatible numeric types: 'Bool' and 'Integer'`.
The comparison is parsed left-associatively, so the second `<` gets the first one's boolean.
Spelling the same bound with `AND` runs. Neo4j reads `a <= b < c` as the conjunction.
