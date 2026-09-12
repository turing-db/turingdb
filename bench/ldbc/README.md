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
and `Person.speaks` are lists in LDBC; TuringDB properties are scalars, so both stay the
';'-separated strings the CSV holds. Every date is the epoch-millisecond integer the
long-date-formatter serializer produces, which is also what the queries' datetime
parameters are replaced by.

## How queries reach the engine

The v3 engine is reachable through the shell's `#v3 ` prefix in local mode, so
`run_ldbc.py` drives one shell process over stdin and reads the per-query timing the shell
prints. The engine takes no query parameters, so each `$name` is replaced by a literal from
`params/turingdb.json` first. Writes need an open change, so the update queries run after
`CHANGE NEW` / `checkout change-0`.

`--repeat N` runs each query N times and reports the fastest.

## Results

Run on 2026-09-12 at commit 16c1fdc59, release build, `--repeat 5`.

**14 of the 55 queries run.** Setting aside the 10 that call a Neo4j library (`gds.*` in
BI 15, 19 and 20; `apoc.*` in BI 10) rather than the Cypher language, it is 14 of 45.

| query | rows | ms |
| --- | --- | --- |
| interactive-short-1 | 1 | 0.47 |
| interactive-short-3 | 48 | 0.51 |
| interactive-short-4 | 1 | 0.41 |
| interactive-short-5 | 1 | 0.47 |
| interactive-short-7 | 14 | 0.76 |
| interactive-complex-2 | 20 | 0.63 |
| interactive-complex-8 | 20 | 0.57 |
| interactive-update-2 | 0 | 0.46 |
| interactive-update-3 | 0 | 0.43 |
| interactive-update-5 | 0 | 0.43 |
| interactive-update-8 | 0 | 0.44 |
| bi-5 | 20 | 0.84 |
| bi-6 | 20 | 1.05 |
| bi-11 | 1 | 9.11 |

The answers are right, not just the row counts. BI 11 counts 25 friend triangles in India,
which is what counting them over the CSVs in Python gives. IC 8's top 20 replies match
that computation row for row, ids, names, dates and order. IS 3 returns 48 friends for
person 4398046511333, its degree in `person_knows_person`.

The four that run now and did not on 2026-09-11 were checked the same way. BI 5 scores the
same 20 persons as the CSV computation, top row 6597069766759 at 293. IC 2's top 20 match
id and date. IS 4 reads post 206158431892, whose `imageFile` is empty, so `coalesce`
returns its content. IS 7 finds 14 replies, and all 14 of their authors know person 102,
who wrote the message, so `true` on every row is right.

### What stops the other 31

| queries | blocked on |
| --- | --- |
| IC 3, 5, 6, 9, 10, 11, IS 2, 6, BI 3, 4, 9, 12, 17 | variable-length paths, `[:KNOWS*1..2]` |
| IC 1, 13, 14, BI 19, 20 | `shortestPath` / `allShortestPaths` |
| IU 1, 4, 6, 7 | `CREATE` followed by `WITH` |
| BI 7, 18 | path expressions |
| BI 1, 2, 13 | datetime: `duration`, `.year` / `.month` on a date |
| IC 7 | `head` |
| IC 12, BI 8 | edge type alternation, `[:A\|B]` |
| BI 8 | pattern comprehensions, `size([(a)-[r]-(b) \| r])` |
| BI 16 | `CALL { subquery }` |
| BI 14 | `collect` of a map literal, `collect({score: score})` |
| IC 4 | chained comparison, `a <= b < c` |

The last one is not a missing feature. It is a query the engine has every piece to run and
an analyzer rule turns away, so it is written up separately below.

### One rejection that looks too broad

It was reduced to the smallest query that reproduces it.

**Chained comparison** (blocks IC 4). `1275350400000 <= p.creationDate < 1277856000000` is
rejected with `Operands are not valid or compatible numeric types: 'Bool' and 'Integer'`.
The comparison is parsed left-associatively, so the second `<` gets the first one's boolean.
Spelling the same bound with `AND` runs. Neo4j reads `a <= b < c` as the conjunction.

The other two rejections written up on 2026-09-11 are gone. `ORDER BY` over a `DISTINCT`
column's property runs, and so does an aggregate whose alias reuses its input column's
name. BI 14 now stops one step further on, at `collect` of a map literal.
