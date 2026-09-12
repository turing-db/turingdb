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

Run on 2026-09-11 at commit 34b0219f0, release build, `--repeat 5`.

**10 of the 55 queries run.** Setting aside the 10 that call a Neo4j library (`gds.*` in
BI 15, 19 and 20; `apoc.*` in BI 10) rather than the Cypher language, it is 10 of 45.

| query | rows | ms |
| --- | --- | --- |
| interactive-short-1 | 1 | 0.49 |
| interactive-short-3 | 48 | 0.51 |
| interactive-short-5 | 1 | 0.48 |
| interactive-complex-8 | 20 | 0.57 |
| interactive-update-2 | 0 | 0.46 |
| interactive-update-3 | 0 | 0.44 |
| interactive-update-5 | 0 | 0.43 |
| interactive-update-8 | 0 | 0.45 |
| bi-6 | 20 | 1.11 |
| bi-11 | 1 | 9.83 |

The answers are right, not just the row counts. BI 11 counts 25 friend triangles in India,
which is what counting them over the CSVs in Python gives. IC 8's top 20 replies match
that computation row for row, ids, names, dates and order. IS 3 returns 48 friends for
person 4398046511333, its degree in `person_knows_person`.

### What stops the other 35

| queries | blocked on |
| --- | --- |
| IC 3, 5, 6, 9, 10, 11, IS 2, 6, BI 3, 4, 9, 12, 17 | variable-length paths, `[:KNOWS*1..2]` |
| IC 1, 13, 14, BI 19, 20 | `shortestPath` / `allShortestPaths` |
| IU 1, 4, 6, 7 | `CREATE` followed by `WITH` |
| IC 2, IS 4 | `coalesce` |
| BI 7, 18 | path expressions |
| BI 1, 2, 13 | datetime: `duration`, `.year` / `.month` on a date |
| IC 7 | `head` |
| IC 12, BI 8 | edge type alternation, `[:A\|B]` |
| BI 8 | pattern comprehensions, `size([(a)-[r]-(b) \| r])` |
| BI 16 | `CALL { subquery }` |
| IS 7 | `CASE r WHEN null` over an edge variable |
| IC 4 | chained comparison, `a <= b < c` |
| BI 14 | `ORDER BY` a property of a `DISTINCT` column |
| BI 5 | an aggregate whose alias reuses its input column's name |

The last three are not missing features. Each is a query the engine has every piece to run
and an analyzer rule turns away, so they are written up separately below.

### Three rejections that look too broad

Each was reduced to the smallest query that reproduces it.

**Chained comparison** (blocks IC 4). `1275350400000 <= p.creationDate < 1277856000000` is
rejected with `Operands are not valid or compatible numeric types: 'Bool' and 'Integer'`.
The comparison is parsed left-associatively, so the second `<` gets the first one's boolean.
Spelling the same bound with `AND` runs. Neo4j reads `a <= b < c` as the conjunction.

**`ORDER BY` over a `DISTINCT` column's property** (blocks BI 14):

```
MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person)
WITH DISTINCT c, p ORDER BY c.name ASC, p.id ASC
RETURN c.name, p.id LIMIT 3
```

`ORDER BY with DISTINCT may only order by returned columns.` `c` is a returned column and
`c.name` is functionally determined by it, which is the case CLAUDE.md's own worked example
calls valid for grouping keys. Dropping `DISTINCT` runs.

**An aggregate whose alias reuses its input column's name** (blocks BI 5):

```
MATCH (p:Person)<-[:HAS_CREATOR]-(m:Message)
WITH p, m, count(m) AS perMessage
WITH p, sum(perMessage) AS perMessage
RETURN p.id, perMessage ORDER BY perMessage DESC LIMIT 2
```

`Aggregate functions may not be nested: the argument of 'sum' names an aggregate of the
same projection.` It does not — `perMessage` is a plain column from the preceding `WITH`.
Renaming the alias to anything else runs, so the argument is being resolved against the
projection's own output rather than its input.
