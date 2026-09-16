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

The v3 engine is reachable through the shell's `#v3 ` prefix in local mode, so
`run_ldbc.py` drives one shell process over stdin and reads the per-query timing the shell
prints. The engine takes no query parameters, so each `$name` is replaced by a literal from
`params/turingdb.json` first. A list parameter is spelled as a list there, so `$tagIds`,
`$studyAt` and `$workAt` reach the engine as `[1524]` and `[[2435, 2004]]`. Writes need an
open change, so the update queries run after `CHANGE NEW` / `checkout change-0`.

`--repeat N` runs each query N times and reports the fastest.

## Results

Run on 2026-09-16 against main at ec20eb10d, release build, `--repeat 5`.

**17 of the 55 queries run.** Setting aside the 10 that call a Neo4j library (`gds.*` in
BI 15, 19 and 20; `apoc.*` in BI 10) rather than the Cypher language, it is 17 of 45.

| query | rows | ms |
| --- | --- | --- |
| interactive-short-1 | 1 | 0.54 |
| interactive-short-3 | 48 | 0.58 |
| interactive-short-4 | 1 | 0.51 |
| interactive-short-5 | 1 | 0.53 |
| interactive-short-7 | 14 | 0.88 |
| interactive-complex-2 | 20 | 0.73 |
| interactive-complex-8 | 20 | 0.67 |
| interactive-update-2 | 0 | 0.49 |
| interactive-update-3 | 0 | 0.50 |
| interactive-update-4 | 0 | 0.96 |
| interactive-update-5 | 0 | 0.50 |
| interactive-update-6 | 0 | 1.07 |
| interactive-update-7 | 0 | 1.24 |
| interactive-update-8 | 0 | 0.49 |
| bi-5 | 20 | 0.96 |
| bi-6 | 20 | 1.21 |
| bi-11 | 1 | 12.27 |

The answers are right, not just the row counts. BI 11 counts 25 friend triangles in India,
which is what counting them over the CSVs in Python gives. IC 8's top 20 replies match that
computation row for row, ids, names, dates and order. IS 3 returns 48 friends for person
4398046511333, its degree in `person_knows_person`.

The three that run now and did not on 2026-09-12 are the updates IU 4, 6 and 7, which
`CREATE` and then `WITH`. Each was read back after `COMMIT`. IU 4's forum 999999999 has its
`HAS_MODERATOR` edge to person 4398046511333 and its `HAS_TAG` edge to tag 1524. IU 6's post
carries `bench post` and the same tag. IU 7's comment replies to message 343597383680, which
is `$replyToPostId + $replyToCommentId + 1`.

Two of the blockers written up on 2026-09-12 are gone and the queries stopped one step
further on. IC 7 passes `head` and now stops at `collect` of a map literal, where BI 14
already stopped. IU 1 passes `CREATE` followed by `WITH` and now stops at `s[0]`, indexing
an element of a nested list, with `EXEC_ERROR: Unsupported nullable value chunk element
type`.

BI 11 timed 9.11 ms on 2026-09-12 and 12.27 ms here. The machine carried other load during
this run, and repeated runs of the short queries moved by a comparable fraction, so this is
not read as a regression.

### What stops the other 28

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
| BI 16 | `CALL { subquery }` |
| IU 1 | indexing an element of a nested list, `s[0]` |
| IC 4 | chained comparison, `a <= b < c` |

The last one is not a missing feature. It is a query the engine has every piece to run and
an analyzer rule turns away, so it is written up separately below.

### One rejection that looks too broad

It was reduced to the smallest query that reproduces it.

**Chained comparison** (blocks IC 4). `1275350400000 <= p.creationDate < 1277856000000` is
rejected with `Operands are not valid or compatible numeric types: 'Bool' and 'Integer'`.
The comparison is parsed left-associatively, so the second `<` gets the first one's boolean.
Spelling the same bound with `AND` runs. Neo4j reads `a <= b < c` as the conjunction.
