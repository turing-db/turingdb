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
echo 'LOAD JSONL "ldbc.jsonl" AS ldbc WITH DATETIMES ["birthday", "creationDate", "joinDate"]' \
    | turingdb -turing-dir /tmp/ldbc
./run_ldbc.py --turing-dir /tmp/ldbc --repeat 5
```

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

Run on 2026-09-26 against main at c651fd852, release build.

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

The 16 that run now and did not at 451e6f8a8 are IS 2, 6, IC 3, 4, 5, 6, 9, 11, 12 and
BI 1, 3, 7, 8, 9, 12, 18. Main implemented what 14 of them stopped at: variable-length paths
at 166c5f1e0, edge type disjunction at b1cae6bea, chained comparisons at ecc9143e7, pattern
comprehensions at d4428fe0f and pattern predicates at e8f5e1004. BI 12 stopped at the
parameter file, which spelled `$languages` as the string `'en'`. It is now `['ar', 'hu']`,
the query's own default. No post in this data set has the language `en`. BI 1 stopped at
`message.creationDate.year` on an integer. It runs now that the dates are loaded as
`DateTime`, with the component access main implemented at a6365bfd1.

The other 33 give the same answers on `DateTime` dates as on integer dates. Only the
rendering of a date changes, from `1290673245079` to `2010-11-25T08:20:45.079000Z`.

The answers are right, not just the row counts. The 16 new ones, BI 11, IC 8 and IS 3 were
recomputed in Python from the CSVs and match row for row, in order. BI 11 counts 25 friend
triangles in India. IC 8's top 20 replies match ids, names, dates and order. IS 3 returns
48 friends for person 4398046511333, its degree in `person_knows_person`.

Three of the new ones return 0 rows, and 0 is the answer on this data. IC 3's window holds
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
| BI 2 | `duration` |
| BI 13 | property access on a function call, `datetime('2010-11-25T08:20:45.079Z').year` |
| BI 16 | property access on a map, `UNWIND [{letter: 'A'}] AS param` then `param.letter` |
| BI 17 | a relationship with both arrowheads, `(forum1)<-[:HAS_MEMBER]->(person2)` |
| BI 4 | the `WHERE` of a `WITH` reading a variable the `WITH` does not project |

The last one is not a missing feature. It is a query the engine has every piece to run and
an analyzer rule turns away, so it is written up separately below.

### One rejection that looks too broad

It was reduced to the smallest query that reproduces it.

**`WHERE` after `WITH`** (blocks BI 4). Inside BI 4's `CALL`, `WITH person, message,
topForum2 WHERE topForum2 IN topForums` is rejected with `Variable 'topForums' not found`.
The smallest query that shows it is `WITH 5 AS k MATCH (p:Person) WITH p WHERE p.id > k
RETURN count(p)`. Projecting `k` as well runs. The Neo4j reference implementation runs BI 4
as written, so Neo4j lets the `WHERE` of a `WITH` read the variables in scope before it.
