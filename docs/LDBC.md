# Validating against LDBC SNB

## Why this document

`test/query/ir/LdbcBi11Test.cpp` runs LDBC SNB BI query 11 ("Friend triangles") against a
five-person graph written for the purpose, and asserts a count derived by hand. That shows
the query compiles and executes, and that the engine agrees with our reading of it. It is
**not** conformance: the expected value came from us, not from LDBC, so a misreading of the
query would be baked into both sides.

This is what closing that gap costs, and in what order.

## What LDBC ships

| artefact | where | note |
| --- | --- | --- |
| Queries | `ldbc/ldbc_snb_bi`, `neo4j/queries/bi-*.cypher` | 20 BI queries; Interactive lives in `ldbc_snb_interactive_v1_impls` |
| Data sets | `datasets.ldbcouncil.org/bi-pre-audit/` | zstd-compressed CSV, `composite-merged-fk` or `composite-projected-fk`, SF1 upwards |
| Substitution parameters | `ldbc-snb-bi-parameters-sf1-to-sf30000.zip` | pre-generated, so `paramgen` need not be run |
| Reference output | `output-sf10-validation-umbra.tar.zst` | SF10 only, generated with Umbra |
| Validation tool | `scripts/cross-validate.py` | diffs two implementations' `results.csv` |

There is no golden results file inside the repository - `parameters/` and `neo4j/output/`
are both gitignored. Validation is by cross-validation: one implementation's output is
declared expected and another's is diffed against it. The published SF10 Umbra output is
what makes that possible without standing up a second engine.

`results.csv` is one line per query instance, pipe-separated:
`query|variant|parameters|result-as-json`.

## What blocks us, and what does not

**The data cannot be loaded verbatim, by design.** `Person.csv`'s header is
`creationDate:DATETIME|id:ID(Person)|...|birthday:DATE|...|speaks:STRING[]|email:STRING[]`.
Two of those columns are list-valued, and a list-valued property is not representable here
(see the data model invariants in `CLAUDE.md`); `DATETIME`/`DATE` have no `ValueType`
either. So any run of ours is over a *converted* data set, and the conversion has to be
stated as part of the result.

**A converted date is enough for some queries and not others.** Convert a date to epoch
milliseconds and convert the query's date parameters the same way, and every comparison
orders identically - so a query that only *filters* on dates returns exactly what a
date-aware engine returns. A query that *returns* a date does not: its output differs
textually from the reference whatever we do. BI-11 is in the first group: it filters on
`creationDate` and returns nothing but a count, which is why it is the query to start with.

**Parameters have to be substituted as text.** `Expr::Kind` has no `PARAMETER`;
`ParameterExpr` is a forward declaration with no class behind it. Substituting literals is
correct for a correctness check but is not how the benchmark is meant to be driven.

## Phase 0 - one query, real data, real expected values

No engine work. Deliverables:

1. **Converter**, LDBC CSV to the two Parquet files `ParquetImporter` expects
   (`import/parquet/ParquetImporter.h`): a node file with `__id` INT64 and `__labels`
   list-of-string, an edge file with `__source`, `__target`, `__type`. Three things it must
   handle: LDBC ids are unique per entity type but `__id` is global, so each type needs an
   offset; `DATETIME`/`DATE` become INT64 epoch milliseconds; `speaks` and `email` are
   dropped. Input is headerless, `|`-separated, quoted, with headers supplied separately in
   `neo4j/headers/`.
2. **Runner** that reads BI-11's parameter rows, substitutes them into the query text,
   executes through `QueryInterpreterV3`, and writes LDBC's `results.csv` format.
3. **Compare** against `output-sf10-validation-umbra.tar.zst`, which needs the SF10 data
   set. If SF10 does not fit the machine, fall back to SF1 and cross-validate against
   Neo4j run locally over the *same converted* data - a weaker check, since it validates us
   against Neo4j rather than against LDBC.

What this buys: one query's answers confirmed against LDBC's own, at scale, over data we
did not choose. What it does not: anything about the other 19.

## Phase 1 - what each further step unlocks

Measured over the 26 of 41 SNB queries that use `WITH` (`Interactive` short 1-7 and
complex 1-14, `BI` 1-20):

| missing feature | queries it blocks | note |
| --- | --- | --- |
| `collect()` | 17 | `db.collect` and `db.unwind_collect` exist and are tested; the frontend refuses it beside a grouping key |
| variable-length paths | 13 | see `CYCLE_TRAVERSAL.md` for the semantics to settle first |
| `OPTIONAL MATCH` | 11 | BI-5 needs nothing else |
| `UNWIND <variable>` | 9 | almost always paired with `collect()` |
| `CASE` | 7 | IC-4 needs nothing else |
| date functions | 6 | `datetime()`, `.year`, durations |
| list/string functions | 5 | |

`collect()` plus `UNWIND <variable>` are usually the same idiom
(`WITH k, collect(x) AS xs UNWIND xs AS y`), the ops are already in the dialect, and
between them they gate 17 of the 26. That is the first feature to take.

A **date type** is a prerequisite for validating any query that returns a date, which is
most of Interactive. It reaches storage - `ValueType`, the column types, comparison, the
importer - so it is the largest single item here and worth deciding on its own terms rather
than as a benchmark chore.

## Not in scope

An audited LDBC result. That needs the full query set, the driver, and a review process;
this document is about correctness validation, which is useful long before a publishable
number is.

## Provenance

Query classification is by pattern-matching the 41 query texts, with the near-misses read
by hand. Everything above about LDBC's artefacts was read from the repository and the
data-set index; no data set has been downloaded, so nothing here should be read as a
statement about their size or load time.
