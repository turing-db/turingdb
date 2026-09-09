---
description: Review recent main, hunt for Cypher bugs on the v3 engine, land the tests in a PR
argument-hint: "[optional focus, e.g. 'MERGE' or 'aggregates']"
allowed-tools: Bash, Read, Write, Edit, Glob, Grep, Agent, TodoWrite
---

# QA hunt on the v3 engine

Find Cypher queries that v3 gets wrong, pin each one with a unit test, and open a PR
carrying the tests. The PR is meant to be red — a failing test is the bug report.

`$ARGUMENTS` names the area to focus on. Empty means the review picks the areas.

Run the phases in order. Only phase 3 is on a clock. `<scratch>` below is this session's
scratchpad directory.

## 1. Sync main

```bash
git status --porcelain
git checkout main && git pull --ff-only
```

Tracked modifications abort the run: report them and stop. Untracked files are fine —
this working tree normally carries test `.out/` directories and scratch turing dirs.

## 2. Review what landed in the last 5 days

```bash
git log --since="5 days ago" --date=short --pretty=format:'%h %ad %s'
git diff "$(git rev-list -1 --before='5 days ago' main)"..main --stat
```

Read the diff. Review the whole of it, but rank every finding by one question: **can a
Cypher query reach this?** A wrong offset in a chunk writer that a `MATCH` can hit outranks
anything in `net/`, `python/` or CI.

Write `<scratch>/qa/review.md`: the findings, then a ranked list of hypotheses. Each
hypothesis must be a **query shape**, not a code observation — "an OPTIONAL MATCH whose
null row reaches a grouping key", not "the null mask handling looks fragile". A hypothesis
you cannot write a query for is not a hypothesis, it is a code-review note; keep it in the
findings section and move on.

Report the top hypotheses to the user in a few lines before starting phase 3.

## 3. Hunt (1 hour)

```bash
DEADLINE=$(( $(date +%s) + 3600 ))
```

Re-read `date +%s` at the top of every iteration. Never `sleep` to pass time.

### Fixture, once

```bash
./build/samples/simpledb/simpledb -o <scratch>/qa/fixture -turing-dir <scratch>/qa/fixture-turing
mkdir -p <scratch>/qa/turing/graphs
cp -r <scratch>/qa/fixture/simpledb <scratch>/qa/turing/graphs/simpledb
```

SimpleGraph is 18 nodes and is defined in `examples/SimpleGraph.cpp` — read it once and
keep the node table to hand, every expected result is derived against it. `/tmp/ldbcdb` is
a second, larger fixture when a hypothesis needs scale.

### The two probes

Read-only, v3 only, ~1s, no port:

```bash
./build/samples/mlir/mlir -q "<query>" -e -g <scratch>/qa/fixture/simpledb
```

Add `-d` for the db dialect and `-l` for the lowered nl dialect when triaging a hit.

Differential, v2 then v3, same process, one query each:

```bash
printf 'MATCH (n) RETURN count(n)\n#v3 MATCH (n) RETURN count(n)\n' \
  | ./build/tools/turingdb/turingdb -turing-dir <scratch>/qa/turing -load simpledb -p 71NN
```

`#v3` routes that one query through `QueryInterpreterV3`; the bare line runs v2. Give each
concurrent shell its own port (`-p 7101`, `7102`, …) — the default 6666 collides.

Writes (`CREATE`, `MERGE`, `DELETE`, `SET`) go through the shell only; the mlir driver has
no isolated turing directory. In the shell a write needs the change dance: `CHANGE NEW`
prints the id but does not select it, so `checkout change-0`, then the writes, then
`COMMIT` before anything can read them, then `CHANGE SUBMIT` and a bare `checkout`.

### Each iteration

Fan out 3-5 `qa-probe` subagents in one message, one hypothesis each, distinct ports.
Each returns candidates with a verdict. You triage; probes never write to the repo.

A candidate is a **hit** only when all four hold:

1. It reproduces twice from a clean fixture.
2. You can state the expected result and the openCypher rule that decides it —
   independently of what either engine printed. v2 is a witness, not an oracle; both
   engines can be wrong at once.
3. It is not on the known-limitations list below.
4. It is reduced to the smallest query that still fails.

Prefer four distinct root causes over four shapes of one bug. When a probe finds several
shapes of the same cause, keep the clearest one and keep hunting.

A probe's `OUTPUT-BUG` — the engine holds the right rows and a client cannot render them —
is a finding but not a hit. Verify it the way you verify a hit, then record it in
`<scratch>/qa/hits.md` under a heading of its own and carry it into the PR body in phase 7.
It does not fill one of the four test slots, because the harness hands
`QueryInterpreterV3` its own sink and a test of it would pass. Keep hunting for four hits
beside it.

Keep `<scratch>/qa/hits.md` current: query, expected, v3 actual, v2 actual, the rule.

Stop the loop at 4 hits, or at the deadline.

### The bug classes worth hunting

- **A valid query rejected.** The richest class. CLAUDE.md is explicit that an analyzer or
  codegen rejection of valid openCypher is a regression dressed as validation. An
  `Unsupported ...` or `ANALYZE_ERROR` on a query Neo4j accepts is a hit.
- **Wrong rows.** Missing, duplicated, mis-grouped, mis-ordered, or wrongly-null.
- **Crash or assertion.** `bioassert` does not abort — it throws a catchable
  `FatalException` whose message opens `Internal Error: The assertion '<expr>' failed at`.
  That string in a query error is an internal logic error, not a user-facing rejection,
  and always a hit.
- **A result the client cannot print.** The rows are right and the user still never sees
  them. `MATCH (n) RETURN n:Person` returns 18 rows and the shell answers
  `EXEC_ERROR: Unsupported unary operation on column of type db::ColumnMask` with no rows,
  because `writeColumnCell` in `tools/turingdb/TuringShell.cpp` excludes ColumnMask; the
  mlir driver prints `?` for every Bool column. Worth hunting and worth reporting, but a
  finding rather than a hit — see above.

### Known limitations — not bugs

- `CONTAINS`, `STARTS WITH`, `ENDS WITH` are unimplemented. `StringOperator` in the AST and
  `ExprAnalyzer::analyzeStringExpr` exist and mean nothing; there is no execution path.
- Variable-length paths are rejected by v3 on purpose (`MLIR: reject variable-length paths`).
- The data model cannot represent the violation, so a query that assumes otherwise is not a
  bug: every node has at least one label, every edge exactly one type, and properties are
  scalars — there is no list-valued property.
- A write is invisible to a later `MATCH` in the same change until `COMMIT`. Missing rows
  without an intervening `COMMIT` are the documented semantics.
- LSH vector search may return fewer than k results; that is the algorithm, not a defect.
- A bare `Killed` is the OOM killer on a contended machine. Re-run.
- The `Unsupported ...` strings in `query/ir/` are a map of what is genuinely unbuilt. A
  rejection listed there is still a hit **if the query is valid openCypher** — that is the
  gap, not the excuse. Judge by the language, never by how far the engine reaches.

## 4. If the hour ends with fewer than 4 hits

Switch to coverage. For each feature merged in the review window, list the query shapes its
tests already cover, then write tests for the adjacent shapes they do not: the empty input,
the null, the aggregate over it, the second one in the same query, the interaction with
`WITH`, `SKIP`/`LIMIT`, `DISTINCT`, `ORDER BY`. These are expected to pass. One that fails
is a hit — take it and count it.

Make up the number to 4 tests either way.

## 5. Write the tests

One bug, one file, one target. Follow `test/query/ir/AnonymousPatternPropertyTest.cpp`
exactly: `TuringTest` + `TuringTestEnv` + `QueryInterpreterV3` + `SimpleGraph` +
`StringRowSink`, with `expectRows` / `expectError` helpers.

- New `test/query/ir/<Behaviour>Test.cpp`, plus `add_ir_tests(test_query_ir_<behaviour>
  <Behaviour>Test.cpp)` in `test/query/ir/CMakeLists.txt`. Add
  `target_link_libraries(... turing_db_examples_s)` when the test uses SimpleGraph.
- Never append to an existing test file.
- Name the test for the behaviour it asserts, never for the bug or the engine.
- The expected rows are the openCypher-correct ones. Do not encode what v3 prints today.
- Comments: at most one 2-4 line class-level note stating the invariant under test, only if
  the name cannot carry it. Nothing else. No annotation of which test fails.
- The rest of the C++ style rules in CLAUDE.md apply in full.

Write no test for an output-layer finding. The test passes the sink of its choice to
`QueryInterpreterV3`, so the query the shell cannot print hands back its rows here and the
assertion holds. The PR body reports it instead.

## 6. Build and run

```bash
make -j8 test_query_ir_<name>          # from build/, one target at a time. No cmake ..
./build/test/query/ir/test_query_ir_<name>
```

Every test must compile and every assertion must be a real comparison. Capture the failure
output of each failing test — it goes in the PR body.

## 7. Commit and open the PR

Branch `qa/<YYYY-MM-DD>-<slug>`. One commit per test, subject in the repo's style — one
short area-prefixed line, no body:

```
QA: pin the grouping of a null OPTIONAL MATCH key

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
```

The co-author line is deliberate here and overrides CLAUDE.md's no-co-author rule: /qatest
output is machine-found and is marked as such.

PR title carries the detail — `QA: 4 failing Cypher tests on v3`. The body is one
imperative sentence, then a fenced code block of bare facts: each query, its expected rows,
and what v3 returned. No headers, no bullets, no prose paragraphs.

```
Pin four Cypher queries v3 answers wrong.

<query>
expected: ...
v3:       ...

not pinned, <the client> cannot print the result:
<query>
expected: ...
<the client>: ...
```

An output-layer finding goes at the end of that same block, under the line naming the
client, so the body stays one sentence and one code block. The title still counts tests.

End the body with the Claude Code footer. `gh pr create` works; `gh pr edit` does not on
this repo — edit a body afterwards with
`gh api -X PATCH repos/turing-db/turingdb/pulls/<n> -f body=...` and verify with
`gh pr view <n> --json body -q .body`.

Say plainly in the final message that CI will be red and why, and name any output-layer
finding the PR carries without a test.
