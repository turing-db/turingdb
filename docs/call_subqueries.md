# CALL subqueries in the v3 engine

Implementation plan for `CALL { ... }` subqueries in the MLIR engine. It records what the
engine had before the work, the semantics to implement, the design, and the order of the
work. Phases 1 to 3 are implemented as of 2026-09-16; section 7 records what the
implementation settled that the plan left open.

## 1. Semantics

Neo4j is the reference; openCypher 9 does not have the clause.

The body runs once per incoming row. The variables it can read are the ones the scope
clause `CALL (a, b) { ... }` names, or the ones a leading importing `WITH a, b` names.
An importing WITH holds plain variable references only, with no alias, WHERE, DISTINCT,
ORDER BY, SKIP or LIMIT. `CALL () { ... }` and a body opening on no WITH import nothing.

What the scope clause imports stays readable through the whole body: "A subsequent WITH
within the subquery cannot descope an imported variable", and a clause of the body cannot
declare an imported name again. Both rules are the scope clause's; a name imported through
a leading WITH is an ordinary projection, which an ordinary WITH below it descopes.

A returning subquery ends on RETURN. Each of its rows is appended to the input row it
came from, so the row count after the CALL is the sum over input rows of the inner row
counts, and an input row the body produces nothing for is dropped. Aggregation, DISTINCT,
ORDER BY, SKIP and LIMIT inside the body apply per input row: `RETURN x ORDER BY x.name
LIMIT 2` is a top 2 for each input row. A keyless aggregate over zero inner rows still
yields one row, `count` giving 0.

A unit subquery ends on an updating clause and has no RETURN. Every input row passes
through once, whatever the body wrote and however many rows it walked.

A returned name may not already exist in the outer scope; an imported variable can only
be returned under an alias.

`OPTIONAL CALL` keeps an input row the body produces nothing for, with the returned
columns null, exactly as OPTIONAL MATCH does. Over a unit body it does nothing: such a body
yields nothing for every input row and every row passes through whatever it wrote, so there
is no row to pad. `OPTIONAL CALL (p) { CREATE (:Audit) }` answers as the same query without
the keyword.

Out of scope: `CALL { ... } IN TRANSACTIONS`, and UNION inside the body until UNION itself
is implemented.

```
MATCH (p:Person)
CALL (p) {
  MATCH (p)-[:INTERESTED_IN]->(i)
  RETURN i ORDER BY i.name LIMIT 2
}
RETURN p.name, i.name

MATCH (p:Person)
CALL { WITH p MATCH (p)-->(x) RETURN count(x) AS degree }
RETURN p.name, degree

MATCH (p:Person)
CALL (p) { CREATE (p)-[:VISITED]->(:Place {name: 'Paris'}) }
RETURN count(p)
```

## 2. What exists

### Parser and AST

The grammar recognises every shape and rejects it as not implemented:
`CALL OBRACE query CBRACE`, `CALL OPAREN CPAREN OBRACE query CBRACE` and
`CALL OPAREN callCapture CPAREN OBRACE query CBRACE` at `query/parser/CypherParser.y:979`.
`callCapture` accepts `symbol AS symbol`, which Neo4j forbids, and there is no `CALL (*)`.
The nonterminal `query` yields a `QueryCommand*` and also covers `LOAD GRAPH`, `CHANGE`,
`EXPLAIN` and the UNION stub, so the body has to be checked to be a `SinglePartQuery`.

`CallStmt` (`query/AST/stmt/CallStmt.h`) is the procedure call: an invocation, a YIELD, an
optional flag and a standalone flag. `Stmt::Kind` has no subquery kind and
`Stmt::isUpdating` decides which half of a part a clause belongs to.

`SinglePartQuery::create` (`query/AST/SinglePartQuery.cpp:17`) creates a `DeclContext` with
no parent and appends the query to `CypherAST::_queries`. `CypherAnalyzer::analyze` walks
that list, and `DBProgramGenerator::generate` throws "Multiple queries not yet supported"
when it holds more than one, so a nested query must not be registered there.

### Analyzer

Scoping is already the right shape. `DeclContext::getDecl` reads its own map and never
walks the parent (`query/AST/decl/DeclContext.cpp:30`). `CypherAnalyzer::openWithScope`
(`query/analyzer/CypherAnalyzer.cpp:340`) creates a fresh context holding one declaration
per published column and points the expression, read and write analyzers at it, so the
statements after a WITH resolve nothing the projection dropped. A subquery scope is the
same mechanism seeded with the imported declarations.

`CypherAnalyzer::analyze(const SinglePartQuery*)` at line 193 holds the per-query rules
the body needs again: `throwOnReadAfterUpdate`, the RETURN being mandatory unless the
query writes, `WriteStmtAnalyzer::startPart` (`query/analyzer/WriteStmtAnalyzer.h:48`).
`ReadStmtAnalyzer::analyze(const CallStmt*)` at `query/analyzer/ReadStmtAnalyzer.cpp:151`
rejects `OPTIONAL CALL` for procedures. WITH is v3 only and is rejected under v2 at
`CypherAnalyzer.cpp:263`; subqueries follow the same rule.

### Codegen

`DBProgramGenerator::generateQueryParts` (`query/ir/codegen/DBProgramGenerator.cpp:1076`)
splits the statements at each WITH and at each cut, and `generateOptionalParts` splits
again at each OPTIONAL MATCH. Each part is generated by `generatePart` over a fresh
variable dependency graph.

OPTIONAL MATCH is the template for a correlated region, `generateOptionalMatch` at line
3851. It collects every column in scope with `collectPublishedColumns` (line 4104,
deduplicated by name and sorted), makes each an argument of a scratch block, adds a
`!db.column<ui64>` row tag argument, rebinds the scope to those arguments through
`rebindScopeKeepingWrittenEntities`, registers the tag as a bound variable under
`optionalTagName` (line 223, a backtick name no query can spell), calls `generatePart` in
the block, and terminates with `db.optional_yield` listing the inputs as the pattern left
them then the pattern's own variables. The op's results take the scope over.

WITH is the template for the inner RETURN. `generateWith` at line 3822 runs
`generateGroupAggregate`, `translateProjection`, `translateProjectionTail` (line 4230,
DISTINCT then ORDER BY then SKIP then LIMIT over every column in flight) and
`publishBoundColumns`, and emits no `db.output`.

Procedure CALL holds the uncorrelated case. `generateCall` at line 2438 checks whether the
call reads any in-flight column, and `generateCrossedCall` at line 2846 crosses the rows
of one that does not with the rows in flight.

`collectInFlightColumns` at line 1460 only takes columns bound in the current block, which
is what keeps a region's carry sets to the rows of its own step.

### db dialect

`db.optional_match` at `query/ir/dialect/db/DBOps.td:2146` and `db.optional_yield` at line
2204 are the region op and terminator to copy: one `SizedRegion<1>`, one block argument
per input column plus the trailing tag, results lined up with the yield.
`db.cross_product` and `db.hash_join` at lines 833 and 888 hold regions with no block
arguments and recover their result types from their `db.yield`s.

### Lowering

`DBLowering::lowerOptionalMatch` (`query/ir/lowering/DBLowering.cpp:1572`) maps each block
argument to the input chunk, finds the step block with `deepestOwnerBlock`, creates the
`nl.optional_buffer` there, roots the body's loops in that block by swapping `_rootBlock`,
`_innermostLoopBody` and `_innermostCardinality` and restoring them after, lowers the body
op by op with `lowerOperation`, places `nl.optional_collect` at the deepest yielded chunk
and drives `nl.optional_drain` through `buildLoopForSource`.

Every pipeline breaker hoists its state to the function entry block, and every site says a
correlated form would hoist into its enclosing loop body instead: limit handles in the
pre-scan at line 814, `lowerSort` 2023, `lowerRemoveDuplicates` 2090, `lowerCount` 2127,
`lowerAggregate` 2220, `lowerGroupAggregate` 2314, `lowerCollect` 2432,
`lowerShortestPath` 2497, `lowerUnwindCollect` 2561, `lowerCallProcedure` 2656. The limit
pre-scan walks the whole function, nested regions included, and `detectTopKFusion` runs
ahead of it.

### Runtime

The translator already resets a state op per step of the loop whose body holds it:
`translateLimit` at `query/ir/exec/NLTranslator.cpp:2463` says "once at function
scope for a top-level LIMIT, per enclosing step for a nested one", and `translateSortBuffer`
at line 2701 empties its buffers each time its block runs. `nl.optional_buffer`,
`nl.optional_collect` and `nl.optional_drain` (`query/ir/dialect/nl/NLOps.td:2561`) are the
working example of per-step state: `NLOptionalState` at `query/ir/exec/NLProgram.h:3364`
borrows the step's input chunks, keeps one matched flag per input row and one buffer per
yielded column, `runOptionalReset` at `NLExecutor.cpp:5599` clears it and lays the tag out
as 0..n-1, and `runOptionalDrainLoop` at line 5639 re-chunks the matched rows then pads
the missed ones.

## 3. Design

### Front end

Add `Stmt::Kind::CALL_SUBQUERY` and a `CallSubqueryStmt` holding the imported symbols, the
optional flag and the nested `SinglePartQuery*`. `Stmt::isUpdating` answers true for a body
with no RETURN that writes (`SinglePartQuery::writesToTheGraph`, which recurses through a
nested subquery), so `CypherAnalyzer::analyze(SinglePartQuery)` and `generatePartStatements`
place a unit subquery with the updating clauses and a returning one with the reading ones.
A body that only reads is the reading clause it is, and what it is told about is the RETURN
it is missing.

The parser builds the body with a constructor that creates the `DeclContext` and skips
`CypherAST::addQuery`, and rejects a body that is not a `SinglePartQuery`. A leading WITH
of the body whose items are all plain symbols is the importing form; the parser leaves it
in place and the analyzer reads it as the import list.

The analyzer, for one subquery: check each imported name resolves in the current context;
create a `DeclContext` and declare one variable per import with the outer declaration's
type and list shape; save `_ctxt`, point the three analyzers at the new context, run the
body through `analyze(SinglePartQuery)` with `startPart`, restore `_ctxt`; for a returning
body, declare each published name of the inner projection in the outer context and reject
a name already declared there; reject an importing WITH carrying WHERE, DISTINCT, ORDER
BY, SKIP, LIMIT or an alias; reject under v2.

### db op

```
%p2, %h2, %x = db.call_subquery(%p, %h) ({
  ^bb0(%pIn: !db.column<!storage.node_id>, %hIn: !db.column<none>, %tag: !db.column<ui64>):
    %s, %e, %et, %x, %hIn2, %tag2 = db.get_out_edges(%pIn, {%hIn, %tag}) : ...
    db.subquery_yield %tag2, {%s, %hIn2, %x} : {...}
}) {unit = false, optional = false} : (!db.column<!storage.node_id>, !db.column<none>) -> (...)
```

`db.call_subquery` is `db.optional_match` with two attributes. `input_columns` are every
column in scope, one block argument each plus the trailing row tag. The terminator lists
the tag, then the inputs as the body left them, then the RETURN columns, and the results
line up with it, the `db.optional_yield` contract. A unit subquery yields nothing, has no
results, and is not Pure, so the outer columns stay bound to the operands and the op
survives dead-code elimination.

Every in-flight column enters the region so the body's hops carry it through. A column the
scope clause does not import is bound inside under a hidden name, `` `hidden_<name>``,
which the body cannot spell and cannot collide with, and comes back under its own name.

### Codegen

`generateCallSubquery` copies `generateOptionalMatch`: collect the published columns, split
out the constants, build the scratch block and its arguments, rebind the scope with the
imports under their names and the rest under hidden names, register the tag. Inside the
block run `generateQueryParts` on the body, then for a returning body a `generateWith`-style
projection of the inner RETURN that publishes rather than outputs. Collect the published
columns again, yield inputs first then the RETURN columns, create the op, rebind the outer
scope to its results under the outer names. A unit body ends on `generateUpdates` and
yields nothing.

Codegen emits the breakers of the body as it would at top level, `db.sort`, `db.limit`,
`db.group_aggregate` inside the region, and does not choose how they run. That is the
lowering's decision, in line with the no-optimisation-in-codegen rule.

### Lowering

Two strategies, chosen by whether the region holds a pipeline breaker: a sort, skip,
limit, remove_duplicates, count, aggregate, group_aggregate or collect. A procedure call is
not one: `generateCrossedCall` crosses its rows with the rows in flight, so each row it
makes still carries the input row it was paired with, and `statementCarriesRows` classes it
with the clauses that keep the pairing.

Inline, for a body with none. This is `lowerOptionalMatch` without the buffer, collect and
drain: map the block arguments to the step chunks, root the body in the step block, lower
it, map the results to the yielded chunks. The body's hops fan the step's rows out and
carry the inputs along, which is what the rows after the CALL are. A unit body lowers the
same way and maps nothing back.

```
nl.for %p in %persons {
  %edges = nl.get_out_edges(%p, {})
  nl.for %s, %e, %et, %x in %edges {
    nl.output(%s, %x)
  }
}
```

Per row, for a body with a breaker. A new iterator `nl.each_row {%p, %h}` yields one-row
chunks of the step's input columns. The body roots in the `nl.for` over it, and every
hoist site that writes `_builder.setInsertionPointToStart(_entryBlock)` writes
`_rootBlock` instead, so a breaker's state sits in that body and the translator resets it
once per input row. Nothing in the breakers changes: a top 2 is a top 2 of one row's inner
rows, `nl.count_result` emits its one row at the body's scope so an empty count gives 0,
and a distinct dedups one row's rows. A breaker that drops its carried columns, a grouped
aggregate, gets them back through an `nl.cross_product {%p1, %h1} {yielded}` of the
one-row chunks against its emitted chunks.

```
nl.for %p in %persons {
  %rows = nl.each_row {%p}
  nl.for %p1 in %rows {
    %state = nl.sort_buffer ... top_k 2
    %edges = nl.get_out_edges(%p1, {})
    nl.for %s, %e, %et, %i in %edges {
      %name = nl.get_node_properties(%i, %nameType)
      nl.sort_collect %state, (%s, %i, %name)
    }
    %sorted = nl.sort(%state)
    nl.for %s2, %i2, %name2 in %sorted {
      nl.output(%s2, %i2, %name2)
    }
  }
}
```

Per-row driving costs one handler dispatch per input row plus one reset per breaker. The
inner traversal stays chunked over the row's own adjacency. The vectorised forms come
later as lowering specialisations of the same db op: a grouped aggregate keyed by the row
tag for an aggregating body, and a partitioned top-k for `ORDER BY ... LIMIT`.

The limit pre-scan hoists every `db.limit` handle to the entry block. A limit inside a
subquery region hoists into the region's root block, so the pre-scan has to record which
region each limit sits in.

### OPTIONAL CALL

`optional = true` wraps either strategy in the existing `nl.optional_buffer`,
`nl.optional_collect` and `nl.optional_drain`: the body's yielded chunks are collected
against the tag, and the drain pads each input row nothing came back for with null in the
RETURN columns.

## 4. Phases

Phase 1. Parser, AST and analyzer, `db.call_subquery` and `db.subquery_yield` with their
verifiers, codegen, inline lowering. Covers every unit subquery and every returning body
with no breaker.

Phase 2. `nl.each_row`, `_rootBlock` hoisting, the limit pre-scan per region, the cross
product rejoin. Covers aggregation and cuts inside the body.

Phase 3. `optional = true` over the optional buffer.

Later. Tag-keyed grouped aggregation and partitioned top-k as lowering specialisations.
UNION inside the body once UNION exists. `CALL (*)`.

## 5. Tests

One new test file per phase under `test/query/ir/`, on simpledb, in the
`WithMultiPartTest.cpp` harness. Expected rows are derived from the fixture when the test is
written.

```
Phase 1
  scope: a body reading a variable the scope clause did not import is rejected
  scope: a returned name already in the outer scope is rejected
  scope: an importing WITH with WHERE, DISTINCT, ORDER BY, SKIP, LIMIT or an alias is rejected
  scope: a variable the body declares and does not return is invisible after the CALL
  rows:  MATCH (p:Person) CALL (p) { MATCH (p)-->(x) RETURN x } RETURN count(x)
         equals MATCH (p:Person)-->(x) RETURN count(x)
  rows:  an input row the body matches nothing for is dropped
  unit:  MATCH (p:Person) CALL (p) { CREATE (p)-[:VISITED]->(:Place) } RETURN count(p)
         returns the Person count, and one edge per Person is written
  codegen: the region takes every in-flight column, the non-imported ones under hidden names

Phase 2
  top-k per row: RETURN i ORDER BY i.name LIMIT 2 inside gives at most 2 rows per Person
  count per row: RETURN count(x) AS degree inside gives one row per Person, 0 included
  distinct per row: RETURN DISTINCT labels(x) inside dedups within a Person only
  duplicates: UNWIND [1, 1] AS k MATCH (p:Person {name: 'Remy'}) CALL (p) { ... RETURN count(x) AS c }
              gives two rows, not one
  nesting: a subquery inside a subquery, and an OPTIONAL MATCH inside a subquery

Phase 3
  OPTIONAL CALL keeps the Persons the body matched nothing for, with null returned columns
```

## 6. Open points

Whether the scope clause keeps accepting `a AS b`, which the grammar allows and Neo4j does
not.

Whether an inner `RETURN p.name` without an alias is accepted. A WITH requires the alias
today, and treating the inner RETURN as a WITH inherits that.

Whether `TrimUnreadColumns` should trim the carry sets inside the region. It does not
descend into `db.optional_match` today, so an untrimmed region is the existing behaviour.

## 7. What the implementation settled

The block arguments are every column in flight, imported or not, in both forms. The
imports are bound in the body under the declarations the body's own `DeclContext` holds
for them. A body carrying its scope binds every input under a hidden name too,
`` `hidden_<name>``, that no clause can spell; a body run per row binds the imports
alone, since the lowering re-attaches the row. A barrier inside the body appends the hidden
columns to what it publishes (`appendHiddenColumns`), which is how a WITH in the body keeps
the input rows beside what it projects.

The op carries three unit attributes. `unit` is a body with no RETURN: no results, the
rows in flight stay the operands. `carries_scope` is a body none of whose clauses
aggregates, sorts, cuts, dedups or runs a SHORTESTPATH
(`DBProgramGenerator::subqueryCarriesRows`): a returning one yields the inputs as its rows
left them, read off the hidden names, then the RETURN columns, and either lowers in place.
Without it the body runs one input row at a time, a unit one writing over one row per step;
a returning one yields the RETURN columns alone and `lowerSubqueryPerRow` drives it through
`nl.each_row`, hoists its accumulators and limit handles into the row loop, and crosses the
row's one-row chunks with what the body yielded. `optional` wraps either in the optional
buffer, collect and drain; the body carries the row tag itself only when it carries the
scope over input rows it has. It is set for a returning body alone
(`optional = returning && subquery->isOptional()`): OPTIONAL over a unit body is accepted
and does nothing, since there is no row it yields for the drain to pad.

Every pipeline breaker now hoists its state to `_rootBlock` rather than `_entryBlock`, which
is the entry block at top level and the row loop's body inside a per-row subquery. The
limit pre-scan became `hoistLimitHandles`, run once at function level for the limits held
by no per-row body and once per per-row body for its own.

The importing WITH is the body's leading WITH when the CALL has no scope clause; the
analyzer reads its items as the import list and then analyzes it as an ordinary WITH.
`CALL () { ... }` and a body opening on no WITH import nothing. A returned name already in
the outer scope is rejected. A returning subquery after an updating clause of the same
part is rejected by the read-after-update rule, as any reading clause is; a WITH between
them lifts that.

Tests: `CallSubqueryTest.cpp` (carrying bodies and scoping), `CallSubqueryWriteTest.cpp`
(unit bodies), `CallSubqueryPerRowTest.cpp` (aggregation, ORDER BY, SKIP, LIMIT and
DISTINCT per input row), `CallSubqueryOptionalTest.cpp`, `CallSubqueryCodegenTest.cpp`.

A barrier of the body carries the imports it does not project: `carrySubqueryImports`
appends each of them to the projection as a bare variable item, so they ride the barrier
the way its own items do, a cut keeps them with the rows it keeps and a dedup reads them
beside the rest of the row. This holds because an import has one value per invocation, so
it tells no two rows of that invocation apart. The one barrier it stops at is a reduction
over no grouping key: carrying the import would key the reduction, and a keyed reduction
over no row reports no group where a keyless one still reports its single row.

Tested against the 21 examples of Neo4j's CALL subquery manual page on its own dataset.
Every example whose other features the engine has returns Neo4j's rows, and each example
the page marks as rejected is rejected. The rest need UNION, `CALL (*)`, conditional
`WHEN ... THEN`, `REMOVE`, `range()` or `rand()`, none of which is about subqueries.

Still open: the vectorised forms of section 4, UNION inside the body, `CALL (*)`, an import
read below a keyless reduction in the body, and trimming inside the region. The scope
clause alias is settled: the grammar rejects `CALL (t AS teams)`, as Neo4j does.
