# COLLECT subqueries in the v3 engine

`COLLECT { ... }` in the MLIR engine. The list-building sibling of `docs/count_subqueries.md`:
the body is scoped, generated and lowered the way a COUNT body is, and this page only
covers where the two differ. Implemented as of 2026-10-21.

## 1. Semantics

Neo4j is the reference; openCypher 9 has no `COLLECT { }`.

`COLLECT { ... }` is a list expression. For each row in flight it is the list of the values
the body returns for that row, in the order the body produced them, and `[]` where it
produces none. It changes no row count.

```
MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }
MATCH (p:Person) WHERE 'Gym' IN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } RETURN p.name
```

The body is correlated and read-only, as a COUNT body is, and binds nothing outside itself.
Unlike COUNT and EXISTS, it must end with a RETURN of exactly one column; the analyzer
rejects a body with no RETURN, or with 2 columns, `RETURN *` included. The projection
decides the values: `ORDER BY` orders the list, `LIMIT 2` keeps at most 2, and a keyless
aggregate `RETURN count(i)` is one value.

A null the body returns is an element of the list: `RETURN e.duration` over Remy's three
interests is `[20, null, 20]`. The `collect()` aggregate drops nulls; COLLECT does not.

A body can be a UNION, and every branch returns the same column name. `UNION ALL` is the
lists of the branches concatenated in branch order. `UNION` is the distinct values of the
branches. A chain mixing both follows UnionQuery, as COUNT does.

The result is a list column. Its element type is the body's column's, resolved during
lowering, as a pattern comprehension's is.

Out of scope: the conditional `WHEN ... THEN { } ELSE { }` body, as in COUNT and EXISTS.
`CollectNeo4jManualTest` holds the manual's example, skipped.

## 2. Ops

`db.collect_subquery` has the operands, body and `carries_scope` flag of
`db.count_subquery`. It yields through `db.collect_subquery_yield`, which names the tag, the
one returned column, and, for an untagged body, the columns it holds.

It lowers to the accumulator a pattern comprehension uses: one list per row of the step.

```
%state, %tag = nl.row_lists_buffer(%p) : !nl.chunk<!storage.node_id>
nl.for ... {
  nl.row_lists_collect %state, tag %tag2, %name : !nl.chunk<!storage.nullable<!storage.string>>
}
%lists = nl.row_lists_result(%state, %p) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<!storage.string>>
```

`db.pattern_comprehension` keeps its own db op and lowering, and ends on the same three nl
ops.

A body run one row at a time opens the `nl.each_row` loop a COUNT body does. The step is
then one row, and `nl.row_lists_collect` takes no tag: every value goes into that row's list.

## 3. UNION

A chain of `UNION ALL` is generated as one `db.collect_subquery` per branch, joined with
`db.concat`, so each branch keeps the tagged path when it can take it. A chain holding a
`UNION` is one `db.collect_subquery` over the `db.union` a CALL body's UNION generates, run
one input row at a time so the dedup covers that row alone.

## 4. Tests

`test/query/ir/subquery/CollectSubqueryTest.cpp` runs the queries over SimpleGraph.
`test/query/ir/subquery/CollectSubqueryCodegenTest.cpp` reads which path each body took.
`test/query/ir/subquery/CollectNeo4jManualTest.cpp` holds every example of the Neo4j manual's
COLLECT page, over the graph that page builds.
