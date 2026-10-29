# COUNT subqueries in the v3 engine

`COUNT { ... }` in the MLIR engine. The counting sibling of `docs/exists_subqueries.md`: the
body is scoped, generated and lowered the way an EXISTS body is, and this page only covers
where the two differ. Implemented as of 2026-09-28.

## 1. Semantics

Neo4j is the reference; openCypher 9 has no `COUNT { }`.

`COUNT { ... }` is an integer expression. For each row in flight it is the number of rows
the body produces for that row, 0 where it produces none. It changes no row count.

```
MATCH (p:Person) RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() }
MATCH (p:Person) WHERE COUNT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true } > 1 RETURN p.name
```

The body is correlated and read-only, exactly as an EXISTS body is, and binds nothing
outside itself. A RETURN in the body is optional. Its projection decides the rows counted:
`RETURN DISTINCT i.isReal` counts distinct values, `RETURN i LIMIT 2` counts at most 2, and
a keyless aggregate `RETURN count(i)` is one row, so it counts 1.

A body can be a UNION. `UNION ALL` counts every row of every branch, and a branch needs no
RETURN. `UNION` counts the distinct rows the branches return, so every branch needs a
RETURN naming the same columns, as Neo4j requires. A chain mixing both follows UnionQuery:
`A UNION ALL B UNION C` counts distinct(A ++ B ++ C), `A UNION B UNION ALL C` counts
distinct(A ++ B) ++ C.

A body can be a WHEN: it counts the rows of the branch its predicates pick for the row, and
a branch needs no RETURN, as in an EXISTS body.

The result is a `ui64` column, as `count(*)` is. A `UNION ALL` body adds its branch counts
together, and that sum is an `i64`, as any arithmetic over a count is.

## 2. Ops

`db.count_subquery` has the operands, body and `carries_scope` flag of
`db.exists_subquery`, and yields through `db.count_subquery_yield`. Its result is a
`!db.column<ui64>`.

It lowers to three nl ops, the counting siblings of the EXISTS ones:

```
%state, %tag = nl.count_subquery_buffer (%p) : {!nl.chunk<!storage.node_id>}
nl.for ... {
  nl.count_subquery_tally %state, %tag2              // tagged: +1 per tag entry
}
%n = nl.count_subquery_result(%state, %p) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
```

A body with no tag - one run per row, or one over the single empty row - is tallied off the
row count of a chunk it holds: `nl.count_subquery_tally %state rows(%k : ...)`. That chunk
is the first one that is not a constant, since a constant holds one cell for every row. A
body run per row names the row it counts toward, `nl.count_subquery_tally %state row %r
rows(...)`, with the tag `nl.each_row` hands it.

## 3. UNION

A chain of `UNION ALL` is generated as one `db.count_subquery` per branch, added together,
so each branch keeps the tagged path when it can take it. A chain holding a `UNION` is one
`db.count_subquery` whose body is the `db.union` a CALL body's UNION generates
(`generateSubqueryUnion`), run one input row at a time so the dedup covers that row alone.

## 4. A body run per row

A body run one row at a time opens an `nl.each_row` loop over the step's chunks and the
buffer's tag. The accumulator is opened over the whole step ahead of the loop, and the counts
are read over the step once the loop closes:

```
%state, %tag = nl.count_subquery_buffer(%p) : {!nl.chunk<!storage.node_id>}
%rows = nl.each_row{%p, %tag} : {!nl.chunk<!storage.node_id>, !nl.chunk<ui64>}
nl.for %p1, %r in %rows {
  ...
  nl.count_subquery_tally %state row %r rows(%k : !nl.chunk<!storage.node_id>)
}
%n = nl.count_subquery_result(%state, %p) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
```

So the op is one count per row of the step on either path, and nothing after it is lowered
inside the loop. It stands wherever an expression does - a CASE branch, a comprehension, a
reduce, an UNWIND - and a column read off the step before it is read after it unchanged:

```
MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 }
```

An outer LIMIT budgets the scan feeding the step, so at most one chunk of rows runs the body
past the budget.

## 5. Tests

`test/query/ir/subquery/CountSubqueryTest.cpp` runs the queries over SimpleGraph.
`test/query/ir/subquery/CountSubqueryCodegenTest.cpp` reads which path each body took.
`test/query/ir/subquery/CountNeo4jManualTest.cpp` holds every example of the Neo4j manual's COUNT
page, over the graph that page builds.
