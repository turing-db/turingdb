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
is the first one that is not a constant, since a constant holds one cell for every row.

## 3. UNION

A chain of `UNION ALL` is generated as one `db.count_subquery` per branch, added together,
so each branch keeps the tagged path when it can take it. A chain holding a `UNION` is one
`db.count_subquery` whose body is the `db.union` a CALL body's UNION generates
(`generateSubqueryUnion`), run one input row at a time so the dedup covers that row alone.

## 4. A body run per row, beside other columns

A body run one row at a time opens an `nl.each_row` loop over the step, and the rest of the
query is lowered inside it. A column computed over the step ahead of the op and read after
it would still hold the whole step there:

```
MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 }
```

`p.name` is read off `p` before the COUNT and output after it. `collectReadPastRowLoop` in
`DBLowering.cpp` finds those columns and the loop takes them along beside the op's inputs.
It serves `db.exists_subquery` as well, which failed on the same query with "operand does not
dominate this use".

## 5. Tests

`test/query/ir/subquery/CountSubqueryTest.cpp` runs the queries over SimpleGraph.
`test/query/ir/subquery/CountSubqueryCodegenTest.cpp` reads which path each body took.
`test/query/ir/subquery/CountNeo4jManualTest.cpp` holds every example of the Neo4j manual's COUNT
page, over the graph that page builds.
