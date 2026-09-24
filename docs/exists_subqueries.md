# EXISTS subqueries in the v3 engine

`EXISTS { ... }` in the MLIR engine: the semantics, the ops it lowers to, and what is out
of scope. Implemented as of 2026-09-22. The sibling of `docs/call_subqueries.md`, which
covers the clause form.

## 1. Semantics

Neo4j is the reference; openCypher 9 has no `EXISTS { }`.

`EXISTS { ... }` is a boolean expression, not a clause. It holds for a row exactly when its
body produces at least one row for that row, and it changes no row count: it stands wherever
a boolean stands - a WHERE, a RETURN item, a WITH item, a CASE condition, an ORDER BY key,
either side of a comparison.

Two body forms parse into the same AST:

```
MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS_WELL]->(k) } RETURN p.name
MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS_WELL]->(k) WHERE k.age = p.age } RETURN p.name
```

The pattern shorthand is one MATCH the parser builds; everything below reads the same for
both.

EXISTS is correlated and imports nothing: every variable in flight is readable inside the
body, under no scope clause, and stays readable through the whole of it - a WITH inside the
body carries those variables past the barrier rather than descoping them, as a CALL carries
what its scope clause names. What the body binds stays inside it - `EXISTS { (p)-->(k) }`
leaves `k` unbound outside, and a later clause naming it fails to resolve.

The body is read-only. A body holding an updating clause is rejected by the analyzer, which
is the language's rule, not a gap: `EXISTS { MATCH (p) CREATE (n) }` is not a query that
means anything.

A RETURN in the body answers for no column, but its cut answers for rows: `RETURN i SKIP 1`
holds only where the body matched twice. A keyless aggregate yields a row whatever it
counted, so `EXISTS { MATCH (p)-->(k) RETURN count(k) }` holds for every row.

Out of scope: UNION inside the body, which a CALL body has but an EXISTS body does not, and the
conditional `WHEN ... THEN { } ELSE { }` body, which the grammar has no clause for at all.
`test/query/ir/ExistsNeo4jManualTest.cpp` holds both, skipping with what is missing and
keeping the rows the manual documents, so each is the test to make pass when its clause
lands.

## 2. Ops

Two db ops and three nl ops, modelled on the OPTIONAL MATCH family. EXISTS answers for the
input rows rather than re-emitting rows, so it buffers none of them and opens no drain loop.

`db.exists_subquery` takes every named column in flight and holds the body in a region whose
block arguments stand for them, plus a trailing row tag - the position of each input row,
carried through the body as any other column is. Its one result is a `!db.column<!storage.bool>`
row-aligned with the inputs. `db.exists_yield` names the tag as the body left it and the
columns the body holds there.

The lowering is an accumulator over one step of the input rows:

```
%state, %tag = nl.exists_buffer (%p) : {!nl.chunk<!storage.node_id>}
nl.for ... {                                  // the body's own nest
  nl.exists_mark %state, %tag2, (%k) : {...}
}
%b = nl.exists_result(%state) : !nl.chunk<!storage.bool>
```

`nl.exists_buffer` sits at the top of the block binding the input chunks, so the flags are
cleared once per step. `nl.exists_mark` runs in the body's innermost loop body and marks the
input rows the tag names. `nl.exists_result` reads the flags back after the nest, in the
block the buffer sits in.

## 3. Two paths, as OPTIONAL CALL has

A body whose own dataflow keeps each row paired with the input row it came from carries the
tag, and the whole step is answered in one walk. A barrier inside the body carries the tag
past it under a hidden name no clause can spell, exactly as a CALL body's tag is carried.

A body that cannot - one that aggregates, sorts, cuts or dedups - runs one input row at a
time, under an `nl.each_row` loop over the step's rows, and a row is marked when the body
yielded anything at all for it. The rest of the query goes on inside that loop, where every
column of the step is bound a row at a time, so the boolean stays row-aligned with them.
`runsPerRow` in `DBLowering.cpp` answers for both this op and `db.call_subquery`, and a
limit inside a per-row body is hoisted into that body's step, so its budget resets per input
row.

## 4. Tests

`test/query/ir/ExistsSubqueryTest.cpp` runs the queries over the shared SimpleGraph fixture.
`test/query/ir/ExistsSubqueryCodegenTest.cpp` reads the emitted db op and the lowered nl
program, because both paths answer the same rows when both are right: it is what says which
one a body took.
