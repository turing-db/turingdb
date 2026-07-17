# UNWIND — implementation study / design

How to bring `UNWIND` to the `db`/`nl` MLIR execution path. The closest existing
analogs in that engine are the pipeline breakers with a buffer + drain-source shape:
[`ORDER_BY.md`](ORDER_BY.md) and [`AGGREGATE.md`](AGGREGATE.md). The list machinery
this leans on is [`lists.md`](lists.md).

Scope: in-memory, column-oriented, chunk-at-a-time execution — the regime TuringDB
operates in.

---

## 1. What UNWIND does

`UNWIND` is a cardinality-**expanding** clause: for each input row carrying a list of
length `L`, it emits `L` output rows — the unwound scalar in a new column, and every
other in-scope column broadcast (repeated) alongside. A null or empty list contributes
**zero** rows for that input row (so `UNWIND` of an empty list drops the row, the same
way a hop over a node with no out-edges drops it).

```cypher
UNWIND [1, 2, 3] AS x RETURN x            -- 3 rows
MATCH (a) UNWIND [1, 2] AS x RETURN a, x  -- 2 rows per a, `a` repeated
```

The expansion-plus-broadcast shape is not new to the engine: it is what
`db.get_out_edges` (variable fan-out per row, carry set filtered/repeated alongside)
and `db.cross_product` (`blockRepeatColumn` / `tileColumn` in `NLExecutor.cpp`) already
do. What is missing is a **list-valued column**: both dialects are strictly scalar. The
only `storage` element types are `node_id`, `edge_id`, `edge_type_id`, `string`, `bool`,
`embedding`, and `nullable<T>` (`query/ir/dialect/storage/StorageTypes.td`). There is no
`list<T>`. (`embedding`, a per-row span of floats, is the one variable-length-per-row
cell that already exists in the dialect — the precedent that such a cell is
representable, not a list.)

---

## 2. Current state in the engine

UNWIND exists **end to end in the legacy push-based pipeline**, and **not at all in the
`db`/`nl` MLIR path**.

Legacy path (`query/pipeline/`, `query/plan/`):
- Parser → `UnwindStmt` (`query/AST/stmt/UnwindStmt.h`).
- `ReadStmtAnalyzer::analyze(const UnwindStmt*)` — restricts the argument to a **literal
  list**: it throws `"Non-literal UNWIND expressions are not yet supported."` and
  `"Non-list arguments to UNWIND are not yet supported"`. It computes homogeneity across
  the list items.
- `ReadStmtGenerator::generateUnwindStmt` → `UnwindNode` → `PipelineGenerator::translateUnwindNode`
  materializes a `ListView` from the literal (`LocalMemory::listBuffer().insert(items)`)
  and calls `PipelineBuilder::addUnwind`.
- `UnwindProcessor` (`query/pipeline/processors/UnwindProcessor.cpp`) is the runtime: a
  **source** processor that streams the `ListView` chunk by chunk into an output column.
  Homogeneous lists fill a typed `ColumnVector<Primitive>`; heterogeneous lists fill a
  `ColumnVector<ListElementView>` (the type-tagged variant column). It carries no input
  rows — it opens the dataflow.

Behavior spec to regress against: `test/query-test-suite/tests/*unwind*.json`
(homogeneous, heterogeneous, empty, singleton, list-of-lists, `UNWIND … WHERE`,
`avg(UNWIND …)`) and `test/query/pipeline/processors/UnwindProcessorTest.cpp`.

MLIR path: nothing. `DBProgramGenerator` (`query/ir/codegen/`) only walks the
`VariableDependencyGraph` (MATCH patterns); it has no `UnwindStmt` handling, and today it
throws on `Expr::Kind::LIST` and on list literals (`"List literals are not yet supported
in MLIR codegen."`), and supports only single-part queries (no `WITH`).

---

## 3. The list problem: where does the list come from?

UNWIND's cost is entirely determined by where its list originates. Split it into two
regimes.

### Regime 1 — the list is independent of the input rows

`UNWIND [1,2,3] AS x`, `UNWIND range(...) AS x`, `UNWIND $events AS e`. This is what the
legacy engine supports (and only this). The list is known at plan time, so UNWIND is a
pure **source** — it needs no list *column*, just a column of the elements, exactly like
`db.const_scan_nodes` emits a column of node IDs.

- **db:** `db.unwind_const` source op (or generalize `const_scan_nodes` to arbitrary
  scalars), elements carried as an attribute; homogeneous → a typed `!db.column<T>`,
  heterogeneous → a `ListElementView` variant column.
- **nl:** `nl.unwind_const` source iterator driven by an `nl.for`, chunk-streaming the
  elements — a direct port of `UnwindProcessor`. Lowering is `buildLoopForSource`,
  identical to `lowerScanNodes`.
- **With incoming rows** (`MATCH (a) UNWIND [1,2,3] AS x`): **no new expansion op** — it
  is a `db.cross_product` between the `a` dataflow and the unwind source.

This reaches legacy parity with no list type and no `collect`. Frontend cost: teach
codegen to emit the source from an `UnwindStmt` whose argument is a literal/parameter
(materializing the `ListView` at translation time sidesteps general list-literal support).

### Regime 2 — the list is a per-row value

`WITH collect(n.name) AS names UNWIND names AS name`. Because **properties are
scalar-only** (there is no stored list property to unwind), the *only* in-query producer
of a per-row list is `collect` — which does not exist in either engine. So Regime 2 is,
in practice, always a `collect → unwind` pair. §4 is the design for it.

---

## 4. `collect → unwind` is a round trip, and that shapes the design

`WITH collect(n.name) AS names UNWIND names AS name RETURN name` is: take a column of
scalars → aggregate it into one row holding one list → immediately explode that list back
into a column of scalars. The data shape ends where it started. Materializing a real
`list<T>` column just to round-trip through it would be pure waste.

It is not a literal no-op, and the two reasons why are exactly what the operation is for:

- **`collect` is an aggregation horizon.** After `WITH collect(n.name) AS names`, `n` is
  gone — only `names` survives. The round trip's real effect is "detach the values from
  every other binding."
- **`collect` can group.** `WITH k, collect(x) AS xs … UNWIND xs AS y RETURN k, y`
  regroups the `(k, x)` pairs through a per-group list and re-expands them.

The architectural consequence: `collect → unwind` is not a "list-consuming op" problem.
It is the **buffer + drain-source** pattern the engine already has twice —

- `nl.sort_buffer` (accumulate every row) → `nl.sort` (source drains it, an `nl.for`
  emits chunks), and
- `nl.group_aggregate_buffer` (accumulate per group) → `nl.group_aggregate` (source
  drains, emits one row per group).

`collect` is another buffer — accumulate values into a list per grouping tuple — and
`UNWIND` of the collected variable is a drain-source, except it emits **one row per
element** instead of one row per group. The list stays *internal to the accumulator* and
never becomes a column value. No `list<T>` type, and no new broadcast kernel: the
per-element fan-out lives inside the drain's chunk-fill.

You need the real `list<T>` column **only** when the list survives as a first-class value
between collect and unwind — `size(xs)`, `xs[0]`, `xs + ys`, list comprehensions, or
returning `xs` to the client. Those are list-*expression* features, orthogonal to UNWIND
(see §7).

---

## 5. Design: the collect buffer and its two drains

This is the `group_aggregate` triple (`buffer` → `update` → drain-source), except the
buffer has **two** possible drains over the same keyed per-group storage:

- `nl.unwind` — one row per collected *element* (scalar value column). No list type.
- `nl.collect` — one row per *group* (value column = a list-typed cell). Needs
  `!storage.list<T>`.

`collect_buffer` + `collect_update` are drain-agnostic; the consumer picks the drain
(§4 explains why unwind is the common one and why it is a round trip). The list only
becomes a real column value when a non-unwind consumer forces the `nl.collect` drain.

### 5.1 New nl type — the accumulator handle

Parameterless handle, like `!nl.group_aggregate_state` / `!nl.sort_state`; the column
types are recovered from the update op that feeds it.

```tablegen
// NLTypes.td
def NLCollectState : NLType<"CollectState", "collect_state"> {
    let summary = "A per-group value-list accumulator handle for one collect";
    let description = [{
        Stands for one collect's per-group state: a hash table from the serialized
        grouping-key tuple to a group index, the distinct key values per group, and
        one growing value list per group. Produced by nl.collect_buffer, appended to
        by nl.collect_update once per producing-loop step, and drained by the nl.for
        over the nl.unwind source, which emits one row per collected element. The
        list-valued sibling of !nl.group_aggregate_state: where a grouped aggregate
        folds each group's rows to one scalar per aggregate, this keeps every value,
        because unwind will re-expand them.
    }];
}

// Constraint + BuildableType so ops can omit the handle type (mirrors
// NLGroupAggregateStateHandle).
def NLCollectStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::CollectStateType>($_self)">,
        "collect state handle", "::mlir::nl::CollectStateType">,
    BuildableType<"::mlir::nl::CollectStateType::get($_builder.getContext())">;
```

### 5.2 The nl triple

```tablegen
// NLOps.td

// NOT Pure: each buffer is a distinct accumulator (no CSE), and resetting its
// group table is a side effect (like nl.group_aggregate_buffer).
def CollectBuffer : NLOp<"collect_buffer", []> {
  let summary = "Create (and reset) a per-group value-list accumulator for one collect";
  let description = [{
    Sits at the top of the scope it governs, re-initializing the group table once
    per enclosing step. `keys` is how many of the columns nl.collect_update carries
    are grouping keys; the single remaining column is the value being collected.
    keyCount may be 0 - an ungrouped collect keeps one global list. The column types
    are recovered from the update, so only the key count appears here.

      %buf = nl.collect_buffer keys 1
  }];

  let arguments = (ins UI64Attr:$keyCount);
  let results   = (outs NLCollectState:$state);
  let assemblyFormat = "`keys` $keyCount attr-dict";
}

// NOT Pure: appends this step's rows to the per-group lists; must run where it
// sits, once per producing-loop step (like nl.group_aggregate_update).
def CollectUpdate : NLOp<"collect_update", []> {
  let summary = "Append this step's rows to the per-group value lists";
  let description = [{
    Runs in the producing loop body, once per step. Assigns each row to its group
    by the serialized grouping-key tuple (creating a group the first time a tuple is
    seen), then appends that row's value to the group's list. Columns are the
    grouping keys first, then the single collected value column - the same order
    nl.collect_buffer's `keys` describes. Cypher collect ignores nulls, so a null
    value row is not appended.

      nl.collect_update %buf, (%city, %name) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
  }];

  let arguments = (ins NLCollectStateHandle:$state, Variadic<NLChunk>:$columns);
  let assemblyFormat = "$state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))";
  let hasVerifier = 1;   // columns == keyCount + 1
}

// Pure: given a filled accumulator it deterministically yields the same rows; the
// per-element fan-out happens in the driving nl.for. Mirrors nl.group_aggregate.
def Unwind : NLOp<"unwind", [Pure]> {
  let summary = "Create a database iterator that drains a collect accumulator, one row per collected element";
  let description = [{
    The source op for the emit phase of a collect-then-unwind. It consumes a filled
    nl.collect_state and produces an iterator whose steps yield one row per collected
    element: the group's grouping-key values, then one element of that group's list.
    A group of L elements yields L rows (the key values repeated across them); an
    empty group yields none, matching UNWIND of an empty list. One chunk per output
    column per step, so an enclosing nl.for binds one loop variable per column:

      %rows = nl.unwind(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>>
      nl.for %city, %name in %rows : !nl.iter<...> {
        nl.output(%city, %name) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
      }

    The iterator chunk types - the grouping-key columns then the element column -
    cannot be inferred from the parameterless handle, so they are spelled after the
    colon (as nl.group_aggregate's are). Placing nl.unwind after the producing loop
    guarantees the buffer is full before the first element is emitted.
  }];

  let arguments = (ins NLCollectStateHandle:$state);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}
```

`nl.unwind` is a **source**, exactly like `nl.group_aggregate`. The per-element fan-out
(repeating the group's key values across its elements, flattening the list) lives inside
the drain's chunk-fill — so there is no list column and no new broadcast kernel, and the
value column it emits is a plain scalar chunk.

### 5.3 The list type (only the `nl.collect` drain needs it)

`nl.collect` emits the list *as a value*, so its cell type must exist in the dialects.
Add it to `storage`, a parameterized wrapper like `nullable<T>`:

```tablegen
// StorageTypes.td
def List : StorageType<"List", "list"> {
    let summary = "A list value";
    let description = [{
        The element type of a chunk holding one list per row - storage's
        ColumnVector<ListView>, per docs/lists.md. Each cell is a ListView: a span over
        the list's elements in the query-scoped ListBuffer, so a row copies in O(1)
        regardless of list length. The wrapped type is the (homogeneous) element type; a
        heterogeneous list rides the type-erased ListElementView layout, spelled
        `!storage.list<none>`.
    }];
    let parameters = (ins "::mlir::Type":$elementType);
    let assemblyFormat = "`<` $elementType `>`";
}
```

This is exactly `lists.md`'s `types::List::Primitive = ListView`. `nl.unwind` never
spells it; it is the price of the list *escaping*.

### 5.4 `nl.collect` — the per-group list drain

```tablegen
// NLOps.td

// Pure: given a filled accumulator it deterministically yields the same group rows -
// mirrors nl.group_aggregate. The per-group sibling of nl.unwind: same buffer, one row
// per group instead of one per element.
def Collect : NLOp<"collect", [Pure]> {
  let summary = "Create a database iterator that drains a collect accumulator, one row per group carrying the whole list";
  let description = [{
    The list-materializing drain of an nl.collect_state, for when the collected variable
    is consumed as a list rather than unwound. It produces an iterator whose steps yield
    one row per group: the grouping-key values, then a list cell spanning that group's
    contiguous run in the accumulator (a ListView into the query ListBuffer). An empty
    group yields an empty list, not zero rows (unlike nl.unwind).

      %rows = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>>
      nl.for %city, %names in %rows : !nl.iter<...> {
        nl.output(%city, %names) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>
      }

    Because collect is a pipeline breaker, every append is done before this source steps,
    so each group's run is final and the ListView cells are stable for the rest of the
    query - the same lifetime nl.sort's buffers have over its emit loop. The iterator
    chunk types (the keys, then the list cell) cannot be inferred from the parameterless
    handle, so they are spelled after the colon, as nl.group_aggregate's are.
  }];

  let arguments = (ins NLCollectStateHandle:$state);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}
```

So `nl.collect` and `nl.unwind` are two iterators over one accumulator: `nl.collect`
spans each group's run into a `ListView` cell, `nl.unwind` walks it element by element.

### 5.5 The db ops

The general op is `db.collect` — group + collect into a list column, one row per group.
`db.unwind_collect` is its *fused* form, valid only when the collected value's sole use
is an unwind: it drops the list column entirely and re-emits per element. Both are the
`db.group_aggregate` shape with a `keys` count and no reduction kinds.

The fused op, `db.unwind_collect`:

```tablegen
// DBOps.td
def UnwindCollect : TuringOp<"unwind_collect", [Pure]> {
  let summary = "Group rows by a key tuple, collect one column per group, and re-emit one row per collected element";
  let description = [{
    The declarative counterpart of `WITH k..., collect(x) AS xs UNWIND xs AS y`. It
    consumes the whole dataflow of its columns and emits, per distinct grouping-key
    tuple, one row for each collected value: the key values then that value. The
    operands are the grouping-key columns first (keyCount of them, may be 0), then
    the single collected value column; the results are the key columns (unchanged)
    then the unwound value column (same element type as the collected column).

      %a    = db.scan_nodes() : !db.column<!storage.node_id>
      %city = db.get_node_properties(%a, "city") : (!db.column<!storage.node_id>) -> !db.column<none>
      %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
      %gcity, %y = db.unwind_collect(%city, %name) keys 1
                     : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)
      db.output(%gcity, %y) : !db.column<none>, !db.column<none>

    A pipeline breaker: it must see every row before it can emit any, so it lowers
    to a hoisted nl.collect_buffer, an nl.collect_update in the producing loop, and
    an nl.unwind source drained by an nl.for after that loop.
  }];

  let arguments = (ins Variadic<Column>:$columns, UI64Attr:$keyCount);
  let results   = (outs Variadic<Column>:$results);
  let assemblyFormat = "`(` $columns `)` `keys` $keyCount attr-dict `:` functional-type(operands, results)";
  let hasVerifier = 1;   // operands == results == keyCount + 1; leading keyCount results pass through
}
```

The general op, `db.collect` (produces the list column):

```tablegen
// DBOps.td
def Collect : TuringOp<"collect", [Pure]> {
  let summary = "Group rows by a key tuple and collect one column per group into a list";
  let description = [{
    The declarative counterpart of `WITH k..., collect(x) AS xs`. Emits one row per
    distinct grouping-key tuple: the key values, then a list of the collected column's
    non-null values for that group. Operands are the grouping-key columns first
    (keyCount of them, may be 0) then the single collected value column; results are the
    key columns (unchanged) then one !db.column<!storage.list<T>>.

      %gcity, %names = db.collect(%city, %name) keys 1
                         : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)

    A pipeline breaker: lowers to nl.collect_buffer + nl.collect_update + the nl.collect
    drain. db.unwind_collect is the peephole fusion of a db.collect whose list result has
    a single use, an unwind.
  }];

  let arguments = (ins Variadic<Column>:$columns, UI64Attr:$keyCount);
  let results   = (outs Variadic<Column>:$results);
  let assemblyFormat = "`(` $columns `)` `keys` $keyCount attr-dict `:` functional-type(operands, results)";
  let hasVerifier = 1;   // operands == keyCount + 1; results == keyCount + 1; trailing result is a list column
}
```

With **mixed consumers** (`xs` unwound *and* used as a list), lower to one `db.collect`
and route both the list consumer and a general unwind off its list column — don't drain
the buffer twice.

### 5.6 Lowering (`lowerUnwindCollect` / `lowerCollect`)

A near-copy of `lowerGroupAggregate` (`query/ir/lowering/DBLowering.cpp`); the only
difference is the drain source:

1. Hoist `nl.collect_buffer keys N` to the entry block (resets there, dominates the loop
   and the drain).
2. In the producing loop body, map the key/value columns to their nl chunks and emit
   `nl.collect_update %buf, (keys…, value)`.
3. After the producing loop, create the drain and `buildLoopForSource` it:
   - `lowerUnwindCollect` → `nl.unwind(%buf) : !nl.iter<keyChunks…, valueChunk>`; the
     wrapping `nl.for` binds `(keys…, element)`.
   - `lowerCollect` → `nl.collect(%buf) : !nl.iter<keyChunks…, listChunk>`; the wrapping
     `nl.for` binds `(keys…, list)`.
   Map the db op's results to those loop chunks; `db.output` (or a downstream traversal)
   consumes them.

### 5.7 Runtime

- **`NLCollectState`** (`query/ir/interpreter/NLProgram.h`): a hash table
  `hash(serialized key tuple) → group index`, the distinct key values per group (as
  `group_aggregate` keeps), and — the accumulator — **per-group contiguous storage** for
  the collected values, in the query-scoped `LocalMemory` arena. `keyCount == 0` → one
  implicit group. Resetting empties it. Because collect is a pipeline breaker, every
  append completes before either drain steps, so a group's run is final and immovable
  when a drain reads it — that is what lets `nl.collect` hand out `ListView`s over the
  runs with zero copy and stable lifetime (the arena outlives the downstream consumers,
  as `nl.sort`'s buffers outlive its emit loop).
  - Where the run physically lives is the one design choice: **per-group list-layout
    runs** (append lands tagged, so `nl.collect` spans them with no drain-time work, but
    the hot `collect_update` path tags each element), or **shared buffer + per-group
    staging** (`collect_update` is a tight typed append; `nl.collect` pays one
    `ListBuffer::insert` per group at drain to lay down the contiguous tagged run and take
    the span). Given the barrier, the stage-then-insert default keeps the update loop
    cheap; the per-group-run form wins if `collect` results are common. `nl.unwind` needs
    neither — it reads values straight from the runs.
- **`runCollectUpdate`**: per row, serialize the key tuple, find-or-create the group,
  append the value to that group's run (skip nulls, per Cypher). Mirrors
  `runGroupAggregateUpdate`.
- **`runUnwindLoop`** (the per-element drain, like `runGroupAggregateLoop`): walk groups
  in append order; a chunk writer fills up to `CHUNK_SIZE` rows, each row = `(group's key
  values, next element of its run)`, advancing to the next group when a run is exhausted;
  run the body per filled chunk. The key-value repeat-per-element is a plain `std::fill_n`
  / `std::copy` inside the writer — no exposed variable-repeat kernel.
- **`runCollectLoop`** (the per-group drain): walk groups `CHUNK_SIZE` at a time; each row
  = `(group's key values, a ListView spanning that group's run)`. The value column is a
  `ColumnVector<ListView>` chunk — O(1) per cell, so a chunk fill is cheap. This is
  `lists.md`'s "column of lists = vector of spans", and the spans point into the
  accumulator arena, so it flows through downstream cross_product / joins in O(1).

### 5.8 Worked examples (lowered form)

Ungrouped — `WITH collect(n.name) AS names UNWIND names AS name RETURN name`:

```mlir
%pt   = nl.get_property_type("name")
%buf  = nl.collect_buffer keys 0
%scan = nl.scan_nodes()
nl.for %a in %scan : !nl.iter<!nl.chunk<!storage.node_id>> {
  %name = nl.get_node_properties(%a, %pt) : !nl.chunk<!storage.nullable<!storage.string>>
  nl.collect_update %buf, (%name) : !nl.chunk<!storage.nullable<!storage.string>>
}
%rows = nl.unwind(%buf) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>>
nl.for %name in %rows : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>> {
  nl.output(%name) : !nl.chunk<!storage.nullable<!storage.string>>
}
```

Grouped — `WITH n.city AS city, collect(n.name) AS names UNWIND names AS name RETURN city, name`:
`nl.collect_buffer keys 1`, `nl.collect_update %buf, (%city, %name)`, then
`nl.unwind(%buf) : !nl.iter<cityChunk, nameChunk>` drained by an `nl.for %city2, %name in %rows`.

List escapes (the drain is `nl.collect`, not `nl.unwind`) —
`WITH n.city AS city, collect(n.name) AS names RETURN city, names`:

```mlir
%pc   = nl.get_property_type("city")
%pn   = nl.get_property_type("name")
%buf  = nl.collect_buffer keys 1
%scan = nl.scan_nodes()
nl.for %a in %scan : !nl.iter<!nl.chunk<!storage.node_id>> {
  %city = nl.get_node_properties(%a, %pc) : !nl.chunk<!storage.nullable<i64>>
  %name = nl.get_node_properties(%a, %pn) : !nl.chunk<!storage.nullable<!storage.string>>
  nl.collect_update %buf, (%city, %name) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
}
%rows = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>>
nl.for %city, %names in %rows : !nl.iter<...> {
  nl.output(%city, %names) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>
}
```

Same buffer and update as the grouped unwind above — only the drain differs (`nl.collect`
yields a list cell per group; `nl.unwind` yields a scalar per element).

---

## 6. An even cheaper alternative (two ops, not three)

Add `Collect` to `GroupAggregateKind` (`query/ir/dialect/storage/StorageEnums.td`) so the
existing `nl.group_aggregate_buffer` / `nl.group_aggregate_update` accumulate the list for
that aggregate, and add **only** `nl.unwind` as a second drain of
`!nl.group_aggregate_state`. Attractive if you also want `collect` as a first-class
aggregate (to RETURN a list later). It gets awkward when other aggregates sit in the same
horizon as the unwound one — their per-group scalars would have to broadcast per element
too — so the dedicated triple in §5 stays clean when collect is the whole horizon; this
reuse path wins once `collect` exists as a general aggregate.

---

## 7. Notes and open questions

- **Nulls & order.** Cypher `collect` drops nulls, so the per-group lists hold only
  present values; the emitted column can ride `nullable<T>` for uniformity with property
  fetches (as `aggregate_result` does). `collect` order is undefined, so emit in append
  (input) order — the cheap correct choice.
- **Keyless degenerate case** (`keys 0`, value column = input): a full-materialization
  near-identity. A lowering fold could delete the whole op when nothing downstream depends
  on the barrier — analogous to `foldTruncatesIntoOutputs`.
- **List escapes to a non-unwind consumer** (`RETURN xs`, `xs + ys`, a comprehension, a
  dynamic index, a UDF): the fusion cannot fire, so lower to `db.collect` and its
  `nl.collect` drain (§5), materializing the `!storage.list<T>` column (see
  [`lists.md`](lists.md)). `db.unwind_collect` fuses only when the collected value's
  single use is the unwind; with mixed consumers, materialize once via `db.collect` and
  route both the list consumer and a general unwind off that column. Several apparent list
  consumers instead fold to scalar group-aggregates and never materialize a list:
  `size(xs)` → `count`, `head(xs)` / `xs[0]` → a first aggregate, `min` / `max` /
  `sum(xs)` → those aggregates.
- **Frontend prerequisites.** Regime 1 needs an `UnwindStmt` → source path in
  `DBProgramGenerator`. Regime 2 additionally needs `WITH`-chaining (multi-part queries),
  which the MLIR frontend does not yet support, before `collect(...) … UNWIND` is even
  expressible.

---

## 8. Recommended sequencing

1. **Regime 1** (`db.unwind_const` + `nl.unwind_const`, port `UnwindProcessor`; combine
   with incoming rows via `db.cross_product`). No list type. Reaches legacy parity.
2. **`collect → unwind`** via the buffer+drain pair of §5 (or the reuse path of §6). Still
   no `list<T>` column — the list stays inside the accumulator.
3. **`list<T>` as a value** (`lists.md`) only when list expressions land. UNWIND of a list
   value then falls out for free, but must not be the thing that drives building the type.
