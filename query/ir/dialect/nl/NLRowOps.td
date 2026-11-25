#ifndef TURINGDB_NL_ROW_OPS
#define TURINGDB_NL_ROW_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* For
*/
def For : NLOp<"for", [SingleBlockImplicitTerminator<"Yield">]> {
  let summary = "Drive a database iterator chunk by chunk";
  let description = [{
    Runs the body once per iterator step until the iterator is exhausted.
    Each step fills the next chunks of the iterator and binds them to the
    loop variables, one variable per chunk:

    nl.for %chunk in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
        ...
    }

    An optional `limit` operand carries an !nl.limit_state handle. A loop that
    names one stops driving its iterator as soon as the budget is exhausted, so
    every nl.for in a LIMIT's nest carries the same handle and the break unwinds
    the whole nest:

    nl.for %chunk in %nodes limit %h : !nl.iter<!nl.chunk<!storage.node_id>> {
        ...
    }
  }];

  // The optional limit handle is the only variable-length operand, so MLIR can
  // tell it apart from the iterator without operand-segment sizes.
  let arguments = (ins NLIterator:$iterator, Optional<NLLimitStateHandle>:$limit);
  let regions = (region SizedRegion<1>:$bodyRegion);
  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The builders take the iterator value - the loop variables are created by
  // looking up the chunk types on the iterator type, so C++ users never spell
  // any type - plus an optional limit handle (null for an unbounded loop). The
  // iterator-only overload is the unbounded form most callers want.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::Value":$iterator)>,
      OpBuilder<(ins "::mlir::Value":$iterator, "::mlir::Value":$limit)>
  ];
}

/**
* Yield
*/
def Yield : NLOp<"yield", [Pure, Terminator, ParentOneOf<["For", "ExplorePaths"]>]> {
  let summary = "End one step of an nl.for loop, or yield the mask of an explore_paths hop";
  let description = [{
    The implicit terminator of an nl.for body carries nothing. The terminator of an
    nl.explore_paths hop region yields the one boolean mask chunk that says which
    candidates of a hop pass its predicate.
  }];

  let arguments = (ins Variadic<NLChunk>:$columns);

  let assemblyFormat = "( $columns^ `:` qualified(type($columns)) )? attr-dict";

  // nl.for's implicit terminator is built with no operands
  let builders = [
      OpBuilder<(ins)>
  ];
}

/**
* EachRow
*/
def EachRow : NLOp<"each_row", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create an iterator over the rows of a step, one row per step";

  let description = [{
    Each step of an enclosing nl.for binds one row of the given chunks, in the order
    they hold it: step k fills one-row chunks with row k of every column. The
    iterator's chunk types are the columns' own, so the body reads the row through
    the same kinds of chunk the step held it in:

      %rows = nl.each_row {%p, %h} : {!nl.chunk<!storage.node_id>, !nl.chunk<none>}
      nl.for %p1, %h1 in %rows : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<none>> {
        ...
      }

    This is how a CALL subquery whose body aggregates, sorts, cuts or dedups runs
    once per row in flight: the body's loops nest in this loop and its accumulators
    are hoisted into its body, so they cover one input row at a time. An EXISTS, COUNT
    or COLLECT body run this way walks its buffer's tag beside the rows, so each step
    names the row of the step it answers for. The columns
    must be row-aligned with each other, and there must be at least one - the step's
    row count is read from the first. A `limit` on the enclosing nl.for stops the
    walk over the rows once the budget is spent.
  }];

  let arguments = (ins Variadic<NLChunk>:$columns);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `{` $columns `}` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

/**
* Output
*/
def Output : NLOp<"output", [AttrSizedOperandSegments]> {
  let summary = "Send a row of chunks out as columns of the query result";
  let description = [{
    The sink of the nested loop, mirroring a result builder (LingoDB's
    dsa.append): inside an nl.for body it hands the current chunks to the
    query result, one output column per chunk. The columns are typically loop
    variables, so each nl.for step emits one chunk per column:

    nl.output(%targets) : !nl.chunk<!storage.node_id>
    nl.output(%sources, %targets) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>

    Variadic<NLChunk> accepts any number of chunks of any element type. The op
    is deliberately not Pure: emitting output is its whole point, so it must
    survive dead-code elimination even though it has no results.

    An optional `limit` operand carries an !nl.limit_state handle. When present,
    output emits only the prefix of length limit.getEmitThisStep() set by the
    preceding nl.limit_update; absent, it emits the whole chunk and is fully
    limit-oblivious. The limit form is the fold of an adjacent nl.limit_truncate
    whose only consumer was this output: when a LIMIT feeds the sink directly,
    output can read the budgeted prefix off the handle instead of copying it. It
    never mutates the budget - it only reads how many rows to emit:

    nl.output(%targets) limit %h : !nl.chunk<!storage.node_id>

    An optional `skip` operand carries an !nl.skip_state handle and is the skip
    sibling of the limit form: when present, output emits the surviving suffix
    [skip.getSkipThisStep(), skip.getSkipThisStep() + skip.getEmitThisStep()) set
    by the preceding nl.skip_update - it reads the dropped-row count as a starting
    offset rather than copying the suffix to the front. It is the fold of an
    adjacent nl.skip_truncate whose only consumer was this output (the copy-free
    path the suffix could not take through the sink before output learned an
    offset). Like the limit form it never mutates the counter:

    nl.output(%targets) skip %h : !nl.chunk<!storage.node_id>

    An optional `cardinality` operand carries a representative chunk of the driving
    relation - any loop variable of the loop this output nests in. A projection of
    only constants - RETURN 5 - has no per-row column to size the emission, yet its
    cardinality is the driving relation's (projection preserves cardinality), so the
    output nests in the driving loop and reads its row count from this chunk; the
    constants broadcast against it. Absent for a per-row projection (its own columns
    size it) or a single-row emit.

    nl.output(%c) cardinality(%n : !nl.chunk<!storage.node_id>) : !nl.chunk<i64>

    Only that row count is read, never a value, so the driver is a chunk of any
    element type - the node IDs a scan binds, the values an UNWIND unwinds, the return
    values a procedure drive binds - as nl.broadcast_constant's own cardinality
    operand is:

    nl.output(%c) cardinality(%x : !nl.chunk<!storage.nullable<i64>>) : !nl.chunk<i64>

    An optional `names` array carries the result column names db.output was given,
    one per column, which the interpreter hands to the sink so it can label the
    columns it is fed. They name the result and never steer execution, so an output
    that carries none emits exactly the same rows.

    nl.output(%a) names ["person"] : !nl.chunk<!storage.node_id>
  }];

  // Four variable-length operand groups - the columns, the optional limit, the
  // optional skip and the optional cardinality driver - so the op carries
  // operand-segment sizes to tell them apart, the same as nl.cross_product's column
  // groups. A folded output carries at most one of limit/skip (the truncate adjacent
  // to it); the two are not produced together (see foldSkipTruncatesIntoOutputs).
  let arguments = (ins Variadic<NLChunk>:$columns,
                       Optional<NLLimitStateHandle>:$limit,
                       Optional<NLSkipStateHandle>:$skip,
                       Optional<NLChunk>:$cardinality,
                       OptionalAttr<StrArrayAttr>:$column_names);

  // The chunk element types vary from call to call and - unlike the source
  // ops - cannot be inferred, so they are spelled after the colon. This keeps
  // the text self-describing: the parser builds each operand's type from the
  // printed list rather than looking it up on the defining nl.for (which MLIR
  // cannot do during a parse), the same reason nl.for keeps its : !nl.iter<...>
  // annotation. qualified(...) forces the leading !nl. on every printed type.
  // The optional limit and skip handles, when present, print after a `limit` or
  // `skip` keyword; both types are buildable and so omitted. Each optional group
  // has its own literal anchor, so the declarative parser tells them apart. The
  // cardinality driver is a chunk of any element type, which no recipe can build, so
  // it is parenthesized with its own type - keeping the trailing colon the columns'.
  let assemblyFormat = [{
    `(` $columns `)` (`names` $column_names^)? (`limit` $limit^)? (`skip` $skip^)? (`cardinality` `(` $cardinality^ `:` qualified(type($cardinality)) `)`)? attr-dict `:` qualified(type($columns))
  }];

  // Emitting an unnamed result is the common case - every hand-written program and
  // every lowering of a db.output that carries no names - so it gets a builder that
  // takes the operands alone.
  let builders = [
      OpBuilder<(ins "::mlir::ValueRange":$columns,
                     "::mlir::Value":$limit,
                     "::mlir::Value":$skip,
                     "::mlir::Value":$cardinality)>
  ];

  // Enforce the "at most one of limit/skip" invariant the description promises:
  // a folded output carries the single handle of the truncate adjacent to it,
  // never both. runOutput reads them as an if(skip)/else if(limit) chain, so a
  // malformed both-handles op would silently drop the limit; reject it here.
  // One name per column, when the output carries names at all.
  let hasVerifier = 1;
}

/**
* Limit
*/
// NOT Pure: each nl.limit is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - its reset is a side effect.
def Limit : NLOp<"limit", []> {
  let summary = "Create (and reset) a row-limit counter, initialized to N rows";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level
    LIMIT, or an enclosing nl.for body for a nested one, where it re-initializes
    once per enclosing step. Produces the !nl.limit_state handle the loops,
    nl.limit_update and nl.output read. The result type is buildable and omitted.

    The budget is the count db.limit carried, or the chunk holding the count it bound,
    read each time the counter resets:

      %h = nl.limit(3)
      %b = nl.limit(%n : !nl.chunk<i64>)
  }];

  // Unsigned: a row budget is never negative.
  let arguments = (ins OptionalAttr<UI64Attr>:$count, Optional<NLChunk>:$bound_count);
  let results   = (outs NLLimitState:$state);
  let assemblyFormat = "`(` custom<BoundCount>($count, $bound_count, type($bound_count)) `)` attr-dict";
  let hasVerifier = 1;

  let builders = [
    OpBuilder<(ins "uint64_t":$count), [{
      build($_builder, $_state, $_builder.getIntegerAttr($_builder.getIntegerType(64, /*isSigned=*/false), count), ::mlir::Value());
    }]>,
    OpBuilder<(ins "::mlir::Value":$boundCount), [{
      build($_builder, $_state, ::mlir::IntegerAttr(), boundCount);
    }]>
  ];
}

/**
* LimitRemaining
*/
// NOT Pure: like nl.limit, it resets a distinct counter each time it runs, so it must
// not be CSE'd, hoisted or eliminated.
def LimitRemaining : NLOp<"limit_remaining", []> {
  let summary = "Create (and reset) a row-limit counter, initialized to what another one still admits";
  let description = [{
    Reads the remaining budget of the source counter at the point it runs and
    produces a fresh !nl.limit_state initialized to it. The source is only read;
    the new counter is charged by its own nl.limit_update and read by the loops
    it bounds, as a counter nl.limit produces is. The result type is buildable
    and omitted.

      %budget = nl.limit_remaining(%h)
  }];

  let arguments = (ins NLLimitStateHandle:$source);
  let results   = (outs NLLimitState:$state);
  let assemblyFormat = "`(` $source `)` attr-dict";
}

/**
* LimitUpdate
*/
// NOT Pure: it mutates the handle, so it must run exactly where it sits and must
// not be eliminated.
def LimitUpdate : NLOp<"limit_update", []> {
  let summary = "Charge this step's rows against a limit counter";
  let description = [{
    Runs in the innermost loop body just before nl.output. Reads the row count of
    the representative chunk (the same column nl.output will emit, so the count
    matches after any cross product), records min(rows, remaining) as the count
    nl.output should emit this step, and subtracts it from the budget. The sole
    mutator of the counter.
  }];

  let arguments = (ins NLLimitStateHandle:$state, NLChunk:$rows);
  let assemblyFormat = "$state `,` $rows attr-dict `:` qualified(type($rows))";
}

/**
* LimitTruncate
*/
// NOT Pure: it reads the handle's mutable emitThisStep (set by the preceding
// nl.limit_update), so it must run exactly where it sits and must not be hoisted,
// CSE'd or reordered past the update. InferTypeOpAdaptor: the results mirror the
// column operand types, copied across by inferReturnTypes, as on nl.cross_product.
def LimitTruncate : NLOp<"limit_truncate", [InferTypeOpAdaptor]> {
  let summary = "Copy the first emitThisStep rows of each column into fresh chunks";
  let description = [{
    Sits in the producing loop body, just after nl.limit_update and just before the
    limited columns' consumer - nl.output when the query is not chained, the inner
    sub-pipeline when it is. Copies the prefix [0, emitThisStep) of each input chunk
    (the count nl.limit_update recorded this step) into a fresh, independently-owned
    chunk, so a downstream op reads a genuinely truncated chunk and needs no limit
    awareness of its own. The columns pass through unchanged in type; only the row
    count is cut.

      %a' = nl.limit_truncate %h, (%a) : !nl.chunk<!storage.node_id>
  }];

  // The limit handle whose emitThisStep sizes the copy, then the columns to cut.
  let arguments = (ins NLLimitStateHandle:$state, Variadic<NLChunk>:$columns);

  // One result per input column, same type. The results are inferred from the
  // column operands, so only the column chunk types are spelled, after the colon;
  // the handle type is buildable and omitted.
  let results = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
}

/**
* Skip
*/
// NOT Pure: each nl.skip is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - its reset is a side effect. The skip
// sibling of nl.limit.
def Skip : NLOp<"skip", []> {
  let summary = "Create (and reset) a row-skip counter, initialized to M rows to drop";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level SKIP
    - and re-initializes once per execution of that block. Produces the
    !nl.skip_state handle nl.skip_update and nl.skip_truncate read. Unlike nl.limit
    it never appears on an nl.for: a skip drives no loop early-exit, since every row
    past the dropped prefix must still be produced. The result type is buildable and
    omitted. The count is the one db.skip carried, or the chunk holding the count it
    bound, read as nl.limit reads one.
  }];

  // Unsigned: a count of rows to drop is never negative.
  let arguments = (ins OptionalAttr<UI64Attr>:$count, Optional<NLChunk>:$bound_count);
  let results   = (outs NLSkipState:$state);
  let assemblyFormat = "`(` custom<BoundCount>($count, $bound_count, type($bound_count)) `)` attr-dict";
  let hasVerifier = 1;

  let builders = [
    OpBuilder<(ins "uint64_t":$count), [{
      build($_builder, $_state, $_builder.getIntegerAttr($_builder.getIntegerType(64, /*isSigned=*/false), count), ::mlir::Value());
    }]>,
    OpBuilder<(ins "::mlir::Value":$boundCount), [{
      build($_builder, $_state, ::mlir::IntegerAttr(), boundCount);
    }]>
  ];
}

/**
* SkipUpdate
*/
// NOT Pure: it mutates the handle, so it must run exactly where it sits and must
// not be eliminated. The skip sibling of nl.limit_update.
def SkipUpdate : NLOp<"skip_update", []> {
  let summary = "Charge this step's rows against a skip counter";
  let description = [{
    Runs in the innermost loop body just before nl.skip_truncate. Reads the row
    count of the representative chunk (the same column the consumer will read),
    drops min(rows, remaining) of them from the front of this step, records that
    offset and the emitted-row count nl.skip_truncate should copy, and subtracts the
    dropped count from the budget. The sole mutator of the counter.
  }];

  let arguments = (ins NLSkipStateHandle:$state, NLChunk:$rows);
  let assemblyFormat = "$state `,` $rows attr-dict `:` qualified(type($rows))";
}

/**
* SkipTruncate
*/
// NOT Pure: it reads the handle's mutable skipThisStep/emitThisStep (set by the
// preceding nl.skip_update), so it must run exactly where it sits and must not be
// hoisted, CSE'd or reordered past the update. InferTypeOpAdaptor: the results
// mirror the column operand types, copied across by inferReturnTypes, as on
// nl.limit_truncate.
def SkipTruncate : NLOp<"skip_truncate", [InferTypeOpAdaptor]> {
  let summary = "Copy the surviving suffix of each column into fresh chunks";
  let description = [{
    The skip sibling of nl.limit_truncate. Sits in the producing loop body, just
    after nl.skip_update and just before the columns' consumer - nl.output when the
    query is not chained, the inner sub-pipeline when it is. Copies the suffix
    [skipThisStep, skipThisStep + emitThisStep) of each input chunk (the offset and
    count nl.skip_update recorded this step) into a fresh, independently-owned,
    front-aligned chunk, so a downstream op reads a genuinely skipped chunk and
    needs no skip awareness of its own. The columns pass through unchanged in type;
    only the dropped prefix is removed.

    When the sole consumer is the query sink, DBLowering folds this op away: an
    nl.output gained an optional skip handle and emits the surviving suffix in
    place at an offset (see foldSkipTruncatesIntoOutputs), the copy-free path the
    suffix could not take while the sink only read from row zero. The copy here
    survives only when the suffix feeds an inner sub-pipeline (a chained query)
    that reads from row zero and so needs the rows lifted to the front.

      %a' = nl.skip_truncate %h, (%a) : !nl.chunk<!storage.node_id>
  }];

  // The skip handle whose offset/count size the copy, then the columns to cut.
  let arguments = (ins NLSkipStateHandle:$state, Variadic<NLChunk>:$columns);

  // One result per input column, same type. The results are inferred from the
  // column operands, so only the column chunk types are spelled, after the colon;
  // the handle type is buildable and omitted.
  let results = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
}

/**
* SortBuffer
*/
// NOT Pure: each nl.sort_buffer is a distinct accumulator, so two must never be
// CSE'd into one, and an unread one must not be DCE'd - resetting its buffers is
// a side effect (like nl.limit).
def SortBuffer : NLOp<"sort_buffer", []> {
  let summary = "Create (and reset) a row accumulator for one ORDER BY";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level
    ORDER BY - where it re-initializes the per-column buffers once per enclosing
    step. Produces the !nl.sort_state handle that nl.sort_collect appends to and
    the nl.for over nl.sort drains.

    `keys` is the list of column indices to sort by, most significant first;
    `ascending` gives the direction of each key (true = ascending). The buffers'
    column types are not spelled here - they are recovered from the nl.sort_collect
    that feeds the same handle - so only the sort spec appears. The result type is
    buildable and omitted.

      %state = nl.sort_buffer keys [1] ascending [true]

    An optional `limit` count turns the accumulator into a bounded top-K: it keeps
    only the `limit` best rows seen so far rather than every row, so the memory is
    O(limit) instead of O(rows) and the sort is O(rows log limit). It is the fused
    form of an ORDER BY ... LIMIT k - a constant bound, distinct from the mutable
    !nl.limit_state handle the streaming limits carry, since the bound is private
    to the sort and never decremented:

      %state = nl.sort_buffer keys [1] ascending [true] limit 10

    The bound may also be the chunk holding the count a db.limit bound, read each
    time the accumulator resets:

      %state = nl.sort_buffer keys [1] ascending [true] limit %k : !nl.chunk<i64>
  }];

  // keyColumns are indices into the collected column set, most significant first;
  // keyAscending gives one direction per key. The two arrays are parallel. The
  // optional topK count, or the bound one, bounds the accumulator to its best rows.
  let arguments = (ins DenseI64ArrayAttr:$keyColumns,
                       DenseBoolArrayAttr:$keyAscending,
                       OptionalAttr<UI64Attr>:$topK,
                       Optional<NLChunk>:$boundTopK);
  let results   = (outs NLSortState:$state);

  let assemblyFormat = [{
    `keys` $keyColumns `ascending` $keyAscending (`limit` custom<BoundCount>($topK, $boundTopK, type($boundTopK))^)? attr-dict
  }];
  let hasVerifier = 1;

  let builders = [
    OpBuilder<(ins "::mlir::DenseI64ArrayAttr":$keyColumns, "::mlir::DenseBoolArrayAttr":$keyAscending, "::mlir::IntegerAttr":$topK), [{
      build($_builder, $_state, keyColumns, keyAscending, topK, ::mlir::Value());
    }]>
  ];
}

/**
* SortCollect
*/
// NOT Pure: it appends the current chunk's rows to the handle's buffers, so it
// must run exactly where it sits, once per producing-loop step, and must not be
// hoisted, CSE'd or eliminated.
def SortCollect : NLOp<"sort_collect", []> {
  let summary = "Append this step's chunk of every column to a sort accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Appends the current chunk of
    each column - the whole row set of this step - to the matching buffer of the
    handle, so that by the time the producing loop is exhausted the buffers hold
    every row. The columns are taken together so the buffers stay row-aligned.

      nl.sort_collect %state, (%a, %score) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
  }];

  // The accumulator handle, then the columns to append. The column chunk types
  // are spelled after the colon (as nl.output does), since the buffers' element
  // types are recovered from them.
  let arguments = (ins NLSortStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
  let hasVerifier = 1;
}

/**
* Sort
*/
// Pure: given a filled accumulator it deterministically yields the same sorted
// iterator, and it produces no side effect of its own - the sort and the
// chunk-by-chunk emission happen in the driving nl.for. Mirrors nl.scan_nodes:
// a source op whose nl.for does the work.
def Sort : NLOp<"sort", [Pure]> {
  let summary = "Create a database iterator over the sorted rows of an accumulator";
  let description = [{
    The source op for the emit phase of an ORDER BY, the sorted counterpart of
    nl.scan_nodes. It consumes a filled nl.sort_state and produces an iterator
    whose steps yield the accumulated rows sorted by the handle's keys, one chunk
    per column per step - the same chunk shape the buffers were collected with, so
    an enclosing nl.for binds one loop variable per column:

      %sorted = nl.sort(%state) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>>
      nl.for %a, %score in %sorted : !nl.iter<...> {
        nl.output(%a, %score) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
      }

    The iterator chunk types match the collected columns, but cannot be inferred
    from the parameterless state handle, so they are spelled after the colon (as
    the nl.for iterator type is). The sort itself runs once when the driving loop
    first steps; placing nl.sort after the producing loop guarantees the buffers
    are full by then.
  }];

  let arguments = (ins NLSortStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

// NOT Pure: resetting the seen-set is a side effect, and two seen-sets must never
// be CSE'd into one (like nl.limit / nl.sort_buffer).
def Distinct : NLOp<"distinct", []> {
  let summary = "Create (and reset) a seen-set for one DISTINCT";
  let description = [{
    Hoisted to the top of the scope it governs (function scope for a top-level or
    mid-query DISTINCT), where it empties the seen-set. Produces the
    !nl.distinct_state handle nl.distinct_filter reads and mutates. DISTINCT streams
    rather than breaking the pipeline, so no nl.for names this handle - the filter
    runs inside the producing loop.

      %s = nl.distinct
  }];

  let results = (outs NLDistinctState:$state);
  let assemblyFormat = "attr-dict";
}

// NOT Pure: it mutates the seen-set, so it must run where it sits and must not be
// hoisted, CSE'd or eliminated. InferTypeOpAdaptor: results mirror the columns.
def DistinctFilter : NLOp<"distinct_filter", [InferTypeOpAdaptor]> {
  let summary = "Keep only the rows of each column not seen before, as fresh chunks";
  let description = [{
    Runs in the producing loop body, once per step. Treats each row as the tuple
    across all columns (Cypher DISTINCT dedups the whole projection, nulls included),
    copies the rows not yet in the seen-set into fresh output chunks and records
    them, and drops the duplicates. The columns pass through unchanged in type.

    It emits genuinely cut chunks like nl.limit_truncate, so a downstream consumer -
    an nl.output, or a further nl.get_out_edges in the chained case - needs no
    DISTINCT awareness. And because it shrinks each chunk, a following nl.limit_update
    charges the deduped count, so a downstream LIMIT bounds the producing loop.

      %da = nl.distinct_filter %s, (%a) : !nl.chunk<!storage.node_id>
  }];

  let arguments = (ins NLDistinctStateHandle:$state, Variadic<NLChunk>:$columns);
  let results   = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];

  let hasVerifier = 1;
}

def Filter : NLOp<"filter", [Pure]> {
  let summary = "Keep only the rows where the mask chunk is true, as freshly cut chunks";
  let description = [{
    Applies a boolean mask to a carry set of chunks.

    %a2 = nl.filter %mask, (%a) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
  }];

  let arguments = (ins NLChunk:$mask, Variadic<NLChunk>:$columns);
  let results   = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $mask `,` `(` $columns `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* RowBuffer
*/
// NOT Pure: each nl.row_buffer is a distinct accumulator, and resetting its buffers
// is a side effect (like nl.sort_buffer).
def RowBuffer : NLOp<"row_buffer", []> {
  let summary = "Create (and reset) a row accumulator";
  let description = [{
    Sits at the top of the block whose step it covers - the block the branches of a
    UNION inside a CALL body, or of a NEXT segment, are rooted in - so the accumulator
    is empty again at every step. The buffers' column types are recovered from the
    nl.row_collect ops that feed the handle.

      %state = nl.row_buffer
  }];

  let results = (outs NLRowState:$state);

  let assemblyFormat = "attr-dict";
}

/**
* RowCollect
*/
// NOT Pure: it appends the current chunk's rows to the handle's buffers, so it must run
// exactly where it sits (like nl.sort_collect).
def RowCollect : NLOp<"row_collect", []> {
  let summary = "Append this step's chunk of every column to a row accumulator";
  let description = [{
    Runs in the innermost loop body of what produces the rows, once per step, and
    appends the current chunk of each column to the matching buffer. Each branch of a
    UNION in a CALL body has its own nl.row_collect on the same handle, with the same
    column types, so the buffers hold the rows of one branch and then of the next.

      nl.row_collect %state, (%name) : !nl.chunk<!storage.nullable<!storage.string>>
  }];

  let arguments = (ins NLRowStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
  let hasVerifier = 1;
}

/**
* RowDrain
*/
// Pure: given a filled accumulator it deterministically yields the same rows, and
// produces no side effect of its own - the emission happens in the driving nl.for.
// Mirrors nl.sort: a source op whose loop does the work.
def RowDrain : NLOp<"row_drain", [Pure]> {
  let summary = "Create a database iterator over the rows a row accumulator collected";
  let description = [{
    The emit phase of a row accumulator. Reads a filled nl.row_state without consuming
    it, so each drain of a NEXT segment's table walks every row, and produces an
    iterator whose steps yield its rows in the order they were collected, one chunk per
    column per step:

      %rows = nl.row_drain(%state) : !nl.iter<!nl.chunk<!storage.node_id>>
      nl.for %z in %rows : !nl.iter<!nl.chunk<!storage.node_id>> { ... }

    Its first step must find every row already collected, so place it after the loop
    nests that collect, in the block holding the nl.row_buffer.
  }];

  let arguments = (ins NLRowStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

#endif // TURINGDB_NL_ROW_OPS
