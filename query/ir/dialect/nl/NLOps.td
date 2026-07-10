#ifndef TURINGDB_NL_OPS
#define TURINGDB_NL_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* ScanNodes
*/
def ScanNodes : NLOp<"scan_nodes", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over all nodes in the graph";
  let description = [{
    Imperative counterpart of db.scan_nodes. Each step of an enclosing
    nl.for fills one chunk of node IDs. The result type is always
    !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled out.
  }];
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(`operands`)` attr-dict";
}

/**
* ScanNodesByLabel
*/
def ScanNodesByLabel : NLOp<"scan_nodes_by_label", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.scan_nodes_by_label. Each step of an enclosing
    nl.for fills one chunk of the node IDs whose label set is a superset of
    `labels`:

      %nodes = nl.scan_nodes_by_label(["Person", "Employee"])

    `labels` is the same list of label names db.scan_nodes_by_label carried;
    translation resolves each name to a storage LabelID, the same way an
    nl.get_property_type name is resolved by the property fetch that consumes
    it. A label absent from the schema leaves the conjunction unsatisfiable, so
    the loop yields no chunk. Filtering by label narrows only the rows, never
    their shape, so - like nl.scan_nodes - the result is always
    !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled out.
  }];
  let arguments = (ins StrArrayAttr:$labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $labels `)` attr-dict";
}

/**
* GetOutEdges
*/
def GetOutEdges : NLOp<"get_out_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of a chunk of nodes";
  let description = [{
    Imperative counterpart of db.get_out_edges. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs.

    `columns_to_filter` is the carry set, mirroring db.get_out_edges: chunks
    bound by an outer nl.for that this hop must filter down to the rows that
    survive the traversal. Each carried chunk comes back - same type, filtered -
    as a trailing chunk of the result iterator, so an enclosing nl.for binds it
    as an extra loop variable after the four fixed edge chunks:

      %edges = nl.get_out_edges(%a, {})
      nl.for %srcs, %eids, %etypes, %b in %edges {
        // second hop b->c, carrying the `a` of this step (here %srcs)
        %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!storage.node_id>
        nl.for %srcs2, %eids2, %etypes2, %c, %aFiltered in %hop { ... }
      }

    The input chunk type and the four fixed result chunks are inferred; only
    the carried chunk types are spelled out, after the colon.
  }];

  // `columns_to_filter`: the variadic carry set, after the fixed input_nodes.
  // NLChunk, not NLNodeIDChunk, so chunks of any element type can be carried.
  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The carried chunk element types vary from call to call and - unlike
  // input_nodes and the inferred result - cannot be built from a recipe, so
  // they are spelled after the colon, the same as nl.output. The optional
  // group (anchored on the carried types) drops the colon when the carry set
  // is empty, keeping the first-hop syntax `(%nodes, {})`.
  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetInEdges
*/
def GetInEdges : NLOp<"get_in_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of a chunk of nodes";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges: where that walks each
    node's successors, this walks its predecessors. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs - the same chunk shape as out-edges, so the two are
    interchangeable in a loop.

    Like nl.get_out_edges it takes a `columns_to_filter` carry set: chunks from
    an outer nl.for that this hop filters and hands back as trailing chunks of
    the result iterator. The input chunk type and the four fixed result chunks
    are inferred; only the carried chunk types are spelled out, after the colon.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetPropertyType
*/
def GetPropertyType : NLOp<"get_property_type", [Pure]> {
  let summary = "Resolve a property name to a property type handle, once, ahead of the loops";
  let description = [{
    Looks the property name up in the graph schema and yields a
    !nl.property_type handle for its storage PropertyTypeID. It takes no chunk,
    so it sits at function scope above the loop nest: a query resolves each name
    once here and feeds the handle to every get_node_properties /
    get_edge_properties step, instead of resolving the name per chunk.

      %weight = nl.get_property_type("weight")

    The result is always !nl.property_type. That type is parameterless and so
    buildable, so the assembly omits it and the parser builds it from the
    recipe - no InferTypeOpAdaptor needed. Pure, so it can be hoisted and shared
    across the loops below it.
  }];

  // The property name, the same `name`/`weight` spelled in the query.
  let arguments = (ins StrAttr:$name);
  let results   = (outs NLPropertyType:$result);

  // No operands and a constant result type, so nothing is printed after the name.
  let assemblyFormat = "`(` $name `)` attr-dict";
}

/**
* GetNodeProperties
*/
def GetNodeProperties : NLOp<"get_node_properties", [Pure]> {
  let summary = "Read one property of a chunk of nodes, as a nullable value chunk";
  let description = [{
    Imperative counterpart of db.get_node_properties. The property is named once
    by nl.get_property_type and passed in as a %property_type handle. It opens no
    loop: it maps the input node chunk to one value per node, in order, straight
    into the current nl.for body.

    A node may not have the property, so the result is a nullable value chunk -
    !nl.chunk<!storage.nullable<...>>, storage's ColumnOptVector. Missing values come
    back null; no row is dropped. The wrapped type is the concrete primitive the
    property resolved to during lowering.

      %weight = nl.get_property_type("weight")
      nl.for %nodes in %scan {
        %values = nl.get_node_properties(%nodes, %weight) : !nl.chunk<!storage.nullable<f64>>
        nl.output(%nodes, %values) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>
      }
  }];

  // The node chunk and the property handle are both buildable, so their types
  // are omitted; only the nullable value chunk type is spelled, after the colon.
  let arguments = (ins NLNodeIDChunk:$input_nodes, NLPropertyTypeHandle:$property_type);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_nodes `,` $property_type `)` attr-dict `:` qualified(type($values))
  }];
}

/**
* GetEdgeProperties
*/
def GetEdgeProperties : NLOp<"get_edge_properties", [Pure]> {
  let summary = "Read one property of a chunk of edges, as a nullable value chunk";
  let description = [{
    The edge counterpart of get_node_properties: same shape, but the input is a
    chunk of edge IDs and the property is read from the edge side of the graph.
    It opens no loop and yields a nullable value chunk, one value per input edge.
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges, NLPropertyTypeHandle:$property_type);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_edges `,` $property_type `)` attr-dict `:` qualified(type($values))
  }];
}

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
def Yield : NLOp<"yield", [Pure, Terminator, HasParent<"For">]> {
  let summary = "End one step of an nl.for loop";
  let assemblyFormat = "attr-dict";
}

/**
* CrossProduct
*/
def CrossProduct : NLOp<"cross_product", [Pure, InferTypeOpAdaptor, AttrSizedOperandSegments]> {
  let summary = "Cross every row of one loop's chunks with every row of another";

  let description = [{
    Given an outer chunk of N rows and an inner chunk of M rows it produces N*M
    rows: each outer column is block-repeated x M (outer row i becomes M
    consecutive rows) and each inner column is tiled x N (the whole inner chunk
    repeated N times), so result row k pairs outer row k/M with inner row k%M -
    every outer row against every inner row.

      %a, %b = nl.cross_product {%a} {%b}
                 : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}

    The two brace groups are the columns each factor contributes, in db.yield
    order; the op's results are the outer columns followed by the inner, each
    result chunk keeping its input's element type - only the row count changes.
    N and M are read from the first column of each group, so each group must be
    non-empty.

    An optional `limit` operand carries an !nl.limit_state handle. When present,
    the product is built only up to limit.getRemaining() rows: rows are laid out in
    (outer, inner) order, so the product's prefix is exactly the prefix the
    following nl.limit_update/nl.output can emit this step, and building the rest
    would be wasted work. Absent, the whole N*M product is built.

      %a, %b = nl.cross_product {%a} {%b} limit %h
                 : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
  }];

  // The outer and inner column groups to cross, plus the optional limit handle.
  // Three operand groups of independent size need AttrSizedOperandSegments to
  // record each group's size.
  let arguments = (ins Variadic<NLChunk>:$outer_columns,
                       Variadic<NLChunk>:$inner_columns,
                       Optional<NLLimitStateHandle>:$limit);

  // One result per crossed column, outer group then inner group; inferred from
  // the operands since the broadcast keeps each chunk's element type.
  let results = (outs Variadic<NLChunk>:$results);

  // The two SSA brace groups mirror the two factors. The result types are
  // inferred, so only the operand chunk types are spelled - the same two
  // groups, each in braces, after the colon (as nl.output spells its chunk
  // types). The optional limit handle, when present, prints after a `limit`
  // keyword (its type is buildable and so omitted, as on nl.output). The
  // operandSegmentSizes attribute is populated and elided automatically by the
  // generated parser/printer.
  let assemblyFormat = [{
    `{` $outer_columns `}` `{` $inner_columns `}` (`limit` $limit^)? attr-dict `:`
    `{` qualified(type($outer_columns)) `}` `{` qualified(type($inner_columns)) `}`
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
  }];

  // Three variable-length operand groups - the columns, the optional limit and
  // the optional skip - so the op carries operand-segment sizes to tell them
  // apart, the same as nl.cross_product's column groups. A folded output carries
  // at most one of limit/skip (the truncate adjacent to it); the two are not
  // produced together (see DBLowering::foldSkipTruncatesIntoOutputs).
  let arguments = (ins Variadic<NLChunk>:$columns,
                       Optional<NLLimitStateHandle>:$limit,
                       Optional<NLSkipStateHandle>:$skip);

  // The chunk element types vary from call to call and - unlike the source
  // ops - cannot be inferred, so they are spelled after the colon. This keeps
  // the text self-describing: the parser builds each operand's type from the
  // printed list rather than looking it up on the defining nl.for (which MLIR
  // cannot do during a parse), the same reason nl.for keeps its : !nl.iter<...>
  // annotation. qualified(...) forces the leading !nl. on every printed type.
  // The optional limit and skip handles, when present, print after a `limit` or
  // `skip` keyword; both types are buildable and so omitted. Each optional group
  // has its own literal anchor, so the declarative parser tells them apart.
  let assemblyFormat = [{
    `(` $columns `)` (`limit` $limit^)? (`skip` $skip^)? attr-dict `:` qualified(type($columns))
  }];

  // Enforce the "at most one of limit/skip" invariant the description promises:
  // a folded output carries the single handle of the truncate adjacent to it,
  // never both. runOutput reads them as an if(skip)/else if(limit) chain, so a
  // malformed both-handles op would silently drop the limit; reject it here.
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
  }];

  // Unsigned: a row budget is never negative.
  let arguments = (ins UI64Attr:$count);
  let results   = (outs NLLimitState:$state);
  let assemblyFormat = "`(` $count `)` attr-dict";
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
    omitted.
  }];

  // Unsigned: a count of rows to drop is never negative.
  let arguments = (ins UI64Attr:$count);
  let results   = (outs NLSkipState:$state);
  let assemblyFormat = "`(` $count `)` attr-dict";
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
  }];

  // keyColumns are indices into the collected column set, most significant first;
  // keyAscending gives one direction per key. The two arrays are parallel. The
  // optional topK count, when present, bounds the accumulator to its best rows.
  let arguments = (ins DenseI64ArrayAttr:$keyColumns,
                       DenseBoolArrayAttr:$keyAscending,
                       OptionalAttr<UI64Attr>:$topK);
  let results   = (outs NLSortState:$state);

  let assemblyFormat = "`keys` $keyColumns `ascending` $keyAscending (`limit` $topK^)? attr-dict";
  let hasVerifier = 1;
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

/**
* Count
*/
// NOT Pure: each nl.count is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - resetting the tally is a side effect
// (like nl.limit / nl.sort_buffer).
def Count : NLOp<"count", []> {
  let summary = "Create (and reset) a row-count accumulator, initialized to zero";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level or
    mid-query COUNT - where it zeroes the tally once per execution of that block.
    Produces the !nl.count_state handle nl.count_update increments and
    nl.count_result reads. The result type is buildable and omitted.

      %c = nl.count
  }];

  let results = (outs NLCountState:$state);
  let assemblyFormat = "attr-dict";
}

/**
* CountUpdate
*/
// NOT Pure: it increments the handle, so it must run exactly where it sits, once
// per producing-loop step, and must not be hoisted, CSE'd or eliminated. The count
// sibling of nl.limit_update / nl.sort_collect.
def CountUpdate : NLOp<"count_update", []> {
  let summary = "Charge this step's non-null rows against a count accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Adds the number of non-null
    rows of the current chunk to the tally - all rows of an ID chunk (IDs are never
    null), or the present values of a nullable value chunk (Cypher count(x) counts
    the rows in which x is not null). The sole counter mutator.

      nl.count_update %c, %a : !nl.chunk<!storage.node_id>
  }];

  // The accumulator handle, then the column whose non-null rows are charged. The
  // handle type is buildable and omitted; the column chunk type is spelled.
  let arguments = (ins NLCountStateHandle:$state, NLChunk:$rows);

  let assemblyFormat = "$state `,` $rows attr-dict `:` qualified(type($rows))";
}

/**
* CountResult
*/
// NOT Pure: it reads the handle's mutable tally (set by the nl.count_updates in
// the producing loop), so it must run exactly where it sits - after that loop -
// and must not be hoisted, CSE'd or eliminated. The count sibling of
// nl.limit_truncate, which likewise reads mutable counter state.
def CountResult : NLOp<"count_result", []> {
  let summary = "Materialize the single result row of a count as a chunk";
  let description = [{
    The emit step of a COUNT. It reads a filled nl.count_state and materializes the
    tally directly as a single-row chunk - one unsigned i64. A count is a
    non-negative number that is never null, so the chunk is a plain !nl.chunk<ui64>,
    not a nullable value chunk.

    Unlike nl.sort it is not a source iterator: a COUNT collapses to exactly one
    row, so there is nothing to iterate. It produces the chunk in place, at function
    scope after the producing loop, and nl.output consumes it there - no emit loop:

      %r = nl.count_result(%c) : !nl.chunk<ui64>
      nl.output(%r) : !nl.chunk<ui64>

    The result chunk type is fixed (a ui64) but cannot be inferred from the
    parameterless state handle, so it is spelled after the colon. Placing
    nl.count_result after the producing loop guarantees the tally is final.
  }];

  let arguments = (ins NLCountStateHandle:$state);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* Aggregate
*/
// NOT Pure: the value-reducing sibling of nl.count. Each nl.aggregate is a
// distinct accumulator, so two must never be CSE'd into one, and an unread one
// must not be DCE'd - resetting the accumulator is a side effect.
def Aggregate : NLOp<"aggregate", []> {
  let summary = "Create (and reset) a value-aggregate accumulator";
  let description = [{
    The value-reducing counterpart of nl.count. Sits at the top of the scope it
    governs, where it resets the accumulator once per execution of that block, and
    produces the !nl.aggregate_state handle nl.aggregate_update folds into and
    nl.aggregate_result reads. `$kind` selects the reduction (sum/min/max/avg); the
    state type carries the accumulator element type, so the result type is spelled.

      %s = nl.aggregate sum : !nl.aggregate_state<i64>
  }];

  let arguments = (ins AggregateKind:$kind);
  let results   = (outs NLAggregateState:$state);
  let assemblyFormat = "$kind attr-dict `:` qualified(type($state))";

  // avg accumulates a running sum as f64, so its accumulator state must be f64;
  // the other kinds accumulate in the value's own type.
  let hasVerifier = 1;
}

/**
* AggregateUpdate
*/
// NOT Pure: it folds the chunk into the handle, so it must run exactly where it
// sits, once per producing-loop step. The value sibling of nl.count_update.
def AggregateUpdate : NLOp<"aggregate_update", []> {
  let summary = "Fold this step's non-null rows into a value-aggregate accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Reduces the present (non-null)
    values of the current nullable value chunk into the accumulator - adding them
    (sum/avg) or keeping the extreme (min/max) - matching Cypher, where these
    aggregates ignore null rows. The sole accumulator mutator.

      nl.aggregate_update sum %s, %v : !nl.aggregate_state<i64>, !nl.chunk<!storage.nullable<i64>>

    The input is always a nullable value chunk (a property fetch); its value type
    need not match the accumulator's - an avg of an i64 column folds into an f64
    accumulator, so both types are spelled after the colon.
  }];

  let arguments = (ins NLAggregateState:$state, NLChunk:$rows, AggregateKind:$kind);

  let assemblyFormat = [{
    $kind $state `,` $rows attr-dict `:` qualified(type($state)) `,` qualified(type($rows))
  }];

  // The fold handler is picked from this op's kind but the accumulator was reset by
  // the producing nl.aggregate's kind, and the handler reads the accumulator as its
  // own value type - so a kind that disagrees with the producer, an input that is
  // not a nullable value chunk, or a state element type that does not match the
  // reduction would fold into a wrongly-typed or wrongly-initialized accumulator.
  // The verifier reconciles all three so such IR is rejected rather than crashing.
  let hasVerifier = 1;
}

/**
* AggregateResult
*/
// NOT Pure: it reads the handle's mutable accumulator (set by the
// nl.aggregate_updates in the producing loop), so it must run exactly where it
// sits - after that loop. The value sibling of nl.count_result.
def AggregateResult : NLOp<"aggregate_result", []> {
  let summary = "Materialize the single result row of a value-aggregate as a chunk";
  let description = [{
    The emit step of a SUM/MIN/MAX/AVG. It reads a filled nl.aggregate_state and
    materializes the reduced value directly as a single-row nullable value chunk.
    Unlike count, an aggregate can be null (min/max/avg of no non-null row), so the
    result is a nullable value chunk, not a plain chunk; sum is never null (0 for an
    empty input) but rides the same nullable representation for uniformity.

      %r = nl.aggregate_result sum (%s) : !nl.aggregate_state<i64> -> !nl.chunk<!storage.nullable<i64>>
      nl.output(%r) : !nl.chunk<!storage.nullable<i64>>

    Like nl.count_result it is not a source iterator: an aggregate collapses to one
    row, so it produces the chunk in place at function scope after the producing
    loop, and nl.output consumes it there - no emit loop.
  }];

  let arguments = (ins NLAggregateState:$state, AggregateKind:$kind);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    $kind `(` $state `)` attr-dict `:` qualified(type($state)) `->` qualified(type($result))
  }];

  // The emit handler copies the accumulator into the result (or divides for avg),
  // both read as the result's value type, so the result must be a nullable value
  // chunk whose value type matches the reduction, and the kind must match the
  // producing nl.aggregate - the verifier enforces both. The result sibling of
  // AggregateUpdate's verifier.
  let hasVerifier = 1;
}

def Constant : NLOp<"constant", [Pure, InferTypeOpAdaptor]> {
  let summary = "Materialize a constant scalar as a single-row chunk";
  let description = [{
      Represents a ColumnVector<T> with a single row
      %c = nl.constant(30 : i64) : !nl.chunk<i64>
  }];

  let arguments = (ins TypedAttrInterface:$value);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` $value `)` attr-dict";
}

/**
* Add
*/
def Add : NLOp<"add", [Pure]> {
  let summary = "Row-wise addition of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double + double), and nullability (e.g. null + non-null = null).

      %r = nl.add %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Sub : NLOp<"sub", [Pure]> {
  let summary = "Row-wise subtraction of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double - double), and nullability (e.g. null - non-null = null).

      %r = nl.sub %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Eq : NLOp<"eq", [Pure]> {
  let summary = "Row-wise equality of two value chunks";
  let description = [{
      %r = nl.eq %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

#endif //TURINGDB_NL_OPS
