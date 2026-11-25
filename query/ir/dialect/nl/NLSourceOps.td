#ifndef TURINGDB_NL_SOURCE_OPS
#define TURINGDB_NL_SOURCE_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* UnwindConst
*/
// Pure: emitting a fixed list is deterministic; the per-element fan-out is in the nl.for.
def UnwindConst : NLOp<"unwind_const", [Pure]> {
  let summary = "Create a database iterator over a compile-time-known literal list";
  let description = [{
    Imperative counterpart of db.unwind_const. Each step of an enclosing nl.for fills
    one chunk of the literal list's elements:

      %rows = nl.unwind_const([1, 2, 3]) : !nl.iter<!nl.chunk<!storage.nullable<i64>>>
      nl.for %x in %rows : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
        nl.output(%x) : !nl.chunk<!storage.nullable<i64>>
      }

    `elements` is the same literal array db.unwind_const carried, each entry an
    attribute keeping its own type. A homogeneous list yields a nullable value chunk of
    that type - the shape a property fetch and the unwind_collect drain emit, so the
    unwound column feeds the same consumers, even though such a literal is never null;
    a heterogeneous or empty list yields a type-erased !storage.any chunk (a
    tagged scalar per row), which is also what a list holding a null or a nested list
    unwinds into. The single chunk element type depends on the elements, so -
    unlike nl.scan_nodes, whose node-ID iterator is fixed and inferred - it is spelled
    after the colon, as nl.collect's is. It opens the dataflow: no input, no carry.
  }];

  let arguments = (ins ArrayAttr:$elements);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $elements `)` attr-dict `:` qualified(type($result))";
}

/**
* LoadCSV
*/
// Pure: reading a file is deterministic; the per-record fan-out is in the nl.for.
def LoadCSV : NLOp<"load_csv", [Pure]> {
  let summary = "Create a database iterator over the records of a CSV file";
  let description = [{
    Imperative counterpart of db.load_csv. Each step of an enclosing nl.for parses the
    next chunk of records, filling one chunk per field the load produces:

      %name, %age = nl.load_csv("people.csv", [0 : ui64, 1 : ui64])
        : !nl.iter<!nl.chunk<!storage.string>, !nl.chunk<!storage.string>>

    The path, the field list and the two flags are the ones db.load_csv carried,
    forwarded as-is, a bound path as the chunk holding it:

      %name = nl.load_csv(%path : !nl.chunk<!storage.string>, [0 : ui64])
        : !nl.iter<!nl.chunk<!storage.string>>

    The path is resolved against the data directory, and a header name against the
    file's header line, when the loop first runs - the file is the one thing a loop of
    this family reads outside the graph. Like nl.unwind_const it reads no row and
    carries nothing: it opens the dataflow.

    Every field rides a string chunk viewing characters copied into the query's string
    buffer - a record carries no type to read them as. The number of
    chunks is the number of fields, so the iterator type is spelled after the colon
    rather than inferred.
  }];

  let arguments = (ins OptionalAttr<StrAttr>:$path,
                       Optional<NLChunk>:$bound_path,
                       ArrayAttr:$fields,
                       UnitAttr:$with_headers,
                       UnitAttr:$skip_on_error);
  let results   = (outs NLIterator:$result);
  let hasVerifier = 1;
  let assemblyFormat = [{
    `(` custom<BoundString>($path, $bound_path, type($bound_path)) `,` $fields `)`
    (`with_headers` $with_headers^)? (`skip_on_error` $skip_on_error^)?
    attr-dict `:` qualified(type($result))
  }];
}

/**
* VectorSearch
*/
// Pure: reading the index is deterministic; the per-neighbour fan-out is in the nl.for.
def VectorSearch : NLOp<"vector_search", [Pure, InferTypeOpAdaptor, AttrSizedOperandSegments]> {
  let summary = "Create a database iterator over the nearest neighbours of a query vector";
  let description = [{
    Imperative counterpart of db.vector_search. Each step of an enclosing nl.for fills
    one row of neighbour chunks - the nodes the index holds the vectors under, and the
    distances the metric scored them at:

      %n = nl.vector_search("vec1", 3, [1.000000e+00, 0.000000e+00])
        : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>

    The parameters are the ones db.vector_search carried, forwarded as-is, a bound
    index name, neighbour count or query vector as the chunk holding it:

      %n = nl.vector_search(%idx : !nl.chunk<!storage.string>, %k : !nl.chunk<i64>,
                            %v : !nl.chunk<!storage.embedding>)

    The search runs again when the loop starts over values other than the last search's,
    as it does once per row in the body of a subquery run once per row.

    The search runs against the session's vector indexes, which is the one thing a loop
    of this family reads outside the graph. Like nl.unwind_const it reads no row and
    carries nothing: it opens the dataflow. The neighbours ride the same node ID chunk a
    scan binds, so a hop expands them unchanged; the distances ride a nullable value
    chunk, the shape every value-chunk consumer dispatches on, even though a neighbour is
    never null. Both element types are fixed, so the iterator type is inferred rather
    than spelled.
  }];

  let arguments = (ins OptionalAttr<StrAttr>:$index_name,
                       Optional<NLChunk>:$bound_index_name,
                       OptionalAttr<UI64Attr>:$k,
                       Optional<NLChunk>:$bound_k,
                       OptionalAttr<DenseF32ArrayAttr>:$query_vector,
                       Optional<NLChunk>:$bound_query_vector);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = [{
    `(` custom<BoundString>($index_name, $bound_index_name, type($bound_index_name)) `,`
    custom<BoundCount>($k, $bound_k, type($bound_k)) `,`
    custom<QueryVector>($query_vector, $bound_query_vector, type($bound_query_vector)) `)`
    attr-dict `:` qualified(type($result))
  }];
  let hasVerifier = 1;
}

/**
* Unwind
*/
// Pure: expanding a chunk of values is deterministic; the per-element fan-out is in the nl.for.
def Unwind : NLOp<"unwind", [Pure]> {
  let summary = "Create a database iterator that expands a chunk of values, one row per element";
  let description = [{
    Imperative counterpart of db.unwind. Each step of an enclosing nl.for fills one
    chunk of unwound elements plus one chunk per carried column, gathered so a carried
    cell repeats once per row its source cell produced:

      %rows = nl.unwind(%names, {%nodes})
                : (!nl.chunk<!storage.list<!storage.string>>, !nl.chunk<!storage.node_id>)
                -> !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>>
      nl.for %x, %n in %rows : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>> {
        nl.output(%x, %n) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>
      }

    A list cell contributes one row per element, a null cell none, and any other cell
    the single row it is - a tagged scalar included, so a !storage.any cell that is
    itself a list unwinds one level deeper. Unlike nl.unwind_const it reads an input
    chunk, so it nests in the loop that binds it, the way nl.get_out_edges nests in the
    loop binding its nodes. The element chunk type follows the source's
    (`unwoundElementType`) - a list of IDs or paths unwinds into a chunk of them, a list
    of values into a nullable chunk of their type, any other list into the type-erased
    !storage.any chunk, a scalar column into its own - and, like nl.collect's, is
    spelled out rather than inferred.
  }];

  // `columns_to_filter`: the variadic carry set, after the fixed source, mirroring
  // nl.get_out_edges.
  let arguments = (ins NLChunk:$source, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The source chunk's element type decides the element chunk's, and the carried types
  // vary from call to call, so operands and result print as a function type - as
  // nl.broadcast_constant's do - rather than the result alone.
  let assemblyFormat = [{
    `(` $source `,` `{` $columns_to_filter `}` `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* UnwindRange
*/
// Pure: counting out a row's integers is deterministic; the per-integer fan-out is in the nl.for.
def UnwindRange : NLOp<"unwind_range", [Pure, AttrSizedOperandSegments]> {
  let summary = "Create a database iterator that counts out the integers each row's bounds span";
  let description = [{
    Imperative counterpart of db.unwind_range. Each step of an enclosing nl.for fills one
    chunk of the next integers plus one chunk per carried column, gathered so a carried
    cell repeats once per integer its row's bounds span:

      %rows = nl.unwind_range(%from, %to, %by, {%nodes})
                : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>,
                   !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.node_id>)
                -> !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.node_id>>

    Every bound carries rows - lowering lays a constant one out over them first - and is
    read as a nullable value chunk, or as a chunk of type-erased cells whose integer each
    row reads out of its cell, raising on any other value. The integers are computed as the
    chunk is filled, so the iterator holds one chunk of them however many a row spans.
    `from`, `to` and `reversed` are db.unwind_range's, and the slice bounds are read as its
    range bounds are.
  }];

  let arguments = (ins NLChunk:$start,
                       NLChunk:$end,
                       NLChunk:$step,
                       Optional<NLChunk>:$from,
                       Optional<NLChunk>:$to,
                       Variadic<NLChunk>:$columns_to_filter,
                       UnitAttr:$reversed);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $start `,` $end `,` $step (`,` `from` $from^)? (`,` `to` $to^)? `,` `{` $columns_to_filter `}` `)`
    (`reversed` $reversed^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* UnwindComputed
*/
def UnwindComputed : NLOp<"unwind_computed", [Pure]> {
  let summary = "Create a database iterator that computes its source over a few input rows at a time and unwinds it";
  let description = [{
    Imperative counterpart of db.unwind_computed. The region's arguments are the carried
    chunks cut to a run of input rows, and its yield the source cells of that run. Each
    step of an enclosing nl.for fills one chunk of elements, as nl.unwind does, plus one
    chunk per carried column gathered from the whole input; once a run's cells are spent
    the region runs again over the next, sized so it holds about one chunk of elements:

      %rows = nl.unwind_computed({%names}) ({
        ^bb0(%namesIn: !nl.chunk<!storage.nullable<!storage.string>>):
          %parts = nl.split %namesIn, %comma : ...
          nl.unwind_yield %parts : !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>
      }) : (!nl.chunk<!storage.nullable<!storage.string>>)
         -> !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>,
                     !nl.chunk<!storage.nullable<!storage.string>>>
  }];

  let arguments = (ins Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // Exactly one block, ending in an nl.unwind_yield.
  let regions = (region SizedRegion<1>:$source);

  let assemblyFormat = [{
    `(` `{` $columns_to_filter `}` `)` $source attr-dict `:` functional-type(operands, results)
  }];
}

/**
* UnwindYield
*/
def UnwindYield : NLOp<"unwind_yield", [Pure, Terminator, HasParent<"UnwindComputed">]> {
  let summary = "Name the source cells an nl.unwind_computed's region computes for a run of rows";

  let arguments = (ins NLChunk:$source);

  let assemblyFormat = [{
    $source attr-dict `:` qualified(type($source))
  }];
}

/**
* Procedure
*/
// NOT Pure: it prepares a live procedure - the alloc callback's data and the
// procedure's PREPARE step - so two must never be CSE'd into one (each CALL is its
// own call), and an unread one must not be DCE'd: preparing it is a side effect
// (like nl.limit / nl.sort_buffer).
def Procedure : NLOp<"procedure", []> {
  let summary = "Prepare one procedure call, once, ahead of the loops";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level CALL -
    where it prepares the procedure once and re-initializes it each further time
    that block runs. Produces the !nl.procedure_state handle nl.procedure_init reads.

      %call = nl.procedure("db.labels") yields ["id", "label"]

    `name` is the procedure's full name, resolved against the procedure registry
    during translation - the procedure sibling of nl.get_property_type, which
    resolves a property name once above the loops so the consumers below reuse it.
    `yields` names the return values this call binds, in the order the consuming op
    produces them, so the columns the procedure writes into are allocated once here
    and shared by every step. It may be empty - a procedure declaring no return value
    binds no column and is driven for what it does rather than for rows. The result
    type is buildable and omitted.
  }];

  let arguments = (ins StrAttr:$name, StrArrayAttr:$yields);
  let results   = (outs NLProcedureState:$state);

  let assemblyFormat = "`(` $name `)` `yields` $yields attr-dict";
  let hasVerifier = 1;
}

/**
* ProcedureInit
*/
// NOT Pure: driving the iterator runs the procedure's execute callback, so no CSE of two
// iterators over one call and no DCE of an unread one.
def ProcedureInit : NLOp<"procedure_init", [AttrSizedOperandSegments]> {
  let summary = "Create a database iterator driving a procedure over its arguments";
  let description = [{
    The row-producing form of a CALL, the procedure counterpart of nl.scan_nodes and
    nl.get_out_edges: it opens an iterator over the rows the procedure produces, and
    each step of the enclosing nl.for calls the procedure's execute callback once,
    which refills the result columns with that step's rows. The loop runs until the
    procedure declares itself finished, so - exactly like an edge hop over one chunk of
    nodes - a procedure may answer one chunk of arguments with as many chunks of rows as
    it needs:

      %call = nl.procedure("db.labels") yields ["id", "label"]
      %rows = nl.procedure_init(%call, (), {}) : (!nl.procedure_state) -> !nl.iter<!nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>>
      nl.for %ids, %labels in %rows : !nl.iter<...> {
        nl.output(%ids, %labels) : !nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>
      }

    `inputs` are the call's arguments, in the procedure's declaration order, so operand
    i is argument i. With none the procedure is a source and its loop sits at function
    scope, as above. With arguments the iterator is created inside the loop that binds
    them and re-opened once per chunk of them - the nested-loop shape nl.get_out_edges
    has - so the procedure is reset at the top of each drive and starts afresh for the
    new chunk.

    `columns_to_filter` is the carry set, mirroring nl.get_out_edges: the chunks bound
    alongside the arguments that must survive the call. A procedure need not emit one
    row per input row, so a carried chunk cannot pass through untouched - each step
    rebuilds it from the input-row index the procedure reports for every row it emits,
    which replicates the rows of an expanding call and drops those of a filtering one.
    Each carried chunk comes back as a trailing chunk of the iterator, so the nl.for
    binds it after the yields:

      %rows = nl.procedure_init(%call, (%m), {%n, %m})
                : (!nl.procedure_state, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
                  -> !nl.iter<!nl.chunk<i64>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>>
      nl.for %x, %n2, %m2 in %rows : !nl.iter<...> {
        nl.output(%n2, %m2, %x) : ...
      }

    Only a procedure that declares it makes that report can be carried past
    (Procedure::getIndices), which lowering settles at plan time; hand-written IR
    is caught during translation. A procedure that declares it and then reports nothing
    is caught at the first step it emits, rather than silently misaligning the
    projection.

    The iterator's chunk types are the yielded return values' column types followed by
    the carried ones. Neither can be recovered from the parameterless state handle, so
    they are spelled after the colon (as the nl.for iterator type is). A call binding no
    return value gives an iterator of no chunk, whose nl.for binds no variable and runs an
    empty body: the drive is all there is to it, which is what a call made for its side
    effect wants.
  }];

  // Two variadic operand groups - the arguments and the carry set - so the op carries
  // operand-segment sizes to tell them apart, as nl.cross_product does.
  let arguments = (ins NLProcedureStateHandle:$state,
                       Variadic<NLChunk>:$inputs,
                       Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The handle prints first, then the parenthesized argument chunks and the
  // brace-wrapped carry set, so a source call reads `nl.procedure_init(%call, (), {})`
  // - the brace style nl.get_out_edges uses, which keeps the syntax total without an
  // optional group. Neither operand group's chunk types are buildable and the iterator
  // type cannot be inferred from them, so functional-type spells both sides, as
  // nl.filter does.
  let assemblyFormat = [{
    `(` $state `,` `(` $inputs `)` `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

#endif // TURINGDB_NL_SOURCE_OPS
