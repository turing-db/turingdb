#ifndef TURINGDB_NL_SUBQUERY_OPS
#define TURINGDB_NL_SUBQUERY_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* OptionalBuffer
*/
// NOT Pure: it re-initializes the accumulator, so it must run exactly where it
// sits - once per step of the rows the OPTIONAL MATCH joins onto - and must not
// be hoisted, CSE'd or eliminated (like nl.sort_buffer).
def OptionalBuffer : NLOp<"optional_buffer", [RowAlignedThroughOperands]> {
  let summary = "Create (and reset) the accumulator for one OPTIONAL MATCH over this step's rows";
  let description = [{
    Sits at the top of the block whose step it covers - the innermost loop body
    binding the columns the OPTIONAL MATCH joins onto - so the accumulator is
    empty again at every step. It records this step's input chunks, clears the
    matched flag of each of their rows, and produces the row tag: a chunk holding
    each input row's position, which the pattern carries alongside the columns it
    walks so nl.optional_collect knows which rows matched.

      %state, %tag = nl.optional_buffer (%p) : {!nl.chunk<!storage.node_id>}

    With no input columns - a query opening on OPTIONAL MATCH - the step is the
    single empty row Cypher starts from, so the accumulator holds one flag and the
    tag one row. The pattern has nothing to join onto there, so it never carries
    that tag, which is why the op is row-aligned through its operands: a tag over
    input rows holds exactly the rows those chunks do.
  }];

  // The chunks the OPTIONAL MATCH joins onto; the accumulator reads its row count
  // from the first of them, and rebuilds the rows the pattern missed out of them.
  let arguments = (ins Variadic<NLChunk>:$input_columns);

  // The accumulator, then the row tag over this step's input rows.
  let results = (outs NLOptionalState:$state, NLUInt64Chunk:$tag);

  // The tag's type is buildable and so omitted; only the input chunk types are
  // spelled, in braces as nl.cross_product spells a factor's.
  let assemblyFormat = [{
    `(` $input_columns `)` attr-dict (`:` `{` qualified(type($input_columns))^ `}`)?
  }];
}

/**
* OptionalCollect
*/
// NOT Pure: it appends the step's matched rows to the accumulator and marks the
// input rows they came from, so it must run exactly where it sits (like
// nl.sort_collect).
def OptionalCollect : NLOp<"optional_collect", [AttrSizedOperandSegments]> {
  let summary = "Append this step's matched rows to an OPTIONAL MATCH accumulator";
  let description = [{
    Runs in the pattern's innermost loop body, once per step. Appends the current
    chunk of every column the pattern contributes to the accumulator's buffers,
    and marks as matched each input row the tag names, so once the pattern is
    walked the unmarked rows are exactly the ones it missed.

      nl.optional_collect %state, %tag, (%p2, %e, %f)
        : {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>}

    The tag is absent when the accumulator has no input columns: the step is then
    the single empty row, which any collected row marks.
  }];

  let arguments = (ins NLOptionalStateHandle:$state,
                       Optional<NLUInt64Chunk>:$tag,
                       Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` ($tag^ `,`)? `(` $columns `)` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

/**
* OptionalDrain
*/
// Pure: given a filled accumulator it deterministically yields the same rows, and
// produces no side effect of its own - the emission happens in the driving
// nl.for. Mirrors nl.sort: a source op whose loop does the work.
def OptionalDrain : NLOp<"optional_drain", [Pure]> {
  let summary = "Create a database iterator over an OPTIONAL MATCH's matched rows, then its missed ones";
  let description = [{
    The emit phase of an OPTIONAL MATCH. Consumes a filled nl.optional_state and
    produces an iterator whose steps yield first the rows the pattern matched, as
    collected, and then one row per input row it missed: the input columns taken
    from the step's own chunks and the pattern's own columns null - an invalid ID,
    which reads as null through a property fetch.

      %rows = nl.optional_drain(%state) : !nl.iter<!nl.chunk<!storage.node_id>, ...>
      nl.for %p, %e, %f in %rows : !nl.iter<...> { ... }

    Its first step must find every matched row already collected, so place it after
    the pattern's loop nest, in the block holding the nl.optional_buffer that opened
    the accumulator.
  }];

  let arguments = (ins NLOptionalStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* ExistsBuffer
*/
def ExistsBuffer : NLOp<"exists_buffer", [RowAlignedThroughOperands]> {
  let summary = "Create (and reset) the accumulator for one EXISTS over this step's rows";
  let description = [{
    Sits at the top of the block whose step it covers - the innermost loop body
    binding the columns the EXISTS answers for - so the accumulator is empty again at
    every step. It records this step's input chunks, clears the matched flag of each
    of their rows, and produces the row tag: a chunk holding each input row's
    position, which the body carries alongside the columns it walks so nl.exists_mark
    knows which rows it produced a row for.

      %state, %tag = nl.exists_buffer (%p) : {!nl.chunk<!storage.node_id>}

    With no input columns - an EXISTS over the single empty row Cypher starts from -
    the accumulator holds one flag and the tag one row. The body has nothing to join
    onto there, so it never carries that tag, which is why the op is row-aligned
    through its operands: a tag over input rows holds exactly the rows those chunks do.
  }];

  // The chunks the EXISTS answers for; the accumulator reads its row count from the
  // first of them, and nl.exists_result lays its answer out over the same rows.
  let arguments = (ins Variadic<NLChunk>:$input_columns);

  // The accumulator, then the row tag over this step's input rows.
  let results = (outs NLExistsState:$state, NLUInt64Chunk:$tag);

  // The tag's type is buildable and so omitted; only the input chunk types are
  // spelled, in braces as nl.optional_buffer spells its own.
  let assemblyFormat = [{
    `(` $input_columns `)` attr-dict (`:` `{` qualified(type($input_columns))^ `}`)?
  }];
}

/**
* ExistsMark
*/
// NOT Pure: it marks the input rows this step's rows came from, so it must run exactly
// where it sits (like nl.optional_collect).
def ExistsMark : NLOp<"exists_mark", [AttrSizedOperandSegments]> {
  let summary = "Mark the input rows an EXISTS body produced a row for";
  let description = [{
    Runs in the body's innermost loop body, once per step. Marks as matched each input
    row the tag names, so once the body is walked the marked rows are exactly the ones
    it produced a row for.

      nl.exists_mark %state, %tag, (%k) : {!nl.chunk<!storage.node_id>}

    The tag is absent when every row the body produced answers for one input row, which
    the columns mark when they hold a row at all - which is what they are named for, an
    untagged mark having nothing else to answer from. That row is the one whose position
    the one-row `row` chunk holds: the row of the step a body run one row at a time is
    handed, its tag read through nl.each_row. With no `row` it is the single empty row an
    accumulator with no input column holds.

      nl.exists_mark %state, row %r, (%k) : {!nl.chunk<!storage.node_id>}
  }];

  let arguments = (ins NLExistsStateHandle:$state,
                       Optional<NLUInt64Chunk>:$tag,
                       Optional<NLUInt64Chunk>:$row,
                       Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` ($tag^ `,`)? (`row` $row^ `,`)? `(` $columns `)` attr-dict (`:` `{` qualified(type($columns))^ `}`)?
  }];

  let hasVerifier = 1;
}

/**
* ExistsResult
*/
// Pure: given a filled accumulator it deterministically lays out the same flags and
// produces no side effect of its own. Mirrors nl.optional_drain, and like it must be
// placed after the body's loop nest, in the block holding the nl.exists_buffer.
def ExistsResult : NLOp<"exists_result", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read an EXISTS accumulator's flags as a boolean chunk";
  let description = [{
    The answer phase of an EXISTS. Consumes a filled nl.exists_state and produces one
    boolean per input row of the step - true for a row the body produced a row for -
    row-aligned with the chunks the accumulator was opened over, which is what lets the
    result stand beside them in the rows in flight. The cardinality operand is one of
    those chunks; it is absent when the accumulator was opened over none, which is one
    row of its own.

      %b = nl.exists_result(%state, %p) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>

    Its read must find every mark already made, so place it after the body's loop nest,
    in the block holding the nl.exists_buffer that opened the accumulator.
  }];

  let arguments = (ins NLExistsStateHandle:$state, Optional<NLChunk>:$cardinality);
  let results   = (outs NLBoolChunk:$result);

  let assemblyFormat = [{
    `(` $state (`,` $cardinality^)? `)` attr-dict `:` functional-type($cardinality, results)
  }];
}

/**
* CountSubqueryBuffer
*/
def CountSubqueryBuffer : NLOp<"count_subquery_buffer", [RowAlignedThroughOperands]> {
  let summary = "Create (and reset) the accumulator for one COUNT subquery over this step's rows";
  let description = [{
    nl.exists_buffer's counting sibling: it sits at the top of the block whose step it
    covers, records this step's input chunks, zeroes the count of each of their rows and
    produces the row tag the body carries.

      %state, %tag = nl.count_subquery_buffer (%p) : {!nl.chunk<!storage.node_id>}

    With no input columns the accumulator holds one count, for the single empty row
    Cypher starts from.
  }];

  let arguments = (ins Variadic<NLChunk>:$input_columns);

  let results = (outs NLCountSubqueryState:$state, NLUInt64Chunk:$tag);

  let assemblyFormat = [{
    `(` $input_columns `)` attr-dict (`:` `{` qualified(type($input_columns))^ `}`)?
  }];
}

/**
* CountSubqueryTally
*/
// NOT Pure: it adds to the counts of the input rows this step's rows came from, so it
// must run exactly where it sits.
def CountSubqueryTally : NLOp<"count_subquery_tally", [AttrSizedOperandSegments]> {
  let summary = "Count the rows a COUNT body produced toward the input rows they came from";
  let description = [{
    Runs in the body's innermost loop body, once per step. Adds one to the count of the
    input row each entry of the tag names, so once the body is walked every input row
    holds the number of rows it produced.

      nl.count_subquery_tally %state, %tag

    A body with no tag answers for a single row and adds the row count of `rows` to it
    instead: the row whose position the one-row `row` chunk holds - the row of the step a
    body run one row at a time is handed - or, with no `row`, the single empty row.

      nl.count_subquery_tally %state row %r rows(%k : !nl.chunk<!storage.node_id>)
  }];

  let arguments = (ins NLCountSubqueryStateHandle:$state,
                       Optional<NLUInt64Chunk>:$tag,
                       Optional<NLUInt64Chunk>:$row,
                       Optional<NLChunk>:$rows);

  let assemblyFormat = [{
    $state (`,` $tag^)? (`row` $row^)? (`rows` `(` $rows^ `:` qualified(type($rows)) `)`)? attr-dict
  }];

  let hasVerifier = 1;
}

/**
* CountSubqueryResult
*/
// Pure, as nl.exists_result is, and placed like it: after the body's loop nest, in the
// block holding the nl.count_subquery_buffer.
def CountSubqueryResult : NLOp<"count_subquery_result", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read a COUNT subquery accumulator's counts as a chunk";
  let description = [{
    Produces one count per input row of the step, row-aligned with the chunks the
    accumulator was opened over. The cardinality operand is one of those chunks; it is
    absent when the accumulator was opened over none, which is one row of its own.

      %n = nl.count_subquery_result(%state, %p) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
  }];

  let arguments = (ins NLCountSubqueryStateHandle:$state, Optional<NLChunk>:$cardinality);
  let results   = (outs NLUInt64Chunk:$result);

  let assemblyFormat = [{
    `(` $state (`,` $cardinality^)? `)` attr-dict `:` functional-type($cardinality, results)
  }];
}

/**
* RowListsBuffer
*/
// NOT Pure: it re-initializes the accumulator, so it must run exactly where it sits -
// once per step of the rows the lists are read beside - and must not be hoisted, CSE'd
// or eliminated (like nl.optional_buffer).
def RowListsBuffer : NLOp<"row_lists_buffer", [RowAlignedThroughOperands]> {
  let summary = "Create (and reset) the accumulator for one list per row of this step";

  let description = [{
    The accumulator of a pattern comprehension or a COLLECT subquery. Sits at the top of
    the block whose step it covers - the innermost loop body binding the columns the lists
    are read beside - so the accumulator is empty again at every step. It sizes itself to
    the rows of the cardinality chunk and produces the row tag: a chunk holding each row's
    position, which the body carries alongside the columns it walks so
    nl.row_lists_collect knows which row's list a value goes into.

      %state, %tag = nl.row_lists_buffer(%a) : !nl.chunk<!storage.node_id>

    The cardinality operand is absent when nothing is in flight where the lists are read -
    RETURN [(a:Person)-->(b) | b.name] - which is one row of its own. The body has nothing
    to join onto there, so it never carries that tag, and neither does a COLLECT body run
    one row at a time. That is why the op is row-aligned through its operands: a tag over
    the step's rows holds exactly the rows that chunk does.
  }];

  // Any chunk of the step, which the tag runs over.
  let arguments = (ins Optional<NLChunk>:$cardinality);

  // The accumulator, then the row tag over this step's rows.
  let results = (outs NLRowListsState:$state, NLUInt64Chunk:$tag);

  // The tag's type is buildable and so omitted; only the cardinality chunk's is spelled.
  let assemblyFormat = [{
    `(` ($cardinality^)? `)` attr-dict (`:` qualified(type($cardinality))^)?
  }];
}

/**
* RowListsCollect
*/
// NOT Pure: it stages this step's values in the accumulator, so it must run exactly
// where it sits (like nl.optional_collect).
def RowListsCollect : NLOp<"row_lists_collect", [AttrSizedOperandSegments]> {
  let summary = "Stage what this step of a body's rows contribute to the lists";

  let description = [{
    Runs in the innermost loop body of the body's nest, once per step of it. Stages one
    value per row the body produced, each under the row the tag names, so once the body
    is walked every row's values are in hand.

      nl.row_lists_collect %state, tag %tag, %name : !nl.chunk<!storage.nullable<!storage.string>>

    The tag is absent when every value belongs to one row: the row whose position the
    one-row `row` chunk holds - the row of the step a COLLECT body run one row at a time
    is handed - or, with no `row`, the single empty row where nothing is in flight.

      nl.row_lists_collect %state, row %r, %name : !nl.chunk<!storage.nullable<!storage.string>>
  }];

  let arguments = (ins NLRowListsStateHandle:$state,
                       Optional<NLUInt64Chunk>:$tag,
                       Optional<NLUInt64Chunk>:$row,
                       NLChunk:$value);

  let assemblyFormat = [{
    $state `,` (`tag` $tag^ `,`)? (`row` $row^ `,`)? $value attr-dict `:` qualified(type($value))
  }];

  let hasVerifier = 1;
}

/**
* RowListsResult
*/
// Pure: given a filled accumulator it deterministically builds the same lists, and
// produces no side effect of its own. Mirrors nl.collect, except that it writes a chunk
// in place rather than opening an iterator: the rows are the step's own.
def RowListsResult : NLOp<"row_lists_result", [Pure, RowAlignedThroughOperands]> {
  let summary = "Build this step's lists out of a filled row lists accumulator";

  let description = [{
    The build phase. Consumes a filled !nl.row_lists_state and produces one list cell per
    row of the step, in row order: the values the body staged for that row, and the empty
    list for a row it produced nothing for.

      %lists = nl.row_lists_result(%state, %a) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<!storage.string>>

    Its lists are complete only once every value is staged, so place it after the body's
    loop nest, in the block holding the nl.row_lists_buffer that opened the accumulator.
    The cardinality operand is a chunk of that step, which the lists are row-aligned with;
    it is absent when nothing was in flight where the lists are read, which is one row of
    its own.
  }];

  // The cardinality is the only variable-length operand, so MLIR can tell it apart from
  // the handle without operand-segment sizes (as nl.broadcast_constant's is).
  let arguments = (ins NLRowListsStateHandle:$state, Optional<NLChunk>:$cardinality);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $state (`,` $cardinality^)? `)` attr-dict `:` functional-type($cardinality, results)
  }];
}

#endif // TURINGDB_NL_SUBQUERY_OPS
