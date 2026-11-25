#ifndef TURINGDB_NL_LIST_OPS
#define TURINGDB_NL_LIST_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageEnums.td"

/**
* MakeList
*/
def MakeList : NLOp<"make_list", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise build of one list per row out of every element chunk";
  let description = [{
    Imperative counterpart of db.make_list. Row r of the result is the list of row r of
    each operand, in operand order, written into the query's list buffer as one
    contiguous run:

      %xs = nl.make_list(%n, %m)
              : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
              -> !nl.chunk<!storage.list<!storage.node_id>>

    Every operand carries rows - lowering lays a constant one out over them first - so
    the cells are read at the same row index and the chunk is as long as they are. The
    element type is the list of the type the operands share, or of type-erased tagged
    scalars where they disagree, and is spelled out rather than inferred, as nl.unwind's
    is.
  }];

  let arguments = (ins Variadic<NLChunk>:$elements);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "`(` $elements `)` attr-dict `:` functional-type(operands, results)";
}

/**
* MakeMap
*/
def MakeMap : NLOp<"make_map", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise build of one map per row out of every value chunk";
  let description = [{
    Imperative counterpart of db.make_map. Row r of the result is the map whose key i
    holds row r of operand i, written into the query's map buffer as one contiguous run:

      %m = nl.make_map(%age, %name) keys ["age", "name"]
             : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>

    Every operand carries rows - lowering lays a constant one out over them first.
  }];

  let arguments = (ins Variadic<NLChunk>:$values, StrArrayAttr:$keys);
  let results   = (outs NLMapChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "`(` $values `)` `keys` $keys attr-dict `:` type($values)";
}

/**
* MergeMaps
*/
def MergeMaps : NLOp<"merge_maps", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise merge of the maps of every operand, later keys replacing earlier ones";
  let description = [{
    Imperative counterpart of db.merge_maps. Row r of the result holds every key the
    operands' maps hold at row r, under the value of the last operand holding it, written
    into the step's map buffer as one contiguous run sorted on key:

      %m = nl.merge_maps(%properties, %entries)
             : (!nl.chunk<!storage.nullable<!storage.map>>, !nl.chunk<!storage.map>)
             -> !nl.chunk<!storage.nullable<!storage.map>>

    An operand is a chunk of maps, nullable or not, or of tagged cells. A row where one
    holds no map gets none, so the result is nullable unless every operand is a plain
    chunk of maps. Every operand carries rows - lowering lays a constant one out over them
    first.
  }];

  let arguments = (ins Variadic<NLChunk>:$maps);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let extraClassDeclaration = [{
    static ::mlir::Type resultChunkType(::mlir::MLIRContext* context, ::mlir::TypeRange mapTypes);
  }];

  let assemblyFormat = "`(` $maps `)` attr-dict `:` functional-type(operands, results)";
}

/**
* StaticMapKey
*/
def StaticMapKey : NLOp<"static_map_key", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise read of one value of a map chunk, by a key fixed at plan time";
  let description = [{
    Imperative counterpart of db.static_map_key. Row r of the result views the value row r's
    map already holds, a tagged cell, so the read copies nothing:

      %v = nl.static_map_key %m key "name"
             : (!nl.chunk<!storage.nullable<!storage.map>>) -> !nl.chunk<!storage.any>

    The operand is nullable where a stored property fetched it and plain where a literal
    built it, so both spellings are accepted. It is a chunk of tagged cells where the map
    was itself read out of a list or a map. A cell holding a node or an edge reads that
    entity's property, and one holding a datetime or a duration the component the key
    names, written into the step's map buffer as an entry of its own. A cell holding any
    other value raises, and so does a datetime or a duration with no such component. The
    result is never nullable: a cell carries its null in its own tag, so a missing key and
    a null row both read back as a Null-tagged cell rather than as an absent optional.
  }];

  let arguments = (ins NLChunk:$map, StrAttr:$key);
  let results   = (outs NLCellChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "$map `key` $key attr-dict `:` functional-type(operands, results)";
}

/**
* Range
*/
def Range : NLOp<"range", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise build of the list of integers each row's bounds span";

  let description = [{
    Imperative counterpart of db.range. Row r of the result is the list counting from row
    r of `start` to row r of `end`, both included, by row r of `step`, written into the
    query's list buffer as one contiguous run:

      %xs = nl.range %from, %to
              : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>)
              -> !nl.chunk<!storage.nullable<!storage.list<i64>>>

    Every bound carries rows - lowering lays a constant one out over them first - and is
    read as a nullable value chunk, or as a chunk of type-erased cells whose integer each
    row reads out of its cell, raising on any other value. A row whose bound is absent gets
    no list. The step is absent where the query gave none, which counts by 1; a step of 0
    is the error the row raises, as is a row spanning more than 100000 integers.
  }];

  // The step is the only variable-length operand, so MLIR tells it apart from the bounds
  // without operand-segment sizes, as nl.broadcast_constant's cardinality is.
  let arguments = (ins NLChunk:$start, NLChunk:$end, Optional<NLChunk>:$step);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "$start `,` $end (`,` $step^)? attr-dict `:` functional-type(operands, results)";
}

/**
* ListSlice
*/
def ListSlice : NLOp<"list_slice", [Pure, RowAlignedThroughOperands, AttrSizedOperandSegments]> {
  let summary = "Row-wise read of the run of a list its bounds span";

  let description = [{
    Imperative counterpart of db.list_slice. Row r of the result views the elements of row
    r's list from the lower bound up to but not including the upper one, which needs no
    copy: a list is a run of elements, and a slice of it is a shorter run of the same ones.

      %run = nl.list_slice %xs, from %a, to %b
               : (!nl.chunk<!storage.nullable<!storage.list<i64>>>,
                  !nl.chunk<!storage.nullable<i64>>,
                  !nl.chunk<!storage.nullable<i64>>)
               -> !nl.chunk<!storage.nullable<!storage.list<i64>>>

    Each bound carries rows - lowering lays a constant one out over them first - and is
    read as a nullable value chunk, or as a chunk of type-erased cells whose integer each
    row reads out of its cell, raising on any other value. A bound the query left out is no
    operand at all, which runs from the start or to the end.
  }];

  let arguments = (ins NLChunk:$list, Optional<NLChunk>:$from, Optional<NLChunk>:$to);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    $list (`,` `from` $from^)? (`,` `to` $to^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* SortMulti
*/
def SortMulti : NLOp<"sort_multi", [Pure, RowAlignedThroughOperands, AttrSizedOperandSegments]> {
  let summary = "Row-wise sort of a list of maps on the values of some keys";

  let description = [{
    Imperative counterpart of db.sort_multi. Row r of the result holds the maps of row r's
    list in the order row r's fields sort them, cut by its skip and its limit. A sorted list
    lives in the step's own buffer, which the next run of the step clears; with no field the
    row is a slice of its input list, which copies nothing.

      %sorted = nl.sort_multi %maps fields %fields limit %limit skip %skip
                  : (!nl.chunk<!storage.nullable<!storage.list<!storage.any>>>,
                     !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>,
                     !nl.chunk<!storage.nullable<i64>>,
                     !nl.chunk<!storage.nullable<i64>>)
                  -> !nl.chunk<!storage.nullable<!storage.list<!storage.any>>>

    Every operand carries rows - lowering lays a constant one out over them first. The
    limit and the skip are read as nullable value chunks, or as chunks of type-erased
    cells whose integer each row reads out of its cell.
  }];

  let arguments = (ins NLChunk:$list, Optional<NLChunk>:$fields, Optional<NLChunk>:$limit, Optional<NLChunk>:$skip);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    $list (`fields` $fields^)? (`limit` $limit^)? (`skip` $skip^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* PercentileCont / PercentileDisc
*/
// Imperative counterparts of db.percentile_cont and db.percentile_disc. Row r of the
// result is the percentile row r's percentile names of the numbers row r's list holds,
// null where the list holds none. The percentile carries rows - lowering lays a constant
// one out over them first - and is read as a nullable value chunk.
//
//   %m = nl.percentile_cont %values, %p
//          : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.nullable<f64>>)
//          -> !nl.chunk<!storage.nullable<f64>>
class NLPercentileOp<string mnemonic> : NLOp<mnemonic, [NoMemoryEffect, RowAlignedThroughOperands]> {
  let summary = "Row-wise read of a percentile of the numbers a list holds";

  let arguments = (ins NLChunk:$list, NLChunk:$percentile);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$list `,` $percentile attr-dict `:` functional-type(operands, results)";
}

def PercentileCont : NLPercentileOp<"percentile_cont">;
def PercentileDisc : NLPercentileOp<"percentile_disc">;

/**
* ListComprehension
*/
def ListComprehension : NLOp<"list_comprehension", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise build of one list per row out of the elements of that row's list";

  let description = [{
    Imperative counterpart of db.list_comprehension. Row r of the result is the list of
    what the body yields for the elements of row r that its predicate keeps:

      %ts = nl.list_comprehension(%tags, {%n}) ({
        ^bb0(%x: !nl.chunk<!storage.nullable<!storage.string>>,
             %tag: !nl.chunk<ui64>,
             %nIn: !nl.chunk<!storage.node_id>):
          %keep = nl.neq %x, %c : ...
          %xk, %tagk, %nk = nl.filter %keep, (%x, %tag, %nIn) : ...
          nl.comprehension_yield %tagk, %xk : !nl.chunk<!storage.nullable<!storage.string>>
      }) : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>,
            !nl.chunk<!storage.node_id>)
        -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>

    A chunk of the elements does not measure the step the source chunk does, so the body
    runs over chunks of its own: the op walks every (row, element) pair of the source
    chunk in row order, fills the element chunk, the row tag holding the row each element
    came from and each carried chunk gathered by that row - the cursor of nl.unwind - and
    runs the body once per chunkful. The body hands back the tag as it left it, so the
    elements a WHERE cut are gone from it, and each value goes to the list of the row its
    tag names. The elements of one row arrive in order and in one run, so a row's list is
    complete as soon as the run past it starts.

    The result is nullable: a row whose source cell holds no list gets no list, which is
    how `[x IN null | x]` reads, and a row whose elements were all cut gets the empty list.
  }];

  let arguments = (ins NLChunk:$source, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLChunk:$result);

  // Exactly one block, ending in an nl.comprehension_yield.
  let regions = (region SizedRegion<1>:$body);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $source `,` `{` $columns_to_filter `}` `)` $body attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* ListPredicate
*/
def ListPredicate : NLOp<"list_predicate", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise decision of whether a predicate holds for the elements of that row's list";

  let description = [{
    Imperative counterpart of db.list_predicate. Row r of the result is whether the
    predicate the body yields holds for as many elements of row r as the kind asks:

      %ok = nl.list_predicate(%tags, {%n}) kind all ({
        ^bb0(%x: !nl.chunk<!storage.nullable<!storage.string>>,
             %tag: !nl.chunk<ui64>,
             %nIn: !nl.chunk<!storage.node_id>):
          %p = nl.neq %x, %c : ...
          nl.comprehension_yield %tag, %p : !nl.chunk<!storage.nullable<!storage.bool>>
      }) : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>,
            !nl.chunk<!storage.node_id>)
        -> !nl.chunk<!storage.nullable<!storage.bool>>

    The body runs over chunks of the (row, element) pairs exactly as nl.list_comprehension's
    does, and hands back the row tag and the predicate's value for each element. The
    result is nullable: a row whose source cell holds no list gets a null, and so does a
    row whose elements leave the answer unknown - a null predicate counts as neither
    holding nor failing.
  }];

  let arguments = (ins NLChunk:$source, Variadic<NLChunk>:$columns_to_filter, ListPredicateKind:$kind);
  let results   = (outs NLChunk:$result);

  // Exactly one block, ending in an nl.comprehension_yield.
  let regions = (region SizedRegion<1>:$body);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $source `,` `{` $columns_to_filter `}` `)` `kind` $kind $body attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* Reduce
*/
def Reduce : NLOp<"reduce", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise fold of the elements of each row's list into one value per row";

  let description = [{
    Imperative counterpart of db.reduce. Row r of the result is the initial value of row
    r, replaced by what the body yields for it and each element of row r's list in turn:

      %sum = nl.reduce(%xs, %zero, {%n}) ({
        ^bb0(%x: !nl.chunk<!storage.nullable<i64>>,
             %acc: !nl.chunk<!storage.nullable<i64>>,
             %nIn: !nl.chunk<!storage.node_id>):
          %next = nl.add %acc, %x : ...
          nl.reduce_yield %next : !nl.chunk<!storage.nullable<i64>>
      }) : (!nl.chunk<!storage.nullable<!storage.list<i64>>>,
            !nl.chunk<i64>,
            !nl.chunk<!storage.node_id>)
        -> !nl.chunk<!storage.nullable<i64>>

    Each element depends on the accumulator the previous one left, so the body runs once
    per element position rather than once per chunk of elements: run k holds element k of
    every row whose list is longer than k, beside that row's accumulator and carried
    chunks. The accumulator chunk has the result's type; the initial value and the body's
    value are written into it, so either may be the narrower type it holds - a plain
    value where it is nullable, a list of any element type where it holds lists of tagged
    cells, an entity, a path, a list, a map, a cell or a nullable value where it holds
    tagged cells.
  }];

  let arguments = (ins NLChunk:$source, NLChunk:$initial_value, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLChunk:$result);

  // Exactly one block, ending in an nl.reduce_yield.
  let regions = (region SizedRegion<1>:$body);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $source `,` $initial_value `,` `{` $columns_to_filter `}` `)` $body attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* ReduceYield
*/
def ReduceYield : NLOp<"reduce_yield", [Pure, Terminator, ParentOneOf<["Reduce", "AllReduce"]>]> {
  let summary = "Name the accumulator an nl.reduce's or an nl.all_reduce's body computed for each element";

  let arguments = (ins NLChunk:$value);

  let assemblyFormat = [{
    $value attr-dict `:` qualified(type($value))
  }];
}

/**
* AllReduce
*/
def AllReduce : NLOp<"all_reduce", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise decision of whether a predicate holds after every step of a fold over that row's list";

  let description = [{
    Imperative counterpart of db.all_reduce. Row r of the result is whether the predicate
    the body yields held after every step of the fold of row r's list:

      %ok = nl.all_reduce(%xs, %zero, {%n}) ({
        ^bb0(%x: !nl.chunk<!storage.nullable<i64>>,
             %acc: !nl.chunk<!storage.nullable<i64>>,
             %nIn: !nl.chunk<!storage.node_id>):
          %next = nl.add %acc, %x : ...
          nl.reduce_yield %next : !nl.chunk<!storage.nullable<i64>>
      }) predicate ({
        ^bb0(%x: !nl.chunk<!storage.nullable<i64>>,
             %acc: !nl.chunk<!storage.nullable<i64>>,
             %nIn: !nl.chunk<!storage.node_id>):
          %holds = nl.lt %acc, %ten : ...
          nl.all_reduce_yield %holds : !nl.chunk<!storage.nullable<i1>>
      }) : (!nl.chunk<!storage.nullable<!storage.list<i64>>>,
            !nl.chunk<i64>,
            !nl.chunk<!storage.node_id>)
        -> !nl.chunk<!storage.nullable<i1>>

    The body runs as nl.reduce's does, once per element position, and its value is
    written into the accumulator, of the type of the body's accumulator argument. The
    predicate region then runs over the same rows, its accumulator argument read back
    from the accumulator just written. A row leaves the fold at the first step whose
    predicate is false, so the body never runs on its later elements. The result is
    nullable: a row whose source cell holds no list gets a null, and so does a row with
    no false step and a null one.
  }];

  let arguments = (ins NLChunk:$source, NLChunk:$initial_value, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLChunk:$result);

  // Exactly one block each, the body ending in an nl.reduce_yield and the predicate in an
  // nl.all_reduce_yield.
  let regions = (region SizedRegion<1>:$body, SizedRegion<1>:$predicate);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $source `,` $initial_value `,` `{` $columns_to_filter `}` `)` $body `predicate` $predicate attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* AllReduceYield
*/
def AllReduceYield : NLOp<"all_reduce_yield", [Pure, Terminator, HasParent<"AllReduce">]> {
  let summary = "Name whether an nl.all_reduce's predicate holds on the accumulator each element left";

  let arguments = (ins NLChunk:$holds);

  let assemblyFormat = [{
    $holds attr-dict `:` qualified(type($holds))
  }];
}

/**
* ComprehensionYield
*/
def ComprehensionYield : NLOp<"comprehension_yield",
                              [Pure, Terminator, ParentOneOf<["ListComprehension", "ListPredicate"]>]> {
  let summary = "Name the row each element of an nl.list_comprehension or nl.list_predicate came from and what it contributes";

  let arguments = (ins NLUInt64Chunk:$row_tags, NLChunk:$value);

  // The tag's type is buildable and so left out; only the value's is spelled.
  let assemblyFormat = [{
    $row_tags `,` $value attr-dict `:` qualified(type($value))
  }];
}

def ListIndex : NLOp<"list_index", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise read of one element of a list chunk, by position";
  let description = [{
      %e = nl.list_index %xs, %i : (!nl.chunk<!storage.list<!storage.any>>, !nl.chunk<i64>) -> !nl.chunk<!storage.any>

      A list of nodes or of edges hands its elements back as the entity chunk they were
      gathered from, an invalid ID standing for a position the list does not hold:

      %n = nl.list_index %ns, %i : (!nl.chunk<!storage.list<!storage.node_id>>, !nl.chunk<i64>) -> !nl.chunk<!storage.node_id>

      A chunk of tagged cells is indexed too - a list read out of a list or out of a map. A
      cell holding null reads back as the absent element, and one holding another value
      raises.
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def DynamicMapKey : NLOp<"dynamic_map_key", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise read of one value of a map chunk, by a key read per row";
  let description = [{
    Imperative counterpart of db.dynamic_map_key. Row r of the result views the value row
    r's map already holds under row r's key, a tagged cell, so the read copies nothing:

      %v = nl.dynamic_map_key %m, %k
             : (!nl.chunk<!storage.nullable<!storage.map>>, !nl.chunk<!storage.string>)
               -> !nl.chunk<!storage.any>

    Either operand is nullable where a stored property fetched it and plain where a literal
    built it, so both spellings are accepted on both. The map operand is a chunk of tagged
    cells where the map was itself read out of a list or a map, and a cell holding a node or
    an edge reads that entity's property under the key. A cell holding any other value
    raises. The result is never nullable: a cell carries its null in its own tag, so a map
    holding no such key, a null row and a row holding no key all read back as a
    Null-tagged cell.
  }];

  let arguments = (ins NLChunk:$map, NLChunk:$key);
  let results   = (outs NLCellChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "$map `,` $key attr-dict `:` functional-type(operands, results)";
}

def ContainerIndex : NLOp<"container_index", [Pure, RowAlignedThroughOperands]> {
  let summary = "Row-wise read of an element of a list or a value of a map, by an index read per row";
  let description = [{
    Imperative counterpart of db.container_index. Row r reads the element row r's list holds
    at row r's position, as nl.list_index does, or the value row r's map, node or edge holds
    under row r's key, as nl.dynamic_map_key does:

      %e = nl.container_index %xs, %i
             : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.any>) -> !nl.chunk<!storage.nullable<i64>>

      %v = nl.container_index %c, %i
             : (!nl.chunk<!storage.any>, !nl.chunk<!storage.any>) -> !nl.chunk<!storage.any>

    The base is a chunk of lists, of maps or of tagged cells, and the index a chunk of tagged
    cells, each optionally nullable. A null base or a null index reads null. A position on a
    value that is no list, a key on a value that is no map, node or edge, and an index that is
    neither a position nor a key raise.
  }];

  let arguments = (ins NLChunk:$base, NLChunk:$index);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "$base `,` $index attr-dict `:` functional-type(operands, results)";
}

#endif // TURINGDB_NL_LIST_OPS
