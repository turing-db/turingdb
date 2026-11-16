#ifndef TURINGDB_LIST_OPS
#define TURINGDB_LIST_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageEnums.td"

/**
* MakeList
*/
def MakeList : TuringOp<"make_list", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Build one list per row out of the row's cell in each element column";

  let description = [{
    The list `[n, m]` whose elements are read per row rather than known at plan time -
    the row-varying form db.constant's array of literals is the constant case of. It
    reads one column per element and produces one list cell per row:

      %xs = db.make_list(%n, %m) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
                                -> !db.column<!storage.list<!storage.node_id>>

    Row r of the result is the list of row r of each operand, in operand order. A cell
    is a view over the query's list buffer, as an nl.collect drain's is, so a row copies
    in O(1) however long the list. An operand holding one value for every row rather
    than one per row is laid out over the rows during lowering, so `[n, 1]` gives every
    row a two-element list.

    The result element type is the homogeneity verdict, as db.unwind_const's is: columns
    that agree give a list of that type, columns that disagree a list of type-erased
    tagged scalars, matching Cypher's LIST OF ANY. A column whose own type is resolved
    during lowering - a property fetch's - names none here, and no verdict is taken
    against a type that is not known yet: one such column makes the whole list none, and
    lowering takes the verdict over the resolved chunks:

      %xs = db.make_list(%age, %name) : (!db.column<none>, !db.column<none>)
                                     -> !db.column<!storage.list<none>>

    `UNWIND [n, m] AS x` is this op feeding db.unwind, which spreads each cell back into
    one row per element.
  }];

  let arguments = (ins Variadic<Column>:$elements);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  // The element columns print inside the parens, as db.collect's columns do; the operand
  // and result types follow as a function type, so the list is spelled beside the columns
  // it was built from.
  let assemblyFormat = [{
    `(` $elements `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* MakeMap
*/
def MakeMap : TuringOp<"make_map", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Build one map per row out of the row's cell in each value column";

  let description = [{
    The map `{name: n.name, age: m.age}` whose values are read per row rather than known
    at plan time. Key i names the value read from operand i:

      %m = db.make_map(%age, %name) keys ["age", "name"] : !db.column<none>, !db.column<none>

    Row r of the result is the map of row r of each operand. A null cell is an entry
    holding null, not a missing key. An operand holding one value for every row is laid
    out over the rows during lowering.
  }];

  let arguments = (ins Variadic<Column>:$values, StrArrayAttr:$keys);
  let results   = (outs ColumnMaps:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $values `)` `keys` $keys attr-dict `:` type($values)
  }];
}

/**
* MergeMaps
*/
def MergeMaps : TuringOp<"merge_maps", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Merge the maps of each row, a later operand's entry replacing an earlier one's";

  let description = [{
    The map projection `p {.*, name: 'x'}`: the properties of p, with the entry of a later
    item replacing the one under the same key. Row r of the result holds every key the
    operands' maps hold at row r, each under the value of the last operand holding it:

      %m = db.merge_maps(%properties, %entries)
             : (!db.column<none>, !db.column<!storage.map>) -> !db.column<none>

    A row where an operand holds no map gets no map, as a function of a null is null. An
    operand holding one map for every row is laid out over the rows during lowering. The
    result is a map column, nullable where an operand is, resolved during lowering.
  }];

  let arguments = (ins Variadic<Column>:$maps);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $maps `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* StaticMapKey
*/
def StaticMapKey : TuringOp<"static_map_key", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Row-wise read of one value of a map column, by a key known at plan time";

  let description = [{
    `m.name`, or `n.attrs.name`. The key is spelled in the query, so it rides an attribute
    rather than a column:

      %v = db.static_map_key %m key "name" : (!db.column<none>) -> !db.column<any>

    Row r of the result is the value row r's map holds under the key, a tagged cell. A row
    whose map holds no such key - and a null row - reads back as a Null-tagged cell. The
    map column is a column of tagged cells where the map was itself read out of a list or a
    map. A cell holding a node or an edge reads that entity's property under the key, as
    `n.name` does, and one holding a datetime or a duration reads the component the key
    names, as `d.year` does. A cell holding any other value, or a datetime or a duration
    with no such component, raises. A column of nodes or edges reads each entity's
    property under the key, which is how a property whose type the program decides as it
    runs - one only a write of tagged cells creates - is read.
  }];

  let arguments = (ins Column:$map, StrAttr:$key);
  let results   = (outs ColumnCells:$result);

  let assemblyFormat = "$map `key` $key attr-dict `:` functional-type(operands, results)";
}

/**
* Range
*/
def Range : TuringOp<"range", [MayRaise, ConstantThroughOperands, DeclareOpInterfaceMethods<ConditionallySpeculatable>, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Build one list of the integers a row's bounds span";

  let description = [{
    Cypher's `range(start, end)` and `range(start, end, step)`. The op reads one column
    per bound and produces one list cell per row, as db.make_list does:

      %xs = db.range(%from, %to) : (!db.column<i64>, !db.column<i64>)
                                -> !db.column<!storage.list<i64>>

    Row r's list counts from row r of `start` to row r of `end`, both included, by
    `step` - 1 where it is absent, and negative to count down. A step walking away from
    `end` gives the empty list; a step of 0 is the error the row raises, as is a row
    spanning more than 100000 integers or a type-erased bound holding no integer. A bound
    holding no value gives its row no list, which is what Cypher reads `range(null, 3)` as.

    The elements are integers whatever the bounds were, so the result element type is
    written here rather than inferred, unlike db.make_list's homogeneity verdict. A bound
    holding one value for every row rather than one per row is laid out over the rows
    during lowering, so `range(1, n.age)` is read at the rows the fetch carries.
  }];

  // The step is the only variable-length operand, so MLIR tells it apart from the bounds
  // without operand-segment sizes, as db.get_node_properties' pending mask is.
  let arguments = (ins Column:$start, Column:$end, Optional<Column>:$step);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $start `,` $end (`,` $step^)? `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* ListSlice
*/
def ListSlice : TuringOp<"list_slice", [MayRaise, ConstantThroughOperands, AttrSizedOperandSegments, DeclareOpInterfaceMethods<ConditionallySpeculatable>, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Read the run of each row's list that its bounds span";

  let description = [{
    Cypher's `xs[1..3]`, `xs[..3]`, `xs[1..]`. The op reads one list column and produces
    one list cell per row - the elements from the lower bound up to but not including the
    upper one - so the rows in flight are the ones it started with:

      %run = db.list_slice(%xs, from %a, to %b) : (!db.column<!storage.list<i64>>,
                                                   !db.column<i64>,
                                                   !db.column<i64>) -> !db.column<none>

    A bound is absent where the query left it out, which runs from the start or to the
    end. A negative bound counts from the end, and one running past the list is clamped to
    it, so no row errors on an integer bound; a row whose list or whose bound is null gets a
    null cell, which is what Cypher reads `null[1..2]` as. A list or a bound read out of a
    type-erased cell may hold another value, which raises.

    The elements are the ones the list already holds, so the result element type is the
    input's and is resolved during lowering, as db.list_comprehension's is.
  }];

  let arguments = (ins Column:$list, Optional<Column>:$from, Optional<Column>:$to);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $list (`,` `from` $from^)? (`,` `to` $to^)? `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* SortMulti
*/
def SortMulti : TuringOp<"sort_multi", [MayRaise, ConstantThroughOperands, AttrSizedOperandSegments, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Sort each row's list of maps on the values of some keys";

  let description = [{
    `coll.sortMulti(maps, fields, limit, skip)`, APOC's `apoc.coll.sortMulti`. The op reads
    one list column and produces one list cell per row:

      %sorted = db.sort_multi(%maps) fields %fields limit %limit skip %skip
                  : (!db.column<none>, !db.column<!storage.list<!storage.string>>,
                     !db.column<i64>, !db.column<i64>) -> !db.column<none>

    Row r's maps are sorted on the keys row r's fields name, the first one first: a field
    starting with `^` sorts ascending on the key after it, any other descending. Two values
    compare as ORDER BY compares them, and a missing key is a null, which APOC orders before
    every value: first ascending, last descending. Maps that tie keep their order. The sorted maps past the first `skip` are
    kept, at most `limit` of them, all where `limit` is negative.

    An operand the query left out is absent: no field keeps the list as it is, whatever it
    holds, no limit keeps every map and no skip keeps the first. A row with a null operand
    gets a null cell. Sorted on a field, a null element holds no key, as `null.a` is null,
    and an element that is no map raises, as do a field that is no string and a negative skip.
    The result element type is the input's and is resolved during lowering.
  }];

  let arguments = (ins Column:$list, Optional<Column>:$fields, Optional<Column>:$limit, Optional<Column>:$skip);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $list `)` (`fields` $fields^)? (`limit` $limit^)? (`skip` $skip^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* PercentileCont / PercentileDisc
*/
// Cypher's percentileCont(x, p) and percentileDisc(x, p) are a db.collect of the values
// followed by one of these, which reads each row's list and sorts the numbers in it.
// percentileCont interpolates between the two values around the percentile, so it is a
// float; percentileDisc answers the value at it, with the type the list holds. A list
// holding no value has no percentile, so its row is null. Beside a list holding numbers,
// a percentile that is null or outside 0.0 to 1.0 is the error the row raises, as is an
// element that is no number - which is why the op is not speculatable.
//
//   %m = db.percentile_cont(%values, %p) : (!db.column<none>, !db.column<f64>) -> !db.column<none>
class PercentileOp<string mnemonic> : TuringOp<mnemonic, [MayRaise, ConstantThroughOperands]> {
  let summary = "Read a percentile of the numbers each row's list holds";

  let arguments = (ins Column:$list, Column:$percentile);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $list `,` $percentile `)` attr-dict `:` functional-type(operands, results)
  }];
}

def PercentileCont : PercentileOp<"percentile_cont">;
def PercentileDisc : PercentileOp<"percentile_disc">;

/**
* ListComprehension
*/
def ListComprehension : TuringOp<"list_comprehension", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRegionCarrySet"]>]> {
  let summary = "Build one list per row out of the elements of that row's list, filtered and projected";

  let description = [{
    Cypher's `[x IN xs WHERE p(x) | f(x)]`. The op reads one list column and produces one
    list cell per row, so - unlike db.unwind, which spreads the elements over rows of
    their own - the rows in flight are the ones it started with:

      %ts = db.list_comprehension(%tags, {%n}) ({
        ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>, %nIn: !db.column<!storage.node_id>):
          %keep = db.neq %x, %c : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
          %xk, %tagk, %nk = db.filter(%keep, {%x, %tag, %nIn}) : ...
          %up = db.concat %xk, %suffix : (!db.column<none>, !db.column<none>) -> !db.column<none>
          db.comprehension_yield %tagk, %up : !db.column<none>
      }) : (!db.column<none>, !db.column<!storage.node_id>) -> !db.column<none>

    The region's entry block takes the element the variable is bound to, the row tag - the
    position of the row whose list that element came from - and then one argument per
    carried column: the element rows are not the rows in flight, so a column the body
    reads has to be repeated once per element of its own row, which is what carrying it
    does - the carry set of db.unwind, laid out the same way. Everything else the body
    reads stands for every row already: a constant needs no carrying.

    The WHERE is a db.filter over those arguments rather than a mask the op is handed, so
    an element it drops is one the projection is never computed for - `[x IN xs WHERE x <>
    0 | 100 / x]` divides by no zero. The yield names the tag as the body left it, which
    is what says how many elements each row's list ends up with, and the value one element
    contributes.

    The element column's type is the source's to decide and is resolved during lowering,
    as db.unwind's element is - a list of tagged scalars hands out tagged scalars, a list
    of one type hands out that type. So is the result's: the list is of whatever the body
    yields, and a row whose source cell is null gets a null list rather than an empty one,
    which is what Cypher reads `[x IN null | x]` as.
  }];

  let arguments = (ins Column:$source, Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$result);

  // Exactly one block, ending in a db.comprehension_yield.
  let regions = (region SizedRegion<1>:$body);

  let hasVerifier = 1;

  // The carry set prints in braces, as db.unwind's does; the region follows, then the
  // source and the carried types as a function type.
  let assemblyFormat = [{
    `(` $source `,` `{` $columns_to_filter `}` `)` $body attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* ListPredicate
*/
def ListPredicate : TuringOp<"list_predicate", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRegionCarrySet"]>]> {
  let summary = "Decide, per row, whether a predicate holds for the elements of that row's list";

  let description = [{
    Cypher's `all(x IN xs WHERE p(x))`, and `any`, `none` and `single` over the same
    form. The op reads one list column and produces one boolean per row: whether the
    predicate holds for as many of the row's elements as the kind asks.

      %ok = db.list_predicate(%tags, {%n}) kind all ({
        ^bb0(%x: !db.column<none>, %tag: !db.column<ui64>, %nIn: !db.column<!storage.node_id>):
          %p = db.neq %x, %c : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
          db.comprehension_yield %tag, %p : !db.column<!storage.bool>
      }) : (!db.column<none>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>

    The region is db.list_comprehension's: the entry block takes the element, the row
    tag and one argument per carried column, and the yield names the tag and the
    predicate's value for each element. The body cuts nothing: an element whose
    predicate is null counts as unknown, which is what makes the answer null where
    Cypher says so - `all` is null when no element fails and one is unknown, `any` when
    none holds and one is unknown, `none` as `any` is, `single` when the elements known
    to hold are at most one and one is unknown. An empty list holds for `all` and `none`
    and fails `any` and `single`; a row whose source cell is null gets a null.
  }];

  let arguments = (ins Column:$source, Variadic<Column>:$columns_to_filter, ListPredicateKind:$kind);
  let results   = (outs ColumnBools:$result);

  // Exactly one block, ending in a db.comprehension_yield.
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
def Reduce : TuringOp<"reduce", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRegionCarrySet"]>]> {
  let summary = "Fold the elements of each row's list into one value per row";

  let description = [{
    Cypher's `reduce(acc = init, x IN xs | f(acc, x))`. The op reads one list column and
    produces one value per row: the initial value, replaced by what the body computes
    from it and each element of the row's list in turn.

      %sum = db.reduce(%xs, %zero, {%n}) ({
        ^bb0(%x: !db.column<none>, %acc: !db.column<none>, %nIn: !db.column<!storage.node_id>):
          %next = db.add %acc, %x : (!db.column<none>, !db.column<none>) -> !db.column<none>
          db.reduce_yield %next : !db.column<none>
      }) : (!db.column<none>, !db.column<none>, !db.column<!storage.node_id>) -> !db.column<none>

    The region's entry block takes the element, the accumulator as the previous element
    left it, and one argument per carried column - db.list_comprehension's carry set. The
    body cuts nothing, so the yield names only the next accumulator.

    The accumulator's type is resolved during lowering: it is the type the initial value
    and the body's value share, so `reduce(s = 0, x IN [1.5] | s + x)` accumulates a
    float. An empty list gives the initial value, and a row whose source cell is null
    gives a null.
  }];

  let arguments = (ins Column:$source, Column:$initial_value, Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$result);

  // Exactly one block, ending in a db.reduce_yield.
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
def ReduceYield : TuringOp<"reduce_yield", [Pure, Terminator, ParentOneOf<["Reduce", "AllReduce"]>]> {
  let summary = "Name the accumulator a db.reduce's or a db.all_reduce's body computed for each element";

  let arguments = (ins Column:$value);

  let assemblyFormat = [{
    $value attr-dict `:` qualified(type($value))
  }];
}

/**
* AllReduce
*/
def AllReduce : TuringOp<"all_reduce", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRegionCarrySet"]>]> {
  let summary = "Decide, per row, whether a predicate holds after every step of a fold over that row's list";

  let description = [{
    Cypher's `allReduce(acc = init, x IN xs | f(acc, x), p(acc, x))`. The op folds each
    row's list as db.reduce does, and produces one boolean per row: whether the predicate
    held on the accumulator after every step.

      %ok = db.all_reduce(%xs, %zero, {%n}) ({
        ^bb0(%x: !db.column<none>, %acc: !db.column<none>, %nIn: !db.column<!storage.node_id>):
          %next = db.add %acc, %x : (!db.column<none>, !db.column<none>) -> !db.column<none>
          db.reduce_yield %next : !db.column<none>
      }) predicate ({
        ^bb0(%x: !db.column<none>, %acc: !db.column<none>, %nIn: !db.column<!storage.node_id>):
          %holds = db.lt %acc, %ten : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
          db.all_reduce_yield %holds : !db.column<!storage.bool>
      }) : (!db.column<none>, !db.column<none>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>

    The body is db.reduce's. The predicate region takes the same arguments, its
    accumulator the one the body just computed, as the fold holds it, and yields whether
    the predicate holds on it. The initial value is never tested. A row stops folding at
    the first step whose predicate is false, and gets false. A row with no false step gets
    null when one was null, and true otherwise, so an empty list gets true. A row whose
    source cell is null gets null.
  }];

  let arguments = (ins Column:$source, Column:$initial_value, Variadic<Column>:$columns_to_filter);
  let results   = (outs ColumnBools:$result);

  // Exactly one block each, the body ending in a db.reduce_yield and the predicate in a
  // db.all_reduce_yield.
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
def AllReduceYield : TuringOp<"all_reduce_yield", [Pure, Terminator, HasParent<"AllReduce">]> {
  let summary = "Name whether a db.all_reduce's predicate holds on the accumulator each element left";

  let arguments = (ins Column:$holds);

  let assemblyFormat = [{
    $holds attr-dict `:` qualified(type($holds))
  }];
}

/**
* ComprehensionYield
*/
def ComprehensionYield : TuringOp<"comprehension_yield",
                                  [Pure, Terminator,
                                   ParentOneOf<["ListComprehension", "ListPredicate", "PatternComprehension"]>]> {
  let summary = "Name the row each surviving element of a comprehension came from and what it contributes";

  let description = [{
    Terminates the body of a db.list_comprehension or a db.list_predicate, or the pattern
    of a db.pattern_comprehension. `row_tags` is the row tag as the body left it - the row
    each surviving element came from, which a WHERE cuts alongside the element - and
    `value` is what that element contributes to its row's list, or its row's predicate. The tag is absent only for a
    db.pattern_comprehension that takes no input column: the one list it builds covers the
    single empty row, which every match belongs to.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$row_tags, Column:$value);

  // The tag's type is buildable and so left out, as db.optional_yield leaves it out; only
  // the value's is spelled.
  let assemblyFormat = [{
    ($row_tags^ `,`)? $value attr-dict `:` qualified(type($value))
  }];
}

def ListIndex : BinaryOp<"list_index", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Row-wise read of one element of a list column, by position";

  let description = [{
    `[1, 3, 4][2]`

      %e = db.list_index %xs, %i : (!db.column<none>, !db.column<i64>) -> !db.column<none>

    A position past either end of the list reads null. A list read out of a type-erased
    cell may hold another value, which raises; a cell holding null reads null.
  }];

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def DynamicMapKey : BinaryOp<"dynamic_map_key", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Row-wise read of one value of a map column, by a key computed per row";

  let description = [{
    `m[k]`. The key is an expression rather than a name spelled in the query, so it rides a
    column rather than the attribute db.static_map_key carries:

      %v = db.dynamic_map_key %m, %k : (!db.column<none>, !db.column<none>) -> !db.column<any>

    Row r of the result is the value row r's map holds under row r's key, a tagged cell. A
    row whose map holds no such key - and a null row - reads back as a Null-tagged cell, as
    db.static_map_key's does. The map column is a column of tagged cells where the map was
    itself read out of a list or a map, and a cell holding a node or an edge reads that
    entity's property under the key. A cell holding any other value raises: a datetime and
    a duration have components, which only `d.year` reads.
  }];

  let arguments = (ins Column:$map, Column:$key);
  let results   = (outs ColumnCells:$result);

  let assemblyFormat = "$map`,` $key attr-dict `:` functional-type(operands, results)";
}

def ContainerIndex : BinaryOp<"container_index", [MayRaise, DeclareOpInterfaceMethods<ConditionallySpeculatable>]> {
  let summary = "Row-wise read of an element of a list or a value of a map, by an index whose kind each row decides";

  let description = [{
    `c[i]` where `i` is a type-erased cell, so only the row says whether it is a position or
    a key. Neo4j's ContainerIndex expression dispatches the same way at run time.

      %e = db.container_index %c, %i : (!db.column<none>, !db.column<!storage.any>) -> !db.column<none>

    Row r reads what db.list_index reads where row r's base holds a list and its index an
    integer, and what db.dynamic_map_key reads where the base holds a map, a node or an edge
    and the index a string. A null base or a null index reads null. Any other pair raises,
    so the op is speculatable only where every row holds one of those two pairs, nulls aside.

    A list hands its elements out as db.list_index does, and any other base a tagged cell.
    Lowering resolves the result type.
  }];

  let arguments = (ins Column:$base, Column:$index);
  let results   = (outs Column:$result);

  let assemblyFormat = "$base `,` $index attr-dict `:` functional-type(operands, results)";
}

#endif // TURINGDB_LIST_OPS
