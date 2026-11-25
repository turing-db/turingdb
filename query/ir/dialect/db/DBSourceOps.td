#ifndef TURINGDB_SOURCE_OPS
#define TURINGDB_SOURCE_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"

/**
* UnwindConst
*/
def UnwindConst : TuringOp<"unwind_const", [Pure]> {
  let summary = "Produce one row per element of a compile-time-known literal list";

  let description = [{
    The source op for `UNWIND [<literals>] AS x` where the list is known at plan
    time, independent of any input row. It opens a dataflow, one row per element, the
    way db.const_scan_nodes opens one from a fixed set of node IDs:

      %x = db.unwind_const([1, 2, 3]) : !db.column<i64>

    `elements` carries the literals as an array of attributes, so each element keeps
    its own type: a typed attribute for a scalar, a unit attribute for a null, a
    nested array attribute for a nested list. When they share one type the list is
    homogeneous and the result is that typed column. A null takes the type of the
    other elements, and a nested list is typed by its own elements, so [1, null, 2]
    gives i64 and [[1, 2], [3]] gives !storage.list<i64>. When the types differ, or
    the list is empty or holds only nulls, it is heterogeneous and the result is a
    type-erased !storage.any column, a tagged scalar per row, matching Cypher's
    LIST OF ANY:

      %row = db.unwind_const([true, "s", 10]) : !db.column<!storage.any>

    So the result element type is the homogeneity verdict: a downstream op sees a
    concrete typed column for a homogeneous list, and a !storage.any column - which
    only pass-through ops (a cross_product broadcast, the output) accept - for a
    heterogeneous one. An empty list yields no row, matching UNWIND of an empty list.
    Producing a fixed list is deterministic, so the op is Pure. With incoming rows
    (MATCH (a) UNWIND [...] AS x) it is crossed with the input through
    db.cross_product; there is no expanding variant of this op.
  }];

  // `elements` is the literal list, each entry a typed builtin attribute carrying its
  // own type. Being an attribute and not an SSA value, it is absent from `operands`.
  let arguments = (ins ArrayAttr:$elements);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  // The literals print inside the parens as `[1, 2, 3]`, ahead of the colon, the same
  // place db.const_scan_nodes prints its IDs; the result column type follows,
  // qualified, as db.const_scan_nodes prints it.
  let assemblyFormat = "`(` $elements `)` attr-dict `:` qualified(type($result))";
}

/**
* LoadCSV
*/
def LoadCSV : TuringOp<"load_csv", [Pure]> {
  let summary = "Produce one row per record of a CSV file, a column per field the query reads";

  let description = [{
    The source op for `LOAD CSV 'people.csv' AS row`. The file is fixed for the whole run,
    so it reads no row and opens a dataflow of its own, the way db.unwind_const opens one
    from a literal list - one row per record of the file:

      %name, %age = db.load_csv("people.csv", [0 : ui64, 1 : ui64])
        : !db.column<!storage.string>, !db.column<!storage.string>

    The path is either `path`, the one the query spells, or `bound_path`, a string column
    holding one value for the whole run, which the load reads when it starts. A parameter
    is one, so `LOAD CSV $path AS row` loads

      %name = db.load_csv(%path : !db.column<!storage.string>, [0 : ui64])
        : !db.column<!storage.string>

    Exactly one of the two is given, and the program does not depend on the path bound.

    A record is many values, and an op has a fixed number of results, so the row is not
    one column: `fields` names the fields the query reads and the op produces one column
    per name, in that order. A positional entry is the index `row[2]` names; a string
    entry is the header `row.age` names, which only a `with_headers` load can resolve.
    Every access to the same field reads the same column, so a field appears once
    however many times the query names it, and a field the query never names is not
    produced at all - the 768 columns of an embedding file cost nothing to skip. At least
    one field is named even so: a query reading none still runs over the records, and a
    row set needs a column to ride, so the first field stands for them.

      %age = db.load_csv("people.csv", ["age"]) with_headers
        : !db.column<!storage.string>

    Every field is a string: a CSV file carries no types, so a record's fields are the
    characters as written and a query converts them itself (`toInteger(row[1])`). The
    characters are copied into the query's string buffer, so a row's view outlives the
    chunk that parsed it.

    `with_headers` consumes the first record as the header line - it names the fields
    rather than holding a row - and is what lets a string entry of `fields` resolve.
    `skip_on_error` is `ON ERROR SKIP`: a malformed record (an unterminated quote, a
    field count differing from the file's) is dropped with a warning instead of failing
    the query.

    Reading a file is deterministic, so the op is Pure. With incoming rows
    (`LOAD CSV ... MATCH (a)`) it is crossed with them through db.cross_product, the way
    a search reading no row is; there is no expanding variant of this op.
  }];

  // The file to read and the fields to produce, each entry a ui64 index or a header
  // name. The path is resolved against the data directory, and a header name against
  // the file's header line, when the load runs.
  let arguments = (ins OptionalAttr<StrAttr>:$path,
                       Optional<Column>:$bound_path,
                       ArrayAttr:$fields,
                       UnitAttr:$with_headers,
                       UnitAttr:$skip_on_error);
  let results   = (outs Variadic<ColumnStrings>:$results);
  let hasVerifier = 1;

  // The path and the field list print inside the parens, ahead of the colon, the same
  // place db.unwind_const prints its literals; the flags print as their keywords after
  // it, as db.count's do. The result types follow, qualified, as db.scan_nodes prints
  // its one.
  let assemblyFormat = [{
    `(` custom<BoundString>($path, $bound_path, type($bound_path)) `,` $fields `)`
    (`with_headers` $with_headers^)? (`skip_on_error` $skip_on_error^)?
    attr-dict `:` qualified(type($results))
  }];
}

/**
* VectorSearch
*/
def VectorSearch : TuringOp<"vector_search", [Pure, AttrSizedOperandSegments]> {
  let summary = "Produce the nearest neighbours of a query vector, with the distance each scored";

  let description = [{
    The source op for `VECTOR SEARCH IN vec1 FOR 3 (1.0, 0.0) YIELD ids, score`. The
    index, the neighbour count and the query vector each hold one value for every row the
    search is paired with, so it reads no row and opens a dataflow of its own, the way
    db.unwind_const opens one from a literal list:

      %ids, %scores = db.vector_search("vec1", 3, [1.000000e+00, 0.000000e+00])
        : !db.column<!storage.node_id>, !db.column<f64>

    Each of the three is either the value the query spells, `index_name`, `k` or
    `query_vector`, or a column holding it, `bound_index_name`, `bound_k` or
    `bound_query_vector`, which the search reads when it starts. A parameter is one, so
    `VECTOR SEARCH IN $idx FOR $k $v` searches

      %ids, %scores = db.vector_search(%idx : !db.column<!storage.string>, %k : !db.column<i64>,
                                       %v : !db.column<!storage.embedding>)
        : !db.column<!storage.node_id>, !db.column<f64>

    Exactly one of each pair is given. A bound column is computed from parameters and
    literals, by ops that may raise, or holds one value for every row, as the db.row_constant
    reading an argument of a body run once per row does. A search reading a value of each
    row runs in such a body: `MATCH (n) VECTOR SEARCH IN vec1 FOR 3 n.e` hands n.e to a
    db.call_subquery and searches once per row it is handed. A bound index name that is no
    string naming an index, a bound count that is no integer of at least 1, and a bound
    query vector that is no embedding fail the search when it runs.

    The two results are row-aligned, one row per neighbour, ordered nearest first:
    `ids` is the node the index holds each vector under - the ID a `LOAD VECTOR` row
    carried, read as a node ID - and `scores` the distance the index' metric scored it
    at (squared L2 for euclidean, inner product for cosine). The search returns at most
    `k` neighbours and fewer when the index holds fewer, so a `k` larger than the index
    is not an error.

    Reporting a node is what lets the rest of the query treat the neighbours as matched
    rows: a pattern walks out of them (`YIELD ids MATCH (ids)-[e]->(m)`, which expands
    this column rather than scanning) and an equality compares a matched node to them
    (`WHERE n = ids`). An ID naming no node is kept rather than dropped - it simply
    matches no edge and holds no property - so an index whose IDs are not node IDs still
    reports every neighbour it found.

    The query vector must have as many elements as the index' dimension, which the schema
    holds rather than the IR, so the search itself reports a mismatch. Reading the index
    is a deterministic read, so the op is Pure. With incoming rows
    (VECTOR SEARCH ... YIELD ids MATCH (n)) it is crossed with them through
    db.cross_product.
  }];

  // The index to search, how many neighbours to report, and the vector to search for.
  let arguments = (ins OptionalAttr<StrAttr>:$index_name,
                       Optional<Column>:$bound_index_name,
                       OptionalAttr<UI64Attr>:$k,
                       Optional<Column>:$bound_k,
                       OptionalAttr<DenseF32ArrayAttr>:$query_vector,
                       Optional<Column>:$bound_query_vector);
  let results   = (outs ColumnNodeIDs:$ids, ColumnDoubles:$scores);
  let hasVerifier = 1;

  // The parameters print inside the parens ahead of the colon, as db.const_scan_nodes
  // prints its IDs; the two result types follow, qualified, as db.scan_edges prints its.
  let assemblyFormat = [{
    `(` custom<BoundString>($index_name, $bound_index_name, type($bound_index_name)) `,`
    custom<BoundCount>($k, $bound_k, type($bound_k)) `,`
    custom<QueryVector>($query_vector, $bound_query_vector, type($bound_query_vector)) `)`
    attr-dict `:` qualified(type($ids)) `,` qualified(type($scores))
  }];
}

/**
* Unwind
*/
def Unwind : TuringOp<"unwind", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Produce one row per element of a value computed for each input row";

  let description = [{
    `UNWIND <expr> AS x` where the expression is evaluated per row rather than known
    at plan time - the general form db.unwind_const is the literal special case of. It
    reads one source column and expands the rows in flight through it:

      %x, %n = db.unwind(%names, {%node}) : (!db.column<none>, !db.column<!storage.node_id>)
                                        -> (!db.column<none>, !db.column<!storage.node_id>)

    Each source cell contributes the rows Cypher unwinds it into: a list one row per
    element, a null none at all, and any other value the single row that value is - the
    singleton an UNWIND of a scalar spreads to. A tagged scalar is read the same way, so
    a cell that is itself a list unwinds one level deeper. `columns_to_filter` is the
    carry set, as on db.get_out_edges: each carried column comes back replicated, its
    cell repeated once per row its source cell produced, so everything in flight stays
    row-aligned with the elements. A constant source stands for every carried row, so it
    expands each of them.

    The element column's type is the source's to decide (`unwoundElementType`):
    unwinding a list of IDs or paths yields a column of them, a list of values a
    nullable column of their type, and a list whose elements share no type a
    type-erased !storage.any column of tagged scalars, matching Cypher's LIST OF ANY,
    as does a list of nulls alone.
    A source that is already a scalar column keeps its own type - the singleton is the
    value it already holds.
  }];

  let arguments = (ins Column:$source, Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$element, Variadic<Column>:$carried);
  let hasVerifier = 1;

  // The carry set prints in braces, as db.get_out_edges' does; the source and the
  // result types follow as a function type, so the element column's type is spelled
  // beside the ones the carried columns keep.
  let assemblyFormat = [{
    `(` $source `,` `{` $columns_to_filter `}` `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* UnwindRange
*/
def UnwindRange : TuringOp<"unwind_range", [MayRaise, AttrSizedOperandSegments, DeclareOpInterfaceMethods<ConditionallySpeculatable>, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getCarrySetMutable"]>]> {
  let summary = "Produce one row per integer a row's range bounds span, without building the list";

  let description = [{
    `UNWIND range(start, end, step) AS x`: db.unwind over db.range, fused so the list is
    never built. It reads one column per bound and expands the rows in flight through
    them, as db.unwind expands them through its source:

      %x, %n = db.unwind_range(%from, %to, %by, {%node})
        : (!db.column<i64>, !db.column<i64>, !db.column<i64>, !db.column<!storage.node_id>)
        -> (!db.column<!storage.nullable<i64>>, !db.column<!storage.node_id>)

    Row r contributes one row per integer from row r of `start` to row r of `end`, both
    included, by row r of `step`: none when a bound is null or the step walks away from
    `end`, and the error db.range raises for a step of 0. The step is always an operand,
    so a range the query gave none counts by a constant 1. Nothing is laid out, so no
    length limit applies. `columns_to_filter` is the carry set, replicated as db.unwind's
    is, and the element column is the one db.unwind gives a list of integers, or none
    where the list above the range was left for lowering to type, as db.reverse's is.

    `from` and `to` restrict each row to the run of its range that db.list_slice would
    keep, with the same bounds, and `reversed` counts that run out backwards. Both are
    where db.reverse, db.list_slice and db.tail over a db.range fold, so the list is not
    built for those either:

      %x = db.unwind_range(%from, %to, %by, from %a, {}) reversed : ...

    reads `UNWIND reverse(range(from, to, by)[a..]) AS x`.
  }];

  let arguments = (ins Column:$start,
                       Column:$end,
                       Column:$step,
                       Optional<Column>:$from,
                       Optional<Column>:$to,
                       Variadic<Column>:$columns_to_filter,
                       UnitAttr:$reversed);
  let results   = (outs Column:$element, Variadic<Column>:$carried);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $start `,` $end `,` $step (`,` `from` $from^)? (`,` `to` $to^)? `,` `{` $columns_to_filter `}` `)`
    (`reversed` $reversed^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* UnwindComputed
*/
def UnwindComputed : TuringOp<"unwind_computed", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "collectCarriedColumns"]>]> {
  let summary = "Produce one row per element of a value its region computes for each input row";

  let description = [{
    db.unwind whose source is computed inside the op rather than before it, so lowering
    can compute it over a few input rows at a time and never hold the lists of a whole
    chunk. sink_unwind_source moves the ops computing a db.unwind's source in here:

      %x, %n = db.unwind_computed({%node}) ({
        ^bb0(%nodeIn: !db.column<!storage.node_id>):
          %name = db.get_node_properties(%nodeIn) {property = "name"} : ...
          %parts = db.split(%name, %comma) : ...
          db.unwind_yield %parts : !db.column<!storage.list<!storage.string>>
      }) : (!db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)

    The region's entry block takes one argument per carried column and reads the input
    rows through them; a constant it reads stands for every row and needs no carrying.
    The yield names the source, whose cells unwind as db.unwind's do. `columns_to_filter`
    is the carry set, replicated as db.unwind's is, and the element type is resolved the
    same way.
  }];

  let arguments = (ins Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$element, Variadic<Column>:$carried);

  // Exactly one block, ending in a db.unwind_yield.
  let regions = (region SizedRegion<1>:$source);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` `{` $columns_to_filter `}` `)` $source attr-dict `:` functional-type(operands, results)
  }];
}

/**
* UnwindYield
*/
def UnwindYield : TuringOp<"unwind_yield", [Pure, Terminator, HasParent<"UnwindComputed">]> {
  let summary = "Name the source a db.unwind_computed's region computes";

  let arguments = (ins Column:$source);

  let assemblyFormat = [{
    $source attr-dict `:` qualified(type($source))
  }];
}

/**
* CallProcedure
*/
def CallProcedure : TuringOp<"call_procedure", [AttrSizedOperandSegments, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getCarrySetMutable"]>]> {
  let summary = "Call a registered procedure, yielding the return values named";

  let description = [{
    The declarative counterpart of a Cypher CALL ... YIELD. Calls the procedure
    registered under `procedure` and produces one column per yielded return value,
    then one per column carried past the call:

      %id, %label = db.call_procedure("db.labels", {}, {}) yields ["id", "label"]
                      : () -> (!db.column<none>, !db.column<none>)
      db.output(%id, %label) : !db.column<none>, !db.column<none>

    `yields` names the return values the query asked
    for, one per leading result and in result order. A procedure's other return
    values are left unbound and the procedure skips them. A yielded name that the
    procedure does not return, or a result count that disagrees with `yields` and
    the carry set, is rejected.

    The result value types come from the procedure's declared return types, so they
    are left typed none here and resolved during lowering.

    An argument-less call is a source: the procedure produces rows on its own and is
    driven until it declares itself finished, so it lowers to an nl.procedure_init
    and its own nl.for. With arguments it is a per-chunk call: the procedure's
    execute callback runs once per chunk of its argument columns, inside the loop
    that produces them, refilling its result columns each step - the same
    nl.procedure_init shape, opened where those arguments are bound.

    A carried column must be row-aligned with the arguments (both bound by the same
    loop), since the correspondence the procedure reports is an index into its own
    input rows. Only a procedure reporting those indices can be carried past, which
    every procedure declaring a row-aligned argument owes: one taking no argument, or
    only constant ones, has no input row to report against (cross its rows in with
    db.cross_product instead).
  }];

  let arguments = (ins StrAttr:$procedure,
                       Variadic<Column>:$inputs,
                       Variadic<Column>:$carried_columns,
                       StrArrayAttr:$yields);

  // One result per yielded return value, then one per carried column.
  let results   = (outs Variadic<Column>:$results);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $procedure `,` `{` $inputs `}` `,` `{` $carried_columns `}` `)` `yields` $yields
    attr-dict `:` functional-type(operands, results)
  }];
}

#endif // TURINGDB_SOURCE_OPS
