#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"
include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "DBDialect.td"
include "DBTypes.td"
include "DBSystemOps.td"

/**
* ScanNodes
*/
def ScanNodes : TuringOp<"scan_nodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs ColumnNodeIDs:$result);
  // `qualified(...)` keeps the full `!db.column<...>` mnemonic on the result type
  // rather than the stripped `<...>` form, matching how get_out_edges prints.
  let assemblyFormat = "`(`operands`)` attr-dict `:` qualified(type($result))";
}

/**
* ScanNodesByLabel
*/
def ScanNodesByLabel : TuringOp<"scan_nodes_by_label", [Pure]> {
  let summary = "Produce the nodes in the graph carrying every given label";

  let description = [{
    In `MATCH (a:Person)` the node column is not every node but only those
    carrying the Person label; `MATCH (a:Person:Employee)` narrows further to
    the nodes carrying both:

      %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %b = db.scan_nodes_by_label(["Person", "Employee"]) : !db.column<!storage.node_id>

    `labels` is that set of label names, spelled the same as in the query. A
    node qualifies when its own label set contains every requested label (a
    superset match), so a node labelled `Person:Employee:Manager` still matches
    `["Person", "Employee"]`. It follows that if any requested label was never
    created on a node - it is absent from the schema - the conjunction matches
    nothing and the scan yields no rows. Lowering resolves each name to a
    storage LabelID. Scanning the graph is a deterministic read, so the op is
    Pure.

    The label list must not be empty: a label-free scan is `db.scan_nodes`.
  }];

  // `labels` is the set of symbolic label names. Being an attribute and not an
  // SSA value, it is absent from `operands`; lowering resolves each name to a
  // storage LabelID.
  let arguments = (ins StrArrayAttr:$labels);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  // The label list prints inside the parens as `["Person", "Employee"]`, ahead
  // of the colon, the same place db.get_node_properties prints its name; the
  // result type follows, qualified, as db.scan_nodes prints it.
  let assemblyFormat = "`(` $labels `)` attr-dict `:` qualified(type($result))";
}

/**
* ConstScanNodes
*/
def ConstScanNodes : TuringOp<"const_scan_nodes", [Pure]> {
  let summary = "Produce a fixed, explicitly listed set of nodes";

  let description = [{
    The constant-set counterpart of db.scan_nodes: where that produces every node
    in the graph, this produces exactly the nodes whose IDs are listed on the op -
    a set known at plan time rather than discovered by walking the graph:

      %a = db.const_scan_nodes([0, 3, 7]) : !db.column<!storage.node_id>

    It seeds a dataflow from a known set of nodes - the declarative counterpart of
    the plan's ConstScanNode, which turns a node-ID membership predicate
    (MATCH (n) WHERE id(n) IN [0, 3, 7]) into a direct scan of those IDs rather
    than a full scan followed by a filter. Like db.scan_nodes it reads no column
    and takes no input, so it opens a dataflow rather than consuming one; the IDs
    are emitted in the order listed. Producing a fixed list is deterministic, so
    the op is Pure.

    A node ID is a non-negative handle, so the signless i64 dense array matches
    the index list db.sort carries; each entry is a storage NodeID once lowered.
    A listed ID that names no node in the graph matches nothing, so it is dropped
    during lowering and the scan yields only the nodes that exist - the same
    set-membership semantics as the WHERE id(n) IN ... it models. A list whose IDs
    are all absent, or an empty list, simply yields no row - not an error, matching
    that same set-membership.
  }];

  // `nodeIDs` is the constant set of node IDs to emit. Being an attribute and not
  // an SSA value, it is absent from `operands`; lowering forwards it as-is.
  let arguments = (ins DenseI64ArrayAttr:$nodeIDs);
  let results   = (outs ColumnNodeIDs:$result);

  // The node ID list prints inside the parens as `[0, 3, 7]`, ahead of the colon,
  // the same place db.scan_nodes_by_label prints its labels; the result type
  // follows, qualified, as db.scan_nodes prints it.
  let assemblyFormat = "`(` $nodeIDs `)` attr-dict `:` qualified(type($result))";
}

/**
* ScanNodesByPropertyValue
*/
def ScanNodesByPropertyValue : TuringOp<"scan_nodes_by_property_value", [Pure]> {
  let summary = "Produce the nodes whose named property equals a literal";

  let description = [{
    The property-equality counterpart of db.scan_nodes_by_label: where that
    produces the nodes carrying a label, this produces the nodes whose named
    property holds exactly the given literal, optionally among the nodes
    carrying every listed label:

      %a = db.scan_nodes_by_property_value("age", 32 : i64) : !db.column<!storage.node_id>
      %b = db.scan_nodes_by_property_value("age", 32 : i64, ["Person"]) : !db.column<!storage.node_id>

    It is what a `WHERE n.age = 32` filter over a plain or labelled scan fuses
    into (FuseScanByPropertyValue): rather than scanning every node, fetching the
    property of each and masking, the scan walks each part's property column in
    place - only the ranges of the matching label sets when labels are given -
    and emits only the matching node IDs. A node that lacks the property never
    appears, exactly as the filter dropped it on a null value.

    `property` is the property name as spelled in the query; `value` is the
    literal it is compared against - the typed attribute db.constant carried,
    which is one of the literal kinds the query language can spell: a signless
    i64 integer, an f64 double, an i1 boolean or a !storage.string. Lowering
    forwards both as-is; translation resolves the name against the schema and
    coerces the literal to the stored type. A name the schema does not hold, or
    a literal no stored value of that property could equal, deliberately scans
    nothing rather than raising: the fusion is more permissive than the filter
    form, which throws on both. Neither shape reaches here from Cypher - the
    analyzer rejects an unknown property and a mismatched literal type before
    codegen. `labels`, when present, is the label list db.scan_nodes_by_label
    carried, with the same superset semantics: a label absent from the schema
    likewise yields no rows.

    Like db.scan_nodes it reads no column and takes no input, so it opens a
    dataflow rather than consuming one. Scanning the graph is a deterministic
    read, so the op is Pure.
  }];

  let arguments = (ins StrAttr:$property, TypedAttrInterface:$value, OptionalAttr<StrArrayAttr>:$labels);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  // The name, literal and optional labels print inside the parens as
  // `"age", 32 : i64, ["Person"]`, ahead of the colon, the same place
  // db.get_node_properties prints its name; the result type follows, qualified,
  // as db.scan_nodes prints it.
  let assemblyFormat = "`(` $property `,` $value (`,` $labels^)? `)` attr-dict `:` qualified(type($result))";
}

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
    homogeneous and the result is that typed column; when they differ - or the list
    is empty, or holds a null or a nested list, neither of which has a type - it is
    heterogeneous and the result is a type-erased !storage.list_element column, a
    tagged scalar per row, matching Cypher's LIST OF ANY:

      %row = db.unwind_const([true, "s", 10]) : !db.column<!storage.list_element>

    So the result element type is the homogeneity verdict: a downstream op sees a
    concrete typed column for a homogeneous list, and a list_element column - which
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
    The source op for `LOAD CSV 'people.csv' AS row`. The file is fixed by the query, so
    it reads no column and opens a dataflow of its own, the way db.unwind_const opens one
    from a literal list - one row per record of the file:

      %name, %age = db.load_csv("people.csv", [0 : ui64, 1 : ui64])
        : !db.column<!storage.owned_string>, !db.column<!storage.owned_string>

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
        : !db.column<!storage.owned_string>

    Every field is a string: a CSV file carries no types, so a record's fields are the
    characters as written and a query converts them itself (`toInteger(row[1])`). The
    characters live in the column that parsed them rather than in the graph, so the
    columns own their strings.

    `with_headers` consumes the first record as the header line - it names the fields
    rather than holding a row - and is what lets a string entry of `fields` resolve.
    `skip_on_error` is `ON ERROR SKIP`: a malformed record (an unterminated quote, a
    field count differing from the file's) is dropped with a warning instead of failing
    the query.

    Reading a file is deterministic, so the op is Pure. With incoming rows
    (`LOAD CSV ... MATCH (a)`) it is crossed with them through db.cross_product, the way
    a search reading no column is; there is no expanding variant of this op.
  }];

  // The file to read and the fields to produce, each entry a ui64 index or a header
  // name. Being attributes and not SSA values they are absent from `operands`; the
  // path is resolved against the data directory, and a header name against the file's
  // header line, when the load runs.
  let arguments = (ins StrAttr:$path,
                       ArrayAttr:$fields,
                       UnitAttr:$with_headers,
                       UnitAttr:$skip_on_error);
  let results   = (outs Variadic<ColumnOwnedStrings>:$results);
  let hasVerifier = 1;

  // The path and the field list print inside the parens, ahead of the colon, the same
  // place db.unwind_const prints its literals; the flags print as their keywords after
  // it, as db.count's do. The result types follow, qualified, as db.scan_nodes prints
  // its one.
  let assemblyFormat = [{
    `(` $path `,` $fields `)` (`with_headers` $with_headers^)? (`skip_on_error` $skip_on_error^)?
    attr-dict `:` qualified(type($results))
  }];
}

/**
* VectorSearch
*/
def VectorSearch : TuringOp<"vector_search", [Pure]> {
  let summary = "Produce the nearest neighbours of a query vector, with the distance each scored";

  let description = [{
    The source op for `VECTOR SEARCH IN vec1 FOR 3 (1.0, 0.0) YIELD ids, score`. The
    index, the neighbour count and the query vector are all fixed by the query, so it
    reads no column and opens a dataflow of its own, the way db.unwind_const opens one
    from a literal list:

      %ids, %scores = db.vector_search("vec1", 3, [1.000000e+00, 0.000000e+00])
        : !db.column<!storage.node_id>, !db.column<f64>

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

    `query_vector` must have as many elements as the index' dimension, which the schema
    holds rather than the IR, so the search itself reports a mismatch. Reading the index
    is a deterministic read, so the op is Pure. With incoming rows
    (VECTOR SEARCH ... YIELD ids MATCH (n)) it is crossed with them through
    db.cross_product; there is no variant of this op that takes a column.
  }];

  // Every parameter is fixed by the query, so none is an SSA value: the index to search,
  // how many neighbours to report, and the vector to search for.
  let arguments = (ins StrAttr:$index_name, UI64Attr:$k, DenseF32ArrayAttr:$query_vector);
  let results   = (outs ColumnNodeIDs:$ids, ColumnDoubles:$scores);
  let hasVerifier = 1;

  // The parameters print inside the parens ahead of the colon, as db.const_scan_nodes
  // prints its IDs; the two result types follow, qualified, as db.scan_edges prints its.
  let assemblyFormat = [{
    `(` $index_name `,` $k `,` $query_vector `)`
    attr-dict `:` qualified(type($ids)) `,` qualified(type($scores))
  }];
}

/**
* Unwind
*/
def Unwind : TuringOp<"unwind", [Pure]> {
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

    The element column's type is the source's to decide and is resolved during
    lowering, as a property fetch's value column is: unwinding a list yields a
    type-erased !storage.list_element column of tagged scalars, matching Cypher's LIST
    OF ANY, while a source that is already a scalar column keeps its own type - the
    singleton is the value it already holds.
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
* MakeList
*/
def MakeList : TuringOp<"make_list", [Pure]> {
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
def MakeMap : TuringOp<"make_map", [Pure]> {
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
* Range
*/
def Range : TuringOp<"range", [Pure]> {
  let summary = "Build one list of the integers a row's bounds span";

  let description = [{
    Cypher's `range(start, end)` and `range(start, end, step)`. The op reads one column
    per bound and produces one list cell per row, as db.make_list does:

      %xs = db.range(%from, %to) : (!db.column<i64>, !db.column<i64>)
                                -> !db.column<!storage.list<i64>>

    Row r's list counts from row r of `start` to row r of `end`, both included, by
    `step` - 1 where it is absent, and negative to count down. A step walking away from
    `end` gives the empty list; a step of 0 is the error the row raises, as is a row
    spanning more than 100000 integers. A bound holding no value gives its row no list,
    which is what Cypher reads `range(null, 3)` as.

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
def ListSlice : TuringOp<"list_slice", [Pure, AttrSizedOperandSegments]> {
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
    it, so no row errors on a bound; a row whose list or whose bound is null gets a null
    cell, which is what Cypher reads `null[1..2]` as.

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
* ListComprehension
*/
def ListComprehension : TuringOp<"list_comprehension", [Pure]> {
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
* PatternComprehension
*/
def PatternComprehension : TuringOp<"pattern_comprehension", [Pure]> {
  let summary = "Build one list per row out of the matches a pattern finds for that row";

  let description = [{
    Cypher's `[(a)-[:KNOWS]->(b) WHERE p(b) | f(b)]`. The op matches a pattern once per row
    in flight and produces one list cell per row - the values the matches of that row
    contribute - so the rows in flight are the ones it started with, as
    db.list_comprehension leaves them:

      %names = db.pattern_comprehension(%a, %c) ({
        ^bb0(%aIn: !db.column<!storage.node_id>, %cIn: !db.column<none>, %tag: !db.column<ui64>):
          %s, %e, %et, %b, %tag2 = db.get_out_edges(%aIn, {%tag}) : ...
          %name = db.get_node_properties(%b) {property = "name"} : ...
          db.comprehension_yield %tag2, %name : !db.column<none>
      }) : (!db.column<!storage.node_id>, !db.column<none>) -> !db.column<none>

    `input_columns` are every column in flight where the comprehension is read, and the
    region's entry block takes one argument per input column plus a trailing
    `!db.column<ui64>` row tag - the position of each input row, carried through the
    pattern the way any other column is, so the row each match belongs to is known once
    the pattern is walked. The pattern joins onto the columns whose variables it names and
    carries the rest along, exactly as db.optional_match's does; a comprehension read
    where no column is in flight - `RETURN [(a:Person)-->(b) | b.name]` - has no input
    column and so no tag either, and its one list covers the single empty row Cypher
    starts from.

    The WHERE is a db.filter inside the region, so a match it drops is one the projection
    is never computed for, and it cuts the tag alongside the match. The db.comprehension_yield
    names the tag as the pattern left it and the value each match contributes.

    The result's type is the body's to decide and is resolved during lowering, as
    db.list_comprehension's is: the list is of whatever the body yields. A row the pattern
    matches nothing for gets the empty list, never a null - a pattern comprehension is the
    one list-valued expression that cannot be null.
  }];

  let arguments = (ins Variadic<Column>:$input_columns);
  let results   = (outs Column:$result);

  // Exactly one block, ending in a db.comprehension_yield.
  let regions = (region SizedRegion<1>:$pattern);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $input_columns `)` $pattern attr-dict `:` functional-type($input_columns, results)
  }];
}

/**
* ComprehensionYield
*/
def ComprehensionYield : TuringOp<"comprehension_yield",
                                  [Pure, Terminator,
                                   ParentOneOf<["ListComprehension", "PatternComprehension"]>]> {
  let summary = "Name the row each surviving element of a comprehension came from and what it contributes";

  let description = [{
    Terminates the body of a db.list_comprehension or the pattern of a
    db.pattern_comprehension. `row_tags` is the row tag as the body left it - the row each
    surviving element came from, which a WHERE cuts alongside the element - and `value` is
    what that element contributes to its row's list. The tag is absent only for a
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

/**
* ScanEdges
*/
def ScanEdges : TuringOp<"scan_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce every edge in the graph as four row-aligned columns";

  let description = [{
    The edge counterpart of db.scan_nodes: where that produces every node, this
    produces every edge, expanded into the same four row-aligned columns a hop
    exposes - the source node, the edge, its type and the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It reads no column and takes no input, so - like db.scan_nodes - it opens a
    dataflow rather than consuming one. Unlike db.get_out_edges there is no source
    node column to walk from and no carry set to filter: every edge is emitted
    once. The four result chunks are the same shape and order db.get_out_edges
    produces (sources, edge IDs, edge type IDs, targets), so a downstream op reads
    them the same way. Scanning the graph is a deterministic read, so the op is
    Pure.
  }];

  // No operands: a scan opens the dataflow. The four results are the source
  // nodes, the edges, their types and the target nodes, in the same order
  // db.get_out_edges produces them.
  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);

  // A source op has no meaningful operand arrow, so - like db.scan_nodes - the
  // result types are spelled straight after the colon rather than through a
  // functional-type; qualified(...) keeps the leading !db. on each.
  let assemblyFormat = [{
    `(`operands`)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanEdgesByType
*/
def ScanEdgesByType : TuringOp<"scan_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce every edge of the named types in the graph as four row-aligned columns";

  let description = [{
    The by-type sibling of db.scan_edges: where that produces every edge, this
    produces every edge carrying one of the named types, in the same four
    row-aligned columns - the source node, the edge, its type and the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_edges_by_type(["KNOWS"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    An edge carries exactly one type, so `edge_types` is a disjunction over the
    types the pattern named, as `-[:KNOWS|LIKES]->` spells it: an edge matches when
    it carries any one of them. The names are spelled the same as in the query and
    resolved against the graph schema; one absent from the schema drops out, and a
    set nothing resolved into matches no edge, so the scan yields no rows. An empty
    `edge_types` says that outright - a pass narrowing the read to nothing writes one -
    and the scan then walks no edge at all. Like
    db.scan_edges it reads no column and takes no input, so it opens a dataflow
    rather than consuming one, and the result shape is identical - only the rows
    are narrowed.
  }];

  // `edge_types` are the symbolic relationship-type names. Being an attribute and
  // not an SSA value, the op still has no operands; lowering hoists them into an
  // nl.get_edge_type_set handle so they are resolved once above the loops.
  let arguments = (ins StrArrayAttr:$edge_types);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);

  // The type names print inside the parens where db.scan_edges prints nothing;
  // the result types follow the colon, as on db.scan_edges, since a source op has
  // no meaningful operand arrow.
  let assemblyFormat = [{
    `(` $edge_types `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanOutEdgesByLabelSrc
*/
def ScanOutEdgesByLabelSrc : TuringOp<"scan_out_edges_by_label_src", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce every out-edge of the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The by-label sibling of db.scan_edges: where that produces every edge, this
    produces the edges leaving the nodes whose label set is a superset of `labels`,
    in the same four row-aligned columns - the source node, the edge, its type and
    the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a:Person)-->(b)` read off the edge index rather than walked: the
    same edges a db.scan_nodes_by_label feeding a db.get_out_edges produces, in one
    op that never builds the node column between them. `labels` is the label set
    db.scan_nodes_by_label carried, matched the same way - a node labelled
    `Person:Employee` still matches `["Person"]`, and a label absent from the schema
    leaves the conjunction unsatisfiable, so the scan yields no rows. Like
    db.scan_edges it reads no column and takes no input, so it opens a dataflow
    rather than consuming one.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  // `labels` is the set of symbolic label names the source nodes must carry. Being
  // an attribute and not an SSA value, the op still has no operands; lowering
  // resolves each name to a storage LabelID.
  let arguments = (ins StrArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  // The label list prints inside the parens where db.scan_edges_by_type prints its
  // type name; the result types follow the colon, as on db.scan_edges, since a
  // source op has no meaningful operand arrow.
  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanInEdgesByLabelTgt
*/
def ScanInEdgesByLabelTgt : TuringOp<"scan_in_edges_by_label_tgt", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce every in-edge of the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The reverse-direction sibling of db.scan_out_edges_by_label_src: where that
    produces the edges leaving the nodes whose label set is a superset of `labels`,
    this produces the edges arriving at them, in the same four row-aligned columns:

      %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_tgt(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a:Person)<--(b)` read off the edge index rather than walked: the
    same edges a db.scan_nodes_by_label feeding a db.get_in_edges produces. The
    labels constrain the node the walk arrives at, which is the edge's target, so
    `srcids` holds the unconstrained end and `tgtids` the labelled one - the reverse
    of the out-edge scan, where the labelled end is the source. Both columns still
    hold the edge as the graph stores it, exactly as db.get_in_edges fills them, so
    a consumer reads them the same way whichever scan produced them.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  let arguments = (ins StrArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanOutEdgesByLabelTgt
*/
def ScanOutEdgesByLabelTgt : TuringOp<"scan_out_edges_by_label_tgt", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce every out-edge arriving at the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The other endpoint's sibling of db.scan_out_edges_by_label_src: there the labels
    constrain the node the hop leaves, here the one it arrives at, which is the edge's
    target. It produces the edges whose target's label set is a superset of `labels`,
    in the same four row-aligned columns:

      %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_tgt(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a)-->(b:Person)` read off the edge index rather than scanned and
    filtered: the same edges a db.scan_edges keeps once a db.check_label_constraint
    over its target column has dropped the rest. `srcids` holds the unconstrained end
    and `tgtids` the labelled one. Those are the rows db.scan_in_edges_by_label_tgt
    produces, since both read the index keyed by the target's label set; the two are
    told apart by the hop the query wrote, not by what they emit.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  let arguments = (ins StrArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanInEdgesByLabelSrc
*/
def ScanInEdgesByLabelSrc : TuringOp<"scan_in_edges_by_label_src", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce every in-edge leaving the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The other endpoint's sibling of db.scan_in_edges_by_label_tgt: there the labels
    constrain the node the in-hop arrives at, here the one it leaves, which is the
    edge's source. It produces the edges whose source's label set is a superset of
    `labels`:

      %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a)<--(b:Person)` read off the edge index rather than scanned and
    filtered: the same edges a db.scan_edges keeps once a db.check_label_constraint
    over its source column has dropped the rest. `srcids` holds the labelled end and
    `tgtids` the unconstrained one. Those are the rows db.scan_out_edges_by_label_src
    produces, since both read the index keyed by the source's label set.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  let arguments = (ins StrArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* GetOutEdges
*/
def GetOutEdges : TuringOp<"get_out_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch node and edge successors of input, filtering carried columns alongside";

  let description = [{
    In `MATCH (a)->(b)->(c)` the `a` we return are not the raw scan but the nodes
    with a successor `b` that itself has a successor `c`, so each hop must filter
    the columns carried from earlier hops.

    `columns_to_filter` is that carry set; each one comes back in `filtered_columns`:

      %b = get_out_edges(%a, {})
      %c, %b2, %a2 = get_out_edges(%b, {%a})   // eids/etypes omitted
  }];

  // `columns_to_filter`: the only variadic operand, after the fixed `input_nodes`.
  let arguments = (ins Column:$input_nodes, Variadic<Column>:$columns_to_filter);

  // `filtered_columns`: one per carried column, after the four fixed results.
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  // Brace-wrap the carry set so it reads like `get_out_edges(%b, {%a})`.
  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetInEdges
*/
def GetInEdges : TuringOp<"get_in_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch node and edge predecessors of input, filtering carried columns alongside";

  let description = [{
    The reverse-direction sibling of get_out_edges: where that walks each node's
    successors, this walks its predecessors. In `MATCH (a)<-(b)<-(c)` the `a` we
    return are not the raw scan but the nodes with a predecessor `b` that itself
    has a predecessor `c`, so each hop must filter the columns carried from
    earlier hops.

    `columns_to_filter` is that carry set; each one comes back in `filtered_columns`:

      %b = get_in_edges(%a, {})
      %c, %b2, %a2 = get_in_edges(%b, {%a})   // eids/etypes omitted

    The result chunk shape - srcids, eids, etypes, tgtids - matches
    get_out_edges, so the two are interchangeable in a hop.
  }];

  // `columns_to_filter`: the only variadic operand, after the fixed `input_nodes`.
  let arguments = (ins Column:$input_nodes, Variadic<Column>:$columns_to_filter);

  // `filtered_columns`: one per carried column, after the four fixed results.
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  // Brace-wrap the carry set so it reads like `get_in_edges(%b, {%a})`.
  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetEdges
*/
def GetEdges : TuringOp<"get_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch the neighbours of input over an edge in either direction, filtering carried columns alongside";

  let description = [{
      %s, %e, %t, %b = get_edges(%a, {})
      %c, %b2, %a2 = get_edges(%b, {%a})
  }];

  let arguments = (ins Column:$input_nodes, Variadic<Column>:$columns_to_filter);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetOutEdgesByType
*/
def GetOutEdgesByType : TuringOp<"get_out_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch the out-edges of the named types, filtering carried columns alongside";

  let description = [{
    The by-type sibling of get_out_edges. In `MATCH (a)-[:KNOWS]->(b)` only the
    successors reached over a KNOWS edge count, so this walks each node's out-edges
    but keeps only those carrying one of the named types - the plain get_out_edges
    narrowed by the relationship types spelled in the query:

      %s, %e, %t, %b = get_out_edges_by_type(%a, ["KNOWS"], {})
      %c, %b2, %a2 = get_out_edges_by_type(%b, ["LIKES", "LOVES"], {%a})   // eids/etypes omitted

    An edge carries exactly one type, so `edge_types` is a disjunction over the
    types the pattern named, as `-[:KNOWS|LIKES]->` spells it: an edge matches when
    it carries any one of them. The names are spelled the same as in the query and
    resolved against the graph schema; one absent from the schema drops out, and a
    set nothing resolved into matches no edge, so the hop yields no rows. An empty
    `edge_types` says that outright - a pass narrowing the read to nothing writes one -
    and the hop then walks no edge at all.
    `columns_to_filter` is the carry set, exactly as on get_out_edges: each one
    comes back in `filtered_columns`, and the result chunk shape - srcids, eids,
    etypes, tgtids - is identical, so a hop is interchangeable with the unfiltered
    fetch.
  }];

  // `edge_types` are the symbolic relationship-type names. Being an attribute and
  // not an SSA value, they are absent from `operands`; lowering hoists them into an
  // nl.get_edge_type_set handle so they are resolved once above the loops.
  // `columns_to_filter` is the variadic carry set.
  let arguments = (ins Column:$input_nodes, StrArrayAttr:$edge_types, Variadic<Column>:$columns_to_filter);

  // `filtered_columns`: one per carried column, after the four fixed results.
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  // The type names print inside the parens as `["KNOWS"]`, between input_nodes and
  // the brace-wrapped carry set; MLIR elides them from attr-dict because the format
  // already names them. functional-type spells only the operand and result types.
  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetInEdgesByType
*/
def GetInEdgesByType : TuringOp<"get_in_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch the in-edges of the named types, filtering carried columns alongside";

  let description = [{
    The reverse-direction sibling of get_out_edges_by_type: where that keeps a
    node's out-edges of the named types, this keeps its in-edges of those types. In
    `MATCH (a)<-[:KNOWS]-(b)` only the predecessors reached over a KNOWS edge
    count:

      %s, %e, %t, %b = get_in_edges_by_type(%a, ["KNOWS"], {})

    The result chunk shape - srcids, eids, etypes, tgtids - and the
    `columns_to_filter` carry set match get_out_edges_by_type, so the two are
    interchangeable in a hop; `edge_types` is resolved the same way and a set
    nothing resolved into, or an empty one a pass wrote, yields no rows.
  }];

  let arguments = (ins Column:$input_nodes, StrArrayAttr:$edge_types, Variadic<Column>:$columns_to_filter);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetOutEdgesByLabel
*/
def GetOutEdgesByLabel : TuringOp<"get_out_edges_by_label", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch the out-edges arriving at the nodes carrying every given label, filtering carried columns alongside";

  let description = [{
    The by-label sibling of get_out_edges. In `MATCH (a)-->(b:Person)` only the
    successors that are Person count, so this walks each input node's out-edges but
    keeps only those whose target's label set is a superset of `labels`:

      %s, %e, %t, %b = get_out_edges_by_label(%a, ["Person"], {})
      %c, %b2, %a2 = get_out_edges_by_label(%b, ["Movie"], {%a})   // eids/etypes omitted

    The labelled end is the one the hop arrives at - the edge's target - since the
    end it leaves is the input column, which a by-label node scan constrains
    instead. db.scan_out_edges_by_label_tgt keeps the same edges of the whole graph;
    this one keeps them of a chunk of nodes, so the hop carries the input rows the
    scan has none of.

    `columns_to_filter` is the carry set, exactly as on get_out_edges: each one comes
    back in `filtered_columns`, and the result chunk shape - srcids, eids, etypes,
    tgtids - is identical, so a hop is interchangeable with the unfiltered fetch. The
    label list must not be empty: a label-free hop is db.get_out_edges. A label absent
    from the schema leaves the conjunction unsatisfiable, so the hop yields no rows.
  }];

  // `labels` is the label conjunction the target must carry, spelled as in the query.
  // Being an attribute and not an SSA value, it is absent from `operands`; lowering
  // hands the list to the nl hop, which resolves each name once.
  let arguments = (ins Column:$input_nodes, StrArrayAttr:$labels, Variadic<Column>:$columns_to_filter);

  // `filtered_columns`: one per carried column, after the four fixed results.
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);
  let hasVerifier = 1;

  // The label list prints inside the parens as `["Person"]`, between input_nodes and
  // the brace-wrapped carry set, the way get_out_edges_by_type prints its type name.
  let assemblyFormat = [{
    `(` $input_nodes `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetInEdgesByLabel
*/
def GetInEdgesByLabel : TuringOp<"get_in_edges_by_label", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch the in-edges leaving the nodes carrying every given label, filtering carried columns alongside";

  let description = [{
    The reverse-direction sibling of get_out_edges_by_label: where that keeps the
    out-edges arriving at a label set, this keeps the in-edges leaving one. In
    `MATCH (a)<--(b:Person)` only the predecessors that are Person count:

      %s, %e, %t, %b = get_in_edges_by_label(%a, ["Person"], {})

    The labelled end is the one the in-hop leaves, which is the edge's source, so
    `srcids` holds it and `tgtids` the input node the hop walked from. The result
    chunk shape and the `columns_to_filter` carry set match get_out_edges_by_label,
    so the two are interchangeable in a hop; the label list must not be empty and an
    absent name yields no rows.
  }];

  let arguments = (ins Column:$input_nodes, StrArrayAttr:$labels, Variadic<Column>:$columns_to_filter);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $input_nodes `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* ExplorePaths
*/
def ExplorePaths : TuringOp<"explore_paths", [Pure, AttrSizedOperandSegments, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Enumerate the variable-length paths leaving each input node, filtering carried columns alongside";

  let description = [{
    The variable-length sibling of get_out_edges. In `MATCH (n)-[e]->{1,3}(m)` every
    trail of one to three edges leaving `n` is one row: `srcids` is the seed `n`,
    `tgtids` the node the path ends on, `paths` the path itself - a handle into the
    query's path trie that db.expand_path turns into the list of edge IDs `e` binds to.
    An edge appears at most once on a path; nodes may repeat. `min_hops` of zero emits
    one zero-length path per seed (its end is the seed, its list empty); an absent
    `max_hops` is unbounded, which trail semantics keep finite.

      %s, %t, %p = db.explore_paths(%n, {}) forward hops 1 to 3
      %s, %t, %p, %a2 = db.explore_paths(%n, {%a}) both hops 0 edge_types ["KNOWS"]
      %s, %t, %p = db.explore_paths(%n, {}) forward hops 2 to 4 end_labels ["Interest"]

    `direction` is which edges a hop follows; `srcids` is always the seed, whatever the
    direction. `edge_types` restricts every hop to edges carrying one of those types, a
    disjunction as `-[:KNOWS|LIKES*]->` spells it; a name absent from the schema drops
    out, and a set left empty that way matches no edge. The optional `hop`
    region is a predicate on each hop, evaluated over its source node, its edge and its
    end node - the first three block arguments - and yielding one boolean column; a hop
    that fails it is neither emitted nor expanded. The region reads nothing from outside
    but constants. `columns_to_filter` is the carry set, exactly as on get_out_edges.

    `hop_imports` are the columns the hop predicate reads from outside the hop, the way
    `MATCH (n)-[e*1..3 WHERE e.since > n.born]->(m)` reads the seed. Each is row-aligned
    with `input_nodes` and becomes one further block argument of the region, in that order,
    holding the seed row's own value repeated over the candidates of the hop. They are
    operands of their own rather than carried columns: a carry set is trimmed and
    reordered, and nothing comes back for them. An exploration that imports is walked seed
    by seed, so the distinct search, which expands many seeds at one level, is not used for
    it.

    `end_labels` keeps only the paths ending on a node carrying every listed label, the
    way `(m:Interest)` constrains the end of `MATCH (n)-[e]->{2,4}(m:Interest)`. The
    fuse_explore_end_constraint pass sets it from the label filter codegen leaves over
    `tgtids`. A path ending elsewhere is walked through but never emitted, and a prefix
    that cannot reach such a node within the hops it has left is not walked at all.

    `hop_labels` is a hop predicate that asks only for labels on the hop's end node, the way
    `(b:Event)` constrains every hop of `MATCH (n)((a)-[e]->(b:Event)){1,3}(m)`: a hop whose end
    lacks one of them is neither emitted nor expanded. The fuse_explore_hop_labels pass sets it
    from a `hop` region that asks nothing else, which it replaces, so the two never come together.

      %s, %t, %p = db.explore_paths(%n, {}) forward hops 1 to 3 hop_labels ["Event"]

    `end_column` names, by its index in `columns_to_filter`, the carried node column
    holding each seed row's own end: the way `(a)-->(b)` binds the end of
    `MATCH (a)-[e]->+(b), (a)-->(b)` before the exploration runs. The
    fuse_explore_end_nodes pass sets it from the equality filter codegen leaves between
    `tgtids` and that column's carried copy; the copy still comes back, equal to `tgtids`
    row by row. Only the paths ending on the row's target are emitted, and each seed prunes
    the prefixes that cannot reach its target in time.

      %s, %t, %p, %b2 = db.explore_paths(%a, {%b}) forward hops 1 end_column 0

    `end_nodes` binds the end to one set of nodes every seed shares, the way
    `MATCH (a)-[e]->+(b {name:"Adam"})` ends on the nodes named Adam: where `end_column`
    gives each row its own target, this gives the whole walk the same ones. The column is
    consumed in full before the walk runs, as db.shortest_path consumes its two node columns,
    so it is a pipeline breaker on that operand: it comes from a scan of its own, standing
    ahead of the one the seeds come from, and lowering rejects a set the walk would outrun.
    Only the paths ending on a node of the set are emitted, the prefixes that cannot reach one
    in time are not walked, and an empty set emits nothing.

      %s, %t, %p = db.explore_paths(%a, {}) forward hops 1 to 5 end_nodes %b

    `ends_on_seed` is that same constraint when the end the pattern names is the seed it
    started from - `MATCH (a)-[e]->+(a)`, the cycle through `a` - so each row's target is
    its own `srcids` and no carried column holds it. The same pass sets it, from the
    equality codegen leaves between `tgtids` and `srcids`, and the paths that do not come
    back are pruned exactly as a bound end's are.

      %s, %t, %p = db.explore_paths(%a, {}) forward hops 1 to 7 ends_on_seed

    `distinct` asks for each (seed, end) pair once and no path at all, which is what a
    downstream DISTINCT or distinct aggregate reads out of an exploration whose path is
    unread; the fuse_explore_distinct_ends pass sets it. Up to a `min_hops` of one the
    walk answers it as a reachability search, since a walk of one to `max_hops` hops
    always shortens to a trail of as many; deeper minimums keep the trail semantics and
    walk, remembering each subtree no held edge constrained so that the prefixes arriving
    at a node later stop there. `paths` must have no use, which the verifier requires.
  }];

  let arguments = (ins Column:$input_nodes,
                       Variadic<Column>:$columns_to_filter,
                       Optional<ColumnNodeIDs>:$end_nodes,
                       Variadic<Column>:$hop_imports,
                       PathDirection:$direction,
                       UI64Attr:$min_hops,
                       OptionalAttr<UI64Attr>:$max_hops,
                       OptionalAttr<StrArrayAttr>:$edge_types,
                       OptionalAttr<StrArrayAttr>:$end_labels,
                       OptionalAttr<StrArrayAttr>:$hop_labels,
                       OptionalAttr<UI64Attr>:$end_column,
                       UnitAttr:$ends_on_seed,
                       UnitAttr:$distinct);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnNodeIDs:$tgtids,
                        ColumnPathRefs:$paths,
                        Variadic<Column>:$filtered_columns);

  let regions = (region MaxSizedRegion<1>:$hop);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` $direction `hops` $min_hops
    (`to` $max_hops^)? (`edge_types` $edge_types^)? (`end_labels` $end_labels^)?
    (`hop_labels` $hop_labels^)? (`end_nodes` $end_nodes^)? (`end_column` $end_column^)?
    (`hop_imports` `{` $hop_imports^ `}`)? (`ends_on_seed` $ends_on_seed^)?
    (`distinct` $distinct^)? ($hop^)?
    attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* ExpandPath
*/
def ExpandPath : TuringOp<"expand_path", [Pure]> {
  let summary = "Expand each path handle into the list its variable binds to";

  let description = [{
    Turns a column of path handles into a column of lists, one per row: `kind edges`
    is the ordered edge IDs the path variable binds to, `kind ends` the end node of each
    hop and `kind sources` the source node of each hop (the seed, read from `srcids`,
    followed by every end but the last) - the lists the group variables of a
    quantified path pattern bind to.

    `reversed` writes the list backwards, for an exploration the pattern's far end seeded:
    the walk took its hops the other way round, so the last one it took is the pattern's
    first. The kinds swap with it - a reversed `kind ends` is the pattern's source list and
    a reversed `kind sources` its end list - so it is the reversed `kind sources` that
    reads the seed.

      %e = db.expand_path(%p, %s) kind edges
             : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>
  }];

  // The seeds are read by whichever kind ends up holding them - `kind sources` in walk
  // order, `kind ends` reversed - so the other kind leaves them out
  let arguments = (ins ColumnPathRefs:$paths,
                       Optional<ColumnNodeIDs>:$srcids,
                       PathExpansionKind:$kind,
                       UnitAttr:$reversed);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $paths (`,` $srcids^)? `)` `kind` $kind (`reversed` $reversed^)?
    attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* PathLength
*/
def PathLength : TuringOp<"path_length", [Pure]> {
  let summary = "Read the number of edges of each path";

  let description = [{
    The `size(e)` of a path variable: one unsigned count per row, read off the handle
    without expanding the path.

      %n = db.path_length(%p) : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  }];

  let arguments = (ins ColumnPathRefs:$paths);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $paths `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* MakePath
*/
def MakePath : TuringOp<"make_path", [Pure]> {
  let summary = "Build the path a named pattern element binds, one entity sequence per row";

  let description = [{
    `MATCH p = (a)-[x]->(b)` binds `p` to the path the element spells out. This op lays
    that path out as one sequence of graph entities per row - the nodes and the edges it
    runs through, in the order they are written, each entry saying which of the two it is.

      %p = db.make_path(%a, %x, %b)
             : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>)
               -> !db.column<!storage.entity_list>

    The operands are the element's entities in written order. A node column contributes
    one node entry, an edge column one edge entry, and a path column - what a quantified
    relationship binds - the whole walk it stands for: one edge entry and one node entry
    per hop, its seed left out because the node operand ahead of it already holds it. So
    `MATCH p = (a)-[e]->+(b)` is `db.make_path(%a, %pe)`, with no operand for `b`: the
    last node the walk reaches is `b`.

      %p = db.make_path(%a, %pe) : (...) -> !db.column<!storage.entity_list>
      %p = db.make_path(%a, %x, %b, %pe) : (...) -> !db.column<!storage.entity_list>

    A path of no hops contributes no entry, so `MATCH p = (a)-[e]->*(m)` reads back as
    the single node `a` on the rows where the walk stayed put.

    `reversed_paths` lists the path operands the exploration walked against the pattern,
    because the pattern's far end was bound first and seeding there was cheaper. Such an
    operand contributes its hops from the far end back - the node an edge lands on, then
    that edge - so the sequence still reads the way the pattern is written. The node the
    walk seeded from is then the element's last entity, so it is written after the path
    rather than before it.

      %p = db.make_path(%pe, %b) {reversed_paths = [0]} : (...) -> !db.column<!storage.entity_list>
  }];

  let arguments = (ins Variadic<Column>:$entities,
                       OptionalAttr<I64ArrayAttr>:$reversed_paths);
  let results   = (outs ColumnEntityList:$result);

  let assemblyFormat = [{
    `(` $entities `)` attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* GetNodeProperties
*/
def GetNodeProperties : TuringOp<"get_node_properties", [Pure]> {
  let summary = "Read one named property of each node as a column of values";

  let description = [{
    In `MATCH (a) RETURN a.name` the node column produced by a scan or a
    traversal carries only node IDs; this op fetches the value behind `a.name`,
    one per node, into a new column:

      %a    = db.scan_nodes() : !db.column<!storage.node_id>
      %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    A node may not have the property, so missing values come back null and no
    node is dropped - the lowering uses the with-null property fetch. One op
    reads one property; `RETURN a.name, a.age` is two ops sharing %a. Reading a
    property is a deterministic read of the graph, so the op is Pure.

    `pending`, when present, is the mask a db.merge produced beside its node
    column: those rows hold an offset into the change's write buffer rather than
    an ID the graph knows, so their value is read out of that buffer instead of off
    whatever node the offset would collide with - null there when the write set the
    property on none of them.

      %v = db.get_node_properties(%n, "age") pending %mask
             : (!db.column<!storage.node_id>, !db.column<!storage.bool>) -> !db.column<none>

    `all_pending` says the same of every row without a mask to say it row by row,
    which is what a db.create_node's column is: the read comes wholly out of the
    write buffer.

      %v = db.get_node_properties(%n, "age") all_pending
             : (!db.column<!storage.node_id>) -> !db.column<none>
  }];

  // `property` is the symbolic property name, the same `name`/`age` spelled in
  // the query. Being an attribute and not an SSA value, it is absent from
  // `operands` below; lowering resolves it to a storage PropertyTypeID.
  let arguments = (ins Column:$input_nodes,
                       StrAttr:$property,
                       Optional<Column>:$pending,
                       UnitAttr:$all_pending);
  let results   = (outs Column:$result);

  // The string attribute prints inside the parens, ahead of the colon, and MLIR
  // elides it from attr-dict because the format already names it. The optional
  // mask prints after a `pending` keyword, which anchors its group.
  let assemblyFormat = [{
    `(` $input_nodes `,` $property `)` (`pending` $pending^)? (`all_pending` $all_pending^)?
    attr-dict `:` functional-type(operands, results)
  }];
}

/**
* GetEdgeProperties
*/
def GetEdgeProperties : TuringOp<"get_edge_properties", [Pure]> {
  let summary = "Read one named property of each edge as a column of values";

  let description = [{
    The edge counterpart of get_node_properties. In
    `MATCH (a)-[e]->(b) RETURN e.weight` the edge column carries only edge IDs;
    this op fetches the value behind `e.weight`, one per edge, with missing
    values null:

      %weight = db.get_edge_properties(%eids, "weight") : (!db.column<!storage.edge_id>) -> !db.column<none>

    The input column carries its element type - !storage.edge_id here - and the
    op chosen (get_node_properties vs get_edge_properties) selects which side of
    the graph the property is read from. The value column is left typed none: the
    property's value type is only resolved against the schema during lowering.

    `pending` is get_node_properties' mask, over edges: a row a db.merge wrote
    holds a write-buffer offset, and its value is read out of the write buffer.
    `all_pending` is its maskless form, which a db.create_edge's column is.
  }];

  let arguments = (ins Column:$input_edges,
                       StrAttr:$property,
                       Optional<Column>:$pending,
                       UnitAttr:$all_pending);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `)` (`pending` $pending^)? (`all_pending` $all_pending^)?
    attr-dict `:` functional-type(operands, results)
  }];
}

def GetNodeLabelSet : TuringOp<"get_node_label_set", [Pure]> {
  let summary = "Read the label-set ID of each node as a column";

  let description = [{
    For each node in the input column, fetches its LabelSetID.

      %ls = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
      %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$input_nodes);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_nodes `)` attr-dict `:` functional-type(operands, results)
  }];
}

def GetEdgeTypes : TuringOp<"get_edge_types", [Pure]> {
  let summary = "Read the type ID of each edge as a column";

  let description = [{
    The edge counterpart of get_node_label_set: for each edge in the input column,
    fetches its EdgeTypeID. A traversal already publishes this column beside the edges
    it walks, so this reads the type of edges that reach it any other way - the ones a
    barrier republished, whose type column the barrier did not carry.

      %et = db.get_edge_types(%e) : (!db.column<!storage.edge_id>) -> !db.column<!storage.edge_type_id>
      %ok = db.check_edge_type_constraint(%et, ["KNOWS"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$input_edges);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_edges `)` attr-dict `:` functional-type(operands, results)
  }];
}

def CheckLabelConstraint : TuringOp<"check_label_constraint", [Pure]> {
  let summary = "Test each label-set ID against a required-label constraint";

  let description = [{
    For each LabelSetID in the input column, emits true if the node's label set
    contains every label in `labels` (a superset match), false otherwise.

      %ok = db.check_label_constraint(%ls, ["Person", "Employee"])
              : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$labelset_ids, StrArrayAttr:$labels);
  let results   = (outs Column:$result);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labelset_ids `,` $labels `)` attr-dict `:` functional-type(operands, results)
  }];
}

def CheckEdgeTypeConstraint : TuringOp<"check_edge_type_constraint", [Pure]> {
  let summary = "Test each edge type ID against a required-type constraint";

  let description = [{
    For each EdgeTypeID in the input column, emits true if the edge's type
    matches any of the types in `edge_types` (OR semantics), false otherwise.

      %ok = db.check_edge_type_constraint(%et, ["KNOWS", "LIKES"])
              : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$edge_type_ids, StrArrayAttr:$edge_types);
  let results   = (outs Column:$result);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $edge_type_ids `,` $edge_types `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* CrossProduct
*/
def CrossProduct : TuringOp<"cross_product", [Pure]> {
  let summary = "Cross every row of one column dataflow with every row of another";

  let description = [{
    Models a Cypher disconnected pattern, e.g. `MATCH (a)-->(b), (c)-->(d)`: two
    column dataflows that share no variable, whose result is the cartesian
    product of their rows. Each side is a self-contained db dataflow held in its
    own region - `leftFactor` and `rightFactor` - with no operands and no block
    arguments, so the two factors share no SSA value.

      %a, %b = db.cross_product factor {
        %x = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %x : !db.column<!storage.node_id>
      } factor {
        %y = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %y : !db.column<!storage.node_id>
      }

    Because the factors are disconnected, which columns end up in the result
    cannot be read off a data dependency (unlike get_out_edges' carry set within
    a connected traversal) - it must be stated. Each region ends with a db.yield
    naming exactly the columns that factor contributes, and the op's results are
    the left factor's yielded columns followed by the right factor's. Each factor
    must yield at least one column: a side's row count is read from its first
    yielded column, so a factor that surfaces none cannot be sized and the
    verifier rejects an empty db.yield.

    The result types are not spelled after the regions; the parser recovers them
    from the two yields and the verifier checks they line up. Reading the graph
    is deterministic, so the op is Pure.
  }];

  // The results are the concatenation of the two factors' yielded columns; the
  // count and types are recovered from the yields, never spelled out.
  let results = (outs Variadic<Column>:$results);

  // Each factor is exactly one block: a self-contained dataflow ending in a
  // db.yield, with no operands and no block arguments.
  let regions = (region SizedRegion<1>:$leftFactor, SizedRegion<1>:$rightFactor);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The only builder takes the result types and creates the two empty factor
  // blocks; the caller fills each region and terminates it with a db.yield
  // whose operands match the result types contributed by that factor.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes)>
  ];
}

/**
* HashJoin
*/
def HashJoin : TuringOp<"hash_join", [Pure]> {
  let summary = "Cross two column dataflows, keeping the rows their key columns agree on";

  let description = [{
    The fused form of a db.cross_product whose product one db.filter cuts by a
    single db.eq between a column of each factor - an inner equi-join. It holds
    each side the way the product does, in its own region ending in a db.yield,
    and its results are the left factor's yielded columns followed by the right
    factor's.

      %a, %ka, %b, %kb = db.hash_join factor {
        %x = db.scan_nodes() : !db.column<!storage.node_id>
        %k = db.get_node_properties(%x, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
        db.yield %x, %k : !db.column<!storage.node_id>, !db.column<none>
      } factor {
        %y = db.scan_nodes() : !db.column<!storage.node_id>
        %k = db.get_node_properties(%y, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
        db.yield %y, %k : !db.column<!storage.node_id>, !db.column<none>
      } on 1, 1

    `on` names the key column of each side as an index into that factor's
    db.yield - the left factor's first, the right factor's second. A result row
    pairs a left row with a right row whose keys hold the same value. A row whose
    key is null joins with nothing, which is what the equality it replaces did:
    `=` against a null gives null and the filter keeps only the true rows.

    The right factor is the built side and the left the probed one, so a left
    row's matches come out together and in right-factor order, however either
    side was chunked. That is not the order the product and its filter emitted
    the surviving rows in: the product re-runs its right factor once per chunk
    of the left, so a right side spanning several chunks interleaves them. Row
    order is what an ORDER BY is for, so the fusion is free to change it - but
    a LIMIT with no ORDER BY over a join can keep other rows than the same
    LIMIT over the product.

    As on db.cross_product the result types are not spelled after the regions;
    the parser recovers them from the two yields and the verifier checks they line
    up. Reading the graph is deterministic, so the op is Pure.
  }];

  // Which column of each factor's yield carries the join key: the left factor's,
  // then the right factor's.
  let arguments = (ins UI64Attr:$leftKey, UI64Attr:$rightKey);

  // The results are the concatenation of the two factors' yielded columns; the
  // count and types are recovered from the yields, never spelled out.
  let results = (outs Variadic<Column>:$results);

  // Each factor is exactly one block: a self-contained dataflow ending in a
  // db.yield, with no operands and no block arguments.
  let regions = (region SizedRegion<1>:$leftFactor, SizedRegion<1>:$rightFactor);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The only builder takes the result types and the two key indices and creates
  // the two empty factor blocks; the caller fills each region and terminates it
  // with a db.yield whose operands match the result types contributed by that
  // factor.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes, "uint64_t":$leftKey, "uint64_t":$rightKey)>
  ];
}

/**
* Yield
*/
def Yield : TuringOp<"yield", [Pure, Terminator, ParentOneOf<["CrossProduct", "HashJoin", "Union", "ExplorePaths"]>]> {
  let summary = "Name the columns a region contributes to the result of the op holding it";

  let description = [{
    Terminates a factor region of a db.cross_product or a db.hash_join, naming
    the columns that factor contributes. Either op's results are the left
    factor's yielded columns followed by the right factor's. A factor must yield
    at least one column - the holder's verifier rejects an empty yield, since a
    side's row count is read from its first yielded column.

    It also terminates each branch of a db.union that has results, naming the
    rows that branch adds to them, and the hop region of a db.explore_paths,
    yielding the one boolean column that says which candidates of a hop pass its
    predicate.
  }];

  let arguments = (ins Variadic<Column>:$columns);

  // The yielded column types vary from query to query and cannot be built from
  // a recipe, so - when present - they are spelled after the colon, as in
  // db.output. The optional group, anchored on the operands, keeps the syntax
  // total: an empty yield (`db.yield`) still parses, then the holder's verifier
  // rejects it with a clear db-level diagnostic.
  let assemblyFormat = "( $columns^ `:` qualified(type($columns)) )? attr-dict";
}

/**
* CallProcedure
*/
def CallProcedure : TuringOp<"call_procedure", [AttrSizedOperandSegments]> {
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

    The result value types come from the procedure's declared return types, so -
    like db.get_node_properties' value column - they are left typed none here and
    resolved during lowering.

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

/**
* Output
*/
def Output : TuringOp<"output"> {
  let summary = "Project a set of columns as the query result";
  let description = [{
      Sends a number of columns to output.

      db.output(%a, %b, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>

    Variadic<Column> accepts any number of columns of any value. The op has no
    results and is deliberately not Pure: emitting the result is its whole
    purpose, so it must survive dead-code elimination.

    An optional `names` array gives the result columns their query-level names,
    one per column: the alias a RETURN item was given through AS, or the item
    spelled as it was written when it has none. A hand-written program that
    names nothing leaves the array off, and the result columns are unnamed.

      db.output(%a, %b) names ["person", "n.age"] : !db.column<!storage.node_id>, !db.column<i64>
  }];
  let arguments = (ins Variadic<Column>:$columns,
                       OptionalAttr<StrArrayAttr>:$column_names);

  // The column types vary from query to query and cannot be built from a
  // recipe, so they are spelled after the colon, the same as nl.output.
  // qualified(...) forces the leading !db. on every printed type. The names,
  // when present, print after a `names` keyword, which anchors the optional
  // group for the declarative parser.
  let assemblyFormat = [{
    `(` $columns `)` (`names` $column_names^)? attr-dict `:` qualified(type($columns))
  }];

  // A hand-written program names nothing, so it gets a builder taking the columns alone.
  let builders = [
      OpBuilder<(ins "::mlir::ValueRange":$columns)>
  ];

  // One name per column or none at all: a shorter array would name the wrong
  // column, and a longer one names a column that is not emitted.
  let hasVerifier = 1;
}

/**
* Limit
*/
def Limit : TuringOp<"limit", [Pure]> {
  let summary = "Take the first N rows of a column dataflow";
  let description = [{
    The declarative counterpart of a Cypher RETURN ... LIMIT N. Caps the result
    to the first N rows of the columns flowing into db.output, passing them
    through unchanged:

      %a  = db.scan_nodes() : !db.column<!storage.node_id>
      %la = db.limit(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
      db.output(%la) : !db.column<!storage.node_id>

    It lowers to a hoisted nl.limit handle, an nl.limit_update in the innermost
    loop body, a `limit` operand on every nl.for in the nest, and a `limit`
    operand on nl.output. Reading a prefix of the graph is deterministic, so the
    op is Pure.
  }];

  // The columns flow through unchanged; count is unsigned - a row budget is
  // never negative, so a negative literal fails to parse rather than reaching
  // the verifier.
  let arguments = (ins Variadic<Column>:$columns, UI64Attr:$count);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form and the named count attribute prints
  // ahead of the colon, elided from attr-dict.
  let assemblyFormat = [{
    `(` $columns `)` `count` $count attr-dict `:` functional-type(operands, results)
  }];

  // results arity and types must equal the columns.
  let hasVerifier = 1;
}

/**
* Skip
*/
def Skip : TuringOp<"skip", [Pure]> {
  let summary = "Drop the first M rows of a column dataflow";
  let description = [{
    The declarative counterpart of a Cypher RETURN ... SKIP M. Drops the first M
    rows of the columns flowing through it, passing the rest through unchanged:

      %a  = db.scan_nodes() : !db.column<!storage.node_id>
      %sa = db.skip(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
      db.output(%sa) : !db.column<!storage.node_id>
  }];

  // The columns flow through unchanged; count is unsigned - a count of rows to
  // drop is never negative, so a negative literal fails to parse rather than
  // reaching the verifier.
  let arguments = (ins Variadic<Column>:$columns, UI64Attr:$count);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form and the named count attribute prints
  // ahead of the colon, elided from attr-dict.
  let assemblyFormat = [{
    `(` $columns `)` `count` $count attr-dict `:` functional-type(operands, results)
  }];

  // results arity and types must equal the columns.
  let hasVerifier = 1;
}

/**
* Sort
*/
def Sort : TuringOp<"sort", [Pure]> {
  let summary = "Reorder the rows of a column set by one or more key columns";
  let description = [{
    The declarative counterpart of a Cypher RETURN ... ORDER BY. Takes the whole
    set of columns flowing into db.output and emits them with their rows reordered
    so that the key columns are sorted; every column moves as one row, so the
    projection stays row-aligned:

      %a     = db.scan_nodes() : !db.column<!storage.node_id>
      %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
      %sa, %sscore = db.sort(%a, %score) keys [1] ascending [true]
                       : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
      db.output(%sa, %sscore) : !db.column<!storage.node_id>, !db.column<none>

    A row is sorted across several columns: `keys` is the list of column indices
    to sort by, most significant first (so `keys [1, 0]` sorts by column 1, ties
    broken by column 0), and `ascending` gives the direction of each key. The
    columns pass through unchanged in count and type - only the row order changes.

    Sort is a pipeline breaker: it must see every row before it can emit any, so
    it lowers to an nl.sort_buffer accumulator hoisted above the loops, an
    nl.sort_collect in the producing loop body that appends each chunk, and an
    nl.sort source iterator after the loop whose nl.for emits the sorted rows.
    Reordering a deterministic read of the graph is itself deterministic, so the
    op is Pure.
  }];

  // The columns flow through unchanged. keyColumns are indices into $columns,
  // most significant first; keyAscending gives one direction per key (true =
  // ascending). The two arrays are parallel and the verifier checks they line up.
  let arguments = (ins Variadic<Column>:$columns,
                       DenseI64ArrayAttr:$keyColumns,
                       DenseBoolArrayAttr:$keyAscending);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form. The two key arrays print after their
  // keywords (`keys array<i64: ...>`, `ascending array<i1: ...>`) ahead of the
  // colon, elided from attr-dict because the format already names them.
  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyColumns `ascending` $keyAscending attr-dict `:`
    functional-type(operands, results)
  }];

  // results arity and types must equal the columns, and the key arrays must be
  // parallel and reference columns in range.
  let hasVerifier = 1;
}

/**
* RowBarrier
*/
// NOT Pure: it fixes the point every row has to reach before the ops below it run, which
// a pass must neither move nor fold away.
def RowBarrier : TuringOp<"row_barrier", []> {
  let summary = "Hold every row of a column set before emitting any of them";
  let description = [{
    Takes the whole set of columns in flight and emits them unchanged, rows in the order
    they came, once every row has arrived. A write below it then runs after every read
    above it has finished: a read that runs again for a later chunk cannot see what the
    write did for an earlier one.

      %n = db.scan_nodes() : !db.column<!storage.node_id>
      %bn = db.row_barrier(%n) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    A pipeline breaker like db.sort: it lowers to an nl.row_buffer hoisted above the
    loops, an nl.row_collect in the producing loop body, and an nl.row_drain source
    iterator after the loop whose nl.for emits the rows.
  }];

  let arguments = (ins Variadic<Column>:$columns);
  let results   = (outs Variadic<Column>:$results);

  let assemblyFormat = [{
    `(` $columns `)` attr-dict `:` functional-type(operands, results)
  }];

  // results arity and types must equal the columns.
  let hasVerifier = 1;
}

/**
* RemoveDuplicates
*/
// NOT Pure: it records each row it emits in a seen-set, so it must run where it sits and
// must not be hoisted, CSE'd or eliminated (like nl.distinct_filter, which it lowers to).
def RemoveDuplicates : TuringOp<"remove_duplicates", [AttrSizedOperandSegments]> {
  let summary = "Emit each distinct row of a column set once";
  let description = [{
    The declarative counterpart of a Cypher RETURN DISTINCT / WITH DISTINCT. Takes
    the whole set of columns flowing through it and emits only the first occurrence
    of each distinct row, dropping later duplicates. A row's identity is the tuple
    across all its columns - so the whole projection is the dedup key - and two
    nulls count as equal, as Cypher DISTINCT dedups nulls together. The columns
    pass through unchanged in count and type; only duplicate rows are removed:

      %a  = db.scan_nodes() : !db.column<!storage.node_id>
      %da = db.remove_duplicates(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
      db.output(%da) : !db.column<!storage.node_id>

    Unlike db.sort it is not a pipeline breaker: it emits a row the moment it first
    sees it, keeping only a growing seen-set. It lowers to a hoisted nl.distinct
    seen-set handle and an nl.distinct_filter in the producing loop body that emits
    each step's not-yet-seen rows as fresh chunks - consumed by db.output when the
    query ends here, or by a downstream traversal when a WITH DISTINCT feeds a
    further MATCH. Because it streams and shrinks each chunk, a following db.limit
    charges the deduped survivor count and bounds the producing loop through the
    ordinary early-exit, so DISTINCT ... LIMIT k needs no special fusion - the scan
    stops once k distinct rows are found.
  }];

  // The columns flow through unchanged; every column is part of the dedup key, so
  // - unlike db.sort's keys - no key attribute is needed. The optional set is the
  // seen-set this dedup shares with its siblings; absent, it keeps one of its own.
  let arguments = (ins Variadic<Column>:$columns, Optional<DistinctSetType>:$set);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form, the same as db.limit and db.sort. The set
  // prints after an `against` keyword, ahead of the colon, so the functional type
  // stays the columns alone.
  let assemblyFormat = [{
    `(` $columns `)` (`against` $set^)? attr-dict `:` functional-type($columns, $results)
  }];

  // results arity and types must equal the columns (the shared pass-through check).
  let hasVerifier = 1;
}

/**
* DistinctSet
*/
// NOT Pure: two seen-sets must never be CSE'd into one, and this op has no operands to
// tell one from another (like nl.distinct, which it lowers to).
def DistinctSet : TuringOp<"distinct_set", []> {
  let summary = "Open a dedup key set for several db.remove_duplicates to share";
  let description = [{
    A UNION dedups its whole result, but each of its branches reaches that result
    through a db.output of its own, so no single db.remove_duplicates sees every row.
    This produces the set they all record their rows in, making the dedup one:

      %s = db.distinct_set : !db.distinct_set
      db.union {
        ...
        %da = db.remove_duplicates(%a) against %s : (!db.column<none>) -> !db.column<none>
        db.output(%da) names ["name"] : !db.column<none>
      }, {
        ...
      }

    It lowers to the single nl.distinct handle every branch's nl.distinct_filter reads.
    A db.remove_duplicates naming no set is unaffected: it keeps a set of its own.
  }];

  let results = (outs DistinctSetType:$set);
  let assemblyFormat = "attr-dict `:` qualified(type($set))";
}

/**
* Union
*/
def Union : TuringOp<"union", [NoTerminator]> {
  let summary = "Emit the results of several query bodies as one result table";
  let description = [{
    The declarative counterpart of Cypher's UNION and UNION ALL. Holds one region per
    branch, each a self-contained db query body ending in its own db.output, and emits
    their rows one branch after another:

      db.union {
        %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
        %n = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
        db.output(%n) names ["name"] : !db.column<none>
      }, {
        %c = db.scan_nodes_by_label(["City"]) : !db.column<!storage.node_id>
        %m = db.get_node_properties(%c, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
        db.output(%m) names ["name"] : !db.column<none>
      }

    The branches share no SSA value and read none from outside, bar the db.distinct_set
    a deduping union hands each of them. Every branch outputs the same number of columns
    under the same names - one result table - so the op needs no result of its own and is
    the whole program, as a lone db.output is for a query without a union.

    UNION (as against UNION ALL) is not held here: a deduping union is spelled as a
    db.distinct_set above the op and a db.remove_duplicates against it in each branch it
    covers, so the dedup is in the dataflow it applies to rather than in a flag on the
    holder. It lowers to one loop nest per branch, run in order, in the same nl function -
    the concatenation is the sink seeing each nest's rows in turn. Not Pure: its branches
    emit the result, so it must survive dead-code elimination as db.output does.

    The body of a db.call_subquery is a UNION when the CALL's is. The rows the branches
    produce then feed the rest of the body rather than the result table, so the op has one
    result per column and each branch ends in a db.yield of its columns. The branches read
    the body's arguments, the columns of the row the body runs for:

      %z = db.union {
        %s, %e, %et, %i = db.get_out_edges(%pIn, {}) : ...
        db.yield %i : !db.column<!storage.node_id>
      }, {
        %s2, %e2, %et2, %k = db.get_in_edges(%pIn, {}) : ...
        db.yield %k : !db.column<!storage.node_id>
      } : !db.column<!storage.node_id>
  }];

  let results = (outs Variadic<Column>:$results);

  // One region per branch, each exactly one block. The blocks have no arguments: a
  // branch reads nothing the branch before it bound.
  let regions = (region VariadicRegion<SizedRegion<1>>:$branches);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // Both builders take the branch count and create that many empty blocks; the caller
  // fills each region and ends it with a db.output, or with a db.yield when the op has
  // results.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "size_t":$branchCount)>,
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes, "size_t":$branchCount)>
  ];
}

/**
* Count
*/
def Count : TuringOp<"count", [Pure]> {
  let summary = "Count the non-null rows of a column as a single-row result";
  let description = [{
    The declarative counterpart of a Cypher RETURN count(x). Consumes the whole
    dataflow of one column and produces a single-row column holding how many of its
    rows are non-null - matching Cypher, where count(x) counts the rows in which x
    is not null. A never-null column (node IDs off a scan) counts every row, so
    count(*) is expressed by counting such a column:

      %a = db.scan_nodes() : !db.column<!storage.node_id>
      %n = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<ui64>
      db.output(%n) : !db.column<ui64>

    `rows` makes it Cypher's count(*): the tally charges every row, whatever the input
    column holds, so the wildcard can be anchored on whichever column of the query part
    carries its rows - a nullable property column included, which a null-skipping tally
    would under-count:

      %n = db.count(%age) rows : (!db.column<none>) -> !db.column<ui64>

    `distinct` makes it Cypher's count(DISTINCT x): the tally charges each distinct
    non-null value once, however many rows carry it. It stays one op rather than
    two because the reduction is the same tally over a deduplicated column - it
    lowers to the DISTINCT seen-set (nl.distinct / nl.distinct_filter) feeding the
    ordinary nl.count_update, so nulls are dropped by the count as usual:

      %n = db.count(%a) distinct : (!db.column<!storage.node_id>) -> !db.column<ui64>
  }];

  // One column in, one integer column out. The input and result types are
  // unrelated (an integer count of any column), so - unlike the pass-through ops -
  // there is nothing to cross-check and no verifier. distinct and rows are unit
  // attributes: their presence is the flag, so a plain db.count is two absent ones.
  let arguments = (ins Column:$input, UnitAttr:$distinct, UnitAttr:$rows);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, the same as db.get_node_properties.
  // The flags print as their keywords ahead of the colon when set, so a plain count
  // keeps the form it had before they existed.
  let assemblyFormat = [{
    `(` $input `)` (`distinct` $distinct^)? (`rows` $rows^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* CountScanRows
*/
def CountScanRows : TuringOp<"count_scan_rows", [Pure]> {
  let summary = "Count the rows of whole node scans from the graph's node counts";

  let description = [{
    What a db.count collapses to when the rows it tallies are whole node scans and
    nothing else. `MATCH (a:Reaction) RETURN count(*)` asks how many Reaction nodes
    there are, not which ones, and the graph already holds that number per label -
    so the answer is a metadata read rather than a walk.

    `labels` lists one label conjunction per scan, in factor order, and the result
    is the product of their node counts - the row count of the cartesian product
    those scans form:

      %n = db.count_scan_rows([["Reaction"]]) : !db.column<none>
      %m = db.count_scan_rows([["Reaction"], ["Pathway"]]) : !db.column<none>

    An empty conjunction is an unlabelled scan and counts every node, so
    `MATCH (a) RETURN count(*)` is db.count_scan_rows([[]]). The list itself must
    not be empty: with no scan listed there is nothing to count. A label absent from
    the schema matches no node, so its factor counts 0 and the whole product is 0 -
    the same unsatisfiable conjunction db.scan_nodes_by_label yields no row for.

    `property`, when given, narrows one scan to the nodes holding that property -
    Cypher's count(a.name), which counts the rows in which a.name is not null, so the
    answer is how many of that scan's nodes hold it rather than how many there are.
    That too the graph knows without reading a value: it indexes each property by the
    label set of the nodes holding it.

      %n = db.count_scan_rows([["Person"]]) property "name" : !db.column<none>

    `propertyScan` is which of the listed scans it is read from, counted from 0 and
    defaulting to the first. `MATCH (a:Reaction), (b:Pathway) RETURN count(b.stId)`
    reads it from the second, so the tally is every Reaction crossed with the Pathways
    that hold stId:

      %m = db.count_scan_rows([["Reaction"], ["Pathway"]]) property "stId" of scan 1
             : !db.column<none>

    A count reads one column, so at most one scan is ever narrowed; the others
    contribute their whole node count.

    The result is the single-row unsigned tally db.count produces, typed as loosely
    as db.count types its own so that replacing one leaves its consumers untouched.
    Reading the counts is a deterministic read, so the op is Pure.
  }];

  // `labels` is one label conjunction per scan, `property` the name whose holders one
  // of them is narrowed to and `propertyScan` which one. Being attributes and not SSA
  // values, they are absent from `operands`; lowering forwards them as-is.
  let arguments = (ins StrArrayArrayAttr:$labels,
                       OptionalAttr<StrAttr>:$property,
                       OptionalAttr<UI64Attr>:$propertyScan);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  // The conjunctions print inside the parens as `[["Reaction"], ["Pathway"]]`, ahead
  // of the colon, the same place db.scan_nodes_by_label prints its labels; the
  // property and the scan it is read from follow their keywords when set, the way
  // db.count prints its flags; the result type follows, qualified, as db.count prints
  // its own.
  let assemblyFormat = [{
    `(` $labels `)` (`property` $property^)? (`of` `scan` $propertyScan^)? attr-dict `:` qualified(type($result))
  }];
}

/**
* Aggregates: Sum / Min / Max / Avg
*/
// The value-reducing counterparts of db.count. Each consumes the whole dataflow of
// one property column and produces a single-row column holding the reduction.
// Cypher aggregates ignore nulls, so they reduce only the non-null rows.
//
// Unlike count(*), which tallies rows regardless of value and so accepts an ID
// column, these reduce the values themselves, so the input must be a property
// value column. The result value type follows Cypher and is resolved during
// lowering: sum and min/max keep the input's type, avg is always a float, and the
// result is null when no non-null row was seen (except sum, which is 0). Like
// db.count the input and result types are unrelated, so there is no verifier.
//
// One op per reduction, the way db.count is its own op: the op name is the
// aggregate, so no kind attribute is needed and each reads like `db.sum(%x)`. They
// share this base since only the name differs.
//
// `distinct` is Cypher's sum(DISTINCT x) / avg(DISTINCT x): the reduction charges
// each distinct value once, however many rows carry it. As on db.count it stays one
// op, since the reduction is unchanged over a deduplicated column - it lowers to the
// DISTINCT seen-set (nl.distinct / nl.distinct_filter) feeding the ordinary
// nl.aggregate_update:
//
//   %s = db.sum(%x) distinct : (!db.column<none>) -> !db.column<none>
//
// db.min and db.max share the flag but the frontend never sets it: dropping repeated
// values cannot move an extremum, so min(DISTINCT x) generates a plain db.min.
class AggregateOp<string mnemonic> : TuringOp<mnemonic, [Pure]> {
  let summary = "Reduce a column of property values to a single-row result";

  let arguments = (ins Column:$input, UnitAttr:$distinct);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, the same as db.count. The distinct
  // flag prints as its keyword ahead of the colon when set, so a plain reduction
  // keeps the form it had before the flag existed.
  let assemblyFormat = [{
    `(` $input `)` (`distinct` $distinct^)? attr-dict `:` functional-type(operands, results)
  }];
}

def Sum : AggregateOp<"sum">;
def Min : AggregateOp<"min">;
def Max : AggregateOp<"max">;
def Avg : AggregateOp<"avg">;

class UnaryFunctionOp<string mnemonic> : TuringOp<mnemonic, [Pure, ConstantThroughOperands]> {
  let summary = "Apply a unary function row-wise to a value column";

  let arguments = (ins Column:$input);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input `)` attr-dict `:` functional-type(operands, results)
  }];
}

def Labels : UnaryFunctionOp<"labels">;
def EdgeType : UnaryFunctionOp<"edge_type">;

// The two ends of each edge of a column, as node columns of their own. The edge is read as
// the graph stores it, so a hop that walked it backwards or undirected reports the same
// pair rather than the one its own direction suggests.
def StartNode : UnaryFunctionOp<"start_node">;
def EndNode : UnaryFunctionOp<"end_node">;

def ToInteger : UnaryFunctionOp<"to_integer">;
def ToFloat : UnaryFunctionOp<"to_float">;
def ToBoolean : UnaryFunctionOp<"to_boolean">;
def ToDateTime : UnaryFunctionOp<"to_datetime">;
def ToString : UnaryFunctionOp<"to_string">;

// The calendar fields of an instant, as integer columns. The sub-second pair reads the
// fraction of the second truncated to its own precision, so .123456 of a second in is a
// db.datetime_millisecond of 123 and a db.datetime_microsecond of 123456.
def DateTimeYear : UnaryFunctionOp<"datetime_year">;
def DateTimeMonth : UnaryFunctionOp<"datetime_month">;
def DateTimeDay : UnaryFunctionOp<"datetime_day">;
def DateTimeHour : UnaryFunctionOp<"datetime_hour">;
def DateTimeMinute : UnaryFunctionOp<"datetime_minute">;
def DateTimeSecond : UnaryFunctionOp<"datetime_second">;
def DateTimeMillisecond : UnaryFunctionOp<"datetime_millisecond">;
def DateTimeMicrosecond : UnaryFunctionOp<"datetime_microsecond">;

def CurrentDateTime : TuringOp<"current_datetime", [Pure, ConstantThroughOperands]> {
  let summary = "The instant the query runs, as a constant column";
  let description = [{
      What datetime() emits. The instant is read when the program is translated rather
      than carried in an attribute, so the same query prints the same IR on every run.
      Reading no operand makes it constant through them, which is what lays one instant
      over every row instead of one per row.

        %now = db.current_datetime() : !db.column<!storage.datetime>
  }];

  let arguments = (ins);
  let results   = (outs Column:$result);

  let assemblyFormat = "`(` `)` attr-dict `:` type($result)";
}

// The list family. Each reads a whole !storage.list cell and answers per row: db.size the
// element count as an integer, db.tail the same list one element shorter - a subspan of
// the cell, so the row still costs O(1) - and db.head and db.last the list's first and
// final element. A stored list may mix types, so those two answer with the type-erased
// !storage.list_element every element of a heterogeneous list rides, and an empty list
// reads as the null such a cell carries itself rather than as a nullable column of its own.
// The ID of a node, an edge, or the entity a type-erased cell holds, as an integer.
def ElementID : UnaryFunctionOp<"element_id">;

def Size : UnaryFunctionOp<"size">;
def Head : UnaryFunctionOp<"head">;
def Last : UnaryFunctionOp<"last">;
def Tail : UnaryFunctionOp<"tail">;

class BinaryFunctionOp<string mnemonic> : TuringOp<mnemonic, [Pure, ConstantThroughOperands]> {
  let summary = "Apply a binary function row-wise to two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $lhs `,` $rhs `)` attr-dict `:` functional-type(operands, results)
  }];
}

def CosineSimilarity : BinaryFunctionOp<"cosine_similarity">;
def EuclideanDistance : BinaryFunctionOp<"euclidean_distance">;

def ConstantOp : TuringOp<"constant", [Pure, ConstantLike, DeclareOpInterfaceMethods<InferTypeOpInterface>]> {
  let summary = "Produces a constant value in a column";

  // A typed attribute for a scalar, a dense f32 array for an embedding, an array of
  // per-element attributes for a list literal, or a dictionary for map literals.
  // Neither array carries a type of its own, so the column's element type is what
  // inferReturnTypes reads off them: an embedding for the dense array, the homogeneity
  // verdict over the elements for the list - which is why a list needs no separate op.
  let arguments = (ins AnyAttrOf<[TypedAttrInterface, DenseF32ArrayAttr, ArrayAttr, DictionaryAttr]>: $value);
  let results = (outs Column : $result);

  let assemblyFormat = "`(` $value `)` attr-dict";
}

def BroadcastConstant : TuringOp<"broadcast_constant", [Pure]> {
  let summary = "Lay a constant column's single value out over the rows of a relation";
  let description = [{
    A constant column holds one value standing for every row rather than one per row,
    so nothing shaped by rows can read it: a filter has no row of it to drop, a dedup
    no two rows to tell apart, a count no rows to tally. This binds that value to the
    rows of the relation the driver column carries, giving a column that carries rows
    like any other:

      %p = db.scan_nodes() : !db.column<!storage.node_id>
      %c = db.constant(1 : i64) : !db.column<i64>
      %x = db.broadcast_constant(%c, %p) : (!db.column<i64>, !db.column<!storage.node_id>) -> !db.column<none>

    It is what a WITH publishing constants alone emits: `MATCH (p:Person) WITH 1 AS x`
    publishes one row per Person, not the single row the literal is, so the statements
    after the barrier - a WHERE cutting those rows, a count(*) tallying them, a pattern
    crossed with them - read a column that has them.

    The driver is absent when no relation drives the scope, which is then the single row
    the constants are - `WITH 1 AS x WHERE x > 100`, whose one row the filter drops - so
    the value is laid out over that row:

      %x = db.broadcast_constant(%c) : (!db.column<i64>) -> !db.column<none>

    The result element type is resolved during lowering, as for the arithmetic ops:
    the rows are laid out as a nullable value column, present in every row.
  }];

  // The driver is the only optional operand, so MLIR tells it from the value without
  // operand-segment sizes, as nl.broadcast_constant's cardinality is.
  let arguments = (ins Column:$value, Optional<Column>:$driver);
  let results = (outs Column:$result);

  let assemblyFormat = [{
    `(` $value (`,` $driver^)? `)` attr-dict `:` functional-type(operands, results)
  }];
}

class PredicateOp<string mnemonic, list<Trait> traits = []>
    : TuringOp<mnemonic, !listconcat(traits, [ConstantThroughOperands])>,
      Arguments<(ins Column:$lhs, Column:$rhs)> {
  let results = (outs ColumnBools:$result);
  let assemblyFormat = "$lhs`,` $rhs attr-dict `:` functional-type(operands, results)";
}

def AndOp : PredicateOp<"and", [Pure]> {
  let summary = "Performs row-wise logical AND on its two input columns";
  let arguments = (ins Column : $lhs, Column : $rhs);
}

def OrOp : PredicateOp<"or", [Pure]> {
  let summary = "Performs row-wise logical OR on its two input columns";
  let arguments = (ins Column : $lhs, Column : $rhs);
}

def XorOp : PredicateOp<"xor", [Pure]> {
  let summary = "Performs row-wise logical XOR on its two input boolean columns";
  let arguments = (ins Column : $lhs, Column : $rhs);
}

def NotOp : TuringOp<"not", [Pure, ConstantThroughOperands]> {
  let summary = "Row-wise logical NOT of a boolean column";
  let arguments = (ins Column:$operand);
  let results   = (outs ColumnBools:$result);
  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def EqOp : PredicateOp<"eq", [Pure]> {
  let summary = "Row-wise equality of two value columns";
  let description = [{
      Result is always an abstraction over a (possibly nullable) mask (bool vector).

      %r = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<bool>
  }];
}

def NeqOp : PredicateOp<"neq", [Pure]> {
  let summary = "Row-wise inequality of two value columns";
}

def AddOp : BinaryOp<"add", [Pure]> {
  let summary = "Row-wise addition of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def ConcatOp : BinaryOp<"concat", [Pure]> {
  let summary = "Row-wise concatenation of two string or two list columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def ListIndex : BinaryOp<"list_index", [Pure]> {
  let summary = "Row-wise read of one element of a list column, by position";

  let description = [{
    `[1, 3, 4][2]`

      %e = db.list_index %xs, %i : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  }];

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def SubOp : BinaryOp<"sub", [Pure]> {
  let summary = "Row-wise subtraction of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def MulOp : BinaryOp<"mul", [Pure]> {
  let summary = "Row-wise multiplication of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def DivOp : BinaryOp<"div", [Pure]> {
  let summary = "Row-wise division of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def ModOp : BinaryOp<"mod", [Pure]> {
  let summary = "Row-wise modulo of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def PowOp : BinaryOp<"pow", [Pure]> {
  let summary = "Row-wise exponentiation of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def GtOp : PredicateOp<"gt", [Pure]> {
  let summary = "Row-wise greater-than comparison of two value columns";
}

def LtOp : PredicateOp<"lt", [Pure]> {
  let summary = "Row-wise less-than comparison of two value columns";
}

def GteOp : PredicateOp<"gte", [Pure]> {
  let summary = "Row-wise greater-than-or-equal comparison of two value columns";
}

def LteOp : PredicateOp<"lte", [Pure]> {
  let summary = "Row-wise less-than-or-equal comparison of two value columns";
}

def StartsWithOp : PredicateOp<"starts_with", [Pure]> {
  let summary = "Row-wise test of whether each string column value starts with the given prefix";
}

def EndsWithOp : PredicateOp<"ends_with", [Pure]> {
  let summary = "Row-wise test of whether each string column value ends with the given suffix";
}

def ContainsOp : PredicateOp<"contains", [Pure]> {
  let summary = "Row-wise test of whether each string column value contains the given substring";
}

def InOp : PredicateOp<"in", [Pure]> {
  let summary = "Row-wise test of whether each value is an element of the given list";
}

def Case : TuringOp<"case", [Pure, AttrSizedOperandSegments]> {
  let summary = "Row-wise selection of the first branch whose condition holds";

  let description = [{
    A Cypher CASE. `conditions` and `values` are parallel: a row takes the value of the
    first branch whose condition is true, the default when none is, and null when there
    is no default.

      %r = db.case({%c0, %c1}, {%v0, %v1}, %d)
             : (!db.column<!storage.bool>, !db.column<!storage.bool>,
                !db.column<i64>, !db.column<i64>, !db.column<i64>) -> !db.column<none>

    The simple form `CASE s WHEN v THEN ...` is emitted as the generic one, with a db.eq
    of the subject against each WHEN value as the condition.

    The result element type is resolved during lowering, as for the arithmetic ops: the
    branches promote against each other into one nullable value column, or - when they are
    all nodes, or all edges - into the entity column they already are, which carries the
    null of an unmatched row in its ID. A selection walks rows, so - unlike the arithmetic
    ops - it is not constant through its operands: the constants among them are laid out
    over the driving relation during lowering.
  }];

  let arguments = (ins Variadic<Column>:$conditions,
                       Variadic<Column>:$values,
                       Optional<Column>:$defaultValue);
  let results = (outs Column:$result);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` `{` $conditions `}` `,` `{` $values `}` (`,` $defaultValue^)? `)`
    attr-dict `:` functional-type(operands, results)
  }];
}

def FilterOp : TuringOp<"filter", [Pure]> {
  let summary = "Keep only the rows where the mask is true, filtering carried columns alongside";

  let description = [{
    Applies a boolean mask to a carry set of columns.

    %a2, %b2 = db.filter(%mask, {%a, %b})
  }];

  let arguments = (ins Column:$mask, Variadic<Column>:$columns_to_filter);

  let results   = (outs Variadic<Column>:$filtered_columns);

  let assemblyFormat = [{
    `(` $mask `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

def CreateNode : TuringOp<"create_node", [AttrSizedOperandSegments]> {
  let summary = "Create one node per input row, with given labels and property values";

  let description = [{
    `CREATE (n:Person{name:"Alice"})`
      %name = db.constant("Alice" : !db.column<...>)
      %n    = db.create_node (["Person"], ["name"], {%name})
                : (!db.column<none>) -> !db.column<!storage.node_id>

    `MATCH ... CREATE`,
      %a    = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %name = db.get_node_properties(%a, "name") : ... -> !db.column<none>
      %b    = db.create_node (["Bot"], ["name"], {%name})
                : (!db.column<none>) -> !db.column<!storage.node_id>
  }];

  let arguments = (ins StrArrayAttr:$labels,
                       StrArrayAttr:$prop_names,
                       Variadic<Column>:$prop_values,
                       Optional<Column>:$cardinality);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `,` $prop_names `,` `{` $prop_values `}` `)` ( `foreach` $cardinality^ )?
    attr-dict `:` functional-type(operands, results)
  }];
}

def CreateEdge : TuringOp<"create_edge", [AttrSizedOperandSegments]> {
  let summary = "Create one edge per input row between corresponding source and target nodes";

  let description = [{
    `CREATE (a:Person)-[e:KNOWS]->(b:Person)`
      %n_a = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
      %n_b = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
      %e   = db.create_edge (%n_a, %n_b, "KNOWS", [], {})
               : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
               -> !db.column<!storage.edge_id>

    `MATCH (a) CREATE (a)-[:KNOWS]->(b:Bot)`
      %a   = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %b   = db.create_node (["Bot"], [], {}) : () -> !db.column<!storage.node_id>
      %e   = db.create_edge (%a, %b, "KNOWS", [], {})
               : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
               -> !db.column<!storage.edge_id>
  }];

  let arguments = (ins Column:$src_ids,
                       Column:$tgt_ids,
                       StrAttr:$edge_type,
                       StrArrayAttr:$prop_names,
                       Variadic<Column>:$prop_values,
                       Optional<Column>:$src_pending,
                       Optional<Column>:$tgt_pending,
                       UnitAttr:$src_all_pending,
                       UnitAttr:$tgt_all_pending);
  let results   = (outs ColumnEdgeIDs:$result);
  let hasVerifier = 1;

  // The two optional masks say, row by row, which endpoints are entities this change
  // wrote and has not committed - what a db.merge's node column mixes. A column a
  // db.create_node produced is pending in every row and says so with the matching
  // all_pending flag; one a scan or a traversal bound is pending in none and carries
  // neither a mask nor a flag.
  let assemblyFormat = [{
    `(` $src_ids `,` $tgt_ids `,` $edge_type `,` $prop_names `,` `{` $prop_values `}` `)`
    (`src_pending` $src_pending^)? (`tgt_pending` $tgt_pending^)?
    (`src_all_pending` $src_all_pending^)? (`tgt_all_pending` $tgt_all_pending^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

def Merge : TuringOp<"merge", [AttrSizedOperandSegments]> {
  let summary = "Bind one MERGE path pattern per input row, writing it when the graph does not hold it";

  let description = [{
    The declarative counterpart of Cypher's MERGE. The pattern is one path - a chain
    of nodes joined by hops - and it is matched and written as a unit: for each input
    row the op looks the whole chain up, emits one row per match it finds, and only
    when it finds none writes the entities the pattern does not already have bound
    and emits that one row. That whole-pattern rule is what keeps MERGE from being a
    sequence of per-entity merges: `MERGE (a:P {n: 1})-[:K]->(b:P {n: 2})` writes a
    fresh `a` and `b` when no such hop exists, even when nodes matching each end do.

    The chain is spelled out by the attributes, one entry per entity in pattern order:

      %n, %n_pending, %created = db.merge nodes [["Person"]] props [["name"]]
                                         edges [] props [] dirs []
                                         bound {} pending {} values {%name} {} carrying {}
        : (!db.column<!storage.string>)
          -> (!db.column<!storage.node_id>, !db.column<!storage.bool>, !db.column<!storage.bool>)

    `node_labels` carries each chain node's label set. An empty one marks a node the
    query already bound - `MATCH (a) MERGE (a)-[:K]->(b:Bot)` - whose column comes in
    through `bound_nodes` instead, in chain order; every other node is matched by its
    labels (a superset match, as db.scan_nodes_by_label) and its `node_prop_names`
    values, and written with exactly those when the pattern is missing. A node the
    op writes needs a label, so a node with neither labels nor a bound column is
    rejected.

    `bound_pending` is empty, or holds one boolean column per bound node saying which
    of its rows are entities this change has written but not committed - what an
    earlier db.merge or db.create_node produced. Such a row is in no graph the match
    reads, so the pattern under it can never be found and is written instead.

    `edge_types`, `edge_prop_names` and `edge_directions` carry one entry per hop,
    each hop joining the chain node ahead of it to the one behind. A hop's direction
    is read from the query; `undirected` matches a hop either way round and writes a
    forward one.

    The results pair each entity the op looks up with a boolean mask, nodes in chain
    order then hops: the entity's ID column, then the rows of it that are pending -
    written by this change and not committed, so an offset into the write buffer rather
    than an ID the graph holds. A bound node has no pair of its own: its rows came in
    through `bound_nodes` and come back through the carry set, re-indexed like every
    other column in flight. After those pairs comes the boolean column saying which rows
    the op wrote the whole pattern for - what ON CREATE and ON MATCH select on - then the
    carry set.
    `carried_columns` mirrors db.get_out_edges': the columns in flight, handed back
    re-indexed onto the rows the merge emitted, so a row the op emitted twice repeats
    them and one it emitted nothing for drops them.
  }];

  let arguments = (ins StrArrayArrayAttr:$node_labels,
                       StrArrayArrayAttr:$node_prop_names,
                       StrArrayAttr:$edge_types,
                       StrArrayArrayAttr:$edge_prop_names,
                       DenseI64ArrayAttr:$edge_directions,
                       DenseI64ArrayAttr:$pending_nodes,
                       Variadic<Column>:$bound_nodes,
                       Variadic<Column>:$bound_pending,
                       Variadic<Column>:$node_prop_values,
                       Variadic<Column>:$edge_prop_values,
                       Variadic<Column>:$carried_columns);

  let results = (outs Variadic<Column>:$results);

  let hasVerifier = 1;

  // The five attribute lists print ahead of the operands, each under the keyword
  // naming what it describes, so a chain reads left to right. The directions go
  // through the shared custom directive so they print as keywords rather than as
  // the raw integers the DenseI64ArrayAttr stores.
  let assemblyFormat = [{
    `nodes` $node_labels `props` $node_prop_names
    `edges` $edge_types `props` $edge_prop_names `dirs` custom<EdgeDirections>($edge_directions)
    `bound` `{` $bound_nodes `}` `pending` custom<ColumnIndices>($pending_nodes) `{` $bound_pending `}`
    `values` `{` $node_prop_values `}` `{` $edge_prop_values `}`
    `carrying` `{` $carried_columns `}`
    attr-dict `:` functional-type(operands, results)
  }];
}

def SetNodeProperty : TuringOp<"set_node_property", [AttrSizedOperandSegments]> {
  let summary = "Set one named property on each input node to a row-aligned value";

  let description = [{
      %n   = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %val = db.constant(5 : i64)
      db.set_node_property(%n, "age", %val)
        : (!db.column<!storage.node_id>, !db.column<i64>) -> ()

    Node variable keeps its existing column.
  }];

  let arguments = (ins Column:$input_nodes,
                       StrAttr:$property,
                       Column:$value,
                       Optional<Column>:$pending,
                       Optional<Column>:$rows,
                       UnitAttr:$all_pending);

  // `pending` says, row by row, which entities this change wrote and has not committed -
  // those are written in the write buffer rather than recorded as an update to a
  // committed entity, and `all_pending` says it of every row without a mask. `rows`
  // narrows which rows are written at all: Cypher's ON CREATE passes the merge's created
  // mask, ON MATCH its negation, and a plain SET passes none.
  let assemblyFormat = [{
    `(` $input_nodes `,` $property `,` $value `)` (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def SetEdgeProperty : TuringOp<"set_edge_property", [AttrSizedOperandSegments]> {
  let summary = "Set one named property on each input edge to a row-aligned value";

  let description = [{
      db.set_edge_property(%e, "weight", %val)
        : (!db.column<!storage.edge_id>, !db.column<f64>) -> ()

    Edge variable keeps its existing column.
  }];

  let arguments = (ins Column:$input_edges,
                       StrAttr:$property,
                       Column:$value,
                       Optional<Column>:$pending,
                       Optional<Column>:$rows,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `,` $value `)` (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def DeleteNode : TuringOp<"delete_node"> {
  let summary = "Delete each input node, optionally detaching its incident edges";

  let description = [{
    `detach` carries the DETACH keyword

      db.delete_node(%n) detach : (!db.column<!storage.node_id>) -> ()

    `pending` is the mask a db.merge produced beside its node column: those rows
    hold an offset into the change's write buffer rather than an ID the graph
    knows, so the change drops the write instead of tombstoning an ID.
    `all_pending` says that of every row, which a db.create_node's column is.
  }];

  let arguments = (ins Column:$input_nodes,
                       UnitAttr:$detach,
                       Optional<Column>:$pending,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    `(` $input_nodes `)` (`detach` $detach^)? (`pending` $pending^)?
    (`all_pending` $all_pending^)? attr-dict `:` functional-type(operands, results)
  }];
}

def DeleteEdge : TuringOp<"delete_edge"> {
  let summary = "Delete each input edge";

  let description = [{
      db.delete_edge(%e) : (!db.column<!storage.edge_id>) -> ()

    `pending` is delete_node's mask, over edges, and `all_pending` its maskless form.
  }];

  let arguments = (ins Column:$input_edges, Optional<Column>:$pending, UnitAttr:$all_pending);

  let assemblyFormat = [{
    `(` $input_edges `)` (`pending` $pending^)? (`all_pending` $all_pending^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GroupAggregate
*/
def GroupAggregate : TuringOp<"group_aggregate", [Pure]> {
  let summary = "Group rows by a key tuple and reduce each group to one row";
  let description = [{
    The declarative counterpart of a Cypher aggregating RETURN / WITH with
    grouping keys - `MATCH ... RETURN k1, k2, agg(x), ...`. It consumes the whole
    dataflow of its columns and emits one row per distinct grouping-key tuple: the
    key values followed by each aggregate's reduction over the rows of that group.

    The operands are the grouping-key columns first, then the aggregate-input
    columns; `keys` is how many leading operands are keys (so the rest are the
    aggregate inputs, one per aggregate). `aggregates` names the reduction of each
    aggregate input as GroupAggregateKind keywords (count, count_distinct, sum,
    sum_distinct, min, max, avg, avg_distinct), so its length is the number of
    aggregate inputs. The distinct kinds are Cypher's DISTINCT aggregates: each
    reduces a group's distinct non-null values rather than all of its rows, and keeps
    the result type of the kind it mirrors. min/max have no distinct kind, since
    dropping repeated values cannot move an extremum.

      %a     = db.scan_nodes() : !db.column<!storage.node_id>
      %city  = db.get_node_properties(%a, "city")  : (!db.column<!storage.node_id>) -> !db.column<none>
      %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
      %gcity, %total = db.group_aggregate(%city, %score) keys 1 aggregates [sum]
                         : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)
      db.output(%gcity, %total) : !db.column<none>, !db.column<none>

    The results are the grouping-key columns (in operand order, same types) then
    one column per aggregate: count and count_distinct are ui64 counts, sum/min/max
    keep the input's value type and avg widens to f64 - all resolved during lowering,
    the distinct kinds exactly as the kinds they mirror,
    so - like db.count - the aggregate result types are unconstrained here. count(*)
    is expressed by a count over a never-null column (node IDs).

    Grouped aggregation is a pipeline breaker: it must see every row before it can
    emit any, so - like db.sort - it lowers to a hoisted accumulator
    (nl.group_aggregate_buffer), an nl.group_aggregate_update in the producing loop
    that folds each chunk into the per-group state, and an nl.group_aggregate source
    iterator after the loop whose nl.for emits the group rows in chunks. Reducing a
    deterministic read of the graph is itself deterministic, so the op is Pure.
  }];

  // The grouping-key columns then the aggregate-input columns. keyCount is how
  // many leading columns are keys; kinds names each aggregate's reduction, one per
  // trailing column. Both counts are unsigned - never negative - so a negative
  // literal fails to parse rather than reaching the verifier.
  let arguments = (ins Variadic<Column>:$columns,
                       UI64Attr:$keyCount,
                       DenseI64ArrayAttr:$kinds);
  let results   = (outs Variadic<Column>:$results);

  // One result per key column (same type) then one per aggregate; functional-type
  // prints the `(operandTypes) -> resultTypes` form. keyCount prints after `keys`
  // and the kinds after `aggregates`, ahead of the colon and elided from attr-dict
  // since the format names them - the same shape as db.sort. The kinds print as
  // their GroupAggregateKind keywords (`[count, sum, max]`, not `[0, 1, 3]`) through
  // the shared custom<GroupAggregateKinds> directive.
  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyCount `aggregates` custom<GroupAggregateKinds>($kinds) attr-dict `:`
    functional-type(operands, results)
  }];

  // keyCount and kinds must partition the columns, at least one of each, the key
  // results must pass their key columns through, and every kind must be a valid
  // GroupAggregateKind.
  let hasVerifier = 1;
}

/**
* Collect
*/
def Collect : TuringOp<"collect", [Pure]> {
  let summary = "Group rows by a key tuple and collect one column per group into a list";
  let description = [{
    Cypher `WITH k..., collect(x) AS xs`. Emits one row per distinct grouping-key
    tuple: the key values then a list of the group's non-null collected values.

      %gcity, %names = db.collect(%city, %name) keys 1
                         : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)

    keyCount is how many leading columns are keys (0 = ungrouped); the collected value
    columns follow, one per list the projection returns, and then one input per aggregate
    `aggregates` names - the reductions taken over the same groups, as db.group_aggregate
    takes them. Results are the keys (unchanged), one list column per value, then one
    column per aggregate. The value count is what is left over: columns minus the keys
    and the aggregate inputs.

      %gcity, %names, %n = db.collect(%city, %name, %score) keys 1 aggregates [count]
                             : (...) -> (...)

      %names, %ages = db.collect(%name, %age) keys 0
                        : (...) -> (!db.column<!storage.list<none>>, !db.column<!storage.list<none>>)

    `distinct` is Cypher's collect(DISTINCT x): a value repeated in a group is collected
    once, so the list holds each of the group's distinct values. It names the value
    columns that dedupe by index, since one projection may collect a column both ways.

      %names = db.collect(%name) keys 0 distinct [0] : (!db.column<none>) -> !db.column<!storage.list<none>>
  }];

  // keyCount grouping-key columns, one or more collected columns, then one input per
  // aggregate kinds names; keyCount is unsigned (never negative).
  let arguments = (ins Variadic<Column>:$columns,
                       UI64Attr:$keyCount,
                       OptionalAttr<DenseI64ArrayAttr>:$kinds,
                       OptionalAttr<DenseI64ArrayAttr>:$distinctValues);
  let results   = (outs Variadic<Column>:$results);

  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyCount (`aggregates` custom<GroupAggregateKinds>($kinds)^)?
    (`distinct` custom<ColumnIndices>($distinctValues)^)? attr-dict `:` functional-type(operands, results)
  }];

  // At least one value column after the keys; key results pass through; one list result
  // per value column.
  let hasVerifier = 1;
}

/**
* UnwindCollect
*/
def UnwindCollect : TuringOp<"unwind_collect", [Pure]> {
  let summary = "Group rows by a key tuple, collect one column per group, and re-emit one row per collected element";
  let description = [{
    Cypher `WITH k..., collect(x) AS xs UNWIND xs AS y` where xs feeds only the unwind -
    the fused form, so the list never materializes. Emits one row per collected value:
    the key values (repeated) then that value.

      %gcity, %y = db.unwind_collect(%city, %name) keys 1
                     : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)

    Same operands as db.collect (keyCount keys, one value column); results are the keys
    (unchanged) then one scalar value column of the unwound elements.
  }];

  let arguments = (ins Variadic<Column>:$columns, UI64Attr:$keyCount);
  let results   = (outs Variadic<Column>:$results);

  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyCount attr-dict `:` functional-type(operands, results)
  }];

  // keyCount + 1 columns; key results pass through; trailing result is the value column.
  let hasVerifier = 1;
}

/**
* ShortestPath
*/
def ShortestPath : TuringOp<"shortest_path", [Pure]> {
  let summary = "Weighted shortest path from a source node set to a target node set";
  let description = [{
    TuringDB's `SHORTESTPATH(source, target, edgeProperty, distVar, pathVar)`: the
    minimum-weight directed path from any source node to any target node

      %dist, %path = db.shortest_path(%a, %b) weight "cost"
                       : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
                         -> (!db.column<none>, !db.column<!storage.path>)

    Pipeline breaker as both node columns are consumed in full. The distance column
    is typed `none` here and resolved to the weight property's value type at lowering.
  }];

  let arguments = (ins ColumnNodeIDs:$source, ColumnNodeIDs:$target, StrAttr:$edgeProperty);
  let results   = (outs Column:$distance, ColumnPath:$path);

  let assemblyFormat = [{
    `(` $source `,` $target `)` `weight` $edgeProperty attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* OptionalMatch
*/
def OptionalMatch : TuringOp<"optional_match", [Pure]> {
  let summary = "Match a pattern against the rows in flight, keeping the rows it does not match";

  let description = [{
    Models Cypher's OPTIONAL MATCH: a left outer join of the rows in flight
    against a pattern. A row the pattern matches fans out exactly as a MATCH
    would; a row it matches not at all survives once, with the pattern's own
    variables null. The pattern matches whole or not at all - a second hop that
    finds nothing nulls the first hop's variables too - so it cannot be modelled
    by making each traversal optional, and lives in a region of its own.

    `input_columns` are the columns the pattern joins onto - every named column
    the query holds at this point. The region's entry block takes one argument
    per input column, so the pattern walks the rows in flight rather than the
    whole graph, plus a trailing `!db.column<ui64>` row tag: the position of each
    input row, carried through the pattern the way any other column is, so the
    rows that matched are known once the pattern is walked. A query opening on
    OPTIONAL MATCH has no input column and so no tag either - the rows it joins
    onto are the single empty row Cypher starts from, which any match marks.

      %p2, %e, %f = db.optional_match(%p) ({
        ^bb0(%pIn: !db.column<!storage.node_id>, %tag: !db.column<ui64>):
          %s, %e, %et, %t, %tag2 = db.get_out_edges(%pIn, {%tag}) : ...
          db.optional_yield %tag2, {%s, %e, %t} : !db.column<ui64>, {...}
      }) : (!db.column<!storage.node_id>) -> (...)

    The db.optional_yield names the tag as the pattern left it, then the columns
    the region contributes: one per input column first, in operand order - these
    are the input rows as the pattern filtered and repeated them - then one per
    variable the pattern binds of its own. The op's results line up with those
    columns, so the results past the input count are the pattern's variables, and
    those are the ones a row the pattern missed comes back with null. A null node
    or edge is an invalid ID (`ID::isValid()`), which reads as null through a
    property fetch and renders as null, so no column type changes here.

    Reading the graph is deterministic and the op writes nothing, so it is Pure -
    which says nothing about the row count, and this op changes it.
  }];

  let arguments = (ins Variadic<Column>:$input_columns);

  // One per column the region yields: the input columns as the pattern left
  // them, then the variables the pattern binds.
  let results = (outs Variadic<Column>:$results);

  // Exactly one block, ending in a db.optional_yield.
  let regions = (region SizedRegion<1>:$pattern);

  let assemblyFormat = [{
    `(` $input_columns `)` $pattern attr-dict `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* OptionalYield
*/
def OptionalYield : TuringOp<"optional_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"OptionalMatch">]> {
  let summary = "Name the row tag and the columns a db.optional_match's pattern contributes";

  let description = [{
    Terminates a db.optional_match's pattern region. `tag` is the row tag as the
    pattern left it - the input row each matched row came from, absent when the
    op has no input column - and `columns` are the columns the region
    contributes, one per input column in operand order followed by the pattern's
    own variables. The enclosing op's results are exactly those columns.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  // The tag's type is buildable and so left out; only the contributed column types
  // are spelled, in braces as db.cross_product spells a factor's.
  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

/**
* CallSubquery
*/
def CallSubquery : TuringOp<"call_subquery", []> {
  let summary = "Run a query of its own once per row in flight";

  let description = [{
    Models Cypher's CALL { ... } subquery. `input_columns` are every column the
    query holds at this point, and the body region takes one argument per input
    column - the imported ones read through their own names, the rest carried past
    the body - plus a trailing `!db.column<ui64>` row tag when the op is `optional`
    and the body carries its scope over input rows it has, as db.optional_match
    takes one. The body is a self-contained db dataflow ending in a
    db.subquery_yield.

    The body runs once per input row. For each row a returning body yields, the op
    produces that row's input columns beside the yielded ones: the results are one
    per input column, in operand order, then one per yielded column. A body the row
    yields nothing for drops the row, unless the op is `optional`, which keeps it
    once with the yielded columns null.

      %p2, %x = db.call_subquery(%p) carries_scope ({
        ^bb0(%pIn: !db.column<!storage.node_id>):
          %s, %e, %et, %x = db.get_out_edges(%pIn, {}) : ...
          db.subquery_yield %s, %x : !db.column<!storage.node_id>, !db.column<!storage.node_id>
      }) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)

    `carries_scope` says the body's own dataflow keeps every input row paired with
    what the body made of it, so it runs over a whole chunk of input rows at once. A
    returning body that carries the scope hands the input columns back itself: it
    yields them first, as its dataflow left them - filtered and repeated by its hops -
    then its own columns, and the results line up with that yield. A body whose rows
    cannot carry the inputs through - one that aggregates, sorts, cuts or dedups - is
    run one input row at a time instead: a returning one yields its own columns alone
    and the lowering pairs each row it yields with the input row it came from, a unit
    one writes over one input row per step.

    A `unit` body ends on an updating clause rather than a RETURN. It yields nothing
    and the op has no results, whether or not it carries the scope: the rows in flight
    are the operands still, however many rows the body walked or wrote for each of
    them. A unit body is never `optional` - it yields no row for OPTIONAL to pad.

    The body may write to the graph, so the op is not Pure.
  }];

  let arguments = (ins Variadic<Column>:$input_columns,
                       UnitAttr:$unit,
                       UnitAttr:$carries_scope,
                       UnitAttr:$optional);

  let results = (outs Variadic<Column>:$results);

  // Exactly one block, ending in a db.subquery_yield.
  let regions = (region SizedRegion<1>:$body);

  let assemblyFormat = [{
    `(` $input_columns `)` (`unit` $unit^)? (`carries_scope` $carries_scope^)? (`optional` $optional^)?
    $body attr-dict `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* SubqueryYield
*/
def SubqueryYield : TuringOp<"subquery_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"CallSubquery">]> {
  let summary = "Name the row tag and the columns a db.call_subquery's body contributes";

  let description = [{
    Terminates a db.call_subquery's body region. `tag` is the row tag as the body
    left it, present exactly when the body takes one, and `columns` are the columns
    the body contributes: the input columns as the body left them when the op
    carries its scope, then the columns the body's RETURN publishes. A unit body
    yields no column.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

/**
* ExistsSubquery
*/
def ExistsSubquery : TuringOp<"exists_subquery", [Pure]> {
  let summary = "Answer, per row in flight, whether a body produces a row of its own for it";

  let description = [{
    Models Cypher's `EXISTS { ... }`: a semi-join of the rows in flight against a
    read-only body. The result is one boolean per input row - true exactly when the
    body produced at least one row for it - so the op changes no row count, which is
    what lets it stand in an expression.

    `input_columns` are every named column the query holds at this point, and the body
    region takes one argument per input column - the body reads the scope around it
    through them, EXISTS being correlated - plus a trailing `!db.column<ui64>` row tag:
    the position of each input row, carried through the body the way any other column
    is, so the rows that produced one are known once the body is walked. An EXISTS over
    no input column joins onto the single empty row Cypher starts from and takes no tag.

      %b = db.exists_subquery(%p) carries_scope ({
        ^bb0(%pIn: !db.column<!storage.node_id>, %tag: !db.column<ui64>):
          %s, %e, %et, %k, %tag2 = db.get_out_edges(%pIn, {%tag}) : ...
          db.exists_yield %tag2, {%s, %e, %k} : {...}
      }) : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>

    `carries_scope` says the body's own dataflow keeps every input row paired with what
    it made of it, so it runs over a whole chunk of input rows at once and the tag comes
    back naming the rows that survived. A body that aggregates, sorts, cuts or dedups
    cannot: it is run one input row at a time instead, and the row is marked when the
    body yielded anything at all for it.

    The db.exists_yield names the tag as the body left it and the columns the body holds
    there. Those columns say nothing about which rows matched when the tag is present;
    they carry the row count for a body with no tag to answer from.

    Reading the graph is deterministic and an EXISTS body writes nothing, so it is Pure.
  }];

  let arguments = (ins Variadic<Column>:$input_columns, UnitAttr:$carries_scope);

  let results = (outs ColumnBools:$result);

  // Exactly one block, ending in a db.exists_yield.
  let regions = (region SizedRegion<1>:$body);

  let assemblyFormat = [{
    `(` $input_columns `)` (`carries_scope` $carries_scope^)? $body attr-dict
    `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* ExistsYield
*/
def ExistsYield : TuringOp<"exists_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"ExistsSubquery">]> {
  let summary = "Name the row tag and the columns a db.exists_subquery's body holds";

  let description = [{
    Terminates a db.exists_subquery's body region. `tag` is the row tag as the body left
    it - the input row each surviving row came from, absent when the op has no input
    column - and `columns` are the columns the body holds there, which a body with no tag
    reads its row count off.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  // The tag's type is buildable and so left out; only the held column types are spelled,
  // in braces as db.optional_yield spells the ones its pattern contributes.
  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

#endif //TURINGDB_OPS
