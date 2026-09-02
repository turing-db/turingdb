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
    coerces the literal to the stored type. A property absent from the schema is
    carried by no node, so the scan yields no rows - not an error, matching the
    empty result the filter form produces. `labels`, when present, is the label
    list db.scan_nodes_by_label carried, with the same superset semantics: a
    label absent from the schema likewise yields no rows.

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
  let summary = "Produce every edge of one type in the graph as four row-aligned columns";

  let description = [{
    The by-type sibling of db.scan_edges: where that produces every edge, this
    produces every edge of the named type, in the same four row-aligned columns -
    the source node, the edge, its type and the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_edges_by_type("KNOWS") : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    An edge carries exactly one type, so the filter is an equality on that type,
    not a set match. `edge_type` is the type name spelled the same as in the query,
    resolved against the graph schema; a name absent from the schema matches no
    edge, so the scan yields no rows. Like db.scan_edges it reads no column and
    takes no input, so it opens a dataflow rather than consuming one, and the
    result shape is identical - only the rows are narrowed.
  }];

  // `edge_type` is the symbolic relationship-type name. Being an attribute and
  // not an SSA value, the op still has no operands; lowering hoists it into an
  // nl.get_edge_type handle so the name is resolved once above the loops.
  let arguments = (ins StrAttr:$edge_type);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);

  // The type name prints inside the parens where db.scan_edges prints nothing;
  // the result types follow the colon, as on db.scan_edges, since a source op has
  // no meaningful operand arrow.
  let assemblyFormat = [{
    `(` $edge_type `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
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
  let summary = "Fetch the out-edges of one edge type, filtering carried columns alongside";

  let description = [{
    The by-type sibling of get_out_edges. In `MATCH (a)-[:KNOWS]->(b)` only the
    successors reached over a KNOWS edge count, so this walks each node's out-edges
    but keeps only those of the named type - the plain get_out_edges narrowed by
    the relationship type spelled in the query:

      %s, %e, %t, %b = get_out_edges_by_type(%a, "KNOWS", {})
      %c, %b2, %a2 = get_out_edges_by_type(%b, "LIKES", {%a})   // eids/etypes omitted

    An edge carries exactly one type, so the filter is an equality on that type,
    not a set match. `edge_type` is the type name spelled the same as in the query,
    resolved against the graph schema; a name absent from the schema matches no
    edge, so the hop yields no rows. `columns_to_filter` is the carry set, exactly
    as on get_out_edges: each one comes back in `filtered_columns`, and the result
    chunk shape - srcids, eids, etypes, tgtids - is identical, so a hop is
    interchangeable with the unfiltered fetch.
  }];

  // `edge_type` is the symbolic relationship-type name. Being an attribute and
  // not an SSA value, it is absent from `operands`; lowering hoists it into an
  // nl.get_edge_type handle so the name is resolved once above the loops.
  // `columns_to_filter` is the variadic carry set.
  let arguments = (ins Column:$input_nodes, StrAttr:$edge_type, Variadic<Column>:$columns_to_filter);

  // `filtered_columns`: one per carried column, after the four fixed results.
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  // The type name prints inside the parens as `"KNOWS"`, between input_nodes and
  // the brace-wrapped carry set; MLIR elides it from attr-dict because the format
  // already names it. functional-type spells only the operand and result types.
  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_type `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetInEdgesByType
*/
def GetInEdgesByType : TuringOp<"get_in_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Fetch the in-edges of one edge type, filtering carried columns alongside";

  let description = [{
    The reverse-direction sibling of get_out_edges_by_type: where that keeps a
    node's out-edges of the named type, this keeps its in-edges of that type. In
    `MATCH (a)<-[:KNOWS]-(b)` only the predecessors reached over a KNOWS edge
    count:

      %s, %e, %t, %b = get_in_edges_by_type(%a, "KNOWS", {})

    The result chunk shape - srcids, eids, etypes, tgtids - and the
    `columns_to_filter` carry set match get_out_edges_by_type, so the two are
    interchangeable in a hop; `edge_type` is resolved the same way and an absent
    name yields no rows.
  }];

  let arguments = (ins Column:$input_nodes, StrAttr:$edge_type, Variadic<Column>:$columns_to_filter);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_type `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
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
  }];

  // `property` is the symbolic property name, the same `name`/`age` spelled in
  // the query. Being an attribute and not an SSA value, it is absent from
  // `operands` below; lowering resolves it to a storage PropertyTypeID.
  let arguments = (ins Column:$input_nodes, StrAttr:$property);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, as get_out_edges does; the
  // string attribute prints inside the parens, ahead of the colon, and MLIR
  // elides it from attr-dict because the format already names it.
  let assemblyFormat = [{
    `(` $input_nodes `,` $property `)` attr-dict `:`
    functional-type(operands, results)
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
  }];

  let arguments = (ins Column:$input_edges, StrAttr:$property);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `)` attr-dict `:`
    functional-type(operands, results)
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
* Yield
*/
def Yield : TuringOp<"yield", [Pure, Terminator, HasParent<"CrossProduct">]> {
  let summary = "Name the columns a cross_product factor contributes to the result";

  let description = [{
    Terminates a db.cross_product factor region, naming the columns that factor
    contributes to the product. The cross_product's results are the left
    factor's yielded columns followed by the right factor's. A factor must yield
    at least one column - the cross_product verifier rejects an empty yield,
    since a side's row count is read from its first yielded column.
  }];

  let arguments = (ins Variadic<Column>:$columns);

  // The yielded column types vary from query to query and cannot be built from
  // a recipe, so - when present - they are spelled after the colon, as in
  // db.output. The optional group, anchored on the operands, keeps the syntax
  // total: an empty yield (`db.yield`) still parses, then the cross_product
  // verifier rejects it with a clear db-level diagnostic.
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
* RemoveDuplicates
*/
def RemoveDuplicates : TuringOp<"remove_duplicates", [Pure]> {
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
    stops once k distinct rows are found. Deduplicating a deterministic read of the
    graph is itself deterministic, so the op is Pure.
  }];

  // The columns flow through unchanged; every column is part of the dedup key, so
  // - unlike db.sort's keys - no key attribute is needed.
  let arguments = (ins Variadic<Column>:$columns);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form, the same as db.limit and db.sort.
  let assemblyFormat = [{
    `(` $columns `)` attr-dict `:` functional-type(operands, results)
  }];

  // results arity and types must equal the columns (the shared pass-through check).
  let hasVerifier = 1;
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

class UnaryFunctionOp<string mnemonic> : TuringOp<mnemonic, [Pure]> {
  let summary = "Apply a unary function row-wise to a value column";

  let arguments = (ins Column:$input);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input `)` attr-dict `:` functional-type(operands, results)
  }];
}

def Labels : UnaryFunctionOp<"labels">;
def EdgeType : UnaryFunctionOp<"edge_type">;
def ToInteger : UnaryFunctionOp<"to_integer">;
def ToFloat : UnaryFunctionOp<"to_float">;
def ToBoolean : UnaryFunctionOp<"to_boolean">;

class BinaryFunctionOp<string mnemonic> : TuringOp<mnemonic, [Pure]> {
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

  // A typed attribute for a scalar, a dense f32 array for an embedding, or an array of
  // per-element attributes for a list literal. Neither array carries a type of its own, so
  // the column's element type is what inferReturnTypes reads off them: an embedding for the
  // dense array, the homogeneity verdict over the elements for the list - which is why a
  // list needs no separate op.
  let arguments = (ins AnyAttrOf<[TypedAttrInterface, DenseF32ArrayAttr, ArrayAttr]>: $value);
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
  let summary = "Row-wise concatenation of two string columns";

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

def CreateEdge : TuringOp<"create_edge"> {
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
                       Variadic<Column>:$prop_values);
  let results   = (outs ColumnEdgeIDs:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $src_ids `,` $tgt_ids `,` $edge_type `,` $prop_names `,` `{` $prop_values `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

def SetNodeProperty : TuringOp<"set_node_property"> {
  let summary = "Set one named property on each input node to a row-aligned value";

  let description = [{
      %n   = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %val = db.constant(5 : i64)
      db.set_node_property(%n, "age", %val)
        : (!db.column<!storage.node_id>, !db.column<i64>) -> ()

    Node variable keeps its existing column.
  }];

  let arguments = (ins Column:$input_nodes, StrAttr:$property, Column:$value);

  let assemblyFormat = [{
    `(` $input_nodes `,` $property `,` $value `)` attr-dict `:`
    functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def SetEdgeProperty : TuringOp<"set_edge_property"> {
  let summary = "Set one named property on each input edge to a row-aligned value";

  let description = [{
      db.set_edge_property(%e, "weight", %val)
        : (!db.column<!storage.edge_id>, !db.column<f64>) -> ()

    Edge variable keeps its existing column.
  }];

  let arguments = (ins Column:$input_edges, StrAttr:$property, Column:$value);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `,` $value `)` attr-dict `:`
    functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def DeleteNode : TuringOp<"delete_node"> {
  let summary = "Delete each input node, optionally detaching its incident edges";

  let description = [{
    `detach` carries the DETACH keyword

      db.delete_node(%n) detach : (!db.column<!storage.node_id>) -> ()
  }];

  let arguments = (ins Column:$input_nodes, UnitAttr:$detach);

  let assemblyFormat = [{
    `(` $input_nodes `)` (`detach` $detach^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

def DeleteEdge : TuringOp<"delete_edge"> {
  let summary = "Delete each input edge";

  let description = [{
      db.delete_edge(%e) : (!db.column<!storage.edge_id>) -> ()
  }];

  let arguments = (ins Column:$input_edges);

  let assemblyFormat = [{
    `(` $input_edges `)` attr-dict `:` functional-type(operands, results)
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

#endif //TURINGDB_OPS
