#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"
include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "DBDialect.td"
include "DBTypes.td"

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
* GetOutEdgesByType
*/
def GetOutEdgesByType : TuringOp<"get_out_edges_by_type", [Pure]> {
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
def GetInEdgesByType : TuringOp<"get_in_edges_by_type", [Pure]> {
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
  }];
  let arguments = (ins Variadic<Column>:$columns);

  // The column types vary from query to query and cannot be built from a
  // recipe, so they are spelled after the colon, the same as nl.output.
  // qualified(...) forces the leading !db. on every printed type.
  let assemblyFormat = "`(` $columns `)` attr-dict `:` qualified(type($columns))";
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
  }];

  // One column in, one integer column out. The input and result types are
  // unrelated (an integer count of any column), so - unlike the pass-through ops -
  // there is nothing to cross-check and no verifier.
  let arguments = (ins Column:$input);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, the same as db.get_node_properties.
  let assemblyFormat = [{
    `(` $input `)` attr-dict `:` functional-type(operands, results)
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
class AggregateOp<string mnemonic> : TuringOp<mnemonic, [Pure]> {
  let summary = "Reduce a column of property values to a single-row result";

  let arguments = (ins Column:$input);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, the same as db.count.
  let assemblyFormat = [{
    `(` $input `)` attr-dict `:` functional-type(operands, results)
  }];
}

def Sum : AggregateOp<"sum">;
def Min : AggregateOp<"min">;
def Max : AggregateOp<"max">;
def Avg : AggregateOp<"avg">;

def ConstantOp : TuringOp<"constant", [Pure, ConstantLike, DeclareOpInterfaceMethods<InferTypeOpInterface>]> {
  let summary = "Produces a constant value in a column";

  let arguments = (ins TypedAttrInterface: $value);
  let results = (outs Column : $result);

  let assemblyFormat = "`(` $value `)` attr-dict";
}

class PredicateOp<string mnemonic, list<Trait> traits = []>
    : TuringOp<mnemonic, traits>,
      Arguments<(ins Column:$lhs, Column:$rhs)> {
  let results = (outs ColumnBools:$result);
  let assemblyFormat = "$lhs`,` $rhs attr-dict `:` functional-type(operands, results)";
}

def AndOp : PredicateOp<"and", [Pure]> {
  let summary = "Performs row-wise logical AND on its two input columns";
  let arguments = (ins ColumnBools : $lhs, ColumnBools : $rhs);
}

def OrOp : PredicateOp<"or", [Pure]> {
  let summary = "Performs row-wise logical OR on its two input columns";
  let arguments = (ins ColumnBools : $lhs, ColumnBools : $rhs);
}

def EqOp : PredicateOp<"eq", [Pure]> {
  let summary = "Row-wise equality of two value columns";
  let description = [{
      Result is always an abstraction over a (possibly nullable) mask (bool vector).

      %r = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<bool>
  }];
}

def AddOp : BinaryOp<"add", [Pure]> {
  let summary = "Row-wise addition of two value columns";

  let arguments = (ins Column:$lhs, Column:$rhs);
  let results   = (outs Column:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

#endif //TURINGDB_OPS
