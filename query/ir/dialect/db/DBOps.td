#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBTypes.td"

/**
* ScanNodes
*/
def ScanNodes : TuringOp<"scan_nodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs Column:$result);
  // `qualified(...)` keeps the full `!db.column<...>` mnemonic on the result type
  // rather than the stripped `<...>` form, matching how get_out_edges prints.
  let assemblyFormat = "`(`operands`)` attr-dict `:` qualified(type($result))";
}

/**
* GetOutEdges
*/
def GetOutEdges : TuringOp<"get_out_edges", [Pure]> {
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
  let results   = (outs Column:$srcids,
                        Column:$eids,
                        Column:$etypes,
                        Column:$tgtids,
                        Variadic<Column>:$filtered_columns);

  // Brace-wrap the carry set so it reads like `get_out_edges(%b, {%a})`.
  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetNodeProperties
*/
def GetNodeProperties : TuringOp<"get_node_properties", [Pure]> {
  let summary = "Read one named property of each node, filtering carried columns alongside";

  let description = [{
    In `MATCH (a) RETURN a.name` the node column produced by a scan or a
    traversal carries only node IDs; the value behind `a.name` is fetched
    separately. This op takes such a node column and a property name and
    produces a column of that property's values, one per surviving node:

      %a    = db.scan_nodes() : !db.column<"a">
      %name = db.get_node_properties(%a, "name", {}) : (!db.column<"a">) -> !db.column<"a.name">

    Not every node has every property, so reading one implicitly filters the
    input down to the nodes that do. Like get_out_edges, that makes the op a
    filtering hop: `columns_to_filter` is the carry set of columns from earlier
    hops, each handed back in `filtered_columns` reduced to the surviving rows.
    To keep the node IDs themselves, carry the input column:

      %name, %a2 = db.get_node_properties(%a, "name", {%a})   // %a2: nodes that have "name"

    One op reads one property; `RETURN a.name, a.age` is several
    get_node_properties ops. Reading a property is a deterministic read of the
    graph, so the op is Pure.
  }];

  // `property` is a plain string attribute - the symbolic property name, the
  // same `name`/`age` spelled in the query. Being an attribute and not an SSA
  // value, it is absent from `operands` in the assembly format below. Resolving
  // the name to a storage PropertyTypeID happens later, when the op is lowered.
  //
  // `columns_to_filter`: the variadic carry set, after the fixed input_nodes;
  // `property` sits between them as the attribute.
  let arguments = (ins Column:$input_nodes, StrAttr:$property, Variadic<Column>:$columns_to_filter);

  // `result` is the property value column; `filtered_columns` is one per
  // carried column, mirroring get_out_edges.
  let results   = (outs Column:$result, Variadic<Column>:$filtered_columns);

  // Brace-wrap the carry set so it reads like the get_out_edges form. With the
  // variadic operand and result both present, functional-type(operands, results)
  // prints the full `(...) -> (...)` list; the string attribute prints inside
  // the parens, ahead of the colon, and MLIR elides it from attr-dict because
  // the format already names it.
  let assemblyFormat = [{
    `(` $input_nodes `,` $property `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetEdgeProperties
*/
def GetEdgeProperties : TuringOp<"get_edge_properties", [Pure]> {
  let summary = "Read one named property of each edge, filtering carried columns alongside";

  let description = [{
    The edge counterpart of get_node_properties. In
    `MATCH (a)-[e]->(b) RETURN e.weight` the edge column produced by
    get_out_edges carries only edge IDs; this op fetches the value behind
    `e.weight`, one per surviving edge, filtering the carry set down to the
    edges that have the property:

      %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : ...
      %weight, %srcs2, %b2 = db.get_edge_properties(%eids, "weight", {%srcs, %b})

    A db.column is just a named column at this level, so node and edge columns
    are indistinguishable in the type; the op chosen - node vs edge - is what
    selects which side of the graph the property is read from.
  }];

  let arguments = (ins Column:$input_edges, StrAttr:$property, Variadic<Column>:$columns_to_filter);
  let results   = (outs Column:$result, Variadic<Column>:$filtered_columns);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* Output
*/
def Output : TuringOp<"output"> {
  let summary = "Project a set of columns as the query result";
  let description = [{
      Sends a number of columns to output.

      db.output(%a, %b, %c) : !db.column<a>, !db.column<b>, !db.column<c>

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

#endif //TURINGDB_OPS
