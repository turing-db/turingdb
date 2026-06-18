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
  let summary = "Read one named property of each node as a column of values";

  let description = [{
    In `MATCH (a) RETURN a.name` the node column produced by a scan or a
    traversal carries only node IDs; this op fetches the value behind `a.name`,
    one per node, into a new column:

      %a    = db.scan_nodes() : !db.column<"a">
      %name = db.get_node_properties(%a, "name") : (!db.column<"a">) -> !db.column<"a.name">

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

      %weight = db.get_edge_properties(%eids, "weight") : (!db.column<"eids">) -> !db.column<"e.weight">

    A db.column is just a named column at this level, so node and edge columns
    are indistinguishable in the type; the op chosen - node vs edge - selects
    which side of the graph the property is read from.
  }];

  let arguments = (ins Column:$input_edges, StrAttr:$property);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `)` attr-dict `:`
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
