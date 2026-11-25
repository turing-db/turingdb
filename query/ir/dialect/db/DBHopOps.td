#ifndef TURINGDB_HOP_OPS
#define TURINGDB_HOP_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageTypes.td"

/**
* GetOutEdges
*/
def GetOutEdges : TuringOp<"get_out_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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
def GetInEdges : TuringOp<"get_in_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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
def GetEdges : TuringOp<"get_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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
def GetOutEdgesByType : TuringOp<"get_out_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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
  let arguments = (ins Column:$input_nodes, EdgeTypeRefArrayAttr:$edge_types, Variadic<Column>:$columns_to_filter);

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
def GetInEdgesByType : TuringOp<"get_in_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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

  let arguments = (ins Column:$input_nodes, EdgeTypeRefArrayAttr:$edge_types, Variadic<Column>:$columns_to_filter);

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
def GetOutEdgesByLabel : TuringOp<"get_out_edges_by_label", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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
  let arguments = (ins Column:$input_nodes, LabelRefArrayAttr:$labels, Variadic<Column>:$columns_to_filter);

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
def GetInEdgesByLabel : TuringOp<"get_in_edges_by_label", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
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

  let arguments = (ins Column:$input_nodes, LabelRefArrayAttr:$labels, Variadic<Column>:$columns_to_filter);

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
* GetOutEdgesByTypeAndLabel
*/
def GetOutEdgesByTypeAndLabel : TuringOp<"get_out_edges_by_type_and_label", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
  let summary = "Fetch the out-edges of the named types arriving at the nodes carrying every given label, filtering carried columns alongside";

  let description = [{
    get_out_edges_by_type and get_out_edges_by_label in one hop. In
    `MATCH (a)-[:KNOWS]->(b:Person)` an out-edge is kept when it carries one of
    `edge_types` and its target's label set is a superset of `labels`:

      %s, %e, %t, %b = get_out_edges_by_type_and_label(%a, ["KNOWS"], ["Person"], {})

    `edge_types` is a disjunction resolved as on get_out_edges_by_type, and an empty
    one walks no edge. `labels` is a conjunction resolved as on get_out_edges_by_label
    and must not be empty. The result chunk shape and the `columns_to_filter` carry set
    match get_out_edges.
  }];

  let arguments = (ins Column:$input_nodes, EdgeTypeRefArrayAttr:$edge_types, LabelRefArrayAttr:$labels, Variadic<Column>:$columns_to_filter);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

/**
* GetInEdgesByTypeAndLabel
*/
def GetInEdgesByTypeAndLabel : TuringOp<"get_in_edges_by_type_and_label", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource"]>]> {
  let summary = "Fetch the in-edges of the named types leaving the nodes carrying every given label, filtering carried columns alongside";

  let description = [{
    The reverse-direction sibling of get_out_edges_by_type_and_label. In
    `MATCH (a)<-[:KNOWS]-(b:Person)` an in-edge is kept when it carries one of
    `edge_types` and its source's label set is a superset of `labels`:

      %s, %e, %t, %b = get_in_edges_by_type_and_label(%a, ["KNOWS"], ["Person"], {})

    `srcids` holds the labelled end and `tgtids` the input node the hop walked from,
    as on get_in_edges_by_label.
  }];

  let arguments = (ins Column:$input_nodes, EdgeTypeRefArrayAttr:$edge_types, LabelRefArrayAttr:$labels, Variadic<Column>:$columns_to_filter);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$filtered_columns);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

#endif // TURINGDB_HOP_OPS
