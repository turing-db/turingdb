#ifndef TURINGDB_NL_SCAN_OPS
#define TURINGDB_NL_SCAN_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageTypes.td"

/**
* ScanNodes
*/
def ScanNodes : NLOp<"scan_nodes", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over all nodes in the graph";
  let description = [{
    Imperative counterpart of db.scan_nodes. Each step of an enclosing
    nl.for fills one chunk of node IDs. The result type is always
    !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled out.
  }];
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(`operands`)` attr-dict";
}

/**
* ScanNodesByLabel
*/
def ScanNodesByLabel : NLOp<"scan_nodes_by_label", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.scan_nodes_by_label. Each step of an enclosing
    nl.for fills one chunk of the node IDs whose label set is a superset of
    `labels`:

      %nodes = nl.scan_nodes_by_label(["Person", "Employee"])

    `labels` is the same list of label names db.scan_nodes_by_label carried;
    translation resolves each name to a storage LabelID, the same way an
    nl.get_property_type name is resolved by the property fetch that consumes
    it. A label absent from the schema leaves the conjunction unsatisfiable, so
    the loop yields no chunk. Filtering by label narrows only the rows, never
    their shape, so - like nl.scan_nodes - the result is always
    !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled out.
  }];
  let arguments = (ins LabelRefArrayAttr:$labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $labels `)` attr-dict";
}

/**
* ConstScanNodes
*/
def ConstScanNodes : NLOp<"const_scan_nodes", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over a fixed, explicitly listed set of nodes";
  let description = [{
    Imperative counterpart of db.const_scan_nodes. Each step of an enclosing
    nl.for fills one chunk of the node IDs listed in `nodeIDs` or held by the
    `boundIDs` chunks:

      %nodes = nl.const_scan_nodes([0, 3, 7])
      %bound = nl.const_scan_nodes([], %ids : !nl.chunk<!storage.list<i64>>)

    `nodeIDs` is the same constant list db.const_scan_nodes carried; translation
    turns each entry into a storage NodeID. The bound IDs are read when the loop
    starts, as db.const_scan_nodes reads them, each node emitted once, in ID order.
    A bound chunk may be nullable, and names no node where it holds null.
    The loop drops, when it starts, any ID naming no node a scan would produce.
    Emitting a known set needs no graph walk, but it narrows only which rows are
    produced, never their shape, so - like nl.scan_nodes - the result is always
    !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled out.
  }];
  let arguments = (ins DenseI64ArrayAttr:$nodeIDs, Variadic<NLChunk>:$boundIDs);
  let results   = (outs NLIterator:$result);
  let hasVerifier = 1;

  let builders = [
    OpBuilder<(ins "::mlir::DenseI64ArrayAttr":$nodeIDs), [{
      build($_builder, $_state, nodeIDs, ::mlir::ValueRange());
    }]>
  ];

  let assemblyFormat = "`(` $nodeIDs (`,` $boundIDs^ `:` type($boundIDs))? `)` attr-dict";
}

/**
* ScanNodesByPropertyValue
*/
def ScanNodesByPropertyValue : NLOp<"scan_nodes_by_property_value", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the nodes whose named property equals a value";
  let description = [{
    Imperative counterpart of db.scan_nodes_by_property_value. Each step of an
    enclosing nl.for fills one chunk of the node IDs whose `property` holds
    exactly the value, among the nodes carrying every label in `labels` when the
    list is given:

      %nodes = nl.scan_nodes_by_property_value("age", 32 : i64)
      %people = nl.scan_nodes_by_property_value("age", 32 : i64, ["Person"])
      %bound = nl.scan_nodes_by_property_value("age", %age : !nl.chunk<i64>)

    `property`, `value`, `boundValue` and `labels` are the same name, literal,
    bound chunk and label list the db op carried; translation resolves the names
    against the schema, the same way an nl.get_property_type name is resolved by
    the property fetch that consumes it, and the value is coerced to the stored
    type, the bound one when the loop starts. A bound chunk may be nullable, and
    matches no node where it holds null. A property or label absent from the
    schema is carried by no node, so the loop yields no chunk. Filtering by value
    narrows only the rows, never their shape, so - like nl.scan_nodes - the result
    is always !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled
    out.
  }];
  let arguments = (ins StrAttr:$property,
                       OptionalAttr<TypedAttrInterface>:$value,
                       Optional<NLChunk>:$boundValue,
                       OptionalAttr<LabelRefArrayAttr>:$labels);
  let results   = (outs NLIterator:$result);
  let hasVerifier = 1;
  let assemblyFormat = [{
    `(` $property `,` custom<PropertyScanValue>($value, $boundValue, type($boundValue)) (`,` $labels^)? `)` attr-dict
  }];
}

/**
* FetchNodes
*/
def FetchNodes : NLOp<"fetch_nodes", [Pure]> {
  let summary = "Keep the rows whose ID cell names a node, with that node, as freshly cut chunks";
  let description = [{
    Imperative counterpart of db.fetch_nodes. Each step reads the chunk of ID cells, keeps
    the rows naming a node a scan would produce, and cuts the carry set to them:

      %a, %i2 = nl.fetch_nodes(%ids, {%i})
                  : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>)
                  -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>)
  }];

  let arguments = (ins NLChunk:$ids, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLChunk:$nodes, Variadic<NLChunk>:$carried);

  let assemblyFormat = [{
    `(` $ids `,` `{` $columns_to_filter `}` `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* FetchEdges
*/
def FetchEdges : NLOp<"fetch_edges", [Pure]> {
  let summary = "Keep the rows whose ID cell names an edge, with that edge's four columns, as freshly cut chunks";
  let description = [{
    Imperative counterpart of db.fetch_edges. Each step reads the chunk of ID cells, keeps
    the rows naming an edge a scan would produce, writes the source, edge, type and target
    of each, twice from either end with `undirected`, and cuts the carry set to them:

      %s, %e, %t, %d, %i2 = nl.fetch_edges(%ids, {%i})
                              : (!nl.chunk<!storage.edge_id>, !nl.chunk<!storage.nullable<i64>>)
                              -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>)
  }];

  let arguments = (ins NLChunk:$ids, Variadic<NLChunk>:$columns_to_filter, UnitAttr:$undirected);
  let results   = (outs NLChunk:$srcids, NLChunk:$eids, NLChunk:$etypes, NLChunk:$tgtids, Variadic<NLChunk>:$carried);

  let assemblyFormat = [{
    `(` $ids `,` `{` $columns_to_filter `}` `)` (`undirected` $undirected^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* ScanEdges
*/
def ScanEdges : NLOp<"scan_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over all edges in the graph";
  let description = [{
    Imperative counterpart of db.scan_edges. Each step of an enclosing nl.for
    fills one row of edge chunks - source node IDs, edge IDs, edge type IDs and
    target node IDs - the same chunk shape nl.get_out_edges produces, so a loop
    body written for a hop works unchanged over a full edge scan. Unlike
    nl.get_out_edges it reads no input chunk and carries nothing: it opens the
    dataflow. The result type is always the four-chunk edge iterator and is
    inferred, not spelled out.
  }];
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(`operands`)` attr-dict";
}

/**
* ConstScanEdges
*/
def ConstScanEdges : NLOp<"const_scan_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over a fixed, explicitly listed set of edges";
  let description = [{
    Imperative counterpart of db.const_scan_edges. Each step of an enclosing nl.for fills
    one row of the four edge chunks nl.scan_edges fills, for the edges listed in `edgeIDs`
    or held by the `boundIDs` chunks:

      %edges = nl.const_scan_edges([3, 7])
      %bound = nl.const_scan_edges([], %ids : !nl.chunk<!storage.list<i64>>) undirected

    The IDs are read and the edges they name looked up when the loop starts, as
    nl.const_scan_nodes reads its nodes, each edge emitted once in ID order, or twice, once
    from each end, with `undirected`. Listing edges narrows only which rows are produced,
    never their shape, so the result is always the four-chunk edge iterator and is
    inferred, not spelled out.
  }];
  let arguments = (ins DenseI64ArrayAttr:$edgeIDs, Variadic<NLChunk>:$boundIDs, UnitAttr:$undirected);
  let results   = (outs NLIterator:$result);
  let hasVerifier = 1;

  let assemblyFormat = "`(` $edgeIDs (`,` $boundIDs^ `:` type($boundIDs))? `)` (`undirected` $undirected^)? attr-dict";
}

/**
* ScanEdgesByType
*/
def ScanEdgesByType : NLOp<"scan_edges_by_type", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over all edges of the given types in the graph";
  let description = [{
    Imperative counterpart of db.scan_edges_by_type. Each step of an enclosing
    nl.for fills the same four chunks nl.scan_edges fills - source node IDs, edge
    IDs, edge type IDs and target node IDs - but only for the edges carrying one of
    `edge_types`:

      %knows = nl.get_edge_type_set(["KNOWS"])
      %edges = nl.scan_edges_by_type(%knows)

    `edge_types` is an !nl.edge_type_set handle produced by nl.get_edge_type_set,
    not names on the op, exactly as on nl.get_out_edges_by_type: they are resolved
    once above the loops. Narrowing by type changes only the rows, never their
    shape, so the result is the same four-chunk edge iterator nl.scan_edges
    produces and is inferred, not spelled out.
  }];
  let arguments = (ins NLEdgeTypeHandle:$edge_types);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $edge_types `)` attr-dict";
}

/**
* ScanOutEdgesByLabelSrc
*/
def ScanOutEdgesByLabelSrc : NLOp<"scan_out_edges_by_label_src", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.scan_out_edges_by_label_src. Each step of an
    enclosing nl.for fills the same four chunks nl.scan_edges fills - source node
    IDs, edge IDs, edge type IDs and target node IDs - but only for the edges
    leaving a node whose label set is a superset of `labels`:

      %edges = nl.scan_out_edges_by_label_src(["Person"])

    `labels` is the same list of label names db.scan_out_edges_by_label_src
    carried; translation resolves each name to a storage LabelID, exactly as an
    nl.scan_nodes_by_label name is resolved. A label absent from the schema leaves
    the conjunction unsatisfiable, so the loop yields no chunk. Narrowing by the
    source label changes only the rows, never their shape, so the result is the
    same four-chunk edge iterator nl.scan_edges produces and is inferred, not
    spelled out.
  }];
  let arguments = (ins LabelRefArrayAttr:$labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $labels `)` attr-dict";
}

/**
* ScanInEdgesByLabelTgt
*/
def ScanInEdgesByLabelTgt : NLOp<"scan_in_edges_by_label_tgt", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.scan_in_edges_by_label_tgt, and the
    reverse-direction sibling of nl.scan_out_edges_by_label_src: each step of an
    enclosing nl.for fills the same four chunks nl.scan_edges fills, but only for
    the edges arriving at a node whose label set is a superset of `labels`:

      %edges = nl.scan_in_edges_by_label_tgt(["Person"])

    The labels constrain the edge's target, so that is the end `tgtids` holds and the
    unconstrained source is what `srcids` holds - the chunks carry the edge as the
    graph stores it, as they do under nl.get_in_edges. `labels` is resolved the way an
    nl.scan_nodes_by_label name is, and a label absent from the schema leaves the
    conjunction unsatisfiable, so the loop yields no chunk. The result is the same
    four-chunk edge iterator nl.scan_edges produces and is inferred, not spelled out.
  }];
  let arguments = (ins LabelRefArrayAttr:$labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $labels `)` attr-dict";
}

/**
* ScanOutEdgesByLabelTgt
*/
def ScanOutEdgesByLabelTgt : NLOp<"scan_out_edges_by_label_tgt", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges arriving at the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.scan_out_edges_by_label_tgt. Each step of an
    enclosing nl.for fills the same four chunks nl.scan_edges fills, but only for the
    edges arriving at a node whose label set is a superset of `labels`:

      %edges = nl.scan_out_edges_by_label_tgt(["Person"])

    The labels constrain the edge's target, so `tgtids` holds the labelled end and
    `srcids` the unconstrained one. `labels` is resolved the way an
    nl.scan_nodes_by_label name is, and a label absent from the schema leaves the
    conjunction unsatisfiable, so the loop yields no chunk. The result is the same
    four-chunk edge iterator nl.scan_edges produces and is inferred, not spelled out.
  }];
  let arguments = (ins LabelRefArrayAttr:$labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $labels `)` attr-dict";
}

/**
* ScanInEdgesByLabelSrc
*/
def ScanInEdgesByLabelSrc : NLOp<"scan_in_edges_by_label_src", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges leaving the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.scan_in_edges_by_label_src. Each step of an enclosing
    nl.for fills the same four chunks nl.scan_edges fills, but only for the edges
    leaving a node whose label set is a superset of `labels`:

      %edges = nl.scan_in_edges_by_label_src(["Person"])

    The labels constrain the edge's source, so `srcids` holds the labelled end and
    `tgtids` the unconstrained one. `labels` is resolved the way an
    nl.scan_nodes_by_label name is, and a label absent from the schema leaves the
    conjunction unsatisfiable, so the loop yields no chunk. The result is the same
    four-chunk edge iterator nl.scan_edges produces and is inferred, not spelled out.
  }];
  let arguments = (ins LabelRefArrayAttr:$labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $labels `)` attr-dict";
}

/**
* ScanOutEdgesByLabelSrcTgt
*/
def ScanOutEdgesByLabelSrcTgt : NLOp<"scan_out_edges_by_label_src_tgt", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the edges whose two ends carry their labels, read off the source's index";
  let description = [{
    Imperative counterpart of db.scan_out_edges_by_label_src_tgt. Each step of an
    enclosing nl.for fills the same four chunks nl.scan_edges fills, but only for the
    edges whose source's label set is a superset of `src_labels` and whose target's is
    a superset of `tgt_labels`:

      %edges = nl.scan_out_edges_by_label_src_tgt(["Person"], ["Interest"])

    It walks the index keyed by the source's label set, as
    nl.scan_out_edges_by_label_src does, and keeps each edge whose target carries
    `tgt_labels`. Both lists are resolved the way an nl.scan_nodes_by_label name is,
    and a label absent from the schema in either leaves the scan unsatisfiable, so the
    loop yields no chunk. The result is the same four-chunk edge iterator
    nl.scan_edges produces and is inferred, not spelled out.
  }];
  let arguments = (ins LabelRefArrayAttr:$src_labels, LabelRefArrayAttr:$tgt_labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $src_labels `,` $tgt_labels `)` attr-dict";
}

/**
* ScanInEdgesByLabelSrcTgt
*/
def ScanInEdgesByLabelSrcTgt : NLOp<"scan_in_edges_by_label_src_tgt", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the edges whose two ends carry their labels, read off the target's index";
  let description = [{
    Imperative counterpart of db.scan_in_edges_by_label_src_tgt: the rows of
    nl.scan_out_edges_by_label_src_tgt, read off the index keyed by the target's label
    set, as nl.scan_in_edges_by_label_tgt reads them, keeping each edge whose source
    carries `src_labels`:

      %edges = nl.scan_in_edges_by_label_src_tgt(["Interest"], ["Person"])

    The chunks carry the edge as the graph stores it, so `srcids` holds the source and
    `tgtids` the target, as under nl.get_in_edges. Both lists are resolved the way an
    nl.scan_nodes_by_label name is, and a label absent from the schema in either leaves
    the scan unsatisfiable, so the loop yields no chunk.
  }];
  let arguments = (ins LabelRefArrayAttr:$src_labels, LabelRefArrayAttr:$tgt_labels);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $src_labels `,` $tgt_labels `)` attr-dict";
}

#endif // TURINGDB_NL_SCAN_OPS
