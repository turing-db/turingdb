#ifndef TURINGDB_NL_HOP_OPS
#define TURINGDB_NL_HOP_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageTypes.td"

/**
* GetOutEdges
*/
def GetOutEdges : NLOp<"get_out_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of a chunk of nodes";
  let description = [{
    Imperative counterpart of db.get_out_edges. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs.

    `columns_to_filter` is the carry set, mirroring db.get_out_edges: chunks
    bound by an outer nl.for that this hop must filter down to the rows that
    survive the traversal. Each carried chunk comes back - same type, filtered -
    as a trailing chunk of the result iterator, so an enclosing nl.for binds it
    as an extra loop variable after the four fixed edge chunks:

      %edges = nl.get_out_edges(%a, {})
      nl.for %srcs, %eids, %etypes, %b in %edges {
        // second hop b->c, carrying the `a` of this step (here %srcs)
        %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!storage.node_id>
        nl.for %srcs2, %eids2, %etypes2, %c, %aFiltered in %hop { ... }
      }

    The input chunk type and the four fixed result chunks are inferred; only
    the carried chunk types are spelled out, after the colon.
  }];

  // `columns_to_filter`: the variadic carry set, after the fixed input_nodes.
  // NLChunk, not NLNodeIDChunk, so chunks of any element type can be carried.
  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The carried chunk element types vary from call to call and - unlike
  // input_nodes and the inferred result - cannot be built from a recipe, so
  // they are spelled after the colon, the same as nl.output. The optional
  // group (anchored on the carried types) drops the colon when the carry set
  // is empty, keeping the first-hop syntax `(%nodes, {})`.
  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetInEdges
*/
def GetInEdges : NLOp<"get_in_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of a chunk of nodes";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges: where that walks each
    node's successors, this walks its predecessors. Each step of an enclosing
    nl.for fills one chunk of source node IDs, edge IDs, edge type IDs and
    target node IDs - the same chunk shape as out-edges, so the two are
    interchangeable in a loop.

    Like nl.get_out_edges it takes a `columns_to_filter` carry set: chunks from
    an outer nl.for that this hop filters and hands back as trailing chunks of
    the result iterator. The input chunk type and the four fixed result chunks
    are inferred; only the carried chunk types are spelled out, after the colon.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

def GetEdges : NLOp<"get_edges", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the edges of a chunk of nodes";

  let arguments = (ins NLNodeIDChunk:$input_nodes, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetOutEdgesByType
*/
def GetOutEdgesByType : NLOp<"get_out_edges_by_type", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of the given types of a chunk of nodes";
  let description = [{
    Imperative counterpart of db.get_out_edges_by_type. It fills the same row of
    chunks as nl.get_out_edges - source node IDs, edge IDs, edge type IDs and
    target node IDs, then one filtered chunk per carried column - but keeps only
    the out-edges carrying one of `edge_types`:

      %knows = nl.get_edge_type_set(["KNOWS"])
      %likes = nl.get_edge_type_set(["LIKES", "LOVES"])
      %edges = nl.get_out_edges_by_type(%a, %knows, {})
      nl.for %srcs, %eids, %etypes, %b in %edges {
        %hop = nl.get_out_edges_by_type(%b, %likes, {%srcs}) : !nl.chunk<!storage.node_id>
        ...
      }

    `edge_types` is an !nl.edge_type_set handle produced by nl.get_edge_type_set,
    not names on the op: they are resolved once above the loops, so a hop nested in
    a loop reuses the resolved handle rather than looking them up per step (the same
    split as nl.get_property_type and the property fetches). Filtering by type
    narrows only the rows, never their shape, so the result iterator type is
    identical to nl.get_out_edges' and is inferred; only the carried chunk types
    are spelled out, after the colon.
  }];

  // `edge_types` is the resolved handle from nl.get_edge_type_set. The carry set is
  // NLChunk, not NLNodeIDChunk, so chunks of any element type can be carried,
  // exactly as on nl.get_out_edges.
  let arguments = (ins NLNodeIDChunk:$input_nodes, NLEdgeTypeHandle:$edge_types, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The handle prints as %h inside the parens after input_nodes; its type is
  // buildable and so omitted, as nl.get_node_properties omits its property handle
  // type. As on nl.get_out_edges, only the carried chunk types are spelled, in the
  // optional group that drops the colon when the carry set is empty.
  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetInEdgesByType
*/
def GetInEdgesByType : NLOp<"get_in_edges_by_type", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of the given types of a chunk of nodes";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges_by_type and the by-type
    sibling of nl.get_in_edges: it fills the same chunk shape as in-edges but keeps
    only the in-edges carrying one of `edge_types`. The carry set and the result
    iterator type match nl.get_out_edges_by_type, so the two are interchangeable in
    a loop; `edge_types` is the same nl.get_edge_type_set handle and a set nothing
    resolved into yields no chunk.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, NLEdgeTypeHandle:$edge_types, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetOutEdgesByLabel
*/
def GetOutEdgesByLabel : NLOp<"get_out_edges_by_label", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of a chunk of nodes arriving at the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.get_out_edges_by_label. It fills the same row of
    chunks as nl.get_out_edges - source node IDs, edge IDs, edge type IDs and target
    node IDs, then one filtered chunk per carried column - but keeps only the
    out-edges whose target's label set is a superset of `labels`:

      %edges = nl.get_out_edges_by_label(%a, ["Person"], {})
      nl.for %srcs, %eids, %etypes, %b in %edges {
        %hop = nl.get_out_edges_by_label(%b, ["Movie"], {%srcs}) : !nl.chunk<!storage.node_id>
        ...
      }

    `labels` is the same list of label names db.get_out_edges_by_label carried, on
    the op rather than on a handle: it is resolved to a storage LabelSet when the loop
    is translated, exactly as an nl.scan_nodes_by_label name is. A label absent from
    the schema leaves the conjunction unsatisfiable, so the loop yields no chunk.
    Narrowing by the target label changes only the rows, never their shape, so the
    result iterator type is identical to nl.get_out_edges' and is inferred; only the
    carried chunk types are spelled out, after the colon.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, LabelRefArrayAttr:$labels, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetInEdgesByLabel
*/
def GetInEdgesByLabel : NLOp<"get_in_edges_by_label", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of a chunk of nodes leaving the nodes carrying every given label";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges_by_label and the by-label
    sibling of nl.get_in_edges: it fills the same chunk shape as in-edges but keeps
    only the in-edges whose source's label set is a superset of `labels`. The labels
    constrain the end the hop leaves, so `srcids` holds it and `tgtids` the input node
    the hop walked from - the chunks carry the edge as the graph stores it, as they do
    under nl.get_in_edges. The carry set and the result iterator type match
    nl.get_out_edges_by_label, so the two are interchangeable in a loop; `labels` is
    resolved the same way and an absent name yields no chunk.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, LabelRefArrayAttr:$labels, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetOutEdgesByTypeAndLabel
*/
def GetOutEdgesByTypeAndLabel : NLOp<"get_out_edges_by_type_and_label", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the out-edges of the given types of a chunk of nodes arriving at the nodes carrying every given label";
  let description = [{
    Imperative counterpart of db.get_out_edges_by_type_and_label. It fills the same
    chunks as nl.get_out_edges but keeps only the out-edges carrying one of
    `edge_types` whose target's label set is a superset of `labels`:

      %knows = nl.get_edge_type_set(["KNOWS"])
      %edges = nl.get_out_edges_by_type_and_label(%a, %knows, ["Person"], {})

    `edge_types` is the nl.get_edge_type_set handle nl.get_out_edges_by_type takes, and
    `labels` the name list nl.get_out_edges_by_label takes. A set of types nothing
    resolved into, or a label absent from the schema, yields no chunk.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, NLEdgeTypeHandle:$edge_types, LabelRefArrayAttr:$labels, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetInEdgesByTypeAndLabel
*/
def GetInEdgesByTypeAndLabel : NLOp<"get_in_edges_by_type_and_label", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of the given types of a chunk of nodes leaving the nodes carrying every given label";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges_by_type_and_label: it keeps only
    the in-edges carrying one of `edge_types` whose source's label set is a superset of
    `labels`. The chunks carry the edge as the graph stores it, as under
    nl.get_in_edges.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, NLEdgeTypeHandle:$edge_types, LabelRefArrayAttr:$labels, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_types `,` $labels `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

#endif // TURINGDB_NL_HOP_OPS
