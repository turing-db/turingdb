#ifndef TURINGDB_NL_OPS
#define TURINGDB_NL_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "NLSystemOps.td"

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
  let arguments = (ins StrArrayAttr:$labels);
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
    nl.for fills one chunk of the node IDs listed in `nodeIDs`:

      %nodes = nl.const_scan_nodes([0, 3, 7])

    `nodeIDs` is the same constant list db.const_scan_nodes carried; translation
    resolves each entry to a storage NodeID, dropping any that name no node in the
    graph. Emitting a known set needs no graph walk, but it narrows only which rows
    are produced, never their shape, so - like nl.scan_nodes - the result is always
    !nl.iter<!nl.chunk<!storage.node_id>> and is inferred, not spelled out.
  }];
  let arguments = (ins DenseI64ArrayAttr:$nodeIDs);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $nodeIDs `)` attr-dict";
}

/**
* ScanNodesByPropertyValue
*/
def ScanNodesByPropertyValue : NLOp<"scan_nodes_by_property_value", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the nodes whose named property equals a literal";
  let description = [{
    Imperative counterpart of db.scan_nodes_by_property_value. Each step of an
    enclosing nl.for fills one chunk of the node IDs whose `property` holds
    exactly `value`, among the nodes carrying every label in `labels` when the
    list is given:

      %nodes = nl.scan_nodes_by_property_value("age", 32 : i64)
      %people = nl.scan_nodes_by_property_value("age", 32 : i64, ["Person"])

    `property`, `value` and `labels` are the same name, literal and label list
    the db op carried; translation resolves the names against the schema and
    coerces the literal to the stored type, the same way an nl.get_property_type
    name is resolved by the property fetch that consumes it. A property or label
    absent from the schema is carried by no node, so the loop yields no chunk.
    Filtering by value narrows only the rows, never their shape, so - like
    nl.scan_nodes - the result is always !nl.iter<!nl.chunk<!storage.node_id>>
    and is inferred, not spelled out.
  }];
  let arguments = (ins StrAttr:$property, TypedAttrInterface:$value, OptionalAttr<StrArrayAttr>:$labels);
  let results   = (outs NLIterator:$result);
  let hasVerifier = 1;
  let assemblyFormat = "`(` $property `,` $value (`,` $labels^)? `)` attr-dict";
}

/**
* UnwindConst
*/
// Pure: emitting a fixed list is deterministic; the per-element fan-out is in the nl.for.
def UnwindConst : NLOp<"unwind_const", [Pure]> {
  let summary = "Create a database iterator over a compile-time-known literal list";
  let description = [{
    Imperative counterpart of db.unwind_const. Each step of an enclosing nl.for fills
    one chunk of the literal list's elements:

      %rows = nl.unwind_const([1, 2, 3]) : !nl.iter<!nl.chunk<!storage.nullable<i64>>>
      nl.for %x in %rows : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
        nl.output(%x) : !nl.chunk<!storage.nullable<i64>>
      }

    `elements` is the same literal array db.unwind_const carried, each entry an
    attribute keeping its own type. A homogeneous list yields a nullable value chunk of
    that type - the shape a property fetch and the unwind_collect drain emit, so the
    unwound column feeds the same consumers, even though such a literal is never null;
    a heterogeneous or empty list yields a type-erased !storage.list_element chunk (a
    tagged scalar per row), which is also what a list holding a null or a nested list
    unwinds into. The single chunk element type depends on the elements, so -
    unlike nl.scan_nodes, whose node-ID iterator is fixed and inferred - it is spelled
    after the colon, as nl.collect's is. It opens the dataflow: no input, no carry.
  }];

  let arguments = (ins ArrayAttr:$elements);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $elements `)` attr-dict `:` qualified(type($result))";
}

/**
* LoadCSV
*/
// Pure: reading a file is deterministic; the per-record fan-out is in the nl.for.
def LoadCSV : NLOp<"load_csv", [Pure]> {
  let summary = "Create a database iterator over the records of a CSV file";
  let description = [{
    Imperative counterpart of db.load_csv. Each step of an enclosing nl.for parses the
    next chunk of records, filling one chunk per field the load produces:

      %name, %age = nl.load_csv("people.csv", [0 : ui64, 1 : ui64])
        : !nl.iter<!nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>>

    The path, the field list and the two flags are the ones db.load_csv carried,
    forwarded as-is; the path is resolved against the data directory, and a header name
    against the file's header line, when the loop first runs - the file is the one thing
    a loop of this family reads outside the graph. Like nl.unwind_const it reads no
    input chunk and carries nothing: it opens the dataflow.

    Every field rides an owning string chunk - the characters were parsed here, not
    borrowed from the graph, and a record carries no type to read them as. The number of
    chunks is the number of fields, so the iterator type is spelled after the colon
    rather than inferred.
  }];

  let arguments = (ins StrAttr:$path,
                       ArrayAttr:$fields,
                       UnitAttr:$with_headers,
                       UnitAttr:$skip_on_error);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = [{
    `(` $path `,` $fields `)` (`with_headers` $with_headers^)? (`skip_on_error` $skip_on_error^)?
    attr-dict `:` qualified(type($result))
  }];
}

/**
* VectorSearch
*/
// Pure: reading the index is deterministic; the per-neighbour fan-out is in the nl.for.
def VectorSearch : NLOp<"vector_search", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the nearest neighbours of a query vector";
  let description = [{
    Imperative counterpart of db.vector_search. Each step of an enclosing nl.for fills
    one row of neighbour chunks - the nodes the index holds the vectors under, and the
    distances the metric scored them at:

      %n = nl.vector_search("vec1", 3, [1.000000e+00, 0.000000e+00])
        : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>

    The parameters are the ones db.vector_search carried, forwarded as-is; the search
    runs against the session's vector indexes, which is the one thing a loop of this
    family reads outside the graph. Like nl.unwind_const it reads no input chunk and
    carries nothing: it opens the dataflow. The neighbours ride the same node ID chunk a
    scan binds, so a hop expands them unchanged; the distances ride a nullable value
    chunk, the shape every value-chunk consumer dispatches on, even though a neighbour is
    never null. Both element types are fixed, so the iterator type is inferred rather
    than spelled.
  }];

  let arguments = (ins StrAttr:$index_name, UI64Attr:$k, DenseF32ArrayAttr:$query_vector);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $index_name `,` $k `,` $query_vector `)` attr-dict `:` qualified(type($result))";
  let hasVerifier = 1;
}

/**
* Unwind
*/
// Pure: expanding a chunk of values is deterministic; the per-element fan-out is in the nl.for.
def Unwind : NLOp<"unwind", [Pure]> {
  let summary = "Create a database iterator that expands a chunk of values, one row per element";
  let description = [{
    Imperative counterpart of db.unwind. Each step of an enclosing nl.for fills one
    chunk of unwound elements plus one chunk per carried column, gathered so a carried
    cell repeats once per row its source cell produced:

      %rows = nl.unwind(%names, {%nodes})
                : (!nl.chunk<!storage.list<!storage.string>>, !nl.chunk<!storage.node_id>)
                -> !nl.iter<!nl.chunk<!storage.list_element>, !nl.chunk<!storage.node_id>>
      nl.for %x, %n in %rows : !nl.iter<!nl.chunk<!storage.list_element>, !nl.chunk<!storage.node_id>> {
        nl.output(%x, %n) : !nl.chunk<!storage.list_element>, !nl.chunk<!storage.node_id>
      }

    A list cell contributes one row per element, a null cell none, and any other cell
    the single row it is - a tagged scalar included, so a list_element cell that is
    itself a list unwinds one level deeper. Unlike nl.unwind_const it reads an input
    chunk, so it nests in the loop that binds it, the way nl.get_out_edges nests in the
    loop binding its nodes. The element chunk type follows the source's - a list unwinds
    into the type-erased list_element chunk, a scalar column into its own - and, like
    nl.collect's, is spelled out rather than inferred.
  }];

  // `columns_to_filter`: the variadic carry set, after the fixed source, mirroring
  // nl.get_out_edges.
  let arguments = (ins NLChunk:$source, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The source chunk's element type decides the element chunk's, and the carried types
  // vary from call to call, so operands and result print as a function type - as
  // nl.broadcast_constant's do - rather than the result alone.
  let assemblyFormat = [{
    `(` $source `,` `{` $columns_to_filter `}` `)` attr-dict `:` functional-type(operands, results)
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
* ScanEdgesByType
*/
def ScanEdgesByType : NLOp<"scan_edges_by_type", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over all edges of one type in the graph";
  let description = [{
    Imperative counterpart of db.scan_edges_by_type. Each step of an enclosing
    nl.for fills the same four chunks nl.scan_edges fills - source node IDs, edge
    IDs, edge type IDs and target node IDs - but only for the edges whose type is
    `edge_type`:

      %knows = nl.get_edge_type("KNOWS")
      %edges = nl.scan_edges_by_type(%knows)

    `edge_type` is an !nl.edge_type handle produced by nl.get_edge_type, not a name
    on the op, exactly as on nl.get_out_edges_by_type: the name is resolved once
    above the loops. Narrowing by type changes only the rows, never their shape, so
    the result is the same four-chunk edge iterator nl.scan_edges produces and is
    inferred, not spelled out.
  }];
  let arguments = (ins NLEdgeTypeHandle:$edge_type);
  let results   = (outs NLIterator:$result);
  let assemblyFormat = "`(` $edge_type `)` attr-dict";
}

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
  let summary = "Create a database iterator over the out-edges of one edge type of a chunk of nodes";
  let description = [{
    Imperative counterpart of db.get_out_edges_by_type. It fills the same row of
    chunks as nl.get_out_edges - source node IDs, edge IDs, edge type IDs and
    target node IDs, then one filtered chunk per carried column - but keeps only
    the out-edges whose type is `edge_type`:

      %knows = nl.get_edge_type("KNOWS")
      %likes = nl.get_edge_type("LIKES")
      %edges = nl.get_out_edges_by_type(%a, %knows, {})
      nl.for %srcs, %eids, %etypes, %b in %edges {
        %hop = nl.get_out_edges_by_type(%b, %likes, {%srcs}) : !nl.chunk<!storage.node_id>
        ...
      }

    `edge_type` is an !nl.edge_type handle produced by nl.get_edge_type, not a name
    on the op: the name is resolved once above the loops, so a hop nested in a loop
    reuses the resolved handle rather than looking the name up per step (the same
    split as nl.get_property_type and the property fetches). Filtering by type
    narrows only the rows, never their shape, so the result iterator type is
    identical to nl.get_out_edges' and is inferred; only the carried chunk types
    are spelled out, after the colon.
  }];

  // `edge_type` is the resolved handle from nl.get_edge_type. The carry set is
  // NLChunk, not NLNodeIDChunk, so chunks of any element type can be carried,
  // exactly as on nl.get_out_edges.
  let arguments = (ins NLNodeIDChunk:$input_nodes, NLEdgeTypeHandle:$edge_type, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The handle prints as %h inside the parens after input_nodes; its type is
  // buildable and so omitted, as nl.get_node_properties omits its property handle
  // type. As on nl.get_out_edges, only the carried chunk types are spelled, in the
  // optional group that drops the colon when the carry set is empty.
  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_type `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetInEdgesByType
*/
def GetInEdgesByType : NLOp<"get_in_edges_by_type", [Pure, InferTypeOpAdaptor]> {
  let summary = "Create a database iterator over the in-edges of one edge type of a chunk of nodes";
  let description = [{
    The reverse-direction sibling of nl.get_out_edges_by_type and the by-type
    sibling of nl.get_in_edges: it fills the same chunk shape as in-edges but keeps
    only the in-edges whose type is `edge_type`. The carry set and the result
    iterator type match nl.get_out_edges_by_type, so the two are interchangeable in
    a loop; `edge_type` is the same nl.get_edge_type handle and an unresolved type
    yields no chunk.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, NLEdgeTypeHandle:$edge_type, Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    `(` $input_nodes `,` $edge_type `,` `{` $columns_to_filter `}` `)` attr-dict
    ( `:` qualified(type($columns_to_filter))^ )?
  }];
}

/**
* GetPropertyType
*/
def GetPropertyType : NLOp<"get_property_type", [Pure]> {
  let summary = "Resolve a property name to a property type handle, once, ahead of the loops";
  let description = [{
    Looks the property name up in the graph schema and yields a
    !nl.property_type handle for its storage PropertyTypeID. It takes no chunk,
    so it sits at function scope above the loop nest: a query resolves each name
    once here and feeds the handle to every get_node_properties /
    get_edge_properties step, instead of resolving the name per chunk.

      %weight = nl.get_property_type("weight")

    The result is always !nl.property_type. That type is parameterless and so
    buildable, so the assembly omits it and the parser builds it from the
    recipe - no InferTypeOpAdaptor needed. Pure, so it can be hoisted and shared
    across the loops below it.
  }];

  // The property name, the same `name`/`weight` spelled in the query.
  let arguments = (ins StrAttr:$name);
  let results   = (outs NLPropertyType:$result);

  // No operands and a constant result type, so nothing is printed after the name.
  let assemblyFormat = "`(` $name `)` attr-dict";
}

/**
* GetEdgeType
*/
def GetEdgeType : NLOp<"get_edge_type", [Pure]> {
  let summary = "Resolve an edge type name to an edge type handle, once, ahead of the loops";
  let description = [{
    The edge sibling of nl.get_property_type. Looks the relationship-type name up
    in the graph schema and yields an !nl.edge_type handle for its storage
    EdgeTypeID. It takes no chunk, so it sits at function scope above the loop
    nest: a query resolves each name once here and feeds the handle to every
    get_out_edges_by_type / get_in_edges_by_type hop, instead of resolving the name
    per step when the hop is nested in a loop.

      %knows = nl.get_edge_type("KNOWS")

    The result is always !nl.edge_type. That type is parameterless and so
    buildable, so the assembly omits it. Pure, so it can be hoisted and shared
    across the loops below it.
  }];

  // The edge type name, the same `KNOWS` spelled in the query.
  let arguments = (ins StrAttr:$name);
  let results   = (outs NLEdgeType:$result);

  // No operands and a constant result type, so nothing is printed after the name.
  let assemblyFormat = "`(` $name `)` attr-dict";
}

/**
* GetNodeProperties
*/
def GetNodeProperties : NLOp<"get_node_properties", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read one property of a chunk of nodes, as a nullable value chunk";
  let description = [{
    Imperative counterpart of db.get_node_properties. The property is named once
    by nl.get_property_type and passed in as a %property_type handle. It opens no
    loop: it maps the input node chunk to one value per node, in order, straight
    into the current nl.for body.

    A node may not have the property, so the result is a nullable value chunk -
    !nl.chunk<!storage.nullable<...>>, storage's ColumnOptVector. Missing values come
    back null; no row is dropped. The wrapped type is the concrete primitive the
    property resolved to during lowering.

      %weight = nl.get_property_type("weight")
      nl.for %nodes in %scan {
        %values = nl.get_node_properties(%nodes, %weight) : !nl.chunk<!storage.nullable<f64>>
        nl.output(%nodes, %values) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>
      }
  }];

  // The node chunk and the property handle are both buildable, so their types
  // are omitted; only the nullable value chunk type is spelled, after the colon.
  let arguments = (ins NLNodeIDChunk:$input_nodes, NLPropertyTypeHandle:$property_type);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_nodes `,` $property_type `)` attr-dict `:` qualified(type($values))
  }];
}

/**
* GetEdgeProperties
*/
def GetEdgeProperties : NLOp<"get_edge_properties", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read one property of a chunk of edges, as a nullable value chunk";
  let description = [{
    The edge counterpart of get_node_properties: same shape, but the input is a
    chunk of edge IDs and the property is read from the edge side of the graph.
    It opens no loop and yields a nullable value chunk, one value per input edge.
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges, NLPropertyTypeHandle:$property_type);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_edges `,` $property_type `)` attr-dict `:` qualified(type($values))
  }];
}

def GetNodeLabelSet : NLOp<"get_node_label_set", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read the label-set ID of each node in a chunk";
  let description = [{
    For each node ID in the input chunk, fetches its LabelSetID.

      nl.for %nodes in %scan {
        %ls = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
        ...
      }
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes);
  let results   = (outs NLChunk:$label_set_ids);

  let assemblyFormat = [{
    `(` $input_nodes `)` attr-dict `:` qualified(type($label_set_ids))
  }];
}

def CheckLabelConstraint : NLOp<"check_label_constraint", [Pure, RowAlignedThroughOperands]> {
  let summary = "Test each label-set ID against a precomputed matching set";
  let description = [{
    For each LabelSetID in the input chunk, emits true if the ID appears in
    `matching_ids`.

      nl.for %nodes in %scan {
        %ls  = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
        %ok  = nl.check_label_constraint(%ls, [3, 7]) : !nl.chunk<!storage.bool>
        ...
      }
  }];

  let arguments = (ins NLLabelSetIDChunk:$labelset_ids, DenseI64ArrayAttr:$matching_ids);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $labelset_ids `,` $matching_ids `)` attr-dict `:` qualified(type($result))
  }];
}

def CheckEdgeTypeConstraint : NLOp<"check_edge_type_constraint", [Pure, RowAlignedThroughOperands]> {
  let summary = "Test each edge type ID against a precomputed allowed set";
  let description = [{
    For each EdgeTypeID in the input chunk, emits true if the ID appears in
    `matching_ids` (OR semantics: any match passes).

      nl.for %etypes in %scan {
        %ok = nl.check_edge_type_constraint(%etypes, [2, 5]) : !nl.chunk<!storage.bool>
        ...
      }
  }];

  let arguments = (ins NLEdgeTypeIDChunk:$edge_type_ids, DenseI64ArrayAttr:$matching_ids);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $edge_type_ids `,` $matching_ids `)` attr-dict `:` qualified(type($result))
  }];
}

/**
* For
*/
def For : NLOp<"for", [SingleBlockImplicitTerminator<"Yield">]> {
  let summary = "Drive a database iterator chunk by chunk";
  let description = [{
    Runs the body once per iterator step until the iterator is exhausted.
    Each step fills the next chunks of the iterator and binds them to the
    loop variables, one variable per chunk:

    nl.for %chunk in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
        ...
    }

    An optional `limit` operand carries an !nl.limit_state handle. A loop that
    names one stops driving its iterator as soon as the budget is exhausted, so
    every nl.for in a LIMIT's nest carries the same handle and the break unwinds
    the whole nest:

    nl.for %chunk in %nodes limit %h : !nl.iter<!nl.chunk<!storage.node_id>> {
        ...
    }
  }];

  // The optional limit handle is the only variable-length operand, so MLIR can
  // tell it apart from the iterator without operand-segment sizes.
  let arguments = (ins NLIterator:$iterator, Optional<NLLimitStateHandle>:$limit);
  let regions = (region SizedRegion<1>:$bodyRegion);
  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The builders take the iterator value - the loop variables are created by
  // looking up the chunk types on the iterator type, so C++ users never spell
  // any type - plus an optional limit handle (null for an unbounded loop). The
  // iterator-only overload is the unbounded form most callers want.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::Value":$iterator)>,
      OpBuilder<(ins "::mlir::Value":$iterator, "::mlir::Value":$limit)>
  ];
}

/**
* Yield
*/
def Yield : NLOp<"yield", [Pure, Terminator, HasParent<"For">]> {
  let summary = "End one step of an nl.for loop";
  let assemblyFormat = "attr-dict";
}

/**
* CrossProduct
*/
def CrossProduct : NLOp<"cross_product", [Pure, InferTypeOpAdaptor, AttrSizedOperandSegments]> {
  let summary = "Cross every row of one loop's chunks with every row of another";

  let description = [{
    Given an outer chunk of N rows and an inner chunk of M rows it produces N*M
    rows: each outer column is block-repeated x M (outer row i becomes M
    consecutive rows) and each inner column is tiled x N (the whole inner chunk
    repeated N times), so result row k pairs outer row k/M with inner row k%M -
    every outer row against every inner row.

      %a, %b = nl.cross_product {%a} {%b}
                 : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}

    The two brace groups are the columns each factor contributes, in db.yield
    order; the op's results are the outer columns followed by the inner, each
    result chunk keeping its input's element type - only the row count changes.
    N and M are read from the first column of each group, so each group must be
    non-empty.

    An optional `limit` operand carries an !nl.limit_state handle. When present,
    the product is built only up to limit.getRemaining() rows: rows are laid out in
    (outer, inner) order, so the product's prefix is exactly the prefix the
    following nl.limit_update/nl.output can emit this step, and building the rest
    would be wasted work. Absent, the whole N*M product is built.

      %a, %b = nl.cross_product {%a} {%b} limit %h
                 : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
  }];

  // The outer and inner column groups to cross, plus the optional limit handle.
  // Three operand groups of independent size need AttrSizedOperandSegments to
  // record each group's size.
  let arguments = (ins Variadic<NLChunk>:$outer_columns,
                       Variadic<NLChunk>:$inner_columns,
                       Optional<NLLimitStateHandle>:$limit);

  // One result per crossed column, outer group then inner group; inferred from
  // the operands since the broadcast keeps each chunk's element type.
  let results = (outs Variadic<NLChunk>:$results);

  // The two SSA brace groups mirror the two factors. The result types are
  // inferred, so only the operand chunk types are spelled - the same two
  // groups, each in braces, after the colon (as nl.output spells its chunk
  // types). The optional limit handle, when present, prints after a `limit`
  // keyword (its type is buildable and so omitted, as on nl.output). The
  // operandSegmentSizes attribute is populated and elided automatically by the
  // generated parser/printer.
  let assemblyFormat = [{
    `{` $outer_columns `}` `{` $inner_columns `}` (`limit` $limit^)? attr-dict `:`
    `{` qualified(type($outer_columns)) `}` `{` qualified(type($inner_columns)) `}`
  }];
}

/**
* Output
*/
def Output : NLOp<"output", [AttrSizedOperandSegments]> {
  let summary = "Send a row of chunks out as columns of the query result";
  let description = [{
    The sink of the nested loop, mirroring a result builder (LingoDB's
    dsa.append): inside an nl.for body it hands the current chunks to the
    query result, one output column per chunk. The columns are typically loop
    variables, so each nl.for step emits one chunk per column:

    nl.output(%targets) : !nl.chunk<!storage.node_id>
    nl.output(%sources, %targets) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>

    Variadic<NLChunk> accepts any number of chunks of any element type. The op
    is deliberately not Pure: emitting output is its whole point, so it must
    survive dead-code elimination even though it has no results.

    An optional `limit` operand carries an !nl.limit_state handle. When present,
    output emits only the prefix of length limit.getEmitThisStep() set by the
    preceding nl.limit_update; absent, it emits the whole chunk and is fully
    limit-oblivious. The limit form is the fold of an adjacent nl.limit_truncate
    whose only consumer was this output: when a LIMIT feeds the sink directly,
    output can read the budgeted prefix off the handle instead of copying it. It
    never mutates the budget - it only reads how many rows to emit:

    nl.output(%targets) limit %h : !nl.chunk<!storage.node_id>

    An optional `skip` operand carries an !nl.skip_state handle and is the skip
    sibling of the limit form: when present, output emits the surviving suffix
    [skip.getSkipThisStep(), skip.getSkipThisStep() + skip.getEmitThisStep()) set
    by the preceding nl.skip_update - it reads the dropped-row count as a starting
    offset rather than copying the suffix to the front. It is the fold of an
    adjacent nl.skip_truncate whose only consumer was this output (the copy-free
    path the suffix could not take through the sink before output learned an
    offset). Like the limit form it never mutates the counter:

    nl.output(%targets) skip %h : !nl.chunk<!storage.node_id>

    An optional `cardinality` operand carries a representative chunk of the driving
    relation - any loop variable of the loop this output nests in. A projection of
    only constants - RETURN 5 - has no per-row column to size the emission, yet its
    cardinality is the driving relation's (projection preserves cardinality), so the
    output nests in the driving loop and reads its row count from this chunk; the
    constants broadcast against it. Absent for a per-row projection (its own columns
    size it) or a single-row emit.

    nl.output(%c) cardinality(%n : !nl.chunk<!storage.node_id>) : !nl.chunk<i64>

    Only that row count is read, never a value, so the driver is a chunk of any
    element type - the node IDs a scan binds, the values an UNWIND unwinds, the return
    values a procedure drive binds - as nl.broadcast_constant's own cardinality
    operand is:

    nl.output(%c) cardinality(%x : !nl.chunk<!storage.nullable<i64>>) : !nl.chunk<i64>

    An optional `names` array carries the result column names db.output was given,
    one per column, which the interpreter hands to the sink so it can label the
    columns it is fed. They name the result and never steer execution, so an output
    that carries none emits exactly the same rows.

    nl.output(%a) names ["person"] : !nl.chunk<!storage.node_id>
  }];

  // Four variable-length operand groups - the columns, the optional limit, the
  // optional skip and the optional cardinality driver - so the op carries
  // operand-segment sizes to tell them apart, the same as nl.cross_product's column
  // groups. A folded output carries at most one of limit/skip (the truncate adjacent
  // to it); the two are not produced together (see foldSkipTruncatesIntoOutputs).
  let arguments = (ins Variadic<NLChunk>:$columns,
                       Optional<NLLimitStateHandle>:$limit,
                       Optional<NLSkipStateHandle>:$skip,
                       Optional<NLChunk>:$cardinality,
                       OptionalAttr<StrArrayAttr>:$column_names);

  // The chunk element types vary from call to call and - unlike the source
  // ops - cannot be inferred, so they are spelled after the colon. This keeps
  // the text self-describing: the parser builds each operand's type from the
  // printed list rather than looking it up on the defining nl.for (which MLIR
  // cannot do during a parse), the same reason nl.for keeps its : !nl.iter<...>
  // annotation. qualified(...) forces the leading !nl. on every printed type.
  // The optional limit and skip handles, when present, print after a `limit` or
  // `skip` keyword; both types are buildable and so omitted. Each optional group
  // has its own literal anchor, so the declarative parser tells them apart. The
  // cardinality driver is a chunk of any element type, which no recipe can build, so
  // it is parenthesized with its own type - keeping the trailing colon the columns'.
  let assemblyFormat = [{
    `(` $columns `)` (`names` $column_names^)? (`limit` $limit^)? (`skip` $skip^)? (`cardinality` `(` $cardinality^ `:` qualified(type($cardinality)) `)`)? attr-dict `:` qualified(type($columns))
  }];

  // Emitting an unnamed result is the common case - every hand-written program and
  // every lowering of a db.output that carries no names - so it gets a builder that
  // takes the operands alone.
  let builders = [
      OpBuilder<(ins "::mlir::ValueRange":$columns,
                     "::mlir::Value":$limit,
                     "::mlir::Value":$skip,
                     "::mlir::Value":$cardinality)>
  ];

  // Enforce the "at most one of limit/skip" invariant the description promises:
  // a folded output carries the single handle of the truncate adjacent to it,
  // never both. runOutput reads them as an if(skip)/else if(limit) chain, so a
  // malformed both-handles op would silently drop the limit; reject it here.
  // One name per column, when the output carries names at all.
  let hasVerifier = 1;
}

/**
* Limit
*/
// NOT Pure: each nl.limit is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - its reset is a side effect.
def Limit : NLOp<"limit", []> {
  let summary = "Create (and reset) a row-limit counter, initialized to N rows";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level
    LIMIT, or an enclosing nl.for body for a nested one, where it re-initializes
    once per enclosing step. Produces the !nl.limit_state handle the loops,
    nl.limit_update and nl.output read. The result type is buildable and omitted.
  }];

  // Unsigned: a row budget is never negative.
  let arguments = (ins UI64Attr:$count);
  let results   = (outs NLLimitState:$state);
  let assemblyFormat = "`(` $count `)` attr-dict";
}

/**
* LimitUpdate
*/
// NOT Pure: it mutates the handle, so it must run exactly where it sits and must
// not be eliminated.
def LimitUpdate : NLOp<"limit_update", []> {
  let summary = "Charge this step's rows against a limit counter";
  let description = [{
    Runs in the innermost loop body just before nl.output. Reads the row count of
    the representative chunk (the same column nl.output will emit, so the count
    matches after any cross product), records min(rows, remaining) as the count
    nl.output should emit this step, and subtracts it from the budget. The sole
    mutator of the counter.
  }];

  let arguments = (ins NLLimitStateHandle:$state, NLChunk:$rows);
  let assemblyFormat = "$state `,` $rows attr-dict `:` qualified(type($rows))";
}

/**
* LimitTruncate
*/
// NOT Pure: it reads the handle's mutable emitThisStep (set by the preceding
// nl.limit_update), so it must run exactly where it sits and must not be hoisted,
// CSE'd or reordered past the update. InferTypeOpAdaptor: the results mirror the
// column operand types, copied across by inferReturnTypes, as on nl.cross_product.
def LimitTruncate : NLOp<"limit_truncate", [InferTypeOpAdaptor]> {
  let summary = "Copy the first emitThisStep rows of each column into fresh chunks";
  let description = [{
    Sits in the producing loop body, just after nl.limit_update and just before the
    limited columns' consumer - nl.output when the query is not chained, the inner
    sub-pipeline when it is. Copies the prefix [0, emitThisStep) of each input chunk
    (the count nl.limit_update recorded this step) into a fresh, independently-owned
    chunk, so a downstream op reads a genuinely truncated chunk and needs no limit
    awareness of its own. The columns pass through unchanged in type; only the row
    count is cut.

      %a' = nl.limit_truncate %h, (%a) : !nl.chunk<!storage.node_id>
  }];

  // The limit handle whose emitThisStep sizes the copy, then the columns to cut.
  let arguments = (ins NLLimitStateHandle:$state, Variadic<NLChunk>:$columns);

  // One result per input column, same type. The results are inferred from the
  // column operands, so only the column chunk types are spelled, after the colon;
  // the handle type is buildable and omitted.
  let results = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
}

/**
* Skip
*/
// NOT Pure: each nl.skip is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - its reset is a side effect. The skip
// sibling of nl.limit.
def Skip : NLOp<"skip", []> {
  let summary = "Create (and reset) a row-skip counter, initialized to M rows to drop";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level SKIP
    - and re-initializes once per execution of that block. Produces the
    !nl.skip_state handle nl.skip_update and nl.skip_truncate read. Unlike nl.limit
    it never appears on an nl.for: a skip drives no loop early-exit, since every row
    past the dropped prefix must still be produced. The result type is buildable and
    omitted.
  }];

  // Unsigned: a count of rows to drop is never negative.
  let arguments = (ins UI64Attr:$count);
  let results   = (outs NLSkipState:$state);
  let assemblyFormat = "`(` $count `)` attr-dict";
}

/**
* SkipUpdate
*/
// NOT Pure: it mutates the handle, so it must run exactly where it sits and must
// not be eliminated. The skip sibling of nl.limit_update.
def SkipUpdate : NLOp<"skip_update", []> {
  let summary = "Charge this step's rows against a skip counter";
  let description = [{
    Runs in the innermost loop body just before nl.skip_truncate. Reads the row
    count of the representative chunk (the same column the consumer will read),
    drops min(rows, remaining) of them from the front of this step, records that
    offset and the emitted-row count nl.skip_truncate should copy, and subtracts the
    dropped count from the budget. The sole mutator of the counter.
  }];

  let arguments = (ins NLSkipStateHandle:$state, NLChunk:$rows);
  let assemblyFormat = "$state `,` $rows attr-dict `:` qualified(type($rows))";
}

/**
* SkipTruncate
*/
// NOT Pure: it reads the handle's mutable skipThisStep/emitThisStep (set by the
// preceding nl.skip_update), so it must run exactly where it sits and must not be
// hoisted, CSE'd or reordered past the update. InferTypeOpAdaptor: the results
// mirror the column operand types, copied across by inferReturnTypes, as on
// nl.limit_truncate.
def SkipTruncate : NLOp<"skip_truncate", [InferTypeOpAdaptor]> {
  let summary = "Copy the surviving suffix of each column into fresh chunks";
  let description = [{
    The skip sibling of nl.limit_truncate. Sits in the producing loop body, just
    after nl.skip_update and just before the columns' consumer - nl.output when the
    query is not chained, the inner sub-pipeline when it is. Copies the suffix
    [skipThisStep, skipThisStep + emitThisStep) of each input chunk (the offset and
    count nl.skip_update recorded this step) into a fresh, independently-owned,
    front-aligned chunk, so a downstream op reads a genuinely skipped chunk and
    needs no skip awareness of its own. The columns pass through unchanged in type;
    only the dropped prefix is removed.

    When the sole consumer is the query sink, DBLowering folds this op away: an
    nl.output gained an optional skip handle and emits the surviving suffix in
    place at an offset (see foldSkipTruncatesIntoOutputs), the copy-free path the
    suffix could not take while the sink only read from row zero. The copy here
    survives only when the suffix feeds an inner sub-pipeline (a chained query)
    that reads from row zero and so needs the rows lifted to the front.

      %a' = nl.skip_truncate %h, (%a) : !nl.chunk<!storage.node_id>
  }];

  // The skip handle whose offset/count size the copy, then the columns to cut.
  let arguments = (ins NLSkipStateHandle:$state, Variadic<NLChunk>:$columns);

  // One result per input column, same type. The results are inferred from the
  // column operands, so only the column chunk types are spelled, after the colon;
  // the handle type is buildable and omitted.
  let results = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
}

/**
* SortBuffer
*/
// NOT Pure: each nl.sort_buffer is a distinct accumulator, so two must never be
// CSE'd into one, and an unread one must not be DCE'd - resetting its buffers is
// a side effect (like nl.limit).
def SortBuffer : NLOp<"sort_buffer", []> {
  let summary = "Create (and reset) a row accumulator for one ORDER BY";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level
    ORDER BY - where it re-initializes the per-column buffers once per enclosing
    step. Produces the !nl.sort_state handle that nl.sort_collect appends to and
    the nl.for over nl.sort drains.

    `keys` is the list of column indices to sort by, most significant first;
    `ascending` gives the direction of each key (true = ascending). The buffers'
    column types are not spelled here - they are recovered from the nl.sort_collect
    that feeds the same handle - so only the sort spec appears. The result type is
    buildable and omitted.

      %state = nl.sort_buffer keys [1] ascending [true]

    An optional `limit` count turns the accumulator into a bounded top-K: it keeps
    only the `limit` best rows seen so far rather than every row, so the memory is
    O(limit) instead of O(rows) and the sort is O(rows log limit). It is the fused
    form of an ORDER BY ... LIMIT k - a constant bound, distinct from the mutable
    !nl.limit_state handle the streaming limits carry, since the bound is private
    to the sort and never decremented:

      %state = nl.sort_buffer keys [1] ascending [true] limit 10
  }];

  // keyColumns are indices into the collected column set, most significant first;
  // keyAscending gives one direction per key. The two arrays are parallel. The
  // optional topK count, when present, bounds the accumulator to its best rows.
  let arguments = (ins DenseI64ArrayAttr:$keyColumns,
                       DenseBoolArrayAttr:$keyAscending,
                       OptionalAttr<UI64Attr>:$topK);
  let results   = (outs NLSortState:$state);

  let assemblyFormat = "`keys` $keyColumns `ascending` $keyAscending (`limit` $topK^)? attr-dict";
  let hasVerifier = 1;
}

/**
* SortCollect
*/
// NOT Pure: it appends the current chunk's rows to the handle's buffers, so it
// must run exactly where it sits, once per producing-loop step, and must not be
// hoisted, CSE'd or eliminated.
def SortCollect : NLOp<"sort_collect", []> {
  let summary = "Append this step's chunk of every column to a sort accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Appends the current chunk of
    each column - the whole row set of this step - to the matching buffer of the
    handle, so that by the time the producing loop is exhausted the buffers hold
    every row. The columns are taken together so the buffers stay row-aligned.

      nl.sort_collect %state, (%a, %score) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
  }];

  // The accumulator handle, then the columns to append. The column chunk types
  // are spelled after the colon (as nl.output does), since the buffers' element
  // types are recovered from them.
  let arguments = (ins NLSortStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
  let hasVerifier = 1;
}

/**
* Sort
*/
// Pure: given a filled accumulator it deterministically yields the same sorted
// iterator, and it produces no side effect of its own - the sort and the
// chunk-by-chunk emission happen in the driving nl.for. Mirrors nl.scan_nodes:
// a source op whose nl.for does the work.
def Sort : NLOp<"sort", [Pure]> {
  let summary = "Create a database iterator over the sorted rows of an accumulator";
  let description = [{
    The source op for the emit phase of an ORDER BY, the sorted counterpart of
    nl.scan_nodes. It consumes a filled nl.sort_state and produces an iterator
    whose steps yield the accumulated rows sorted by the handle's keys, one chunk
    per column per step - the same chunk shape the buffers were collected with, so
    an enclosing nl.for binds one loop variable per column:

      %sorted = nl.sort(%state) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>>
      nl.for %a, %score in %sorted : !nl.iter<...> {
        nl.output(%a, %score) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
      }

    The iterator chunk types match the collected columns, but cannot be inferred
    from the parameterless state handle, so they are spelled after the colon (as
    the nl.for iterator type is). The sort itself runs once when the driving loop
    first steps; placing nl.sort after the producing loop guarantees the buffers
    are full by then.
  }];

  let arguments = (ins NLSortStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

// NOT Pure: resetting the seen-set is a side effect, and two seen-sets must never
// be CSE'd into one (like nl.limit / nl.sort_buffer).
def Distinct : NLOp<"distinct", []> {
  let summary = "Create (and reset) a seen-set for one DISTINCT";
  let description = [{
    Hoisted to the top of the scope it governs (function scope for a top-level or
    mid-query DISTINCT), where it empties the seen-set. Produces the
    !nl.distinct_state handle nl.distinct_filter reads and mutates. DISTINCT streams
    rather than breaking the pipeline, so no nl.for names this handle - the filter
    runs inside the producing loop.

      %s = nl.distinct
  }];

  let results = (outs NLDistinctState:$state);
  let assemblyFormat = "attr-dict";
}

// NOT Pure: it mutates the seen-set, so it must run where it sits and must not be
// hoisted, CSE'd or eliminated. InferTypeOpAdaptor: results mirror the columns.
def DistinctFilter : NLOp<"distinct_filter", [InferTypeOpAdaptor]> {
  let summary = "Keep only the rows of each column not seen before, as fresh chunks";
  let description = [{
    Runs in the producing loop body, once per step. Treats each row as the tuple
    across all columns (Cypher DISTINCT dedups the whole projection, nulls included),
    copies the rows not yet in the seen-set into fresh output chunks and records
    them, and drops the duplicates. The columns pass through unchanged in type.

    It emits genuinely cut chunks like nl.limit_truncate, so a downstream consumer -
    an nl.output, or a further nl.get_out_edges in the chained case - needs no
    DISTINCT awareness. And because it shrinks each chunk, a following nl.limit_update
    charges the deduped count, so a downstream LIMIT bounds the producing loop.

      %da = nl.distinct_filter %s, (%a) : !nl.chunk<!storage.node_id>
  }];

  let arguments = (ins NLDistinctStateHandle:$state, Variadic<NLChunk>:$columns);
  let results   = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];

  let hasVerifier = 1;
}

/**
* Count
*/
// NOT Pure: each nl.count is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - resetting the tally is a side effect
// (like nl.limit / nl.sort_buffer).
def Count : NLOp<"count", []> {
  let summary = "Create (and reset) a row-count accumulator, initialized to zero";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level or
    mid-query COUNT - where it zeroes the tally once per execution of that block.
    Produces the !nl.count_state handle nl.count_update increments and
    nl.count_result reads. The result type is buildable and omitted.

      %c = nl.count
  }];

  let results = (outs NLCountState:$state);
  let assemblyFormat = "attr-dict";
}

/**
* CountUpdate
*/
// NOT Pure: it increments the handle, so it must run exactly where it sits, once
// per producing-loop step, and must not be hoisted, CSE'd or eliminated. The count
// sibling of nl.limit_update / nl.sort_collect.
def CountUpdate : NLOp<"count_update", []> {
  let summary = "Charge this step's non-null rows against a count accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Adds the number of non-null
    rows of the current chunk to the tally - all rows of an ID chunk (IDs are never
    null), or the present values of a nullable value chunk (Cypher count(x) counts
    the rows in which x is not null). The sole counter mutator.

      nl.count_update %c, %a : !nl.chunk<!storage.node_id>

    `all_rows` charges the chunk's row count whatever it holds - Cypher count(*),
    which counts rows and reads no value, so the chunk it is anchored on may be a
    nullable one whose nulls a value tally would have skipped:

      nl.count_update %c, %age all_rows : !nl.chunk<!storage.nullable<i64>>
  }];

  // The accumulator handle, then the column whose non-null rows are charged. The
  // handle type is buildable and omitted; the column chunk type is spelled. all_rows
  // is a unit attribute: its presence is the flag.
  let arguments = (ins NLCountStateHandle:$state, NLChunk:$rows, UnitAttr:$all_rows);

  let assemblyFormat = "$state `,` $rows (`all_rows` $all_rows^)? attr-dict `:` qualified(type($rows))";
}

/**
* CountResult
*/
// NOT Pure: it reads the handle's mutable tally (set by the nl.count_updates in
// the producing loop), so it must run exactly where it sits - after that loop -
// and must not be hoisted, CSE'd or eliminated. The count sibling of
// nl.limit_truncate, which likewise reads mutable counter state.
def CountResult : NLOp<"count_result", []> {
  let summary = "Materialize the single result row of a count as a chunk";
  let description = [{
    The emit step of a COUNT. It reads a filled nl.count_state and materializes the
    tally directly as a single-row chunk - one unsigned i64. A count is a
    non-negative number that is never null, so the chunk is a plain !nl.chunk<ui64>,
    not a nullable value chunk.

    Unlike nl.sort it is not a source iterator: a COUNT collapses to exactly one
    row, so there is nothing to iterate. It produces the chunk in place, at function
    scope after the producing loop, and nl.output consumes it there - no emit loop:

      %r = nl.count_result(%c) : !nl.chunk<ui64>
      nl.output(%r) : !nl.chunk<ui64>

    The result chunk type is fixed (a ui64) but cannot be inferred from the
    parameterless state handle, so it is spelled after the colon. Placing
    nl.count_result after the producing loop guarantees the tally is final.
  }];

  let arguments = (ins NLCountStateHandle:$state);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* Aggregate
*/
// NOT Pure: the value-reducing sibling of nl.count. Each nl.aggregate is a
// distinct accumulator, so two must never be CSE'd into one, and an unread one
// must not be DCE'd - resetting the accumulator is a side effect.
def Aggregate : NLOp<"aggregate", []> {
  let summary = "Create (and reset) a value-aggregate accumulator";
  let description = [{
    The value-reducing counterpart of nl.count. Sits at the top of the scope it
    governs, where it resets the accumulator once per execution of that block, and
    produces the !nl.aggregate_state handle nl.aggregate_update folds into and
    nl.aggregate_result reads. `$kind` selects the reduction (sum/min/max/avg); the
    state type carries the accumulator element type, so the result type is spelled.

      %s = nl.aggregate sum : !nl.aggregate_state<i64>
  }];

  let arguments = (ins AggregateKind:$kind);
  let results   = (outs NLAggregateState:$state);
  let assemblyFormat = "$kind attr-dict `:` qualified(type($state))";

  // avg accumulates a running sum as f64, so its accumulator state must be f64;
  // the other kinds accumulate in the value's own type.
  let hasVerifier = 1;
}

/**
* AggregateUpdate
*/
// NOT Pure: it folds the chunk into the handle, so it must run exactly where it
// sits, once per producing-loop step. The value sibling of nl.count_update.
def AggregateUpdate : NLOp<"aggregate_update", []> {
  let summary = "Fold this step's non-null rows into a value-aggregate accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Reduces the present (non-null)
    values of the current nullable value chunk into the accumulator - adding them
    (sum/avg) or keeping the extreme (min/max) - matching Cypher, where these
    aggregates ignore null rows. The sole accumulator mutator.

      nl.aggregate_update sum %s, %v : !nl.aggregate_state<i64>, !nl.chunk<!storage.nullable<i64>>

    The input is always a nullable value chunk (a property fetch); its value type
    need not match the accumulator's - an avg of an i64 column folds into an f64
    accumulator, so both types are spelled after the colon.
  }];

  let arguments = (ins NLAggregateState:$state, NLChunk:$rows, AggregateKind:$kind);

  let assemblyFormat = [{
    $kind $state `,` $rows attr-dict `:` qualified(type($state)) `,` qualified(type($rows))
  }];

  // The fold handler is picked from this op's kind but the accumulator was reset by
  // the producing nl.aggregate's kind, and the handler reads the accumulator as its
  // own value type - so a kind that disagrees with the producer, an input that is
  // not a nullable value chunk, or a state element type that does not match the
  // reduction would fold into a wrongly-typed or wrongly-initialized accumulator.
  // The verifier reconciles all three so such IR is rejected rather than crashing.
  let hasVerifier = 1;
}

/**
* AggregateResult
*/
// NOT Pure: it reads the handle's mutable accumulator (set by the
// nl.aggregate_updates in the producing loop), so it must run exactly where it
// sits - after that loop. The value sibling of nl.count_result.
def AggregateResult : NLOp<"aggregate_result", []> {
  let summary = "Materialize the single result row of a value-aggregate as a chunk";
  let description = [{
    The emit step of a SUM/MIN/MAX/AVG. It reads a filled nl.aggregate_state and
    materializes the reduced value directly as a single-row nullable value chunk.
    Unlike count, an aggregate can be null (min/max/avg of no non-null row), so the
    result is a nullable value chunk, not a plain chunk; sum is never null (0 for an
    empty input) but rides the same nullable representation for uniformity.

      %r = nl.aggregate_result sum (%s) : !nl.aggregate_state<i64> -> !nl.chunk<!storage.nullable<i64>>
      nl.output(%r) : !nl.chunk<!storage.nullable<i64>>

    Like nl.count_result it is not a source iterator: an aggregate collapses to one
    row, so it produces the chunk in place at function scope after the producing
    loop, and nl.output consumes it there - no emit loop.
  }];

  let arguments = (ins NLAggregateState:$state, AggregateKind:$kind);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    $kind `(` $state `)` attr-dict `:` qualified(type($state)) `->` qualified(type($result))
  }];

  // The emit handler copies the accumulator into the result (or divides for avg),
  // both read as the result's value type, so the result must be a nullable value
  // chunk whose value type matches the reduction, and the kind must match the
  // producing nl.aggregate - the verifier enforces both. The result sibling of
  // AggregateUpdate's verifier.
  let hasVerifier = 1;
}

def Constant : NLOp<"constant", [Pure, ConstantLike, InferTypeOpAdaptor]> {
  let summary = "Materialize a constant scalar as a single-row chunk";
  let description = [{
      Represents a ColumnVector<T> with a single row
      %c = nl.constant(30 : i64) : !nl.chunk<i64>
  }];

  let arguments = (ins AnyAttrOf<[TypedAttrInterface, DenseF32ArrayAttr, ArrayAttr]>:$value);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` $value `)` attr-dict";
}

def BroadcastConstant : NLOp<"broadcast_constant", [Pure, RowAlignedThroughOperands]> {
  let summary = "Repeat a constant chunk's single value over the rows of a step";
  let description = [{
      A constant chunk holds one value standing for every row rather than one per
      row, so a step that walks rows - an aggregate's fold, a grouped fold - cannot
      read it: it would see the single row the constant is, not the rows the step
      has. This lays that value out over the rows of the driving relation, whose
      chunk the cardinality operand is, giving a row-aligned nullable value chunk
      the folds read like any column. The projection's sibling is nl.output's own
      cardinality operand, against which its constants broadcast as it emits.

      %r = nl.broadcast_constant %c, %n : (!nl.chunk<i64>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.nullable<i64>>

      The cardinality operand is absent when no relation drives the projection -
      RETURN 1 AS x, count(x) with no MATCH - which is one row of its own, so the
      value is laid out over that single row:

      %r = nl.broadcast_constant %c : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  // The cardinality is the only variable-length operand, so MLIR can tell it apart
  // from the value without operand-segment sizes (as nl.for's limit handle is).
  let arguments = (ins NLChunk:$value, Optional<NLChunk>:$cardinality);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$value (`,` $cardinality^)? attr-dict `:` functional-type(operands, results)";
}

/**
* Add
*/
def Add : NLOp<"add", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise addition of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double + double), and nullability (e.g. null + non-null = null).

      %r = nl.add %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Concat : NLOp<"concat", [Pure, ConstantThroughOperands]> {
  let summary = "Row-wise concatenation of two string chunks";
  let description = [{
      %r = nl.concat %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.string>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Sub : NLOp<"sub", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise subtraction of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double - double), and nullability (e.g. null - non-null = null).

      %r = nl.sub %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Mul : NLOp<"mul", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise multiplication of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double * double), and nullability (e.g. null * non-null = null).

      %r = nl.mul %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Div : NLOp<"div", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise division of two value chunks";
  let description = [{
      Result type is determined by the types (e.g. int -> double promotion
      on double / double), and nullability (e.g. null / non-null = null).
      Division by zero throws.

      %r = nl.div %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Mod : NLOp<"mod", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise modulo of two value chunks";
  let description = [{
      Modulo by zero throws.

      %r = nl.mod %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Pow : NLOp<"pow", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise exponentiation of two value chunks";
  let description = [{
      %r = nl.pow %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<f64>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Eq : NLOp<"eq", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise equality of two value chunks";
  let description = [{
      %r = nl.eq %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Neq : NLOp<"neq", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise inequality of two value chunks";
  let description = [{
      %r = nl.neq %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Gt : NLOp<"gt", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise greater-than comparison of two value chunks";
  let description = [{
      %r = nl.gt %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Lt : NLOp<"lt", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise less-than comparison of two value chunks";
  let description = [{
      %r = nl.lt %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Gte : NLOp<"gte", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise greater-than-or-equal comparison of two value chunks";
  let description = [{
      %r = nl.gte %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Lte : NLOp<"lte", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise less-than-or-equal comparison of two value chunks";
  let description = [{
      %r = nl.lte %x, %y : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def StartsWith : NLOp<"starts_with", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each string chunk value starts with the given prefix";
  let description = [{
      %r = nl.starts_with %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def EndsWith : NLOp<"ends_with", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each string chunk value ends with the given suffix";
  let description = [{
      %r = nl.ends_with %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Contains : NLOp<"contains", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise test of whether each string chunk value contains the given substring";
  let description = [{
      %r = nl.contains %x, %y : (!nl.chunk<!storage.string>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def And : NLOp<"and", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical AND of two boolean chunks";
  let description = [{
      Three-valued (Kleene) logic: null AND false is false, null AND true is
      null, so the result is nullable when either operand is.

      %r = nl.and %x, %y : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Or : NLOp<"or", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical OR of two boolean chunks";
  let description = [{
      Three-valued (Kleene) logic: null OR true is true, null OR false is
      null, so the result is nullable when either operand is.

      %r = nl.or %x, %y : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Xor : NLOp<"xor", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical XOR of two boolean chunks";
  let description = [{
      %r = nl.xor %x, %y : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
  }];

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def Not : NLOp<"not", [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Row-wise logical NOT of a boolean chunk";
  let description = [{
      Three-valued logic: NOT null is null, so the result is nullable when the
      operand is.

      %r = nl.not %x : (!nl.chunk<i1>) -> !nl.chunk<i1>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def ToNullable : NLOp<"to_nullable", [Pure]> {
  let summary = "Read a plain value chunk as a nullable value chunk holding a value in every row";
  let description = [{
      %r = nl.to_nullable %x : (!nl.chunk<ui64>) -> !nl.chunk<!storage.nullable<ui64>>
  }];

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

class NLUnaryFunctionOp<string mnemonic, list<Trait> extraTraits = []>
    : NLOp<mnemonic, !listconcat([Pure, RowAlignedThroughOperands], extraTraits)> {
  let summary = "Apply a unary scalar function row-wise to a value chunk";

  let arguments = (ins NLChunk:$operand);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$operand attr-dict `:` functional-type(operands, results)";
}

def Labels : NLUnaryFunctionOp<"labels">;
def EdgeType : NLUnaryFunctionOp<"edge_type">;
def ToInteger : NLUnaryFunctionOp<"to_integer", [ConstantThroughOperands]>;
def ToFloat : NLUnaryFunctionOp<"to_float", [ConstantThroughOperands]>;
def ToBoolean : NLUnaryFunctionOp<"to_boolean", [ConstantThroughOperands]>;

class NLBinaryFunctionOp<string mnemonic> : NLOp<mnemonic, [Pure, ConstantThroughOperands, RowAlignedThroughOperands]> {
  let summary = "Apply a binary scalar function row-wise to two value chunks";

  let arguments = (ins NLChunk:$lhs, NLChunk:$rhs);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` functional-type(operands, results)";
}

def CosineSimilarity : NLBinaryFunctionOp<"cosine_similarity">;
def EuclideanDistance : NLBinaryFunctionOp<"euclidean_distance">;

def Filter : NLOp<"filter", [Pure]> {
  let summary = "Keep only the rows where the mask chunk is true, as freshly cut chunks";
  let description = [{
    Applies a boolean mask to a carry set of chunks.

    %a2 = nl.filter %mask, (%a) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
  }];

  let arguments = (ins NLChunk:$mask, Variadic<NLChunk>:$columns);
  let results   = (outs Variadic<NLChunk>:$results);

  let assemblyFormat = [{
    $mask `,` `(` $columns `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* GroupAggregateBuffer
*/
// NOT Pure: each nl.group_aggregate_buffer is a distinct accumulator, so two must
// never be CSE'd into one, and an unread one must not be DCE'd - resetting its
// group table is a side effect (like nl.sort_buffer).
def GroupAggregateBuffer : NLOp<"group_aggregate_buffer", []> {
  let summary = "Create (and reset) a per-group accumulator for one grouped aggregation";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level
    grouped RETURN - where it re-initializes the group table once per enclosing
    step. Produces the !nl.group_aggregate_state handle that nl.group_aggregate_update
    folds into and the nl.for over nl.group_aggregate drains.

    `keys` is how many of the columns nl.group_aggregate_update collects are the
    grouping keys (the rest are the aggregate inputs, one per aggregate), and
    `aggregates` names each aggregate's reduction as GroupAggregateKind keywords
    (count, sum, min, max, avg). The column types are not spelled here - they are
    recovered from the nl.group_aggregate_update that feeds the same handle - so only
    the shape appears. The result type is buildable and omitted.

      %state = nl.group_aggregate_buffer keys 1 aggregates [sum]
  }];

  // keyCount is how many collected columns are grouping keys; kinds names each
  // aggregate's reduction, one per aggregate input. The two together must
  // partition the collected columns (checked during translation, where the column
  // count is known). kinds prints as its GroupAggregateKind keywords through the
  // shared custom<GroupAggregateKinds> directive (`[sum]`, not `[1]`).
  let arguments = (ins UI64Attr:$keyCount, DenseI64ArrayAttr:$kinds);
  let results   = (outs NLGroupAggregateState:$state);

  let assemblyFormat = "`keys` $keyCount `aggregates` custom<GroupAggregateKinds>($kinds) attr-dict";
  let hasVerifier = 1;
}

/**
* GroupAggregateUpdate
*/
// NOT Pure: it folds the current chunk into the handle's group table, so it must
// run exactly where it sits, once per producing-loop step, and must not be
// hoisted, CSE'd or eliminated. The grouped sibling of nl.sort_collect.
def GroupAggregateUpdate : NLOp<"group_aggregate_update", []> {
  let summary = "Fold this step's chunk of every column into a grouped accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Assigns each row to its group
    (by the serialized tuple of the grouping-key columns, creating a new group the
    first time a tuple is seen), then folds each aggregate-input column's row into
    that group's accumulator. The columns are taken together so the group
    assignment and the per-group folds stay row-aligned.

    The columns are the grouping keys first, then the aggregate inputs, in the
    order nl.group_aggregate_buffer's `keys` / `aggregates` describe; the split and
    the reductions are recovered from that buffer op (which produced the handle),
    so only the column chunk types are spelled here, after the colon.

      nl.group_aggregate_update %state, (%city, %score) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLGroupAggregateStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
  let hasVerifier = 1;
}

/**
* GroupAggregate
*/
// Pure: given a filled accumulator it deterministically yields the same group
// rows, and it produces no side effect of its own - the emission happens in the
// driving nl.for. Mirrors nl.sort: a source op whose nl.for does the work.
def GroupAggregate : NLOp<"group_aggregate", [Pure]> {
  let summary = "Create a database iterator over the rows of a grouped accumulator";
  let description = [{
    The source op for the emit phase of a grouped aggregation, the grouped
    counterpart of nl.sort. It consumes a filled nl.group_aggregate_state and
    produces an iterator whose steps yield one row per group - the grouping-key
    values followed by each aggregate's reduction - one chunk per output column per
    step, so an enclosing nl.for binds one loop variable per column:

      %groups = nl.group_aggregate(%state) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>>
      nl.for %city, %total in %groups : !nl.iter<...> {
        nl.output(%city, %total) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
      }

    The iterator chunk types are the grouping-key column types followed by the
    aggregate result types (a ui64 for count, the input's value type for
    sum/min/max, f64 for avg), but cannot be inferred from the parameterless state
    handle, so they are spelled after the colon (as the nl.for iterator type is).
    Placing nl.group_aggregate after the producing loop guarantees every row has
    been folded before the first group is emitted.
  }];

  let arguments = (ins NLGroupAggregateStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* Procedure
*/
// NOT Pure: it prepares a live procedure - the alloc callback's data and the
// procedure's PREPARE step - so two must never be CSE'd into one (each CALL is its
// own call), and an unread one must not be DCE'd: preparing it is a side effect
// (like nl.limit / nl.sort_buffer).
def Procedure : NLOp<"procedure", []> {
  let summary = "Prepare one procedure call, once, ahead of the loops";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level CALL -
    where it prepares the procedure once and re-initializes it each further time
    that block runs. Produces the !nl.procedure_state handle nl.procedure_init reads.

      %call = nl.procedure("db.labels") yields ["id", "label"]

    `name` is the procedure's full name, resolved against the procedure registry
    during translation - the procedure sibling of nl.get_property_type, which
    resolves a property name once above the loops so the consumers below reuse it.
    `yields` names the return values this call binds, in the order the consuming op
    produces them, so the columns the procedure writes into are allocated once here
    and shared by every step. It may be empty - a procedure declaring no return value
    binds no column and is driven for what it does rather than for rows. The result
    type is buildable and omitted.
  }];

  let arguments = (ins StrAttr:$name, StrArrayAttr:$yields);
  let results   = (outs NLProcedureState:$state);

  let assemblyFormat = "`(` $name `)` `yields` $yields attr-dict";
  let hasVerifier = 1;
}

/**
* CollectBuffer
*/
// NOT Pure: a distinct accumulator (no CSE); resetting its group table is a side effect.
def CollectBuffer : NLOp<"collect_buffer", []> {
  let summary = "Create (and reset) a per-group value-list accumulator for one collect";
  let description = [{
    Hoisted above the loops; produces the !nl.collect_state handle nl.collect_update
    appends to and the drain reads. `keys` is how many collected columns are grouping
    keys (0 = one global list); the column types, and how many value columns follow the
    keys, are recovered from the update.

      %buf = nl.collect_buffer keys 1

    `distinct` makes the accumulator charge each of a group's values once - Cypher's
    collect(DISTINCT x) - naming the value columns that dedupe by index. It sits on the
    buffer rather than the update because it is part of what the accumulator is, as its
    key count is: the update carries no spec of its own.

      %buf = nl.collect_buffer keys 0 distinct [1]
  }];

  // keyCount is unsigned; 0 is a valid ungrouped collect, so nothing to verify here
  // (the value-column split is checked during translation). kinds names the reductions
  // taken over the same groups as the list, one per aggregate input the update carries
  // after the value columns, and is absent when the collect reduces nothing beside it.
  let arguments = (ins UI64Attr:$keyCount,
                       OptionalAttr<DenseI64ArrayAttr>:$kinds,
                       OptionalAttr<DenseI64ArrayAttr>:$distinctValues);
  let results   = (outs NLCollectState:$state);

  let assemblyFormat = [{
    `keys` $keyCount (`aggregates` custom<GroupAggregateKinds>($kinds)^)?
    (`distinct` custom<ColumnIndices>($distinctValues)^)? attr-dict
  }];
}

/**
* CollectUpdate
*/
// NOT Pure: appends to the per-group lists; must run where it sits, once per step.
def CollectUpdate : NLOp<"collect_update", []> {
  let summary = "Append this step's rows to the per-group value lists";
  let description = [{
    Assigns each row to its group by the grouping-key tuple and appends its values to
    that group's lists. Columns are the grouping keys then the value columns, per the
    buffer's `keys`; nulls are dropped (Cypher collect ignores them).

      nl.collect_update %buf, (%city, %name) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
  }];

  let arguments = (ins NLCollectStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];

  let hasVerifier = 1;
}

/**
* UnwindCollect
*/
// Pure: deterministically drains a filled accumulator; the fan-out is in the nl.for.
def UnwindCollect : NLOp<"unwind_collect", [Pure]> {
  let summary = "Create a database iterator that drains a collect accumulator, one row per collected element";
  let description = [{
    Per-element drain of an nl.collect_state: one row per element - the group's key
    values (repeated) then one element. An empty group yields no row (UNWIND of an
    empty list). The iterator chunk types (keys then the value chunk) are spelled after
    the colon, and it is placed after the producing loop.

      %rows = nl.unwind_collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>>
  }];

  let arguments = (ins NLCollectStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* Collect
*/
// Pure: the per-group sibling of nl.unwind_collect - one row per group, not per element.
def Collect : NLOp<"collect", [Pure]> {
  let summary = "Create a database iterator that drains a collect accumulator, one row per group carrying the whole list";
  let description = [{
    Per-group drain of an nl.collect_state: one row per group - the key values then one
    list cell per value column (a ListView over the group's elements). An empty group
    yields an empty list (unlike nl.unwind_collect). The iterator chunk types (the keys
    then the list cells) are spelled after the colon, and it is placed after the
    producing loop.

      %rows = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>>
  }];

  let arguments = (ins NLCollectStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

def CreateNode : NLOp<"create_node", [AttrSizedOperandSegments]> {
  let summary = "Write one node per input row into the open transaction's write buffer";
  let description = [{
    Adds one pending node per row to the transaction's CommitWriteBuffer.
    Returns a chunk of provisional nodeIDs that a following nl.create_edge can use as
    source or target.

      nl.for %nodes in %scan {
        %n = nl.create_node ["Person"], ["name"], {%name_chunk}
      }
  }];

  let arguments = (ins StrArrayAttr:$labels,
                       StrArrayAttr:$prop_names,
                       Variadic<NLChunk>:$prop_values,
                       Optional<NLChunk>:$cardinality);
  let results   = (outs NLNodeIDChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    $labels `,` $prop_names `,` `{` $prop_values `}`
    ( `foreach` $cardinality^ `:` qualified(type($cardinality)) )? attr-dict
    ( `:` qualified(type($prop_values))^ )?
  }];
}

def CreateEdge : NLOp<"create_edge"> {
  let summary = "Write one edge per input row into the open transaction's write buffer";
  let description = [{
    Adds one pending edge per row to the transaction's CommitWriteBuffer.
    `src_ids` and `tgt_ids` are zipped pairwise

      nl.for %nodes in %scan {
        %n = nl.create_node ["Bot"], [], {}
        %e = nl.create_edge %nodes, %n, "KNOWS", [], {}
      }
  }];

  let arguments = (ins NLNodeIDChunk:$src_ids,
                       NLNodeIDChunk:$tgt_ids,
                       StrAttr:$edge_type,
                       StrArrayAttr:$prop_names,
                       Variadic<NLChunk>:$prop_values);
  let results   = (outs NLEdgeIDChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    $src_ids `,` $tgt_ids `,` $edge_type `,` $prop_names `,` `{` $prop_values `}` attr-dict
    ( `:` qualified(type($prop_values))^ )?
  }];
}

def SetNodeProperty : NLOp<"set_node_property"> {
  let summary = "Update one named property per input node in the open transaction's write buffer";
  let description = [{
      nl.for %nodes in %scan {
        nl.set_node_property %nodes, "age", %val_chunk : !nl.chunk<i64>
      }
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, StrAttr:$property, NLChunk:$value);

  let assemblyFormat = [{
    $input_nodes `,` $property `,` $value attr-dict `:` qualified(type($value))
  }];

  let hasVerifier = 1;
}

def SetEdgeProperty : NLOp<"set_edge_property"> {
  let summary = "Update one named property per input edge in the open transaction's write buffer";
  let description = [{
      nl.for %s, %e, %t in %edges {
        nl.set_edge_property %e, "weight", %val_chunk : !nl.chunk<f64>
      }
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges, StrAttr:$property, NLChunk:$value);

  let assemblyFormat = [{
    $input_edges `,` $property `,` $value attr-dict `:` qualified(type($value))
  }];

  let hasVerifier = 1;
}

def DeleteNode : NLOp<"delete_node"> {
  let summary = "Tombstone each input node in the open transaction's write buffer";
  let description = [{
      nl.for %nodes in %scan {
        nl.delete_node %nodes detach
      }
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes, UnitAttr:$detach);

  let assemblyFormat = [{
    $input_nodes (`detach` $detach^)? attr-dict
  }];
}

def DeleteEdge : NLOp<"delete_edge"> {
  let summary = "Tombstone each input edge in the open transaction's write buffer";
  let description = [{
      nl.for %s, %e, %t in %edges {
        nl.delete_edge %e
      }
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges);

  let assemblyFormat = [{
    $input_edges attr-dict
  }];
}

/**
* ProcedureInit
*/
// NOT Pure: driving the iterator runs the procedure's execute callback, so no CSE of two
// iterators over one call and no DCE of an unread one.
def ProcedureInit : NLOp<"procedure_init", [AttrSizedOperandSegments]> {
  let summary = "Create a database iterator driving a procedure over its arguments";
  let description = [{
    The row-producing form of a CALL, the procedure counterpart of nl.scan_nodes and
    nl.get_out_edges: it opens an iterator over the rows the procedure produces, and
    each step of the enclosing nl.for calls the procedure's execute callback once,
    which refills the result columns with that step's rows. The loop runs until the
    procedure declares itself finished, so - exactly like an edge hop over one chunk of
    nodes - a procedure may answer one chunk of arguments with as many chunks of rows as
    it needs:

      %call = nl.procedure("db.labels") yields ["id", "label"]
      %rows = nl.procedure_init(%call, (), {}) : (!nl.procedure_state) -> !nl.iter<!nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>>
      nl.for %ids, %labels in %rows : !nl.iter<...> {
        nl.output(%ids, %labels) : !nl.chunk<!storage.label_id>, !nl.chunk<!storage.string>
      }

    `inputs` are the call's arguments, in the procedure's declaration order, so operand
    i is argument i. With none the procedure is a source and its loop sits at function
    scope, as above. With arguments the iterator is created inside the loop that binds
    them and re-opened once per chunk of them - the nested-loop shape nl.get_out_edges
    has - so the procedure is reset at the top of each drive and starts afresh for the
    new chunk.

    `columns_to_filter` is the carry set, mirroring nl.get_out_edges: the chunks bound
    alongside the arguments that must survive the call. A procedure need not emit one
    row per input row, so a carried chunk cannot pass through untouched - each step
    rebuilds it from the input-row index the procedure reports for every row it emits,
    which replicates the rows of an expanding call and drops those of a filtering one.
    Each carried chunk comes back as a trailing chunk of the iterator, so the nl.for
    binds it after the yields:

      %rows = nl.procedure_init(%call, (%m), {%n, %m})
                : (!nl.procedure_state, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
                  -> !nl.iter<!nl.chunk<i64>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>>
      nl.for %x, %n2, %m2 in %rows : !nl.iter<...> {
        nl.output(%n2, %m2, %x) : ...
      }

    Only a procedure that declares it makes that report can be carried past
    (Procedure::getIndices), which lowering settles at plan time; hand-written IR
    is caught during translation. A procedure that declares it and then reports nothing
    is caught at the first step it emits, rather than silently misaligning the
    projection.

    The iterator's chunk types are the yielded return values' column types followed by
    the carried ones. Neither can be recovered from the parameterless state handle, so
    they are spelled after the colon (as the nl.for iterator type is). A call binding no
    return value gives an iterator of no chunk, whose nl.for binds no variable and runs an
    empty body: the drive is all there is to it, which is what a call made for its side
    effect wants.
  }];

  // Two variadic operand groups - the arguments and the carry set - so the op carries
  // operand-segment sizes to tell them apart, as nl.cross_product does.
  let arguments = (ins NLProcedureStateHandle:$state,
                       Variadic<NLChunk>:$inputs,
                       Variadic<NLChunk>:$columns_to_filter);
  let results   = (outs NLIterator:$result);

  // The handle prints first, then the parenthesized argument chunks and the
  // brace-wrapped carry set, so a source call reads `nl.procedure_init(%call, (), {})`
  // - the brace style nl.get_out_edges uses, which keeps the syntax total without an
  // optional group. Neither operand group's chunk types are buildable and the iterator
  // type cannot be inferred from them, so functional-type spells both sides, as
  // nl.filter does.
  let assemblyFormat = [{
    `(` $state `,` `(` $inputs `)` `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
  }];
}

#endif //TURINGDB_NL_OPS
