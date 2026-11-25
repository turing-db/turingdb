#ifndef TURINGDB_NL_PROPERTY_OPS
#define TURINGDB_NL_PROPERTY_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageTypes.td"

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
* GetEdgeTypeSet
*/
def GetEdgeTypeSet : NLOp<"get_edge_type_set", [Pure]> {
  let summary = "Resolve a pattern's edge type names to an edge type set, once, ahead of the loops";
  let description = [{
    The edge sibling of nl.get_property_type. Looks the relationship-type names up
    in the graph schema and yields an !nl.edge_type_set handle for their storage
    EdgeTypeIDs. It takes no chunk, so it sits at function scope above the loop
    nest: a query resolves each set once here and feeds the handle to every
    get_out_edges_by_type / get_in_edges_by_type hop, instead of resolving the
    names per step when the hop is nested in a loop.

      %knows = nl.get_edge_type_set(["KNOWS"])
      %either = nl.get_edge_type_set(["KNOWS", "LIKES"])

    The names are a disjunction, as `-[:KNOWS|LIKES]->` spells it: an edge matches
    when it carries any one of them. A name no edge has ever carried drops out of
    the set when it is resolved, and a set left empty that way matches no edge. An empty
    `names` says the same outright, for a read a pass has narrowed to nothing.

    The result is always !nl.edge_type_set. That type is parameterless and so
    buildable, so the assembly omits it. Pure, so it can be hoisted and shared
    across the loops below it.
  }];

  // The edge type names, the same `KNOWS|LIKES` spelled in the query.
  let arguments = (ins EdgeTypeRefArrayAttr:$names);
  let results   = (outs NLEdgeTypeSet:$result);

  // No operands and a constant result type, so nothing is printed after the names.
  let assemblyFormat = "`(` $names `)` attr-dict";
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
    back null; no row is dropped. The wrapped type is the one the db read declares.

      %weight = nl.get_property_type("weight")
      nl.for %nodes in %scan {
        %values = nl.get_node_properties(%nodes, %weight) : !nl.chunk<!storage.nullable<f64>>
        nl.output(%nodes, %values) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>
      }
  }];

  // The node chunk, the property handle and the optional pending mask are all
  // buildable, so their types are omitted; only the nullable value chunk type is
  // spelled, after the colon. `pending` is the mask an nl.merge produced beside its
  // node chunk: such a row names a node this change wrote by the ID it will commit as,
  // past every node the graph holds, so its value is read out of the write buffer -
  // null there when the write set the property on none.
  // `all_pending` says that of every row without a mask, which an nl.create_node's
  // chunk is.
  let arguments = (ins NLNodeIDChunk:$input_nodes,
                       NLPropertyTypeHandle:$property_type,
                       Optional<NLBoolChunk>:$pending,
                       UnitAttr:$all_pending);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_nodes `,` $property_type `)` (`pending` $pending^)? (`all_pending` $all_pending^)?
    attr-dict `:` qualified(type($values))
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

  let arguments = (ins NLEdgeIDChunk:$input_edges,
                       NLPropertyTypeHandle:$property_type,
                       Optional<NLBoolChunk>:$pending,
                       UnitAttr:$all_pending);
  let results   = (outs NLChunk:$values);

  let assemblyFormat = [{
    `(` $input_edges `,` $property_type `)` (`pending` $pending^)? (`all_pending` $all_pending^)?
    attr-dict `:` qualified(type($values))
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

def GetEdgeTypes : NLOp<"get_edge_types", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read the type ID of each edge in a chunk";
  let description = [{
    For each edge ID in the input chunk, fetches its EdgeTypeID.

      nl.for %edges in %scan {
        %et = nl.get_edge_types(%edges) : !nl.chunk<!storage.edge_type_id>
        ...
      }
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges);
  let results   = (outs NLChunk:$edge_type_ids);

  let assemblyFormat = [{
    `(` $input_edges `)` attr-dict `:` qualified(type($edge_type_ids))
  }];
}

def CheckLabelConstraint : NLOp<"check_label_constraint", [Pure, RowAlignedThroughOperands]> {
  let summary = "Test each label-set ID against a required-label constraint";
  let description = [{
    For each LabelSetID in the input chunk, emits true if the label set it names
    carries every label of at least one of `alternatives` - a superset match. A single
    alternative is written flat, several are written nested.

      nl.for %nodes in %scan {
        %ls  = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
        %ok  = nl.check_label_constraint(%ls, ["Person"]) : !nl.chunk<!storage.bool>
        ...
      }

    The names are turned into the IDs of the label sets that satisfy them when the
    program is translated rather than when it is lowered: a CREATE ahead of the test
    interns a label set of its own, and that one is in the change's schema only once
    the create has been translated.
  }];

  let arguments = (ins NLLabelSetIDChunk:$labelset_ids, LabelRefArrayArrayAttr:$alternatives);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $labelset_ids `,` custom<LabelAlternatives>($alternatives) `)` attr-dict `:` qualified(type($result))
  }];
}

def CheckEdgeTypeConstraint : NLOp<"check_edge_type_constraint", [Pure, RowAlignedThroughOperands]> {
  let summary = "Test each edge type ID against an allowed-type constraint";
  let description = [{
    For each EdgeTypeID in the input chunk, emits true if it is one of `edge_types`
    (OR semantics: any match passes).

      nl.for %etypes in %scan {
        %ok = nl.check_edge_type_constraint(%etypes, ["KNOWS"]) : !nl.chunk<!storage.bool>
        ...
      }

    The names are resolved when the program is translated, for the reason
    nl.check_label_constraint's are: a CREATE ahead of the test may be what put the
    type in the change's schema.
  }];

  let arguments = (ins NLEdgeTypeIDChunk:$edge_type_ids, EdgeTypeRefArrayAttr:$edge_types);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $edge_type_ids `,` $edge_types `)` attr-dict `:` qualified(type($result))
  }];
}

#endif // TURINGDB_NL_PROPERTY_OPS
