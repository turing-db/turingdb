#ifndef TURINGDB_NL_WRITE_OPS
#define TURINGDB_NL_WRITE_OPS

include "NLDialect.td"
include "NLTypes.td"
include "StorageTypes.td"

def CreateNode : NLOp<"create_node", [AttrSizedOperandSegments, RowAlignedThroughOperands]> {
  let summary = "Write one node per input row into the open transaction's write buffer";
  let description = [{
    Adds one pending node per row to the transaction's CommitWriteBuffer.
    Returns a chunk of provisional nodeIDs that a following nl.create_edge can use as
    source or target.

      nl.for %nodes in %scan {
        %n = nl.create_node ["Person"], ["name"], {%name_chunk}
      }
  }];

  let arguments = (ins LabelRefArrayAttr:$labels,
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

def CreateEdge : NLOp<"create_edge", [AttrSizedOperandSegments, RowAlignedThroughOperands]> {
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
                       EdgeTypeRef:$edge_type,
                       StrArrayAttr:$prop_names,
                       Variadic<NLChunk>:$prop_values,
                       Optional<NLBoolChunk>:$src_pending,
                       Optional<NLBoolChunk>:$tgt_pending,
                       UnitAttr:$src_all_pending,
                       UnitAttr:$tgt_all_pending);
  let results   = (outs NLEdgeIDChunk:$result);
  let hasVerifier = 1;

  // The two optional masks say, row by row, which endpoints are nodes this change wrote
  // and has not committed - what an nl.merge's node chunk mixes. A chunk an
  // nl.create_node produced is pending in every row and says so with the matching
  // all_pending flag instead of a mask.
  let assemblyFormat = [{
    $src_ids `,` $tgt_ids `,` $edge_type `,` $prop_names `,` `{` $prop_values `}`
    (`src_pending` $src_pending^)? (`tgt_pending` $tgt_pending^)?
    (`src_all_pending` $src_all_pending^)? (`tgt_all_pending` $tgt_all_pending^)? attr-dict
    ( `:` qualified(type($prop_values))^ )?
  }];
}

def Merge : NLOp<"merge", [AttrSizedOperandSegments]> {
  let summary = "Look one MERGE path pattern up per row of the input chunks, writing it when the graph does not hold it";

  let description = [{
    Imperative counterpart of db.merge, and the one op of the family that both reads
    the graph and writes the transaction's CommitWriteBuffer. For each row of its
    input chunks it looks the whole chain up - candidate nodes by label and property
    value, then each hop by type and property value - and fills its result chunks
    with one row per match. A row that matches nothing has the entities the pattern
    does not already bind written as pending nodes and edges, and contributes the one
    row those make.

      nl.for %names in %unwind {
        %n, %n_pending, %created = nl.merge nodes [["Person"]] props [["name"]]
                                           edges [] props [] dirs []
                                           bound {} pending {} values {%names} {} carrying {}
          : (!nl.chunk<!storage.string>)
            -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>)
      }

    The results grow or shrink the row count, the way nl.cross_product does: a row
    matched twice is emitted twice and the carry set is gathered onto the emitted
    rows, so every result chunk of one step holds the same rows.

    A node chunk the op produces mixes node IDs the graph holds with offsets into the
    write buffer, and the mask beside it says which rows are which - the same mask a
    following nl.set_node_property or nl.merge reads to know how to resolve a row. A
    bound node has no chunk of its own: it came in through `bound_nodes` and comes back
    through the carry set, gathered onto the emitted rows like every other chunk.
    The graph the match reads is the commit the change sits on, so a pattern this
    query wrote is invisible to it; what the op wrote itself it does remember, so
    merging the same node twice in one query writes it once.
  }];

  let arguments = (ins LabelRefArrayArrayAttr:$node_labels,
                       StrArrayArrayAttr:$node_prop_names,
                       EdgeTypeRefArrayAttr:$edge_types,
                       StrArrayArrayAttr:$edge_prop_names,
                       DenseI64ArrayAttr:$edge_directions,
                       DenseI64ArrayAttr:$pending_nodes,
                       Variadic<NLChunk>:$bound_nodes,
                       Variadic<NLChunk>:$bound_pending,
                       Variadic<NLChunk>:$node_prop_values,
                       Variadic<NLChunk>:$edge_prop_values,
                       Variadic<NLChunk>:$carried_columns);

  let results = (outs Variadic<NLChunk>:$results);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `nodes` $node_labels `props` $node_prop_names
    `edges` $edge_types `props` $edge_prop_names `dirs` custom<EdgeDirections>($edge_directions)
    `bound` `{` $bound_nodes `}` `pending` custom<ColumnIndices>($pending_nodes) `{` $bound_pending `}`
    `values` `{` $node_prop_values `}` `{` $edge_prop_values `}`
    `carrying` `{` $carried_columns `}`
    attr-dict `:` functional-type(operands, results)
  }];
}

def SetNodeProperty : NLOp<"set_node_property", [AttrSizedOperandSegments]> {
  let summary = "Update one named property per input node in the open transaction's write buffer";
  let description = [{
      nl.for %nodes in %scan {
        nl.set_node_property %nodes, "age", %val_chunk : !nl.chunk<i64>
      }
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes,
                       StrAttr:$property,
                       NLChunk:$value,
                       Optional<NLBoolChunk>:$pending,
                       Optional<NLBoolChunk>:$rows,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    $input_nodes `,` $property `,` $value (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? attr-dict `:` qualified(type($value))
  }];

  let hasVerifier = 1;
}

def SetEdgeProperty : NLOp<"set_edge_property", [AttrSizedOperandSegments]> {
  let summary = "Update one named property per input edge in the open transaction's write buffer";
  let description = [{
      nl.for %s, %e, %t in %edges {
        nl.set_edge_property %e, "weight", %val_chunk : !nl.chunk<f64>
      }
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges,
                       StrAttr:$property,
                       NLChunk:$value,
                       Optional<NLBoolChunk>:$pending,
                       Optional<NLBoolChunk>:$rows,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    $input_edges `,` $property `,` $value (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? attr-dict `:` qualified(type($value))
  }];

  let hasVerifier = 1;
}

def SetNodeProperties : NLOp<"set_node_properties", [AttrSizedOperandSegments]> {
  let summary = "Write each input node's map as its properties in the open transaction's write buffer";
  let description = [{
      nl.for %nodes in %scan {
        nl.set_node_properties %nodes, %maps replaces : !nl.chunk<!storage.map>
      }
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes,
                       NLChunk:$value,
                       Optional<NLBoolChunk>:$pending,
                       Optional<NLBoolChunk>:$rows,
                       UnitAttr:$all_pending,
                       UnitAttr:$replaces,
                       OptionalAttr<DictionaryAttr>:$created_types);

  let assemblyFormat = [{
    $input_nodes `,` $value (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? (`replaces` $replaces^)? (`creates` $created_types^)?
    attr-dict `:` qualified(type($value))
  }];

  let hasVerifier = 1;
}

def SetEdgeProperties : NLOp<"set_edge_properties", [AttrSizedOperandSegments]> {
  let summary = "Write each input edge's map as its properties in the open transaction's write buffer";
  let description = [{
      nl.for %s, %e, %t in %edges {
        nl.set_edge_properties %e, %maps replaces : !nl.chunk<!storage.map>
      }
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges,
                       NLChunk:$value,
                       Optional<NLBoolChunk>:$pending,
                       Optional<NLBoolChunk>:$rows,
                       UnitAttr:$all_pending,
                       UnitAttr:$replaces,
                       OptionalAttr<DictionaryAttr>:$created_types);

  let assemblyFormat = [{
    $input_edges `,` $value (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? (`replaces` $replaces^)? (`creates` $created_types^)?
    attr-dict `:` qualified(type($value))
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

  // `pending` is the mask an nl.merge produced beside its node chunk: such a row names
  // a node this change wrote and has not committed, which is dropped from the write
  // buffer rather than tombstoned by an ID the graph does not hold. `all_pending` says
  // that of every row without a mask, which an nl.create_node's chunk is.
  let arguments = (ins NLNodeIDChunk:$input_nodes,
                       UnitAttr:$detach,
                       Optional<NLBoolChunk>:$pending,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    $input_nodes (`detach` $detach^)? (`pending` $pending^)? (`all_pending` $all_pending^)? attr-dict
  }];
}

def DeleteEdge : NLOp<"delete_edge"> {
  let summary = "Tombstone each input edge in the open transaction's write buffer";
  let description = [{
      nl.for %s, %e, %t in %edges {
        nl.delete_edge %e
      }
  }];

  let arguments = (ins NLEdgeIDChunk:$input_edges,
                       Optional<NLBoolChunk>:$pending,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    $input_edges (`pending` $pending^)? (`all_pending` $all_pending^)? attr-dict
  }];
}

#endif // TURINGDB_NL_WRITE_OPS
