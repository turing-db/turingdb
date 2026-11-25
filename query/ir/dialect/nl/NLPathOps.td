#ifndef TURINGDB_NL_PATH_OPS
#define TURINGDB_NL_PATH_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageEnums.td"
include "StorageTypes.td"

/**
* NodeSetBuffer
*/
def NodeSetBuffer : NLOp<"node_set_buffer", []> {
  let summary = "Create (and empty) the node set one exploration ends on";
  let description = [{
    Hoisted above the loops; produces the !nl.node_set_state handle the collects append
    chunks to and nl.explore_paths reads its ends from. The result type is buildable and
    omitted.

      %ends = nl.node_set_buffer
  }];

  let results = (outs NLNodeSetState:$state);

  let assemblyFormat = "attr-dict";
}

/**
* NodeSetCollect
*/
def NodeSetCollect : NLOp<"node_set_collect", []> {
  let summary = "Append this step's nodes to a node set";
  let description = [{
    Runs in the loop that binds the node chunk, once per step, so that by the time that loop
    is exhausted the set holds every node of the column.

      nl.node_set_collect %ends, %b
  }];

  let arguments = (ins NLNodeSetStateHandle:$state, NLNodeIDChunk:$nodes);

  let assemblyFormat = "$state `,` $nodes attr-dict";
}

/**
* ExplorePaths
*/
def ExplorePaths : NLOp<"explore_paths", [Pure, InferTypeOpAdaptor, AttrSizedOperandSegments]> {
  let summary = "Create a database iterator over the variable-length paths leaving a chunk of nodes";
  let description = [{
    Imperative counterpart of db.explore_paths. Each step of an enclosing nl.for fills
    one chunk of seed node IDs, end node IDs and path handles, then one filtered chunk
    per carried column, exactly as nl.get_out_edges fills its edge chunks:

      %paths = nl.explore_paths(%a, {%x}) forward hops 1 to 3 : !nl.chunk<!storage.node_id>
      nl.for %seeds, %ends, %p, %x2 in %paths { ... }

    The direction, hop bounds, end labels, end column and distinct flag are the db op's,
    carried through unchanged. Its edge_types names arrive resolved, as the same
    nl.get_edge_type_set handle a by-type hop takes, so a query resolves one set above the
    loops and both read it. The db op's end_nodes column arrives as a node set
    handle instead: the loop binding it fills the set through nl.node_set_collect, and the
    walk reads the set once that loop has run to its end. The optional `hop` region is the
    db op's hop predicate lowered: a block over the source node, edge and end node chunks of one hop,
    yielding a boolean mask chunk; `hop_labels` is the db op's label-only hop predicate,
    carried as it is. The input chunk type and the three fixed result chunks
    are inferred; only the carried chunk types are spelled out, after the colon.

    A body of several steps is the db op's too: a direction, a hop labels list and a hop
    region per step, the region of step `k` over the node and edge chunks of its repetition
    up to its hop. Each edge type handle filters one step; `edge_type_steps` names which
    when not every step has one.
  }];

  let arguments = (ins NLNodeIDChunk:$input_nodes,
                       Variadic<NLChunk>:$columns_to_filter,
                       Optional<NLNodeSetStateHandle>:$end_nodes,
                       Variadic<NLEdgeTypeHandle>:$edge_types,
                       Variadic<NLChunk>:$hop_imports,
                       DenseI64ArrayAttr:$directions,
                       UI64Attr:$min_hops,
                       OptionalAttr<UI64Attr>:$max_hops,
                       OptionalAttr<DenseI64ArrayAttr>:$edge_type_steps,
                       OptionalAttr<LabelRefArrayAttr>:$end_labels,
                       OptionalAttr<LabelRefArrayArrayAttr>:$hop_labels,
                       OptionalAttr<UI64Attr>:$end_column,
                       UnitAttr:$ends_on_seed,
                       UnitAttr:$distinct);
  let results   = (outs NLIterator:$result);

  let regions = (region VariadicRegion<MaxSizedRegion<1>>:$hops);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` custom<PathDirections>($directions) `hops` $min_hops
    (`to` $max_hops^)? (`edge_types` $edge_types^)? (`edge_type_steps` $edge_type_steps^)? (`end_labels` $end_labels^)?
    (`hop_labels` custom<StepNames>($hop_labels)^)? (`end_nodes` $end_nodes^)? (`end_column` $end_column^)?
    (`hop_imports` `{` $hop_imports^ `:` qualified(type($hop_imports)) `}`)? (`ends_on_seed` $ends_on_seed^)?
    (`distinct` $distinct^)? `` custom<HopRegions>($hops, ref($directions))
    attr-dict ( `:` qualified(type($columns_to_filter))^ )?
  }];

  let extraClassDeclaration = [{
    size_t getStepCount() { return getDirections().size(); }
    ::mlir::storage::PathDirection getStepDirection(size_t step);

    // The edge type handle filtering one step, null when the step has none
    ::mlir::Value getStepEdgeTypes(size_t step);
  }];

  let hasVerifier = 1;
}

/**
* ExpandPath
*/
def ExpandPath : NLOp<"expand_path", [Pure, RowAlignedThroughOperands]> {
  let summary = "Expand each path handle of a chunk into a list";
  let description = [{
    Imperative counterpart of db.expand_path: one list per row, of the path's edges, of
    the end node of each hop, of the source node of each hop (the seed chunk's row
    then every end but the last), or of every node (the seed then every end).

      %e = nl.expand_path(%p, %s) kind edges
             : (!nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<!storage.edge_id>>
  }];

  let arguments = (ins NLPathRefChunk:$paths,
                       Optional<NLNodeIDChunk>:$srcids,
                       PathExpansionKind:$kind,
                       UnitAttr:$reversed,
                       OptionalAttr<UI64Attr>:$step,
                       OptionalAttr<UI64Attr>:$steps);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $paths (`,` $srcids^)? `)` `kind` $kind (`reversed` $reversed^)? (`step` $step^ `of` $steps)?
    attr-dict `:` functional-type(operands, results)
  }];
}

/**
* PathLength
*/
def PathLength : NLOp<"path_length", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read the number of edges, or of nodes, of each path of a chunk";
  let description = [{
    Imperative counterpart of db.path_length: one unsigned count per row, the
    repetitions of a body of `steps` steps when it is set, and the nodes of the path,
    one more than its edges, under `nodes`.

      %n = nl.path_length(%p) : (!nl.chunk<!storage.path_ref>) -> !nl.chunk<ui64>
      %n = nl.path_length(%p) steps 2 : (!nl.chunk<!storage.path_ref>) -> !nl.chunk<ui64>
      %n = nl.path_length(%p) nodes : (!nl.chunk<!storage.path_ref>) -> !nl.chunk<ui64>
  }];

  let arguments = (ins NLPathRefChunk:$paths, OptionalAttr<UI64Attr>:$steps, UnitAttr:$nodes);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $paths `)` (`steps` $steps^)? (`nodes` $nodes^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* MakePath
*/
def MakePath : NLOp<"make_path", [Pure, RowAlignedThroughOperands]> {
  let summary = "Build the path of a named pattern element, one path per row";
  let description = [{
    Imperative counterpart of db.make_path: one path per row, the nodes and edges the
    element runs through in written order, a path operand expanded into the hops it
    stands for.

      %p = nl.make_path(%a, %x, %b)
             : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>)
               -> !nl.chunk<!storage.path>
  }];

  let arguments = (ins Variadic<NLChunk>:$entities,
                       OptionalAttr<I64ArrayAttr>:$reversed_paths);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $entities `)` attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* PathElements
*/
def PathElements : NLOp<"path_elements", [Pure, RowAlignedThroughOperands]> {
  let summary = "Read the nodes, the relationships or the length of each named path of a chunk";
  let description = [{
    Imperative counterpart of db.path_elements: one list of node IDs, one list of edge
    IDs or one hop count per row, read off the path nl.make_path laid out or the path
    nl.shortest_path found, and null on the rows whose path is null - the paths an
    OPTIONAL MATCH missed. A chunk of tagged cells reads the path each cell holds, null
    where it holds a null.

      %ns = nl.path_elements(%p) kind nodes
              : (!nl.chunk<!storage.path>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.node_id>>>
  }];

  let arguments = (ins NLChunk:$path, PathElementsKind:$kind);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    `(` $path `)` `kind` $kind attr-dict `:` functional-type(operands, results)
  }];
}

/**
* ShortestPathBuffer
*/
def ShortestPathBuffer : NLOp<"shortest_path_buffer", []> {
  let summary = "Create (and reset) a shortest-path search accumulator";
  let description = [{
    Hoisted above the loops; produces the !nl.shortest_path_state handle the updates
    append node sets to and the drain runs the search over. The result type is buildable
    and omitted.

      %buf = nl.shortest_path_buffer
  }];

  let results = (outs NLShortestPathState:$state);

  let assemblyFormat = "attr-dict";
}

/**
* ShortestPathUpdate
*/
def ShortestPathUpdate : NLOp<"shortest_path_update", []> {
  let summary = "Append this step's nodes to the source or target node set";
  let description = [{
    Appends the step's node chunk to the target set when `target` is present, else the
    source set. Source and target are filled by separate updates, each in its own loop.

      nl.shortest_path_update %buf, %a
      nl.shortest_path_update %buf, %b target
  }];

  let arguments = (ins NLShortestPathStateHandle:$state, NLNodeIDChunk:$nodes, UnitAttr:$target);

  let assemblyFormat = "$state `,` $nodes (`target` $target^)? attr-dict";
}

/**
* ShortestPath
*/
def ShortestPath : NLOp<"shortest_path", [Pure]> {
  let summary = "Create a database iterator that runs the weighted search and drains its one result row";
  let description = [{
      %rows = nl.shortest_path(%buf, %cost) : !nl.iter<!nl.chunk<f64>, !nl.chunk<!storage.path>>
  }];

  let arguments = (ins NLShortestPathStateHandle:$state, NLPropertyTypeHandle:$edgeProperty);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `,` $edgeProperty `)` attr-dict `:` qualified(type($result))";
}

#endif // TURINGDB_NL_PATH_OPS
