#ifndef TURINGDB_PATH_OPS
#define TURINGDB_PATH_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageEnums.td"
include "StorageTypes.td"

/**
* ExplorePaths
*/
def ExplorePaths : TuringOp<"explore_paths", [Pure, AttrSizedOperandSegments, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getRowSource", "getRegionCarrySet", "getCarrySetMutable", "getRegionCarrySetMutable"]>]> {
  let summary = "Enumerate the variable-length paths leaving each input node, filtering carried columns alongside";

  let description = [{
    The variable-length sibling of get_out_edges. In `MATCH (n)-[e]->{1,3}(m)` every
    trail of one to three edges leaving `n` is one row: `srcids` is the seed `n`,
    `tgtids` the node the path ends on, `paths` the path itself - a handle into the
    query's path trie that db.expand_path turns into the list of edge IDs `e` binds to.
    An edge appears at most once on a path; nodes may repeat. `min_hops` of zero emits
    one zero-length path per seed (its end is the seed, its list empty); an absent
    `max_hops` is unbounded, which trail semantics keep finite.

      %s, %t, %p = db.explore_paths(%n, {}) forward hops 1 to 3
      %s, %t, %p, %a2 = db.explore_paths(%n, {%a}) both hops 0 edge_types ["KNOWS"]
      %s, %t, %p = db.explore_paths(%n, {}) forward hops 2 to 4 end_labels ["Interest"]

    `direction` is which edges a hop follows; `srcids` is always the seed, whatever the
    direction. `edge_types` restricts every hop to edges carrying one of those types, a
    disjunction as `-[:KNOWS|LIKES*]->` spells it; a name absent from the schema drops
    out, and a set left empty that way matches no edge. The optional `hop`
    region is a predicate on each hop, evaluated over its source node, its edge and its
    end node - the first three block arguments - and yielding one boolean column; a hop
    that fails it is neither emitted nor expanded. The region reads nothing from outside
    but constants. `columns_to_filter` is the carry set, exactly as on get_out_edges.

    A walk can repeat a body of several steps instead of one hop, the way
    `MATCH (a)((x)-[:KNOWS]->(y)<-[:LIKES]-(z)){1,2}(c)` repeats two. The direction is then a
    list with one entry per step, `edge_types` and `hop_labels` hold one list per step, an
    empty one leaving that step free, and there is one hop region per step, `{}` for a step
    without a predicate. The hop bounds count repetitions of the body, and a path ends only
    after a whole number of them. The region of step `k` takes the nodes and edges of the
    repetition up to its hop, interleaved from the node the repetition started on - `k`
    earlier nodes and edges, then the hop's source node, edge and end node - and then the
    hop imports.

      %s, %t, %p = db.explore_paths(%a, {}) [forward, backward] hops 1 to 2
                     edge_types [["KNOWS"], ["LIKES"]]

    `hop_imports` are the columns the hop predicate reads from outside the hop, the way
    `MATCH (n)-[e*1..3 WHERE e.since > n.born]->(m)` reads the seed. Each is row-aligned
    with `input_nodes` and becomes one further block argument of the region, in that order,
    holding the seed row's own value repeated over the candidates of the hop. They are the
    carry set into the hop regions, apart from `columns_to_filter`: that one is reordered,
    and each of its columns comes back as a result, where an import does not. An
    exploration that imports is walked seed by seed, so the distinct search, which expands
    many seeds at one level, is not used for it.

    `end_labels` keeps only the paths ending on a node carrying every listed label, the
    way `(m:Interest)` constrains the end of `MATCH (n)-[e]->{2,4}(m:Interest)`. The
    fuse_explore_end_constraint pass sets it from the label filter codegen leaves over
    `tgtids`. A path ending elsewhere is walked through but never emitted, and a prefix
    that cannot reach such a node within the hops it has left is not walked at all.

    `hop_labels` is a hop predicate that asks only for labels on the hop's end node, the way
    `(b:Event)` constrains every hop of `MATCH (n)((a)-[e]->(b:Event)){1,3}(m)`: a hop whose end
    lacks one of them is neither emitted nor expanded. The fuse_explore_hop_labels pass sets it
    from the label checks a `hop` region asks of the end node, which leave the region; the region
    goes when they were all it asked. A `hop` region beside it runs over the hops that carry the
    labels.

      %s, %t, %p = db.explore_paths(%n, {}) forward hops 1 to 3 hop_labels ["Event"]

    `end_column` names, by its index in `columns_to_filter`, the carried node column
    holding each seed row's own end: the way `(a)-->(b)` binds the end of
    `MATCH (a)-[e]->+(b), (a)-->(b)` before the exploration runs. The
    fuse_explore_end_nodes pass sets it from the equality filter codegen leaves between
    `tgtids` and that column's carried copy; the copy still comes back, equal to `tgtids`
    row by row. Only the paths ending on the row's target are emitted, and each seed prunes
    the prefixes that cannot reach its target in time.

      %s, %t, %p, %b2 = db.explore_paths(%a, {%b}) forward hops 1 end_column 0

    `end_nodes` binds the end to one set of nodes every seed shares, the way
    `MATCH (a)-[e]->+(b {name:"Adam"})` ends on the nodes named Adam, and
    `MATCH (a)-[e]->+(b)-->(c {name:"Adam"})` on the nodes with an edge to one: where
    `end_column` gives each row its own target, this gives the whole walk the same ones. The
    column is consumed in full before the walk runs, as db.shortest_path consumes its two node
    columns, so it is a pipeline breaker on that operand: it comes from a scan of its own and
    the hops walked from it, standing ahead of the scan the seeds come from, and lowering
    rejects a set the walk would outrun.
    Only the paths ending on a node of the set are emitted, the prefixes that cannot reach one
    in time are not walked, and an empty set emits nothing.

      %s, %t, %p = db.explore_paths(%a, {}) forward hops 1 to 5 end_nodes %b

    `ends_on_seed` is that same constraint when the end the pattern names is the seed it
    started from - `MATCH (a)-[e]->+(a)`, the cycle through `a` - so each row's target is
    its own `srcids` and no carried column holds it. The same pass sets it, from the
    equality codegen leaves between `tgtids` and `srcids`, and the paths that do not come
    back are pruned exactly as a bound end's are.

      %s, %t, %p = db.explore_paths(%a, {}) forward hops 1 to 7 ends_on_seed

    `distinct` asks for each (seed, end) pair once and no path at all, which is what a
    downstream DISTINCT or distinct aggregate reads out of an exploration whose path is
    unread; the fuse_explore_distinct_ends pass sets it. Up to a `min_hops` of one the
    walk answers it as a reachability search, since a walk of one to `max_hops` hops
    always shortens to a trail of as many; deeper minimums keep the trail semantics and
    walk, remembering each subtree no held edge constrained so that the prefixes arriving
    at a node later stop there. `paths` must have no use, which the verifier requires.
  }];

  let arguments = (ins Column:$input_nodes,
                       Variadic<Column>:$columns_to_filter,
                       Optional<ColumnNodeIDs>:$end_nodes,
                       Variadic<Column>:$hop_imports,
                       DenseI64ArrayAttr:$directions,
                       UI64Attr:$min_hops,
                       OptionalAttr<UI64Attr>:$max_hops,
                       OptionalAttr<EdgeTypeRefArrayArrayAttr>:$edge_types,
                       OptionalAttr<LabelRefArrayAttr>:$end_labels,
                       OptionalAttr<LabelRefArrayArrayAttr>:$hop_labels,
                       OptionalAttr<UI64Attr>:$end_column,
                       UnitAttr:$ends_on_seed,
                       UnitAttr:$distinct);

  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnNodeIDs:$tgtids,
                        ColumnPathRefs:$paths,
                        Variadic<Column>:$filtered_columns);

  let regions = (region VariadicRegion<MaxSizedRegion<1>>:$hops);

  let assemblyFormat = [{
    `(` $input_nodes `,` `{` $columns_to_filter `}` `)` custom<PathDirections>($directions) `hops` $min_hops
    (`to` $max_hops^)? (`edge_types` custom<StepNames>($edge_types)^)? (`end_labels` $end_labels^)?
    (`hop_labels` custom<StepNames>($hop_labels)^)? (`end_nodes` $end_nodes^)? (`end_column` $end_column^)?
    (`hop_imports` `{` $hop_imports^ `}`)? (`ends_on_seed` $ends_on_seed^)?
    (`distinct` $distinct^)? `` custom<HopRegions>($hops, ref($directions))
    attr-dict `:` functional-type(operands, results)
  }];

  let extraClassDeclaration = [{
    size_t getStepCount() { return getDirections().size(); }
    ::mlir::storage::PathDirection getStepDirection(size_t step);

    // The names of one step, null when the step has none
    ::mlir::ArrayAttr getStepEdgeTypes(size_t step);
    ::mlir::ArrayAttr getStepHopLabels(size_t step);
  }];

  let hasVerifier = 1;
}

/**
* ExpandPath
*/
def ExpandPath : TuringOp<"expand_path", [Pure]> {
  let summary = "Expand each path handle into the list its variable binds to";

  let description = [{
    Turns a column of path handles into a column of lists, one per row: `kind edges`
    is the ordered edge IDs the path variable binds to, `kind ends` the end node of each
    hop and `kind sources` the source node of each hop (the seed, read from `srcids`,
    followed by every end but the last) - the lists the group variables of a
    quantified path pattern bind to. `kind nodes` is every node the path runs through,
    the seed then the end of each hop, which is what nodes() reads off a named path
    over the walk; a path of no hops gives the seed alone.

    `reversed` writes the list backwards, for an exploration the pattern's far end seeded:
    the walk took its hops the other way round, so the last one it took is the pattern's
    first. The kinds swap with it - a reversed `kind ends` is the pattern's source list and
    a reversed `kind sources` its end list - so it is the reversed `kind sources` that
    reads the seed.

      %e = db.expand_path(%p, %s) kind edges
             : (!db.column<!storage.path_ref>, !db.column<!storage.node_id>) -> !db.column<!storage.list<!storage.edge_id>>

    `step k of n` reads a walk repeating a body of `n` steps: the list holds hop `k` of each
    repetition alone, one entry per repetition, the way a group variable of a body of several
    hops binds one entity per repetition. `reversed` reverses that list in turn. `kind nodes`
    lists every node of the walk and takes no step.

      %y = db.expand_path(%p) kind ends step 0 of 2
             : (!db.column<!storage.path_ref>) -> !db.column<!storage.list<!storage.node_id>>
  }];

  // The seeds are read by whichever kind ends up holding them - `kind sources` in walk
  // order, `kind ends` reversed, `kind nodes` either way - so the other kind leaves them out
  let arguments = (ins ColumnPathRefs:$paths,
                       Optional<ColumnNodeIDs>:$srcids,
                       PathExpansionKind:$kind,
                       UnitAttr:$reversed,
                       OptionalAttr<UI64Attr>:$step,
                       OptionalAttr<UI64Attr>:$steps);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $paths (`,` $srcids^)? `)` `kind` $kind (`reversed` $reversed^)? (`step` $step^ `of` $steps)?
    attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* PathLength
*/
def PathLength : TuringOp<"path_length", [Pure]> {
  let summary = "Read the number of edges, or of nodes, of each path";

  let description = [{
    The `size(e)` of a path variable: one unsigned count per row, read off the handle
    without expanding the path. The fuse_path_length pass rewrites the db.size of a
    db.expand_path into one.

      %n = db.path_length(%p) : (!db.column<!storage.path_ref>) -> !db.column<ui64>

    `steps n` reads a walk repeating a body of `n` steps, the way db.expand_path's
    `step k of n` does: the count is the repetitions, one per entity a group variable binds.

      %n = db.path_length(%p) steps 2 : (!db.column<!storage.path_ref>) -> !db.column<ui64>

    `nodes` counts the entries of db.expand_path's `kind nodes` list instead: the seed and
    the end of each hop, one more than the hops.

      %n = db.path_length(%p) nodes : (!db.column<!storage.path_ref>) -> !db.column<ui64>
  }];

  let arguments = (ins ColumnPathRefs:$paths, OptionalAttr<UI64Attr>:$steps, UnitAttr:$nodes);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $paths `)` (`steps` $steps^)? (`nodes` $nodes^)? attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* MakePath
*/
def MakePath : TuringOp<"make_path", [Pure]> {
  let summary = "Build the path a named pattern element binds, one path per row";

  let description = [{
    `MATCH p = (a)-[x]->(b)` binds `p` to the path the element spells out. This op lays
    that path out as one path per row - the nodes and the edges it runs through, in the
    order they are written.

      %p = db.make_path(%a, %x, %b)
             : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>)
               -> !db.column<!storage.path>

    The operands are the element's entities in written order. A node column contributes
    one node, an edge column one edge, and a path column - what a quantified relationship
    binds - the whole walk it stands for: one edge and one node per hop, its seed left
    out because the node operand ahead of it already holds it. So
    `MATCH p = (a)-[e]->+(b)` is `db.make_path(%a, %pe)`, with no operand for `b`: the
    last node the walk reaches is `b`.

      %p = db.make_path(%a, %pe) : (...) -> !db.column<!storage.path>
      %p = db.make_path(%a, %x, %b, %pe) : (...) -> !db.column<!storage.path>

    A path of no hops contributes nothing, so `MATCH p = (a)-[e]->*(m)` reads back as
    the single node `a` on the rows where the walk stayed put.

    `reversed_paths` lists the path operands the exploration walked against the pattern,
    because the pattern's far end was bound first and seeding there was cheaper. Such an
    operand contributes its hops from the far end back - the node an edge lands on, then
    that edge - so the path still reads the way the pattern is written. The node the
    walk seeded from is written after the path rather than before it. Written after a
    node, the path writes over that node, which is the one the walk ended at.

      %p = db.make_path(%pe, %b) {reversed_paths = [0]} : (...) -> !db.column<!storage.path>
      %p = db.make_path(%a, %x, %k, %pe, %b) {reversed_paths = [3]} : (...) -> !db.column<!storage.path>
  }];

  let arguments = (ins Variadic<Column>:$entities,
                       OptionalAttr<I64ArrayAttr>:$reversed_paths);
  let results   = (outs ColumnPath:$result);

  let assemblyFormat = [{
    `(` $entities `)` attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

/**
* PathElements
*/
def PathElements : TuringOp<"path_elements", [NoMemoryEffect,
                                               DeclareOpInterfaceMethods<ConditionallySpeculatable>,
                                               ConstantThroughOperands]> {
  let summary = "Read the nodes, the relationships or the length of each named path";

  let description = [{
    `nodes(p)`, `relationships(p)` and `length(p)` over the path a named pattern
    element binds: one list of node IDs, one list of edge IDs or one hop count per row,
    read off the path db.make_path laid out, or off the path db.shortest_path
    found. A path held in a list reaches it too, as the element a reduce or a
    comprehension binds, whose column type lowering resolves, or as a tagged cell: a
    tagged cell holding anything but a path or a null raises, so the op is speculatable
    over a path column alone.

      %ns = db.path_elements(%p) kind nodes
              : (!db.column<!storage.path>) -> !db.column<!storage.nullable<!storage.list<!storage.node_id>>>
      %n = db.path_elements(%p) kind length
              : (!db.column<!storage.path>) -> !db.column<!storage.nullable<ui64>>

    A path an OPTIONAL MATCH missed is null, and each function of it is
    null, so the results are nullable. Over the path of one node and one walk,
    fuse_path_elements answers from the walk's handles instead: db.path_length for the
    length and db.expand_path for the two lists.
  }];

  let arguments = (ins Column:$path, PathElementsKind:$kind);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $path `)` `kind` $kind attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;

  let extraClassDefinition = [{
    ::mlir::Speculation::Speculatability $cppClass::getSpeculatability() {
      return speculatableOverElement<::mlir::storage::PathType>(getPath());
    }
  }];
}

/**
* ShortestPath
*/
def ShortestPath : TuringOp<"shortest_path", [Pure]> {
  let summary = "Weighted shortest path from a source node set to a target node set";
  let description = [{
    TuringDB's `SHORTESTPATH(source, target, edgeProperty, distVar, pathVar)`: the
    minimum-weight directed path from any source node to any target node

      %dist, %path = db.shortest_path(%a, %b) weight "cost"
                       : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
                         -> (!db.column<none>, !db.column<!storage.path>)

    Pipeline breaker as both node columns are consumed in full. The distance column
    is typed `none` here and resolved to the weight property's value type at lowering.
  }];

  let arguments = (ins ColumnNodeIDs:$source, ColumnNodeIDs:$target, StrAttr:$edgeProperty);
  let results   = (outs Column:$distance, ColumnPath:$path);

  let assemblyFormat = [{
    `(` $source `,` $target `)` `weight` $edgeProperty attr-dict `:`
    functional-type(operands, results)
  }];
}

#endif // TURINGDB_PATH_OPS
