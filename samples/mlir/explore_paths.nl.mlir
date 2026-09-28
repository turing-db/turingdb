// Generated nl-dialect lowering of explore_paths.mlir (db dialect).
// Reproduce with: mlir -dump-lowered explore_paths.mlir
// This is the DBLowering output; edit explore_paths.mlir, not this file.
//
// MATCH (n)-[e:KNOWS]->{1,3}(m) RETURN n, e, m, size(e). The exploration nests in the
// scan loop like a hop, and its nl.for binds the seed, the end node and the path handle;
// the edge list and the hop count are read off the handle inside that loop.
module {
  func.func @main() {
    %0 = nl.get_edge_type_set(["KNOWS"])
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.explore_paths(%arg0, {}) forward hops 1 to 3 edge_types %0
      nl.for %arg1, %arg2, %arg3 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.path_ref>> {
        %3 = nl.expand_path(%arg3, %arg1) kind edges : (!nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<!storage.edge_id>>
        %4 = nl.path_length(%arg3) : (!nl.chunk<!storage.path_ref>) -> !nl.chunk<ui64>
        nl.output(%arg1, %3, %arg2, %4) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.list<!storage.edge_id>>, !nl.chunk<!storage.node_id>, !nl.chunk<ui64>
      }
    }
    return
  }
}
