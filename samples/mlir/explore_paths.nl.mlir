// Generated nl-dialect lowering of explore_paths.mlir (db dialect).
// Reproduce with: mlir -dump-lowered explore_paths.mlir
// This is the DBLowering output; edit explore_paths.mlir, not this file.
//
// MATCH (n)-[e:KNOWS]->{1,3}(m) RETURN n, e, m, size(e). The exploration nests in the
// scan loop like a hop, and its nl.for binds the seed, the end node and the path handle;
// the edge list and the hop count are read off the handle inside that loop.
module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %1 = nl.explore_paths(%arg0, {}) forward hops 1 to 3 edge_type "KNOWS"
      nl.for %arg1, %arg2, %arg3 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.path_ref>> {
        %2 = nl.expand_path(%arg3, %arg1) kind edges : (!nl.chunk<!storage.path_ref>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.list<!storage.edge_id>>
        %3 = nl.path_length(%arg3) : (!nl.chunk<!storage.path_ref>) -> !nl.chunk<ui64>
        nl.output(%arg1, %2, %arg2, %3) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.list<!storage.edge_id>>, !nl.chunk<!storage.node_id>, !nl.chunk<ui64>
      }
    }
    return
  }
  
}
