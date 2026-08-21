// Generated nl-dialect lowering of count_distinct.mlir (db dialect).
// Reproduce with: mlir -dump-lowered count_distinct.mlir
// This is the DBLowering output; edit count_distinct.mlir, not this file.
//
// A distinct db.count lowers to the plain count shape with the DISTINCT pair wrapped
// around the update: the nl.count tally and the nl.distinct seen-set are both hoisted
// above the loops, and inside the traversal loop an nl.distinct_filter cuts the chunk
// down to the not-yet-seen targets before nl.count_update charges them. So the tally
// counts each distinct value once - and never counts the null the filter lets through,
// since nl.count_update charges only non-null rows.
module {
  func.func @main() {
    %0 = nl.count
    %1 = nl.distinct
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %5 = nl.distinct_filter %1, (%arg4) : !nl.chunk<!storage.node_id>
        nl.count_update %0, %5 : !nl.chunk<!storage.node_id>
      }
    }
    %3 = nl.count_result(%0) : !nl.chunk<ui64>
    nl.output(%3) : !nl.chunk<ui64>
    return
  }
}
