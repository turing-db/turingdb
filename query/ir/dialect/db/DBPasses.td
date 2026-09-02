#ifndef TURINGDB_PASSES
#define TURINGDB_PASSES

include "mlir/Pass/PassBase.td"

def FuseScanByLabel : Pass<"fuse_scan_by_label"> {
    let summary = "Fuse a scan_nodes and its label-set filter into a scan_nodes_by_label";
    let dependentDialects = ["mlir::db::DB"];
}

def PushDownFilters : Pass<"push_down_filters"> {
    let summary = "Sink a single-variable property-predicate filter to its variable's scan";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseScanByNodeIDs : Pass<"fuse_scan_by_node_ids"> {
    let summary = "Fuse a node scan and its node-ID disjunction filter into a const_scan_nodes";
    let dependentDialects = ["mlir::db::DB"];
}

#endif // TURINGDB_PASSES
