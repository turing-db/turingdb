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

def FuseUnwindEquality : Pass<"fuse_unwind_equality"> {
    let summary = "Replace a constant unwind crossed into one equality by that equality's disjunction";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseScanByNodeIDs : Pass<"fuse_scan_by_node_ids"> {
    let summary = "Fuse a node scan and its node-ID disjunction filter into a const_scan_nodes";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseScanEdges : Pass<"fuse_scan_edges"> {
    let summary = "Fuse a whole-graph node scan and its directed untyped hop into a scan_edges";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseEdgesByType : Pass<"fuse_edges_by_type"> {
    let summary = "Fuse a directed hop and the edge-type filter over it into a by-type hop";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseScanEdgesByType : Pass<"fuse_scan_edges_by_type"> {
    let summary = "Fuse an edge scan and the edge-type filter over it into a scan_edges_by_type";
    let dependentDialects = ["mlir::db::DB"];
}

def TrimUnreadColumns : Pass<"trim_unread_columns"> {
    let summary = "Drop the columns no later op reads from hops, filters, cuts, sorts, products and grouping ops";
    let dependentDialects = ["mlir::db::DB"];
}

#endif // TURINGDB_PASSES
