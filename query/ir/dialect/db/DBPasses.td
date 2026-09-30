#ifndef TURINGDB_PASSES
#define TURINGDB_PASSES

include "mlir/Pass/PassBase.td"

def ProveDistinctEdges : Pass<"prove_distinct_edges"> {
    let summary = "Drop the edge pairs of a check_edge_distinct that the pattern's edge types or node labels prove distinct";
    let dependentDialects = ["mlir::db::DB"];
}

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

def FuseScanByPropertyValue : Pass<"fuse_scan_by_property_value"> {
    let summary = "Fuse a node scan and its property-equality filter into a scan_nodes_by_property_value";
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

def FuseScanOutEdgesByLabel : Pass<"fuse_scan_out_edges_by_label"> {
    let summary = "Fuse a by-label node scan and its directed untyped out-hop into a scan_out_edges_by_label_src";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseScanInEdgesByLabel : Pass<"fuse_scan_in_edges_by_label"> {
    let summary = "Fuse a by-label node scan and its directed untyped in-hop into a scan_in_edges_by_label_tgt";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseScanEdgesByEndpointLabel : Pass<"fuse_scan_edges_by_endpoint_label"> {
    let summary = "Fuse a whole edge scan and the label check on one of its endpoints into a by-label edge scan";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseEdgesByEndpointLabel : Pass<"fuse_edges_by_endpoint_label"> {
    let summary = "Fuse a directed hop and the label check on the endpoint it reaches into a by-label or by-type-and-label hop";
    let dependentDialects = ["mlir::db::DB"];
}

def RemoveRedundantLabelChecks : Pass<"remove_redundant_label_checks"> {
    let summary = "Remove a label filter over nodes already known to carry every label it asks for";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseEdgeTypePredicates : Pass<"fuse_edge_type_predicates"> {
    let summary = "Combine two edge-type checks over the same column into one";
    let dependentDialects = ["mlir::db::DB"];
}

def NarrowEdgeTypeReads : Pass<"narrow_edge_type_reads"> {
    let summary = "Fold an edge-type check into the by-type read feeding it, narrowing its types";
    let dependentDialects = ["mlir::db::DB"];
}

def ReusePropertyReads : Pass<"reuse_property_reads"> {
    let summary = "Reuse a property already read of the same rows instead of reading it again";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseListFetchNode : Pass<"fuse_list_fetch_node"> {
    let summary = "Replace a whole node scan bound by an equality to a column of IDs with a fetch of those IDs";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseHashJoin : Pass<"fuse_hash_join"> {
    let summary = "Fuse a cross product and the equality filter over it into a hash_join";
    let dependentDialects = ["mlir::db::DB"];
}

def CountFromMetadata : Pass<"count_from_metadata"> {
    let summary = "Answer a count over whole node scans from the graph's node counts";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseExploreEndConstraint : Pass<"fuse_explore_end_constraint"> {
    let summary = "Fuse a path exploration and the label filter over its end nodes into an exploration constrained to those ends";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseDistinctEdges : Pass<"fuse_distinct_edges"> {
    let summary = "Fold the edge-distinctness check over a path exploration's paths into the exploration, which then takes none of a seed row's bound edges";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseProductDistinctEdges : Pass<"fuse_product_distinct_edges"> {
    let summary = "Fold the edge-distinctness check between the two factors of a cross product into the product, which then leaves out the pairs holding one edge twice";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseExploreHopLabels : Pass<"fuse_explore_hop_labels"> {
    let summary = "Replace the hop region of a path exploration that only asks each hop's end node for labels with those labels";
    let dependentDialects = ["mlir::db::DB"];
}

def SinkMakePath : Pass<"sink_make_path"> {
    let summary = "Build a named path after the filters that only carry it, over the rows they keep";
    let dependentDialects = ["mlir::db::DB"];
}

def FusePathElements : Pass<"fuse_path_elements"> {
    let summary = "Read the nodes, the relationships or the length of a named path over one walk off the walk's handles instead of the path it builds";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseExploreEndNodes : Pass<"fuse_explore_end_nodes"> {
    let summary = "Fuse a path exploration and the equality filter binding its end nodes to a carried column into an exploration ending on that column";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseExploreEndFactor : Pass<"fuse_explore_end_factor"> {
    let summary = "Turn the cross-product factor a path exploration's end column is drawn from into the set of ends the walk heads for";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseExploreEndSet : Pass<"fuse_explore_end_set"> {
    let summary = "Fuse a path exploration and the property-equality filter over its end nodes into an exploration ending on the set of nodes holding that value";
    let dependentDialects = ["mlir::db::DB"];
}

def FuseExploreDistinctEnds : Pass<"fuse_explore_distinct_ends"> {
    let summary = "Mark a path exploration whose path is unread and whose rows are only ever deduplicated as distinct, so it emits each end once";
    let dependentDialects = ["mlir::db::DB"];
}

def CountPathRows : Pass<"count_path_rows"> {
    let summary = "Count the rows a path was built over instead of building the path to count it";
}

def TrimUnreadColumns : Pass<"trim_unread_columns"> {
    let summary = "Drop the columns no later op reads from hops, filters, cuts, sorts, products, grouping ops, calls and CASEs";
    let dependentDialects = ["mlir::db::DB"];
}

#endif // TURINGDB_PASSES
