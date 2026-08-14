#ifndef TURINGDB_PASSES
#define TURINGDB_PASSES

include "mlir/Pass/PassBase.td"

def FuseScanByLabel : Pass<"fuse_scan_by_label"> {
    let summary = "Fuse a scan_nodes and its label-set filter into a scan_nodes_by_label";
    let dependentDialects = ["mlir::db::DB"];
}

#endif // TURINGDB_PASSES
