#ifndef TURINGDB_PASSES
#define TURINGDB_PASSES

include "mlir/Pass/PassBase.td"

def GetOutEdgesDCE : Pass<"get_out_edges_dce"> {
    let summary = "Replaces unused variables in GetOutEdges with null";
    let dependentDialects = ["mlir::db::DB"];
}

#endif // TURINGDB_PASSES
