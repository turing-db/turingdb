#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBTypes.td"

/**
* ScanNodes
*/
def ScanNodes : TuringOp<"scan_nodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs Column:$result);
  let assemblyFormat = "`(`operands`)` attr-dict `:` type($result)";
}

/**
* GetOutEdges
*/
def GetOutEdges : TuringOp<"get_out_edges", [Pure]> {
  let summary = "Fetch node and edge successors of input";
  let arguments = (ins Column:$input_nodes);
  let results   = (outs Column:$srcids, Column:$eids, Column:$etypes, Column:$tgtids);
}

#endif //TURINGDB_OPS
