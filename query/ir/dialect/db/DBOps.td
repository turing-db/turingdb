#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBTypes.td"

/**
* ScanNodes
* Pure: this operation has no side effects
*/
def ScanNodes : TuringOp<"ScanNodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs Column:$result);
}

#endif //TURINGDB_OPS
