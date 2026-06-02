#ifndef TURINGDB_OPS
#define TURINGDB_OPS

/**
* ScanNodes
* Pure: this operation has no side effects
*/
def ScanNodes : TuringOp<"ScanNodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs I64:$result);
}

#endif TURINGDB_OPS
