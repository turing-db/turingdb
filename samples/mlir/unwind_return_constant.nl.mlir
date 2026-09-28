// UNWIND [1, 2, 3] AS x RETURN 5
//
// The scan-driven scan_return_constant.nl.mlir with a list for a driving relation: the
// projection is a constant alone either way, and the cardinality operand is whatever
// chunk carries the rows it stands for - here the unwound values, a nullable i64 chunk
// rather than the node IDs a scan binds. Only its row count is read.

func.func @main() {
  %0 = nl.constant(5 : i64)
  %1 = nl.unwind_const([1, 2, 3]) : !nl.iter<!nl.chunk<!storage.nullable<i64>>>
  nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
    nl.output(%0) names ["5"] cardinality(%arg0 : !nl.chunk<!storage.nullable<i64>>) : !nl.chunk<i64>
  }
  return
}
