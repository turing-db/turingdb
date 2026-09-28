// Generated nl-dialect lowering of aggregate.mlir (db dialect).
// Reproduce with: mlir -dump-lowered aggregate.mlir -graph <graph>
// This is the DBLowering output; edit aggregate.mlir, not this file.
//
// db.sum - like db.min/db.max/db.avg - is a pipeline breaker like db.count: a
// hoisted nl.aggregate accumulator, an nl.aggregate_update inside the scan loop
// that folds each chunk's non-null values, and - after the loop - an
// nl.aggregate_result that materializes the single reduced row as a nullable value
// chunk, which a function-scope nl.output emits. It collapses to one row, so there
// is no emit loop.
//
// Unlike the other lowered samples this one needs -graph: the result element type
// is resolved from the "score" property during lowering. score is Int64, so sum
// keeps it and the chunk is !storage.nullable<i64>; an avg would widen to
// !storage.nullable<f64>.
module {
  func.func @main() {
    %0 = nl.aggregate sum : !nl.aggregate_state<i64>
    %1 = nl.get_property_type("score")
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
      nl.aggregate_update sum %0, %4 : !nl.aggregate_state<i64>, !nl.chunk<!storage.nullable<i64>>
    }
    %3 = nl.aggregate_result sum(%0) : !nl.aggregate_state<i64> -> !nl.chunk<!storage.nullable<i64>>
    nl.output(%3) : !nl.chunk<!storage.nullable<i64>>
    return
  }
}
