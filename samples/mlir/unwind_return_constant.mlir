// UNWIND [1, 2, 3] AS x RETURN 5

func.func @main() {
  %0 = db.unwind_const([1, 2, 3]) : !db.column<i64>
  %1 = db.constant(5 : i64)
  %2 = db.broadcast_constant(%1, %0) : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>
  db.output(%2) names ["5"] : !db.column<i64>
  return
}
