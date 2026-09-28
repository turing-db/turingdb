// UNWIND [1, 2, 3] AS x RETURN 5

func.func @main() {
  %0 = db.unwind_const([1, 2, 3])
  %1 = db.constant(5)
  db.output(%1) names ["5"]
  return
}
