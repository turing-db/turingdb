module {
  func.func @main() {
    %i = db.constant(30 : i64)
    %u = db.constant(7 : ui64)
    %f = db.constant(2.5 : f64)
    %b = db.constant(true)
    db.output(%i, %u, %f, %b) : !db.column<i64>, !db.column<ui64>, !db.column<f64>, !db.column<i1>
    return
  }
}
