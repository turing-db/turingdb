// RETURN 2 ^ 10

module {
  func.func @main() {
    %x = db.constant(2 : i64)
    %y = db.constant(10 : i64)
    %s = db.pow %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<f64>

    db.output(%s) : !db.column<f64>

    return
  }
}
