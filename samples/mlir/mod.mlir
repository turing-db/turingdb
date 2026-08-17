// RETURN 10 % 3

module {
  func.func @main() {
    %x = db.constant(10 : i64)
    %y = db.constant(3 : i64)
    %s = db.mod %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>

    db.output(%s) : !db.column<i64>

    return
  }
}
