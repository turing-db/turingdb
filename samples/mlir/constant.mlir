module {
  func.func @main() {
    %c = db.constant(30 : i64)

    db.output(%c) : !db.column<i64>

    return
  }
}
