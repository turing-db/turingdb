module {
  func.func @main() {
    %c = db.constant(30)

    db.output(%c)

    return
  }
}
