module {
  func.func @main() {
    %i = db.constant(30)
    %u = db.constant(7)
    %f = db.constant(2.5)
    %b = db.constant(true)
    db.output(%i, %u, %f, %b)
    return
  }
}
