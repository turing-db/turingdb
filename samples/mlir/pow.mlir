// RETURN 2 ^ 10

module {
  func.func @main() {
    %x = db.constant(2)
    %y = db.constant(10)
    %s = db.pow %x, %y

    db.output(%s)

    return
  }
}
