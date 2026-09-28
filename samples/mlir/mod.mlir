// RETURN 10 % 3

module {
  func.func @main() {
    %x = db.constant(10)
    %y = db.constant(3)
    %s = db.mod %x, %y

    db.output(%s)

    return
  }
}
