// RETURN toInteger("42")

module {
  func.func @main() {
    %s = db.constant("42")

    %i = db.to_integer(%s)

    db.output(%i)

    return
  }
}
