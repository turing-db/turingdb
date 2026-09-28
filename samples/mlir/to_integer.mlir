// RETURN toInteger("42")

module {
  func.func @main() {
    %s = db.constant("42" : !storage.string)

    %i = db.to_integer(%s) : (!db.column<!storage.string>) -> !db.column<none>

    db.output(%i) : !db.column<none>

    return
  }
}
