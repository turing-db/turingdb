// RETURN toInteger("42")

module {
  func.func @main() {
    %0 = nl.constant("42")
    %1 = nl.to_integer %0
    nl.output(%1)
    return
  }
}
