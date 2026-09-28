// RETURN 2 ^ 10

module {
  func.func @main() {
    %0 = nl.constant(10)
    %1 = nl.constant(2)
    %2 = nl.pow %1, %0
    nl.output(%2)
    return
  }
}
