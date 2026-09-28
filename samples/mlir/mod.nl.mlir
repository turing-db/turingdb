// RETURN 10 % 3

module {
  func.func @main() {
    %0 = nl.constant(3)
    %1 = nl.constant(10)
    %2 = nl.mod %1, %0
    nl.output(%2)
    return
  }
}
