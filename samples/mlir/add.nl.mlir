module {
  func.func @main() {
    %0 = nl.constant(20)
    %1 = nl.constant(10)
    %2 = nl.add %1, %0
    nl.output(%2)
    return
  }
}
