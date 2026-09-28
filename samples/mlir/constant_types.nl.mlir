module {
  func.func @main() {
    %0 = nl.constant(true)
    %1 = nl.constant(2.500000e+00)
    %2 = nl.constant(7)
    %3 = nl.constant(30)
    nl.output(%3, %2, %1, %0)
    return
  }
}
