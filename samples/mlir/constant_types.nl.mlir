module {
  func.func @main() {
    %0 = nl.constant(true)
    %1 = nl.constant(2.500000e+00 : f64)
    %2 = nl.constant(7 : ui64)
    %3 = nl.constant(30 : i64)
    nl.output(%3, %2, %1, %0) : !nl.chunk<i64>, !nl.chunk<ui64>, !nl.chunk<f64>, !nl.chunk<i1>
    return
  }
}
