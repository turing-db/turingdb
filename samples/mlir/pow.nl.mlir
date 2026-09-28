// RETURN 2 ^ 10

module {
  func.func @main() {
    %0 = nl.constant(10 : i64)
    %1 = nl.constant(2 : i64)
    %2 = nl.pow %1, %0 : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<f64>
    nl.output(%2) : !nl.chunk<f64>
    return
  }
}
