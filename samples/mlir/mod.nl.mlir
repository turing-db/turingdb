// RETURN 10 % 3

module {
  func.func @main() {
    %0 = nl.constant(3 : i64)
    %1 = nl.constant(10 : i64)
    %2 = nl.mod %1, %0 : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i64>
    nl.output(%2) : !nl.chunk<i64>
    return
  }
}
