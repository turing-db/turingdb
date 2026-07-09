module {
  func.func @main() {
    %0 = nl.constant(30 : i64)
    nl.output(%0) : !nl.chunk<i64>
    return
  }
}
